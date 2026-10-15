"""Private, bounded Wikisource image/reference-text pairing from frozen PDFs.

This module does not upload, create repositories, run OCR, or certify gold text.
The source association and one-based page position are preserved as evidence,
not presented as independently verified image/text correspondence.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import gzip
import io
import json
import os
import math
from pathlib import Path
import re
import shutil
import sqlite3
import sys
import threading
import time
from urllib.parse import urlsplit, urlunsplit, parse_qsl
from urllib.robotparser import RobotFileParser
import zlib

from datasets import Features, Image as HFImage, load_dataset
import pyarrow as pa
import pyarrow.parquet as pq
import pypdfium2 as pdfium
import requests
from PIL import Image

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))
from audit.package_community import SCHEMA as COMMUNITY_SCHEMA
from quality.validate import image_fingerprints

PROFILE = 'wikisource-original-pdf-pdfium-longedge2400-jpeg90-reference-v1'
USER_AGENT = 'BayanatDocumentAtlas/0.1 (+https://www.bayanatlabs.com; hello@bayanatlabs.com)'
PDF_LOCK = threading.Lock()
MIN_FREE = 4 * 1024**3
KNOWN_IMAGE_RIGHTS_HOLDS = {
    'ae04efe19611e6917ec0388c9293b4c05976795d': 'Exact Commons original PDFpage4 licenses edition design/cover CC-BY-NC-ND4.0, contrary to source metadata CC-BY4.0; current publisher PDF is a different version.'}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def sha(path, algorithm='sha256'):
    h = hashlib.new(algorithm)
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1024**2), b''):
            h.update(block)
    return h.hexdigest()


def atomic_json(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    temporary.replace(path)


def schema():
    fields = list(COMMUNITY_SCHEMA)
    fields += [pa.field(n, pa.string(), nullable=False) for n in (
        'page_id', 'reference_pair_id', 'document_id', 'split', 'reference_text',
        'reference_text_source', 'image_request_profile', 'source_pdf_sha256',
        'source_pdf_sha1', 'source_pdf_download_url', 'image_sha256', 'pixel_sha256',
        'image_reference_pairing_basis', 'processing_config_sha256', 'renderer', 'renderer_version')]
    fields += [pa.field(n, pa.int32(), nullable=False) for n in (
        'source_pdf_page_index', 'source_pdf_page_count', 'width', 'height')]
    fields += [pa.field('image', pa.struct([pa.field('bytes', pa.binary()), pa.field('path', pa.string())]))]
    raw = pa.schema(fields)
    features = Features.from_arrow_schema(raw)
    features['image'] = HFImage()
    return raw.with_metadata(features.arrow_schema.metadata)


SCHEMA = schema()


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError('Timestamp is missing')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('Timestamp needs an explicit timezone')
    return result


def download_url(value):
    """Drop only documented analytics parameters; retain exact source URL in row."""
    if not isinstance(value, str):
        raise ValueError('Missing original PDF URL')
    u = urlsplit(value)
    if (u.scheme != 'https' or u.hostname != 'upload.wikimedia.org' or u.username or u.password
            or u.port not in (None, 443) or u.fragment or not u.path.lower().endswith('.pdf')):
        raise ValueError('Original PDF URL is outside the source allowlist')
    query = parse_qsl(u.query, keep_blank_values=True)
    if any(k not in {'utm_source', 'utm_campaign', 'utm_content'} for k, _ in query):
        raise ValueError('Unknown source PDF query parameters')
    return urlunsplit((u.scheme, u.netloc, u.path, '', ''))


def explicit_cc_license(value):
    try:
        u=urlsplit(value)
        return (u.scheme in ('http','https') and u.hostname=='creativecommons.org' and not u.username and not u.password
                and u.port in (None,80,443) and not u.query and not u.fragment
                and re.fullmatch(r'/licenses/by(?:-sa)?/[1-4]\.0/?',u.path) is not None)
    except (TypeError,ValueError): return False


def selection_reason(row, max_pdf_bytes, explicit_cc_only=True):
    if row.get('scan_mime') != 'application/pdf': return 'non_pdf_format'
    if row.get('scan_file_sha1') in KNOWN_IMAGE_RIGHTS_HOLDS: return 'known_original_pdf_rights_notice_conflict'
    if 'scan_image_redistribution_held_pdf_license_conflict' in (row.get('quality_flags') or []): return 'source_image_rights_conflict_flag'
    if row.get('source_quality_level') not in (3, 4) or row.get('quality_fields_agree') is not True: return 'source_quality_not_3_or_4_or_disagrees'
    if not isinstance(row.get('source_linked_aco_book_ids'), list): return 'unknown_aco_link_state'
    if row['source_linked_aco_book_ids']: return 'known_aco_linked_file'
    if row.get('scan_file_version_postdates_text_revision') is not False: return 'postdated_or_unknown_scan_version'
    try:
        if timestamp(row.get('scan_file_timestamp')) > timestamp(row.get('source_revision_timestamp')): return 'postdated_or_unknown_scan_version'
    except (ValueError, TypeError): return 'invalid_scan_or_text_timestamp'
    if row.get('scan_rights_review_status') not in ('source_asserted_public_domain', 'source_asserted_open_license'): return 'unrecognized_or_restrictive_file_rights'
    rights = ' '.join(str(row.get(k) or '') for k in ('scan_license_short_name', 'scan_license_code', 'scan_license_url', 'scan_usage_terms')).lower()
    if re.search(r'\bnc\b|\bnd\b|noncommercial|no.?derivative|fair.?use', rights): return 'contradictory_restrictive_file_terms'
    if explicit_cc_only and not explicit_cc_license(row.get('scan_license_url')): return 'pilot_requires_explicit_cc_by_or_by_sa_file_license'
    if row.get('text_license') != 'CC-BY-SA-4.0': return 'unexpected_text_license'
    if row.get('text_scope') != 'transcluded_page_body_excluding_noinclude_header_footer': return 'unknown_reference_body_scope'
    if not isinstance(row.get('source_wikitext'), str) or hashlib.sha1(row['source_wikitext'].encode()).hexdigest()!=row.get('source_revision_sha1'): return 'source_wikitext_revision_hash_mismatch'
    if not isinstance(row.get('text'), str) or not row['text'].strip(): return 'missing_reference_text'
    if not isinstance(row.get('record_id'), str) or not row['record_id']: return 'missing_source_record_identity'
    if not isinstance(row.get('file_group_id'),str) or not row['file_group_id']: return 'missing_source_file_group_identity'
    if not re.fullmatch(r'[0-9a-f]{40}', str(row.get('scan_file_sha1') or '')): return 'missing_hex_scan_sha1'
    if type(row.get('scan_file_size_bytes')) is not int or row['scan_file_size_bytes'] <= 0: return 'missing_source_pdf_size'
    if row['scan_file_size_bytes'] > max_pdf_bytes: return 'pdf_exceeds_byte_cap'
    n, count = row.get('scan_page_number'), row.get('scan_file_page_count')
    if type(n) is not int or type(count) is not int or not 1 <= n <= count: return 'invalid_source_page_range'
    try: download_url(row.get('scan_file_url'))
    except ValueError: return 'unsafe_original_pdf_url'
    return None


def verify_pdf(path, row):
    if Path(path).stat().st_size != row['scan_file_size_bytes']: raise ValueError('Original PDF byte size differs from source metadata')
    if sha(path, 'sha1') != row['scan_file_sha1']: raise ValueError('Original PDF SHA1 differs from frozen source version')
    with Path(path).open('rb') as stream:
        if stream.read(5) != b'%PDF-': raise ValueError('Source payload is not a PDF')
    with PDF_LOCK:
        with pdfium.PdfDocument(str(path)) as pdf:
            if len(pdf) != row['scan_file_page_count']: raise ValueError('Actual PDF page count differs from source metadata')
    return sha(path)


def audit_native_rights_notices(path):
    """A negative-evidence screen, not independent clearance of scanned notices."""
    import unicodedata
    text=[];matches=[]
    with PDF_LOCK:
        with pdfium.PdfDocument(str(path)) as pdf:
            for index in range(min(12,len(pdf))):
                page=pdf[index]
                try:
                    native=page.get_textpage()
                    try: text.append(native.get_text_range(0,min(200000,native.count_chars())))
                    finally: native.close()
                finally: page.close()
    joined=unicodedata.normalize('NFKC','\n'.join(text)).lower()
    for index,value in enumerate(text):
        normalized=unicodedata.normalize('NFKC',value).lower();compact=re.sub('[^a-z]','',normalized)
        if ('creativecommons.org/licenses/by-nc' in normalized or 'creativecommons.org/licenses/by-nd' in normalized
                or 'creativecommonsorglicensesbync' in compact or 'creativecommonsorglicensesbynd' in compact
                or ('creativecommons' in compact and ('noncommercial' in compact or 'noderivatives' in compact))):
            matches.append({'source_pdf_page_index':index,'extracted_text':value,'extracted_text_sha256':hashlib.sha256(value.encode()).hexdigest()})
    return {'first_pages_checked':min(12,len(text)),'native_text_characters':len(joined),'restrictive_cc_notice_found':bool(matches),'notice_matches':matches,
            'scope':'Native PDF text only; scanned notices and later pages are not independently cleared.'}


def source_file_evidence(source):
    result={}
    with gzip.open(source/'metadata/wikisource/scan_files.jsonl.gz','rt',encoding='utf-8') as f:
        for line in f:
            row=json.loads(line);result[row['scan_filename'].replace('_',' ')]=row
    return result


def vrt_permission(evidence):
    permission=evidence.get('source_file_info',{}).get('extmetadata',{}).get('Permission',{}).get('value','')
    return bool(re.search('PermissionTicket|ticket.wikimedia.org',permission))


def retain_verified_input(path,private,expected_sha256):
    """Keep this small fixed cohort's verified originals as private proof."""
    if path.is_symlink() or sha(path)!=expected_sha256: raise ValueError('Verified input retention hash/path differs')
    target=private/'verified-inputs'/path.name;target.parent.mkdir(exist_ok=True)
    if target.exists():
        if target.is_symlink() or sha(target)!=expected_sha256: raise ValueError('Archived verified input differs')
        path.unlink()
    else:path.replace(target)


def render(path, index):
    with PDF_LOCK:
        with pdfium.PdfDocument(str(path)) as pdf:
            page = pdf[index]
            try:
                size = page.get_size()
                if not all(math.isfinite(v) and v>0 for v in size): raise ValueError('Invalid PDF geometry')
                bitmap = page.render(scale=2400 / max(size))
                try:
                    image = bitmap.to_pil().convert('RGB')
                    try:
                        output = io.BytesIO(); image.save(output, 'JPEG', quality=90)
                        return output.getvalue()
                    finally: image.close()
                finally: bitmap.close()
            finally: page.close()


def make_pair(row, path, pdf_sha256, config_hash):
    index = row['scan_page_number'] - 1
    identity = hashlib.sha256(canonical([row['record_id'], row['scan_file_sha1'], index, PROFILE]).encode()).hexdigest()
    encoded = render(path, index)
    fingerprints = image_fingerprints(encoded)
    with Image.open(io.BytesIO(encoded)) as image:
        image.load()
        if image.format != 'JPEG' or image.size != (fingerprints['width'], fingerprints['height']): raise ValueError('Published JPEG validation failed')
    result = {name: row.get(name) for name in COMMUNITY_SCHEMA.names}
    result.update(page_id=identity, reference_pair_id=identity, document_id=row['file_group_id'], split='train',
                  reference_text=row['text'], reference_text_source='source_community_proofread_body_not_independently_verified',
                  image_request_profile=PROFILE, source_pdf_sha256=pdf_sha256, source_pdf_sha1=row['scan_file_sha1'],
                  source_pdf_download_url=download_url(row['scan_file_url']), source_pdf_page_index=index,
                  source_pdf_page_count=row['scan_file_page_count'], image={'bytes': encoded, 'path': identity + '.jpg'},
                  image_sha256=fingerprints['image_sha256'], pixel_sha256=fingerprints['pixel_sha256'],
                  width=fingerprints['width'], height=fingerprints['height'], contains_image_bytes=True,
                  image_reference_pairing_basis='source_scan_association_and_frozen_sha1_page_position_not_independent_visual_alignment',
                  processing_config_sha256=config_hash, renderer='pypdfium2', renderer_version=str(pdfium.PYPDFIUM_INFO),
                  image_version_correspondence_independently_verified=False, bayanat_human_verified=False,
                  has_human_line_or_region_boxes=False)
    return result


def validate_output(output):
    """Reopen every published row; decode JPEGs and exercise real HFImage loading."""
    output=Path(output).resolve()
    report=json.loads((output/'reports/reference-pairs-build.json').read_text())
    report['status']='built_pending_validation';atomic_json(output/'reports/reference-pairs-build.json',report)
    proof=output/'reports/reference-pairs-validation.json';proof.unlink(missing_ok=True)
    summary_path=output/'metadata/reference-pairs-summary.json'
    summary=json.loads(summary_path.read_text()) if summary_path.exists() else {}
    summary.update(status='built_pending_validation',validation_passed=False)
    atomic_json(summary_path,summary)
    config=json.loads((output/'private/processing-config.json').read_text())
    if hashlib.sha256(canonical(config['config']).encode()).hexdigest()!=config['sha256']: raise ValueError('Frozen processing configuration digest differs')
    public_file_evidence={}
    evidence_path=output/'metadata/reference-pairs-source-files.jsonl.gz'
    if config['config'].get('requires_source_vrt_holder_permission_assertion'):
        with gzip.open(evidence_path,'rt',encoding='utf-8') as f:
            for line in f:
                value=json.loads(line);public_file_evidence[value['scan_filename'].replace('_',' ')]=value
    with closing(sqlite3.connect(f'file:{output/"private/state.sqlite"}?mode=ro',uri=True)) as source:
        pixels=set();identities=set();count=0;quality=Counter();licenses=Counter()
        for shard in report['data_files']:
            path=(output/shard['path']).resolve()
            if not path.is_relative_to(output) or sha(path)!=shard['sha256'] or path.stat().st_size!=shard['bytes']: raise ValueError('Reference shard hash/size/path mismatch')
            actual_schema=pq.read_schema(path)
            # Parquet canonicalizes list-child labels from item to element; compare
            # semantic Arrow fields plus the complete HF feature metadata separately.
            if not actual_schema.equals(SCHEMA) or actual_schema.metadata!=SCHEMA.metadata: raise ValueError('Reference Arrow/HFImage schema differs')
            observed=0
            for batch in pq.ParquetFile(path).iter_batches(batch_size=16):
                for row in batch.to_pylist():
                    if selection_reason(row,config['config']['max_pdf_bytes']): raise ValueError('Released reference row fails frozen selection')
                    original=source.execute('SELECT payload FROM candidates WHERE id=?',(row['record_id'],)).fetchone()
                    if not original: raise ValueError('Reference source record is not in frozen candidate inventory')
                    original=json.loads(zlib.decompress(original[0]))
                    for key in COMMUNITY_SCHEMA.names:
                        if key=='contains_image_bytes': continue
                        if row[key]!=original[key]: raise ValueError('Community source field changed: '+key)
                    if row['reference_text']!=row['text'] or row['source_pdf_page_index']!=row['scan_page_number']-1: raise ValueError('Reference text/page association differs')
                    identity=hashlib.sha256(canonical([row['record_id'],row['scan_file_sha1'],row['source_pdf_page_index'],PROFILE]).encode()).hexdigest()
                    if row['page_id']!=identity or row['reference_pair_id']!=identity or identity in identities: raise ValueError('Reference pair identity differs or repeats')
                    if row['processing_config_sha256']!=config['sha256']: raise ValueError('Reference processing config differs')
                    if row['source_pdf_sha1']!=row['scan_file_sha1'] or not re.fullmatch('[0-9a-f]{64}',row['source_pdf_sha256']): raise ValueError('Missing PDF version binding')
                    download=json.loads((output/'private/download-proofs'/f'{row["source_pdf_sha1"]}.json').read_text())
                    if any(download[k]!=row[field] for k,field in [('sha1','source_pdf_sha1'),('sha256','source_pdf_sha256'),('bytes','scan_file_size_bytes')]): raise ValueError('PDF version differs from verified original download')
                    if row['source_pdf_download_url']!=download_url(row['scan_file_url']) or row['source_pdf_page_count']!=row['scan_file_page_count']: raise ValueError('PDF URL/page-count binding differs')
                    if row['document_id']!=row['file_group_id'] or row['split']!='train' or row['contains_image_bytes'] is not True: raise ValueError('Pair document/split/image semantics differ')
                    if config['config'].get('requires_source_vrt_holder_permission_assertion'):
                        evidence=public_file_evidence.get(row['scan_filename'].replace('_',' '),{})
                        if not vrt_permission(evidence) or evidence.get('image_redistribution_held') or evidence.get('source_file_info',{}).get('sha1')!=row['scan_file_sha1']: raise ValueError('Missing/conflicting frozen source holder-permission evidence')
                        notice=json.loads((output/'private/native-notice-proofs'/f'{row["source_pdf_sha1"]}.json').read_text())
                        if notice['restrictive_cc_notice_found'] or notice['source_pdf_sha1']!=row['source_pdf_sha1'] or notice['source_pdf_sha256']!=row['source_pdf_sha256']: raise ValueError('Native PDF notice proof conflicts or differs')
                    if row['image_version_correspondence_independently_verified'] or row['bayanat_human_verified'] or row['has_human_line_or_region_boxes']: raise ValueError('Unsupported gold/correspondence/geometry claim')
                    actual=image_fingerprints(row['image']['bytes'])
                    if actual['format']!='JPEG': raise ValueError('Reference raster codec differs from JPEG profile')
                    if any(actual[key]!=row[key] for key in ('image_sha256','pixel_sha256','width','height')): raise ValueError('Published reference raster differs from row')
                    if row['pixel_sha256'] in pixels: raise ValueError('Duplicate decoded raster in reference release')
                    pixels.add(row['pixel_sha256']);identities.add(identity);quality[str(row['source_quality_level'])]+=1;licenses[row['scan_license_short_name']]+=1
                    count+=1;observed+=1
            if observed!=shard['rows']: raise ValueError('Reference shard count differs')
            first=next(iter(load_dataset('parquet',data_files=str(path),split='train',streaming=True)))
            if not isinstance(first['image'],Image.Image) or first['image'].size!=(first['width'],first['height']): raise ValueError('Actual Hugging Face Image feature did not decode to PIL')
        if count!=report['rows']: raise ValueError('Reference total count differs')
        validated={'status':'passed','rows_checked':count,'unique_decoded_rasters':len(pixels),'source_quality_counts':dict(quality),
                   'source_file_license_counts':dict(licenses),'hfimage_pil_files_checked':len(report['data_files']),
                   'validator_sha256':sha(__file__),'processing_config_sha256':config['sha256'],
                   'scope':'All serialized source/text/version fields, exact raster hashes/dimensions, unique IDs/pixels and actual HFImage→PIL decoding; not independently proofread or visually aligned ground truth.'}
        atomic_json(proof,validated)
        report['status']='passed';atomic_json(output/'reports/reference-pairs-build.json',report)
        summary.update(status='passed',validation_passed=True);atomic_json(summary_path,summary)
        return validated


class Client:
    def __init__(self, output, delay=1.0):
        self.output, self.delay, self.last = output, max(0.5, delay), 0.0
        self.session = requests.Session(); self.session.headers['User-Agent'] = USER_AGENT
        self.robots = None

    def gap(self):
        time.sleep(max(0, self.last + self.delay - time.monotonic())); self.last = time.monotonic()

    def check_robots(self, url):
        if self.robots is None:
            self.gap(); r = self.session.get('https://upload.wikimedia.org/robots.txt', timeout=(15, 45))
            if len(r.content) > 1024**2: raise ValueError('Oversized robots response')
            atomic_json(self.output / 'private/robots.json', {'url': r.url, 'status': r.status_code, 'text': r.text, 'sha256': hashlib.sha256(r.content).hexdigest()})
            parser = RobotFileParser()
            if r.status_code == 200: parser.parse(r.text.splitlines())
            elif r.status_code in (404, 410): parser.parse([])
            else: raise ValueError('Cannot establish source robots policy')
            self.robots = parser
            self.delay = max(self.delay, parser.crawl_delay(USER_AGENT) or parser.crawl_delay('*') or 0)
        if not self.robots.can_fetch(USER_AGENT, url): raise PermissionError('Source robots disallows PDF')

    def fetch(self, row, cache, cap):
        url = download_url(row['scan_file_url']); self.check_robots(url)
        path = cache / (row['scan_file_sha1'] + '.pdf')
        if path.is_symlink(): raise ValueError('Owned PDF cache path must not be a symlink')
        if path.exists():
            digest=verify_pdf(path,row)
            proof=self.output/'private/download-proofs'/f'{row["scan_file_sha1"]}.json'
            if not proof.exists(): atomic_json(proof,{'source_url_raw':row['scan_file_url'],'download_url':url,'sha1':row['scan_file_sha1'],
                'sha256':digest,'bytes':path.stat().st_size,'proof_basis':'cache_reverified_against_frozen_source_sha1_not_fresh_http_retrieval',
                'verified_at':datetime.now(timezone.utc).isoformat()})
            return path
        temporary = path.with_suffix('.pdf.part')
        for other in cache.iterdir():
            if other==temporary: continue
            if other.is_file() and other.name.endswith(('.pdf', '.part')): raise ValueError('One-file cache contains another input; resume previous file first')
        # A partial download is not reusable evidence. Only this exact owned path
        # may be replaced; verified completed-file cache cleanup happens in build.
        if temporary.exists(): temporary.unlink()
        for attempt in range(4):
            if shutil.disk_usage(cache).free < MIN_FREE + cap: raise RuntimeError('FourGiB free-space floor would be violated')
            self.gap()
            with self.session.get(url, stream=True, timeout=(15, 90), allow_redirects=False) as response:
                if response.status_code in (401, 403): raise PermissionError('Source access denied; not retried')
                if response.status_code in (429, 500, 502, 503, 504):
                    try: pause = float(response.headers.get('Retry-After', 2 ** (attempt + 1)))
                    except ValueError: pause = 30
                    if pause > 120: raise RuntimeError('Long source Retry-After; resume later')
                    time.sleep(max(1, pause)); continue
                response.raise_for_status()
                if response.is_redirect: raise ValueError('Unexpected PDF redirect; review source URL')
                size = 0
                with temporary.open('wb') as f:
                    for block in response.iter_content(256 * 1024):
                        size += len(block)
                        if size > cap or size > row['scan_file_size_bytes']: raise ValueError('Source PDF exceeds byte cap/frozen size')
                        f.write(block)
                    f.flush(); os.fsync(f.fileno())
                verify_pdf(temporary, row); temporary.replace(path)
                atomic_json(self.output / 'private/download-proofs' / (row['scan_file_sha1'] + '.json'), {
                    'source_url_raw': row['scan_file_url'], 'download_url': url, 'sha1': row['scan_file_sha1'],
                    'sha256': sha(path), 'bytes': size, 'retrieved_at': datetime.now(timezone.utc).isoformat(),
                    'response_headers': {k: response.headers.get(k) for k in ('ETag', 'Last-Modified', 'Content-Type', 'Content-Length')}})
                return path
        raise RuntimeError('Source retries exhausted')


def build(args):
    output = Path(args.output).resolve(); output.mkdir(parents=True, exist_ok=True)
    private = output / 'private'; private.mkdir(exist_ok=True)
    # Invalidate prior success before any authorized extension can fail.
    (output/'reports/reference-pairs-validation.json').unlink(missing_ok=True)
    old_summary=output/'metadata/reference-pairs-summary.json'
    if old_summary.exists():
        value=json.loads(old_summary.read_text());value.update(status='build_in_progress',validation_passed=False);atomic_json(old_summary,value)
    old_report=output/'reports/reference-pairs-build.json'
    if old_report.exists():
        value=json.loads(old_report.read_text());value['status']='build_in_progress';atomic_json(old_report,value)
    cache = private / 'pdf-cache'; cache.mkdir(exist_ok=True)
    source = Path(args.community).resolve()
    report = json.loads((source / 'reports/community-text-build.json').read_text())
    if report.get('status') != 'passed' or report.get('contains_image_bytes') is not False: raise ValueError('Passing text-only community package required')
    validation_path=source/'reports/community-text-validation.json'
    if json.loads(validation_path.read_text()).get('status')!='passed': raise ValueError('Independent all-row community validation required')
    inputs = report['data_files']
    metadata_paths=['metadata/wikisource/scan_files.jsonl.gz','metadata/wikisource/scan_file_descriptions.jsonl.gz']
    evidence=source_file_evidence(source)
    for entry in inputs:
        path = (source / entry['path']).resolve()
        if not path.is_relative_to(source) or sha(path) != entry['sha256']: raise ValueError('Community shard hash/path mismatch')
    config = {'profile': PROFILE, 'builder_sha256': sha(__file__), 'community_report_sha256': sha(source / 'reports/community-text-build.json'),
              'community_validation_sha256':sha(validation_path),
              'source_rights_metadata':{p:sha(source/p) for p in metadata_paths},
              'input_shards': inputs, 'max_pdf_bytes': args.max_pdf_bytes, 'max_pages_per_file': args.max_pages_per_file,
              'pdfium_version': str(pdfium.PDFIUM_INFO), 'pypdfium2_version': str(pdfium.PYPDFIUM_INFO),
              'pillow_version': Image.__version__, 'pyarrow_version': pa.__version__,
              'pairing_is_not_independently_verified': True, 'split': 'train', 'known_aco_linked_files_excluded': True,
              'postdated_or_unknown_scan_versions_excluded': True, 'explicit_cc_file_license_only':True,
              'requires_source_vrt_holder_permission_assertion':True,'native_rights_notice_screen_first_pages':12}
    config['retain_verified_originals_privately']=True
    config_hash = hashlib.sha256(canonical(config).encode()).hexdigest()
    prior = private / 'processing-config.json'
    if prior.exists() and json.loads(prior.read_text()) != {'sha256': config_hash, 'config': config}: raise ValueError('Frozen reference build inputs/config changed')
    atomic_json(prior, {'sha256': config_hash, 'config': config})
    with closing(sqlite3.connect(private / 'state.sqlite')) as db:
        db.row_factory = sqlite3.Row
        db.executescript('''CREATE TABLE IF NOT EXISTS candidates(id TEXT PRIMARY KEY,file_sha1 TEXT,size INTEGER,quality INTEGER,page INTEGER,payload BLOB);
            CREATE TABLE IF NOT EXISTS emitted(id TEXT PRIMARY KEY,pixel_sha256 TEXT UNIQUE,shard TEXT,sha256 TEXT,bytes INTEGER);
            CREATE TABLE IF NOT EXISTS completed_files(sha1 TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY,payload TEXT);
            CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT);
            CREATE TABLE IF NOT EXISTS blocked_files(sha1 TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS file_holds(sha1 TEXT PRIMARY KEY,reason TEXT NOT NULL);''')
        for entry in db.execute('SELECT DISTINCT shard,sha256,bytes FROM emitted'):
            p = output / entry['shard']
            if not p.exists() or p.stat().st_size != entry['bytes'] or sha(p) != entry['sha256']: raise ValueError('Committed reference shard missing or corrupt')
        for cached in cache.glob('*.pdf'):
            if db.execute('SELECT 1 FROM completed_files WHERE sha1=?',(cached.stem,)).fetchone():
                proof=private/'download-proofs'/f'{cached.stem}.json'
                if cached.is_symlink() or not proof.exists() or sha(cached)!=json.loads(proof.read_text())['sha256']: raise ValueError('Completed cache input differs from verified download')
                retain_verified_input(cached,private,json.loads(proof.read_text())['sha256'])
        exclusions = Counter()
        if not db.execute("SELECT 1 FROM settings WHERE key='inventory_complete'").fetchone():
            db.execute('DELETE FROM candidates'); db.commit()
            for entry in inputs:
                for batch in pq.ParquetFile(source / entry['path']).iter_batches(batch_size=256):
                    for row in batch.to_pylist():
                        if row.get('source_linked_aco_book_ids'):
                            db.execute('INSERT OR IGNORE INTO blocked_files VALUES(?)',(row.get('scan_file_sha1'),))
                        reason = selection_reason(row, args.max_pdf_bytes)
                        file_evidence=evidence.get(row['scan_filename'].replace('_',' '),{})
                        if not reason and file_evidence.get('image_redistribution_held'): reason='source_file_image_redistribution_held'
                        if not reason and not vrt_permission(file_evidence): reason='pilot_requires_source_vrt_holder_permission_assertion'
                        if reason: exclusions[reason] += 1; continue
                        db.execute('INSERT INTO candidates VALUES(?,?,?,?,?,?)', (row['record_id'],row['scan_file_sha1'],row['scan_file_size_bytes'],row['source_quality_level'],row['scan_page_number'],zlib.compress(canonical(row).encode())))
            blocked=db.execute('SELECT count(*) FROM candidates WHERE file_sha1 IN (SELECT sha1 FROM blocked_files)').fetchone()[0]
            exclusions['known_aco_linked_binary_from_other_record']+=blocked
            db.execute('DELETE FROM candidates WHERE file_sha1 IN (SELECT sha1 FROM blocked_files)')
            db.execute("INSERT INTO settings VALUES('inventory_exclusions',?)", (canonical(exclusions),))
            db.execute("INSERT INTO settings VALUES('inventory_complete','true')"); db.commit()
        else: exclusions.update(json.loads(db.execute("SELECT value FROM settings WHERE key='inventory_exclusions'").fetchone()[0]))
        client = Client(output)
        files = db.execute('SELECT file_sha1,min(size) size,max(quality) quality FROM candidates WHERE file_sha1 NOT IN (SELECT sha1 FROM completed_files) AND file_sha1 NOT IN (SELECT sha1 FROM file_holds) GROUP BY file_sha1 ORDER BY quality DESC,size,file_sha1').fetchall()
        for file in files:
            emitted = db.execute('SELECT count(*) FROM emitted').fetchone()[0]
            if emitted >= args.max_pages: break
            if shutil.disk_usage(output).free < MIN_FREE + args.max_pdf_bytes: raise RuntimeError('FourGiB free-space floor reached')
            candidates = db.execute('SELECT * FROM candidates WHERE file_sha1=? ORDER BY quality DESC,page,id', (file['file_sha1'],)).fetchall()
            selected = candidates[:args.max_pages_per_file] if args.max_pages_per_file else candidates
            selected = selected[:args.max_pages-emitted]
            row = json.loads(zlib.decompress(selected[0]['payload']))
            first = row
            path = client.fetch(row, cache, args.max_pdf_bytes); pdf_sha256 = verify_pdf(path, row)
            notice=audit_native_rights_notices(path)
            atomic_json(private/'native-notice-proofs'/f'{file["file_sha1"]}.json',{'source_pdf_sha1':file['file_sha1'],'source_pdf_sha256':pdf_sha256,**notice})
            if notice['restrictive_cc_notice_found']:
                with db: db.execute('INSERT INTO file_holds VALUES(?,?)',(file['file_sha1'],'native_pdf_restrictive_cc_notice_requires_review'))
                path.unlink();continue
            pending = []
            for candidate in selected:
                row = json.loads(zlib.decompress(candidate['payload']))
                if any(row[k]!=first[k] for k in ('scan_file_sha1','scan_file_size_bytes','scan_file_page_count','scan_mime')): raise ValueError('Conflicting file metadata')
                pair = make_pair(row, path, pdf_sha256, config_hash)
                prior_pixel = db.execute('SELECT id FROM emitted WHERE pixel_sha256=?', (pair['pixel_sha256'],)).fetchone()
                within = next((p for p in pending if p['pixel_sha256'] == pair['pixel_sha256']), None)
                if prior_pixel or within:
                    event = {'source_record_id': row['record_id'], 'reason': 'exact_decoded_raster_duplicate', 'retained_pair_id': prior_pixel['id'] if prior_pixel else within['page_id']}
                    db.execute('INSERT OR REPLACE INTO events VALUES(?,?)', (row['record_id'],canonical(event))); continue
                pending.append(pair)
            if pending:
                relative = f'data/reference_pairs/{file["file_sha1"]}.parquet'; destination = output / relative; destination.parent.mkdir(parents=True, exist_ok=True)
                temporary = destination.with_suffix('.parquet.tmp')
                pq.write_table(pa.Table.from_pylist(pending, schema=SCHEMA), temporary, compression='zstd', row_group_size=16)
                temporary.replace(destination); digest = sha(destination); size = destination.stat().st_size
                with db:
                    for pair in pending: db.execute('INSERT INTO emitted VALUES(?,?,?,?,?)', (pair['page_id'],pair['pixel_sha256'],relative,digest,size))
                    db.execute('INSERT INTO completed_files VALUES(?)', (file['file_sha1'],))
            else:
                with db: db.execute('INSERT INTO completed_files VALUES(?)', (file['file_sha1'],))
            # Preserve the two verified originals privately through root packaging.
            if path.parent != cache or path.is_symlink() or sha(path) != pdf_sha256: raise ValueError('Input cache eviction verification failed')
            retain_verified_input(path,private,pdf_sha256)
            print(canonical({'stage':'reference_file_complete','sha1':file['file_sha1'],'retained_pairs':len(pending)}), flush=True)
        shards = [dict(r) for r in db.execute('SELECT shard path,sha256,bytes,count(*) rows FROM emitted GROUP BY shard,sha256,bytes ORDER BY shard')]
        public = {'status':'built_pending_validation','config':'reference_pairs','split':'train','rows':sum(s['rows'] for s in shards),'source_pdf_files':len(shards),
                  'contains_image_bytes':True,'has_ocr_or_word_boxes':False,'is_gold_or_heldout_benchmark':False,
                  'input_selection_exclusions':dict(exclusions),'eligible_source_rows':db.execute('SELECT count(*) FROM candidates').fetchone()[0],
                  'exact_pixel_duplicate_reference_occurrences':db.execute('SELECT count(*) FROM events').fetchone()[0],
                  'runtime_pdf_rights_holds':[dict(r) for r in db.execute('SELECT * FROM file_holds')],
                  'processing_config_sha256':config_hash,'data_files':shards,'pilot_max_pages':args.max_pages,
                  'pairing_scope':'Source-associated PDF page, frozen SHA1 and page position; temporal filter is not independently verified visual/text alignment.',
                  'reference_scope':'Rendered transcluded page body; non-transcluded headers/footers excluded. Source community labels are not Bayanat gold certification.',
                  'image_rights_scope':'Per-file source rights/license/credit retained separately from CC-BY-SA community text; no blanket image license.',
                  'scan_type_scope':'PDF page renders; not every source PDF is independently verified as a physical scan.'}
        atomic_json(output / 'reports/reference-pairs-build.json', public)
        atomic_json(output / 'metadata/reference-pairs-processing-config.json', {'sha256':config_hash,'config':config})
        summary={**public,'source_file_documents':public['source_pdf_files'],'parquet_bytes':sum(s['bytes'] for s in shards),
                 'profile':PROFILE,'validation_passed':False,'upload_sha256_verified':False,'selection_exclusions':dict(exclusions)}
        atomic_json(output/'metadata/reference-pairs-summary.json',summary)
        index=output/'metadata/reference-pairs-index.jsonl.gz';temporary=index.with_suffix('.gz.tmp')
        with gzip.open(temporary,'wt',encoding='utf-8') as f:
            columns=['page_id','reference_pair_id','pixel_sha256','image_sha256','document_id','source_pdf_sha1','source_pdf_sha256']
            for shard in shards:
                for batch in pq.ParquetFile(output/shard['path']).iter_batches(batch_size=256,columns=columns):
                    for row in batch.to_pylist():
                        row['source_file_sha1']=row['source_pdf_sha1'];row['shard_path']=shard['path'];f.write(canonical(row)+'\n')
        temporary.replace(index)
        selected_names=set()
        for shard in shards:
            for batch in pq.ParquetFile(output/shard['path']).iter_batches(batch_size=256,columns=['scan_filename']):
                selected_names.update(n.replace('_',' ') for n in batch.column(0).to_pylist())
        with gzip.open(output/'metadata/reference-pairs-source-files.jsonl.gz','wt',encoding='utf-8') as f:
            for name in sorted(selected_names):f.write(canonical(evidence[name])+'\n')
        with gzip.open(source/'metadata/wikisource/scan_file_descriptions.jsonl.gz','rt',encoding='utf-8') as src,gzip.open(output/'metadata/reference-pairs-source-descriptions.jsonl.gz','wt',encoding='utf-8') as dest:
            for line in src:
                row=json.loads(line)
                if row['page']['title'].split(':',1)[-1].replace('_',' ') in selected_names:dest.write(canonical(row)+'\n')
        return public


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--community', required=True); parser.add_argument('--output', required=True)
    parser.add_argument('--max-pages', type=int, default=12)
    parser.add_argument('--max-pages-per-file', type=int, default=4)
    parser.add_argument('--max-pdf-bytes', type=int, default=64 * 1024**2)
    parser.add_argument('--validate-only',action='store_true')
    args = parser.parse_args()
    if args.max_pages < 1 or not 1<=args.max_pages_per_file<=512 or not 1 <= args.max_pdf_bytes <= 256*1024**2: parser.error('Invalid bounds; per-file page cap must be1–512 to bound memory')
    if args.validate_only: print(json.dumps(validate_output(args.output),ensure_ascii=False,indent=2))
    else:
        build(args);validate_output(args.output)
        print((Path(args.output)/'metadata/reference-pairs-summary.json').read_text())
