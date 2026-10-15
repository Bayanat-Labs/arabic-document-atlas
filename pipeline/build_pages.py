"""Bounded, resumable page-image/OCR processing from the ACO inventory.

Source PDFs are temporary inputs, never silently replaced on resume. Completed
Parquet shards are verified against Hub LFS SHA-256 before optional local eviction.
No source files, user caches or earlier Bayanat releases are removed.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import io
import json
import math
import importlib.metadata
import platform
import selectors
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unicodedata
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
import pypdfium2 as pdfium
import pypdfium2_raw
import requests
from PIL import Image

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))
from quality.validate import canonical_ids, text_metrics, validate_page, image_fingerprints, normalized_text_hash, provenance_issues

REPO = 'Bayanat-Labs/arabic-document-atlas'
USER_AGENT = 'BayanatDocumentAtlas/0.1 (+https://www.bayanatlabs.com; hello@bayanatlabs.com)'
PROFILE = 'aco-high-pdf-pdfium-longedge2400-jpeg90-vision-r3-v1'
SCHEMA_VERSION = 'arabic-document-atlas-pages-v2'
RENDER_LOCK = threading.Lock()  # PDFium is not thread safe; native OCR remains parallel.
NATIVE_TIMEOUT_SECONDS = 60
MAX_NATIVE_RESPONSE_BYTES = 16 * 1024**2


def page_schema():
    """One explicit schema for blank and text pages, including nullable references."""
    st, i32, i64, f64, boolean = pa.string(), pa.int32(), pa.int64(), pa.float64(), pa.bool_()
    bbox = pa.list_(i32, 4)
    word = pa.struct([pa.field(k, t, nullable=False) for k, t in [
        ('id', st), ('text', st), ('bbox', bbox), ('bbox_method', st),
        ('bbox_boundary_epsilon_clipped', boolean), ('confidence', f64),
        ('confidence_basis', st), ('line_id', st)]])
    line = pa.struct([pa.field(k, t, nullable=False) for k, t in [
        ('id', st), ('text', st), ('bbox', bbox), ('word_ids', pa.list_(st)),
        ('order_index', i32), ('order_method', st), ('confidence', f64),
        ('bbox_method', st), ('engine_line_text', st)]])
    strings = '''page_id logical_page_id document_id work_group_id source_collection source_document_id split image_request_profile image_sha256 pixel_sha256 dhash64 format source_url source_manifest_url source_manifest_sha256 source_canvas_id source_page_label source_image_url source_pdf_url source_pdf_sha256 canvas_pdf_mapping source_rights source_rights_url source_rights_basis source_publication_date source_title_ar source_title_en source_provider page_text text_source annotation_review confidence_basis page_status ocr_engine ocr_operating_system bbox_accuracy renderer renderer_version processing_config_sha256'''.split()
    fields = [pa.field(k, st, nullable=False) for k in strings]
    fields += [pa.field('image', pa.struct([pa.field('bytes', pa.binary()), pa.field('path', st)]))]
    fields += [pa.field(k, i32, nullable=False) for k in 'width height source_page_index source_image_width source_image_height source_pdf_page_index source_pdf_page_count words_count lines_count ocr_engine_revision'.split()]
    fields += [pa.field(k, st) for k in ('reference_text', 'proofread_status')]
    fields += [pa.field('words', pa.list_(word), nullable=False), pa.field('lines', pa.list_(line), nullable=False)]
    fields += [pa.field('mean_word_confidence', f64), pa.field('arabic_char_fraction', f64, nullable=False), pa.field('ocr_seconds', f64, nullable=False)]
    fields += [pa.field('ocr_recognition_languages', pa.list_(st), nullable=False), pa.field('ocr_uses_language_correction', boolean, nullable=False), pa.field('curated_eligible', boolean, nullable=False)]
    fields += [pa.field(k, pa.list_(st), nullable=False) for k in ('curated_exclusion_reasons', 'quality_warning_codes')]
    return pa.schema(fields, metadata={b'arrow_schema_version': SCHEMA_VERSION.encode()})


def duplicate_schema():
    names = '''page_id logical_page_id document_id source_collection source_document_id split image_request_profile image_sha256 pixel_sha256 canvas_pdf_mapping renderer renderer_version source_url source_manifest_url source_manifest_sha256 source_canvas_id source_page_label source_image_url source_pdf_url source_pdf_sha256 source_rights source_rights_url source_rights_basis source_publication_date source_title_ar source_title_en source_provider processing_config_sha256'''.split()
    fields = [page_schema().field(k) for k in names]
    fields += [page_schema().field(k) for k in ('source_page_index', 'source_image_width', 'source_image_height', 'source_pdf_page_index', 'source_pdf_page_count', 'width', 'height')]
    fields += [pa.field(k, pa.string(), nullable=False) for k in ('retained_page_id', 'retained_logical_page_id', 'retained_document_id', 'retained_image_sha256', 'duplicate_method')]
    return pa.schema(fields, metadata={b'arrow_schema_version': SCHEMA_VERSION.encode()})


def renderer_library_path():
    directory=Path(pypdfium2_raw.__file__).parent
    matches=[directory/name for name in ('libpdfium.dylib','libpdfium.so','pdfium.dll') if (directory/name).is_file()]
    if len(matches)!=1:raise ValueError('Expected exactly one installed PDFium platform library')
    return matches[0]


def freeze_processing_config(args, output):
    """Reject resume if source, executable, policy or semantic rendering changed."""
    binary = Path(args.vision_binary).resolve()
    source = PROJECT/'pipeline/vision_ocr.swift'
    config = {
        'schema_version': SCHEMA_VERSION, 'profile': PROFILE, 'split': 'train',
        'split_claim': 'training_only_no_held_out_benchmark',
        'builder_sha256': sha_file(__file__), 'validator_sha256': sha_file(PROJECT/'quality/validate.py'),
        'native_binary_sha256': sha_file(binary), 'native_source_sha256': sha_file(source),
        'native_timeout_seconds': NATIVE_TIMEOUT_SECONDS, 'renderer': 'pypdfium2',
        'renderer_binary_sha256': sha_file(renderer_library_path()),
        'pdfium_version': str(pdfium.PDFIUM_INFO),
        'raster_long_edge': 2400, 'jpeg_quality': 90, 'raster_colorspace': 'RGB',
        'packages': {k: importlib.metadata.version(k) for k in ('pypdfium2','Pillow','pyarrow','datasets')},
        'operating_system': platform.platform(), 'python_version':sys.version,
        'unicode_database_version':unicodedata.unidata_version,
        'exact_duplicate_policy': 'first_successful_page_per_canonical_decoded_RGBA_sha256',
        'quality_policy': 'mechanical_screen_not_transcription_accuracy',
    }
    encoded = json.dumps(config, sort_keys=True, separators=(',',':'), ensure_ascii=False).encode()
    digest = hashlib.sha256(encoded).hexdigest()
    path = output/'private/processing-config.json'
    if path.exists():
        previous = json.loads(path.read_text())
        if previous.get('processing_config_sha256') != digest or previous.get('config') != config:
            raise ValueError('Frozen processing configuration differs; use a new output directory or restore the exact inputs/code/binary')
    else:
        # Never silently adopt outputs produced before source/config binding existed.
        if (output/'private/build.sqlite').exists():
            raise ValueError('Existing build has no frozen processing configuration; explicit migration is required')
        atomic_json(path, {'processing_config_sha256': digest, 'config': config})
    inventory_path = output/'private/input-inventories.json'
    inventories = json.loads(inventory_path.read_text()) if inventory_path.exists() else []
    inventory_sha = sha_file(args.books)
    if not any(x['sha256']==inventory_sha for x in inventories):
        inventories.append({'sha256':inventory_sha,'bytes':Path(args.books).stat().st_size,
                            'filename':Path(args.books).name,'observed_utc':time.strftime('%FT%TZ',time.gmtime())})
        atomic_json(inventory_path,inventories)
    return digest


def pdf_page_count(path):
    with RENDER_LOCK:
        with pdfium.PdfDocument(str(path)) as pdf:
            return len(pdf)


def render_pdf_page(path, index):
    with RENDER_LOCK:
        with pdfium.PdfDocument(str(path)) as pdf:
            page = pdf[index]
            try:
                width, height = page.get_size()
                if not all(math.isfinite(x) and x > 0 for x in (width, height)):
                    raise ValueError('Invalid PDF page geometry')
                bitmap = page.render(scale=2400/max(width,height))
                try:
                    image = bitmap.to_pil().convert('RGB')
                    try:
                        out = io.BytesIO(); image.save(out, format='JPEG', quality=90)
                        return out.getvalue()
                    finally: image.close()
                finally: bitmap.close()
            finally: page.close()

def sha_file(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda:f.read(8*1024*1024),b''):h.update(b)
    return h.hexdigest()

def atomic_json(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_name(path.name+'.tmp');temp.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n');temp.replace(path)

def jsonlines(path):
    with gzip.open(path,'rt',encoding='utf-8') as f:
        for line in f:
            if line.strip():yield json.loads(line)

def check_disk(output,minimum=4*1024**3):
    free=shutil.disk_usage(output).free
    if free<minimum:raise RuntimeError(f'Local storage floor reached: {free} bytes free; required {minimum}')
    return free

def select_pdf(book):
    # Retain the publisher URL in provenance; upgrade to HTTPS for transport.
    renderings=book.get('renderings',[])
    for rendering in renderings:
        raw=rendering.get('id','')
        if raw.endswith('_hi.pdf') and urlsplit(raw).hostname=='mc.dlib.nyu.edu':
            return raw.replace('http://','https://',1)
    raise ValueError('No allowed high-resolution publisher PDF rendering')

def eligible(book):
    return (book.get('manifest_state')=='complete'
            and book.get('institution_asserts_public_domain') is True
            and book.get('eligibility_proposal')=='candidate_institution_asserted_pd_pre1931'
            and 'Arabic' in book.get('language',[]))

def download_pdf(book,output,db):
    doc=book['source_book_id'];url=select_pdf(book);directory=output/'private/pdf-cache';directory.mkdir(parents=True,exist_ok=True)
    path=directory/f'{doc}.pdf';previous=db.execute('SELECT pdf_sha256 FROM documents WHERE document_id=?',(doc,)).fetchone()
    if path.exists():
        digest=sha_file(path)
        if previous and previous[0] and previous[0]!=digest:raise ValueError('Cached source PDF changed')
        return path,digest,url
    check_disk(output)
    partial=path.with_suffix('.pdf.part')
    try:
        for attempt in range(4):
            try:
                with requests.get(url,headers={'User-Agent':USER_AGENT},stream=True,timeout=(20,90)) as r:
                    r.raise_for_status()
                    if urlsplit(r.url).hostname not in {'mc.dlib.nyu.edu','sites.dlib.nyu.edu'}:raise ValueError('Unexpected source PDF redirect host')
                    length=int(r.headers.get('Content-Length','0'))
                    if length>700*1024**2:raise ValueError('Source PDF exceeds bounded 700MiB input limit')
                    h=hashlib.sha256();size=0
                    with partial.open('wb') as f:
                        for chunk in r.iter_content(1024*1024):
                            if not chunk:continue
                            size+=len(chunk)
                            if size>700*1024**2:raise ValueError('Source PDF exceeds bounded input limit')
                            if size%(16*1024**2)<1024*1024:check_disk(output)
                            h.update(chunk);f.write(chunk)
                    if length and length!=size:raise ValueError('Incomplete PDF response')
                    digest=h.hexdigest()
                    if previous and previous[0] and previous[0]!=digest:raise ValueError('Upstream source PDF hash differs from prior completed input')
                    with partial.open('rb') as f:
                        if f.read(5)!=b'%PDF-':raise ValueError('Input does not have PDF signature')
                    partial.replace(path);return path,digest,url
            except (requests.RequestException,OSError):
                if attempt==3:raise
                time.sleep(min(30,2**attempt))
    finally:
        if partial.exists():partial.unlink()

class VisionWorker:
    """Persistent per-thread native process, killed/restarted on bounded timeout.

    Read binary pipe chunks under a monotonic deadline: text readline could block
    forever after a partial line even when select said some data was available.
    """
    def __init__(self, binary, timeout_seconds=NATIVE_TIMEOUT_SECONDS):
        self.binary = binary; self.timeout_seconds = timeout_seconds
        self.local = threading.local(); self.processes = []; self.lock = threading.Lock()

    def _stop(self, process):
        if process.poll() is None:
            process.terminate()
            try: process.wait(timeout=1)
            except subprocess.TimeoutExpired: process.kill(); process.wait(timeout=2)
        for pipe in (process.stdin, process.stdout):
            if pipe: pipe.close()

    def run(self, image_path):
        p = getattr(self.local, 'process', None)
        if p is None or p.poll() is not None:
            p = subprocess.Popen([str(self.binary),'--server'], stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0)
            self.local.process = p
            with self.lock: self.processes.append(p)
        deadline = time.monotonic() + self.timeout_seconds
        try:
            request = (json.dumps({'path':str(image_path)})+'\n').encode()
            if len(request) > 16384: raise ValueError('Native request path is too long')
            p.stdin.write(request); p.stdin.flush()
            data = bytearray()
            with selectors.DefaultSelector() as selector:
                selector.register(p.stdout, selectors.EVENT_READ)
                while True:
                    remaining = deadline-time.monotonic()
                    if remaining <= 0 or not selector.select(remaining):
                        raise TimeoutError(f'Native OCR exceeded {self.timeout_seconds:g}s timeout')
                    chunk = os.read(p.stdout.fileno(), 65536)
                    if not chunk: raise ValueError('Native OCR exited before a complete response')
                    data.extend(chunk)
                    if len(data) > MAX_NATIVE_RESPONSE_BYTES: raise ValueError('Native OCR response exceeds bound')
                    if b'\n' in data:
                        line, extra = data.split(b'\n',1)
                        if extra.strip(): raise ValueError('Unexpected extra native response')
                        value = json.loads(line)
                        if not value.get('ok'): raise ValueError('Native OCR failed: '+str(value.get('error')))
                        return value['result']
        except Exception:
            self._stop(p); self.local.process = None
            raise

    def close(self):
        for p in self.processes: self._stop(p)

def clean_ocr(result):
    # Keep text unchanged. Expand the containing line rectangle to encompass its
    # source engine word guides, rather than silently clipping word rectangles.
    words=result['words'];mapping={w['id']:w for w in words}
    for line in result['lines']:
        boxes=[line['bbox']]+[mapping[w]['bbox'] for w in line['word_ids']]
        line['bbox']=[min(b[0] for b in boxes),min(b[1] for b in boxes),max(b[2] for b in boxes),max(b[3] for b in boxes)]
        line['bbox_method']='union_of_vision_line_and_word_guides'
        # Reconstruction preserves every observed word, and records the original
        # engine line string separately when whitespace segmentation differs.
        line['engine_line_text']=line['text']
        line['text']=' '.join(mapping[w]['text'] for w in line['word_ids'])
    result['page_text']='\n'.join(l['text'] for l in result['lines'])
    return result

def preflight_metadata(book):
    """Check every source canvas/statement before downloading/rendering/OCR."""
    common={
        'source_collection':'nyu_aco','source_document_id':book['source_book_id'],
        'document_id':book['source_book_id'],'work_group_id':book['source_book_id'],
        'image_request_profile':PROFILE,'source_url':book['_source_homepage'],
        'source_manifest_url':book['manifest_url'],'source_rights':book['rights_statement_raw'],
        'source_rights_url':'https://aco.dlib.nyu.edu/about',
        'source_rights_basis':'institution_asserted_public_domain_edition_pre1931_not_independent_worldwide_clearance',
        'page_status':'machine_extracted',
    }
    for index,canvas in enumerate(book['_canvases']):
        values={**common,'source_canvas_id':canvas['id'],'source_page_label':canvas['label'],
                'source_image_url':canvas['image_service']}
        issues=provenance_issues(values)
        if any(not isinstance(canvas[k],int) or isinstance(canvas[k],bool) or canvas[k]<=0 for k in ('width','height')):
            issues.append({'code':'invalid_dimension','path':f'canvas[{index}]','message':'Positive integer source dimension required'})
        if issues:raise ValueError('Metadata preflight: '+json.dumps(issues,ensure_ascii=False)[:2500])
    select_pdf(book)


def evict_verified_pdf(path, output, db, docid):
    """Remove only an owned, finished book input after all output is verified.

    Partial books may be re-downloaded later; their source SHA binding survives.
    The builder is the sole owner of this cache. No separate concurrent resume is
    supported. Planned/deleted ledger states distinguish crashes before unlink.
    """
    status=db.execute('SELECT status,pdf_sha256 FROM documents WHERE document_id=?',(docid,)).fetchone()
    if not status or status[0] not in {'complete','partial'}:raise ValueError('Only finished book inputs may be evicted')
    if db.execute('SELECT count(*) FROM shards WHERE uploaded=0').fetchone()[0]:return False
    expected=output/'private/pdf-cache'/f'{docid}.pdf'
    if Path(path)!=expected or path.is_symlink() or path.resolve().parent!=(output/'private/pdf-cache').resolve():
        raise ValueError('Input eviction path is not an owned PDF cache file')
    if not path.exists():return False
    if not path.is_file() or sha_file(path)!=status[1]:raise ValueError('Input eviction source hash mismatch')
    ledger=output/'private/input-evictions.jsonl'
    entry={'document_id':docid,'pdf_sha256':status[1],'bytes':path.stat().st_size,'document_status':status[0]}
    def record(state):
        with ledger.open('a') as stream:
            stream.write(json.dumps({**entry,'state':state,'recorded_utc':time.strftime('%FT%TZ',time.gmtime())})+'\n')
            stream.flush();os.fsync(stream.fileno())
    record('planned');path.unlink();record('deleted')
    return True


def process_page(task,worker,output):
    book,pdfpath,pdfsha,pdfurl,index,expected_pages=task
    docid=book['source_book_id'];source_canvas=book['_canvases'][index];canvas=source_canvas['id']
    logical,pageid=canonical_ids('nyu_aco',docid,canvas,PROFILE)
    image=render_pdf_page(pdfpath,index)
    with tempfile.TemporaryDirectory(prefix='page-',dir=output/'private/scratch') as directory:
        imagepath=Path(directory)/'page.jpg';imagepath.write_bytes(image)
        result=clean_ocr(worker.run(imagepath))
    fingerprints=image_fingerprints(image);metrics=text_metrics(result['page_text'])
    source_image=source_canvas['image_service']
    row={
        'page_id':pageid,'logical_page_id':logical,'document_id':docid,'work_group_id':docid,
        'source_collection':'nyu_aco','source_document_id':docid,'split':'train',
        'image_request_profile':PROFILE,'image':{'bytes':image,'path':pageid+'.jpg'},
        **fingerprints,
        'source_url':book['_source_homepage'],
        'source_manifest_url':book['manifest_url'],'source_manifest_sha256':book['manifest_sha256'],
        'source_canvas_id':canvas,'source_page_label':source_canvas['label'],'source_page_index':index,
        'source_image_url':source_image,'source_image_width':source_canvas['width'],'source_image_height':source_canvas['height'],
        'source_pdf_url':pdfurl,'source_pdf_sha256':pdfsha,'source_pdf_page_index':index,'source_pdf_page_count':expected_pages,
        'canvas_pdf_mapping':'publisher_rendering_order_with_equal_page_count_not_independently_verified_per_page',
        'source_rights':book['rights_statement_raw'],'source_rights_url':'https://aco.dlib.nyu.edu/about',
        'source_rights_basis':'institution_asserted_public_domain_edition_pre1931_not_independent_worldwide_clearance',
        'source_publication_date':json.dumps(book.get('publication_date_raw',[]),ensure_ascii=False),
        'source_title_ar':book.get('title_ar',''),'source_title_en':book.get('title_en',''),
        'source_provider':'; '.join(book.get('provider',[])),
        'page_text':result['page_text'],'text_source':'apple_vision','annotation_review':'machine_generated',
        'reference_text':None,'proofread_status':None,'words':result['words'],'lines':result['lines'],
        'words_count':len(result['words']),'lines_count':len(result['lines']),
        'mean_word_confidence':sum(w['confidence'] for w in result['words'])/len(result['words']) if result['words'] else None,
        'confidence_basis':'line_candidate_score_repeated_per_word_uncalibrated',
        'arabic_char_fraction':metrics['arabic_char_fraction'],'page_status':'machine_extracted',
        'ocr_engine':result['engine'],'ocr_engine_revision':result['engine_revision'],'ocr_operating_system':result['operating_system'],
        'ocr_recognition_languages':result['recognition_languages'],'ocr_uses_language_correction':False,
        'ocr_seconds':result['ocr_seconds'],'bbox_accuracy':'approximate_machine_word_guides_not_pixel_gold',
        'renderer':'pypdfium2','renderer_version':importlib.metadata.version('pypdfium2'),
        'processing_config_sha256':book['_processing_config_sha256'],
    }
    report=validate_page(row,image_bytes=image)
    if not report['valid']:raise ValueError(json.dumps(report['errors'],ensure_ascii=False)[:2500])
    row['curated_eligible']=report['curated_eligible'];row['curated_exclusion_reasons']=report['curated_exclusion_reasons']
    row['quality_warning_codes']=[w['code'] for w in report['warnings']]
    return row

def connect_state(output):
    (output/'private').mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(output/'private/build.sqlite');db.execute('PRAGMA journal_mode=WAL');db.execute('PRAGMA cache_size=-32768')
    db.executescript("""
    CREATE TABLE IF NOT EXISTS documents(document_id TEXT PRIMARY KEY,pdf_sha256 TEXT,pdf_url TEXT,pages INTEGER,status TEXT,error TEXT,
      manifest_sha256 TEXT,manifest_url TEXT,book_record_sha256 TEXT);
    CREATE TABLE IF NOT EXISTS shards(path TEXT PRIMARY KEY,sha256 TEXT,rows INTEGER,bytes INTEGER,uploaded INTEGER DEFAULT 0,commit_sha TEXT,kind TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS pages(page_id TEXT PRIMARY KEY,logical_id TEXT UNIQUE,document_id TEXT,page_index INTEGER,shard_path TEXT,pixel_sha256 TEXT UNIQUE,text_hash TEXT,split TEXT,words INTEGER,curated INTEGER,image_sha256 TEXT);
    CREATE TABLE IF NOT EXISTS occurrences(page_id TEXT PRIMARY KEY,logical_id TEXT UNIQUE,document_id TEXT,page_index INTEGER,retained_page_id TEXT NOT NULL,is_duplicate INTEGER NOT NULL,pixel_sha256 TEXT NOT NULL,lineage_shard TEXT,UNIQUE(document_id,page_index));
    CREATE TABLE IF NOT EXISTS failures(document_id TEXT,page_index INTEGER,error TEXT,recorded_at TEXT);
    CREATE TABLE IF NOT EXISTS batches(batch_id INTEGER PRIMARY KEY);
    """);db.commit();return db


def book_record_hash(book):
    clean={k:v for k,v in book.items() if not k.startswith('_')}
    return hashlib.sha256(json.dumps(clean,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()


def bind_document(db, book, *, pdf_sha256=None, pdf_url=None, pages=None):
    """Bindings are immutable once observed, even after any processing failure."""
    values={'manifest_sha256':book['manifest_sha256'],'manifest_url':book['manifest_url'],
            'book_record_sha256':book_record_hash(book),'pdf_sha256':pdf_sha256,'pdf_url':pdf_url,'pages':pages}
    names=list(values)
    previous=db.execute('SELECT '+','.join(names)+' FROM documents WHERE document_id=?',(book['source_book_id'],)).fetchone()
    if previous:
        for name,old in zip(names,previous):
            if old is not None and values[name] is not None and old!=values[name]:
                raise ValueError(f'Frozen document {name} binding differs for {book["source_book_id"]}')
    db.execute('INSERT INTO documents(document_id,status) VALUES (?,?) ON CONFLICT(document_id) DO NOTHING',(book['source_book_id'],'pending'))
    for name,value in values.items():
        if value is not None: db.execute(f'UPDATE documents SET {name}=COALESCE({name},?) WHERE document_id=?',(value,book['source_book_id']))
    db.commit()


def record_document_error(db, docid, error):
    db.execute('INSERT INTO documents(document_id,status,error) VALUES (?,?,?) ON CONFLICT(document_id) DO UPDATE SET status=excluded.status,error=excluded.error',
               (docid,'error',str(error)[:4000]));db.commit()


def verify_staged_shards(output, db):
    """Fail closed on missing/untracked or corrupted local outputs before resume."""
    registered={r[0]:r[1:] for r in db.execute('SELECT path,sha256,uploaded FROM shards')}
    for path in (output/'data').rglob('*.parquet'):
        relative=str(path.relative_to(output))
        if relative not in registered:
            raise ValueError('Uncommitted Parquet output requires review before resume: '+relative)
    for relative,(digest,uploaded) in registered.items():
        path=output/relative
        if path.exists():
            if sha_file(path)!=digest:raise ValueError('Staged shard SHA-256 mismatch: '+relative)
        elif not uploaded:raise ValueError('Unuploaded staged shard is missing: '+relative)


def write_parquet(rows, schema, path, *, image_feature=False):
    from datasets import Features,Image as HFImage
    # Reject missing/non-schema fields instead of silently changing a release schema.
    expected=set(schema.names)
    for row in rows:
        if set(row)!=expected:raise ValueError('Row/schema fields differ: '+str(sorted(set(row)^expected)))
    table=pa.Table.from_pylist(rows,schema=schema)
    table.validate(full=True)
    if image_feature:
        features=Features.from_arrow_schema(schema);features['image']=HFImage()
        metadata=dict(schema.metadata or {});metadata[b'huggingface']=json.dumps({'info':{'features':features.to_dict()}}).encode()
        table=table.replace_schema_metadata(metadata)
    path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_suffix('.parquet.tmp')
    try:
        pq.write_table(table,temp,compression='zstd',compression_level=3,row_group_size=16)
        temp.replace(path)
    finally: temp.unlink(missing_ok=True)
    return sha_file(path),path.stat().st_size


def stage_shard(rows,output,db):
    """One committed source occurrence per successful index, one image per pixel hash.

    Duplicate decisions include prior batches and earlier rows in this batch.
    Publication shards and DB lineage are committed together; an interrupted file
    write is detected as an uncommitted output on resume rather than overwritten.
    """
    if not rows:return {'retained':0,'duplicates':0}
    check_disk(output)
    number=db.execute('SELECT COALESCE(MAX(batch_id),-1)+1 FROM batches').fetchone()[0]
    retained=[];duplicates=[];occurrences=[];seen={};source_indices=set();source_ids=set()
    for row in rows:
        source_index=(row['document_id'],row['source_pdf_page_index'])
        if source_index in source_indices or row['logical_page_id'] in source_ids:
            raise ValueError('Repeated source occurrence within uncommitted batch')
        source_indices.add(source_index);source_ids.add(row['logical_page_id'])
        if row['split']!='train':raise ValueError('First release is train-only')
        if db.execute('SELECT 1 FROM occurrences WHERE page_id=? OR logical_id=? OR (document_id=? AND page_index=?)',
            (row['page_id'],row['logical_page_id'],row['document_id'],row['source_pdf_page_index'])).fetchone():
            raise ValueError('Source page already committed; resume must skip processed occurrences')
        pixel=row['pixel_sha256'];first=seen.get(pixel)
        if first is None:
            found=db.execute('SELECT page_id,logical_id,document_id,image_sha256 FROM pages WHERE pixel_sha256=?',(pixel,)).fetchone()
            if found:first=dict(zip(('page_id','logical_page_id','document_id','image_sha256'),found))
        duplicate=first is not None
        if duplicate:
            d={name:row[name] for name in duplicate_schema().names if name in row}
            d.update(retained_page_id=first['page_id'],retained_logical_page_id=first['logical_page_id'],retained_document_id=first['document_id'],retained_image_sha256=first['image_sha256'],duplicate_method='exact_canonical_decoded_RGBA_sha256')
            duplicates.append(d)
        else:
            retained.append(row);first=row;seen[pixel]=row
        occurrences.append((row,first['page_id'],int(duplicate)))
    paths=[]
    for kind,part,schema in [('pages',retained,page_schema()),('duplicate_occurrences',duplicates,duplicate_schema())]:
        if not part:continue
        relative=f'data/{kind}/shard-{number:06d}.parquet';path=output/relative
        if path.exists():raise ValueError('Refusing to overwrite uncommitted shard: '+relative)
        digest,size=write_parquet(part,schema,path,image_feature=kind=='pages')
        paths.append((relative,digest,len(part),size,kind))
    pagepath=next((x[0] for x in paths if x[4]=='pages'),None)
    duplicatepath=next((x[0] for x in paths if x[4]=='duplicate_occurrences'),None)
    with db:
        db.execute('INSERT INTO batches VALUES (?)',(number,))
        for relative,digest,count,size,kind in paths:
            db.execute('INSERT INTO shards VALUES (?,?,?,?,0,NULL,?)',(relative,digest,count,size,kind))
        for row in retained:
            db.execute('INSERT INTO pages VALUES (?,?,?,?,?,?,?,?,?,?,?)',(row['page_id'],row['logical_page_id'],row['document_id'],row['source_pdf_page_index'],pagepath,row['pixel_sha256'],normalized_text_hash(row['page_text']),row['split'],row['words_count'],int(row['curated_eligible']),row['image_sha256']))
        for row,first,duplicate in occurrences:
            db.execute('INSERT INTO occurrences VALUES (?,?,?,?,?,?,?,?)',(row['page_id'],row['logical_page_id'],row['document_id'],row['source_pdf_page_index'],first,duplicate,row['pixel_sha256'],duplicatepath if duplicate else pagepath))
    for relative,digest,count,size,kind in paths:
        print(json.dumps({'stage':'shard_staged','kind':kind,'path':relative,'rows':count,'bytes':size,'sha256':digest}),flush=True)
    return {'retained':len(retained),'duplicates':len(duplicates)}

def upload_pending(output,db,*,evict):
    from huggingface_hub import HfApi,CommitOperationAdd
    statepath=output/'private/publication.json';state=json.loads(statepath.read_text()) if statepath.exists() else {}
    api=HfApi()
    if not state.get('dataset_created'):
        if api.repo_exists(REPO,repo_type='dataset'):raise ValueError('Dataset repo exists outside this build state')
        api.create_repo(REPO,repo_type='dataset',private=True);state={'repo_id':REPO,'dataset_created':True,'private':True};atomic_json(statepath,state)
    pending=db.execute('SELECT path,sha256,rows,bytes FROM shards WHERE uploaded=0 ORDER BY path').fetchall()
    for relative,digest,count,size in pending:
        path=output/relative
        if sha_file(path)!=digest:raise ValueError('Local staged shard hash mismatch')
        info=api.create_commit(REPO,repo_type='dataset',operations=[CommitOperationAdd(path_in_repo=relative,path_or_fileobj=path)],commit_message=f'Stage verified image/OCR shard {relative}: {count} pages')
        remote=api.get_paths_info(REPO,[relative],repo_type='dataset',revision=info.oid)[0]
        if not remote.lfs or remote.lfs.sha256!=digest:raise ValueError('Remote shard SHA-256 mismatch')
        db.execute('UPDATE shards SET uploaded=1,commit_sha=? WHERE path=?',(info.oid,relative));db.commit()
        state['last_commit']=info.oid;atomic_json(statepath,state)
        if evict:path.unlink()
        print(json.dumps({'stage':'shard_remote_verified','path':relative,'rows':count,'commit':info.oid,'local_evicted':evict}),flush=True)

def progress(output,db,started):
    row=db.execute('SELECT count(*),sum(words),sum(curated),count(DISTINCT document_id) FROM pages').fetchone()
    shards=db.execute('SELECT count(*),sum(bytes),sum(uploaded) FROM shards').fetchone()
    occurrences=db.execute('SELECT count(*),sum(is_duplicate) FROM occurrences').fetchone()
    obj={'processed_source_occurrences':occurrences[0],'exact_pixel_duplicate_occurrences':occurrences[1] or 0,'pages':row[0],'words':row[1] or 0,'mechanical_screen_passed_pages':row[2] or 0,'documents':row[3],'shards':shards[0],'shard_bytes':shards[1] or 0,'uploaded_shards':shards[2] or 0,'elapsed_seconds':time.monotonic()-started,'local_free_bytes':shutil.disk_usage(output).free,'updated_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())}
    atomic_json(output/'private/build-progress.json',obj);print(json.dumps({'stage':'progress',**obj}),flush=True)

def build(args):
    output=Path(args.output).resolve();output.mkdir(parents=True,exist_ok=True)
    for d in ('private/scratch','private/pdf-cache','data/pages'):(output/d).mkdir(parents=True,exist_ok=True)
    config_sha=freeze_processing_config(args,output)
    db=connect_state(output);worker=VisionWorker(Path(args.vision_binary).resolve());started=time.monotonic()
    try:
        verify_staged_shards(output,db)
        if args.upload:upload_pending(output,db,evict=args.evict_verified)
        books=[b for b in jsonlines(args.books) if eligible(b)]
        books.sort(key=lambda b:hashlib.sha256(b['source_book_id'].encode()).hexdigest())
        selected=getattr(args,'document_id',None)
        if selected:
            wanted=set(selected);books=[b for b in books if b['source_book_id'] in wanted]
            if {b['source_book_id'] for b in books}!=wanted:raise ValueError('Requested document missing or ineligible')
        if args.max_books:books=books[:args.max_books]
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for book in books:
                if args.max_pages and db.execute('SELECT count(*) FROM occurrences').fetchone()[0]>=args.max_pages:break
                docid=book['source_book_id'];book['_processing_config_sha256']=config_sha
                try:
                    check_disk(output)
                    # Check every frozen metadata binding before skipping a completed document.
                    bind_document(db,book)
                    existing=db.execute('SELECT status FROM documents WHERE document_id=?',(docid,)).fetchone()
                    if existing and existing[0]=='complete':continue
                    raw=Path(args.books).resolve().parent/book['raw_manifest_path']
                    if not raw.resolve().is_relative_to(Path(args.books).resolve().parent):raise ValueError('Manifest cache path escapes acquisition root')
                    with gzip.open(raw,'rb') as f:rawbytes=f.read()
                    if hashlib.sha256(rawbytes).hexdigest()!=book['manifest_sha256']:raise ValueError('Manifest cache SHA mismatch')
                    manifest=json.loads(rawbytes)
                    canvases=[]
                    for canvas in manifest['items']:
                        body=canvas['items'][0]['items'][0]['body']
                        canvases.append({'id':canvas['id'],'label':str(next(iter(canvas.get('label',{}).values()),[''])[0]),'width':canvas['width'],'height':canvas['height'],'image_service':body['service'][0]['id']})
                    if len({c['id'] for c in canvases})!=len(canvases):raise ValueError('Repeated source canvas ID')
                    book['_canvases']=canvases
                    book['_source_homepage']=manifest.get('homepage',[{}])[0].get('id',book['manifest_url'])
                    preflight_metadata(book)
                    pdfpath,pdfsha,pdfurl=download_pdf(book,output,db)
                    count=pdf_page_count(pdfpath)
                    # Bind the PDF before later checks fail; a failed book never loses this identity.
                    bind_document(db,book,pdf_sha256=pdfsha,pdf_url=pdfurl,pages=count)
                    if count!=book['source_canvas_count']:raise ValueError(f'PDF/canvas page-count disagreement: {count} vs {book["source_canvas_count"]}')
                    if count!=len(canvases):raise ValueError('Manifest canvas count changed')
                    db.execute('UPDATE documents SET status=?,error=NULL WHERE document_id=?',('processing',docid));db.commit()
                    done={x[0] for x in db.execute('SELECT page_index FROM occurrences WHERE document_id=?',(docid,))}
                    indices=[i for i in range(count) if i not in done]
                    if args.max_pages:
                        remaining=args.max_pages-db.execute('SELECT count(*) FROM occurrences').fetchone()[0];indices=indices[:max(0,remaining)]
                    for start in range(0,len(indices),args.shard_pages):
                        part=indices[start:start+args.shard_pages]
                        tasks=[(book,str(pdfpath),pdfsha,pdfurl,i,count) for i in part]
                        buffer=[]
                        for task,result in zip(tasks,pool.map(lambda task: safe_process(task,worker,output),tasks)):
                            if isinstance(result,Exception):
                                db.execute('INSERT INTO failures VALUES (?,?,?,?)',(docid,task[4],str(result)[:4000],time.strftime('%FT%TZ',time.gmtime())));db.commit()
                                print(json.dumps({'stage':'page_failed','document':docid,'page':task[4],'error':str(result)[:500]}),flush=True)
                            else:buffer.append(result)
                        stage_shard(buffer,output,db)
                        if args.upload:upload_pending(output,db,evict=args.evict_verified)
                        progress(output,db,started)
                    actual=db.execute('SELECT count(*) FROM occurrences WHERE document_id=?',(docid,)).fetchone()[0]
                    status='complete' if actual==count else 'partial'
                    db.execute('UPDATE documents SET status=? WHERE document_id=?',(status,docid));db.commit()
                    if args.evict_verified:
                        evict_verified_pdf(pdfpath,output,db,docid)
                except Exception as exc:
                    record_document_error(db,docid,exc)
                    print(json.dumps({'stage':'document_failed','document':docid,'error':str(exc)[:1000]}),flush=True)
                    if 'storage floor' in str(exc).lower():raise
    finally:
        worker.close();progress(output,db,started);db.close()

def safe_process(task,worker,output):
    try:return process_page(task,worker,output)
    except Exception as exc:return exc

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--books',required=True);p.add_argument('--output',required=True);p.add_argument('--vision-binary',required=True)
    p.add_argument('--workers',type=int,default=2);p.add_argument('--shard-pages',type=int,default=128)
    p.add_argument('--max-books',type=int);p.add_argument('--max-pages',type=int)
    p.add_argument('--document-id',action='append',help='Restrict to specific eligible source IDs; repeatable')
    p.add_argument('--upload',action='store_true');p.add_argument('--evict-verified',action='store_true')
    args=p.parse_args()
    if args.evict_verified and not args.upload:p.error('--evict-verified requires --upload')
    if not 1<=args.workers<=4 or not 1<=args.shard_pages<=256:p.error('Bounded worker/shard limits exceeded')
    build(args)
