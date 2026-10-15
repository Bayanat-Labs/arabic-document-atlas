#!/usr/bin/env python3
"""Bounded independent re-download/image/row audit of uploaded primary shards.

Read-only source ledger, immutable Hub commits and one owned temporary cache.
The deliberately diverse sample is deterministic, not statistically random.
"""
from __future__ import annotations
import argparse
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import shutil
import sqlite3
import sys
import tempfile
import time

import pyarrow.parquet as pq
import requests
from PIL import Image
from datasets import load_dataset
from huggingface_hub import HfApi, get_token

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))
from quality.validate import validate_page
from pipeline.build_pages import page_schema

REPO = 'Bayanat-Labs/arabic-document-atlas'
FLOOR = 4 * 1024**3
MIN_ROWS = 64
MAX_SHARD_BYTES = 320 * 1024**2
SELECTION = 'bayanat-additional-remote-shards-audit-v1'


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024**2), b''): h.update(block)
    return h.hexdigest()


def atomic(path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.json.tmp')
    if path.is_symlink() or temporary.is_symlink(): raise ValueError('Unsafe audit report symlink')
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    temporary.replace(path)


def freeze_selection(output, inventory):
    raw = inventory.read_bytes()
    books = {r['source_book_id']: r for r in map(json.loads, gzip.decompress(raw).decode().splitlines())}
    conn = sqlite3.connect(f'file:{output / "private/build.sqlite"}?mode=ro', uri=True)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute('BEGIN')
        shards = [dict(r) for r in conn.execute("SELECT * FROM shards WHERE uploaded=1 AND kind='pages' ORDER BY path")]
        pages = [dict(r) for r in conn.execute('SELECT * FROM pages ORDER BY shard_path,page_id')]
        bindings = {r['document_id']: dict(r) for r in conn.execute('SELECT document_id,manifest_sha256,book_record_sha256,pdf_sha256 FROM documents')}
        observed_at = datetime.now(timezone.utc).isoformat()
    finally:
        conn.rollback(); conn.close()
    byshard = {}
    for page in pages: byshard.setdefault(page['shard_path'], []).append(page)
    candidates = []
    for shard in shards:
        if (shard['path'] == 'data/pages/shard-000000.parquet' or shard['rows'] < MIN_ROWS
                or not 0 < shard['bytes'] <= MAX_SHARD_BYTES): continue
        if not re.fullmatch(r'data/pages/shard-[0-9]{6}\.parquet', shard['path']): raise ValueError('Unexpected ledger shard path')
        if not re.fullmatch('[0-9a-f]{40}', shard['commit_sha'] or ''): raise ValueError('Unpinned ledger upload commit')
        ledger = byshard[shard['path']]
        if len(ledger) != shard['rows']: raise ValueError('Frozen shard/ledger row mismatch')
        docs = sorted({r['document_id'] for r in ledger})
        providers, bins, metadata = set(), set(), []
        for doc in docs:
            book = books[doc]
            clean = {k: v for k, v in book.items() if not k.startswith('_')}
            bound = hashlib.sha256(json.dumps(clean, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            if bound != bindings[doc]['book_record_sha256'] or book['manifest_sha256'] != bindings[doc]['manifest_sha256']:
                raise ValueError('Current inventory differs from frozen document metadata')
            provider = book.get('provider') or ['Unknown source provider']
            years = book.get('publication_year_tokens') or []
            year = years[0] if len(years) == 1 else None
            yearbin = f'{year // 10 * 10}-{year // 10 * 10 + 9}' if year is not None else 'unknown_or_ambiguous'
            providers.update(provider); bins.add(yearbin)
            metadata.append({'source_document_id': doc, 'providers': provider, 'catalog_publication_year': year,
                             'catalog_year_bin': yearbin, 'manifest_sha256': book['manifest_sha256'],
                             'book_record_sha256': bound, 'source_pdf_sha256': bindings[doc]['pdf_sha256']})
        candidates.append({**shard, 'source_documents': metadata, 'provider_keys': sorted(providers), 'year_bin_keys': sorted(bins),
            'selection_tiebreak_sha256': hashlib.sha256((SELECTION + ':' + shard['path']).encode()).hexdigest()})
    selected, seen_providers, seen_bins = [], set(), set()
    while len(selected) < 3:
        remaining = [r for r in candidates if r['path'] not in {s['path'] for s in selected}]
        if not remaining: raise ValueError('Three eligible bounded shards are required')
        pick = min(remaining, key=lambda r: (-3 * len(set(r['provider_keys']) - seen_providers)
                   - len(set(r['year_bin_keys']) - seen_bins), r['selection_tiebreak_sha256'], r['path']))
        selected.append(pick); seen_providers.update(pick['provider_keys']); seen_bins.update(pick['year_bin_keys'])
    ledger = {s['path']: {p['page_id']: p for p in byshard[s['path']]} for s in selected}
    population = {'sqlite_snapshot_observed_at': observed_at, 'uploaded_primary_shards': len(shards),
        'uploaded_primary_rows': sum(s['rows'] for s in shards), 'eligible_bounded_shards': len(candidates),
        'inventory_sha256': hashlib.sha256(raw).hexdigest(), 'inventory_bytes': len(raw),
        'selection_rule': 'Exclude shard000000; require >=64 rows and <=320MiB; greedily maximize3*new source-provider labels + new catalog-decade bins, breaking ties by SHA256(fixed policy ID:path) then path. No OCR scores or recognition outcomes participate.',
        'policy_id': SELECTION, 'is_random_sample': False,
        'catalog_dates_are_source_metadata_not_independent_page_dates': True}
    return selected, ledger, population


def download(shard, path, api):
    info = api.repo_info(REPO, repo_type='dataset', revision=shard['commit_sha'], files_metadata=True)
    if info.sha != shard['commit_sha']: raise ValueError('Remote commit differs from frozen ledger')
    entries = [entry for entry in info.siblings if entry.rfilename == shard['path']]
    if len(entries) != 1 or entries[0].size != shard['bytes'] or not entries[0].lfs or entries[0].lfs.sha256 != shard['sha256']:
        raise ValueError('Remote metadata differs from frozen shard size/SHA256')
    url = f'https://huggingface.co/datasets/{REPO}/resolve/{shard["commit_sha"]}/{shard["path"]}'
    if shutil.disk_usage(path.parent).free < FLOOR + shard['bytes'] + 32 * 1024**2:
        raise RuntimeError('FourGiB free-space floor would be violated')
    headers = {'User-Agent': 'BayanatDocumentAtlasAudit/0.1 (+https://www.bayanatlabs.com)'}
    token = get_token()
    if token: headers['Authorization'] = 'Bearer ' + token
    digest, total = hashlib.sha256(), 0
    # Requests strips Authorization on a cross-host redirect. Never serialize the
    # signed final URL, auth headers, cookies or response headers into this report.
    with requests.get(url, headers=headers, stream=True, timeout=(20, 90)) as response:
        response.raise_for_status()
        with path.open('xb') as stream:
            for block in response.iter_content(1024**2):
                if shutil.disk_usage(path.parent).free < FLOOR + len(block): raise RuntimeError('FourGiB free-space floor reached')
                total += len(block)
                if total > shard['bytes']: raise ValueError('Downloaded bytes exceed frozen shard size')
                stream.write(block); digest.update(block)
    if total != shard['bytes'] or digest.hexdigest() != shard['sha256']:
        raise ValueError('Complete downloaded SHA256/size differs')
    return url


def inspect(path, shard, ledger):
    parquet = pq.ParquetFile(path)
    if parquet.metadata.num_rows != shard['rows']: raise ValueError('Parquet row count differs')
    if not parquet.schema_arrow.equals(page_schema(), check_metadata=False): raise ValueError('Primary Arrow schema differs')
    observed, warnings, excluded, words = set(), Counter(), Counter(), 0
    providers, dates, documents = Counter(), Counter(), Counter()
    first_id = None
    for batch in parquet.iter_batches(batch_size=8):
        for row in batch.to_pylist():
            rid = row['page_id']
            if rid in observed or rid not in ledger: raise ValueError('Unexpected/duplicate page ID')
            observed.add(rid); expected = ledger[rid]
            if first_id is None: first_id = rid
            for key, stored in [('document_id', 'document_id'), ('source_pdf_page_index', 'page_index'),
                ('pixel_sha256', 'pixel_sha256'), ('image_sha256', 'image_sha256'), ('split', 'split'), ('words_count', 'words')]:
                if row[key] != expected[stored]: raise ValueError('Frozen ledger source/image field differs: ' + key)
            encoded = row['image']['bytes']
            with Image.open(io.BytesIO(encoded)) as image:
                if image.format != 'JPEG': raise ValueError('Retained image is not JPEG')
                image.verify()
            report = validate_page(row, image_bytes=encoded)
            if not report['valid']:
                raise ValueError('Serialized image/geometry/text/provenance validation failed: ' + json.dumps(report['errors'], ensure_ascii=False))
            if report['metrics']['pixel_sha256'] != row['pixel_sha256'] or report['metrics']['dhash64'] != row['dhash64']:
                raise ValueError('Actual decoded pixel/hash evidence differs')
            if report['curated_eligible'] != row['curated_eligible'] or int(row['curated_eligible']) != expected['curated']:
                raise ValueError('Recomputed mechanical screen differs')
            if report['curated_exclusion_reasons'] != row['curated_exclusion_reasons']:
                raise ValueError('Recomputed screen reasons differ')
            warnings.update(w['code'] for w in report['warnings']); excluded.update(report['curated_exclusion_reasons'])
            words += row['words_count']; providers[row['source_provider']] += 1
            dates[row['source_publication_date']] += 1; documents[row['document_id']] += 1
    if observed != set(ledger): raise ValueError('Frozen ledger page set differs from full shard')
    data = load_dataset('parquet', data_files=str(path), split='train', streaming=True, cache_dir=str(path.parent / 'hf-cache'))
    first = next(iter(data)); image = first['image']
    if not isinstance(image, Image.Image): raise ValueError('Actual native HF Image feature did not decode to PIL')
    image.load()
    if first['page_id'] != first_id or image.size != (first['width'], first['height']): raise ValueError('HF decoded example identity/size differs')
    return {'status': 'passed', 'registered_rows': shard['rows'], 'rows_checked': len(observed),
        'all_rows_PIL_verified_and_fully_decoded': len(observed), 'all_rows_quality_validate_valid': len(observed),
        'all_rows_frozen_ledger_fields_matched': len(observed), 'native_HF_Image_PIL_rows_checked': 1,
        'native_HF_example_page_id': first_id, 'machine_word_records': words,
        'quality_warning_counts': dict(warnings), 'mechanical_screen_exclusion_counts': dict(excluded),
        'observed_source_provider_field_counts': dict(providers), 'observed_catalog_date_field_counts': dict(dates),
        'source_document_row_counts': dict(documents)}


def run(output, inventory):
    output, inventory = Path(output).resolve(), Path(inventory).resolve()
    report_path = output / 'reports/additional-remote-shards-audit.json'
    report = {'status': 'running', 'audit': 'independent_additional_remote_primary_shards_v1', 'repo_id': REPO,
        'started_at': datetime.now(timezone.utc).isoformat(), 'sample_is_random': False, 'selected_shards': [],
        'errors': [], 'no_OCR_accuracy_or_gold_claim': True, 'free_space_floor_bytes': FLOOR,
        'read_only_source_ledger': True, 'method': ['Freeze candidate shard paths/size/SHA256/upload commits and page ledger in one read-only SQLite transaction.',
        'Bind provider/catalog-date selection evidence to the frozen document metadata hash.',
        'Check immutable Hub LFS metadata; download complete bytes into one owned temporary cache with4GiB floor.',
        'Check complete SHA256/byte length/Arrow schema/row counts and frozen ledger page set.',
        'Every row: actual JPEG/Pillow verification, full decoded-pixel hash and quality.validate geometry/text/provenance/screen validation.',
        'At least one actual Hugging Face streaming Image→PIL decode per selected shard.'],
        'scope': 'These three deliberately selected uploaded primary shards only, not a random or representative corpus audit. Checks structural/image/provenance integrity; does not measure OCR CER/WER, semantic reading order, box accuracy, image/text visual alignment, physical-page independence or worldwide rights clearance.',
        'audit_code_sha256': sha(Path(__file__)), 'validator_sha256': sha(PROJECT / 'quality/validate.py'),
        'primary_builder_schema_code_sha256': sha(PROJECT / 'pipeline/build_pages.py')}
    atomic(report_path, report)
    start = time.monotonic()
    try:
        selected, ledger, population = freeze_selection(output, inventory)
        report['selection_population'] = population
        report['selected_shards'] = [{k: v for k, v in s.items() if k != 'uploaded'} for s in selected]
        atomic(report_path, report)
        with tempfile.TemporaryDirectory(prefix='additional-remote-audit-', dir=output / 'private') as temporary:
            cache = Path(temporary); api = HfApi()
            for shard, result in zip(selected, report['selected_shards']):
                path = cache / Path(shard['path']).name
                print(json.dumps({'stage': 'download', 'path': shard['path'], 'commit': shard['commit_sha'], 'bytes': shard['bytes']}), flush=True)
                result['canonical_download_url'] = download(shard, path, api)
                result.update(inspect(path, shard, ledger[shard['path']]))
                path.unlink(); result['owned_download_evicted_after_checks'] = True
                atomic(report_path, report)
                print(json.dumps({'stage': 'shard_passed', 'path': shard['path'], 'rows_checked': result['rows_checked']}), flush=True)
        report['status'] = 'passed'
        report['totals'] = {'shards_checked': len(selected), 'downloaded_bytes_verified': sum(s['bytes'] for s in selected),
                           'rows_checked': sum(s['rows_checked'] for s in report['selected_shards']),
                           'native_HF_Image_PIL_rows_checked': sum(s['native_HF_Image_PIL_rows_checked'] for s in report['selected_shards'])}
        if sha(PROJECT / 'quality/validate.py') != report['validator_sha256'] or sha(PROJECT / 'pipeline/build_pages.py') != report['primary_builder_schema_code_sha256']:
            raise ValueError('Frozen validation/schema code changed during independent audit')
    except Exception as error:
        report['status'] = 'failed'
        # Exception strings from HTTP clients can contain signed query URLs.
        report['errors'].append({'error_type': type(error).__name__, 'message': 'Audit stopped; see private local command output for non-HTTP details.'})
        atomic(report_path, report)
        raise
    finally:
        report['finished_at'] = datetime.now(timezone.utc).isoformat()
        report['elapsed_seconds'] = time.monotonic() - start
        atomic(report_path, report)
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--inventory', type=Path, required=True)
    args = parser.parse_args()
    result = run(args.output, args.inventory)
    print(json.dumps({'status': result['status'], 'totals': result['totals']}))
