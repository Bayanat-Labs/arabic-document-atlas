"""Package and publish a quiet, verified Atlas build without copying its images.

The page builder owns dataset creation. This script only adopts its exact
publication state, verifies every remote shard and publishes a pinned snapshot.
Source packaging uses an allowlist and never includes private acquisition data.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import sqlite3
import subprocess
import time
import pyarrow.parquet as pq
from huggingface_hub import HfApi
from build_pages import book_record_hash

SOURCE = Path(__file__).resolve().parents[1]
REPO = 'Bayanat-Labs/arabic-document-atlas'
SOURCE_FILES = '''README.md requirements.txt DATA_LICENSE.md ATTRIBUTION.md LICENSE CITATION.cff .gitignore
.github/workflows/validate.yml
acquisition/inventory.py acquisition/README.md acquisition/wikisource.py
audit/SOURCE_RIGHTS_AND_QUALITY.md audit/audit_file_templates.py audit/package_community.py audit/test-report.json
audit/test_community_package.py audit/test_source_semantics.py audit/validate_community.py audit/wikisource_inventory_qc.py
docs/METHODOLOGY.md docs/USAGE.md docs/SCALING.md
explorer/README.md explorer/index.html explorer/style.css explorer/app.js
pipeline/build_pages.py pipeline/build_report.py pipeline/evict_inputs.py pipeline/package_explorer.py pipeline/publish_explorer.py pipeline/release.py pipeline/vision_ocr.swift
quality/BUILDER_VALIDATION.md quality/QUALITY_CONTRACT.md quality/__init__.py quality/page.schema.json quality/source_metadata_notes.json
quality/test_build_pages.py quality/test_build_report.py quality/test_validate.py quality/validate.py'''.split()
OPTIONAL_SOURCE_FILES = ['pipeline/build_reference_pairs.py','pipeline/REFERENCE_PAIRS.md',
    'pipeline/release_components.py','pipeline/publish_snapshot.py',
    'quality/test_reference_pairs.py','quality/test_release.py','quality/test_release_components.py',
    'quality/test_publish_snapshot.py','quality/test_publish_explorer.py','quality/test_release_integration.py']

def dump(path, value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')

def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(8*1024**2),b''):h.update(block)
    return h.hexdigest()

def rows(path):
    with gzip.open(path,'rt',encoding='utf-8') as f:
        for line in f:
            if line.strip():yield json.loads(line)

def source_package(destination):
    destination=Path(destination);destination.mkdir(parents=True,exist_ok=True)
    for name in SOURCE_FILES+[n for n in OPTIONAL_SOURCE_FILES if (SOURCE/n).exists()]:
        file=SOURCE/name
        if file.is_symlink() or not file.resolve().is_relative_to(SOURCE):raise ValueError('Unsafe source package path: '+name)
        target=destination/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(file,target)

def copy_public_community(community,release):
    """Copy declared component paths only; private/unknown files never escape."""
    community=Path(community).resolve();release=Path(release).resolve()
    report=json.loads((community/'reports/community-text-build.json').read_text())
    fixed={'reports/community-text-build.json','reports/community-text-validation.json',
        'reports/wikisource-file-description-template-audit.json','reports/wikisource-image-rights-holds.json',
        'reports/wikisource-inventory-qc.json','reports/wikisource-rights-exclusions.json',
        'metadata/wikisource/README.md','metadata/wikisource/scan_file_descriptions.jsonl.gz',
        'metadata/wikisource/scan_files.jsonl.gz','metadata/wikisource/source_rights_and_quality.json'}
    declared=[r['path'] for r in report['data_files']+report['metadata_files']+report['reports']]
    if len(set(declared))!=len(declared):raise ValueError('Repeated community artifact path')
    names=declared+['reports/community-text-build.json','reports/community-text-validation.json']
    for name in names:
        value=PurePosixPath(name)
        if str(value)!=name or value.is_absolute() or '..' in value.parts or '\\' in name:
            raise ValueError('Unsafe community artifact path')
        if name not in fixed and not re.fullmatch(r'data/community_text/part-[0-9]{5}\.parquet',name):
            raise ValueError('Undeclared public community prefix: '+name)
        src=community/name;target=release/name
        if any((community/Path(*value.parts[:i])).is_symlink() for i in range(1,len(value.parts)+1)) or not src.is_file():
            raise ValueError('Unsafe community artifact symlink or missing file')
        if target.exists() or any((release/Path(*value.parts[:i])).is_symlink() for i in range(1,len(value.parts)+1)):
            raise ValueError('Community artifact conflicts with another component')
    for name in names:
        target=release/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(community/name,target)

def package(output, books, community, version, references=None):
    output=Path(output).resolve();final_release=output/'release'
    if final_release.exists():
        raise ValueError('Release directory already exists. Preserve it; use a new snapshot build or explicitly archive the task-owned old package before repackaging.')
    state=json.loads((output/'private/publication.json').read_text())
    if state.get('dataset_version')==version:
        raise ValueError('This immutable version was already staged; do not regenerate its local evidence')
    release=output/f'.release-staging-{time.time_ns()}';release.mkdir()
    db=sqlite3.connect(f'file:{output/"private/build.sqlite"}?mode=ro',uri=True);db.row_factory=sqlite3.Row
    try:
        data_version=db.execute('PRAGMA data_version').fetchone()[0]
        db.execute('BEGIN')
        if db.execute("SELECT count(*) FROM documents WHERE status IN ('processing','pending')").fetchone()[0]:
            raise ValueError('A document is still processing; stop the builder before packaging')
        shards=[dict(r) for r in db.execute('SELECT * FROM shards ORDER BY path')]
        if not shards or any(not s['uploaded'] for s in shards):raise ValueError('Every page shard must be uploaded and verified first')
        count=db.execute('SELECT count(*) FROM pages').fetchone()[0]
        page_shards=[s for s in shards if s.get('kind','pages')=='pages']
        if count!=sum(s['rows'] for s in page_shards):raise ValueError('Page/shard row-count mismatch')
        # Independent remote file metadata verification covers evicted local shards.
        api=HfApi()
        if state.get('repo_id')!=REPO or not state.get('dataset_created'):raise ValueError('Builder-owned publication state required')
        remote=api.repo_info(REPO,repo_type='dataset',files_metadata=True)
        indexed={s.rfilename:s for s in remote.siblings}
        unknown_data=[name for name in indexed if name.startswith('data/') and name.endswith('.parquet')
                      and not name.startswith(('data/pages/','data/duplicate_occurrences/','data/community_text/','data/reference_pairs/'))]
        if unknown_data:raise ValueError('Remote data paths differ: unsupported component '+str(unknown_data))
        if {s['path'] for s in shards}!={name for name in indexed if name.startswith(('data/pages/','data/duplicate_occurrences/')) and name.endswith('.parquet')}:
            raise ValueError('Remote data paths differ from the quiet build snapshot; stop the builder before packaging')
        remote_community={name for name in indexed if name.startswith('data/community_text/') and name.endswith('.parquet')}
        if remote_community:
            if not community:raise ValueError('Existing community data require an explicitly verified package')
            community_root=Path(community).resolve()
            expected_community={str(p.relative_to(community_root)):p for p in (community_root/'data/community_text').glob('*.parquet')}
            if remote_community!=set(expected_community):raise ValueError('Remote community files differ from the supplied package')
            for name,path in expected_community.items():
                if not indexed[name].lfs or indexed[name].lfs.sha256!=sha(path) or indexed[name].size!=path.stat().st_size:
                    raise ValueError('Existing remote community shard hash mismatch')
        for shard in shards:
            entry=indexed.get(shard['path'])
            if entry is None or not entry.lfs or entry.lfs.sha256!=shard['sha256'] or entry.size!=shard['bytes']:
                raise ValueError('Remote shard hash/size mismatch: '+shard['path'])
        reference_info=None
        if references:
            from release_components import validate_copy_reference
            reference_info=validate_copy_reference(Path(references).resolve(),release,indexed)
        elif any(name.startswith('data/reference_pairs/') for name in indexed):
            raise ValueError('Existing reference data require an explicitly verified package')
        now=datetime.now(timezone.utc).isoformat()
        documents=[dict(r) for r in db.execute('SELECT document_id,count(*) AS image_pages,sum(words) AS words,sum(curated) AS mechanically_screened FROM pages GROUP BY document_id ORDER BY document_id')]
        docmap={d['document_id']:d for d in documents}
        bindings={r['document_id']:dict(r) for r in db.execute('SELECT * FROM documents')}
        sourceids={r[0] for r in db.execute('SELECT DISTINCT document_id FROM occurrences')}
        sourcebooks=[]
        for book in rows(books):
            if book['source_book_id'] in sourceids:
                binding=bindings[book['source_book_id']]
                if book['manifest_sha256']!=binding['manifest_sha256'] or book_record_hash(book)!=binding['book_record_sha256']:
                    raise ValueError('Current book metadata differs from frozen source binding: '+book['source_book_id'])
                clean={k:v for k,v in book.items() if k not in {'raw_manifest_path','request_headers','response_headers'}}
                clean.update(docmap.get(book['source_book_id'],{'document_id':book['source_book_id'],'image_pages':0,'words':0,'mechanically_screened':0}))
                clean.update(source_pdf_sha256=binding['pdf_sha256'],source_pdf_url=binding['pdf_url'],source_pdf_page_count=binding['pages'])
                sourcebooks.append(clean)
                raw=Path(books).resolve().parent/book['raw_manifest_path']
                if not raw.resolve().is_relative_to(Path(books).resolve().parent):raise ValueError('Manifest cache path escapes acquisition root')
                with gzip.open(raw,'rb') as f:rawbytes=f.read()
                if hashlib.sha256(rawbytes).hexdigest()!=binding['manifest_sha256']:raise ValueError('Frozen raw manifest hash mismatch')
                target=release/'metadata/source-manifests'/f'{book["source_book_id"]}.json.gz'
                target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(raw,target)
        if len(sourcebooks)!=len(sourceids):raise ValueError('Missing or duplicated source book metadata')
        with gzip.open(release/'source-documents.jsonl.gz','wt',encoding='utf-8') as f:
            for book in sourcebooks:f.write(json.dumps(book,ensure_ascii=False)+'\n')
        tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        duplicate_count=db.execute('SELECT count(*) FROM occurrences WHERE is_duplicate=1').fetchone()[0] if 'occurrences' in tables else 0
        if 'occurrences' in tables:
            with gzip.open(release/'duplicate-occurrences.jsonl.gz','wt',encoding='utf-8') as f:
                for r in db.execute('SELECT * FROM occurrences WHERE is_duplicate=1'):f.write(json.dumps(dict(r),ensure_ascii=False)+'\n')
        with gzip.open(release/'page-index.jsonl.gz','wt',encoding='utf-8') as f:
            for r in db.execute('SELECT * FROM pages ORDER BY shard_path,page_index'):f.write(json.dumps(dict(r),ensure_ascii=False)+'\n')
        failures=db.execute('SELECT count(*) FROM failures').fetchone()[0]
        with gzip.open(release/'page-failure-attempts.jsonl.gz','wt',encoding='utf-8') as f:
            for item in db.execute('SELECT * FROM failures'):
                item=dict(item);item['error']=item['error'].replace(str(output),'[BUILD]').replace(str(SOURCE),'[SOURCE]')
                f.write(json.dumps(item,ensure_ascii=False)+'\n')
        words=db.execute('SELECT coalesce(sum(words),0) FROM pages').fetchone()[0]
        curated=db.execute('SELECT coalesce(sum(curated),0) FROM pages').fetchone()[0]
        community=Path(community).resolve() if community else None
        community_count=0;community_bytes=0
        if community:
            if (community/'reports').is_symlink() or (community/'reports/community-text-validation.json').is_symlink():
                raise ValueError('Community validation report must be an owned regular path')
            from audit.validate_community import validate as validate_community_package
            community_validation=validate_community_package(community)
            if community_validation.get('status')!='passed':raise ValueError('Community package validation failed')
            files=sorted((community/'data/community_text').glob('*.parquet'))
            if not files:raise ValueError('Community package has no Parquet')
            schema=pq.read_schema(files[0])
            for file in files:
                if not pq.read_schema(file).equals(schema):raise ValueError('Community schemas differ')
                community_count+=pq.ParquetFile(file).metadata.num_rows;community_bytes+=file.stat().st_size
            if community_count!=community_validation['rows_checked']:raise ValueError('Community validated row count mismatch')
            copy_public_community(community,release)
        primary_pixels={r[0] for r in db.execute('SELECT pixel_sha256 FROM pages')}
        reference_pixels=set(reference_info['pixel_hashes']) if reference_info else set()
        unique_total=len(primary_pixels|reference_pixels)
        summary={
            'name':'Bayanat Arabic Document Atlas','version':version,'snapshot_at':now,
            'unique_image_pages':count,'source_documents_with_images':len(documents),'source_documents_with_any_occurrence':len(sourceids),
            'machine_word_records':words,'mechanical_screen_pass_pages':curated,
            'exact_pixel_duplicate_occurrences_removed':duplicate_count,
            'page_shards':len(page_shards),'page_parquet_bytes':sum(s['bytes'] for s in page_shards),
            'community_text_records':community_count,'community_text_parquet_bytes':community_bytes,
            'community_text_contains_image_bytes':False,'synthetic_image_pages':0,
            'reference_pair_rows':reference_info['rows'] if reference_info else 0,
            'reference_pair_source_documents':reference_info['source_document_count'] if reference_info else 0,
            'reference_pair_parquet_bytes':reference_info['parquet_bytes'] if reference_info else 0,
            'reference_source_pdf_bytes':reference_info['source_pdf_bytes'] if reference_info else 0,
            'total_unique_document_images':unique_total,
            'cross_component_exact_pixel_overlap':len(primary_pixels & reference_pixels),
            'total_source_document_records_with_images':len(documents)+(reference_info['source_document_count'] if reference_info else 0),
            'remote_reference_sha256_verified':reference_info['remote_data_sha256_verified'] if reference_info else None,
            'remote_community_sha256_verified':bool(remote_community) if community_count else None,
            'manually_verified_bbox_pages':0,'held_out_evaluation_split':False,
            'recorded_page_failure_attempts':failures,'remote_page_sha256_verified':True,
            'page_data_commit_before_documentation':remote.sha,
            'counts_are_not_additive':'The total image count is the union of decoded-pixel hashes across image components. Community text, reference transcriptions, word records and duplicate occurrences are not additional unique images.',
            'source_selection':'Institution-asserted public domain; unambiguous edition year <=1930; Arabic; no contrary manifest rights evidence.',
            'catalog_publication_year_exclusive_max':1931,
            'catalog_years_are_not_independently_verified_page_dates':True,
        }
        dump(release/'metadata/release-summary.json',summary)
        dump(release/'metadata/shards.json',{'version':version,'shards':shards})
        dump(release/'metadata/source-bindings.json',{'documents':[
            {k:r[k] for k in ['document_id','pdf_sha256','pdf_url','pages','status','manifest_sha256','manifest_url','book_record_sha256']}
            for r in bindings.values()]})
        for filename in ['processing-config.json','input-inventories.json']:
            shutil.copy2(output/'private'/filename,release/'metadata'/filename)
        dump(release/'reports/integrity.json',{
            'passed':True,'snapshot_at':now,'pages_checked_before_sharding':count,
            'remote_page_shards_verified':len(page_shards),'remote_all_shards_verified':len(shards),
            'scope':'Every retained main-tier page image was decoded and checked for dimensions, byte/pixel hashes, word/line geometry, text reconstruction and provenance during the build. Every released remote main-tier shard SHA-256 and byte length was rechecked when packaging. Optional reference images have separate all-row source/image validation and no word/line geometry.',
            'not_measured':['OCR character accuracy','semantic reading-order accuracy','worldwide legal clearance','human box accuracy'],
        })
        coverage=json.loads((output/'reports/coverage.json').read_text())
        if coverage['counts']['retained_unique_images']!=count or coverage['counts']['machine_word_records']!=words:
            raise ValueError('Coverage report does not match this quiet snapshot')
        for filename in ['coverage.json','COVERAGE.md']:
            shutil.copy2(output/'reports'/filename,release/'reports'/filename)
        first_audit=output/'reports/first-remote-shard-audit.json'
        if first_audit.exists():shutil.copy2(first_audit,release/'reports'/first_audit.name)
        for name in ['DATA_LICENSE.md','ATTRIBUTION.md','LICENSE','CITATION.cff']:
            shutil.copy2(SOURCE/name,release/name)
        for name in [n for n in SOURCE_FILES if n.startswith('docs/')]:
            source=SOURCE/name
            if source.is_symlink() or not source.resolve().is_relative_to(SOURCE):raise ValueError('Unsafe public documentation path')
            target=release/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(source,target)
        (release/'README.md').write_text(dataset_card(summary))
        (release/'.gitattributes').write_text('*.parquet filter=lfs diff=lfs merge=lfs -text\n*.gz filter=lfs diff=lfs merge=lfs -text\n*.pdf filter=lfs diff=lfs merge=lfs -text\n')
        # Checksums cover all release files and already-verified page shards without
        # re-downloading the page images onto this disk.
        checks={s['path']:s['sha256'] for s in shards}
        for file in release.rglob('*'):
            if file.is_file() and file.name!='SHA256SUMS':checks[str(file.relative_to(release))]=sha(file)
        (release/'SHA256SUMS').write_text(''.join(f'{v}  {k}\n' for k,v in sorted(checks.items())))
        db.commit()
        if db.execute('PRAGMA data_version').fetchone()[0]!=data_version:
            raise ValueError('Build changed while packaging; stop the builder and regenerate a quiet snapshot')
        release.replace(final_release)
        return summary
    finally:
        db.close()

def dataset_card(s):
    n=s['unique_image_pages'];total=s.get('total_unique_document_images',n)
    category='n<1K' if total<1000 else '1K<n<10K' if total<10000 else '10K<n<100K' if total<100000 else '100K<n<1M' if total<1000000 else '1M<n<10M'
    community='''  - config_name: community_text
    data_files:
      - split: train
        path: data/community_text/*.parquet
''' if s['community_text_records'] else ''
    duplicates='''  - config_name: duplicate_occurrences
    data_files:
      - split: train
        path: data/duplicate_occurrences/*.parquet
''' if s['exact_pixel_duplicate_occurrences_removed'] else ''
    references='''  - config_name: reference_pairs
    data_files:
      - split: train
        path: data/reference_pairs/*.parquet
''' if s.get('reference_pair_rows') else ''
    return f'''---
language:
  - ar
  - en
license: other
license_name: component-specific-public-domain-cc-by-cc-by-sa
license_link: https://huggingface.co/datasets/{REPO}/blob/main/DATA_LICENSE.md
pretty_name: Bayanat Arabic Document Atlas
size_categories:
  - {category}
task_categories:
  - image-to-text
  - image-text-to-text
tags:
  - arabic
  - document-ai
  - ocr
  - historical-documents
  - scanned-documents
  - provenance
  - arabic-collections-online
configs:
  - config_name: pages
    default: true
    data_files:
      - split: train
        path: data/pages/*.parquet
{community}{references}{duplicates}---

# Bayanat Arabic Document Atlas

**{total:,} unique document images · {s['machine_word_records']:,} machine word records · {s['community_text_records']:,} attributed community-text records.**

A visual Arabic document resource with JPEG page bytes, machine OCR, approximate word localization and line order, exact source PDF/manifest hashes, edition metadata, duplicate lineage and mechanical quality flags. The main scan tier comes from Arabic Collections Online and its supplying institutions. A separate, smaller PDF image/reference tier preserves Wikisource transcriptions and file-specific permission evidence. Bayanat builds the normalized visual representation and auditable processing pipeline.

[Explore actual page examples](https://huggingface.co/spaces/{REPO}) · [Source and pipeline](https://github.com/{REPO}) · [Research and release record](https://www.bayanatlabs.com/open/arabic-document-atlas) · [Usage](docs/USAGE.md) · [Rights and attribution](DATA_LICENSE.md)

## Snapshot {s['version']}

| Component | Records | What a row means |
|---|---:|---|
| `pages` | {n:,} | One retained unique decoded raster, with image bytes and machine OCR |
| `community_text` | {s['community_text_records']:,} | A separate attributed Wikisource revision/transclusion body; no image bytes |
| `reference_pairs` | {s.get('reference_pair_rows',0):,} | A source-associated PDF page render plus community body transcription; no machine OCR or word boxes |
| Machine word annotations | {s['machine_word_records']:,} | Approximate word guides inside the page rows, not extra pages |

The main image shards occupy {s['page_parquet_bytes']/1024**3:,.2f} GiB across {s['source_documents_with_images']:,} source books. Reference-pair Parquet occupies {s.get('reference_pair_parquet_bytes',0)/1024**2:,.2f} MiB across {s.get('reference_pair_source_documents',0):,} original PDFs; permitted originals and credit evidence are included separately. Exact decoded-pixel duplicate occurrences removed within the main tier: {s['exact_pixel_duplicate_occurrences_removed']:,}. Cross-component identical rasters: {s.get('cross_component_exact_pixel_overlap',0):,}; these are counted once in the {total:,}-image union. Mechanical screen passed: {s['mechanical_screen_pass_pages']:,} main-tier pages. That screen uses text amount/script and encoding checks; it is not measured OCR accuracy. Covers and sparse pages may remain in the full image tier with flags. The snapshot has no synthetic images and no human-verified bounding boxes.

Counts describe the pinned snapshot, not the full upstream catalog. Source canvases, variants, community-text records and word records are not added to the unique-image total. See [release summary](metadata/release-summary.json), [coverage and failure analysis](reports/COVERAGE.md), [integrity report](reports/integrity.json), [shard manifest](metadata/shards.json) and `SHA256SUMS`. Pin `revision="{s['version']}"` for reproducibility; development on `main` may continue.

## Load the image tier

```python
from datasets import load_dataset

pages = load_dataset("{REPO}", "pages",
                     split="train", streaming=True, revision="{s['version']}")
page = next(iter(pages))
image = page["image"]
print(page["page_text"])
print(page["source_pdf_sha256"], page["source_pdf_page_index"])
```

English is the default for documentation and interfaces. Source Arabic is preserved. Streaming avoids downloading the complete image collection.

## Useful work this supports

Authentic Arabic document-image pretraining, noisy-supervision OCR research, historical print analysis, visual document retrieval and citation-preserving region-highlighting applications. Related open ACO OCR collections already exist; this release contributes page-image bytes, a spatial representation and quality/provenance controls rather than claiming the first ACO transcription.

## Annotation limits

Apple Vision produces the OCR. Its word boxes are approximate guides, not pixel-accurate crop ground truth. Word scores repeat the parent line's uncalibrated confidence and are not word correctness probabilities. Reading order is a machine hypothesis and can fail on columns, poetry, footnotes and tables. No manual table-cell or semantic-layout labels are supplied. OCR errors remain. Native OCR reproduction requires a compatible macOS/Apple Vision system; the pipeline source is open, the system OCR model is proprietary.

`bbox=[left,top,right,bottom]` uses top-left-origin published-image pixels with half-open bounds. JPEG long edge is at most 2400 pixels; this does not certify scanner DPI or native source resolution. The frozen PDF hash plus zero-based page index identify the processed input. Manifest-to-PDF correspondence follows publisher order with equal page counts; individual canvas/PDF pairs have not all been independently verified.

The separate Wikisource text tier preserves source proofread/validated labels, exact revision and history links, raw body wikitext, rendered text and file-rights metadata. Bayanat did not independently proofread it. The body normally excludes non-transcluded headers/footers, and some current scan versions postdate the text revision. It is not error-free whole-image evaluation gold.

`reference_pairs` adds original-PDF image bytes only for a narrower cohort with source quality 3/4, explicit compatible file licensing, copyright-holder permission evidence and a binary version that does not postdate the text revision. Those PDF renders may be scanned or born digital. Page association follows the source Page number; it is not independently visually certified. Transcriptions cover the transcluded body and omit some headers/footers. Source proofreading status is preserved, not upgraded to Bayanat gold. These pairs overlap text already in `community_text`; do not count them as extra transcriptions. See [reference provenance](metadata/reference-pairs-summary.json) and [usage](docs/USAGE.md).

`train` is a loading split. There is no claimed held-out benchmark. Related editions and cross-source mirrored scans may overlap; use document/work-level leakage audits for your own evaluation.

## Source selection and rights

The main image tier uses ACO's institutional public-domain assertion and a conservative pre-1931 edition filter with no contradictory retrieved rights evidence. This is not independent worldwide legal clearance. Source institutions, exact statements and source links remain in the rows. Underlying scans/works retain their existing rights. Reference images and included originals retain their individual CC BY-SA 4.0 file terms and credits. Bayanat's original annotation representation/docs are CC BY 4.0; adapted Wikisource community text is CC BY-SA 4.0, with attribution/share-alike requirements. Code is Apache-2.0. **There is no blanket CC0 license.** See [component-specific terms](DATA_LICENSE.md) and [attribution](ATTRIBUTION.md).

## Citation

```bibtex
@dataset{{bayanat_arabic_document_atlas_2026,
  title = {{Bayanat Arabic Document Atlas}},
  author = {{{{Bayanat Labs}}}},
  year = {{2026}},
  version = {{{s['version']}}},
  url = {{https://huggingface.co/datasets/{REPO}}}
}}
```

Also credit the original institution and, for community text, the row's Wikisource revision/history. Corrections and rights concerns: hello@bayanatlabs.com, including page ID and source URL.
'''

def publish(output, version, public):
    from publish_snapshot import publish as publish_verified_snapshot
    return publish_verified_snapshot(output, version, public)

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',required=True);p.add_argument('--books');p.add_argument('--community');p.add_argument('--references')
    p.add_argument('--version',default='v0.1.0');p.add_argument('--publish',action='store_true');p.add_argument('--public',action='store_true')
    p.add_argument('--publish-existing',action='store_true',help='Publish the preserved release directory without repackaging it')
    a=p.parse_args()
    if a.publish_existing:
        if not a.publish:p.error('--publish-existing requires --publish')
    else:
        if not a.books:p.error('--books is required when packaging')
        print(json.dumps(package(a.output,a.books,a.community,a.version,a.references)))
    if a.publish:publish(a.output,a.version,a.public)
