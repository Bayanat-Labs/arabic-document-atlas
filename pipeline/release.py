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
from pathlib import Path
import shutil
import sqlite3
import subprocess
import time
import pyarrow.parquet as pq
from huggingface_hub import HfApi
from build_pages import book_record_hash

SOURCE = Path(__file__).resolve().parents[1]
REPO = 'Bayanat-Labs/arabic-document-atlas'

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
    allowed={'.py','.swift','.md','.json','.txt','.html','.css','.js','.cff','.yml','.yaml'}
    for folder in ['pipeline','quality','acquisition','audit','docs','tests','explorer']:
        for file in sorted((SOURCE/folder).rglob('*')):
            relative=file.relative_to(SOURCE)
            if (not file.is_file() or file.suffix not in allowed
                    or any(p in {'private','__pycache__','.cache','images','samples'} for p in relative.parts)):
                continue
            target=destination/relative;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(file,target)
    for name in ['README.md','requirements.txt','DATA_LICENSE.md','ATTRIBUTION.md','LICENSE','CITATION.cff','.gitignore','.github/workflows/validate.yml']:
        (destination/name).parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(SOURCE/name,destination/name)

def package(output, books, community, version):
    output=Path(output).resolve();final_release=output/'release'
    if final_release.exists():
        raise ValueError('Release directory already exists. Preserve it; use a new snapshot build or explicitly archive the task-owned old package before repackaging.')
    state=json.loads((output/'private/publication.json').read_text())
    if state.get('dataset_version')==version:
        raise ValueError('This immutable version was already staged; do not regenerate its local evidence')
    release=output/f'.release-staging-{time.time_ns()}';release.mkdir()
    db=sqlite3.connect(f'file:{output/"private/build.sqlite"}?mode=ro',uri=True);db.row_factory=sqlite3.Row
    db.execute('BEGIN')
    if db.execute("SELECT count(*) FROM documents WHERE status='processing'").fetchone()[0]:
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
        files=sorted((community/'data/community_text').glob('*.parquet'))
        if not files:raise ValueError('Community package has no Parquet')
        schema=pq.read_schema(files[0])
        for file in files:
            if not pq.read_schema(file).equals(schema):raise ValueError('Community schemas differ')
            community_count+=pq.ParquetFile(file).metadata.num_rows;community_bytes+=file.stat().st_size
        shutil.copytree(community,release,dirs_exist_ok=True)
    summary={
        'name':'Bayanat Arabic Document Atlas','version':version,'snapshot_at':now,
        'unique_image_pages':count,'source_documents_with_images':len(documents),'source_documents_with_any_occurrence':len(sourceids),
        'machine_word_records':words,'mechanical_screen_pass_pages':curated,
        'exact_pixel_duplicate_occurrences_removed':duplicate_count,
        'page_shards':len(page_shards),'page_parquet_bytes':sum(s['bytes'] for s in page_shards),
        'community_text_records':community_count,'community_text_parquet_bytes':community_bytes,
        'community_text_contains_image_bytes':False,'synthetic_image_pages':0,
        'manually_verified_bbox_pages':0,'held_out_evaluation_split':False,
        'recorded_page_failure_attempts':failures,'remote_page_sha256_verified':True,
        'page_data_commit_before_documentation':remote.sha,
        'counts_are_not_additive':'Community text records, word records and duplicate occurrences are not additional unique scan images.',
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
        'scope':'Every retained image was decoded and checked for dimensions, byte/pixel hashes, word/line geometry, text reconstruction and provenance during the build. Every released remote page shard SHA-256 and byte length was rechecked when packaging.',
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
    shutil.copytree(SOURCE/'docs',release/'docs',dirs_exist_ok=True)
    (release/'README.md').write_text(dataset_card(summary))
    (release/'.gitattributes').write_text('*.parquet filter=lfs diff=lfs merge=lfs -text\n*.gz filter=lfs diff=lfs merge=lfs -text\n')
    # Checksums cover all release files and already-verified page shards without
    # re-downloading the page images onto this disk.
    checks={s['path']:s['sha256'] for s in shards}
    for file in release.rglob('*'):
        if file.is_file() and file.name!='SHA256SUMS':checks[str(file.relative_to(release))]=sha(file)
    (release/'SHA256SUMS').write_text(''.join(f'{v}  {k}\n' for k,v in sorted(checks.items())))
    db.close();release.replace(final_release)
    return summary

def dataset_card(s):
    n=s['unique_image_pages'];category='1K<n<10K' if n<10000 else '10K<n<100K' if n<100000 else '100K<n<1M' if n<1000000 else '1M<n<10M'
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
{community}{duplicates}---

# Bayanat Arabic Document Atlas

**{n:,} unique real scanned page images · {s['machine_word_records']:,} machine word records · {s['source_documents_with_images']:,} source books.**

A visual Arabic document resource with JPEG page bytes, machine OCR, approximate word localization and line order, exact source PDF/manifest hashes, edition metadata, duplicate lineage and mechanical quality flags. Original scans and cataloging come from Arabic Collections Online and its supplying institutions. Bayanat builds the normalized visual representation and auditable processing pipeline.

[Explore actual page examples](https://huggingface.co/spaces/{REPO}) · [Source and pipeline](https://github.com/{REPO}) · [Research and release record](https://www.bayanatlabs.com/open/arabic-document-atlas) · [Usage](docs/USAGE.md) · [Rights and attribution](DATA_LICENSE.md)

## Snapshot {s['version']}

| Component | Records | What a row means |
|---|---:|---|
| `pages` | {n:,} | One retained unique decoded raster, with image bytes and machine OCR |
| `community_text` | {s['community_text_records']:,} | A separate attributed Wikisource revision/transclusion body; no image bytes |
| Machine word annotations | {s['machine_word_records']:,} | Approximate word guides inside the page rows, not extra pages |

The image shards occupy {s['page_parquet_bytes']/1024**3:,.2f} GiB. Exact decoded-pixel duplicate occurrences removed: {s['exact_pixel_duplicate_occurrences_removed']:,}. Mechanical screen passed: {s['mechanical_screen_pass_pages']:,} pages. That screen uses text amount/script and encoding checks; it is not measured OCR accuracy. Covers and sparse pages may remain in the full image tier with flags. The snapshot has no synthetic images and no human-verified bounding boxes.

Counts describe the pinned snapshot, not the full upstream catalog. Source canvases, variants, community-text records and word records are not added to the unique-image total. See [release summary](metadata/release-summary.json), [integrity report](reports/integrity.json), [shard manifest](metadata/shards.json) and `SHA256SUMS`. Pin `revision="{s['version']}"` for reproducibility; development on `main` may continue.

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

`train` is a loading split. There is no claimed held-out benchmark. Related editions and cross-source mirrored scans may overlap; use document/work-level leakage audits for your own evaluation.

## Source selection and rights

The image tier uses ACO's institutional public-domain assertion and a conservative pre-1931 edition filter with no contradictory retrieved rights evidence. This is not independent worldwide legal clearance. Source institutions, exact statements and source links remain in the rows. Underlying scans/works retain their existing rights. Bayanat's original annotation representation/docs are CC BY 4.0; adapted Wikisource community text is CC BY-SA 4.0, with attribution/share-alike requirements. Code is Apache-2.0. **There is no blanket CC0 license.** See [component-specific terms](DATA_LICENSE.md) and [attribution](ATTRIBUTION.md).

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
    output=Path(output).resolve();release=output/'release'
    summary=json.loads((release/'metadata/release-summary.json').read_text())
    if summary['version']!=version:raise ValueError('Version mismatch')
    statepath=output/'private/publication.json';state=json.loads(statepath.read_text())
    if state.get('repo_id')!=REPO or not state.get('dataset_created'):raise ValueError('Builder-owned state required')
    api=HfApi()
    refs=api.list_repo_refs(REPO,repo_type='dataset')
    existing=next((r for r in refs.tags if r.name==version),None)
    if existing:
        if state.get('dataset_version')!=version or state.get('dataset_commit')!=existing.target_commit:
            raise ValueError('Version tag already exists outside this completed publication state')
        if public:
            api.update_repo_settings(REPO,repo_type='dataset',private=False)
            state.update(private=False,dataset_public=True);dump(statepath,state)
        print(json.dumps({'stage':'dataset_snapshot_already_published','commit':existing.target_commit,'version':version,'public':public}));return
    info=api.upload_folder(REPO,repo_type='dataset',folder_path=release,parent_commit=summary['page_data_commit_before_documentation'],commit_message=f'Arabic Document Atlas {version}: verified snapshot and documentation')
    # Avoid silently moving an existing immutable version tag.
    # A single caller owns publication; an external tag appearing here is not
    # overwritten. The immutable snapshot is always the verified commit above.
    api.create_tag(REPO,repo_type='dataset',tag=version,revision=info.oid)
    state.update(dataset_commit=info.oid,dataset_version=version,dataset_staged=True)
    if public:
        api.update_repo_settings(REPO,repo_type='dataset',private=False);state['private']=False;state['dataset_public']=True
    dump(statepath,state);print(json.dumps({'stage':'dataset_snapshot','commit':info.oid,'version':version,'public':public}))

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',required=True);p.add_argument('--books',required=True);p.add_argument('--community')
    p.add_argument('--version',default='v0.1.0');p.add_argument('--publish',action='store_true');p.add_argument('--public',action='store_true')
    a=p.parse_args();print(json.dumps(package(a.output,a.books,a.community,a.version)))
    if a.publish:publish(a.output,a.version,a.public)
