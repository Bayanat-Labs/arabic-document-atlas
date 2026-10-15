"""Extract a small, traceable static example viewer from verified page shards."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile
import requests
import pyarrow.parquet as pq

SOURCE=Path(__file__).resolve().parents[1]
REPO='Bayanat-Labs/arabic-document-atlas'

def package(output,maximum=24):
    output=Path(output).resolve();summary=json.loads((output/'release/metadata/release-summary.json').read_text())
    state=json.loads((output/'private/publication.json').read_text());revision=state['dataset_commit']
    db=sqlite3.connect(f'file:{output/"private/build.sqlite"}?mode=ro',uri=True);db.row_factory=sqlite3.Row
    # Deliberate examples: first text-bearing page and two spread page positions
    # per document, up to eight source documents. This is not a random audit set.
    ids=[]
    docs=[r[0] for r in db.execute('SELECT document_id FROM pages GROUP BY document_id ORDER BY min(shard_path) LIMIT 8')]
    for doc in docs:
        candidates=db.execute('SELECT page_id,shard_path,page_index FROM pages WHERE document_id=? AND words>=30 ORDER BY page_index',(doc,)).fetchall()
        if not candidates:continue
        for index in sorted({0,len(candidates)//2,len(candidates)-1}):ids.append(dict(candidates[index]))
    ids=ids[:maximum]
    if not ids:raise ValueError('No text-bearing actual page examples')
    expected={r['page_id'] for r in ids};needed={r['shard_path'] for r in ids}
    shards={r['path']:dict(r) for r in db.execute('SELECT * FROM shards')}
    destination=output/'explorer';destination.mkdir(exist_ok=True)
    for filename in ['index.html','app.js','style.css','README.md']:shutil.copy2(SOURCE/'explorer'/filename,destination/filename)
    (destination/'images').mkdir(exist_ok=True);samples=[]
    for relative in sorted(needed):
        local=output/relative;temporary=None
        if not local.exists():
            if shutil.disk_usage(output).free<4*1024**3:raise RuntimeError('Local storage floor reached')
            temporary=tempfile.TemporaryDirectory(dir=output/'private',prefix='example-shard-')
            local=Path(temporary.name)/'shard.parquet';size=0;digest=hashlib.sha256()
            with requests.get(f'https://huggingface.co/datasets/{REPO}/resolve/{revision}/{relative}',headers=_headers(),stream=True,timeout=(20,90)) as r:
                r.raise_for_status()
                with local.open('wb') as f:
                    for block in r.iter_content(1024**2):
                        size+=len(block)
                        if size>shards[relative]['bytes']:raise ValueError('Example download exceeds manifest size')
                        f.write(block);digest.update(block)
            if size!=shards[relative]['bytes'] or digest.hexdigest()!=shards[relative]['sha256']:raise ValueError('Example shard hash mismatch')
        try:
            digest=hashlib.sha256()
            with local.open('rb') as f:
                for block in iter(lambda:f.read(8*1024**2),b''):digest.update(block)
            if local.stat().st_size!=shards[relative]['bytes'] or digest.hexdigest()!=shards[relative]['sha256']:
                raise ValueError('Local example shard hash/size mismatch')
            for batch in pq.ParquetFile(local).iter_batches(batch_size=16):
                for row in batch.to_pylist():
                    if row['page_id'] not in expected:continue
                    image=row.pop('image');name=f'images/{row["page_id"]}.jpg';(destination/name).write_bytes(image['bytes'])
                    row['sample_image']=name;samples.append(row)
        finally:
            if temporary:temporary.cleanup()
    if {p['page_id'] for p in samples}!=expected:raise ValueError('Missing selected examples')
    order={r['page_id']:i for i,r in enumerate(ids)};samples.sort(key=lambda r:order[r['page_id']])
    (destination/'samples.json').write_text(json.dumps({'summary':summary,'dataset_revision':revision,'selection':'Deliberately selected text-bearing pages spread across up to eight source documents; not random or representative.','pages':samples},ensure_ascii=False))
    for name in ['DATA_LICENSE.md','ATTRIBUTION.md','LICENSE']:shutil.copy2(SOURCE/name,destination/name)
    print(json.dumps({'examples':len(samples),'source_documents':len(docs),'output':str(destination),'dataset_revision':revision}))
    return destination

def _headers():
    # Only sent to the exact initial Hugging Face URL; requests strips auth on
    # cross-host redirects. Tokens are never serialized into public samples.
    from huggingface_hub import get_token
    token=get_token();return {'Authorization':'Bearer '+token} if token else {}

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',required=True);p.add_argument('--maximum',type=int,default=24)
    a=p.parse_args();package(a.output,a.maximum)
