"""Validate and copy the explicitly public reference-pair component; no uploads."""
from __future__ import annotations
import gzip
import hashlib
import json
from pathlib import Path,PurePosixPath
import re
import shutil
import sys

import pyarrow.parquet as pq

sys.path.insert(0,str(Path(__file__).resolve().parent))
import build_reference_pairs as references

PUBLIC_FILES=(
 'metadata/reference-pairs-index.jsonl.gz','metadata/reference-pairs-processing-config.json',
 'metadata/reference-pairs-source-files.jsonl.gz','metadata/reference-pairs-source-descriptions.jsonl.gz',
 'metadata/reference-pairs-summary.json','reports/reference-pairs-build.json','reports/reference-pairs-validation.json',
)
INDEX_FIELDS=('page_id','reference_pair_id','pixel_sha256','image_sha256','document_id','source_pdf_sha1','source_pdf_sha256')
HEADERS=('ETag','Last-Modified','Content-Type','Content-Length')


def digest(path,algorithm='sha256'):
 h=hashlib.new(algorithm)
 with Path(path).open('rb') as stream:
  for block in iter(lambda:stream.read(1024**2),b''):h.update(block)
 return h.hexdigest()


def checked_file(root,relative):
 value=PurePosixPath(relative)
 if value.is_absolute() or '..' in value.parts or '\\' in str(relative) or value.as_posix()!=str(relative):raise ValueError('Unsafe reference component path')
 path=root/relative
 if any((root/Path(*value.parts[:i])).is_symlink() for i in range(1,len(value.parts)+1)) or not path.resolve().is_relative_to(root) or not path.is_file():raise ValueError('Missing or unsafe reference component file: '+str(relative))
 return path


def remote_value(value,key):
 return value.get(key) if isinstance(value,dict) else getattr(value,key,None)


def validate_copy_reference(reference_dir,release_dir,remote_index):
 """Return verified local component facts, keeping remote verification explicit.

 Revalidates the original builder state/proofs. Copies a whitelist only after
 every index, PDF and existing remote reference file has passed verification.
 """
 source=Path(reference_dir).resolve();destination=Path(release_dir).resolve()
 if source==destination or source.is_relative_to(destination) or destination.is_relative_to(source):raise ValueError('Reference input and release output must be separate directories')
 for name in PUBLIC_FILES:
  if name!='reports/reference-pairs-validation.json' or (source/name).exists():checked_file(source,name)
 checked_file(source,'private/processing-config.json');checked_file(source,'private/state.sqlite')
 validation=references.validate_output(source)
 report=json.loads(checked_file(source,'reports/reference-pairs-build.json').read_text())
 summary=json.loads(checked_file(source,'metadata/reference-pairs-summary.json').read_text())
 if report.get('status')!='passed' or not summary.get('validation_passed') or validation.get('status')!='passed':raise ValueError('Reference component validation did not pass')
 config=json.loads(checked_file(source,'private/processing-config.json').read_text())
 public_config=json.loads(checked_file(source,'metadata/reference-pairs-processing-config.json').read_text())
 if config!=public_config or config['sha256']!=report['processing_config_sha256']:raise ValueError('Public/private reference configuration differs')
 entries=report['data_files'];paths=[x['path'] for x in entries]
 if not entries or len(set(paths))!=len(paths):raise ValueError('Empty or repeated reference data paths')
 if set(paths)!={p.relative_to(source).as_posix() for p in (source/'data/reference_pairs').rglob('*.parquet')}:raise ValueError('Reference manifest paths differ from actual local Parquet set')
 actual={};file_rows={};pixels=set();docids=set();parquet_bytes=0
 for entry in entries:
  if not re.fullmatch(r'data/reference_pairs/[a-zA-Z0-9_-]+\.parquet',entry['path']):raise ValueError('Reference data outside allowed prefix')
  path=checked_file(source,entry['path'])
  if digest(path)!=entry['sha256'] or path.stat().st_size!=entry['bytes']:raise ValueError('Reference manifest hash/size mismatch')
  observed=0
  for batch in pq.ParquetFile(path).iter_batches(batch_size=32,columns=list(INDEX_FIELDS)+['scan_file_size_bytes','scan_file_url','scan_file_page_count','scan_filename']):
   for row in batch.to_pylist():
    key=row['page_id']
    if key in actual or row['pixel_sha256'] in pixels:raise ValueError('Repeated reference identity or pixel hash')
    actual[key]={**{k:row[k] for k in INDEX_FIELDS},'source_file_sha1':row['source_pdf_sha1'],'shard_path':entry['path']}
    pixels.add(row['pixel_sha256']);docids.add(row['document_id']);file_rows.setdefault(row['source_pdf_sha1'],[]).append(row);observed+=1
  if observed!=entry['rows']:raise ValueError('Reference manifest row count mismatch')
  parquet_bytes+=entry['bytes']
 if len(actual)!=report['rows'] or len(actual)!=summary['rows'] or validation['rows_checked']!=len(actual):raise ValueError('Reference component total count mismatch')
 indexed={}
 with gzip.open(checked_file(source,'metadata/reference-pairs-index.jsonl.gz'),'rt',encoding='utf-8') as stream:
  for line in stream:
   row=json.loads(line);key=row.get('page_id')
   if key in indexed or key not in actual or row!=actual[key]:raise ValueError('Reference public index differs from actual Parquet rows')
   indexed[key]=row
 if indexed!=actual:raise ValueError('Reference public index is incomplete')
 remote_index=remote_index or {}
 remote_paths={name for name in remote_index if name.startswith('data/reference_pairs/') and name.endswith('.parquet')}
 if remote_paths and remote_paths!=set(paths):raise ValueError('Existing remote reference paths differ from supplied component')
 if remote_paths:
  for entry in entries:
   remote=remote_index[entry['path']];lfs=remote_value(remote,'lfs')
   if not lfs or remote_value(lfs,'sha256')!=entry['sha256'] or remote_value(remote,'size')!=entry['bytes']:raise ValueError('Existing remote reference hash/size mismatch')
 # Public source evidence and notices remain separate from the text license.
 copies=[(checked_file(source,name),name) for name in PUBLIC_FILES]
 copies += [(checked_file(source,entry['path']),entry['path']) for entry in entries]
 sanitized=[];pdfs=[];pdf_bytes=0
 for sha1,rows in sorted(file_rows.items()):
  if not re.fullmatch('[0-9a-f]{40}',sha1):raise ValueError('Invalid original source PDF identity')
  proof=json.loads(checked_file(source,f'private/download-proofs/{sha1}.json').read_text())
  first=rows[0]
  if any(row['source_pdf_sha256']!=first['source_pdf_sha256'] or row['scan_file_size_bytes']!=first['scan_file_size_bytes'] or row['scan_file_page_count']!=first['scan_file_page_count'] for row in rows):raise ValueError('Conflicting frozen source PDF facts')
  if proof['sha1']!=sha1 or proof['sha256']!=first['source_pdf_sha256'] or proof['bytes']!=first['scan_file_size_bytes']:raise ValueError('Original PDF download proof differs from rows')
  clean_url=references.download_url(proof.get('source_url_raw') or proof.get('download_url'))
  if clean_url!=proof['download_url'] or clean_url not in {references.download_url(row['scan_file_url']) for row in rows}:raise ValueError('Original download proof URL differs from source records')
  pdf=checked_file(source,f'private/verified-inputs/{sha1}.pdf')
  if pdf.stat().st_size!=proof['bytes'] or digest(pdf,'sha1')!=sha1 or digest(pdf)!=proof['sha256']:raise ValueError('Retained original PDF hash/size mismatch')
  if references.verify_pdf(pdf,{'scan_file_size_bytes':proof['bytes'],'scan_file_sha1':sha1,'scan_file_page_count':first['scan_file_page_count']})!=proof['sha256']:raise ValueError('Original PDF page-count/version mismatch')
  notice=json.loads(checked_file(source,f'private/native-notice-proofs/{sha1}.json').read_text())
  if notice.get('source_pdf_sha1')!=sha1 or notice.get('source_pdf_sha256')!=proof['sha256'] or notice.get('restrictive_cc_notice_found') is not False or notice.get('notice_matches')!=[]:raise ValueError('Conflicting original PDF rights-notice proof')
  clean_download={k:proof[k] for k in ('source_url_raw','download_url','sha1','sha256','bytes','retrieved_at','verification_mode') if k in proof}
  clean_download['response_headers']={k:proof.get('response_headers',{}).get(k) for k in HEADERS}
  clean_notice={k:notice[k] for k in ('source_pdf_sha1','source_pdf_sha256','first_pages_checked','native_text_characters','restrictive_cc_notice_found','notice_matches','scope') if k in notice}
  sanitized += [(f'metadata/reference-pairs-download-proofs/{sha1}.json',clean_download),(f'metadata/reference-pairs-native-notice-proofs/{sha1}.json',clean_notice)]
  relative=f'source/reference_pdfs/{sha1}.pdf';copies.append((pdf,relative));pdf_bytes+=proof['bytes']
  pdfs.append({'path':relative,'sha1':sha1,'sha256':proof['sha256'],'bytes':proof['bytes'],'source_pdf_page_count':first['scan_file_page_count']})
 # No private paths or arbitrary component files are copied. Refuse overwrite of
 # any already staged file; outer release staging owns rollback/retry policy.
 def target_path(name):
  value=PurePosixPath(name)
  if value.is_absolute() or '..' in value.parts:raise ValueError('Unsafe component destination')
  target=destination/name
  if any((destination/Path(*value.parts[:i])).is_symlink() for i in range(1,len(value.parts)+1)):raise ValueError('Symlink in release destination')
  if target.exists():raise ValueError('Reference destination already exists: '+name)
  return target
 for _,name in copies:target_path(name)
 for name,_ in sanitized:target_path(name)
 for origin,name in copies:
  target=target_path(name);target.parent.mkdir(parents=True,exist_ok=True)
  shutil.copy2(origin,target)
  if digest(origin)!=digest(target):raise ValueError('Reference component copy hash mismatch')
 for name,value in sanitized:
  target=target_path(name);target.parent.mkdir(parents=True,exist_ok=True);target.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')
 return {'rows':len(actual),'source_document_count':len(docids),'pixel_hashes':sorted(pixels),'document_ids':sorted(docids),'docids':sorted(docids),
         'data_files':entries,'parquet_bytes':parquet_bytes,'source_pdf_bytes':pdf_bytes,'source_pdf_files':pdfs,'validation_proof':validation,
         'remote_data_sha256_verified':bool(remote_paths),'source_pdf_originals_verified_locally':True}
