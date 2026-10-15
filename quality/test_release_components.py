"""Original two-page fixtures and mocked remote metadata; no network or corpus rows."""
import copy
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
from types import SimpleNamespace as NS
import unittest
import zlib

import pyarrow as pa
import pyarrow.parquet as pq

PIPELINE=Path(__file__).resolve().parents[1]/'pipeline';sys.path.insert(0,str(PIPELINE))
import release_components as c
from test_reference_pairs import two_page_pdf,row as original_row
b=c.references


def write_json(path,value):
 path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value))


def write_jsonl(path,rows):
 path.parent.mkdir(parents=True,exist_ok=True)
 with gzip.open(path,'wt') as stream:
  for row in rows:stream.write(json.dumps(row)+'\n')


def fixture(root):
 source=root/'references';(source/'private/download-proofs').mkdir(parents=True)
 pdf=root/'original.pdf';two_page_pdf(pdf);row=original_row(pdf);row['scan_filename']='Original engineering fixture.pdf'
 config={'max_pdf_bytes':1000000,'profile':b.PROFILE,'requires_source_vrt_holder_permission_assertion':True}
 config_hash=hashlib.sha256(b.canonical(config).encode()).hexdigest()
 pair=b.make_pair(row,pdf,b.verify_pdf(pdf,row),config_hash)
 relative=f'data/reference_pairs/{row["scan_file_sha1"]}.parquet';parquet=source/relative;parquet.parent.mkdir(parents=True)
 pq.write_table(pa.Table.from_pylist([pair],schema=b.SCHEMA),parquet)
 manifest={'path':relative,'rows':1,'bytes':parquet.stat().st_size,'sha256':c.digest(parquet)}
 db=sqlite3.connect(source/'private/state.sqlite');db.execute('CREATE TABLE candidates(id TEXT,payload BLOB)');db.execute('INSERT INTO candidates VALUES (?,?)',(row['record_id'],zlib.compress(b.canonical(row).encode())));db.commit();db.close()
 write_json(source/'private/processing-config.json',{'sha256':config_hash,'config':config})
 write_json(source/'metadata/reference-pairs-processing-config.json',{'sha256':config_hash,'config':config})
 write_json(source/'reports/reference-pairs-build.json',{'status':'built_pending_validation','rows':1,'processing_config_sha256':config_hash,'data_files':[manifest]})
 write_json(source/'metadata/reference-pairs-summary.json',{'rows':1,'validation_passed':False})
 write_json(source/f'private/download-proofs/{row["scan_file_sha1"]}.json',{'source_url_raw':row['scan_file_url'],'download_url':b.download_url(row['scan_file_url']),'sha1':row['scan_file_sha1'],'sha256':b.sha(pdf),'bytes':pdf.stat().st_size,'retrieved_at':'2026-10-15T00:00:00Z','private_path':'DO_NOT_EXPORT_PRIVATE_FIXTURE','Authorization':'DO_NOT_EXPORT_PRIVATE_FIXTURE','response_headers':{'Content-Type':'application/pdf','Set-Cookie':'DO_NOT_EXPORT_PRIVATE_FIXTURE','Authorization':'DO_NOT_EXPORT_PRIVATE_FIXTURE'}})
 native=b.audit_native_rights_notices(pdf);write_json(source/f'private/native-notice-proofs/{row["scan_file_sha1"]}.json',{'source_pdf_sha1':row['scan_file_sha1'],'source_pdf_sha256':b.sha(pdf),**native,'debug_private_path':'DO_NOT_EXPORT_PRIVATE_FIXTURE'})
 originals=source/'private/verified-inputs';originals.mkdir();shutil.copy2(pdf,originals/f'{row["scan_file_sha1"]}.pdf')
 evidence={'scan_filename':row['scan_filename'],'source_file_info':{'sha1':row['scan_file_sha1'],'extmetadata':{'Permission':{'value':'PermissionTicket: source assertion in engineering fixture only'},'LicenseUrl':{'value':row['scan_license_url']},'Credit':{'value':'Original fixture author'}}}}
 write_jsonl(source/'metadata/reference-pairs-source-files.jsonl.gz',[evidence])
 write_jsonl(source/'metadata/reference-pairs-source-descriptions.jsonl.gz',[{'page':{'title':'File:'+row['scan_filename']},'content':'Engineering fixture source credit/permission only'}])
 index={'page_id':pair['page_id'],'reference_pair_id':pair['reference_pair_id'],'pixel_sha256':pair['pixel_sha256'],'image_sha256':pair['image_sha256'],'document_id':pair['document_id'],'source_pdf_sha1':pair['source_pdf_sha1'],'source_pdf_sha256':pair['source_pdf_sha256'],'source_file_sha1':pair['source_pdf_sha1'],'shard_path':relative}
 write_jsonl(source/'metadata/reference-pairs-index.jsonl.gz',[index])
 return NS(source=source,pdf=pdf,pair=pair,index=index,manifest=manifest)


class ReferenceComponentTests(unittest.TestCase):
 def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name).resolve();self.f=fixture(self.root)
 def tearDown(self):self.tmp.cleanup()
 def call(self,remote=None):return c.validate_copy_reference(self.f.source,self.root/'release',remote or {})
 def remote(self):return {self.f.manifest['path']:NS(size=self.f.manifest['bytes'],lfs=NS(sha256=self.f.manifest['sha256']))}

 def test_initial_copy_preserves_source_pdfs_and_rights_but_does_not_claim_remote_verification(self):
  (self.f.source/'private/credentials.json').write_text('DO_NOT_EXPORT_PRIVATE_FIXTURE')
  (self.f.source/'metadata/unknown-secret.json').write_text('DO_NOT_EXPORT_PRIVATE_FIXTURE')
  result=self.call();self.assertEqual(result['rows'],1);self.assertEqual(result['source_document_count'],1)
  self.assertEqual(result['pixel_hashes'],[self.f.pair['pixel_sha256']]);self.assertFalse(result['remote_data_sha256_verified'])
  self.assertEqual(result['source_pdf_bytes'],self.f.pdf.stat().st_size)
  released=self.root/'release';self.assertFalse((released/'private').exists());self.assertFalse((released/'metadata/unknown-secret.json').exists())
  original=released/result['source_pdf_files'][0]['path'];self.assertEqual(original.read_bytes(),self.f.pdf.read_bytes())
  for p in (released/'metadata/reference-pairs-download-proofs').glob('*.json'):
   value=json.loads(p.read_text());self.assertNotIn('Set-Cookie',value['response_headers']);self.assertNotIn('Authorization',value['response_headers']);self.assertNotIn('private_path',value)
  for p in released.rglob('*.json'):self.assertNotIn('DO_NOT_EXPORT_PRIVATE_FIXTURE',p.read_text())
  with gzip.open(released/'metadata/reference-pairs-source-files.jsonl.gz','rt') as stream:self.assertIn('PermissionTicket',stream.read())

 def test_existing_remote_requires_exact_path_hash_and_size(self):
  remote=self.remote();self.assertTrue(self.call(remote)['remote_data_sha256_verified'])
  shutil.rmtree(self.root/'release')
  for case in ('hash','size','partial_or_extra'):
   with self.subTest(case=case):
    remote=self.remote()
    if case=='hash':remote[self.f.manifest['path']].lfs.sha256='bad'
    if case=='size':remote[self.f.manifest['path']].size+=1
    if case=='partial_or_extra':remote['data/reference_pairs/unexpected.parquet']=NS(size=1,lfs=NS(sha256='bad'))
    with self.assertRaisesRegex(ValueError,'remote reference'):self.call(remote)
    self.assertFalse((self.root/'release').exists())

 def test_public_index_change_missing_or_duplicate_fails(self):
  path=self.f.source/'metadata/reference-pairs-index.jsonl.gz'
  for rows in ([{**self.f.index,'document_id':'wrong source'}],[],[self.f.index,self.f.index]):
   with self.subTest(rows=len(rows)):
    write_jsonl(path,rows)
    with self.assertRaisesRegex(ValueError,'public index'):self.call()
    self.assertFalse((self.root/'release').exists())

 def test_retained_original_mutation_and_symlink_are_rejected(self):
  original=self.f.source/f'private/verified-inputs/{self.f.pair["source_pdf_sha1"]}.pdf'
  original.write_bytes(original.read_bytes()+b'changed bytes')
  with self.assertRaisesRegex(ValueError,'original PDF hash'):self.call()
  original.unlink();original.symlink_to(self.f.pdf)
  with self.assertRaisesRegex(ValueError,'unsafe reference component'):self.call()

 def test_notice_conflict_or_public_config_mismatch_fails_before_copy(self):
  path=self.f.source/'metadata/reference-pairs-processing-config.json';value=json.loads(path.read_text());value['sha256']='changed';write_json(path,value)
  with self.assertRaisesRegex(ValueError,'configuration differs'):self.call()
  write_json(path,json.loads((self.f.source/'private/processing-config.json').read_text()))
  notice_path=self.f.source/f'private/native-notice-proofs/{self.f.pair["source_pdf_sha1"]}.json';notice=json.loads(notice_path.read_text());notice['restrictive_cc_notice_found']=True;write_json(notice_path,notice)
  with self.assertRaisesRegex(ValueError,'notice proof'):self.call()

 def test_unknown_parquet_and_existing_destination_cannot_be_silently_copied(self):
  extra=self.f.source/'data/reference_pairs/unlisted.parquet';extra.write_bytes(b'not in manifest')
  with self.assertRaisesRegex(ValueError,'actual local Parquet set'):self.call()
  extra.unlink();self.call()
  with self.assertRaisesRegex(ValueError,'already exists'):self.call()

 def test_sanitized_proof_destination_symlink_cannot_escape_release(self):
  outside=self.root/'outside';outside.mkdir()
  dest=self.root/'release/metadata';dest.mkdir(parents=True)
  (dest/'reference-pairs-download-proofs').symlink_to(outside,target_is_directory=True)
  with self.assertRaisesRegex(ValueError,'Symlink in release destination'):self.call()
  self.assertEqual(list(outside.iterdir()),[])

if __name__=='__main__':unittest.main()
