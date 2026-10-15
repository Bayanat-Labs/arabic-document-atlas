"""Engineering-only fixtures for durable builder state; never corpus observations."""
from __future__ import annotations
import argparse
import copy
import gzip
import hashlib
import importlib.util
import io
import json
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from PIL import Image

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('atlas_builder_under_test',ROOT/'pipeline/build_pages.py')
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)
from test_validate import write_native_pdf


class FixtureWorker:
    def __init__(self,text=False): self.text=text
    def run(self,path):
        words=[];lines=[]
        if self.text:
            words=[{'id':'w0','text':'اختبار','bbox':[10,10,100,50], 'bbox_method':'vision_approximate_character_range_word_precision',
                    'bbox_boundary_epsilon_clipped':False,'confidence':75.0,'confidence_basis':'line_candidate_uncalibrated','line_id':'l0'}]
            lines=[{'id':'l0','text':'اختبار','bbox':[10,10,100,50],'word_ids':['w0'],'order_index':0,
                    'order_method':'apple_vision_observation_order','confidence':75.0}]
        return {'words':words,'lines':lines,'page_text':'اختبار' if self.text else '',
                'engine':'Apple Vision VNRecognizeTextRequest','engine_revision':3,'operating_system':'independent fixture',
                'recognition_languages':['ar-SA','en-US'],'ocr_seconds':0.001}


def book_fixture():
    return {'source_book_id':'fixture-document','manifest_sha256':'a'*64,
            'manifest_url':'https://example.org/manifest.json','raw_manifest_path':'raw/fixture.json.gz',
            'source_canvas_count':1,'renderings':[{'id':'https://mc.dlib.nyu.edu/files/independent-fixture_hi.pdf'}],'rights_statement_raw':'Source-independent engineering fixture only',
            'manifest_state':'complete','institution_asserts_public_domain':True,
            'eligibility_proposal':'candidate_institution_asserted_pd_pre1931','language':['Arabic'],
            '_canvases':[{'id':'https://example.org/canvas/0','label':'0','width':400,'height':220,'image_service':'https://example.org/image/0'}],
            '_source_homepage':'https://example.org/book','_processing_config_sha256':'c'*64}


def fixture_row(directory,*,text=False):
    (directory/'private/scratch').mkdir(parents=True,exist_ok=True)
    pdf=directory/'independent-fixture.pdf';write_native_pdf(pdf)
    book=book_fixture()
    return b.process_page((book,str(pdf),b.sha_file(pdf),'https://example.org/fixture_hi.pdf',0,1),FixtureWorker(text),directory)


def variant(row,index,data):
    row=copy.deepcopy(row);row.update(b.image_fingerprints(data))
    row['source_canvas_id']=f'https://example.org/canvas/{index}'
    row['logical_page_id'],row['page_id']=b.canonical_ids('nyu_aco','fixture-document',row['source_canvas_id'],b.PROFILE)
    row['source_page_index']=row['source_pdf_page_index']=index
    row['source_pdf_page_count']=4
    row['image']={'bytes':data,'path':row['page_id']+'.png'}
    return row


class BuilderDurabilityTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.output=Path(self.tmp.name)
        self.db=b.connect_state(self.output)
    def tearDown(self):self.db.close();self.tmp.cleanup()

    def test_failed_book_preserves_manifest_pdf_and_metadata_bindings(self):
        book=book_fixture();b.bind_document(self.db,book,pdf_sha256='d'*64,pdf_url='https://example.org/book_hi.pdf',pages=94)
        b.record_document_error(self.db,book['source_book_id'],'failure after download')
        self.assertEqual(self.db.execute('SELECT pdf_sha256,manifest_sha256,pages,status FROM documents').fetchone(),('d'*64,'a'*64,94,'error'))
        b.bind_document(self.db,book,pdf_sha256='d'*64,pdf_url='https://example.org/book_hi.pdf',pages=94)
        with self.assertRaisesRegex(ValueError,'pdf_sha256'):b.bind_document(self.db,book,pdf_sha256='e'*64)
        changed=copy.deepcopy(book);changed['manifest_sha256']='f'*64
        with self.assertRaisesRegex(ValueError,'manifest_sha256'):b.bind_document(self.db,changed)
        self.assertEqual(self.db.execute('SELECT pdf_sha256 FROM documents').fetchone()[0],'d'*64)

    def test_cached_pdf_cannot_change_after_failure(self):
        book=book_fixture();book['renderings']=[{'id':'https://mc.dlib.nyu.edu/files/fixture_hi.pdf'}]
        cache=self.output/'private/pdf-cache';cache.mkdir()
        path=cache/(book['source_book_id']+'.pdf');path.write_bytes(b'%PDF-1.4\nfixture')
        b.bind_document(self.db,book,pdf_sha256=b.sha_file(path))
        b.record_document_error(self.db,book['source_book_id'],'late failure')
        path.write_bytes(b'%PDF-1.4\nchanged fixture')
        with self.assertRaisesRegex(ValueError,'Cached source PDF changed'):b.download_pdf(book,self.output,self.db)

    def test_fixed_arrow_schema_blank_then_text_and_nullable_reference(self):
        blank=fixture_row(self.output);text=fixture_row(self.output,text=True)
        one=self.output/'blank.parquet';two=self.output/'text.parquet'
        b.write_parquet([blank],b.page_schema(),one,image_feature=True)
        b.write_parquet([text],b.page_schema(),two,image_feature=True)
        self.assertTrue(pq.read_schema(one).equals(pq.read_schema(two),check_metadata=True))
        loaded=pq.ParquetFile(one).read().to_pylist()[0]
        self.assertEqual(loaded['words'],[]);self.assertEqual(loaded['lines'],[])
        self.assertIsNone(loaded['reference_text']);self.assertIsNone(loaded['mean_word_confidence'])
        loaded_text=pq.ParquetFile(two).read().to_pylist()[0]
        self.assertEqual(loaded_text['words'][0]['text'],'اختبار')
        self.assertEqual(loaded_text['image']['bytes'],text['image']['bytes'])
        self.assertEqual(loaded_text['renderer'],'pypdfium2')
        self.assertEqual(loaded_text['split'],'train')
        # Actual library integration: metadata must survive field-nullability equality.
        from datasets import load_dataset,Image as HFImage
        stream=load_dataset('parquet',data_files=str(two),split='train',streaming=True)
        self.assertIsInstance(stream.features['image'],HFImage)
        decoded=next(iter(stream))
        self.assertEqual(decoded['image'].size,(text['width'],text['height']))
        self.assertEqual(decoded['words'][0]['id'],'w0')
        self.assertIsNone(decoded['reference_text'])
        bad=copy.deepcopy(blank);bad['surprise']='no silent schema drift'
        with self.assertRaisesRegex(ValueError,'fields differ'):b.write_parquet([bad],b.page_schema(),self.output/'bad.parquet')

    def test_pixel_dedup_retains_first_with_full_lineage_and_resume_indices(self):
        row=fixture_row(self.output)
        image=Image.new('RGB',(400,220),'white');image.putpixel((7,8),(22,33,44))
        one=io.BytesIO();two=io.BytesIO();image.save(one,format='PNG',compress_level=1);image.save(two,format='PNG',compress_level=9)
        self.assertNotEqual(one.getvalue(),two.getvalue())
        first=variant(row,0,one.getvalue());second=variant(row,1,two.getvalue())
        result=b.stage_shard([first,second],self.output,self.db)
        self.assertEqual(result,{'retained':1,'duplicates':1})
        self.assertEqual(self.db.execute('SELECT count(*) FROM pages').fetchone()[0],1)
        self.assertEqual(self.db.execute('SELECT page_index FROM occurrences ORDER BY page_index').fetchall(),[(0,),(1,)])
        lineage=pq.ParquetFile(self.output/'data/duplicate_occurrences/shard-000000.parquet').read().to_pylist()[0]
        self.assertEqual(lineage['retained_page_id'],first['page_id'])
        self.assertEqual(lineage['source_canvas_id'],second['source_canvas_id'])
        self.assertNotEqual(lineage['image_sha256'],lineage['retained_image_sha256'])
        self.assertEqual(lineage['source_pdf_sha256'],second['source_pdf_sha256'])
        third=variant(row,2,one.getvalue());b.stage_shard([third],self.output,self.db)
        self.assertEqual(self.db.execute('SELECT count(*) FROM pages').fetchone()[0],1)
        self.assertEqual(self.db.execute('SELECT count(*) FROM occurrences').fetchone()[0],3)
        with self.assertRaisesRegex(ValueError,'already committed'):b.stage_shard([second],self.output,self.db)
        b.verify_staged_shards(self.output,self.db)

    def test_uncommitted_or_corrupted_shard_blocks_resume(self):
        row=fixture_row(self.output);b.stage_shard([row],self.output,self.db)
        b.verify_staged_shards(self.output,self.db)
        orphan=self.output/'data/pages/orphan.parquet';orphan.write_bytes(b'not committed')
        with self.assertRaisesRegex(ValueError,'Uncommitted'):b.verify_staged_shards(self.output,self.db)
        orphan.unlink();path=self.output/'data/pages/shard-000000.parquet'
        path.write_bytes(path.read_bytes()+b'changed')
        with self.assertRaisesRegex(ValueError,'SHA-256 mismatch'):b.verify_staged_shards(self.output,self.db)

    def test_partial_native_response_times_out_and_process_restarts(self):
        script=self.output/'worker';script.write_text('#!'+sys.executable+'\nimport sys,time,json\nfor line in sys.stdin:\n p=json.loads(line)["path"]\n if p=="hang":\n  sys.stdout.write(\'{"ok":\');sys.stdout.flush();time.sleep(20)\n else:\n  print(json.dumps({"ok":True,"result":{"fixture":True}}),flush=True)\n')
        script.chmod(0o755);worker=b.VisionWorker(script,timeout_seconds=3)
        try:
            # Complete startup first so the short deadline targets a real partial response.
            self.assertEqual(worker.run('warmup'),{'fixture':True})
            worker.timeout_seconds=.15
            start=time.monotonic()
            with self.assertRaises(TimeoutError):worker.run('hang')
            self.assertLess(time.monotonic()-start,3)
            # Startup latency under a busy production run is unrelated to the timeout check.
            worker.timeout_seconds=3
            self.assertEqual(worker.run('success'),{'fixture':True})
            self.assertGreaterEqual(len(worker.processes),2)
        finally:worker.close()

    def test_metadata_whitespace_preflight_and_strict_identifiers(self):
        book=book_fixture();book['rights_statement_raw']='Institutional statement\nwith preserved lines\r\nand\ttabs.'
        b.preflight_metadata(book)
        row=fixture_row(self.output);row['source_rights']=book['rights_statement_raw']
        before=row['source_rights'];report=b.validate_page(row,image_bytes=row['image']['bytes'])
        self.assertTrue(report['valid'],report['errors']);self.assertEqual(row['source_rights'],before)
        book['source_book_id']='forbidden\nidentifier'
        with self.assertRaisesRegex(ValueError,'Metadata preflight'):b.preflight_metadata(book)
        book=book_fixture();book['rights_statement_raw']='forbidden\x00statement'
        with self.assertRaisesRegex(ValueError,'Metadata preflight'):b.preflight_metadata(book)

    def test_partial_pdf_eviction_requires_verified_outputs_and_preserves_binding(self):
        book=book_fixture();docid=book['source_book_id'];cache=self.output/'private/pdf-cache';cache.mkdir()
        path=cache/(docid+'.pdf');path.write_bytes(b'%PDF-1.4 independent engineering fixture')
        digest=b.sha_file(path);b.bind_document(self.db,book,pdf_sha256=digest)
        self.db.execute("UPDATE documents SET status='partial'");self.db.commit()
        self.db.execute("INSERT INTO shards VALUES ('data/pages/pending.parquet','x',0,0,0,NULL,'pages')");self.db.commit()
        self.assertFalse(b.evict_verified_pdf(path,self.output,self.db,docid));self.assertTrue(path.exists())
        self.db.execute('UPDATE shards SET uploaded=1');self.db.commit()
        self.assertTrue(b.evict_verified_pdf(path,self.output,self.db,docid));self.assertFalse(path.exists())
        self.assertEqual(self.db.execute('SELECT pdf_sha256 FROM documents').fetchone()[0],digest)
        ledger=[json.loads(x) for x in (self.output/'private/input-evictions.jsonl').read_text().splitlines()]
        self.assertEqual([x['state'] for x in ledger],['planned','deleted'])
        self.assertTrue(all(x['pdf_sha256']==digest for x in ledger))
        path.write_bytes(b'%PDF-1.4 changed input')
        with self.assertRaisesRegex(ValueError,'source hash mismatch'):b.evict_verified_pdf(path,self.output,self.db,docid)

    def test_inventory_addition_allowed_but_processing_binary_change_rejected(self):
        # Freeze before creating a build database, as production does.
        output=self.output/'other';output.mkdir()
        binary=self.output/'fixture-binary';binary.write_bytes(b'independent fixture executable bytes')
        inventory=self.output/'inventory.jsonl.gz'
        with gzip.open(inventory,'wt') as f:f.write('{}\n')
        args=argparse.Namespace(vision_binary=str(binary),books=str(inventory))
        digest=b.freeze_processing_config(args,output)
        with gzip.open(inventory,'wt') as f:f.write('{}\n{}\n')
        self.assertEqual(digest,b.freeze_processing_config(args,output))
        self.assertEqual(len(json.loads((output/'private/input-inventories.json').read_text())),2)
        binary.write_bytes(b'changed executable')
        with self.assertRaisesRegex(ValueError,'Frozen processing configuration'):b.freeze_processing_config(args,output)

if __name__=='__main__':unittest.main()
