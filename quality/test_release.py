"""Publication guards tested with original engineering fixtures and no network.

These records, manifests, images and remote responses are invented test inputs.
They are never published or included in Arabic Document Atlas corpus counts.
"""
from __future__ import annotations
import copy
import gzip
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
from types import SimpleNamespace as NS
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from PIL import Image

PIPELINE=Path(__file__).resolve().parents[1]/'pipeline'
sys.path.insert(0,str(PIPELINE))

def module(name,filename):
    spec=importlib.util.spec_from_file_location(name,PIPELINE/filename);value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value

rel=module('atlas_release_under_test','release.py')
exp=module('atlas_explorer_under_test','package_explorer.py')


def write_json(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value))


def gz_json(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    with gzip.open(path,'wb') as stream:stream.write(value)


def source_fixture(path):
    path.mkdir()
    names=set(('README.md','requirements.txt','DATA_LICENSE.md','ATTRIBUTION.md','LICENSE','CITATION.cff','.gitignore','.github/workflows/validate.yml','docs/USAGE.md','pipeline/build_pages.py','quality/source_metadata_notes.json','explorer/index.html','explorer/app.js','explorer/style.css','explorer/README.md'))
    names.update(getattr(rel,'SOURCE_FILES',[]));names.update(getattr(rel,'OPTIONAL_SOURCE_FILES',[]))
    for name in sorted(names):
        target=path/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_text('Original engineering fixture: '+name+'\n')


class FakeAPI:
    """Narrow fake matching the real Hub API's keyword-only upload interface."""
    def __init__(self,siblings,head='fixture-head'):
        self.siblings=copy.deepcopy(siblings);self.head=head;self.tags=[];self.uploads=[];self.tag_calls=[];self.visibility=[];self.on_repo_info=None
    def repo_info(self,repo_id,*,repo_type=None,files_metadata=False):
        if self.on_repo_info:self.on_repo_info()
        return NS(sha=self.head,siblings=self.siblings)
    def list_repo_refs(self,repo_id,*,repo_type=None):return NS(tags=self.tags)
    def upload_folder(self,*,repo_id,folder_path,repo_type=None,parent_commit=None,commit_message=None):
        if parent_commit!=self.head:raise ValueError('stale remote HEAD rejected by parent_commit')
        self.uploads.append({'repo_id':repo_id,'parent_commit':parent_commit,'folder_path':str(folder_path)})
        self.head='fixture-published-commit';return NS(oid=self.head)
    def create_tag(self,repo_id,*,repo_type=None,tag,revision):
        if any(x.name==tag for x in self.tags):raise ValueError('existing tag')
        self.tag_calls.append((tag,revision));self.tags.append(NS(name=tag,target_commit=revision))
    def update_repo_settings(self,repo_id,*,repo_type=None,private=None):self.visibility.append(private)


def make_fixture(root):
    output=root/'build';private=output/'private';private.mkdir(parents=True)
    project=root/'source';source_fixture(project)
    db=sqlite3.connect(private/'build.sqlite');db.execute('PRAGMA journal_mode=WAL')
    db.executescript('''
    CREATE TABLE documents(document_id TEXT PRIMARY KEY,pdf_sha256 TEXT,pdf_url TEXT,pages INTEGER,status TEXT,error TEXT,manifest_sha256 TEXT,manifest_url TEXT,book_record_sha256 TEXT);
    CREATE TABLE shards(path TEXT PRIMARY KEY,sha256 TEXT,rows INTEGER,bytes INTEGER,uploaded INTEGER,commit_sha TEXT,kind TEXT);
    CREATE TABLE pages(page_id TEXT PRIMARY KEY,logical_id TEXT,document_id TEXT,page_index INTEGER,shard_path TEXT,pixel_sha256 TEXT,text_hash TEXT,split TEXT,words INTEGER,curated INTEGER,image_sha256 TEXT);
    CREATE TABLE occurrences(page_id TEXT PRIMARY KEY,logical_id TEXT,document_id TEXT,page_index INTEGER,retained_page_id TEXT,is_duplicate INTEGER,pixel_sha256 TEXT,lineage_shard TEXT);
    CREATE TABLE failures(document_id TEXT,page_index INTEGER,error TEXT,recorded_at TEXT);
    ''')
    image=Image.new('RGB',(80,120),'white');image.putpixel((3,4),(15,35,55));encoded=io.BytesIO();image.save(encoded,format='JPEG',quality=90);data=encoded.getvalue()
    page_id=hashlib.sha256(b'original engineering fixture page').hexdigest()
    row={'page_id':page_id,'document_id':'book-main','source_pdf_page_index':0,'source_pdf_page_count':1,
         'width':80,'height':120,'words_count':30,'lines_count':1,'reference_text':None,
         'image':{'bytes':data,'path':page_id+'.jpg'},
         'words':[{'id':f'w{i}','text':'fixture','bbox':[4,5,20,20],'line_id':'l0','confidence':75.0} for i in range(30)],
         'lines':[{'id':'l0','text':' '.join(['fixture']*30),'word_ids':[f'w{i}' for i in range(30)],'bbox':[4,5,20,20],'order_index':0}],
         'page_text':' '.join(['fixture']*30),'source_url':'https://example.org/original-fixture'}
    relative='data/pages/shard-000000.parquet';parquet=output/relative;parquet.parent.mkdir(parents=True);pq.write_table(pa.Table.from_pylist([row]),parquet)
    duplicate_path='data/duplicate_occurrences/shard-000000.parquet';duplicate=output/duplicate_path;duplicate.parent.mkdir(parents=True);pq.write_table(pa.Table.from_pylist([{'page_id':'duplicate-fixture','retained_page_id':page_id}]),duplicate)
    siblings=[]
    for filename,kind in [(relative,'pages'),(duplicate_path,'duplicate_occurrences')]:
        file=output/filename;sha=hashlib.sha256(file.read_bytes()).hexdigest();size=file.stat().st_size
        db.execute('INSERT INTO shards VALUES (?,?,?,?,?,?,?)',(filename,sha,1,size,1,'fixture-upload-commit',kind))
        siblings.append(NS(rfilename=filename,size=size,lfs=NS(sha256=sha)))
    db.execute('INSERT INTO pages VALUES (?,?,?,?,?,?,?,?,?,?,?)',(page_id,page_id,'book-main',0,relative,'pixel-fixture',None,'train',30,1,hashlib.sha256(data).hexdigest()))
    db.execute('INSERT INTO occurrences VALUES (?,?,?,?,?,?,?,?)',(page_id,page_id,'book-main',0,page_id,0,'pixel-fixture',relative))
    db.execute('INSERT INTO occurrences VALUES (?,?,?,?,?,?,?,?)',('duplicate-fixture','duplicate-fixture','book-duplicate',0,page_id,1,'pixel-fixture',duplicate_path))
    acquisition=root/'acquisition';acquisition.mkdir();books=[]
    for doc in ('book-main','book-duplicate'):
        manifest=json.dumps({'id':f'https://example.org/{doc}/manifest','label':'Original engineering fixture only'}).encode()
        digest=hashlib.sha256(manifest).hexdigest();raw=f'raw/{doc}.json.gz';gz_json(acquisition/raw,manifest)
        book={'source_book_id':doc,'manifest_sha256':digest,'manifest_url':f'https://example.org/{doc}/manifest','raw_manifest_path':raw,'provider':['Independent fixture provider'],'publication_year_tokens':[1900]}
        books.append(book);db.execute('INSERT INTO documents VALUES (?,?,?,?,?,?,?,?,?)',(doc,'f'*64,f'https://example.org/{doc}.pdf',1,'complete',None,digest,book['manifest_url'],rel.book_record_hash(book)))
    inventory=acquisition/'books.jsonl.gz'
    with gzip.open(inventory,'wt') as stream:
        for book in books:stream.write(json.dumps(book)+'\n')
    db.commit();db.close()
    write_json(private/'publication.json',{'repo_id':rel.REPO,'dataset_created':True,'private':True,'dataset_commit':'fixture-head'})
    write_json(private/'processing-config.json',{'processing_config_sha256':'fixture-config'})
    write_json(private/'input-inventories.json',[{'sha256':'fixture-inventory'}])
    write_json(output/'reports/coverage.json',{'counts':{'retained_unique_images':1,'machine_word_records':30}})
    (output/'reports/COVERAGE.md').write_text('Original engineering fixture report only.\n')
    return NS(output=output,project=project,books=inventory,book_records=books,api=FakeAPI(siblings),page_id=page_id,parquet=parquet)


class ReleaseGuardTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.fixture=make_fixture(Path(self.tmp.name).resolve());self.f=self.fixture
        self.patches=[patch.object(rel,'SOURCE',self.f.project),patch.object(exp,'SOURCE',self.f.project),patch.object(rel,'HfApi',return_value=self.f.api)]
        for item in self.patches:item.start()
    def tearDown(self):
        for item in reversed(self.patches):item.stop()
        self.tmp.cleanup()
    def mutate(self,sql):
        db=sqlite3.connect(self.f.output/'private/build.sqlite')
        try:db.execute(sql);db.commit()
        finally:db.close()
    def package(self):return rel.package(self.f.output,self.f.books,None,'v-fixture')
    def rewrite_books(self):
        with gzip.open(self.f.books,'wt') as stream:
            for book in self.f.book_records:stream.write(json.dumps(book)+'\n')

    def test_package_preserves_duplicate_only_source_origin_and_raw_manifest_proofs(self):
        summary=self.package();self.assertEqual(summary['unique_image_pages'],1)
        self.assertEqual(summary['source_documents_with_images'],1);self.assertEqual(summary['source_documents_with_any_occurrence'],2)
        self.assertEqual(summary['exact_pixel_duplicate_occurrences_removed'],1)
        with gzip.open(self.f.output/'release/source-documents.jsonl.gz','rt') as stream:books=[json.loads(x) for x in stream]
        self.assertEqual({b['source_book_id'] for b in books},{'book-main','book-duplicate'})
        self.assertEqual(next(b for b in books if b['source_book_id']=='book-duplicate')['image_pages'],0)
        for book in books:
            with gzip.open(self.f.output/f'release/metadata/source-manifests/{book["source_book_id"]}.json.gz','rb') as stream:
                self.assertEqual(hashlib.sha256(stream.read()).hexdigest(),book['manifest_sha256'])
        self.assertIn('duplicate_occurrences',(self.f.output/'release/README.md').read_text())

    def test_processing_and_pending_documents_cannot_be_packaged(self):
        for state in ('processing','pending'):
            with self.subTest(state=state):
                self.mutate(f"UPDATE documents SET status='{state}' WHERE document_id='book-main'")
                with self.assertRaisesRegex(ValueError,'processing|quiet|pending'):self.package()
                self.assertFalse((self.f.output/'release').exists())

    def test_concurrent_wal_commit_after_begin_is_refused_before_final_package(self):
        self.f.api.on_repo_info=lambda:self.mutate("UPDATE documents SET error='concurrent engineering writer' WHERE document_id='book-main'")
        with self.assertRaisesRegex(ValueError,'changed|quiet|concurrent'):self.package()
        self.assertFalse((self.f.output/'release').exists())

    def test_frozen_book_metadata_change_is_rejected(self):
        self.f.book_records[0]['provider']=['Changed provider'];self.rewrite_books()
        with self.assertRaisesRegex(ValueError,'frozen source binding'):self.package()

    def test_raw_manifest_hash_change_is_rejected(self):
        gz_json(self.f.books.parent/self.f.book_records[0]['raw_manifest_path'],b'Changed raw source fixture')
        with self.assertRaisesRegex(ValueError,'raw manifest hash'):self.package()

    def test_remote_hash_size_missing_and_extra_paths_are_rejected(self):
        baseline=copy.deepcopy(self.f.api.siblings)
        cases=('hash','size','missing','extra','unknown_component')
        for case in cases:
            with self.subTest(case=case):
                self.f.api.siblings=copy.deepcopy(baseline)
                if case=='hash':self.f.api.siblings[0].lfs.sha256='changed'
                if case=='size':self.f.api.siblings[0].size+=1
                if case=='missing':self.f.api.siblings.pop(0)
                if case=='extra':self.f.api.siblings.append(NS(rfilename='data/pages/untracked.parquet',size=1,lfs=NS(sha256='x')))
                if case=='unknown_component':self.f.api.siblings.append(NS(rfilename='data/unrecognized/stale.parquet',size=1,lfs=NS(sha256='x')))
                with self.assertRaisesRegex(ValueError,'Remote.*(mismatch|differ)'):self.package()
                self.assertFalse((self.f.output/'release').exists())

    def test_unverified_shard_and_staged_or_existing_package_refused(self):
        self.mutate('UPDATE shards SET uploaded=0')
        with self.assertRaisesRegex(ValueError,'verified'):self.package()
        self.mutate('UPDATE shards SET uploaded=1')
        state=json.loads((self.f.output/'private/publication.json').read_text());state['dataset_version']='v-fixture';write_json(self.f.output/'private/publication.json',state)
        with self.assertRaisesRegex(ValueError,'already staged'):self.package()
        state.pop('dataset_version');write_json(self.f.output/'private/publication.json',state)
        self.package()
        with self.assertRaisesRegex(ValueError,'already exists'):self.package()

    def test_publish_delegates_all_arguments_and_return_value_to_verified_publisher(self):
        # Snapshot upload/remote verification/tag races are tested independently
        # in test_publish_snapshot.py; this entry point must add no second path.
        marker=object()
        with patch('publish_snapshot.publish',return_value=marker) as publish:
            self.assertIs(rel.publish(self.f.output,'v1.2.3',False),marker)
        publish.assert_called_once_with(self.f.output,'v1.2.3',False)
        self.assertFalse(self.f.api.uploads);self.assertFalse(self.f.api.tag_calls);self.assertFalse(self.f.api.visibility)

    def test_publish_wrapper_propagates_guard_failure_without_side_effects(self):
        original=(self.f.output/'private/publication.json').read_text()
        with patch('publish_snapshot.publish',side_effect=ValueError('independent snapshot guard rejected')) as publish:
            with self.assertRaisesRegex(ValueError,'snapshot guard'):rel.publish(self.f.output,'v1.2.3',True)
        publish.assert_called_once_with(self.f.output,'v1.2.3',True)
        self.assertFalse(self.f.api.uploads);self.assertFalse(self.f.api.tag_calls);self.assertFalse(self.f.api.visibility)
        self.assertEqual((self.f.output/'private/publication.json').read_text(),original)

    def test_source_package_excludes_private_credentials_generated_artifacts_and_symlinks(self):
        blocked=['pipeline/private/auth.json','pipeline/.cache/session.json','pipeline/credentials.json','pipeline/token.txt','pipeline/.env','explorer/samples.json','explorer/images/page.jpg','acquisition/raw/response.json','acquisition/downloads/source.txt','pipeline/build.sqlite']
        for name in blocked:
            path=self.f.project/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text('ENGINEERING_SECRET_FIXTURE_DO_NOT_EXPORT')
        outside=Path(self.tmp.name)/'outside-secret.txt';outside.write_text('ENGINEERING_SECRET_FIXTURE_DO_NOT_EXPORT')
        (self.f.project/'pipeline/escaped.txt').symlink_to(outside)
        destination=Path(self.tmp.name)/'source-package';rel.source_package(destination)
        self.assertTrue((destination/'pipeline/build_pages.py').is_file());self.assertTrue((destination/'quality/source_metadata_notes.json').is_file())
        for name in blocked+['pipeline/escaped.txt']:
            self.assertFalse((destination/name).exists(),name+' must not be exported')
        self.assertTrue(all('ENGINEERING_SECRET_FIXTURE_DO_NOT_EXPORT' not in path.read_text() for path in destination.rglob('*') if path.is_file()))
        # An explicitly allowlisted path cannot be used to smuggle an outside file.
        approved=self.f.project/'pipeline/build_pages.py';approved.unlink();approved.symlink_to(outside)
        with self.assertRaisesRegex(ValueError,'Unsafe source package'):rel.source_package(Path(self.tmp.name)/'blocked-source-package')

    def test_local_explorer_shard_hash_guard_and_actual_image_byte_extraction(self):
        self.package();destination=exp.package(self.f.output,maximum=1)
        samples=json.loads((destination/'samples.json').read_text());self.assertEqual(len(samples['pages']),1)
        self.assertEqual(samples['pages'][0]['page_id'],self.f.page_id)
        original=pq.ParquetFile(self.f.parquet).read().to_pylist()[0]['image']['bytes']
        self.assertEqual((destination/samples['pages'][0]['sample_image']).read_bytes(),original)
        self.f.parquet.write_bytes(self.f.parquet.read_bytes()+b'corrupted bytes')
        with self.assertRaisesRegex(ValueError,'Local example shard hash'):exp.package(self.f.output,maximum=1)

    def test_failed_package_and_explorer_close_read_transactions(self):
        actual_connect=sqlite3.connect;connections=[]
        class TrackedConnection(sqlite3.Connection):
            was_closed=False
            def close(self):self.was_closed=True;return super().close()
        def connect(*args,**kwargs):
            kwargs['factory']=TrackedConnection;value=actual_connect(*args,**kwargs)
            if str(args[0]).startswith('file:'):connections.append(value)
            return value
        self.mutate("UPDATE documents SET status='pending' WHERE document_id='book-main'")
        try:
            with patch.object(rel.sqlite3,'connect',side_effect=connect):
                with self.assertRaises(ValueError):self.package()
            self.assertTrue(connections and all(c.was_closed for c in connections),'failed publisher must close every read transaction')
        finally:
            for c in connections:
                if not c.was_closed:c.close()
        self.mutate("UPDATE documents SET status='complete' WHERE document_id='book-main'")
        self.package();connections.clear()
        self.f.parquet.write_bytes(self.f.parquet.read_bytes()+b'corruption')
        try:
            with patch.object(exp.sqlite3,'connect',side_effect=connect):
                with self.assertRaises(ValueError):exp.package(self.f.output,maximum=1)
            self.assertTrue(connections and all(c.was_closed for c in connections),'failed explorer must close every read transaction')
        finally:
            for c in connections:
                if not c.was_closed:c.close()

if __name__=='__main__':unittest.main()
