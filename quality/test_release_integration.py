"""Combined release guards using original engineering fixtures, never Hub data.

These generated images, source PDFs and metadata are test inputs only. They do
not contribute corpus counts and no test performs network or publication calls.
"""
from __future__ import annotations
import copy
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace as NS
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from test_release import make_fixture, rel, exp
from test_release_components import fixture as reference_fixture


def community_fixture(root):
    """Build one real schema/attribution-validated community engineering row."""
    filename=Path(__file__).resolve().parents[1]/'audit/test_community_package.py'
    spec=importlib.util.spec_from_file_location('community_integration_fixture',filename)
    testmod=importlib.util.module_from_spec(spec);spec.loader.exec_module(testmod)
    page,rendered,file=testmod.fixture();source=root/'community-input';output=root/'community'
    def gz(name,value):
        target=source/name;target.parent.mkdir(parents=True,exist_ok=True)
        target.write_bytes(gzip.compress((json.dumps(value,ensure_ascii=False)+'\n').encode()))
    def js(name,value):
        target=source/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_text(json.dumps(value))
    gz('raw/rendered/example.json.gz',{'parse':'Original engineering fixture response'})
    rendered['raw_response_sha256']=testmod.package.digest(source/rendered['raw_response_path'])
    gz('rendered/batch.jsonl.gz',rendered);gz('pages/quality-4/batch.jsonl.gz',page);gz('files/batch.jsonl.gz',file)
    gz('raw/siteinfo.json.gz',{'fetched_at':'now','response':{'query':{'rightsinfo':{'url':page['text_license_url']},'proofreadqualitylevels':[],'proofreadnamespaces':{}}}})
    js('independent-inventory-qc.json',{'status':'passed'})
    js('render-summary.json',{'all_cached_eligible_pages_rendered':True,'counts':{'rendered_pages':1}})
    js('summary.json',{'counts':{'eligible_source_quality':1}})
    testmod.package.package(source,output,rows_per_shard=1)
    testmod.validator.validate(output)
    return output


class CombinedReleaseTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name).resolve()
        self.f=make_fixture(self.root);self.reference=reference_fixture(self.root)
        self.patches=[patch.object(rel,'SOURCE',self.f.project),patch.object(rel,'HfApi',return_value=self.f.api)]
        for item in self.patches:item.start()

    def tearDown(self):
        for item in reversed(self.patches):item.stop()
        self.tmp.cleanup()

    def package(self,community=None,references=True):
        return rel.package(self.f.output,self.f.books,community,'v-fixture',self.reference.source if references else None)

    def test_actual_reference_component_preserves_originals_credits_and_separate_units(self):
        result=self.package();release=self.f.output/'release'
        self.assertEqual(result['unique_image_pages'],1);self.assertEqual(result['reference_pair_rows'],1)
        self.assertEqual(result['total_unique_document_images'],2);self.assertEqual(result['machine_word_records'],30)
        self.assertEqual(result['cross_component_exact_pixel_overlap'],0)
        self.assertFalse(result['remote_reference_sha256_verified'])
        original=release/f'source/reference_pdfs/{self.reference.pair["source_pdf_sha1"]}.pdf'
        self.assertEqual(original.read_bytes(),self.reference.pdf.read_bytes())
        self.assertFalse((release/'private').exists())
        with gzip.open(release/'metadata/reference-pairs-source-files.jsonl.gz','rt') as stream:
            self.assertIn('PermissionTicket',stream.read())
        for path in release.rglob('*.json'):
            self.assertNotIn('DO_NOT_EXPORT_PRIVATE_FIXTURE',path.read_text())
        checks=dict((name,value) for value,name in (line.split('  ',1) for line in (release/'SHA256SUMS').read_text().splitlines()))
        for path in (original,release/self.reference.manifest['path']):
            self.assertEqual(checks[path.relative_to(release).as_posix()],hashlib.sha256(path.read_bytes()).hexdigest())
        card=(release/'README.md').read_text()
        self.assertIn('config_name: reference_pairs',card)
        self.assertIn('**2 unique document images',card)
        self.assertIn('do not count them as extra transcriptions',card)
        self.assertIn('no machine OCR or word boxes',card)

    def test_real_identical_raster_is_counted_once_across_components(self):
        # Put the reference JPEG into the main engineering page with its own
        # OCR annotations: two component rows, one actual decoded raster.
        row=pq.ParquetFile(self.f.parquet).read().to_pylist()[0]
        row.update(image=self.reference.pair['image'],width=self.reference.pair['width'],height=self.reference.pair['height'])
        pq.write_table(pa.Table.from_pylist([row]),self.f.parquet)
        digest=hashlib.sha256(self.f.parquet.read_bytes()).hexdigest();size=self.f.parquet.stat().st_size
        db=sqlite3.connect(self.f.output/'private/build.sqlite')
        try:
            db.execute('UPDATE pages SET pixel_sha256=?,image_sha256=?',(self.reference.pair['pixel_sha256'],self.reference.pair['image_sha256']))
            db.execute('UPDATE occurrences SET pixel_sha256=?',(self.reference.pair['pixel_sha256'],))
            db.execute("UPDATE shards SET sha256=?,bytes=? WHERE kind='pages'",(digest,size))
            db.commit()
        finally:db.close()
        self.f.api.siblings[0].lfs.sha256=digest;self.f.api.siblings[0].size=size
        result=self.package()
        self.assertEqual(result['unique_image_pages'],1);self.assertEqual(result['reference_pair_rows'],1)
        self.assertEqual(result['total_unique_document_images'],1)
        self.assertEqual(result['cross_component_exact_pixel_overlap'],1)

    def test_reference_remote_contract_remains_exact_when_called_through_package(self):
        base=copy.deepcopy(self.f.api.siblings);m=self.reference.manifest
        for case in ('hash','size','extra'):
            with self.subTest(case=case):
                self.f.api.siblings=copy.deepcopy(base)+[NS(rfilename=m['path'],size=m['bytes'],lfs=NS(sha256=m['sha256']))]
                if case=='hash':self.f.api.siblings[-1].lfs.sha256='changed'
                if case=='size':self.f.api.siblings[-1].size+=1
                if case=='extra':self.f.api.siblings.append(NS(rfilename='data/reference_pairs/unexpected.parquet',size=1,lfs=NS(sha256='changed')))
                with self.assertRaisesRegex(ValueError,'remote reference'):self.package()
                self.assertFalse((self.f.output/'release').exists())
        self.f.api.siblings=base+[NS(rfilename=m['path'],size=m['bytes'],lfs=NS(sha256=m['sha256']))]
        self.assertTrue(self.package()['remote_reference_sha256_verified'])

    def test_existing_reference_files_require_explicit_component(self):
        self.f.api.siblings.append(NS(rfilename=self.reference.manifest['path'],size=1,lfs=NS(sha256='fixture')))
        with self.assertRaisesRegex(ValueError,'reference data require'):self.package(references=False)
        self.assertFalse((self.f.output/'release').exists())

    def test_community_unknown_private_files_cannot_leak_or_overwrite_reference_proof(self):
        community=community_fixture(self.root)
        (community/'private').mkdir();(community/'private/token.txt').write_text('ENGINEERING_PRIVATE_DO_NOT_EXPORT')
        (community/'metadata/reference-pairs-summary.json').write_text('ENGINEERING_PRIVATE_DO_NOT_EXPORT')
        result=self.package(community=community);release=self.f.output/'release'
        self.assertEqual(result['community_text_records'],1);self.assertEqual(result['total_unique_document_images'],2)
        self.assertFalse((release/'private').exists())
        self.assertEqual(json.loads((release/'metadata/reference-pairs-summary.json').read_text())['rows'],1)
        self.assertFalse(result['remote_community_sha256_verified'])
        for path in release.rglob('*'):
            if path.is_file() and path.suffix in ('.txt','.json','.md'):
                self.assertNotIn('ENGINEERING_PRIVATE_DO_NOT_EXPORT',path.read_text())

    def test_community_public_copy_refuses_destination_conflicts_and_symlinks(self):
        community=community_fixture(self.root);destination=self.root/'public-copy'
        destination.mkdir();conflict=destination/'reports/community-text-build.json';conflict.parent.mkdir();conflict.write_text('preserve original')
        with self.assertRaisesRegex(ValueError,'exist|conflict|overwrite'):rel.copy_public_community(community,destination)
        self.assertEqual(conflict.read_text(),'preserve original')
        target=community/'reports/community-text-build.json';real=self.root/'outside.json';real.write_bytes(target.read_bytes());target.unlink();target.symlink_to(real)
        with self.assertRaisesRegex(ValueError,'[Ss]ymlink|[Uu]nsafe'):rel.copy_public_community(community,self.root/'symlink-copy')

    def test_explorer_refuses_resumed_database_count_or_changed_shard(self):
        self.package();database=self.f.output/'private/build.sqlite'
        db=sqlite3.connect(database)
        try:
            original=db.execute("SELECT sha256 FROM shards WHERE kind='pages'").fetchone()[0]
            db.execute("UPDATE shards SET sha256='changed-after-freeze' WHERE kind='pages'");db.commit()
            with self.assertRaisesRegex(ValueError,'shard set differs'):exp.package(self.f.output,maximum=1)
            db.execute("UPDATE shards SET sha256=? WHERE kind='pages'",(original,));db.commit()
            db.execute('DELETE FROM pages');db.commit()
            with self.assertRaisesRegex(ValueError,'database differs'):exp.package(self.f.output,maximum=1)
        finally:db.close()
        self.assertFalse((self.f.output/'explorer').exists())

    def test_explorer_http_failure_removes_its_temporary_download(self):
        self.package();self.f.parquet.unlink()
        with patch.object(exp,'SOURCE',self.f.project),patch.object(exp,'_headers',return_value={}), \
             patch.object(exp.shutil,'disk_usage',return_value=NS(free=8*1024**3)), \
             patch.object(exp.requests,'get',side_effect=RuntimeError('original simulated transport failure')):
            with self.assertRaisesRegex(RuntimeError,'simulated transport'):exp.package(self.f.output,maximum=1)
        self.assertEqual(list((self.f.output/'private').glob('example-shard-*')),[])


if __name__=='__main__':unittest.main()
