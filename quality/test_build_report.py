"""Hand-calculated engineering ledger, not fabricated corpus observations."""
from __future__ import annotations
import gzip
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

path=Path(__file__).resolve().parents[1]/'pipeline/build_report.py'
spec=importlib.util.spec_from_file_location('atlas_report_test',path);r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)


def fixture(output):
    private=output/'private';private.mkdir()
    db=sqlite3.connect(private/'build.sqlite');db.execute('PRAGMA journal_mode=WAL')
    db.executescript('''
    CREATE TABLE documents(document_id TEXT PRIMARY KEY,pdf_sha256 TEXT,pdf_url TEXT,pages INTEGER,status TEXT,error TEXT,manifest_sha256 TEXT,manifest_url TEXT,book_record_sha256 TEXT);
    CREATE TABLE pages(page_id TEXT PRIMARY KEY,logical_id TEXT,document_id TEXT,page_index INTEGER,shard_path TEXT,pixel_sha256 TEXT,text_hash TEXT,split TEXT,words INTEGER,curated INTEGER,image_sha256 TEXT);
    CREATE TABLE occurrences(page_id TEXT PRIMARY KEY,logical_id TEXT,document_id TEXT,page_index INTEGER,retained_page_id TEXT,is_duplicate INTEGER,pixel_sha256 TEXT,lineage_shard TEXT);
    CREATE TABLE shards(path TEXT,sha256 TEXT,rows INTEGER,bytes INTEGER,uploaded INTEGER,commit_sha TEXT,kind TEXT);
    CREATE TABLE failures(document_id TEXT,page_index INTEGER,error TEXT,recorded_at TEXT);
    ''')
    books=[]
    for doc,providers,years,status,count in [('a',['P1'],[1919],'complete',2),('b',['P2','P3'],[1920],'partial',3),('c',['P1'],[],'complete',1),('d',['P4'],[],'error',None)]:
        book={'source_book_id':doc,'manifest_sha256':doc*64,'manifest_url':f'https://example.org/{doc}/manifest','provider':providers,'publication_year_tokens':years}
        books.append(book)
        db.execute('INSERT INTO documents VALUES (?,?,?,?,?,?,?,?,?)',(doc,'f'*64 if count else None,f'https://example.org/{doc}.pdf' if count else None,count,status,'source download failed' if doc=='d' else None,book['manifest_sha256'],book['manifest_url'],r.record_hash(book)))
    for page,doc,index,words,curated,pixel,text in [('a0','a',0,0,0,'p0',None),('a1','a',1,100,1,'p1','text-hash'),('b0','b',0,200,1,'p2','text-hash')]:
        db.execute('INSERT INTO pages VALUES (?,?,?,?,?,?,?,?,?,?,?)',(page,page,doc,index,'data/pages/a.parquet',pixel,text,'train',words,curated,page))
        db.execute('INSERT INTO occurrences VALUES (?,?,?,?,?,?,?,?)',(page,page,doc,index,page,0,pixel,'data/pages/a.parquet'))
    db.execute("INSERT INTO occurrences VALUES ('c0','c0','c',0,'a0',1,'p0','data/duplicate_occurrences/b.parquet')")
    db.execute("INSERT INTO shards VALUES ('data/pages/a.parquet','x',3,1000,1,'commit','pages')")
    db.execute("INSERT INTO shards VALUES ('data/duplicate_occurrences/b.parquet','y',1,200,1,'commit','duplicate_occurrences')")
    for doc,index,error in [('a',1,'Native OCR failed: Invalid input range'),('c',0,'Native OCR exceeded60s timeout'),('b',2,'bad_unicode'),('b',2,'bbox_outside_image')]:
        db.execute('INSERT INTO failures VALUES (?,?,?,?)',(doc,index,error,'2026-10-15T22:00:00Z'))
    db.commit();db.close()
    (private/'processing-config.json').write_text(json.dumps({'processing_config_sha256':'c'*64}))
    inventory=output/'books.jsonl.gz'
    with gzip.open(inventory,'wt') as stream:
        for book in books:stream.write(json.dumps(book)+'\n')
    return inventory


class ReportSemanticsTests(unittest.TestCase):
    def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.output=Path(self.tmp.name);self.books=fixture(self.output)
    def tearDown(self):self.tmp.cleanup()
    def mutate(self,sql):
        with sqlite3.connect(self.output/'private/build.sqlite') as db:db.execute(sql)

    def test_hand_calculated_source_weights_screen_duplicates_and_failure_resolution(self):
        report=r.collect_report(self.output,self.books)
        c=report['counts'];self.assertEqual((c['retained_unique_images'],c['successful_source_occurrences'],c['exact_pixel_duplicate_occurrences']),(3,4,1))
        self.assertEqual(c['machine_word_records'],300);self.assertEqual(c['source_books_enrolled_in_state'],4)
        self.assertEqual(c['source_books_with_retained_images'],2);self.assertEqual(c['source_books_with_successful_occurrences'],3)
        self.assertEqual(report['words_per_retained_image']['p50'],100)
        self.assertEqual(report['words_per_retained_image']['p05'],10)
        self.assertAlmostEqual(report['words_per_retained_image']['p95'],190)
        self.assertEqual(report['words_per_retained_image']['zero_recognized_word_pages'],1)
        self.assertAlmostEqual(report['mechanical_screen']['pass_share'],2/3)
        provider={x['label']:x for x in report['provider_coverage']['rows']}
        self.assertEqual(set(provider),{'P1','P2','P3'})
        self.assertEqual(provider['P1']['source_book_weight'],2)
        self.assertAlmostEqual(provider['P2']['source_book_share'],1/6)
        self.assertAlmostEqual(provider['P2']['retained_image_page_share'],1/6)
        self.assertAlmostEqual(provider['P1']['successful_source_occurrence_share'],3/4)
        for key in ('source_book_share','retained_image_page_share','successful_source_occurrence_share'):
            self.assertAlmostEqual(sum(x[key] for x in provider.values()),1)
        years={x['label']:x for x in report['catalog_publication_year_coverage']['rows']}
        self.assertEqual(years['ambiguous_or_missing']['retained_image_page_weight'],0)
        self.assertEqual(years['ambiguous_or_missing']['successful_source_occurrence_weight'],1)
        self.assertEqual(report['duplicates']['normalized_long_text_duplicate_groups'],1)
        self.assertEqual(report['duplicates']['cross_document_text_duplicate_groups'],1)
        self.assertEqual(report['duplicates']['retained_images_in_text_duplicate_groups'],2)
        f=report['failures'];self.assertEqual(f['recorded_page_failure_attempts'],4)
        self.assertEqual(f['unique_source_pages_with_a_failed_attempt'],3)
        self.assertEqual(f['unique_failed_source_pages_by_final_success'],{'later_retained_image':1,'later_exact_duplicate_success':1,'no_successful_occurrence':1})
        self.assertEqual(f['document_errors_without_successful_occurrences'],1)
        self.assertEqual(report['shards']['total_parquet_bytes'],1200)
        self.assertIn('not OCR accuracy',r.markdown(report))

    def test_quiet_and_verified_requirements(self):
        self.mutate("UPDATE documents SET status='processing' WHERE document_id='a'")
        with self.assertRaisesRegex(ValueError,'not quiet'):r.collect_report(self.output,self.books)
        self.mutate("UPDATE documents SET status='complete' WHERE document_id='a'")
        self.mutate('UPDATE shards SET uploaded=0')
        with self.assertRaisesRegex(ValueError,'unverified'):r.collect_report(self.output,self.books)

    def test_inventory_mutation_is_not_allowed_to_relabel_old_images(self):
        with gzip.open(self.books,'rt') as stream:books=[json.loads(x) for x in stream]
        books[0]['provider']=['Changed source provider']
        with gzip.open(self.books,'wt') as stream:
            for book in books:stream.write(json.dumps(book)+'\n')
        with self.assertRaisesRegex(ValueError,'frozen document binding'):r.collect_report(self.output,self.books)

    def test_concurrent_change_detected_after_ending_read_snapshot(self):
        original=r.throughput
        def concurrent_writer(*args):
            self.mutate("INSERT INTO failures VALUES ('d',0,'late write','2026-10-15T22:00:01Z')")
            return original(*args)
        with patch.object(r,'throughput',side_effect=concurrent_writer):
            with self.assertRaisesRegex(ValueError,'changed during report'):r.collect_report(self.output,self.books)

    def test_corrupt_occurrence_or_shard_accounting_is_rejected(self):
        self.mutate("UPDATE occurrences SET pixel_sha256='different' WHERE page_id='c0'")
        with self.assertRaisesRegex(ValueError,'different retained image'):r.collect_report(self.output,self.books)
        self.mutate("UPDATE occurrences SET pixel_sha256='p0' WHERE page_id='c0'")
        self.mutate("UPDATE shards SET rows=4 WHERE kind='pages'")
        with self.assertRaisesRegex(ValueError,'shard rows'):r.collect_report(self.output,self.books)

    def test_run_restarts_never_pool_elapsed_time_and_require_matching_final_counts(self):
        log=self.output/'run.log'
        def point(elapsed,n,second):return {'stage':'progress','elapsed_seconds':elapsed,'pages':n,'processed_source_occurrences':n,'words':n*100,'shards':n,'updated_utc':f'2026-10-15T22:00:{second:02d}Z'}
        points=[point(10,2,10),point(20,4,20),point(3,5,30),point(13,7,40)]
        log.write_text('ignored human log line\n'+'\n'.join(json.dumps(x) for x in points))
        result=r.throughput(log,{'pages':7,'processed_source_occurrences':7,'words':700,'shards':7})
        self.assertTrue(result['available']);self.assertEqual(result['detected_run_segments'],2)
        self.assertEqual(result['new_successful_source_occurrences'],2);self.assertEqual(result['observed_interval_seconds'],10)
        self.assertAlmostEqual(result['retained_unique_images_per_second'],.2)
        self.assertEqual(result['earlier_run_segments_not_combined'],1)
        self.assertFalse(r.throughput(log,{'pages':8,'processed_source_occurrences':8,'words':800,'shards':8})['available'])

if __name__=='__main__':unittest.main()
