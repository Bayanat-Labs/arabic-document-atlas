"""Engineering fixtures only: exact source versions, page position and HFImage."""
import copy
import argparse
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
import sqlite3
import zlib

from datasets import load_dataset
import pyarrow as pa
import pyarrow.parquet as pq
from PIL import Image

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('reference_pairs_under_test',ROOT/'pipeline/build_reference_pairs.py')
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)


def two_page_pdf(path,notice=False):
    streams=[b'1 0 0 rg 0 0 200 300 re f',b'0 0 1 rg 0 0 200 300 re f']
    if notice:streams[0]=b'BT /F1 3 Tf 10 280 Td (https://creativecommons.org/licenses/by-nc-nd/4.0/) Tj ET'
    objects=[b'<< /Type /Catalog /Pages 2 0 R >>',b'<< /Type /Pages /Kids [3 0 R 4 0 R] /Count 2 >>',
             b'<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 300] /Resources << /Font << /F1 7 0 R >> >> /Contents 5 0 R >>',
             b'<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 300] /Contents 6 0 R >>']
    objects += [b'<< /Length '+str(len(s)).encode()+b' >>\nstream\n'+s+b'\nendstream' for s in streams]
    objects.append(b'<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>')
    data=bytearray(b'%PDF-1.4\n');offsets=[0]
    for i,obj in enumerate(objects,1):
        offsets.append(len(data));data.extend(f'{i} 0 obj\n'.encode()+obj+b'\nendobj\n')
    offset=len(data);data.extend(f'xref\n0 {len(objects)+1}\n0000000000 65535 f \n'.encode())
    for n in offsets[1:]:data.extend(f'{n:010d} 00000 n \n'.encode())
    data.extend(f'trailer << /Size {len(objects)+1} /Root 1 0 R >>\nstartxref\n{offset}\n%%EOF\n'.encode());path.write_bytes(data)


def row(path):
    result={name:None for name in b.COMMUNITY_SCHEMA.names}
    result.update(record_id='arws-engineering-fixture',file_group_id='fixture-file',source_quality_level=4,
                  bayanat_human_verified=False,image_version_correspondence_independently_verified=False,has_human_line_or_region_boxes=False,
                  source_quality_label='source_community_validated',quality_fields_agree=True,
                  source_linked_aco_book_ids=[],scan_file_version_postdates_text_revision=False,
                  scan_file_timestamp='2019-01-01T00:00:00Z',source_revision_timestamp='2020-01-01T00:00:00Z',
                  scan_mime='application/pdf',scan_file_size_bytes=path.stat().st_size,scan_file_page_count=2,
                  scan_file_sha1=b.sha(path,'sha1'),scan_file_url='https://upload.wikimedia.org/wikipedia/commons/a/ab/Fixture.pdf?utm_source=ar.wikisource.org',
                  scan_rights_review_status='source_asserted_open_license',scan_license_short_name='CC BY 4.0',
                  scan_license_code='cc-by-4.0',scan_license_url='https://creativecommons.org/licenses/by/4.0/',
                  scan_credit_html='<b>Engineering-only credit</b>',text='نص عربي أصلي للاختبار',source_wikitext='نص عربي أصلي للاختبار',
                  source_body_wikitext='نص عربي أصلي للاختبار',source_revision_sha1=hashlib.sha1('نص عربي أصلي للاختبار'.encode()).hexdigest(),
                  scan_page_number=2,text_license='CC-BY-SA-4.0',text_scope='transcluded_page_body_excluding_noinclude_header_footer')
    return result


class ReferencePairs(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.pdf=self.root/'fixture.pdf';two_page_pdf(self.pdf);self.row=row(self.pdf)
    def tearDown(self):self.temp.cleanup()

    def test_source_versions_and_integer_page_bounds_fail_closed(self):
        self.assertIsNone(b.selection_reason(self.row,1000000))
        for key,value,expected in [('scan_page_number',True,'invalid_source_page_range'),('scan_file_version_postdates_text_revision',None,'postdated_or_unknown_scan_version'),('scan_file_timestamp','2021-01-01T00:00:00Z','postdated_or_unknown_scan_version'),('source_wikitext','tampered','source_wikitext_revision_hash_mismatch'),('scan_mime','image/vnd.djvu','non_pdf_format'),('source_linked_aco_book_ids',['aco-id'],'known_aco_linked_file')]:
            other=copy.deepcopy(self.row);other[key]=value
            with self.subTest(key=key):self.assertEqual(b.selection_reason(other,1000000),expected)

    def test_explicit_image_license_is_separate_from_text(self):
        for key,value in [('scan_license_url',None),('scan_license_url','https://creativecommons.org/licenses/by-nc/4.0/'),('scan_rights_review_status','review_required_restrictions')]:
            other=copy.deepcopy(self.row);other[key]=value
            self.assertIsNotNone(b.selection_reason(other,1000000))
        other=copy.deepcopy(self.row);other['scan_usage_terms']='Noncommercial use only'
        self.assertEqual(b.selection_reason(other,1000000),'contradictory_restrictive_file_terms')
        for url in ['https://example.org/?license=https://creativecommons.org/licenses/by/4.0/','https://creativecommons.org.evil.test/licenses/by/4.0/','https://user@creativecommons.org/licenses/by/4.0/']:
            self.assertFalse(b.explicit_cc_license(url))

    def test_download_allowlist_preserves_raw_but_drops_tracking(self):
        self.assertEqual(b.download_url(self.row['scan_file_url']),'https://upload.wikimedia.org/wikipedia/commons/a/ab/Fixture.pdf')
        for url in ['http://upload.wikimedia.org/f.pdf','https://example.com/f.pdf','https://upload.wikimedia.org/f.pdf?token=secret','https://x@upload.wikimedia.org/f.pdf']:
            with self.assertRaises(ValueError):b.download_url(url)

    def test_original_binary_sha1_and_pdf_count_are_checked(self):
        self.assertEqual(b.verify_pdf(self.pdf,self.row),b.sha(self.pdf))
        for key,value in [('scan_file_sha1','0'*40),('scan_file_page_count',1),('scan_file_size_bytes',1)]:
            other=copy.deepcopy(self.row);other[key]=value
            with self.assertRaises(ValueError):b.verify_pdf(self.pdf,other)

    def test_second_source_page_hfimage_roundtrip_and_exact_reference(self):
        pair=b.make_pair(self.row,self.pdf,b.verify_pdf(self.pdf,self.row),'f'*64)
        self.assertEqual(pair['source_pdf_page_index'],1)
        self.assertEqual(pair['reference_text'],self.row['text'])
        self.assertEqual(pair['source_wikitext'],self.row['source_wikitext'])
        self.assertEqual(pair['scan_credit_html'],self.row['scan_credit_html'])
        self.assertFalse(pair['image_version_correspondence_independently_verified'])
        self.assertFalse(pair['has_human_line_or_region_boxes'])
        self.assertEqual(pair['width'],1600);self.assertEqual(pair['height'],2400)
        with Image.open(io.BytesIO(pair['image']['bytes'])) as image:
            r,g,blue=image.getpixel((800,1200));self.assertGreater(blue,240);self.assertLess(r,10)
        path=self.root/'pair.parquet';pq.write_table(pa.Table.from_pylist([pair],schema=b.SCHEMA),path)
        loaded=load_dataset('parquet',data_files=str(path),split='train',cache_dir=str(self.root/'datasets-cache'))[0]
        self.assertIsInstance(loaded['image'],Image.Image)
        self.assertEqual(loaded['image'].size,(1600,2400))
        self.assertEqual(loaded['reference_text'],self.row['text'])
        self.assertEqual(loaded['scan_license_url'],self.row['scan_license_url'])
        self.assertEqual(loaded['source_pdf_sha1'],self.row['scan_file_sha1'])

    def validation_fixture(self):
        output=self.root/'release';(output/'private/download-proofs').mkdir(parents=True)
        config={'max_pdf_bytes':1000000,'profile':b.PROFILE}
        digest=hashlib.sha256(b.canonical(config).encode()).hexdigest()
        pair=b.make_pair(self.row,self.pdf,b.verify_pdf(self.pdf,self.row),digest)
        path=output/'data/reference_pairs/fixture.parquet';path.parent.mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist([pair],schema=b.SCHEMA),path)
        db=sqlite3.connect(output/'private/state.sqlite');db.execute('CREATE TABLE candidates(id TEXT,payload BLOB)')
        db.execute('INSERT INTO candidates VALUES(?,?)',(self.row['record_id'],zlib.compress(b.canonical(self.row).encode())));db.commit();db.close()
        b.atomic_json(output/'private/processing-config.json',{'sha256':digest,'config':config})
        b.atomic_json(output/'private/download-proofs'/f'{self.row["scan_file_sha1"]}.json',{'sha1':self.row['scan_file_sha1'],'sha256':b.sha(self.pdf),'bytes':self.pdf.stat().st_size})
        b.atomic_json(output/'reports/reference-pairs-build.json',{'status':'built_pending_validation','rows':1,'data_files':[{'path':str(path.relative_to(output)),'sha256':b.sha(path),'bytes':path.stat().st_size,'rows':1}]})
        b.atomic_json(output/'metadata/reference-pairs-summary.json',{'rows':1,'validation_passed':False})
        return output,path,pair

    def test_serialized_pdf_version_tamper_and_stale_success_fail_closed(self):
        output,path,pair=self.validation_fixture()
        self.assertEqual(b.validate_output(output)['rows_checked'],1)
        self.assertTrue(json.loads((output/'metadata/reference-pairs-summary.json').read_text())['validation_passed'])
        pair['source_pdf_sha256']='a'*64
        pq.write_table(pa.Table.from_pylist([pair],schema=b.SCHEMA),path)
        report=json.loads((output/'reports/reference-pairs-build.json').read_text())
        report['data_files'][0].update(sha256=b.sha(path),bytes=path.stat().st_size)
        b.atomic_json(output/'reports/reference-pairs-build.json',report)
        with self.assertRaisesRegex(ValueError,'verified original download'):b.validate_output(output)
        self.assertFalse((output/'reports/reference-pairs-validation.json').exists())
        self.assertFalse(json.loads((output/'metadata/reference-pairs-summary.json').read_text())['validation_passed'])
        self.assertEqual(json.loads((output/'reports/reference-pairs-build.json').read_text())['status'],'built_pending_validation')

    def test_corrupt_frozen_config_cannot_keep_previous_validation(self):
        output,path,pair=self.validation_fixture();b.validate_output(output)
        config=json.loads((output/'private/processing-config.json').read_text());config['config']['max_pdf_bytes']=999999
        b.atomic_json(output/'private/processing-config.json',config)
        with self.assertRaisesRegex(ValueError,'configuration digest'):b.validate_output(output)
        self.assertFalse((output/'reports/reference-pairs-validation.json').exists())
        self.assertFalse(json.loads((output/'metadata/reference-pairs-summary.json').read_text())['validation_passed'])

    def test_failed_extension_invalidates_old_success_before_input_read(self):
        output,path,pair=self.validation_fixture();b.validate_output(output)
        args=argparse.Namespace(output=str(output),community=str(self.root/'missing-source'))
        with self.assertRaises(FileNotFoundError):b.build(args)
        self.assertFalse((output/'reports/reference-pairs-validation.json').exists())
        self.assertFalse(json.loads((output/'metadata/reference-pairs-summary.json').read_text())['validation_passed'])
        self.assertEqual(json.loads((output/'reports/reference-pairs-build.json').read_text())['status'],'build_in_progress')

    def test_native_restrictive_notice_keeps_exact_page_evidence(self):
        path=self.root/'notice.pdf';two_page_pdf(path,notice=True)
        notice=b.audit_native_rights_notices(path)
        self.assertTrue(notice['restrictive_cc_notice_found'])
        self.assertEqual(notice['notice_matches'][0]['source_pdf_page_index'],0)
        text=notice['notice_matches'][0]['extracted_text']
        self.assertIn('by-nc-nd/4.0',text)
        self.assertEqual(notice['notice_matches'][0]['extracted_text_sha256'],hashlib.sha256(text.encode()).hexdigest())


if __name__=='__main__':unittest.main()
