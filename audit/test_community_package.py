"""Independent selection, attribution, schema, and evidence-integrity tests."""
import copy
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("community_package", ROOT / "package_community.py")
package = importlib.util.module_from_spec(spec); spec.loader.exec_module(package)
spec = importlib.util.spec_from_file_location("community_public_validation", ROOT / "validate_community.py")
validator = importlib.util.module_from_spec(spec); spec.loader.exec_module(validator)


def fixture():
    record_id = "arws-" + hashlib.sha256(b"arwikisource:1:2").hexdigest()
    raw = '<noinclude><pagequality level="4" user="Contributor"/>١</noinclude>نص عربي محفوظ\n'
    page = {"record_id": record_id, "source": "Arabic Wikisource", "source_wiki": "arwikisource",
            "source_page_id": 1, "source_title": "صفحة:كتاب.pdf/2", "source_revision_id": 2,
            "source_parent_revision_id": 1, "source_revision_timestamp": "2020-01-01T00:00:00Z",
            "source_revision_sha1": hashlib.sha1(raw.encode()).hexdigest(),
            "source_revision_url": "https://ar.wikisource.org/w/index.php?oldid=2",
            "source_history_url": "https://ar.wikisource.org/w/index.php?curid=1&action=history",
            "source_quality_level": 4, "source_quality_label": "source_community_validated",
            "quality_fields_agree": True, "eligible_source_quality": True,
            "text_scope": "transcluded_page_body_excluding_noinclude_header_footer",
            "text_license": "CC-BY-SA-4.0", "text_license_url": "https://creativecommons.org/licenses/by-sa/4.0/",
            "text_attribution": "Arabic Wikisource contributors; see revision/history URLs",
            "wikitext": raw, "body_wikitext": "نص عربي محفوظ\n", "noinclude_sections_wikitext": ["١"],
            "source_scan_available": True, "scan_filename": "كتاب.pdf", "scan_page_number": 2,
            "scan_page_number_basis": "source_page_title_suffix", "alignment_level": "source_page_pair_only",
            "raw_response_sha256": "a" * 64, "fetched_at": "2026-10-15T00:00:00Z"}
    rendered = {"record_id": record_id, "source_revision_id": 2, "text": "نص عربي محفوظ\n",
                "text_character_count": 14, "text_unicode_letter_count": 11, "text_arabic_letter_count": 11,
                "text_arabic_letter_fraction": 1.0, "rendered_html": "<div>نص عربي محفوظ</div>",
                "render_quality_flags": [], "text_representation": "source_MediaWiki_parser_HTML_to_plain_text",
                "text_changes": "Source templates expanded; noinclude header/footer excluded.",
                "parse_warnings": [], "raw_response_path": "raw/rendered/example.json.gz",
                "raw_response_sha256": "b" * 64, "rendered_at": "2026-10-15T00:01:00Z"}
    file = {"scan_filename": "كتاب.pdf", "rights_review_status": "source_asserted_public_domain",
            "source_linked_aco_book_ids": ["nyu_aco000001"], "raw_response_path": "raw/private.json.gz",
            "source_file_info": {"pagecount": 3, "mime": "application/pdf", "sha1": "c" * 40,
               "timestamp": "2026-01-01T00:00:00Z", "size": 123,
               "url": "https://upload.wikimedia.org/example.pdf", "descriptionurl": "https://commons.wikimedia.org/wiki/File:Example.pdf",
               "extmetadata": {"LicenseShortName": {"value": "Public domain"}, "License": {"value": "pd"}}}}
    return page, rendered, file


class CommunityPackage(unittest.TestCase):
    def test_exact_arabic_and_attribution_roundtrip(self):
        page, rendered, file = fixture()
        row, reason = package.classify_and_join(page, rendered, file)
        self.assertIsNone(reason)
        self.assertEqual(row["text_license"], "CC-BY-SA-4.0")
        self.assertEqual(row["source_wikitext"], page["wikitext"])
        self.assertFalse(row["contains_image_bytes"])
        self.assertFalse(row["bayanat_human_verified"])
        self.assertFalse(row["image_version_correspondence_independently_verified"])
        self.assertIn("current_scan_version_postdates_text_revision", row["quality_flags"])
        self.assertEqual(row["source_linked_aco_book_ids"], ["nyu_aco000001"])
        table = pa.Table.from_pylist([row], schema=package.SCHEMA)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sample.parquet"; pq.write_table(table, path)
            self.assertEqual(pq.read_table(path).to_pylist()[0], row)

    def test_incompatible_or_missing_scan_is_excluded(self):
        page, rendered, file = fixture()
        for status in ("excluded_restrictive_terms", "review_required_missing_or_unrecognized_terms"):
            other = copy.deepcopy(file); other["rights_review_status"] = status
            self.assertIsNone(package.classify_and_join(page, rendered, other)[0])
        page["source_scan_available"] = False
        self.assertIsNone(package.classify_and_join(page, rendered, file)[0])

    def test_render_errors_empty_text_and_out_of_range_excluded(self):
        page, rendered, file = fixture()
        for flag in ("source_parser_error_element", "source_parser_missing_template"):
            other = copy.deepcopy(rendered); other["render_quality_flags"] = [flag]
            self.assertIsNone(package.classify_and_join(page, other, file)[0])
        other = copy.deepcopy(rendered); other["text"] = ""
        self.assertIsNone(package.classify_and_join(page, other, file)[0])
        page["scan_page_number"] = 4
        self.assertIsNone(package.classify_and_join(page, rendered, file)[0])

    def test_content_tamper_wrong_revision_and_old_extractor_fail(self):
        page, rendered, file = fixture()
        for which in ("text", "revision", "extractor"):
            p, r = copy.deepcopy(page), copy.deepcopy(rendered)
            if which == "text": p["wikitext"] += "changed"
            elif which == "revision": r["source_revision_id"] = 99
            else: r.pop("render_quality_flags")
            with self.subTest(which=which), self.assertRaises(ValueError):
                package.classify_and_join(p, r, file)

    def test_full_packager_is_train_only_and_has_no_private_paths(self):
        page, rendered, file = fixture()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); source = root / 'source'; output = root / 'community-release'
            def gz(relative, value):
                path = source / relative; path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(gzip.compress((json.dumps(value, ensure_ascii=False) + '\n').encode()))
            def js(relative, value):
                path = source / relative; path.parent.mkdir(parents=True, exist_ok=True); path.write_text(json.dumps(value))
            gz('raw/rendered/example.json.gz', {'parse': 'public source response'})
            rendered['raw_response_sha256'] = package.digest(source / rendered['raw_response_path'])
            gz('rendered/batch.jsonl.gz', rendered); gz('pages/quality-4/batch.jsonl.gz', page)
            gz('files/batch.jsonl.gz', file)
            gz('raw/siteinfo.json.gz', {'fetched_at':'now','response':{'query':{'rightsinfo':{'url':page['text_license_url']}, 'proofreadqualitylevels':[], 'proofreadnamespaces':{}}}})
            js('independent-inventory-qc.json', {'status':'passed'})
            js('render-summary.json', {'all_cached_eligible_pages_rendered':True,'counts':{'rendered_pages':1}})
            js('summary.json', {'counts':{'eligible_source_quality':1}})
            report = package.package(source, output, rows_per_shard=1)
            self.assertEqual(report['split'], 'train')
            self.assertFalse(report['contains_image_bytes'])
            self.assertFalse(report['is_heldout_benchmark'])
            self.assertEqual(report['counts']['published_text_records'], 1)
            public = next(package.rows((output / 'metadata/wikisource').glob('scan_files.jsonl.gz')))
            self.assertNotIn('raw_response_path', public)
            self.assertEqual(package.digest(output / report['data_files'][0]['path']), report['data_files'][0]['sha256'])
            validated = validator.validate(output)
            self.assertEqual(validated['status'], 'passed')
            self.assertEqual(validated['rows_checked'], 1)
            gz('raw/rendered/example.json.gz', {'tampered': True})
            with self.assertRaisesRegex(ValueError, 'SHA256'):
                package.package(source, output)
            self.assertFalse((output / 'reports/community-text-build.json').exists())


if __name__ == '__main__':
    unittest.main()
