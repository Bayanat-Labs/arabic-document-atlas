"""Independent bounded semantic tests; no network or corpus mutation."""
import copy
import gzip
import importlib.util
import json
from pathlib import Path
import unittest
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


ws = module("atlas_ws", ROOT / "acquisition/wikisource.py")
aco = module("atlas_aco", ROOT / "acquisition/inventory.py")


def manifest():
    book = "nyu_aco000001"
    base = f"https://sites.dlib.nyu.edu/viewer/api/presentation/books/{book}/"
    service = f"https://sites.dlib.nyu.edu/viewer/api/image/books/{book}/1"
    canvas = {"id": base + "canvas/1", "type": "Canvas", "width": 2000, "height": 3000,
              "items": [{"type": "AnnotationPage", "items": [{"motivation": "painting",
              "body": {"id": service + "/full/full/0/default.jpg", "type": "Image",
                       "service": [{"id": service, "type": "ImageService2"}]}}]}]}
    return {"id": base + "manifest.json", "type": "Manifest", "items": [canvas],
            "requiredStatement": {"value": {"en": ["NYU believes these materials are in the public domain."]}},
            "metadata": [{"label": {"en": ["Collection"]}, "value": {"en": ["Arabic Collections Online"]}},
                         {"label": {"en": ["Date of publication"]}, "value": {"en": ["1901"]}}]}


def page():
    return {"pageid": 13, "ns": 104, "title": "صفحة:Book.pdf/8", "lastrevid": 17,
            "proofread": {"quality": 3}, "pageprops": {"proofread_page_quality_level": "3"},
            "imagesforpage": {"filename": "Book.pdf", "fullsize": "//thumb.wikimedia.org/test/page8.jpg"},
            "revisions": [{"revid": 17, "parentid": 16, "sha1": "a" * 40, "timestamp": "2020-01-01T00:00:00Z",
                           "slots": {"main": {"content": '<noinclude><pagequality level="3" user="A"/>Header</noinclude>\n{{أبيات|نص عربي\\\\بيت آخر}}<noinclude>Footer</noinclude>'}}}]}


class SourceSemantics(unittest.TestCase):
    def test_arabic_letter_count_excludes_combining_superscript_alif(self):
        self.assertEqual(ws.arabic_letter_count('ا\u0670 A'), 1)
        self.assertEqual(ws.arabic_letter_count('\u0670'), 0)

    def test_page_body_keeps_text_bearing_templates(self):
        body, quality = ws.split_page_wikitext(page()["revisions"][0]["slots"]["main"]["content"])
        self.assertEqual(quality, 3)
        self.assertIn("{{أبيات|نص عربي", body)
        self.assertNotIn("Header", body)
        self.assertNotIn("Footer", body)

    def test_source_revision_identity_not_title_or_fetch_time(self):
        a = ws.normalize_page(page(), {"fetched_at": "2026-01-01Z"}, "b" * 64, "raw/test.gz")
        changed = page(); changed["title"] = "صفحة:Renamed.pdf/8"
        b = ws.normalize_page(changed, {"fetched_at": "2026-02-01Z"}, "c" * 64, "raw/other.gz")
        self.assertEqual(a["record_id"], b["record_id"])
        self.assertEqual(a["source_quality_label"], "source_community_proofread")
        self.assertFalse(a["bayanat_human_verified"])
        self.assertFalse(a["has_human_line_or_region_boxes"])
        self.assertEqual(a["scan_page_number"], 8)
        self.assertEqual(a["text_license"], "CC-BY-SA-4.0")

    def test_disagreeing_source_quality_is_not_eligible(self):
        value = page(); value["proofread"]["quality"] = 4
        row = ws.normalize_page(value, {"fetched_at": "now"}, "x", "raw.gz")
        self.assertFalse(row["quality_fields_agree"])
        self.assertFalse(row["eligible_source_quality"])

    def test_missing_revision_content_fails_closed(self):
        value = page(); del value["revisions"][0]["slots"]["main"]["content"]
        with self.assertRaises(ValueError):
            ws.normalize_page(value, {"fetched_at": "now"}, "x", "raw.gz")

    def test_missing_scan_array_is_retained_as_metadata_only(self):
        value = page(); value["imagesforpage"] = []
        row = ws.normalize_page(value, {"fetched_at": "now"}, "x", "raw.gz")
        self.assertIsNone(row["scan_filename"])
        self.assertFalse(row["source_scan_available"])
        self.assertTrue(row["eligible_source_quality"])

    def test_single_page_image_needs_no_artificial_title_suffix(self):
        value = page(); value["title"] = "صفحة:Historical.jpg"
        value["imagesforpage"]["filename"] = "Historical.jpg"
        row = ws.normalize_page(value, {"fetched_at": "now"}, "x", "raw.gz")
        self.assertEqual(row["scan_page_number"], 1)
        self.assertEqual(row["scan_page_number_basis"], "single_image_filename_no_numeric_suffix")
        self.assertTrue(row["source_scan_available"])

    def test_renderer_isolates_pages_when_source_markup_nests_wrappers(self):
        class FakeParser:
            def get(self, params, api):
                text = params["text"]
                two = 'bayanat-source-13' in text and 'bayanat-source-14' in text
                html = ('<div id="bayanat-source-13">FIRST<div id="bayanat-source-14">SECOND</div></div>' if two
                        else '<div id="bayanat-source-13">FIRST</div>' if 'bayanat-source-13' in text
                        else '<div id="bayanat-source-14">SECOND</div>')
                return {"fetched_at": "now", "request_params": {"action": "query", "format": "json", "formatversion": 2, "maxlag": 5, **params},
                        "response": {"parse": {"text": html, "parsewarnings": []}}}
        records = []
        for identity in (13, 14):
            value = page(); value["pageid"] = identity
            records.append(ws.normalize_page(value, {"fetched_at": "now"}, "x", "raw.gz"))
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); destination = root / 'pages/quality-3/batch.jsonl.gz'
            destination.parent.mkdir(parents=True)
            destination.write_bytes(gzip.compress(b''.join((json.dumps(r) + '\n').encode() for r in records)))
            report = ws.render_pages(root, FakeParser(), batch_size=20)
            outputs = [json.loads(line) for p in (root / 'rendered').glob('*.gz') for line in gzip.open(p, 'rt')]
            self.assertEqual(report['counts']['rendered_pages'], 2)
            self.assertEqual(report['counts']['batches_split_for_page_marker_isolation'], 1)
            self.assertEqual({x['text'] for x in outputs}, {'FIRST', 'SECOND'})

    def test_named_citations_are_not_resolved_in_another_pages_scope(self):
        class FakeParser:
            def get(self, params, api):
                text = params['text']
                two = 'bayanat-source-13' in text and 'bayanat-source-14' in text
                html = ('<div id="bayanat-source-13">FIRST</div><div id="bayanat-source-14">WRONG-SHARED-NOTE</div>' if two
                        else '<div id="bayanat-source-14">SECOND-OWN-NOTE</div>')
                return {'fetched_at':'now','request_params':{'action':'query','format':'json','formatversion':2,'maxlag':5,**params},
                        'response':{'parse':{'text':html,'parsewarnings':[]}}}
        records = []
        for identity in (13, 14):
            value = page(); value['pageid'] = identity
            record = ws.normalize_page(value, {'fetched_at':'now'}, 'x', 'raw.gz')
            if identity == 14: record['body_wikitext'] += '<ref name="same">OWN NOTE</ref>'
            records.append(record)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); destination = root / 'pages/quality-3/batch.jsonl.gz'
            destination.parent.mkdir(parents=True)
            destination.write_bytes(gzip.compress(b''.join((json.dumps(r)+'\n').encode() for r in records)))
            report = ws.render_pages(root, FakeParser(), batch_size=20)
            outputs = [json.loads(line) for p in (root/'rendered').glob('*.gz') for line in gzip.open(p,'rt')]
            self.assertEqual(report['counts']['pages_isolated_for_named_reference_scope'], 1)
            self.assertEqual({x['text'] for x in outputs}, {'FIRST','SECOND-OWN-NOTE'})

    def test_scan_rights_triage_is_not_blanket_cc0(self):
        def metadata(**values):
            return {k: {"value": v} for k, v in values.items()}
        self.assertEqual(ws.rights_classification({}), "review_required_missing_or_unrecognized_terms")
        self.assertEqual(ws.rights_classification(metadata(License="pd")), "source_asserted_public_domain")
        self.assertEqual(ws.rights_classification(metadata(License="cc-by-nc-4.0")), "excluded_restrictive_terms")
        self.assertEqual(ws.rights_classification(metadata(LicenseUrl="https://creativecommons.org/licenses/by-sa/4.0/")), "source_asserted_open_license")
        self.assertEqual(ws.rights_classification(metadata(License="pd", Restrictions="trademark")), "review_required_restrictions")

    def test_aco_rejects_malformed_identity_and_dimensions(self):
        for dimension in (0, -1, True, 20.1, "2000", None):
            value = manifest(); value["items"][0]["width"] = dimension
            with self.subTest(dimension=dimension), self.assertRaises(ValueError):
                aco.parse_manifest(value, "nyu_aco000001", "a" * 64)
        value = manifest(); value["id"] = value["id"].replace("000001", "999999")
        with self.assertRaises(ValueError):
            aco.parse_manifest(value, "nyu_aco000001", "a" * 64)

    def test_aco_keeps_duplicate_occurrences_but_flags_them(self):
        value = manifest(); value["items"].append(copy.deepcopy(value["items"][0]))
        book, rows = aco.parse_manifest(value, "nyu_aco000001", "a" * 64)
        self.assertEqual(len(rows), 2)
        self.assertNotEqual(rows[0]["page_occurrence_id"], rows[1]["page_occurrence_id"])
        self.assertEqual(book["duplicate_canvas_id_count"], 1)
        self.assertIn("duplicate_image_service_in_manifest", rows[1]["quality_flags"])
        self.assertFalse(rows[0]["physical_page_uniqueness_verified"])

    def test_aco_missing_statement_or_new_edition_is_review_only(self):
        value = manifest(); value.pop("requiredStatement")
        book, rows = aco.parse_manifest(value, "nyu_aco000001", "a" * 64)
        self.assertTrue(book["eligibility_proposal"].startswith("review_"))
        value = manifest(); value["metadata"][1]["value"]["en"] = ["1955"]
        book, rows = aco.parse_manifest(value, "nyu_aco000001", "a" * 64)
        self.assertTrue(book["eligibility_proposal"].startswith("review_"))
        self.assertIsNone(book["source_rights_uri"])
        self.assertFalse(book["contributor_life_dates_independently_verified"])

    def test_aco_conflicting_license_and_ambiguous_years_are_held(self):
        for uri in ("https://creativecommons.org/licenses/by-nc/4.0/", "https://example.com/unknown-rights", {"bad": "object"}):
            value = manifest(); value["rights"] = uri
            book, _ = aco.parse_manifest(value, "nyu_aco000001", "a" * 64)
            self.assertTrue(book["eligibility_proposal"].startswith("review_"))
        value = manifest(); value["metadata"][1]["value"]["en"] = ["1901-1905"]
        book, _ = aco.parse_manifest(value, "nyu_aco000001", "a" * 64)
        self.assertIn("multiple_publication_year_tokens", book["quality_flags"])
        self.assertTrue(book["eligibility_proposal"].startswith("review_"))


if __name__ == "__main__":
    unittest.main()
