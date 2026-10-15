"""Source-independent engineering fixtures; these are NOT corpus observations."""
from __future__ import annotations

import copy
import csv
import hashlib
import io
import math
import os
import json
import shutil
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

from validate import (
    QualityPolicy, audit_collection, canonical_ids, document_split,
    image_fingerprints, normalized_text_hash, text_metrics, validate_page,
)


def png_bytes(color="white", size=(600,400), compress_level=1):
    from PIL import Image, ImageDraw
    image = Image.new("RGB", size, color)
    ImageDraw.Draw(image).rectangle((35,30,150,45),fill="black")
    out=io.BytesIO(); image.save(out, format="PNG",compress_level=compress_level)
    return out.getvalue()


def fixture_page(*, document="fixture-book", canvas="https://example.org/canvas/1", profile="long-edge-600/jpeg", data=None):
    """Invented validation record, deliberately using reserved example.org URLs."""
    data = data or png_bytes()
    fingerprint=image_fingerprints(data)
    words=[]; lines=[]
    # Repeated fixture words test structure, never training-data quality or yield.
    for line_index in range(3):
        ids=[]
        for j,text in enumerate(["هذه","صفحة","اختبار","مستقلة","للتحقق","من","البيانات","فقط"]):
            wid=len(words); ids.append(wid)
            right=570-j*65
            words.append({"id":wid,"text":text,"bbox":[right-55,40+line_index*70,right,65+line_index*70],"confidence":90.0+j,"line_id":line_index})
        lines.append({"id":line_index,"text":" ".join(words[i]["text"] for i in ids),"bbox":[60,40+line_index*70,570,65+line_index*70],"word_ids":ids,"order_index":line_index,"order_method":"tesseract_hierarchy"})
    logical,page=canonical_ids("fixture",document,canvas,profile)
    text="\n".join(x["text"] for x in lines)
    return {
        "page_id":page,"logical_page_id":logical,"source_collection":"fixture","source_document_id":document,
        "document_id":document,"work_group_id":document,"split":"train","image_request_profile":profile,
        "image":{"bytes":data,"path":None},"image_sha256":fingerprint["image_sha256"],
        "width":fingerprint["width"],"height":fingerprint["height"],
        "source_url":"https://example.org/book/1","source_manifest_url":"https://example.org/book/1/manifest.json",
        "source_manifest_sha256":"a"*64,"source_canvas_id":canvas,"source_page_label":"1","source_page_index":0,
        "source_image_url":"https://example.org/image/1/full/600,/0/default.jpg","source_image_width":1200,"source_image_height":800,
        "source_rights":"Fixture only, not a source license","source_rights_url":"https://example.org/fixture-license",
        "source_rights_basis":"source-independent engineering fixture; never included in corpus",
        "page_text":text,"text_source":"tesseract","annotation_review":"machine_generated","reference_text":None,
        "words":words,"lines":lines,"mean_word_confidence":math.fsum(w["confidence"] for w in words)/len(words),
        "arabic_char_fraction":text_metrics(text)["arabic_char_fraction"],"words_count":len(words),"page_status":"ok",
    }


def error_codes(row):
    return {x["code"] for x in validate_page(row)["errors"]}


def write_native_pdf(path: Path):
    """A tiny original native-text PDF, avoiding a font/package/network dependency."""
    stream=b"BT /F1 18 Tf 36 160 Td (Independent PDF fixture 2026) Tj 0 -35 Td (Values 125 and 3.5 remain visible) Tj ET"
    objects=[b"<< /Type /Catalog /Pages 2 0 R >>",b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 400 220] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",b"<< /Length "+str(len(stream)).encode()+b" >>\nstream\n"+stream+b"\nendstream"]
    data=bytearray(b"%PDF-1.4\n"); offsets=[0]
    for i,obj in enumerate(objects,1):
        offsets.append(len(data));data.extend(f"{i} 0 obj\n".encode()+obj+b"\nendobj\n")
    xref=len(data);data.extend(f"xref\n0 {len(objects)+1}\n0000000000 65535 f \n".encode())
    for offset in offsets[1:]:data.extend(f"{offset:010d} 00000 n \n".encode())
    data.extend(f"trailer << /Size {len(objects)+1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode());path.write_bytes(data)


class SemanticValidationTests(unittest.TestCase):
    def test_valid_rtl_record_and_order_is_not_guessed_from_x(self):
        row=fixture_page();before=copy.deepcopy(row)
        row["lines"]=list(reversed(row["lines"]))
        report=validate_page(row)
        self.assertTrue(report["valid"],report["errors"])
        self.assertTrue(report["curated_eligible"])
        self.assertEqual(row["page_text"],before["page_text"])
        self.assertEqual(report["quality_claim"],"mechanical_screen_not_transcription_accuracy")

    def test_profile_changes_variant_not_physical_page_and_hash_tuple_unambiguous(self):
        a=canonical_ids("a|b","c","d","profile1")
        b=canonical_ids("a","b|c","d","profile1")
        c=canonical_ids("a|b","c","d","profile2")
        self.assertNotEqual(a[0],b[0]);self.assertEqual(a[0],c[0]);self.assertNotEqual(a[1],c[1])

    def test_script_fraction_is_not_language_and_preserves_digits(self):
        self.assertEqual(text_metrics("علم AI ٢٠٢٤ 2024")["arabic_char_fraction"],3/5)
        self.assertEqual(text_metrics("٢٠٢٤ 2024")["arabic_char_fraction"],0)
        self.assertEqual(text_metrics("فارسی اردو")["arabic_char_fraction"],1)

    def test_text_hash_only_nfc_and_whitespace_no_arabic_semantic_folding(self):
        self.assertEqual(normalized_text_hash("أ  ب",minimum_characters=1),normalized_text_hash("ا\u0654\nب",minimum_characters=1))
        for a,b in [("إلى","الى"),("عَلَم","علم"),("كتاب١","كتاب1"),("ـعلم","علم")]:
            self.assertNotEqual(normalized_text_hash(a,minimum_characters=1),normalized_text_hash(b,minimum_characters=1))
        self.assertIsNone(normalized_text_hash(" ",minimum_characters=1))

    def test_invalid_geometry_is_not_clipped_or_accepted(self):
        for bbox in [[-1,0,30,20],[1,1,1,4],[0,0,601,400],[0,0,float('nan'),10],[0,False,10,20]]:
            row=fixture_page();row["words"][0]["bbox"]=bbox
            self.assertFalse(validate_page(row)["valid"])
            self.assertEqual(row["words"][0]["bbox"],bbox)

    def test_word_references_partition_and_line_containment(self):
        row=fixture_page();row["lines"][0]["word_ids"].append(0)
        self.assertIn("word_membership_not_partition",error_codes(row))
        row=fixture_page();row["lines"][0]["word_ids"].remove(0)
        self.assertIn("word_membership_not_partition",error_codes(row))
        row=fixture_page();row["lines"][0]["bbox"]=[100,40,570,65]
        self.assertIn("word_outside_line",error_codes(row))

    def test_order_must_be_permutation_and_page_text_must_follow_it(self):
        row=fixture_page();row["lines"][1]["order_index"]=0
        self.assertIn("order_not_permutation",error_codes(row))
        row=fixture_page();row["page_text"]="نص آخر";row["arabic_char_fraction"]=1.0
        self.assertIn("page_text_mismatch",error_codes(row))

    def test_original_engine_words_cannot_disappear_during_box_reconstruction(self):
        row=fixture_page();row["lines"][0]["engine_line_text"]=row["lines"][0]["text"]+" محذوف"
        self.assertIn("engine_text_roundtrip_mismatch",error_codes(row))

    def test_confidence_sentinel_nan_probability_claim_and_mean(self):
        for value in [-1,101,float('nan'),float('inf')]:
            row=fixture_page();row["words"][0]["confidence"]=value
            self.assertIn("invalid_confidence",error_codes(row))
        row=fixture_page();row["mean_word_confidence"]=0.93
        self.assertIn("mean_confidence_mismatch",error_codes(row))
        row=fixture_page();row["annotation_review"]="source_community_proofread"
        self.assertIn("unsupported_review_claim",error_codes(row))

    def test_apple_scores_have_line_basis_and_never_become_word_gold(self):
        row=fixture_page();row["text_source"]="apple_vision"
        for line in row["lines"]:
            line.update(order_method="apple_vision_observation_order",confidence=50.0)
        for word in row["words"]:
            word.update(confidence=50.0,confidence_basis="line_candidate_uncalibrated")
        row["mean_word_confidence"]=50.0
        report=validate_page(row);self.assertTrue(report["valid"],report["errors"])
        row["words"][0]["confidence_basis"]="word_probability"
        self.assertIn("missing_confidence_basis",error_codes(row))
        row["words"][0]["confidence_basis"]="line_candidate_uncalibrated";row["words"][0]["confidence"]=90
        self.assertIn("line_candidate_score_mismatch",error_codes(row))

    def test_unicode_flags_preserve_source_and_hard_fail_controls(self):
        row=fixture_page();row["page_text"]+="\x00"
        self.assertIn("bad_unicode",error_codes(row))
        row=fixture_page();row["page_text"]+="\ud800"
        self.assertIn("bad_unicode",error_codes(row))
        report=validate_page(fixture_page())
        self.assertEqual(report["metrics"]["replacement_characters"],0)
        self.assertEqual(text_metrics("علم\ufffd\ue000\u202e")["private_use_characters"],1)
        self.assertEqual(text_metrics("علم\ufffd\ue000\u202e")["bidi_override_or_isolate_characters"],1)

    def test_no_text_page_not_confused_with_missing_image_or_zero_confidence(self):
        row=fixture_page();row.update(page_text="",words=[],lines=[],words_count=0,mean_word_confidence=None,arabic_char_fraction=0.0,text_source="none",page_status="no_text")
        report=validate_page(row);self.assertTrue(report["valid"],report["errors"]);self.assertFalse(report["curated_eligible"])
        row["mean_word_confidence"]=0
        self.assertIn("mean_confidence_mismatch",error_codes(row))

    def test_encoded_vs_decoded_image_identity_and_alpha(self):
        a=image_fingerprints(png_bytes(compress_level=1));b=image_fingerprints(png_bytes(compress_level=9))
        self.assertNotEqual(a["image_sha256"],b["image_sha256"]);self.assertEqual(a["pixel_sha256"],b["pixel_sha256"])
        from PIL import Image
        encoded=[]
        for alpha in [0,255]:
            f=io.BytesIO();Image.new("RGBA",(2,2),(255,0,0,alpha)).save(f,format="PNG");encoded.append(image_fingerprints(f.getvalue()))
        self.assertNotEqual(encoded[0]["pixel_sha256"],encoded[1]["pixel_sha256"])

    def test_published_raster_hash_and_dimensions_are_bound(self):
        row=fixture_page();row["image_sha256"]="0"*64
        self.assertIn("image_hash_mismatch",error_codes(row))
        row=fixture_page();row["width"]=601
        self.assertIn("image_dimension_mismatch",error_codes(row))

    def test_source_url_credentials_and_missing_provenance_fail(self):
        for value in ["http://example.org/1","https://user:pass@example.org/1","https://example.org/?token=secret","https://example.org/?X-Amz-Signature=secret"]:
            row=fixture_page();row["source_image_url"]=value
            self.assertIn("unsafe_or_missing_url",error_codes(row))
        row=fixture_page();row["source_manifest_sha256"]=""
        self.assertIn("invalid_sha256",error_codes(row))

    def test_document_split_namespace_determinism_and_split_audit(self):
        self.assertEqual(document_split("a","book",seed="fixture-v1"),document_split("a","book",seed="fixture-v1"))
        a=fixture_page();b=fixture_page(canvas="https://example.org/canvas/2",data=png_bytes("gray"));b["split"]="test"
        result=audit_collection([a,b])
        self.assertFalse(result["document_and_exact_duplicate_split_audit_passed"])
        self.assertEqual(result["duplicate_audit"]["doc"]["cross_split_groups"],1)

    def test_long_text_duplicate_across_different_books_not_silently_dropped(self):
        a=fixture_page(document="a");b=fixture_page(document="b",data=png_bytes("gray"));b["split"]="validation"
        result=audit_collection([a,b])
        self.assertEqual(result["rows"],2);self.assertEqual(result["unique_logical_pages"],2)
        self.assertEqual(result["duplicate_audit"]["text_hash"]["cross_split_groups"],1)
        self.assertEqual(result["duplicate_audit"]["image_hash"]["groups"],0)

    def test_profile_variants_never_inflate_physical_count(self):
        a=fixture_page(profile="small");b=fixture_page(profile="large")
        result=audit_collection([a,b])
        self.assertEqual(result["rows"],2);self.assertEqual(result["unique_logical_pages"],1)
        self.assertTrue(result["duplicate_page_ids_absent"])

    def test_invalid_split_or_identity_cannot_pass_collection_audit(self):
        row=fixture_page();row["split"]=None
        result=audit_collection([row]);self.assertEqual(result["invalid_identity_rows"],1)
        self.assertFalse(result["document_and_exact_duplicate_split_audit_passed"])


REPO_ROOT=Path(__file__).resolve().parents[3]
MODEL_DIR=Path(os.environ.get("ATLAS_TESSDATA_DIR",str(REPO_ROOT/"output/arabic-document-atlas/private/models")))
VISION_BINARY=Path(os.environ.get("ATLAS_VISION_OCR",str(REPO_ROOT/"output/arabic-document-atlas/private/vision_ocr")))
ARABIC_FONT=Path(os.environ.get("ATLAS_ARABIC_FONT","/System/Library/Fonts/GeezaPro.ttc"))


def write_arabic_fixture(path):
    from PIL import Image,ImageDraw,ImageFont,features
    if not features.check_feature('raqm'):
        raise unittest.SkipTest('Arabic fixture generation requires Pillow with RAQM; actual corpus OCR does not')
    image=Image.new("RGB",(1600,500),"white");draw=ImageDraw.Draw(image);font=ImageFont.truetype(str(ARABIC_FONT),52)
    text=["هذه صفحة عربية لاختبار جودة التعرف على النصوص","تحتوي الصفحة على كلمات وأرقام واضحة ٢٠٢٦","هذا مثال مستقل للتحقق من حدود الكلمات"]
    for i,line in enumerate(text):draw.text((1510,80+i*110),line,font=font,fill="black",anchor="ra",direction="rtl",language="ar")
    image.save(path)


def record_from_extraction(extraction, image_bytes, source):
    row=fixture_page(data=image_bytes)
    row.update({k:extraction[k] for k in ["words","lines","page_text","width","height"]})
    row.update(text_source=source,words_count=len(row["words"]),mean_word_confidence=math.fsum(w["confidence"] for w in row["words"])/len(row["words"]) if row["words"] else None,arabic_char_fraction=text_metrics(row["page_text"])["arabic_char_fraction"])
    return row


class ArabicEngineIntegration(unittest.TestCase):
    @unittest.skipUnless(shutil.which("tesseract") and (MODEL_DIR/"ara.traineddata").is_file() and ARABIC_FONT.is_file(),"Pinned Arabic Tesseract model/local test font absent")
    def test_actual_arabic_tesseract_words_boxes_and_roundtrip(self):
        with tempfile.TemporaryDirectory(prefix="atlas-arabic-engine-") as tmp:
            image=Path(tmp)/"independent.png";write_arabic_fixture(image)
            # A standalone pinned model directory has no configs/tsv file.
            out=subprocess.run(["tesseract",str(image),"stdout","--tessdata-dir",str(MODEL_DIR),"-l","ara+eng","--oem","1","--psm","6","-c","tessedit_create_tsv=1"],check=True,capture_output=True,text=True).stdout
            words=[];line_map={};lines=[]
            for item in csv.DictReader(io.StringIO(out),delimiter="\t"):
                if item["level"]!="5" or not item.get("text","").strip():continue
                group=tuple(item[k] for k in ["block_num","par_num","line_num"])
                if group not in line_map:
                    line_map[group]=len(lines);lines.append({"id":f"l{len(lines)}","word_ids":[],"order_index":len(lines),"order_method":"tesseract_hierarchy"})
                line=lines[line_map[group]];x,y,w,h=[int(item[k]) for k in ["left","top","width","height"]]
                word={"id":f"w{len(words)}","text":item["text"],"bbox":[x,y,x+w,y+h],"confidence":float(item["conf"]),"line_id":line["id"]}
                words.append(word);line["word_ids"].append(word["id"])
            wm={w["id"]:w for w in words}
            for line in lines:
                members=[wm[k] for k in line["word_ids"]];line["text"]=" ".join(w["text"] for w in members)
                line["bbox"]=[min(w["bbox"][0] for w in members),min(w["bbox"][1] for w in members),max(w["bbox"][2] for w in members),max(w["bbox"][3] for w in members)]
            extraction={"words":words,"lines":lines,"page_text":"\n".join(l["text"] for l in lines),"width":1600,"height":500}
            self.assertGreater(len(words),15);self.assertGreater(text_metrics(extraction["page_text"])["arabic_letters"],50)
            report=validate_page(record_from_extraction(extraction,image.read_bytes(),"tesseract"))
            self.assertTrue(report["valid"],report["errors"])

    @unittest.skipUnless(VISION_BINARY.is_file() and ARABIC_FONT.is_file(),"Local macOS Vision binary/test font absent")
    def test_actual_apple_arabic_scores_and_geometry_contract(self):
        with tempfile.TemporaryDirectory(prefix="atlas-vision-engine-") as tmp:
            image=Path(tmp)/"independent.png";write_arabic_fixture(image)
            result=json.loads(subprocess.run([str(VISION_BINARY),str(image)],check=True,capture_output=True,text=True).stdout)
            self.assertTrue(result["ok"]);extraction=result["result"]
            self.assertGreater(len(extraction["words"]),15)
            # Explicit union records a containing envelope without changing word boxes.
            wm={w["id"]:w for w in extraction["words"]}
            for line in extraction["lines"]:
                boxes=[line["bbox"]]+[wm[k]["bbox"] for k in line["word_ids"]]
                line["bbox"]=[min(b[0] for b in boxes),min(b[1] for b in boxes),max(b[2] for b in boxes),max(b[3] for b in boxes)]
            report=validate_page(record_from_extraction(extraction,image.read_bytes(),"apple_vision"))
            self.assertTrue(report["valid"],report["errors"])


@unittest.skipUnless(shutil.which("pdftotext") and shutil.which("pdftoppm") and shutil.which("tesseract"),"Poppler/Tesseract CLI integration tools absent")
class NativePDFAndOCRIntegration(unittest.TestCase):
    def test_original_native_pdf_bbox_render_and_actual_ocr(self):
        from PIL import Image
        with tempfile.TemporaryDirectory(prefix="atlas-quality-fixture-") as tmp:
            root=Path(tmp);pdf=root/"native.pdf";write_native_pdf(pdf)
            xml=subprocess.run(["pdftotext","-bbox",str(pdf),"-"],check=True,capture_output=True,text=True).stdout
            element=ET.fromstring(xml);page=next(x for x in element.iter() if x.tag.endswith("page"))
            self.assertEqual(float(page.attrib["width"]),400)
            extracted=[x for x in element.iter() if x.tag.endswith("word")]
            self.assertIn("2026",[x.text for x in extracted]);self.assertIn("3.5",[x.text for x in extracted])
            self.assertTrue(all(0<=float(w.attrib["xMin"])<float(w.attrib["xMax"])<=400 for w in extracted))
            subprocess.run(["pdftoppm","-r","144","-singlefile","-png",str(pdf),str(root/"raster")],check=True,capture_output=True)
            raster=root/"raster.png"
            with Image.open(raster) as image:self.assertEqual(image.size,(800,440))
            tsv=subprocess.run(["tesseract",str(raster),"stdout","-l","eng","--psm","6","tsv"],check=True,capture_output=True,text=True).stdout
            rows=[x.split("\t",11) for x in tsv.splitlines()[1:]]
            words=[x for x in rows if len(x)==12 and x[0]=="5" and x[11].strip()]
            self.assertIn("2026",[x[11] for x in words]);self.assertGreater(len(words),5)
            self.assertTrue(all(0<=float(x[10])<=100 for x in words))
            self.assertTrue(all(0<=int(x[6])<int(x[6])+int(x[8])<=800 and 0<=int(x[7])<int(x[7])+int(x[9])<=440 for x in words))
            # Native PDF points and rendered OCR pixels are different coordinate spaces.
            self.assertAlmostEqual(float(extracted[0].attrib["xMin"])*2,72,places=3)


if __name__ == "__main__":
    unittest.main()
