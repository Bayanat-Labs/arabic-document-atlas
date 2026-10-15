"""Structural and conservative content checks, with bounded-memory split auditing.

Confidence, Unicode plausibility, and valid boxes do not establish transcription
accuracy. Functions preserve input text and never silently repair source records.
"""
from __future__ import annotations

import hashlib
import io
import json
import math
import re
import sqlite3
import tempfile
import unicodedata
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import parse_qsl, urlsplit


SHA256 = re.compile(r"^[0-9a-f]{64}$")
TEXT_SOURCES = {"tesseract", "apple_vision", "native_pdf", "source_proofread", "none"}
MACHINE_SOURCES = {"tesseract", "apple_vision", "native_pdf"}
OCR_SOURCES = {"tesseract", "apple_vision"}
REVIEWS = {"machine_generated", "source_community_proofread"}
SPLITS = {"train", "validation", "test"}
URL_FIELDS = (
    "source_url", "source_manifest_url", "source_image_url", "source_rights_url",
)
STRING_FIELDS = (
    "source_collection", "source_document_id", "document_id", "work_group_id",
    "image_request_profile", "source_canvas_id", "source_page_label",
    "source_rights", "source_rights_basis", "page_status",
)
MULTILINE_PROVENANCE_FIELDS = {"source_rights", "source_rights_basis", "source_page_label"}
HASH_FIELDS = ("page_id", "logical_page_id", "image_sha256", "source_manifest_sha256")
BIDI_OVERRIDES = set("\u202a\u202b\u202c\u202d\u202e\u2066\u2067\u2068\u2069")
SECRET_KEYS = {"token", "access_token", "api_key", "apikey", "authorization", "key", "signature", "credential", "password", "secret"}


@dataclass(frozen=True)
class QualityPolicy:
    """Transparent screening thresholds, not calibrated quality guarantees."""
    min_curated_words: int = 20
    min_curated_arabic_letter_fraction: float = 0.5
    max_curated_replacement_fraction: float = 0.005
    min_text_dedup_characters: int = 80
    box_tolerance_pixels: int = 0
    max_image_pixels: int = 25_000_000
    max_issues: int = 100


def _hash_json(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")).hexdigest()


def canonical_ids(source_collection: str, source_document_id: str, source_canvas_id: str, image_request_profile: str) -> tuple[str, str]:
    """Return (logical physical-page ID, image-profile-specific page ID)."""
    values = (source_collection, source_document_id, source_canvas_id, image_request_profile)
    if any(not isinstance(v, str) or not v.strip() for v in values):
        raise ValueError("Canonical ID components must be nonempty strings")
    logical = _hash_json(list(values[:3]))
    return logical, _hash_json([logical, image_request_profile])


def document_split(source_collection: str, source_document_id: str, *, seed: str, train_fraction: float = 0.9, validation_fraction: float = 0.05) -> str:
    """Deterministic DOCUMENT partition, independent of page order/count.

    Does not claim different editions of the same work are held apart. Exact
    duplicate components must additionally be audited before an evaluation split.
    """
    if not all(isinstance(v, str) and v for v in (source_collection, source_document_id, seed)):
        raise ValueError("Namespace, document ID and versioned split seed are required")
    if not (0 < train_fraction < 1 and 0 <= validation_fraction < 1 and train_fraction + validation_fraction < 1):
        raise ValueError("Split fractions must leave nonempty train and test intervals")
    bucket = int(_hash_json([seed, source_collection, source_document_id])[:16], 16) / 2**64
    return "train" if bucket < train_fraction else "validation" if bucket < train_fraction + validation_fraction else "test"


def _normalized_text(text: str) -> str:
    # Do NOT erase Arabic distinctions, digits, punctuation, diacritics, or tatweel.
    return " ".join(unicodedata.normalize("NFC", text).split())


def normalized_text_hash(text: str, *, minimum_characters: int = 80) -> str | None:
    normalized = _normalized_text(text)
    if len(normalized) < minimum_characters:
        return None
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def text_metrics(text: str) -> dict[str, Any]:
    """Script statistics, not an Arabic-language identification model."""
    letters = [c for c in text if unicodedata.category(c).startswith("L")]
    arabic_letters = sum("ARABIC" in unicodedata.name(c, "") for c in letters)
    replacement = text.count("\ufffd")
    return {
        "characters": len(text), "letters": len(letters), "arabic_letters": arabic_letters,
        "arabic_char_fraction": arabic_letters / len(letters) if letters else 0.0,
        "replacement_characters": replacement,
        "replacement_fraction": replacement / len(text) if text else 0.0,
        "private_use_characters": sum(unicodedata.category(c) == "Co" for c in text),
        "surrogate_characters": sum(unicodedata.category(c) == "Cs" for c in text),
        "forbidden_control_characters": sum(unicodedata.category(c) == "Cc" and c not in "\n\r\t" for c in text),
        "bidi_override_or_isolate_characters": sum(c in BIDI_OVERRIDES for c in text),
        "presentation_form_letters": sum(("\ufb50" <= c <= "\ufdff") or ("\ufe70" <= c <= "\ufeff") for c in text),
        "rtl_strong_characters": sum(unicodedata.bidirectional(c) in {"R", "AL"} for c in text),
        "ltr_strong_characters": sum(unicodedata.bidirectional(c) == "L" for c in text),
    }


def image_fingerprints(data: bytes, *, max_pixels: int = 25_000_000) -> dict[str, Any]:
    """Encoded-byte SHA, canonical decoded RGBA SHA, and candidate-only dHash.

    Requires Pillow only when image bytes are actually inspected. Color/mode and
    dimensions are bound into the pixel hash. No perceptual match is auto-deduped.
    """
    from PIL import Image
    if not isinstance(data, bytes) or not data:
        raise ValueError("Image must be nonempty encoded bytes")
    with Image.open(io.BytesIO(data)) as image:
        width, height = image.size
        if width <= 0 or height <= 0 or width * height > max_pixels:
            raise ValueError("Image dimensions exceed the configured decode bound")
        if getattr(image, "n_frames", 1) != 1:
            raise ValueError("One record must encode one still page image")
        image.load()
        rgba = image.convert("RGBA")
        prefix = json.dumps(["RGBA", width, height], separators=(",", ":")).encode() + b"\0"
        digest = hashlib.sha256(prefix)
        digest.update(rgba.tobytes())
        background = Image.new("RGBA", rgba.size, (255,255,255,255))
        rgb = Image.alpha_composite(background, rgba).convert("RGB")
        small = rgb.convert("L").resize((9, 8), Image.Resampling.LANCZOS)
        pixels = list(small.get_flattened_data() if hasattr(small,"get_flattened_data") else small.getdata())
        bits = 0
        for y in range(8):
            for x in range(8):
                bits = (bits << 1) | int(pixels[y * 9 + x] > pixels[y * 9 + x + 1])
        return {"image_sha256": hashlib.sha256(data).hexdigest(), "pixel_sha256": digest.hexdigest(), "dhash64": f"{bits:016x}", "width": width, "height": height, "format": image.format}


def _int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _finite(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _uid(value: Any) -> tuple[type, Any] | None:
    return (type(value), value) if (_int(value) or isinstance(value, str) and bool(value)) else None


def _safe_url(value: Any) -> bool:
    if not isinstance(value, str) or any(c.isspace() for c in value):
        return False
    try:
        u = urlsplit(value)
        return (u.scheme == "https" and bool(u.hostname) and not u.username and not u.password
                and not any(k.lower() in SECRET_KEYS or k.lower().startswith("x-amz-") for k, _ in parse_qsl(u.query)))
    except ValueError:
        return False


def provenance_issues(row: Mapping[str, Any]) -> list[dict[str, str]]:
    """Shared early metadata check; preserve ordinary human-text whitespace.

    Identifiers and URLs remain strict. Newline/tab/carriage-return are legitimate
    in source statements/labels, while NUL/other controls and surrogates fail.
    """
    issues=[]
    for name in STRING_FIELDS:
        value=row.get(name)
        if not isinstance(value,str) or not value.strip():
            issues.append({"code":"required_string","path":name,"message":"Nonempty source string required"})
        elif any(unicodedata.category(c)=="Cs" or (unicodedata.category(c)=="Cc" and not (name in MULTILINE_PROVENANCE_FIELDS and c in "\n\r\t")) for c in value):
            issues.append({"code":"bad_provenance_unicode","path":name,"message":"Source identifiers cannot contain controls; source statements/labels allow ordinary newline, carriage-return and tab only"})
    for name in URL_FIELDS:
        if not _safe_url(row.get(name)):
            issues.append({"code":"unsafe_or_missing_url","path":name,"message":"Require public HTTPS URL without userinfo or credential/signature parameters"})
    return issues


def validate_page(row: Mapping[str, Any], *, image_bytes: bytes | None = None, policy: QualityPolicy | None = None) -> dict[str, Any]:
    """Validate one normalized core record without modifying it.

    `valid` concerns structure/provenance consistency; `curated_eligible` is an
    explicit mechanical screen and never a statement of OCR correctness.
    """
    policy = policy or QualityPolicy()
    errors, warnings = [], []
    counts = Counter()
    def issue(target, code, path, message):
        counts["errors" if target is errors else "warnings"] += 1
        if len(target) < policy.max_issues:
            target.append({"code": code, "path": path, "message": message})
    def error(code, path, message): issue(errors, code, path, message)
    def warn(code, path, message): issue(warnings, code, path, message)
    if not isinstance(row, Mapping):
        return {"valid": False, "curated_eligible": False, "errors": [{"code": "record_type", "path": "", "message": "Expected a mapping"}], "warnings": [], "metrics": {}}
    for item in provenance_issues(row):
        error(item["code"],item["path"],item["message"])
    for name in HASH_FIELDS:
        if not isinstance(row.get(name), str) or not SHA256.fullmatch(row[name]): error("invalid_sha256", name, "Require lowercase SHA256 hex")
    image_feature = row.get("image")
    if not isinstance(image_feature, Mapping) or not (
        isinstance(image_feature.get("bytes"), bytes) and bool(image_feature["bytes"])
        or isinstance(image_feature.get("path"), str) and bool(image_feature["path"])
    ):
        error("missing_image_feature", "image", "Require a Hugging Face image mapping with bytes or a staging path")
    if isinstance(image_feature, Mapping) and isinstance(image_feature.get("bytes"), bytes) and image_bytes is not None and image_feature["bytes"] != image_bytes:
        error("image_feature_bytes_mismatch", "image", "Supplied verification bytes disagree with the actual embedded image bytes")
    if all(isinstance(row.get(k), str) and row[k] for k in ("source_collection", "source_document_id", "source_canvas_id", "image_request_profile")):
        try:
            logical, page = canonical_ids(row["source_collection"], row["source_document_id"], row["source_canvas_id"], row["image_request_profile"])
            if row.get("logical_page_id") != logical: error("logical_id_mismatch", "logical_page_id", "Logical page ID must bind source collection/document/canvas")
            if row.get("page_id") != page: error("page_id_mismatch", "page_id", "Page ID must bind logical page ID and image request profile")
        except (ValueError, UnicodeError): error("invalid_id_components", "page_id", "Canonical components cannot be encoded as valid UTF-8")
    for name in ("width", "height", "source_image_width", "source_image_height"):
        if not _int(row.get(name)) or row[name] <= 0: error("invalid_dimension", name, "Positive integer dimension required")
    if not _int(row.get("source_page_index")) or row["source_page_index"] < 0: error("invalid_page_index", "source_page_index", "Zero-based nonnegative source index required")
    if row.get("split") not in SPLITS: error("invalid_split", "split", "Expected train, validation, or test")
    source = row.get("text_source")
    if source not in TEXT_SOURCES: error("invalid_text_source", "text_source", "Unrecognized extraction source")
    if row.get("annotation_review") not in REVIEWS: error("invalid_annotation_review", "annotation_review", "Review status is provenance, not a model score")
    if source in MACHINE_SOURCES and row.get("annotation_review") != "machine_generated": error("unsupported_review_claim", "annotation_review", "Machine extraction must not inherit community proofreading of separate reference text")
    if source == "source_proofread":
        if row.get("annotation_review") != "source_community_proofread" or not row.get("proofread_status"): error("missing_proofread_provenance", "proofread_status", "Community-proofread text requires its actual source status")
        warn("reference_not_geometry_gold", "text_source", "Source proofreading does not verify alignment or bounding boxes")
    text = row.get("page_text")
    if not isinstance(text, str): error("invalid_text", "page_text", "Expected string, including empty string for no-text pages"); text = ""
    metrics = text_metrics(text)
    for field in ("surrogate_characters", "forbidden_control_characters"):
        if metrics[field]: error("bad_unicode", "page_text", f"Contains {field}")
    for field in ("replacement_characters", "private_use_characters", "bidi_override_or_isolate_characters", "presentation_form_letters"):
        if metrics[field]: warn(field, "page_text", "Preserved and flagged; not silently normalized or deleted")
    declared_fraction = row.get("arabic_char_fraction")
    if not _finite(declared_fraction) or not math.isclose(declared_fraction, metrics["arabic_char_fraction"], abs_tol=1e-9): error("script_fraction_mismatch", "arabic_char_fraction", "Expected Arabic-script letters / all Unicode letters, with zero for no letters")
    width, height = row.get("width"), row.get("height")
    dimensions_ok = _int(width) and _int(height) and width > 0 and height > 0
    def box(value, path):
        if not isinstance(value, (list, tuple)) or len(value) != 4 or not all(_int(x) for x in value): error("invalid_bbox", path, "Require integer [x0,y0,x1,y1]"); return False
        x0,y0,x1,y1 = value
        if not dimensions_ok or not (0 <= x0 < x1 <= width and 0 <= y0 < y1 <= height): error("bbox_outside_image", path, "Half-open box must have positive area and lie inside published raster"); return False
        return True
    words, lines = row.get("words"), row.get("lines")
    if not isinstance(words, list): error("invalid_words", "words", "Expected list"); words = []
    if not isinstance(lines, list): error("invalid_lines", "lines", "Expected list"); lines = []
    if not _int(row.get("words_count")) or row["words_count"] != len(words): error("word_count_mismatch", "words_count", "Must equal actual word records")
    word_map, confidences = {}, []
    for i,w in enumerate(words):
        path = f"words[{i}]"
        if not isinstance(w, Mapping): error("invalid_word", path, "Expected word mapping"); continue
        uid = _uid(w.get("id"))
        if uid is None or uid in word_map: error("duplicate_or_invalid_word_id", path+".id", "Word IDs must be unique per page")
        else: word_map[uid] = w
        if not isinstance(w.get("text"), str) or not w["text"].strip(): error("empty_word", path+".text", "Word text must be nonempty")
        elif text_metrics(w["text"])["surrogate_characters"] or text_metrics(w["text"])["forbidden_control_characters"]: error("bad_unicode", path+".text", "Invalid Unicode in word")
        box(w.get("bbox"), path+".bbox")
        confidence = w.get("confidence")
        if source in OCR_SOURCES:
            if not _finite(confidence) or not 0 <= confidence <= 100: error("invalid_confidence", path+".confidence", "Recorded OCR score is 0..100; Tesseract -1 is a non-word sentinel")
            else: confidences.append(float(confidence))
            if source == "apple_vision" and w.get("confidence_basis") != "line_candidate_uncalibrated": error("missing_confidence_basis", path+".confidence_basis", "Apple word records inherit their line candidate score; it is not per-word confidence")
        elif confidence is not None: error("unsupported_confidence", path+".confidence", "Native/reference text uses null confidence")
    line_ids, orders, seen_words, joined_lines = set(), [], Counter(), []
    for i,line in enumerate(lines):
        path = f"lines[{i}]"
        if not isinstance(line, Mapping): error("invalid_line", path, "Expected line mapping"); continue
        uid = _uid(line.get("id"))
        if uid is None or uid in line_ids: error("duplicate_or_invalid_line_id", path+".id", "Line IDs must be unique per page")
        else: line_ids.add(uid)
        valid_box = box(line.get("bbox"), path+".bbox")
        order = line.get("order_index")
        if not _int(order) or order < 0: error("invalid_order", path+".order_index", "Nonnegative integer order required")
        else: orders.append(order)
        if not isinstance(line.get("order_method"), str) or not line["order_method"]: error("missing_order_method", path+".order_method", "State the origin of machine/source order")
        if source == "tesseract" and line.get("order_method") != "tesseract_hierarchy": error("unsupported_order_claim", path+".order_method", "Tesseract output is its hierarchy, not verified reading order")
        if source == "apple_vision":
            if line.get("order_method") != "apple_vision_observation_order": error("unsupported_order_claim", path+".order_method", "Apple observation order is not verified reading order")
            if not _finite(line.get("confidence")) or not 0 <= line["confidence"] <= 100: error("invalid_line_confidence", path+".confidence", "Apple line candidate score must be represented on the declared 0..100 scale")
        word_ids = line.get("word_ids")
        if not isinstance(word_ids, list): error("invalid_word_refs", path+".word_ids", "Expected list"); word_ids = []
        line_word_text = []
        for ref in word_ids:
            wid = _uid(ref)
            if wid is None or wid not in word_map: error("unknown_word_ref", path+".word_ids", "Line references nonexistent word"); continue
            seen_words[wid] += 1
            w = word_map[wid]
            if _uid(w.get("line_id")) != uid: error("line_membership_mismatch", path, "Word line_id disagrees with containing line")
            if source == "apple_vision" and _finite(w.get("confidence")) and _finite(line.get("confidence")) and not math.isclose(w["confidence"], line["confidence"], abs_tol=1e-6): error("line_candidate_score_mismatch", path, "Inherited word score must match the actual line candidate confidence")
            if isinstance(w.get("text"), str): line_word_text.append(w["text"])
            wb = w.get("bbox")
            if valid_box and isinstance(wb, (list,tuple)) and len(wb)==4 and all(_int(x) for x in wb):
                lb = line["bbox"]; t = policy.box_tolerance_pixels
                if not (lb[0]-t <= wb[0] < wb[2] <= lb[2]+t and lb[1]-t <= wb[1] < wb[3] <= lb[3]+t): error("word_outside_line", path, "Containing line must enclose each word box")
        if not isinstance(line.get("text"), str): error("invalid_line_text", path+".text", "Expected source line string")
        else:
            if isinstance(line.get("engine_line_text"),str) and _normalized_text(line["engine_line_text"]) != _normalized_text(line["text"]): error("engine_text_roundtrip_mismatch", path+".engine_line_text", "Reconstruction must not silently discard observed words without boxes")
            if source in MACHINE_SOURCES and _normalized_text(line["text"]) != _normalized_text(" ".join(line_word_text)): error("line_text_mismatch", path+".text", "Text must follow recorded word references")
            joined_lines.append((order if _int(order) else i, line["text"]))
    if sorted(orders) != list(range(len(lines))): error("order_not_permutation", "lines", "Order indices must be exactly 0..number_of_lines-1")
    if set(seen_words) != set(word_map) or any(n != 1 for n in seen_words.values()): error("word_membership_not_partition", "lines", "Each word must occur in exactly one line")
    if source in MACHINE_SOURCES and _normalized_text(text) != _normalized_text("\n".join(t for _,t in sorted(joined_lines))): error("page_text_mismatch", "page_text", "Page text must follow declared line order")
    mean = row.get("mean_word_confidence")
    expected_mean = math.fsum(confidences)/len(confidences) if confidences else None
    if expected_mean is None:
        if mean is not None: error("mean_confidence_mismatch", "mean_word_confidence", "No numeric word scores means null mean, never zero")
    elif not _finite(mean) or not math.isclose(mean, expected_mean, abs_tol=1e-6): error("mean_confidence_mismatch", "mean_word_confidence", "Expected unweighted mean of valid numeric word scores")
    if source == "none" and (text or words or lines): error("none_source_has_text", "text_source", "No-text source cannot contain extraction records")
    if image_bytes is None and isinstance(row.get("image"), Mapping): image_bytes = row["image"].get("bytes")
    if image_bytes is not None:
        try:
            fingerprints = image_fingerprints(image_bytes, max_pixels=policy.max_image_pixels)
            metrics.update(fingerprints)
            if fingerprints["image_sha256"] != row.get("image_sha256"): error("image_hash_mismatch", "image_sha256", "Encoded image bytes do not match recorded hash")
            if (fingerprints["width"],fingerprints["height"]) != (width,height): error("image_dimension_mismatch", "image", "Coordinates must refer to the published decoded raster")
        except Exception as exc: error("image_decode_failed", "image", f"Image cannot be verified: {type(exc).__name__}: {exc}")
    else: warn("image_not_verified", "image", "Caller did not supply encoded raster bytes")
    curated_reasons = []
    if len(words) < policy.min_curated_words: curated_reasons.append("below_minimum_words")
    if metrics["arabic_char_fraction"] < policy.min_curated_arabic_letter_fraction: curated_reasons.append("below_arabic_script_fraction")
    if metrics["replacement_fraction"] > policy.max_curated_replacement_fraction: curated_reasons.append("replacement_fraction")
    if metrics["private_use_characters"]: curated_reasons.append("private_use_text")
    if metrics["bidi_override_or_isolate_characters"]: curated_reasons.append("explicit_bidi_controls_require_review")
    if not image_bytes: curated_reasons.append("unverified_image_bytes")
    if source == "none": curated_reasons.append("no_text")
    if source == "source_proofread" and words: curated_reasons.append("proofread_geometry_not_independently_verified")
    return {"valid": counts["errors"] == 0, "curated_eligible": counts["errors"] == 0 and not curated_reasons,
            "curated_exclusion_reasons": curated_reasons, "errors": errors, "warnings": warnings,
            "error_count": counts["errors"], "warning_count": counts["warnings"], "metrics": metrics,
            "policy": policy.__dict__, "quality_claim": "mechanical_screen_not_transcription_accuracy"}


def audit_collection(rows: Iterable[Mapping[str, Any]], *, database_path: str | Path | None = None, policy: QualityPolicy | None = None) -> dict[str, Any]:
    """Audit identities and duplicate leakage with disk-backed SQLite indexes.

    Never drops records or equates a perceptual/text match with one physical page.
    A caller can quarantine ambiguous duplicate components from evaluation.
    """
    policy = policy or QualityPolicy()
    temporary = tempfile.TemporaryDirectory(prefix="atlas-quality-") if database_path is None else None
    path = Path(temporary.name)/"audit.sqlite" if temporary else Path(database_path)
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA cache_size=-32768")
        connection.execute("CREATE TABLE IF NOT EXISTS pages (page_id TEXT, logical_id TEXT, doc TEXT, work_group TEXT, split TEXT, image_hash TEXT, pixel_hash TEXT, text_hash TEXT)")
        if connection.execute("SELECT count(*) FROM pages").fetchone()[0]: raise ValueError("Audit database must be empty to avoid counting old runs")
        total = invalid_identity_rows = 0
        for row in rows:
            if any(not isinstance(row.get(k),str) or not SHA256.fullmatch(row[k]) for k in ("page_id","logical_page_id")) or row.get("split") not in SPLITS or not row.get("source_collection") or not row.get("source_document_id"):
                invalid_identity_rows += 1
            try: text_hash = normalized_text_hash(row.get("page_text", ""), minimum_characters=policy.min_text_dedup_characters)
            except (TypeError, UnicodeError): text_hash = None
            image_hash = row.get("image_sha256")
            if not isinstance(image_hash,str) or not SHA256.fullmatch(image_hash): image_hash = None
            connection.execute("INSERT INTO pages VALUES (?,?,?,?,?,?,?,?)", (row.get("page_id"),row.get("logical_page_id"),json.dumps([row.get("source_collection"),row.get("source_document_id")],separators=(",",":")),row.get("work_group_id"),row.get("split"),image_hash,row.get("pixel_sha256"),text_hash))
            total += 1
            if total % 10000 == 0: connection.commit()
        connection.commit()
        for col in ("page_id","logical_id","doc","image_hash","pixel_hash","text_hash"): connection.execute(f"CREATE INDEX IF NOT EXISTS idx_{col} ON pages ({col})")
        def duplicate_groups(col):
            query = f"SELECT {col},count(*),count(DISTINCT split) FROM pages WHERE {col} IS NOT NULL GROUP BY {col} HAVING count(*)>1"
            n = cross = 0; examples = []
            for key,count,splits in connection.execute(query):
                n += 1; cross += splits > 1
                if len(examples)<10: examples.append({"key":key,"rows":count,"split_count":splits})
            return {"groups":n,"cross_split_groups":cross,"examples":examples}
        duplicates = {c:duplicate_groups(c) for c in ("page_id","logical_id","doc","image_hash","pixel_hash","text_hash")}
        unique_logical = connection.execute("SELECT count(DISTINCT logical_id) FROM pages WHERE logical_id IS NOT NULL").fetchone()[0]
        leakage = bool(invalid_identity_rows) or any(v["cross_split_groups"] for v in duplicates.values())
        return {"rows":total,"unique_logical_pages":unique_logical,"invalid_identity_rows":invalid_identity_rows,"duplicate_audit":duplicates,
                "document_and_exact_duplicate_split_audit_passed": not leakage,
                "duplicate_page_ids_absent": duplicates["page_id"]["groups"] == 0,
                "split_claim": "training_only_no_held_out_benchmark" if {r[0] for r in connection.execute("SELECT DISTINCT split FROM pages")} == {"train"} else "document_partition_assignments_not_verified_work_or_edition_held_out",
                "perceptual_duplicates": "candidate_only_not_proven_or_auto_removed"}
    finally:
        connection.close()
        if temporary: temporary.cleanup()
