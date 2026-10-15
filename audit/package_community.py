#!/usr/bin/env python3
"""Package the Wikisource community-text tier. No image bytes, no heldout split."""
from __future__ import annotations

import argparse
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq


SCHEMA = pa.schema([
    ("record_id", pa.string()), ("source", pa.string()), ("source_wiki", pa.string()),
    ("source_page_id", pa.int64()), ("source_title", pa.string()),
    ("source_revision_id", pa.int64()), ("source_parent_revision_id", pa.int64()),
    ("source_revision_timestamp", pa.string()), ("source_revision_sha1", pa.string()),
    ("source_revision_url", pa.string()), ("source_history_url", pa.string()),
    ("source_quality_level", pa.int8()), ("source_quality_label", pa.string()),
    ("quality_fields_agree", pa.bool_()), ("bayanat_human_verified", pa.bool_()),
    ("text", pa.string()), ("text_character_count", pa.int32()),
    ("text_unicode_letter_count", pa.int32()), ("text_arabic_letter_count", pa.int32()),
    ("text_arabic_letter_fraction", pa.float64()), ("text_scope", pa.string()),
    ("text_extraction_method", pa.string()), ("text_license", pa.string()),
    ("text_license_url", pa.string()), ("text_attribution", pa.string()),
    ("text_changes", pa.string()), ("source_wikitext", pa.string()),
    ("source_body_wikitext", pa.string()), ("source_noinclude_sections_wikitext", pa.list_(pa.string())),
    ("rendered_html", pa.string()), ("scan_filename", pa.string()),
    ("scan_page_number", pa.int32()), ("scan_page_number_basis", pa.string()),
    ("scan_file_sha1", pa.string()), ("scan_file_timestamp", pa.string()),
    ("scan_file_url", pa.string()), ("scan_description_url", pa.string()),
    ("scan_mime", pa.string()), ("scan_file_size_bytes", pa.int64()),
    ("scan_file_page_count", pa.int32()), ("scan_file_page_count_basis", pa.string()),
    ("scan_license_short_name", pa.string()),
    ("scan_license_code", pa.string()), ("scan_license_url", pa.string()),
    ("scan_usage_terms", pa.string()), ("scan_artist_html", pa.string()),
    ("scan_credit_html", pa.string()), ("scan_rights_review_status", pa.string()),
    ("scan_rights_independently_verified", pa.bool_()),
    ("scan_file_version_postdates_text_revision", pa.bool_()),
    ("image_version_correspondence_independently_verified", pa.bool_()),
    ("contains_image_bytes", pa.bool_()), ("alignment_level", pa.string()),
    ("has_human_line_or_region_boxes", pa.bool_()),
    ("source_linked_aco_book_ids", pa.list_(pa.string())), ("file_group_id", pa.string()),
    ("quality_flags", pa.list_(pa.string())), ("parse_warnings_json", pa.string()),
    ("source_api_response_sha256", pa.string()), ("render_api_response_sha256", pa.string()),
    ("source_fetched_at", pa.string()), ("rendered_at", pa.string()),
])


def dumps(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def rows(paths):
    for path in sorted(paths):
        with gzip.open(path, "rt") as stream:
            for line in stream:
                yield json.loads(line)


def classify_and_join(page, rendered, file):
    """A source-evidenced resource filter, not independent transcription clearance."""
    if not page["eligible_source_quality"] or not page["quality_fields_agree"]:
        return None, "source_quality_disagreement_or_not_proofread"
    if not page.get("source_scan_available") or file is None:
        return None, "missing_scan_association_or_file_evidence"
    if file["rights_review_status"] not in ("source_asserted_public_domain", "source_asserted_open_license"):
        return None, "unrecognized_or_restrictive_scan_rights"
    info = file["source_file_info"]
    pagecount = info.get("pagecount")
    pagecount_basis = "source_imageinfo_pagecount"
    if pagecount is None and info.get("mime") in ("image/jpeg", "image/png"):
        pagecount = 1
        pagecount_basis = "single_jpeg_or_png_image"
    if type(pagecount) is not int or not 1 <= page["scan_page_number"] <= pagecount:
        return None, "invalid_or_unknown_scan_page_range"
    if rendered is None:
        return None, "missing_rendered_text"
    if rendered["source_revision_id"] != page["source_revision_id"]:
        raise ValueError("Rendered revision differs from original source revision")
    if "render_quality_flags" not in rendered:
        raise ValueError("Rendered extraction predates required parser-error flags; rerun cached rendering")
    text = rendered.get("text", "")
    if not text.strip() or rendered.get("text_arabic_letter_count", 0) == 0:
        return None, "empty_or_no_arabic_rendered_text"
    render_flags = rendered.get("render_quality_flags", [])
    if any(x in render_flags for x in ("source_parser_error_element", "source_parser_missing_template")):
        return None, "source_parser_error_or_missing_template"
    source_sha = hashlib.sha1(page["wikitext"].encode()).hexdigest()
    if source_sha != page["source_revision_sha1"]:
        raise ValueError("Exact source revision content SHA1 failed")
    for value in (info.get("sha1"),):
        if not isinstance(value, str) or len(value) != 40:
            raise ValueError("Missing source scan-file SHA1")
    timestamp = info.get("timestamp")
    postdates = bool(timestamp and timestamp > page["source_revision_timestamp"])
    flags = list(render_flags)
    if postdates:
        flags.append("current_scan_version_postdates_text_revision")
    if len(text) < 80:
        flags.append("short_rendered_text_under_80_characters")
    if rendered.get("text_arabic_letter_fraction", 0) < 0.5:
        flags.append("less_than_half_unicode_letters_in_arabic_ranges")
    ext = info.get("extmetadata", {})
    ext = ext if isinstance(ext, dict) else {}
    value = lambda name: str(ext.get(name, {}).get("value", "")) or None
    scan_name = page["scan_filename"].replace("_", " ")
    row = {name: None for name in SCHEMA.names}
    for name in ("record_id", "source", "source_wiki", "source_page_id", "source_title", "source_revision_id",
                 "source_parent_revision_id", "source_revision_timestamp", "source_revision_sha1",
                 "source_revision_url", "source_history_url", "source_quality_level", "source_quality_label",
                 "quality_fields_agree", "text_scope", "text_license", "text_license_url", "text_attribution",
                 "scan_filename", "scan_page_number", "scan_page_number_basis", "alignment_level"):
        row[name] = page.get(name)
    for name in ("text", "text_character_count", "text_unicode_letter_count", "text_arabic_letter_count",
                 "text_arabic_letter_fraction", "rendered_html", "rendered_at"):
        row[name] = rendered.get(name)
    row.update({"bayanat_human_verified": False,
                "text_extraction_method": rendered["text_representation"], "text_changes": rendered["text_changes"],
                "source_wikitext": page["wikitext"], "source_body_wikitext": page["body_wikitext"],
                "source_noinclude_sections_wikitext": page.get("noinclude_sections_wikitext", []),
                "scan_file_sha1": info["sha1"], "scan_file_timestamp": timestamp,
                "scan_file_url": info.get("url"), "scan_description_url": info.get("descriptionurl"),
                "scan_mime": info.get("mime"), "scan_file_size_bytes": info.get("size"),
                "scan_file_page_count": pagecount, "scan_file_page_count_basis": pagecount_basis,
                "scan_license_short_name": value("LicenseShortName"),
                "scan_license_code": value("License"), "scan_license_url": value("LicenseUrl"),
                "scan_usage_terms": value("UsageTerms"), "scan_artist_html": value("Artist"),
                "scan_credit_html": value("Credit"), "scan_rights_review_status": file["rights_review_status"],
                "scan_rights_independently_verified": False, "scan_file_version_postdates_text_revision": postdates,
                "image_version_correspondence_independently_verified": False, "contains_image_bytes": False,
                "has_human_line_or_region_boxes": False,
                "source_linked_aco_book_ids": file.get("source_linked_aco_book_ids", []),
                "file_group_id": "arws-file-" + hashlib.sha256(scan_name.encode()).hexdigest(),
                "quality_flags": sorted(set(flags)), "parse_warnings_json": dumps(rendered.get("parse_warnings", [])),
                "source_api_response_sha256": page["raw_response_sha256"],
                "render_api_response_sha256": rendered["raw_response_sha256"], "source_fetched_at": page["fetched_at"]})
    return row, None


def package(source, output, rows_per_shard=2000):
    # A failed rerun must not leave a prior passed report appearing current.
    (output / "reports/community-text-build.json").unlink(missing_ok=True)
    inventory_qc = json.loads((source / "independent-inventory-qc.json").read_text())
    render_report = json.loads((source / "render-summary.json").read_text())
    if inventory_qc["status"] != "passed" or not render_report["all_cached_eligible_pages_rendered"]:
        raise ValueError("Complete acquisition, complete rendering, and passing inventory QC required")
    data = output / "data/community_text"; data.mkdir(parents=True, exist_ok=True)
    metadata = output / "metadata/wikisource"; metadata.mkdir(parents=True, exist_ok=True)
    reports = output / "reports"; reports.mkdir(parents=True, exist_ok=True)
    private = output.parent / "private/community-packaging"; private.mkdir(parents=True, exist_ok=True)
    files = {r["scan_filename"].replace("_", " "): r for r in rows(source.glob("files/*.jsonl.gz"))}
    counts, excluded, quality_counts, flags = Counter(), Counter(), Counter(), Counter()
    used_files, seen = set(), set()
    checked_data = []
    batch = []
    def flush():
        if not batch:
            return
        path = data / f"part-{len(checked_data):05d}.parquet"
        temporary = path.with_suffix(".parquet.tmp")
        pq.write_table(pa.Table.from_pylist(batch, schema=SCHEMA), temporary, compression="zstd", row_group_size=500)
        temporary.replace(path)
        checked_data.append({"path": str(path.relative_to(output)), "rows": len(batch), "sha256": digest(path), "bytes": path.stat().st_size})
        batch.clear()
    with tempfile.TemporaryDirectory(prefix="join-", dir=private) as temporary:
        conn = sqlite3.connect(str(Path(temporary) / "rendered.sqlite3"))
        conn.execute("CREATE TABLE rendered(id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        checked_render_responses = set()
        for rendered in rows(source.glob("rendered/*.jsonl.gz")):
            relative = rendered["raw_response_path"]
            if relative not in checked_render_responses:
                path = (source / relative).resolve()
                if not path.is_relative_to(source.resolve()) or not relative.startswith("raw/rendered/"):
                    raise ValueError("Unexpected raw rendering evidence path")
                if digest(path) != rendered["raw_response_sha256"]:
                    raise ValueError("Raw rendering response SHA256 failed")
                checked_render_responses.add(relative)
            conn.execute("INSERT INTO rendered VALUES(?,?)", (rendered["record_id"], dumps(rendered)))
        conn.commit()
        n_rendered = conn.execute("SELECT COUNT(*) FROM rendered").fetchone()[0]
        expected_rendered = json.loads((source / "summary.json").read_text())["counts"]["eligible_source_quality"]
        if n_rendered != expected_rendered or n_rendered != render_report["counts"]["rendered_pages"]:
            raise ValueError("Rendered records do not exactly match complete source inventory/report counts")
        for page in rows(source.glob("pages/quality-*/*.jsonl.gz")):
            counts["source_revision_records"] += 1
            if page["record_id"] in seen:
                raise ValueError("Duplicate source revision ID")
            seen.add(page["record_id"])
            record = conn.execute("SELECT payload FROM rendered WHERE id=?", (page["record_id"],)).fetchone()
            rendered = json.loads(record[0]) if record else None
            file = files.get((page.get("scan_filename") or "").replace("_", " "))
            row, reason = classify_and_join(page, rendered, file)
            if reason:
                excluded[reason] += 1
                continue
            batch.append(row); counts["published_text_records"] += 1
            quality_counts[str(row["source_quality_level"])] += 1
            flags.update(row["quality_flags"])
            used_files.add(row["scan_filename"].replace("_", " "))
            if len(batch) >= rows_per_shard:
                flush()
        flush()
        conn.close()
    expected = {item["path"] for item in checked_data}
    for stale in data.glob("part-*.parquet"):
        if str(stale.relative_to(output)) not in expected:
            stale.unlink()
    if counts["source_revision_records"] != counts["published_text_records"] + sum(excluded.values()):
        raise AssertionError("Selection counts do not reconcile")
    public_files = []
    for filename in sorted(used_files):
        file = files[filename]
        public_files.append({k: v for k, v in file.items() if k != "raw_response_path"})
    (metadata / "scan_files.jsonl.gz").write_bytes(gzip.compress(b"".join((dumps(r) + "\n").encode() for r in public_files), mtime=0))
    descriptions = []
    for path in sorted(source.glob("raw/file-descriptions-v2/*/*.json.gz")):
        envelope = json.loads(gzip.decompress(path.read_bytes()))
        for page in envelope["response"].get("query", {}).get("pages", []):
            if page["title"].split(":", 1)[-1].replace("_", " ") in used_files:
                descriptions.append({"api_url": envelope["api_url"], "fetched_at": envelope["fetched_at"], "page": page})
    (metadata / "scan_file_descriptions.jsonl.gz").write_bytes(gzip.compress(b"".join((dumps(r) + "\n").encode() for r in descriptions), mtime=0))
    siteinfo = json.loads(gzip.decompress((source / "raw/siteinfo.json.gz").read_bytes()))
    q = siteinfo["response"]["query"]
    rights = {"fetched_at": siteinfo["fetched_at"], "api_url": "https://ar.wikisource.org/w/api.php",
              "rightsinfo": q["rightsinfo"], "proofreadqualitylevels": q["proofreadqualitylevels"],
              "proofreadnamespaces": q["proofreadnamespaces"]}
    (metadata / "source_rights_and_quality.json").write_text(dumps(rights) + "\n")
    report = {"status": "passed", "config": "community_text", "split": "train", "contains_image_bytes": False,
              "is_heldout_benchmark": False, "counts": dict(counts), "excluded_records_by_reason": dict(excluded),
              "source_quality_counts": dict(quality_counts), "quality_flag_counts": dict(flags),
              "source_scan_files_referenced": len(used_files), "data_files": checked_data,
              "checked_raw_render_response_files": len(checked_render_responses),
              "schema": [{"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in SCHEMA],
              "packager_sha256": digest(Path(__file__)),
              "source_inventory_qc_sha256": digest(source / "independent-inventory-qc.json"),
              "source_render_summary_sha256": digest(source / "render-summary.json"),
              "selection_policy": "Source quality3/4 agrees; scan association, recognized source-rights evidence, valid page range; nonempty rendered Arabic text; no parser error/missing-template elements. Short or low-Arabic-fraction text remains flagged.",
              "no_gold_claim": "Source-community proofreading status only; body text excludes noinclude header/footer; no independently verified image correspondence or line/region geometry."}
    (reports / "wikisource-inventory-qc.json").write_text(json.dumps(inventory_qc, ensure_ascii=False, indent=2) + "\n")
    if (source / "file-description-template-audit.json").exists():
        (reports / "wikisource-file-description-template-audit.json").write_bytes((source / "file-description-template-audit.json").read_bytes())
    (metadata / "README.md").write_text("""# Wikisource community text tier

This train-only resource contains text and provenance, **no image bytes**. It is
not a heldout benchmark or independently verified gold transcription. Quality 3
and quality 4 are the source community's proofread/validated labels.

The original Page revision wikitext is retained. `text` is a mechanical derivative
rendered by the source MediaWiki parser; templates are resolved at rendering time.
Its scope excludes `<noinclude>` header/footer sections, often running heads and
printed page numbers. Citation backlinks and style/edit controls are removed.
Newlines are text-node separators, not annotated scan lines. Scan references name
the current observed file version; a later image-file timestamp is flagged.

Wikisource community text and its modified representations are distributed under
[CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/). Attribute Arabic
Wikisource contributors through each record's revision and history URLs, preserve
imported-source notices, identify changes, and comply with ShareAlike. Underlying
public-domain works retain their public-domain status. Source file-description
text is likewise subject to Wikimedia's text terms.

**Scan licensing is separate.** No scan license is inferred from the text license.
Per-file source license/credit fields and exact hosting-wiki file-description revisions are
included in this directory, from Commons or local Arabic Wikisource as applicable.
Public-domain and open-license classifications record
source assertions, not Bayanat worldwide legal clearance. No new CC0 dedication
is made for the collection. Source/API rights evidence is retained without HTTP
headers, credentials, or local private paths.

ACO IDs found in source credits support grouping mirrored editions before any
downstream evaluation split; those links do not independently prove that files
have identical content. Do not randomly split pages and assume book independence.

Primary policies: [Wikimedia Terms, section 7](https://foundation.wikimedia.org/wiki/Policy:Terms_of_Use#7._Licensing_of_Content),
[source quality definitions](https://en.wikisource.org/wiki/Help:Page_status),
[Commons reuse](https://commons.wikimedia.org/wiki/Commons:Reusing_content_outside_Wikimedia).
""")
    report["metadata_files"] = [{"path": str(path.relative_to(output)), "sha256": digest(path), "bytes": path.stat().st_size}
                                for path in sorted(metadata.iterdir()) if path.is_file()]
    report["reports"] = [{"path": str(path.relative_to(output)), "sha256": digest(path)}
                         for path in sorted(reports.glob("wikisource-*.json"))]
    (reports / "community-text-build.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    return report


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", type=Path, required=True); p.add_argument("--output", type=Path, required=True)
    p.add_argument("--rows-per-shard", type=int, default=2000)
    args = p.parse_args()
    if not 1 <= args.rows_per_shard <= 10000:
        raise ValueError("rows-per-shard must be 1..10000")
    print(json.dumps(package(args.source, args.output, args.rows_per_shard), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
