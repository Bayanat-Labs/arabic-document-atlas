#!/usr/bin/env python3
"""Validate every public community-text row, file hash, source identity and type."""
import argparse
from collections import Counter
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import unicodedata

import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("community_package_validation_schema", HERE / "package_community.py")
package = importlib.util.module_from_spec(spec); spec.loader.exec_module(package)


def check(condition, message):
    if not condition:
        raise ValueError(message)


def validate(output):
    result_path = output / "reports/community-text-validation.json"
    result_path.unlink(missing_ok=True)
    report = json.loads((output / "reports/community-text-build.json").read_text())
    check(report["status"] == "passed" and report["split"] == "train", "Build status/split")
    check(report["contains_image_bytes"] is False and report["is_heldout_benchmark"] is False, "Unsupported release claim")
    expected = {item["path"] for item in report["data_files"]}
    actual = {str(path.relative_to(output)) for path in (output / "data/community_text").glob("*.parquet")}
    check(expected == actual, "Unexpected/missing community Parquet shard")
    for item in report["metadata_files"] + report["reports"]:
        path = output / item["path"]
        check(path.resolve().is_relative_to(output.resolve()), "Unsafe public artifact path")
        check(package.digest(path) == item["sha256"], "Public metadata/report hash mismatch")
    files = {}
    with gzip.open(output / "metadata/wikisource/scan_files.jsonl.gz", "rt") as stream:
        for line in stream:
            row = json.loads(line)
            check("raw_response_path" not in row, "Private evidence path in public metadata")
            files[row["scan_filename"].replace("_", " ")] = row
    rights = json.loads((output / "reports/wikisource-rights-exclusions.json").read_text())
    held = {r['scan_filename'] for r in rights['files']}
    check(len(held) == len(rights['files']) == rights['excluded_source_files'], "Duplicate/incorrect held-file count")
    check(not held.intersection(files), "A rights-held file entered public rows")
    check(rights['records_excluded_at_rights_gate'] == sum(r['records_excluded_at_rights_gate'] for r in rights['files']), "Rights exclusion record counts differ")
    check(rights['source_page_revision_records_affected'] == sum(r['source_page_revision_records'] for r in rights['files']), "Rights affected counts differ")
    check(rights['records_excluded_at_rights_gate'] == sum(v for k, v in report['excluded_records_by_reason'].items() if k.startswith('scan_rights_')), "Rights exclusion report/build counts differ")
    seen, source_pages, scan_files = set(), set(), set()
    quality, flags = Counter(), Counter()
    n = 0
    for item in report["data_files"]:
        path = output / item["path"]
        check(package.digest(path) == item["sha256"], "Parquet hash mismatch")
        parquet = pq.ParquetFile(path)
        check(parquet.metadata.num_rows == item["rows"], "Parquet row count mismatch")
        check(parquet.schema_arrow.equals(package.SCHEMA, check_metadata=False), "Arrow schema differs")
        for batch in parquet.iter_batches(batch_size=500):
            for row in batch.to_pylist():
                n += 1
                rid = row["record_id"]
                check(rid not in seen and row["source_page_id"] not in source_pages, "Duplicate source revision/page")
                seen.add(rid); source_pages.add(row["source_page_id"])
                identity = f'arwikisource:{row["source_page_id"]}:{row["source_revision_id"]}'
                check(rid == "arws-" + hashlib.sha256(identity.encode()).hexdigest(), "Source revision identity mismatch")
                check(hashlib.sha1(row["source_wikitext"].encode()).hexdigest() == row["source_revision_sha1"], "Exact original content SHA1 mismatch")
                check(row["source_revision_url"] == f'https://ar.wikisource.org/w/index.php?oldid={row["source_revision_id"]}', "Wrong revision permalink")
                check(row["source_history_url"] == f'https://ar.wikisource.org/w/index.php?curid={row["source_page_id"]}&action=history', "Wrong contributor history link")
                check(row["source_quality_level"] in (3, 4) and row["quality_fields_agree"] is True, "Invalid source proofread quality")
                check(row["source_quality_label"] == {3: "source_community_proofread", 4: "source_community_validated"}[row["source_quality_level"]], "Quality label mismatch")
                check(row["text_license"] == "CC-BY-SA-4.0" and row["text_license_url"] == "https://creativecommons.org/licenses/by-sa/4.0/", "Incorrect text license")
                check(row["text_scope"] == "transcluded_page_body_excluding_noinclude_header_footer", "Missing body-scope boundary")
                for claim in ("bayanat_human_verified", "contains_image_bytes", "scan_rights_independently_verified", "image_version_correspondence_independently_verified", "has_human_line_or_region_boxes"):
                    check(row[claim] is False, "Unsupported verification/image claim: " + claim)
                text = row["text"]
                check(bool(text.strip()) and len(text) == row["text_character_count"], "Text length/content mismatch")
                letters = [char for char in text if unicodedata.category(char).startswith("L")]
                arabic = sum(0x0621 <= ord(c) <= 0x064A or 0x066E <= ord(c) <= 0x06D3 or 0x06FA <= ord(c) <= 0x06FC for c in letters)
                check(len(letters) == row["text_unicode_letter_count"] and arabic == row["text_arabic_letter_count"] and arabic > 0, "Arabic letter census mismatch")
                check(row["text_arabic_letter_fraction"] == arabic / len(letters), "Arabic fraction mismatch")
                check(0 <= row["text_arabic_letter_fraction"] <= 1, "Invalid fraction")
                filename = row["scan_filename"].replace("_", " ")
                file = files[filename]; info = file["source_file_info"]
                image_hold = package.image_rights_hold(file)
                check(file.get('image_redistribution_held') is (image_hold is not None), 'Missing/incorrect image redistribution hold')
                check(('scan_image_redistribution_held_pdf_license_conflict' in row['quality_flags']) == (image_hold is not None), 'Missing/incorrect PDF license conflict flag')
                if image_hold:
                    check(row['scan_page_number'] not in image_hold['non_work_page_numbers'], 'Non-work publisher colophon retained')
                    check(file.get('image_redistribution_hold_evidence') == 'reports/wikisource-image-rights-holds.json#sha1=' + info['sha1'], 'Missing image rights evidence pointer')
                check(row["scan_file_sha1"] == info["sha1"] and row["scan_file_timestamp"] == info["timestamp"], "Source scan version mismatch")
                check(row["scan_file_url"] == info["url"] and row["scan_description_url"] == info["descriptionurl"], "Source scan provenance mismatch")
                check(row["scan_rights_review_status"] == file["rights_review_status"] and file["rights_review_status"] in ("source_asserted_public_domain", "source_asserted_open_license"), "Unsupported scan rights")
                check(1 <= row["scan_page_number"] <= row["scan_file_page_count"], "Source page outside file")
                check(row["source_linked_aco_book_ids"] == file.get("source_linked_aco_book_ids", []), "Cross-source ID evidence mismatch")
                check(row["file_group_id"] == "arws-file-" + hashlib.sha256(filename.encode()).hexdigest(), "File group identity mismatch")
                check(row["scan_file_version_postdates_text_revision"] == (info["timestamp"] > row["source_revision_timestamp"]), "Scan/text version flag mismatch")
                for value in row.values():
                    if isinstance(value, str):
                        check("/Users/mola/" not in value and "/private/tmp/" not in value, "Local private path in public row")
                quality[str(row["source_quality_level"])] += 1
                flags.update(row["quality_flags"]); scan_files.add(filename)
    check(n == report["counts"]["published_text_records"], "Total row count mismatch")
    check(dict(quality) == report["source_quality_counts"] and dict(flags) == report["quality_flag_counts"], "Quality totals mismatch")
    check(len(scan_files) == report["source_scan_files_referenced"], "Referenced scan-file count mismatch")
    check(report["counts"]["source_revision_records"] == n + sum(report["excluded_records_by_reason"].values()), "Selection counts fail reconciliation")
    result = {"status": "passed", "rows_checked": n, "parquet_files_checked": len(expected),
              "unique_source_pages": len(source_pages), "source_quality_counts": dict(quality),
              "quality_flag_counts": dict(flags), "contains_image_bytes": False,
              "source_scan_files_referenced": len(scan_files),
              "build_report_sha256": package.digest(output / "reports/community-text-build.json"),
              "validator_sha256": package.digest(Path(__file__)),
              "scope": "Every public row: Parquet/schema/hash/source-revision content identity/attribution/license evidence/Arabic counts/flags/selection arithmetic. No independent transcription accuracy, geometry, historical image correspondence or legal-clearance certification."}
    result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(); print(json.dumps(validate(args.output), ensure_ascii=False, indent=2))
