#!/usr/bin/env python3
"""Independent full Wikisource inventory checks, without image downloads."""
import argparse
from collections import Counter
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path

spec = importlib.util.spec_from_file_location("wikisource_rights_qc", Path(__file__).resolve().parents[1] / "acquisition/wikisource.py")
source_policy = importlib.util.module_from_spec(spec); spec.loader.exec_module(source_policy)


def rows(paths):
    for path in sorted(paths):
        with gzip.open(path, "rt") as stream:
            for line in stream:
                yield json.loads(line)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.output
    files = {r["scan_filename"].replace("_", " "): r for r in rows(root.glob("files/*.jsonl.gz"))}
    counts = Counter()
    seen, pages, input_paths = set(), set(), set()
    failures = []
    paired = []
    checked_file_responses = set()
    raw_file_metadata = {}
    for file in files.values():
        relative = file["raw_response_path"]
        if relative not in checked_file_responses:
            path = root / relative
            if hashlib.sha256(path.read_bytes()).hexdigest() != file["raw_response_sha256"]:
                failures.append({"path": relative, "error": "file_metadata_response_hash_mismatch"})
            envelope = json.loads(gzip.decompress(path.read_bytes()))
            for item in envelope["response"].get("query", {}).get("pages", []):
                raw_file_metadata[item["title"].split(":", 1)[-1].replace("_", " ")] = (item.get("imageinfo") or [{}])[0]
            checked_file_responses.add(relative)
        if raw_file_metadata.get(file["scan_filename"].replace("_", " ")) != file["source_file_info"]:
            failures.append({"filename": file["scan_filename"], "error": "normalized_file_metadata_differs_from_raw_response"})
        ext = file["source_file_info"].get("extmetadata", {})
        if (file['rights_review_status'] != source_policy.rights_classification(ext)
                or file.get('rights_evidence_flags') != source_policy.rights_evidence_flags(ext)
                or file.get('rights_review_year') != source_policy.RIGHTS_REVIEW_YEAR):
            failures.append({"filename": file["scan_filename"], "error": "rights_triage_differs_from_current_evidence_policy"})
    checked_descriptions = 0
    for path in sorted(root.glob("raw/file-descriptions-v2/*/*.json.gz")):
        envelope = json.loads(gzip.decompress(path.read_bytes()))
        for item in envelope["response"].get("query", {}).get("pages", []):
            revision = (item.get("revisions") or [{}])[0]
            raw = revision.get("slots", {}).get("main", {}).get("content")
            if raw is None:
                counts["missing_exact_file_description_revision"] += 1
                continue
            checked_descriptions += 1
            if hashlib.sha1(raw.encode()).hexdigest() != revision.get("slots", {}).get("main", {}).get("sha1"):
                failures.append({"title": item["title"], "error": "file_description_content_sha1_mismatch"})
    if checked_descriptions != len(files):
        failures.append({"error": "file_description_inventory_incomplete", "expected": len(files), "checked": checked_descriptions})
    for row in rows(root.glob("pages/quality-*/*.jsonl.gz")):
        counts["occurrences"] += 1
        if row["record_id"] in seen:
            counts["duplicate_revision_occurrences"] += 1
            continue
        seen.add(row["record_id"])
        pages.add(row["source_page_id"])
        counts["unique_revision_records"] += 1
        raw_sha1 = hashlib.sha1(row["wikitext"].encode()).hexdigest()
        if raw_sha1 != row["source_revision_sha1"]:
            failures.append({"record_id": row["record_id"], "error": "revision_content_sha1_mismatch"})
        counts["quality_" + str(row["source_quality_level"])] += 1
        counts["quality_fields_disagree"] += not row["quality_fields_agree"]
        if row["raw_response_path"] not in input_paths:
            input_paths.add(row["raw_response_path"])
            actual = hashlib.sha256((root / row["raw_response_path"]).read_bytes()).hexdigest()
            if actual != row["raw_response_sha256"]:
                failures.append({"path": row["raw_response_path"], "error": "raw_response_hash_mismatch"})
        if not row.get("source_scan_available"):
            counts["missing_source_scan_pair"] += 1
            continue
        file = files.get(row["scan_filename"].replace("_", " "))
        if file is None:
            counts["missing_file_metadata"] += 1
            continue
        status = file["rights_review_status"]
        counts["page_file_rights_" + status] += 1
        info = file["source_file_info"]
        pagecount = info.get("pagecount")
        if pagecount is None and info.get("mime") in ("image/jpeg", "image/png"):
            pagecount = 1
        in_range = isinstance(pagecount, int) and 1 <= row["scan_page_number"] <= pagecount
        if not in_range:
            counts["scan_page_count_missing_or_out_of_range"] += 1
        image_timestamp, revision_timestamp = info.get("timestamp"), row.get("source_revision_timestamp")
        postdates = bool(image_timestamp and revision_timestamp and image_timestamp > revision_timestamp)
        counts["scan_file_version_postdates_text_revision"] += postdates
        candidate = (row["eligible_source_quality"] and in_range and row["arabic_letter_count"] > 0
                     and status in ("source_asserted_public_domain", "source_asserted_open_license"))
        counts["page_pair_candidates_with_source_rights_evidence"] += candidate
        if candidate:
            paired.append({"record_id": row["record_id"], "source_page_id": row["source_page_id"],
                           "source_revision_id": row["source_revision_id"], "scan_filename": row["scan_filename"],
                           "scan_page_number": row["scan_page_number"], "scan_file_sha1": info.get("sha1"),
                           "scan_file_timestamp": image_timestamp,
                           "scan_file_version_postdates_text_revision": postdates,
                           "scan_rights_review_status": status,
                           "image_version_correspondence_independently_verified": False})
    report = {"status": "passed" if not failures else "failed", "counts": dict(counts),
              "unique_source_pages": len(pages), "file_metadata_records": len(files),
              "file_rights_counts": dict(Counter(f["rights_review_status"] for f in files.values())),
              "checked_raw_page_response_files": len(input_paths), "failures": failures,
              "checked_file_metadata_response_files": len(checked_file_responses),
              "checked_exact_hosting_wiki_description_revisions": checked_descriptions,
              "limitations": ["No image bytes downloaded or verified", "No independent legal clearance",
                              "Current source page relationship is not verified historical image-version correspondence",
                              "Rendering quality and original print-line fidelity require separate checks"]}
    (root / "independent-inventory-qc.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    (root / "paired-candidates.jsonl.gz").write_bytes(gzip.compress(b"".join(
        (json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n").encode() for r in paired), mtime=0))
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
