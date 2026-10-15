#!/usr/bin/env python3
"""Acquire Arabic Wikisource proofread Page revisions and scan-rights evidence.

Serial, resumable, bounded API batches; downloads metadata/text, never images.
The source quality label is not a Bayanat correctness or geometric-alignment claim.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
import shutil
import time
import unicodedata
from collections import Counter
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

API = "https://ar.wikisource.org/w/api.php"
LICENSE = "https://creativecommons.org/licenses/by-sa/4.0/"
TERMS = "https://foundation.wikimedia.org/wiki/Policy:Terms_of_Use#7._Licensing_of_Content"
USER_AGENT = "BayanatArabicDocumentAtlas/0.1 (https://github.com/Bayanat-Labs)"
SCHEMA_VERSION = "wikisource-page-evidence-v1"
ARABIC_LETTER_RANGE = re.compile(r"[\u0621-\u064A\u066E-\u06D3\u06FA-\u06FC]")


def arabic_letter_count(text):
    return sum(unicodedata.category(char).startswith("L") and ARABIC_LETTER_RANGE.fullmatch(char) is not None for char in text)


def utcnow():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def atomic(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(path.parent).free < 2 * 1024 ** 3 + len(data):
        raise RuntimeError("Acquisition stopped before crossing the 2GiB free-disk floor")
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def json_read(path):
    return json.loads(path.read_bytes())


class APIClient:
    def __init__(self, delay=0.5):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"})
        self.delay = max(0.25, delay)
        self.last_request = 0.0

    def get(self, params, api=API):
        params = {"action": "query", "format": "json", "formatversion": 2,
                  "maxlag": 5, **params}
        for attempt in range(8):
            time.sleep(max(0, self.last_request + self.delay - time.monotonic()))
            self.last_request = time.monotonic()
            try:
                prepared_url = requests.Request("GET", api, params=params).prepare().url
                if len(prepared_url) > 7000:
                    response = self.session.post(api, data=params, timeout=(15, 120))
                else:
                    response = self.session.get(api, params=params, timeout=(15, 120))
                if response.status_code in (429, 500, 502, 503, 504):
                    retry = response.headers.get("Retry-After", "0")
                    try:
                        retry_seconds = float(retry)
                    except ValueError:
                        retry_seconds = max(0, (parsedate_to_datetime(retry) - datetime.now(timezone.utc)).total_seconds())
                    time.sleep(min(60, max(retry_seconds, 2 ** attempt)))
                    continue
                response.raise_for_status()
                result = response.json()
                if result.get("error", {}).get("code") in ("maxlag", "ratelimited"):
                    time.sleep(min(60, 2 ** (attempt + 1)))
                    continue
                if "error" in result:
                    raise ValueError(result["error"])
                return {"fetched_at": utcnow(), "api_url": api, "request_url": response.url,
                        "request_params": params, "response": result}
            except (requests.RequestException, json.JSONDecodeError):
                if attempt == 7:
                    raise
                time.sleep(min(60, 2 ** attempt))
        raise RuntimeError("API retry limit reached")


def split_page_wikitext(raw):
    """Keep exact wikitext; remove only noinclude blocks from transclusion body.

    Newlines and templates do not denote scan line coordinates. Do not infer boxes.
    """
    m = re.search(r'<pagequality\b[^>]*\blevel\s*=\s*[\"\'](\d+)[\"\']', raw, re.I)
    level = int(m.group(1)) if m else None
    body = re.sub(r"<noinclude\b[^>]*>.*?</noinclude\s*>", "", raw, flags=re.I | re.S)
    body = re.sub(r"<includeonly\b[^>]*>|</includeonly\s*>", "", body, flags=re.I)
    return body.strip(), level


def normalize_page(page, envelope, raw_sha, batch_path):
    revisions = page.get("revisions", [])
    if len(revisions) != 1:
        raise ValueError(f"Expected one visible current revision: {page.get('title')}")
    revision = revisions[0]
    slot = revision.get("slots", {}).get("main", {})
    raw = slot.get("content")
    if not isinstance(raw, str):
        raise ValueError(f"Revision content not available: {page.get('title')}")
    body, stored_quality = split_page_wikitext(raw)
    live_quality = page.get("proofread", {}).get("quality")
    prop_quality = page.get("pageprops", {}).get("proofread_page_quality_level")
    live_quality = int(live_quality) if live_quality is not None else None
    prop_quality = int(prop_quality) if prop_quality is not None else None
    agrees = stored_quality == live_quality == prop_quality
    quality = stored_quality
    image = page.get("imagesforpage", {})
    image = image if isinstance(image, dict) else {}
    image_url = image.get("fullsize") or image.get("thumbnail")
    if image_url and image_url.startswith("//"):
        image_url = "https:" + image_url
    title = page["title"]
    index_match = re.search(r"/(\d+)$", title)
    scan_page = int(index_match.group(1)) if index_match else None
    scan_page_basis = "source_page_title_suffix" if scan_page is not None else "unresolved"
    if scan_page is None and re.search(r"\.(?:jpe?g|png)$", str(image.get("filename", "")), re.I):
        scan_page = 1
        scan_page_basis = "single_image_filename_no_numeric_suffix"
    source_url = page.get("fullurl") or "https://ar.wikisource.org/wiki/" + quote(title.replace(" ", "_"))
    revid = revision["revid"]
    canonical_identity = f"arwikisource:{page['pageid']}:{revid}"
    noinclude = re.findall(r"<noinclude\b[^>]*>(.*?)</noinclude\s*>", raw, flags=re.I | re.S)
    noinclude_without_quality = [re.sub(r"<pagequality\b[^>]*/\s*>", "", x, flags=re.I).strip() for x in noinclude]
    return {
        "record_id": "arws-" + sha256(canonical_identity.encode()),
        "source": "Arabic Wikisource", "source_wiki": "arwikisource",
        "source_page_id": page["pageid"], "source_title": title,
        "source_page_language": page.get("pagelanguage"),
        "source_revision_id": revid, "source_parent_revision_id": revision.get("parentid"),
        "source_revision_timestamp": revision.get("timestamp"),
        "source_revision_sha1": revision.get("sha1"),
        "source_url": source_url,
        "source_revision_url": f"https://ar.wikisource.org/w/index.php?oldid={revid}",
        "source_history_url": f"https://ar.wikisource.org/w/index.php?curid={page['pageid']}&action=history",
        "text_license": "CC-BY-SA-4.0", "text_license_url": LICENSE,
        "text_rights_basis": "source_site_rightsinfo_and_Wikimedia_Terms_of_Use",
        "text_attribution": "Arabic Wikisource contributors; see source_revision_url and source_history_url",
        "text_changes": "Original revision wikitext retained; body removes noinclude sections and includeonly tags only.",
        "source_quality_level": quality,
        "source_quality_label": {3: "source_community_proofread", 4: "source_community_validated"}.get(quality, "other"),
        "quality_fields_agree": agrees,
        "bayanat_human_verified": False,
        "wikitext": raw, "body_wikitext": body,
        "noinclude_sections_wikitext": noinclude_without_quality,
        "text_scope": "transcluded_page_body_excluding_noinclude_header_footer",
        "text_representation": "MediaWiki wikitext, not rendered plain text",
        "arabic_letter_count": arabic_letter_count(body),
        "body_character_count": len(body),
        "scan_filename": image.get("filename"), "scan_page_number": scan_page,
        "scan_page_number_basis": scan_page_basis,
        "source_scan_available": bool(image.get("filename") and image_url and scan_page),
        "scan_thumbnail_url": image_url,
        "scan_image_downloaded": False,
        "alignment_level": "source_page_pair_only",
        "has_human_line_or_region_boxes": False,
        "eligible_source_quality": agrees and quality in (3, 4) and page.get("ns") == 104,
        "fetched_at": envelope["fetched_at"],
        "raw_response_path": str(batch_path), "raw_response_sha256": raw_sha,
        "schema_version": SCHEMA_VERSION,
    }


def save_envelope(path, envelope):
    payload = encoded(envelope)
    compressed = gzip.compress(payload, mtime=0)
    atomic(path, compressed)
    return sha256(compressed)


def load_or_get(path, client, params, api=API):
    if path.exists():
        envelope = json.loads(gzip.decompress(path.read_bytes()))
        if envelope["request_params"] != {"action": "query", "format": "json", "formatversion": 2, "maxlag": 5, **params}:
            raise ValueError(f"Cached API parameters differ: {path}")
        return envelope
    envelope = client.get(params, api)
    save_envelope(path, envelope)
    return envelope


def acquire_pages(output, client, max_batches=None):
    raw_root = output / "raw"
    metadata = load_or_get(raw_root / "siteinfo.json.gz", client,
        {"meta": "siteinfo|proofreadinfo", "siprop": "namespaces|rightsinfo|general"})
    rights = metadata["response"]["query"]["rightsinfo"]
    if "by-sa/4.0" not in rights["url"]:
        raise ValueError(f"Source rights changed, review before acquisition: {rights}")
    levels = metadata["response"]["query"]["proofreadqualitylevels"]
    done_batches = 0
    for level in (4, 3):
        category = next(x["category"] for x in levels if x["id"] == level)
        params = {"generator": "categorymembers", "gcmtitle": "تصنيف:" + category,
                  "gcmnamespace": 104, "gcmlimit": 50,
                  "prop": "revisions|info|pageprops|proofread|imageforpage",
                  "rvprop": "ids|timestamp|sha1|content", "rvslots": "main", "inprop": "url",
                  "prppifpprop": "filename|fullsize"}
        batch = 0
        while True:
            if max_batches is not None and done_batches >= max_batches:
                return False
            path = raw_root / f"quality-{level}" / f"batch-{batch:06d}.json.gz"
            envelope = load_or_get(path, client, params)
            raw_sha = sha256(path.read_bytes())
            records = [normalize_page(p, envelope, raw_sha, path.relative_to(output))
                       for p in envelope["response"].get("query", {}).get("pages", [])]
            payload = b"".join(encoded(r) + b"\n" for r in records)
            normalized = output / "pages" / f"quality-{level}" / f"batch-{batch:06d}.jsonl.gz"
            atomic(normalized, gzip.compress(payload, mtime=0))
            done_batches += 1
            if batch % 20 == 0:
                print(json.dumps({"stage": "pages", "quality": level, "batch": batch,
                                  "rows": len(records), "fetched_at": envelope["fetched_at"]}), flush=True)
            continuation = envelope["response"].get("continue")
            if not continuation:
                break
            params = {k: v for k, v in params.items() if k not in ("gcmcontinue", "continue")}
            params.update(continuation)
            batch += 1
    return True


def iter_pages(output):
    for path in sorted((output / "pages").glob("quality-*/*.jsonl.gz")):
        with gzip.open(path, "rt") as stream:
            for line in stream:
                yield json.loads(line)


def rights_classification(metadata):
    """Mechanical triage only: institution/community assertions, never a guarantee."""
    value = lambda key: str(metadata.get(key, {}).get("value", ""))
    short, code, url = value("LicenseShortName"), value("License"), value("LicenseUrl")
    restrictions = value("Restrictions").strip()
    joined = (short + " " + code + " " + url).lower()
    if restrictions:
        return "review_required_restrictions"
    if re.search(r"\bnc\b|\bnd\b|noncommercial|no.?derivative|fair.?use", joined):
        return "excluded_restrictive_terms"
    if "public domain" in joined or code.lower() in ("pd", "cc-zero", "cc0"):
        return "source_asserted_public_domain"
    if re.search(r"creativecommons.org/licenses/by(?:-sa)?/", url) or re.fullmatch(r"cc-by(?:-sa)?(?:-[1-4]\.0)?", code.lower()):
        return "source_asserted_open_license"
    return "review_required_missing_or_unrecognized_terms"


def acquire_file_descriptions(output, client, names):
    """Retain actual hosting-wiki descriptions and main-slot content SHA1.

    Revision SHA1 can aggregate multiple slots on Commons; slotsha1 hashes text.
    """
    repositories = {}
    for path in sorted((output / "files").glob("*.jsonl.gz")):
        with gzip.open(path, "rt") as stream:
            for line in stream:
                row = json.loads(line)
                repositories[row["scan_filename"].replace("_", " ")] = row["image_repository"]
    for repository in ("shared", "local"):
        selected = [name for name in names if repositories[name] == repository]
        api = "https://commons.wikimedia.org/w/api.php" if repository == "shared" else API
        prefix = "File:" if repository == "shared" else "ملف:"
        for start in range(0, len(selected), 20):
            group = selected[start:start + 20]
            params = {"titles": "|".join(prefix + n for n in group),
                      "prop": "revisions|info", "rvslots": "main",
                      "rvprop": "ids|timestamp|sha1|slotsha1|content", "inprop": "url"}
            path = output / "raw" / "file-descriptions-v2" / repository / (sha256(encoded(group))[:24] + ".json.gz")
            load_or_get(path, client, params, api)
    if any(repository not in ("shared", "local") for repository in repositories.values()):
        raise ValueError("Unknown source image repository; review description provenance")


def acquire_files(output, client):
    names = sorted({r["scan_filename"].replace("_", " ") for r in iter_pages(output) if r.get("scan_filename")})
    file_root = output / "files"
    for start in range(0, len(names), 40):
        batch_names = names[start:start + 40]
        params = {"titles": "|".join("ملف:" + n for n in batch_names), "prop": "imageinfo",
                  "iiprop": "url|size|sha1|timestamp|mime|extmetadata|commonmetadata", "iilimit": 1,
                  "iiextmetadatalanguage": "en"}
        path = output / "raw" / "files" / (sha256(encoded(batch_names))[:24] + ".json.gz")
        envelope = load_or_get(path, client, params)
        raw_sha = sha256(path.read_bytes())
        rows = []
        for page in envelope["response"].get("query", {}).get("pages", []):
            infos = page.get("imageinfo") or [{}]
            info = infos[0] if isinstance(infos[0], dict) else {}
            metadata = info.get("extmetadata", {})
            metadata = metadata if isinstance(metadata, dict) else {}
            credit_evidence = "\n".join(str(metadata.get(k, {}).get("value", ""))
                                         for k in ("Credit", "ImageDescription"))
            linked_aco = sorted(set(re.findall(r"[A-Za-z0-9_-]+_aco[0-9]+", credit_evidence))) if "dlib.nyu.edu" in credit_evidence else []
            rows.append({"scan_filename": page["title"].split(":", 1)[-1],
                         "image_repository": page.get("imagerepository"),
                         "source_file_info": info,
                         "rights_review_status": rights_classification(metadata),
                         "rights_independently_verified": False,
                         "source_linked_aco_book_ids": linked_aco,
                         "cross_source_match_basis": "ACO identifier present in source file credit/description with NYU domain; correspondence not independently verified",
                         "raw_response_path": str(path.relative_to(output)),
                         "raw_response_sha256": raw_sha, "fetched_at": envelope["fetched_at"]})
        atomic(file_root / f"batch-{start // 40:05d}.jsonl.gz",
               gzip.compress(b"".join(encoded(r) + b"\n" for r in rows), mtime=0))
        print(json.dumps({"stage": "file_rights", "completed": min(start + 40, len(names)), "total": len(names)}), flush=True)
    acquire_file_descriptions(output, client, names)


def summarize(output, complete):
    counts = Counter()
    identities, files = set(), set()
    for row in iter_pages(output):
        counts["occurrences"] += 1
        identity = (row["source_page_id"], row["source_revision_id"])
        if identity in identities:
            counts["duplicate_revision_occurrences"] += 1
            continue
        identities.add(identity)
        counts["unique_page_revisions"] += 1
        counts["quality_" + str(row["source_quality_level"])] += 1
        counts["eligible_source_quality"] += row["eligible_source_quality"]
        counts["with_scan_pair"] += bool(row["scan_filename"] and row["scan_page_number"] and row["scan_thumbnail_url"])
        counts["with_arabic_letters"] += row["arabic_letter_count"] > 0
        counts["body_characters"] += row["body_character_count"]
        files.add(row.get("scan_filename"))
    files.discard(None)
    summary = {"schema_version": SCHEMA_VERSION, "generated_at": utcnow(),
               "page_acquisition_complete": complete, "counts": dict(counts),
               "unique_scan_files": len(files), "images_downloaded": 0,
               "geometry_annotation": "none", "source_quality_is_bayanat_verification": False,
               "text_license": "CC-BY-SA-4.0", "text_license_url": LICENSE,
               "source_policy_url": TERMS,
               "snapshot_scope": "Pages present in source quality3/quality4 categories during this bounded API crawl; not a frozen whole-wiki transaction."}
    atomic(output / "summary.json", encoded(summary) + b"\n")
    return summary


def render_pages(output, client, batch_size=20, max_batches=None):
    """Expand templates through the source parser; retain HTML and raw evidence.

    Rendered text is a mechanical derivative of the source-community Page text.
    Templates are resolved at render time, not frozen to their historical revision.
    Footnote reference lists are requested inside each page wrapper. This is not
    a geometric transcript or preservation of every original print line break.
    """
    counts = Counter()
    batch = []

    def render(group):
        identities = [r["record_id"] for r in group]
        key = sha256(encoded(identities))[:24]
        text = "\n\n".join(f'<div id="bayanat-source-{r["source_page_id"]}">\n\n'
                             + r["body_wikitext"] + '\n\n<references/>\n</div>\n'
                             for r in group)
        params = {"action": "parse", "text": text, "title": "Project:Bayanat Arabic Document Atlas extraction",
                  "contentmodel": "wikitext", "prop": "text|parsewarnings", "disableeditsection": 1}
        path = output / "raw" / "rendered" / (key + ".json.gz")
        envelope = load_or_get(path, client, params)
        parsed = envelope["response"]["parse"]
        soup = BeautifulSoup(parsed["text"], "html.parser")
        warnings = parsed.get("parsewarnings", [])
        rows = []
        marker_error = None
        for r in group:
            markers = soup.find_all(id=f'bayanat-source-{r["source_page_id"]}')
            if len(markers) != 1 or markers[0].find(id=re.compile("^bayanat-source-")):
                marker_error = r["record_id"]
                break
        if marker_error:
            if len(group) == 1:
                raise ValueError(f"Source parser lost/duplicated/nested singleton page marker: {marker_error}")
            counts["batches_split_for_page_marker_isolation"] += 1
            for single in group:
                render([single])
            return
        # Named citation identifiers are global inside a MediaWiki parse. They
        # must not resolve to another source page's definition in a batch.
        if len(group) > 1:
            isolated = [r for r in group if re.search(r'<ref\b[^>]*\bname\s*=|\{\{\s*#tag:\s*ref\b', r["body_wikitext"], re.I)]
            if isolated:
                counts["pages_isolated_for_named_reference_scope"] += len(isolated)
                isolated_ids = {r["record_id"] for r in isolated}
                for single in isolated:
                    render([single])
                group = [r for r in group if r["record_id"] not in isolated_ids]
        for r in group:
            markers = soup.find_all(id=f'bayanat-source-{r["source_page_id"]}')
            node = markers[0]
            rendered_html = str(node)
            render_flags = []
            if node.select(".error,.scribunto-error,.mw-ext-cite-error"):
                render_flags.append("source_parser_error_element")
            if any(str(link.get("title", "")).startswith(("Template:", "قالب:")) for link in node.select("a.new")):
                render_flags.append("source_parser_missing_template")
            if warnings:
                render_flags.append("source_parser_batch_warning")
            for unsafe in node.select("style,script,noscript,.mw-editsection,.mw-cite-backlink"):
                unsafe.decompose()
            plaintext = "\n".join(line.strip() for line in node.get_text("\n").splitlines() if line.strip())
            letter_count = sum(unicodedata.category(char).startswith("L") for char in plaintext)
            arabic_count = arabic_letter_count(plaintext)
            rows.append({"record_id": r["record_id"], "source_page_id": r["source_page_id"],
                         "source_revision_id": r["source_revision_id"], "rendered_html": rendered_html,
                         "text": plaintext, "text_character_count": len(plaintext),
                         "text_unicode_letter_count": letter_count,
                         "text_arabic_letter_count": arabic_count,
                         "text_arabic_letter_fraction": arabic_count / letter_count if letter_count else None,
                         "text_representation": "source_MediaWiki_parser_HTML_to_plain_text",
                         "text_scope": "transcluded_page_body_excluding_noinclude_header_footer",
                         "text_quality_basis": r["source_quality_label"],
                         "bayanat_human_verified": False,
                         "geometry_annotation": "none", "parse_warnings": warnings,
                         "render_quality_flags": render_flags,
                         "renderer_template_version_policy": "source templates resolved at render time; original Page revision remains separately retained",
                         "text_changes": "Excluded noinclude header/footer; expanded source templates; appended page reference list; removed style/script/edit controls and citation backlinks; extracted nonempty text nodes separated by newlines.",
                         "text_license": "CC-BY-SA-4.0", "text_license_url": LICENSE,
                         "source_revision_url": r["source_revision_url"],
                         "source_history_url": r["source_history_url"],
                         "rendered_at": envelope["fetched_at"],
                         "raw_response_path": str(path.relative_to(output)),
                         "raw_response_sha256": sha256(path.read_bytes())})
        atomic(output / "rendered" / (key + ".jsonl.gz"),
               gzip.compress(b"".join(encoded(r) + b"\n" for r in rows), mtime=0))
        counts["rendered_pages"] += len(rows)
        counts["batches"] += 1
        counts["batches_with_parse_warnings"] += bool(warnings)
        if counts["batches"] % 20 == 0:
            print(json.dumps({"stage": "render", **dict(counts)}), flush=True)

    seen = set()
    exhausted = True
    for row in iter_pages(output):
        if not row["eligible_source_quality"] or row["record_id"] in seen:
            continue
        seen.add(row["record_id"])
        batch.append(row)
        if len(batch) >= batch_size:
            render(batch)
            batch = []
            if max_batches is not None and counts["batches"] >= max_batches:
                exhausted = False
                break
    if batch:
        render(batch)
    report = {"generated_at": utcnow(), "all_cached_eligible_pages_rendered": exhausted,
              "counts": dict(counts), "batch_size": batch_size}
    atomic(output / "render-summary.json", encoded(report) + b"\n")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--delay", type=float, default=0.5)
    parser.add_argument("--max-batches", type=int)
    parser.add_argument("--skip-files", action="store_true")
    parser.add_argument("--render-only", action="store_true")
    parser.add_argument("--render-batch-size", type=int, default=20)
    args = parser.parse_args()
    client = APIClient(args.delay)
    if args.render_only:
        if not 1 <= args.render_batch_size <= 50:
            raise ValueError("render-batch-size must be between 1 and 50")
        print(json.dumps(render_pages(args.output, client, args.render_batch_size, args.max_batches), indent=2))
        return
    complete = acquire_pages(args.output, client, args.max_batches)
    if complete and not args.skip_files:
        acquire_files(args.output, client)
    print(json.dumps(summarize(args.output, complete), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
