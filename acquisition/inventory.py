#!/usr/bin/env python3
"""Resumable, rate-limited metadata inventory of NYU Arabic Collections Online.

No image corpus is downloaded by discover/manifests/export. Image probes are
explicitly bounded. Rights fields describe institutional assertions, not a new
license grant or a legal determination about every work.
"""
from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait as wait_futures
import datetime as dt
import gzip
import hashlib
import html
import io
import json
import os
from pathlib import Path
import re
import sqlite3
import shutil
import time
import threading
from typing import Any
import unicodedata
import zlib
from urllib.parse import urlencode, urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser
from email.utils import parsedate_to_datetime

import requests

ORIGIN = "https://sites.dlib.nyu.edu"
DISCOVERY = ORIGIN + "/viewer/api/v1/objects"
USER_AGENT = "BayanatDocumentAtlas/0.1 (+https://github.com/Bayanat-Labs)"
SCHEMA_VERSION = 2
FREE_DISK_FLOOR = 4 * 1024 ** 3
ID_RE = re.compile(r"^[a-zA-Z0-9_-]+_aco[0-9]+$")
ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")


class ManifestValidationError(ValueError):
    def __init__(self, message: str, proof: dict, raw_path: str, canvas_count: int | None):
        super().__init__(message)
        self.proof, self.raw_path, self.canvas_count = proof, raw_path, canvas_count


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def ensure_space(output: Path, additional: int = 0):
    if shutil.disk_usage(output).free < FREE_DISK_FLOOR + additional:
        raise RuntimeError("Free disk is below the 4GiB floor plus pending-write allowance; resume after freeing space")


def decode_page(value: bytes | str) -> dict:
    return json.loads(zlib.decompress(value) if isinstance(value, bytes) else value)


def strip_tags(value: str) -> str:
    return html.unescape(re.sub(r"<[^>]*>", "", value))


def strings(value: Any, language: str | None = None) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [text for item in value for text in strings(item, language)]
    if isinstance(value, dict):
        if language in value:
            return strings(value[language])
        if "@value" in value:
            return strings(value["@value"])
        return [text for item in value.values() for text in strings(item)]
    return []


def metadata_values(manifest: dict, label: str, language: str = "en") -> list[str]:
    for field in manifest.get("metadata", []):
        if not isinstance(field, dict):
            continue
        labels = [strip_tags(x).casefold() for x in strings(field.get("label"))]
        if label.casefold() in labels:
            return strings(field.get("value"), language)
    return []


def publication_years(values: list[str]) -> tuple[list[int], list[str]]:
    """Extract plausible Gregorian year tokens, preserving ambiguity explicitly."""
    found: set[int] = set()
    flags = []
    for value in values:
        normalized = unicodedata.normalize("NFKC", value).translate(ARABIC_DIGITS)
        for raw in re.findall(r"(?<![0-9])([0-9]{4})(?![0-9])", normalized):
            year = int(raw)
            if 1500 <= year <= dt.datetime.now().year:
                found.add(year)
        if re.search(r"\b(?:A\.?H\.?|Hijr|Hegira)\b|هـ|هجري", normalized, re.I):
            flags.append("calendar_annotation_requires_review")
    if not found:
        flags.append("publication_year_unresolved")
    if len(found) > 1:
        flags.append("multiple_publication_year_tokens")
    return sorted(found), sorted(set(flags))


def rights_class(uri: Any) -> str:
    if uri is None:
        return "no_explicit_rights_uri"
    if not isinstance(uri, str):
        return "unrecognized_rights_value"
    normalized = uri.strip().removeprefix("https://").removeprefix("http://").rstrip("/")
    if normalized in {"creativecommons.org/publicdomain/zero/1.0", "creativecommons.org/publicdomain/mark/1.0"}:
        return "explicit_public_domain_mark_or_cc0"
    if normalized in {"creativecommons.org/licenses/by/4.0", "creativecommons.org/licenses/by/3.0", "creativecommons.org/licenses/by-sa/4.0", "creativecommons.org/licenses/by-sa/3.0"}:
        return "explicit_attribution_license"
    return "unrecognized_or_restrictive_rights_uri"


def safe_url(value: str, *, image: bool = False) -> str:
    parsed = urlsplit(value)
    allowed_path = "/viewer/api/image/" if image else "/viewer/api/presentation/"
    if parsed.scheme != "https" or parsed.netloc != "sites.dlib.nyu.edu" or not parsed.path.startswith(allowed_path):
        raise ValueError("Unexpected source URL: " + value)
    if parsed.username or parsed.password or parsed.fragment:
        raise ValueError("Unexpected source URL credentials or fragment")
    return value


def parse_manifest(manifest: dict, book_id: str, manifest_sha256: str) -> tuple[dict, list[dict]]:
    """Pure parser. Return one book and exact source-canvas occurrences.

    A canvas occurrence is not a proven unique physical page. Source-image and
    content deduplication belong to the downstream image pipeline.
    """
    if not ID_RE.fullmatch(book_id):
        raise ValueError("Malformed ACO identifier")
    if manifest.get("type") != "Manifest" or not isinstance(manifest.get("items"), list):
        raise ValueError("Expected an IIIF Presentation 3 Manifest with items")
    manifest_url = safe_url(manifest.get("id", ""))
    expected = f"{ORIGIN}/viewer/api/presentation/books/{book_id}/manifest.json"
    if manifest_url != expected:
        raise ValueError("Manifest identity differs from requested book")
    if "Arabic Collections Online" not in metadata_values(manifest, "Collection"):
        raise ValueError("Manifest metadata does not identify the ACO collection")
    required = manifest.get("requiredStatement", {})
    rights_statement = "\n".join(strings(required.get("value"), "en"))
    explicit_rights = manifest.get("rights")
    source_rights_class = rights_class(explicit_rights)
    asserted_pd = "public domain" in rights_statement.casefold() and "nyu" in rights_statement.casefold()
    date_values = metadata_values(manifest, "Date of publication")
    years, flags = publication_years(date_values)
    if not rights_statement:
        flags.append("missing_institution_rights_statement")
    if explicit_rights is None:
        flags.append("no_explicit_machine_readable_license_uri")
    elif source_rights_class in ("unrecognized_rights_value", "unrecognized_or_restrictive_rights_uri"):
        flags.append("explicit_rights_requires_review")
    if years and max(years) > 1930:
        flags.append("post_1930_edition_requires_review")
    if "explicit_rights_requires_review" in flags:
        eligibility = "review_explicit_rights_value"
    elif asserted_pd and years and max(years) <= 1930 and not any(f in flags for f in ("calendar_annotation_requires_review", "multiple_publication_year_tokens")):
        eligibility = "candidate_institution_asserted_pd_pre1931"
    elif asserted_pd:
        eligibility = "review_institution_asserted_pd_edition_date"
    else:
        eligibility = "review_missing_recognized_institution_pd_assertion"
    book = {
        "source_book_id": book_id,
        "manifest_url": manifest_url,
        "manifest_sha256": manifest_sha256,
        "title_en": "\n".join(strings(manifest.get("label"), "en")),
        "title_ar": "\n".join(strings(manifest.get("label"), "ar")),
        "metadata": manifest.get("metadata", []),
        "publication_date_raw": date_values,
        "publication_year_tokens": years,
        "modern_editorial_content_review_required": not years or max(years) > 1930,
        "contributor_life_dates_independently_verified": False,
        "publisher": metadata_values(manifest, "Publisher"),
        "provider": metadata_values(manifest, "Provider"),
        "language": metadata_values(manifest, "Language"),
        "source_viewing_direction": manifest.get("viewingDirection"),
        "renderings": manifest.get("rendering", []),
        "rights_statement_raw": rights_statement,
        "source_rights_uri": explicit_rights,
        "source_rights_class": source_rights_class,
        "institution_asserts_public_domain": asserted_pd,
        "eligibility_proposal": eligibility,
        "rights_decision": "not_adjudicated",
        "rights_statement_source_url": manifest_url,
        "quality_flags": sorted(set(flags)),
        "source_canvas_count": len(manifest["items"]),
        "page_inventory_count": 0,
        "pdf_canvas_mapping_status": "unverified_requires_per_book_page_count_and_alignment_validation",
        "source_pdf_renderings": [],
    }
    for rendering in manifest.get("rendering", []):
        if isinstance(rendering, dict) and rendering.get("format") == "application/pdf":
            url = rendering.get("id")
            parsed = urlsplit(url) if isinstance(url, str) else None
            allowed = parsed and parsed.netloc == "mc.dlib.nyu.edu" and parsed.path.startswith(f"/files/books/{book_id}/") and parsed.path.endswith(".pdf")
            if not allowed:
                book["quality_flags"].append("unexpected_pdf_rendering_url")
                continue
            book["source_pdf_renderings"].append({
                "source_pdf_url_raw": url,
                "source_pdf_https_candidate": urlunsplit(("https", parsed.netloc, parsed.path, parsed.query, "")),
                "https_candidate_verified": False,
                "source_label": rendering.get("label"),
                "expected_source_canvas_count": len(manifest["items"]),
                "pdf_page_count": None,
                "pdf_canvas_mapping_status": "unverified",
            })
    pages = []
    seen_canvas_ids: set[str] = set()
    seen_services: set[str] = set()
    for index, canvas in enumerate(manifest["items"]):
        if not isinstance(canvas, dict) or canvas.get("type") != "Canvas":
            raise ValueError(f"Non-Canvas member at /items/{index}")
        canvas_id = canvas.get("id")
        if not isinstance(canvas_id, str) or not canvas_id.startswith(expected.removesuffix("manifest.json") + "canvas/"):
            raise ValueError(f"Unexpected canvas id at /items/{index}")
        width, height = canvas.get("width"), canvas.get("height")
        if isinstance(width, bool) or isinstance(height, bool) or not isinstance(width, int) or not isinstance(height, int) or min(width, height) <= 0:
            raise ValueError(f"Invalid canvas dimensions at /items/{index}")
        painting = []
        for page_index, annotation_page in enumerate(canvas.get("items", [])):
            if not isinstance(annotation_page, dict):
                continue
            for annotation_index, annotation in enumerate(annotation_page.get("items", [])):
                if isinstance(annotation, dict) and annotation.get("motivation") == "painting":
                    painting.append((page_index, annotation_index, annotation))
        flags = []
        service = body_id = body_pointer = None
        if len(painting) != 1:
            flags.append("non_single_painting_annotation")
        else:
            page_index, annotation_index, annotation = painting[0]
            body = annotation.get("body")
            if isinstance(body, dict) and body.get("type") == "Image":
                body_id = body.get("id")
                services = body.get("service", [])
                if isinstance(services, dict):
                    services = [services]
                services = [s for s in services if isinstance(s, dict) and s.get("type") == "ImageService2"]
                if len(services) == 1:
                    service = safe_url(services[0].get("id", ""), image=True)
                    if not service.startswith(f"{ORIGIN}/viewer/api/image/books/{book_id}/"):
                        raise ValueError("Image service points to another book")
                    body_pointer = f"/items/{index}/items/{page_index}/items/{annotation_index}/body"
                else:
                    flags.append("non_single_image_service")
            else:
                flags.append("unsupported_painting_body")
        if canvas_id in seen_canvas_ids:
            flags.append("duplicate_canvas_id_in_manifest")
        if service and service in seen_services:
            flags.append("duplicate_image_service_in_manifest")
        seen_canvas_ids.add(canvas_id)
        if service:
            seen_services.add(service)
        ordinal = index + 1
        occurrence_id = digest((book_id + "\0" + manifest_sha256 + "\0/items/" + str(index)).encode())
        pages.append({
            "page_occurrence_id": occurrence_id,
            "source_book_id": book_id,
            "source_canvas_ordinal": ordinal,
            "source_canvas_id": canvas_id,
            "source_canvas_pointer": f"/items/{index}",
            "source_canvas_label": canvas.get("label"),
            "source_canvas_width": width,
            "source_canvas_height": height,
            "source_body_id": body_id,
            "source_body_pointer": body_pointer,
            "source_image_service": service,
            "source_image_info_url": service + "/info.json" if service else None,
            "source_image_1024_url": service + "/full/!1024,1024/0/default.jpg" if service else None,
            "source_image_1600_url": service + "/full/!1600,1600/0/default.jpg" if service else None,
            "source_image_half_dimensions_url": service + f"/full/!{max(1,width//2)},{max(1,height//2)}/0/default.jpg" if service else None,
            "half_dimensions_is_200dpi_equivalent": "conditional_on_institution_400ppi_master_guideline",
            "manifest_url": manifest_url,
            "manifest_sha256": manifest_sha256,
            "rights_statement_raw": rights_statement,
            "source_rights_uri": explicit_rights,
            "rights_decision": "not_adjudicated",
            "eligibility_proposal": eligibility,
            "quality_flags": flags,
            "image_payload_downloaded": False,
            "physical_page_uniqueness_verified": False,
        })
    book["page_inventory_count"] = len(pages)
    book["duplicate_canvas_id_count"] = len(pages) - len(seen_canvas_ids)
    book["duplicate_image_service_count"] = sum(p["source_image_service"] is not None for p in pages) - len(seen_services)
    return book, pages


class Client:
    """Serial polite HTTP client with robots checks, bounded bodies and retries."""
    def __init__(self, output: Path, delay: float):
        if delay < 0.5:
            raise ValueError("Minimum request interval is 0.5 seconds")
        self.output, self.delay, self.last = output, delay, 0.0
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
        self.robots: dict[str, RobotFileParser | None] = {}
        self._rate_lock = threading.Lock()
        self._robots_lock = threading.Lock()
        self._local = threading.local()

    def wait(self):
        with self._rate_lock:
            time.sleep(max(0.0, self.last + self.delay - time.monotonic()))
            self.last = time.monotonic()

    def check_robots(self, url: str):
        parts = urlsplit(url)
        origin = urlunsplit((parts.scheme, parts.netloc, "", "", ""))
        if origin not in self.robots:
            self.wait()
            response = self.session.get(origin + "/robots.txt", timeout=(15, 60))
            content = response.content
            if len(content) > 1024 * 1024:
                raise RuntimeError("Unexpected oversized robots policy")
            policy = {"url": origin + "/robots.txt", "status": response.status_code,
                      "retrieved_at": now(), "response_sha256": digest(content),
                      "text": content.decode("utf-8", errors="replace")}
            (self.output / "robots-policy.json").write_text(canonical(policy) + "\n")
            if response.status_code in (404, 410):
                self.robots[origin] = None
            elif response.status_code == 200:
                parser = RobotFileParser(origin + "/robots.txt")
                parser.parse(policy["text"].splitlines())
                delay = parser.crawl_delay(USER_AGENT) or parser.crawl_delay("*")
                if delay:
                    self.delay = max(self.delay, delay)
                self.robots[origin] = parser
            else:
                raise RuntimeError("Robots policy unavailable; refusing further requests")
        parser = self.robots[origin]
        if parser is not None and not parser.can_fetch(USER_AGENT, url):
            raise PermissionError("Robots policy disallows source URL")

    def get(self, url: str, max_bytes: int = 32_000_000) -> tuple[bytes, dict]:
        if urlsplit(url).netloc != "sites.dlib.nyu.edu" or urlsplit(url).scheme != "https":
            raise ValueError("Source host is not allowlisted")
        with self._robots_lock:
            self.check_robots(url)
        if not hasattr(self._local, "session"):
            self._local.session = requests.Session()
            self._local.session.headers.update(self.session.headers)
        for attempt in range(5):
            ensure_space(self.output, max_bytes * 3)
            self.wait()
            try:
                with self._local.session.get(url, timeout=(15, 90), stream=True) as response:
                    if response.status_code in (429, 500, 502, 503, 504):
                        retry = response.headers.get("Retry-After", "")
                        try:
                            pause = float(retry)
                        except ValueError:
                            try:
                                pause = max(0.0, (parsedate_to_datetime(retry) - dt.datetime.now(dt.timezone.utc)).total_seconds())
                            except (TypeError, ValueError):
                                pause = min(60, 2 ** (attempt + 1))
                        if pause > 300:
                            raise RuntimeError("Server asks for a long retry delay; resume later")
                        if attempt == 4:
                            response.raise_for_status()
                        time.sleep(max(pause, 2 ** attempt))
                        continue
                    response.raise_for_status()
                    chunks, size = [], 0
                    for chunk in response.iter_content(128 * 1024):
                        size += len(chunk)
                        if size > max_bytes:
                            raise ValueError("Source body exceeds configured byte cap")
                        chunks.append(chunk)
                    body = b"".join(chunks)
                    headers = {key: response.headers.get(key) for key in
                               ("ETag", "Last-Modified", "Content-Type", "Content-Length", "Content-Encoding")}
                    proof = {"requested_url": url, "response_url": response.url, "retrieved_at": now(),
                             "http_status": response.status_code, "response_headers": headers,
                             "decoded_response_bytes": len(body), "decoded_response_sha256": digest(body)}
                    with (self.output / "request-log.jsonl").open("a", encoding="utf-8") as stream:
                        stream.write(canonical(proof) + "\n")
                    return body, proof
            except (requests.Timeout, requests.ConnectionError):
                if attempt == 4:
                    raise
                time.sleep(min(60, 2 ** (attempt + 1)))
        raise RuntimeError("Request retries exhausted")


def atomic_gzip(path: Path, body: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as stream:
            stream.write(body)
    os.replace(temporary, path)


def verify_raw(output: Path, relative_path: str, expected_sha256: str):
    path = output / relative_path
    calculated = hashlib.sha256()
    try:
        with gzip.open(path, "rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                calculated.update(block)
    except (OSError, EOFError) as error:
        raise RuntimeError("Cached source body is missing or corrupt: " + relative_path) from error
    if calculated.hexdigest() != expected_sha256:
        raise RuntimeError("Cached source SHA-256 differs from recorded proof: " + relative_path)


def database(output: Path) -> sqlite3.Connection:
    output.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(output / "inventory.sqlite3", timeout=60)
    connection.row_factory = sqlite3.Row
    connection.executescript("""
      PRAGMA journal_mode=WAL;
      CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS discovery_pages(offset INTEGER PRIMARY KEY,row_count INTEGER,expected_total INTEGER,sha256 TEXT,raw_path TEXT,proof_json TEXT);
      CREATE TABLE IF NOT EXISTS discovery_occurrences(offset INTEGER,ordinal INTEGER,book_id TEXT,record_json TEXT,PRIMARY KEY(offset,ordinal));
      CREATE INDEX IF NOT EXISTS discovery_book_ids ON discovery_occurrences(book_id);
      CREATE TABLE IF NOT EXISTS books(book_id TEXT PRIMARY KEY,manifest_url TEXT NOT NULL,discovery_json TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'pending',error TEXT,manifest_sha256 TEXT,raw_path TEXT,proof_json TEXT,book_json TEXT,canvas_count INTEGER);
      CREATE TABLE IF NOT EXISTS pages(book_id TEXT,ordinal INTEGER,canvas_id TEXT,image_service TEXT,row_json BLOB,PRIMARY KEY(book_id,ordinal));
      CREATE INDEX IF NOT EXISTS pages_image_service ON pages(image_service);
      CREATE TABLE IF NOT EXISTS checkpoints(sequence INTEGER PRIMARY KEY,metadata_json TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS checkpointed_books(book_id TEXT PRIMARY KEY,sequence INTEGER NOT NULL);
    """)
    existing = connection.execute("SELECT value FROM settings WHERE key='schema_version'").fetchone()
    if existing and int(existing[0]) != SCHEMA_VERSION:
        raise ValueError("Incompatible inventory schema; use a new output directory")
    connection.execute("INSERT OR IGNORE INTO settings VALUES('schema_version',?)", (str(SCHEMA_VERSION),))
    connection.commit()
    return connection


def discover(connection: sqlite3.Connection, client: Client, page_size: int, max_pages: int | None):
    if not 1 <= page_size <= 500:
        raise ValueError("Page size must be between 1 and 500")
    stored_size = connection.execute("SELECT value FROM settings WHERE key='page_size'").fetchone()
    if stored_size and int(stored_size[0]) != page_size:
        raise ValueError("Resume must use the original discovery page size")
    connection.execute("INSERT OR IGNORE INTO settings VALUES('page_size',?)", (str(page_size),))
    connection.commit()
    offset, processed, expected = 0, 0, None
    while expected is None or offset < expected:
        old = connection.execute("SELECT * FROM discovery_pages WHERE offset=?", (offset,)).fetchone()
        if old:
            verify_raw(client.output, old["raw_path"], old["sha256"])
            occurrence_count = connection.execute("SELECT count(*) FROM discovery_occurrences WHERE offset=?", (offset,)).fetchone()[0]
            if occurrence_count != old["row_count"]:
                raise RuntimeError("Cached discovery occurrence count is inconsistent")
            expected = old["expected_total"]
            offset += page_size
            continue
        if max_pages is not None and processed >= max_pages:
            break
        url = DISCOVERY + "?" + urlencode({"collection": "aco", "rows": page_size, "start": offset})
        body, proof = client.get(url)
        result = json.loads(body).get("response", {})
        docs = result.get("docs")
        total = result.get("numFound")
        if not isinstance(docs, list) or not isinstance(total, int) or result.get("start") != offset:
            raise ValueError("Unexpected discovery page shape or offset")
        if expected is not None and total != expected:
            raise RuntimeError("Discovery count changed during traversal; inventory remains explicitly incomplete")
        expected = total
        if not docs and offset < total:
            raise ValueError("Empty discovery page before expected end")
        raw_path = client.output / "raw/discovery" / f"offset-{offset:08d}-{proof['decoded_response_sha256']}.json.gz"
        atomic_gzip(raw_path, body)
        with connection:
            for ordinal, record in enumerate(docs):
                identifier = record.get("identifier")
                if not isinstance(identifier, str) or not ID_RE.fullmatch(identifier):
                    raise ValueError("Unexpected discovery identifier")
                if record.get("type") != "book" or not any(c.get("code") == "aco" for c in record.get("collections", []) if isinstance(c, dict)):
                    raise ValueError("Discovery returned a non-ACO book")
                manifest_url = safe_url(record.get("iiif", {}).get("presentation", ""))
                existing = connection.execute("SELECT manifest_url FROM books WHERE book_id=?", (identifier,)).fetchone()
                if existing and existing[0] != manifest_url:
                    raise ValueError("Conflicting manifests for one source identifier")
                connection.execute("INSERT OR IGNORE INTO books(book_id,manifest_url,discovery_json) VALUES(?,?,?)", (identifier, manifest_url, canonical(record)))
                connection.execute("INSERT INTO discovery_occurrences VALUES(?,?,?,?)", (offset, ordinal, identifier, canonical(record)))
            connection.execute("INSERT INTO discovery_pages VALUES(?,?,?,?,?,?)", (offset, len(docs), total, proof["decoded_response_sha256"], str(raw_path.relative_to(client.output)), canonical(proof)))
        processed += 1
        offset += page_size
        print(canonical({"phase": "discover", "source_records_traversed": offset, "expected_source_records": total, "unique_book_ids": connection.execute("SELECT count(*) FROM books").fetchone()[0]}), flush=True)
    complete = expected is not None and offset >= expected
    with connection:
        connection.execute("INSERT OR REPLACE INTO settings VALUES('discovery_complete',?)", (str(complete).lower(),))


def save_manifest(connection: sqlite3.Connection, client: Client, identifier: str, body: bytes, proof: dict, book: dict, pages: list[dict]):
    raw_path = client.output / "raw/manifests" / (identifier + "-" + proof["decoded_response_sha256"] + ".json.gz")
    ensure_space(client.output, len(body) * 3)
    atomic_gzip(raw_path, body)
    with connection:
        connection.execute("DELETE FROM pages WHERE book_id=?", (identifier,))
        connection.executemany("INSERT INTO pages VALUES(?,?,?,?,?)", [(identifier, p["source_canvas_ordinal"], p["source_canvas_id"], p["source_image_service"], zlib.compress(canonical(p).encode(), level=1)) for p in pages])
        connection.execute("UPDATE books SET state='complete',error=NULL,manifest_sha256=?,raw_path=?,proof_json=?,book_json=?,canvas_count=? WHERE book_id=?", (proof["decoded_response_sha256"], str(raw_path.relative_to(client.output)), canonical(proof), canonical(book), len(pages), identifier))


def reparse(connection: sqlite3.Connection, client: Client):
    """Re-normalize verified cached manifests without any source requests."""
    cached_rows = connection.execute("SELECT * FROM books WHERE state='complete' ORDER BY book_id").fetchall()
    for cached in cached_rows:
        verify_raw(client.output, cached["raw_path"], cached["manifest_sha256"])
        body = gzip.decompress((client.output / cached["raw_path"]).read_bytes())
        proof = json.loads(cached["proof_json"])
        book, pages = parse_manifest(json.loads(body), cached["book_id"], cached["manifest_sha256"])
        save_manifest(connection, client, cached["book_id"], body, proof, book, pages)
    print(canonical({"phase": "reparse", "cached_manifests_reparsed": len(cached_rows)}), flush=True)


def book_export_row(row) -> dict:
    return {**(json.loads(row["book_json"]) if row["book_json"] else {"source_book_id": row["book_id"], "manifest_url": row["manifest_url"]}),
            "manifest_state": row["state"], "error": row["error"],
            "discovery_occurrence_count": row["occurrence_count"],
            "raw_manifest_path": row["raw_path"],
            "retrieval_proof": json.loads(row["proof_json"]) if row["proof_json"] else None}


def write_jsonl_gzip(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        with temporary.open("wb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, filename="") as stream:
                for index, row in enumerate(rows):
                    if index % 10000 == 0:
                        ensure_space(path.parent, 10 * 1024 ** 2)
                    stream.write((canonical(row) + "\n").encode())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def file_proof(path: Path, output: Path) -> dict:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return {"path": str(path.relative_to(output)), "bytes": path.stat().st_size, "sha256": h.hexdigest()}


def atomic_json(path: Path, value: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    os.replace(temporary, path)


def write_progress(connection: sqlite3.Connection, output: Path):
    states = dict(connection.execute("SELECT state,count(*) FROM books GROUP BY state"))
    total = connection.execute("SELECT coalesce(sum(canvas_count),0) FROM books WHERE state='complete'").fetchone()[0]
    atomic_json(output / "inventory-progress.json", {"generated_at": now(), "book_states": states, "unique_source_book_ids": sum(states.values()), "source_canvas_occurrences": total,
                "normalized_page_exports": "checkpoint-indexed shards; top-level page inventory is a separate dated full snapshot", "unique_physical_pages_verified": False})


def checkpoint(connection: sqlite3.Connection, output: Path, size: int, flush: bool = False):
    if size < 1:
        raise ValueError("Checkpoint size must be positive")
    ids = [r[0] for r in connection.execute("SELECT b.book_id FROM books b LEFT JOIN checkpointed_books c ON b.book_id=c.book_id WHERE b.state='complete' AND c.book_id IS NULL ORDER BY b.book_id LIMIT ?", (size,))]
    if ids and (len(ids) >= size or flush):
        sequence = connection.execute("SELECT coalesce(max(sequence),0)+1 FROM checkpoints").fetchone()[0]
        directory = output / "checkpoints" / f"batch-{sequence:05d}"
        placeholders = ",".join("?" for _ in ids)
        book_rows = connection.execute(f"SELECT b.*,(SELECT count(*) FROM discovery_occurrences d WHERE d.book_id=b.book_id) occurrence_count FROM books b WHERE b.book_id IN ({placeholders}) ORDER BY b.book_id", ids)
        write_jsonl_gzip(directory / "books.jsonl.gz", (book_export_row(r) for r in book_rows))
        page_rows = connection.execute(f"SELECT row_json FROM pages WHERE book_id IN ({placeholders}) ORDER BY book_id,ordinal", ids)
        write_jsonl_gzip(directory / "pages.jsonl.gz", (decode_page(r[0]) for r in page_rows))
        count = connection.execute(f"SELECT coalesce(sum(canvas_count),0) FROM books WHERE book_id IN ({placeholders})", ids).fetchone()[0]
        metadata = {"sequence": sequence, "created_at": now(), "book_count": len(ids), "source_canvas_occurrences": count,
                    "books": file_proof(directory / "books.jsonl.gz", output), "pages": file_proof(directory / "pages.jsonl.gz", output),
                    "acquisition_code_sha256": digest(Path(__file__).read_bytes())}
        with connection:
            connection.execute("INSERT INTO checkpoints VALUES(?,?)", (sequence, canonical(metadata)))
            connection.executemany("INSERT INTO checkpointed_books VALUES(?,?)", [(identifier, sequence) for identifier in ids])
        full_books = connection.execute("SELECT b.*,(SELECT count(*) FROM discovery_occurrences d WHERE d.book_id=b.book_id) occurrence_count FROM books b ORDER BY b.book_id")
        write_jsonl_gzip(output / "book-inventory.jsonl.gz", (book_export_row(r) for r in full_books))
    batches = [json.loads(row[0]) for row in connection.execute("SELECT metadata_json FROM checkpoints ORDER BY sequence")]
    atomic_json(output / "checkpoints/index.json", {"schema_version": SCHEMA_VERSION, "generated_at": now(), "checkpointed_books": sum(b["book_count"] for b in batches), "checkpointed_source_canvas_occurrences": sum(b["source_canvas_occurrences"] for b in batches), "checkpoints": batches})
    write_progress(connection, output)


def manifests(connection: sqlite3.Connection, client: Client, max_books: int | None, retry_errors: bool, workers: int, checkpoint_every: int):
    if workers not in (1, 2, 3):
        raise ValueError("Manifest worker count must be 1, 2 or 3")
    counts = dict(connection.execute("SELECT book_id,count(*) FROM pages GROUP BY book_id"))
    for cached in connection.execute("SELECT * FROM books WHERE state='complete'"):
        verify_raw(client.output, cached["raw_path"], cached["manifest_sha256"])
        book = json.loads(cached["book_json"])
        if counts.get(cached["book_id"], 0) != cached["canvas_count"] or book["manifest_sha256"] != cached["manifest_sha256"]:
            raise RuntimeError("Cached normalized manifest rows are inconsistent for " + cached["book_id"])
    states = "('pending','error')" if retry_errors else "('pending')"
    queue = list(connection.execute(f"SELECT book_id,manifest_url FROM books WHERE state IN {states}"))
    queue.sort(key=lambda item: digest(("bayanat-aco-inventory-v1\0" + item["book_id"]).encode()))
    if max_books is not None:
        queue = queue[:max_books]
    iterator = iter(queue)
    failures = 0
    next_checkpoint = checkpoint_every
    completed_since_checkpoint = 0
    checkpoint(connection, client.output, checkpoint_every)

    def fetch(item):
        body, proof = client.get(item["manifest_url"])
        raw_path = client.output / "raw/manifests" / (item["book_id"] + "-" + proof["decoded_response_sha256"] + ".json.gz")
        atomic_gzip(raw_path, body)
        source = None
        try:
            source = json.loads(body)
            book, pages = parse_manifest(source, item["book_id"], proof["decoded_response_sha256"])
        except ValueError as error:
            count = len(source["items"]) if isinstance(source, dict) and isinstance(source.get("items"), list) else None
            raise ManifestValidationError(str(error), proof, str(raw_path.relative_to(client.output)), count) from error
        return body, proof, book, pages

    with ThreadPoolExecutor(max_workers=workers) as pool:
        pending = {}
        for _ in range(workers):
            item = next(iterator, None)
            if item is not None:
                pending[pool.submit(fetch, item)] = item
        try:
            while pending:
                completed, _ = wait_futures(pending, return_when=FIRST_COMPLETED)
                for future in completed:
                    item = pending.pop(future)
                    identifier = item["book_id"]
                    try:
                        body, proof, book, pages = future.result()
                        save_manifest(connection, client, identifier, body, proof, book, pages)
                        completed_since_checkpoint += 1
                        if completed_since_checkpoint >= next_checkpoint:
                            checkpoint(connection, client.output, checkpoint_every)
                            completed_since_checkpoint = 0
                        failures = 0
                    except (ValueError, requests.RequestException, PermissionError) as error:
                        with connection:
                            if isinstance(error, ManifestValidationError):
                                connection.execute("UPDATE books SET state='error',error=?,manifest_sha256=?,raw_path=?,proof_json=?,canvas_count=? WHERE book_id=?", (str(error), error.proof["decoded_response_sha256"], error.raw_path, canonical(error.proof), error.canvas_count, identifier))
                            else:
                                connection.execute("UPDATE books SET state='error',error=? WHERE book_id=?", (str(error), identifier))
                        failures += 1
                        print(canonical({"phase": "manifest", "book_id": identifier, "error": str(error)}), flush=True)
                        if isinstance(error, PermissionError) or failures >= 3:
                            raise RuntimeError("Three consecutive source failures or robots restriction; stopping for review") from error
                    print(canonical({"phase": "manifest", "book_id": identifier, "completed_books": connection.execute("SELECT count(*) FROM books WHERE state='complete'").fetchone()[0], "source_canvas_occurrences": connection.execute("SELECT coalesce(sum(canvas_count),0) FROM books WHERE state='complete'").fetchone()[0]}), flush=True)
                    item = next(iterator, None)
                    if item is not None:
                        pending[pool.submit(fetch, item)] = item
        finally:
            for future in pending:
                future.cancel()
    checkpoint(connection, client.output, checkpoint_every, flush=True)


def export(connection: sqlite3.Connection, output: Path):
    def write_rows(name: str, sql: str, transform):
        temporary = output / (name + ".tmp")
        with temporary.open("wb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as stream:
                for row in connection.execute(sql):
                    stream.write((canonical(transform(row)) + "\n").encode())
        os.replace(temporary, output / name)
    write_rows("book-inventory.jsonl.gz", "SELECT b.*, (SELECT count(*) FROM discovery_occurrences d WHERE d.book_id=b.book_id) occurrence_count FROM books b ORDER BY b.book_id", book_export_row)
    write_rows("page-inventory.jsonl.gz", "SELECT row_json FROM pages ORDER BY book_id,ordinal", lambda r: decode_page(r[0]))
    states = dict(connection.execute("SELECT state,count(*) FROM books GROUP BY state"))
    count = connection.execute("SELECT count(*),count(DISTINCT canvas_id),count(DISTINCT image_service) FROM pages").fetchone()
    discovery = connection.execute("SELECT coalesce(sum(row_count),0),max(expected_total) FROM discovery_pages").fetchone()
    complete = connection.execute("SELECT value FROM settings WHERE key='discovery_complete'").fetchone()
    proposals = {}
    year_flags = {}
    for row in connection.execute("SELECT book_json FROM books WHERE state='complete'"):
        book = json.loads(row[0])
        proposals[book["eligibility_proposal"]] = proposals.get(book["eligibility_proposal"], 0) + 1
        for flag in book["quality_flags"]:
            year_flags[flag] = year_flags.get(flag, 0) + 1
    report = {"schema_version": SCHEMA_VERSION, "generated_at": now(), "discovery_complete": bool(complete and complete[0] == "true"),
              "discovery_record_occurrences": discovery[0], "source_reported_discovery_records": discovery[1],
              "unique_source_book_ids": sum(states.values()), "book_states": states,
              "source_canvas_occurrences": count[0], "distinct_source_canvas_ids": count[1], "distinct_source_image_services": count[2],
              "image_content_unique_page_count": None, "image_payload_download_count": 0,
              "eligibility_proposals_by_book": proposals, "book_quality_flags": year_flags,
              "rights_notice": "Institutional public-domain assertions are preserved; no universal CC0 or new license grant is asserted. Publication-year flags are triage, not legal adjudications.",
              "scope_notice": "Canvas and service counts describe source metadata. Unique physical-page and image-content counts require image acquisition/deduplication."}
    for name in ("book-inventory.jsonl.gz", "page-inventory.jsonl.gz"):
        file = output / name
        h = hashlib.sha256()
        with file.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                h.update(block)
        report.setdefault("exports", {})[name] = {"bytes": file.stat().st_size, "sha256": h.hexdigest()}
    (output / "inventory-report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(canonical(report), flush=True)


def probe(connection: sqlite3.Connection, client: Client, limit: int):
    if not 1 <= limit <= 12:
        raise ValueError("Image probes must be bounded to 1..12 source pages")
    from PIL import Image
    rows = connection.execute("SELECT row_json FROM pages WHERE image_service IS NOT NULL ORDER BY book_id,ordinal LIMIT ?", (limit,))
    path = client.output / "image-probes.jsonl"
    with path.open("a") as stream:
        for item in rows:
            row = decode_page(item[0])
            for field in ("source_image_1024_url", "source_image_half_dimensions_url"):
                body, proof = client.get(row[field], max_bytes=4_000_000)
                with Image.open(io.BytesIO(body)) as image:
                    image.verify()
                with Image.open(io.BytesIO(body)) as image:
                    dimensions = list(image.size)
                result = {"source_book_id": row["source_book_id"], "source_canvas_ordinal": row["source_canvas_ordinal"], "candidate": field, "decoded_image_dimensions": dimensions, **proof}
                stream.write(canonical(result) + "\n")
                stream.flush()
                print(canonical(result), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("discover", "manifests", "export", "probe", "reparse", "all"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--delay", type=float, default=1.0, help="Minimum seconds between requests; serial, minimum0.5")
    parser.add_argument("--page-size", type=int, default=250)
    parser.add_argument("--max-discovery-pages", type=int)
    parser.add_argument("--max-books", type=int)
    parser.add_argument("--retry-errors", action="store_true")
    parser.add_argument("--workers", type=int, choices=(1,2,3), default=1)
    parser.add_argument("--checkpoint-every", type=int, default=100)
    parser.add_argument("--probe-pages", type=int, default=3)
    args = parser.parse_args()
    if args.max_books is not None and args.max_books < 1:
        parser.error("--max-books must be positive")
    if args.max_discovery_pages is not None and args.max_discovery_pages < 1:
        parser.error("--max-discovery-pages must be positive")
    connection = database(args.output)
    client = Client(args.output, args.delay)
    try:
        if args.phase in ("all", "discover"):
            discover(connection, client, args.page_size, args.max_discovery_pages)
        if args.phase in ("all", "manifests"):
            manifests(connection, client, args.max_books, args.retry_errors, args.workers, args.checkpoint_every)
        if args.phase == "reparse":
            reparse(connection, client)
        if args.phase == "probe":
            probe(connection, client, args.probe_pages)
        export(connection, args.output)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
