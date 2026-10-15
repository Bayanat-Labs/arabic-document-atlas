# Durable page builder validation

The first image release is a **training-only resource**, with no held-out benchmark claim. An image row has passed mechanical source/structure checks, not a human transcription or localization review. The builder is `pipeline/build_pages.py`; source-independent tests are `quality/test_build_pages.py`.

## Source and processing identity

A document's observed source PDF SHA-256, PDF URL/page count, manifest SHA-256/URL and source metadata-record hash are immutable in the local state database. Recording an error changes status/error only. A retry cannot erase these bindings or accept different cached/upstream PDF bytes. Every page retains its exact PDF hash and zero-based PDF page index. The PDF/manifest correspondence uses publisher order with equal counts; equal counts are not independent verification of every page pair.

`private/processing-config.json` binds the semantic processing profile, builder and validator source hashes, compiled native worker and Swift source hashes, PDFium library hash/version, package versions and operating-system version. The canonical config hash is stored in each row. A changed semantic configuration requires a new output directory. Input-inventory revisions are recorded separately by SHA-256 and byte count, allowing appended books while existing document bindings stay fixed.

Production renders publisher high-resolution PDFs through PDFium, with a 2400-pixel long edge and JPEG quality 90. This is a raster profile, not a scanner-DPI claim. PDFium rendering uses a lock because the library is not thread safe. On-device native Vision OCR remains parallel. A native response has a 60-second monotonic deadline, a bounded response size and an explicit complete-line protocol; timeout/error kills that process before the next request. The deadline covers the OCR subprocess, not the in-process PDF renderer.

## Schema and exact duplicates

`page_schema()` is explicit, including typed empty word/line lists and nullable reference/proofread/confidence fields. Blank OCR results cannot cause later text-bearing shards to acquire an incompatible schema. The image struct uses the nullable field shape expected by Hugging Face Image metadata; mechanical validation still requires actual nonempty image bytes. Parquet stores actual encoded image bytes, and an integration test verifies that `load_dataset(..., streaming=True)` decodes them to a PIL image rather than returning an untyped dictionary. Schema drift and geometry/text reconstruction failures fail closed.

`pages` in SQLite and `data/pages/` in the release contain one retained image per canonical decoded RGBA SHA-256. Exact duplicate decisions cover earlier batches and the current batch. Pixel hashing binds dimensions/mode and preserves alpha; a perceptual hash does not authorize deletion.

`occurrences` records every successfully processed source canvas/index, including exact duplicates. Resume determines completed indices from this table, so a duplicate does not get processed again. `data/duplicate_occurrences/` retains the duplicate source PDF/manifest/canvas/edition/rights lineage, the duplicate encoded-image hash, the shared pixel hash and the retained page ID/image hash. Source-occurrence counts, retained unique-image counts and word counts are separate quantities. Identical rasters can arise on distinct physical pages; first-row retention is a storage/deduplication policy, not a claim that source editions are identical.

A shard is written atomically and then registered with its SHA-256 and byte length in the same database transaction as its page/occurrence rows. Resume checks registered files and fails on corrupted, missing unuploaded, or unregistered Parquet files. A crash between file creation and database commit therefore requires explicit review rather than silent overwrite. Optional local eviction follows verified Hub LFS SHA-256 confirmation. After a book visit finishes, both complete and partial source PDF caches may be evicted only after all output shards are verified, its own PDF hash is rechecked, and a planned/deleted ledger is flushed. Failed OCR page indices remain retryable; the surviving PDF binding requires any later redownload to have the identical hash. A concurrent second builder/resume against the same output is unsupported.

## Tests and actual pilot

On 2026-10-15, all 31 tests passed in the shared build environment, with two generated-Arabic-raster integrations explicitly skipped because that Pillow installation lacked RAQM. All 22 validator tests also passed under the local system Python with RAQM, including actual Arabic Tesseract and Apple Vision extraction. The nine additional builder tests cover:

- Source hash/metadata bindings surviving failures and rejecting mutation on resume.
- Stable Arrow/Parquet schemas for an empty OCR page and a text-bearing page.
- Exact pixel duplicates across encodings and batches, full source lineage, and processed indices.
- Uncommitted/corrupted shard detection before resume.
- A native worker returning only partial JSON, being timed out/killed, then restarting successfully.
- Inventory additions being recorded while a changed processing binary is rejected.
- Multiline institutional rights statements being preserved while invalid controls/IDs fail before OCR.
- Partial-book PDF eviction requiring verified outputs and preserving source hash bindings.

The v2 real-source, **no-upload** pilot processed the first 64 PDF positions from `princeton_aco000672`, an actual 94-page publisher high-resolution PDF. It produced 64 retained images, 12,655 machine word records, zero exact pixel duplicates and 55 mechanically screened pages. All 64 saved images were decoded again, their encoded/pixel hashes recomputed, their word/line geometry and membership revalidated, and their source/config bindings checked. The saved Arrow schema matched the explicit contract. All64 actual saved images also streamed through Hugging Face Datasets into PIL images, preserving word IDs and nullable reference fields. A second CLI run resumed without adding rows. Metadata preflight also passed all318 canvases of actual source book `princeton_aco004437`, whose institutional rights statement contains newlines; the source statement remains unchanged. Visual spot checks included covers, sparse institutional leaves and Arabic text pages; these were not a transcription-accuracy audit.

Pilot source PDF SHA-256: `b4c434fd46f13a349046eb661f039de4f88e5ba488f00eaa22f9bbb3c054760b`.

Pilot source manifest SHA-256: `eabfcf2ea833f1362088d8593c2e8e00f420e8cb4907ebc0f0753885b13bae8d`.

Pilot Parquet SHA-256: `cbeab033990a764b0e47baa7687d68689daf929a40895989e354c5b289a3a1ad` (43,676,150 bytes).

These observations describe this pilot only. They do not predict complete-catalog yield, speed, source eligibility or OCR accuracy, and engineering fixtures never contribute to corpus counts.
