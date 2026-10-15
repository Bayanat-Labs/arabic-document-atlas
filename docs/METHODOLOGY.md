# Collection and processing method

## Source acquisition

The ACO client discovers catalog occurrences, deduplicates source book IDs and
retrieves official IIIF manifests in a deterministic hash order. It stores raw
responses with decoded-response SHA-256 and retrieval evidence. Global request
spacing, robots checks, bounded retries, Retry-After, checkpoint files and a
4 GiB local free-space floor limit source and machine load. A catalog canvas
count is an inventory observation, not a unique-image or completed-release count.

Image selection requires Arabic source language, an institutional public-domain
assertion, an unambiguous retrieved Gregorian catalog edition year at or before
1930 and no conflicting retrieved rights URI. Ambiguous/modern/unknown editions
remain inventory-only. This date filter is project triage; source catalog years
do not independently establish the date or rights of every work in a bound volume.

## Visual representation

Each selected publisher high-resolution PDF is downloaded with a bounded size,
hashed and pinned to its document state. PDF page count must equal manifest
canvas count. The raster is rendered with PDFium to RGB, with a long edge at
most 2400 pixels, then encoded as JPEG quality 90. These settings do not certify
scanner DPI or native source resolution. The PDF hash and zero-based page index
identify the processed input. Canvas association follows the publisher's order;
equal counts do not independently verify every PDF/canvas pair.

Apple Vision revision 3 recognizes Arabic and English with accurate recognition,
language correction disabled and automatic language detection disabled. Word
boundaries follow non-whitespace spans in the engine candidate string. Vision
provides approximate character-range word guides; lines contain the union of
their original line box and associated word guides. Original engine line text
is retained. Text is reconstructed from every associated word without silently
dropping failed localizations. A page with missing/invalid localization is
quarantined and recorded as a failed attempt.

The native worker has a 60-second response deadline and is restarted after a
timeout. PDFium rasterization runs under a process-local render lock because
the library is not thread safe; OCR workers run concurrently. Rasterization is
in-process and has no separate per-page timeout in this version.

## Integrity and duplicates

The validator decodes each JPEG, recomputes dimensions/encoded SHA-256 and a
canonical RGBA pixel SHA-256, checks positive in-bounds half-open coordinates,
word-to-line membership, text reconstruction, source identities, public source
URLs and provenance fields. Blank/sparse pages can be structurally valid. The
mechanical curated flag checks minimum word count, Arabic-script letter fraction
and replacement-character rate; it is not OCR correctness or Arabic-language
classification. Text and unusual Unicode are preserved and flagged where safe.

One retained image row is written per exact canonical decoded-pixel SHA-256.
Other source occurrences retain full lineage in a separate duplicate table.
Perceptual dHash values are candidate evidence only and never automatically
delete pages. Exact text matches are reported, not silently treated as the same
visual page. Related editions can overlap even if pixel hashes differ.

Stable Arrow schemas cover blank and text pages. Hugging Face image metadata is
checked by actually streaming the written Parquet into PIL. Shards are small,
Zstandard-compressed Parquet files with embedded JPEG bytes and16-row groups.
The builder uploads a shard, checks the remote LFS SHA-256, records its commit,
then may delete its own local shard. Finished source-PDF visits can evict their
verified cache after all outputs are remote-confirmed, including partial visits;
retry requires the exact frozen PDF hash. Earlier releases/user files are untouched.

Processing configuration binds the builder, validator, native source/binary,
PDFium binary, dependency/OS/Python/Unicode versions and semantic settings.
Resume rejects changes to that configuration or an existing document's source
metadata/PDF/manifest bindings. New source inventories may append books, with
their inventory hashes recorded. Failed attempts never erase existing bindings.

## Community text

The Arabic Wikisource client retains exact page revisions, wikitext, source
quality 3/4 and exact file-rights metadata. It verifies independent source quality
fields and revision hashes. MediaWiki rendering preserves template-based text
that would be lost by blindly deleting wikitext. Multi-page batches require
correct wrapper/revision alignment; problematic source markup falls back to
single-page rendering. Parser errors and missing/restrictive scan evidence are
excluded from the packaged text tier, with reasons reported.

The transcluded body normally excludes `noinclude` headers/footers. Scan file
version timestamps may postdate proofreading revisions, and explicit links to
ACO source books are preserved where observed. The configuration contains text
and references without scan-image bytes, line boxes or independent Bayanat
proofreading. All first-release data use the `train` loading split; no held-out
evaluation or work/edition isolation is claimed.

## Reproduction and interpretation

Use the immutable Hub version tag, `SHA256SUMS`, shard manifest and processing
configuration. Future upstream replacements can make byte-identical PDF
reacquisition impossible; hashes detect that change instead of accepting it
silently. Compatible Apple system OCR is required to reproduce the native path;
the proprietary system model is not distributed with this Apache-2.0 code.

Useful primary sources:

- [ACO institutions, digitization and copyright explanation](https://aco.dlib.nyu.edu/about)
- [Official IIIF image API](https://sites.dlib.nyu.edu/viewer/api/v1/image)
- [Apple Vision bounding-box semantics](https://developer.apple.com/documentation/vision/vnrecognizedtext/boundingbox(for:))
- [pypdfium2 documentation and thread-safety notes](https://pypdfium2.readthedocs.io/en/stable/)
- [Wikimedia content licensing](https://foundation.wikimedia.org/wiki/Policy:Terms_of_Use/en#7._Licensing_of_Content)
- [Wikisource page-status definitions](https://en.wikisource.org/wiki/Help:Page_status)
- [Hugging Face storage policy](https://huggingface.co/docs/hub/main/storage-limits)

Original sources are credited in `ATTRIBUTION.md` and per-row evidence. Related
book-text OCR releases include
[AdaMLLab's ACO OCR collection](https://huggingface.co/datasets/AdaMLLab/nyu-aco-ocr-full).
The Atlas does not claim to be the first ACO OCR or the largest Arabic dataset.
