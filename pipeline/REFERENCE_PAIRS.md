# Source-associated image/reference pairs

`build_reference_pairs.py` pairs frozen original PDF pages with the separate
Wikisource community text resource. It runs no OCR and supplies no word boxes,
human-verified gold transcription, independent alignment certification or held-out
benchmark. Reference text covers the transcluded page body; non-transcluded
headers and footers are excluded. Some source PDFs are born digital, so the
component is described as PDF page renders rather than uniformly physical scans.

The first cohort requires source quality 3/4 agreement, an original PDF whose
observed version does not postdate the text revision, an explicit CC BY/BY-SA
file license and source VRT copyright-holder permission evidence. Known ACO-linked
files/binaries, image-rights conflict flags and the frozen Hindawi NC-ND original
are held. Source quality 3 and 4 remain distinct. These are source assertions and
conservative checks, not independent worldwide legal clearance.

Every downloaded original must match the source binary SHA1, exact byte size
and page count before rendering. The first twelve PDF pages undergo a negative
native-text CC restriction screen, with exact page/text/hash evidence retained.
This screen does not clear image-only notices or later pages. Source VRT
permission, exact file-description revisions, license, author and credit remain
in the metadata; community text has its separate CC BY-SA 4.0 terms.

Use the pinned project Python dependencies. A small private pilot is the default:

```sh
python pipeline/build_reference_pairs.py \
  --community /path/to/frozen-community-package \
  --output /path/to/reference-pilot \
  --max-pages 12 --max-pages-per-file 8 --max-pdf-bytes 16777216
```

The explicitly approved first production cohort uses the same two source PDFs:

```sh
python pipeline/build_reference_pairs.py \
  --community /path/to/frozen-community-package \
  --output /path/to/reference-release \
  --max-pages 400 --max-pages-per-file 512 --max-pdf-bytes 16777216
```

Requests are serial and identified, with robots checks, retry/backoff and a four
GiB free-space floor. One active PDF is cached at a time; this small cohort's
verified originals and proofs are retained privately for release review. All
Parquet remains local. There is no account creation, repository creation,
upload, publication or output eviction in this module.

Resume with identical inputs, code, rendering versions and per-file cap. A changed
configuration requires a new output directory. Maximum total pages can extend a
cohort under its existing per-file cap; prior success is invalidated until all
serialized output passes validation again. Candidate counts describe selection,
not necessarily retained unique rasters; exact decoded-pixel duplicates are
recorded separately.

Public artifacts are under `data/`, `metadata/` and `reports/`. Root packaging
owns remote SHA256 verification and sets `upload_sha256_verified` only after it
actually succeeds. Never publish `private/`. The authoritative component counts
are `metadata/reference-pairs-summary.json`; the index supplies exact raster,
source version and shard lineage. Validate an existing build without downloading:

```sh
python pipeline/build_reference_pairs.py \
  --community /path/to/frozen-community-package \
  --output /path/to/reference-release --validate-only
```

Run engineering-only regressions with:

```sh
python -m unittest discover -s quality -p 'test_reference_pairs.py' -v
```
