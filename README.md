# Bayanat Arabic Document Atlas

Authentic Arabic scanned pages with embedded JPEG images, machine OCR,
approximate word guides, source-version hashes and quality reports.

[Dataset](https://huggingface.co/datasets/Bayanat-Labs/arabic-document-atlas) ·
[Visual explorer](https://huggingface.co/spaces/Bayanat-Labs/arabic-document-atlas) ·
[Research and release record](https://www.bayanatlabs.com/open/arabic-document-atlas)

The published dataset card and pinned `metadata/release-summary.json` are the
authority for completed counts. A discovered catalog record or source canvas
is not a downloaded unique image. Word records, image variants and the separate
community-text tier are not added to the image-page count.

## Why it exists

Arabic document models need real page images with usable spatial/text structure
and inspectable provenance. This pipeline converts institutional scan PDFs into
a consistent page representation, checks localization/text reconstruction,
removes exact decoded-pixel duplicates, and binds each page to the frozen PDF
hash and page index. It complements existing book-level ACO OCR resources with
embedded images, approximate spatial annotations and source/quality controls.

Original scans and cataloging belong to the credited source institutions.
Arabic Wikisource contributors supply the separately attributed community text.
Bayanat did not perform that digitization or community proofreading.

## Use the published snapshot

```python
from datasets import load_dataset

pages = load_dataset(
    "Bayanat-Labs/arabic-document-atlas", "pages",
    split="train", streaming=True, revision="v0.1.0",
)
page = next(iter(pages))
print(page["page_text"])
image = page["image"]
```

See [usage and field semantics](docs/USAGE.md),
[quality contract](quality/QUALITY_CONTRACT.md) and
[source-rights audit](audit/SOURCE_RIGHTS_AND_QUALITY.md).

## Rebuild

Use Python 3.13 and `pip install -r requirements.txt`. The native OCR worker
requires macOS 26 with Arabic recognition support, a Swift compiler and Apple
Vision. The code is open; the system OCR model is a proprietary dependency.
Processing settings, binaries, source versions and dependency versions are
recorded by the builder. Different OS/model versions can produce different OCR.

```sh
python acquisition/inventory.py discover --output acquisition/private
python acquisition/inventory.py manifests --output acquisition/private \
  --workers 3 --delay 0.5 --checkpoint-every 100
swiftc -O pipeline/vision_ocr.swift -o /tmp/atlas-vision-ocr
python pipeline/build_pages.py --help
python pipeline/build_pages.py \
  --books acquisition/private/book-inventory.jsonl.gz \
  --output /path/to/atlas-build --vision-binary /tmp/atlas-vision-ocr \
  --workers 2 --max-pages 1000
```

Inspect the pilot before enabling `--upload --evict-verified`. Upload uses the
operator's normal Hugging Face credentials, stages the dataset privately, and
verifies the remote shard SHA-256 before removing its own local copy. It does
not buy storage or remove unrelated files. Keep at least 4 GiB free locally.
The publication script creates a version tag after packaging verified shards;
publishing the repository is an explicit `--public` option.

The source acquisition clients retain raw source evidence, honor robots and
rate limits, and resume from checkpoints. Input PDFs remain bound to their
hashes after failures. A changed processing configuration requires a new build;
do not silently mix OCR model versions into an existing snapshot.

## Limits that matter

- This is a historical printed-page collection, not modern invoice/handwriting coverage.
- OCR text, line order and word guides are machine output. Boxes are approximate;
  scores are uncalibrated line scores repeated on words. No human box gold is claimed.
- `train` is a loading split. This release is not an edition-held-out benchmark.
- The community-text configuration contains revision-attributed text and scan
  references, without image bytes or verified word boxes. Its proofread status
  is the source community's status, not a new Bayanat accuracy certification.
- Rights are component-specific. The image policy uses source institutional
  public-domain assertions and an edition-date triage rule, not worldwide clearance.

## License and citation

[Code: Apache-2.0](LICENSE).
[Data and documentation: component-specific terms](DATA_LICENSE.md).
[Original source attribution](ATTRIBUTION.md).
[Citation metadata](CITATION.cff).

Credit the exact dataset revision, supplying institution and relevant Wikisource
revision/history. Corrections: hello@bayanatlabs.com.
