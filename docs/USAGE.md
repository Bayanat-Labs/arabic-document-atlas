# Working with the Atlas

Documentation and interfaces default to English. Original Arabic is preserved.

## Load image pages without downloading the corpus

```python
from datasets import load_dataset

pages = load_dataset(
    "Bayanat-Labs/arabic-document-atlas", "pages",
    split="train", streaming=True, revision="v0.1.0",
)
page = next(iter(pages))
display(page["image"])
print(page["page_text"])
print(page["words"][0])
```

Install the versioned Python dependencies from the source repository. For Arrow
or DuckDB, read the Parquet shards directly; the `image` struct stores JPEG
bytes and a suggested filename. SHA-256 values describe the encoded image and
the canonical decoded raster separately.

## Coordinates

`bbox` is `[left, top, right, bottom]`, in published-image pixels with a top-left
origin and half-open bounds. Width and height describe the actual decoded JPEG,
not an assumed scanner DPI. Apple Vision word guides are approximate, sometimes
overlapping, and unsuitable as pixel-accurate crop ground truth. Word confidence
copies the parent line's uncalibrated engine score; it is not word correctness
probability. Line order is a machine hypothesis and can fail on columns,
footnotes, poetry and tables.

## Research uses

- Continue pretraining document vision-language models on authentic Arabic scans.
- Train or compare OCR systems while treating machine labels as noisy supervision.
- Study historical print, Arabic/Latin mixed text and document-image robustness.
- Build citation-preserving visual document retrieval and region-highlighting tools.
- Analyze quality-screen yield, duplicate occurrences and extraction failures.

The images are historical printed pages. They do not establish coverage of
modern invoices, handwriting, receipts, identity documents or contemporary
dialects. They have no manually verified table cells, semantic regions or
reading-order annotations.

Titles and publication years are source catalog metadata. Bound volumes can
contain additional works, and catalog metadata can disagree with visible title
leaves. The pre-1931 rule applies to retrieved catalog edition metadata, not an
independent date verification of every page. Source assertions are preserved;
they are not silently replaced with an OCR guess.

## Community transcriptions

`community_text` is a separate text-and-source configuration. It preserves the
source's proofread/validated distinction. It does not contain scan bytes or
verified word boxes. Rendered text normally covers the transcluded page body,
excluding `noinclude` headers and footers; see row-level extraction warnings.
Some current scan versions postdate the text revision. This is community source
evidence, not Bayanat-verified error-free evaluation gold.

## Splits and leakage

`train` is the loading split for this resource collection. There is no published
held-out benchmark in the first release. Related editions and mirrored scans can
overlap; explicit ACO links from Wikisource are retained where present. Do not
randomly split individual pages and report an edition-held-out result. Design
and audit document/work/source separation for your intended evaluation.
