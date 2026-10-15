# NYU ACO source inventory

This module inventories official Arabic Collections Online metadata. It does not acquire an image corpus. It records exact source canvas occurrences; neither a canvas count nor a service-URL count proves unique physical pages.

```sh
python inventory.py all --output private --max-discovery-pages 1 --max-books 10
python inventory.py discover --output private
python inventory.py manifests --output private --max-books 100
python inventory.py manifests --output private --workers 3 --delay 0.5 --checkpoint-every 100
python inventory.py export --output private
python inventory.py probe --output private --probe-pages 3
```

Discovery resumes at stored API offsets. Use the same page size on resume. It retains discovery occurrences while deduplicating source book identifiers. Manifest retrieval resumes pending books; `--retry-errors` retries failed books explicitly. Pending books use a deterministic hash order to avoid a provider-name prefix sample. The default client is serial; up to three manifest workers may overlap, sharing one global request-start limiter with a minimum interval of0.5seconds. It follows robots policies and Retry-After and stops after repeated source failures. It refuses further requests below a 4GiB free-disk floor. Request logs, decoded-response SHA-256 hashes, response headers and raw gzip JSON preserve source evidence, including bodies quarantined after manifest validation fails. Resumption verifies stored raw hashes and normalized row counts. SQLite page rows are compressed individually to limit disk usage.

Exports are `book-inventory.jsonl.gz`, `page-inventory.jsonl.gz` and `inventory-report.json`. Public-facing reuse is a separate pipeline decision: the inventory does not assign universal CC0 or independently establish worldwide public-domain status. It records NYU's exact statement, any explicit rights URI, publication-date tokens and unresolved edition/contributor review. The pre-1931 flag is a conservative triage proposal; it is not an automatic rights approval.

Long manifest runs write immutable incremental100-book shards to `checkpoints/batch-NNNNN/{books,pages}.jsonl.gz`. Read the atomically replaced `checkpoints/index.json` for completed batches, counts and SHA-256 digests. `inventory-progress.json` has live aggregate counters. The top-level book export is refreshed at checkpoints; the top-level page export is a dated full snapshot and is refreshed when the command ends. Consumers following a running acquisition must use the checkpoint index for all newly acquired pages. Do not concatenate the initial full page snapshot with checkpoint pages without deduplicating source occurrences.

`reparse` re-normalizes verified cached manifests without source requests. Existing immutable checkpoints are retained as earlier normalization artifacts; use a fresh checkpoint set if parsing policy changes after checkpoints have been issued.

Page fields include `source_book_id`, `source_canvas_ordinal`, `source_canvas_id`, `source_canvas_pointer`, source dimensions, original labels, `source_image_service`, 1024/1600-pixel candidates, half-dimension candidate, `manifest_sha256`, and rights provenance. Half dimensions approximate 200DPI only if the institutional 400ppi master guideline describes the served canvas; the image API does not independently certify DPI. Book exports contain bilingual titles, publication-date strings, provider/publisher, original metadata and retrieval proof. No inferred transcription, text bounding box, semantic reading order, or image-content uniqueness is asserted by this acquisition module.

Image probing is limited to at most12 source pages, two resolutions each, and4MB per response. Probe images are validated in memory, measured and discarded. The logged actual dimensions and bytes inform downstream storage/resolution choices.

Official sources: https://sites.dlib.nyu.edu/viewer/api/v1/image and https://web2.dlib.nyu.edu/about . Runtime dependency: `requests`; image probes additionally use Pillow.
