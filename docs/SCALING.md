# Scaling without losing the evidence

The pipeline separates source discovery, image processing and publication.
Inventory counts describe observed source records and canvases. Only retained,
decoded, validated image rows contribute to published image counts.

## Compute

The initial native path runs on an Apple Silicon Mac with 11 logical CPUs and
18 GiB RAM. Four concurrent Apple Vision workers recognize Arabic/English while
PDFium rendering is serialized for library thread safety. The first production
run observed approximately 4–5 retained pages per second end to end, including
image validation, source retrieval and verified shard uploads. The pinned
coverage report records its actual interval and rate; this is not a promise for
other books, machines or network conditions.

At 4–5 pages/second, one million pages would require approximately 56–69 hours
of uninterrupted processing. That is an extrapolation, not a completed dataset
size. More parallelism needs independently bounded jobs and a coordinated shard
namespace; running two writers against one build database is unsupported.
Native recognition depends on the compatible Apple system OCR model. The
pipeline does not provision cloud compute or incur paid service charges.

## Storage

The build needs the active source PDFs, an in-progress shard, SQLite state,
source evidence and temporary rendering files. JPEG bytes dominate Parquet
storage. Finished output is uploaded, checked against the remote SHA256 and
length, recorded in durable state, and only then eligible for local eviction.
Previously pinned source-PDF hashes remain mandatory even after cache eviction.
Four GiB of local free space is the stop floor; unrelated files are never removed.

Hugging Face public storage is subject to its current best-effort policy and
review, not an unlimited quota. Private organization storage has separate
limits. No paid storage subscription is created by the pipeline. Estimate the
next cohort from the snapshot's measured `page_parquet_bytes / unique_image_pages`
and check the actual account allowance before expanding it. See the
[Hub storage policy](https://huggingface.co/docs/hub/main/storage-limits).

## Snapshot procedure

1. Stop at a bounded processing target and wait for all shards to be verified.
2. Generate the coverage report from a quiet SQLite snapshot.
3. Revalidate optional community and reference components against their source
   evidence, then package only explicitly public artifacts.
4. Upload in bounded, parent-commit-protected batches and verify the complete
   remote file set, sizes and hashes before creating an immutable version tag.
5. Publish the versioned dataset, matching static explorer, source release and
   company research page. Smoke-test anonymous streaming against that version.
6. A later expansion uses the same frozen semantic processing configuration and
   source bindings, or starts a new build if those change. A new release gets a
   new immutable version. Never silently move an existing release tag.

The first release does not claim a million pages or the largest Arabic corpus.
Its public size is the verified snapshot in `metadata/release-summary.json`.
