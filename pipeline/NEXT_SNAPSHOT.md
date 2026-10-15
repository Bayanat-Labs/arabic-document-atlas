# Publishing the next bounded snapshot

Use a new immutable version, for example `v0.2.0`, after a completed `v0.1.0`.
Keep the same task-owned build directory, frozen processing configuration,
SQLite/source bindings and `private/publication.json`. Do not edit that state
or move an existing Hub tag to make a new release pass.

Stop the builder and publisher before archiving. Check that the prior
`pending_snapshot.phase` is `complete`, its version/commit agree with
`completed_snapshot` and `dataset_version`/`dataset_commit`, and the previous
release summary names that version. An interrupted publication must be resumed
with its original package and version first.

Archive only your completed local package, preserving its bytes and checksums.
From the project directory, this example refuses an existing archive and leaves
publication state untouched:

```bash
python - /absolute/path/to/owned-build v0.1.0 <<'PY'
import json
from pathlib import Path
import sys

build = Path(sys.argv[1]).resolve()
version = sys.argv[2]
state = json.loads((build / "private/publication.json").read_text())
pending = state.get("pending_snapshot", {})
completed = state.get("completed_snapshot", {})
if (pending.get("phase") != "complete"
    or pending.get("version") != version
    or completed.get("version") != version
    or state.get("dataset_version") != version
    or pending.get("commit") != completed.get("commit")
    or state.get("dataset_commit") != completed.get("commit")):
    raise SystemExit("Prior publication is not a matching completed snapshot")
release = build / "release"
if release.is_symlink():
    raise SystemExit("Refusing a symlinked release")
if json.loads((release / "metadata/release-summary.json").read_text())["version"] != version:
    raise SystemExit("Local release version differs")
archive = build / "snapshots" / version
if archive.exists() or archive.is_symlink():
    raise SystemExit("Preserve the existing snapshot archive")
archive.parent.mkdir(exist_ok=True)
release.rename(archive)
print(archive)
PY
```

Resume the frozen builder for the next bounded cohort using the same owned
inputs and output directory. Once it stops, verify all shards, regenerate the
quiet coverage report and package the expanded build under the new version.
Supply the original validated community/reference component directories again
when they are part of the release. The package should include the complete
retained cohort, including earlier source shards, rather than only new rows.

Publish that new package through `release.py --publish-existing --publish` with
the new `--version`, adding `--public` only when publication is intended. On
startup the publisher checks the prior completed receipt and its exact remote
tag, then checks the new package's initial remote HEAD and source shard hashes.
It atomically records the old completed proof in `completed_snapshot_history`
and starts the new transaction. Retries resume that transaction without adding
duplicate history entries. The old generated remote verification report is
temporarily admitted only at its previously bound hash, then replaced by the
new final verification report. The final remote file set remains exact.

If a guard rejects an unfinished receipt, changed previous tag, stale HEAD or
foreign new tag, preserve the evidence and diagnose it. Do not delete pending
state or regenerate an already staged version to bypass the guard. A new
snapshot does not restore source PDFs that were legitimately evicted; source
hashes remain bound and any redownload must match them.
