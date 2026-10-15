#!/usr/bin/env python3
"""Mechanical source file-description template census; not legal clearance."""
import argparse
from collections import Counter
import gzip
import json
from pathlib import Path

import mwparserfromhell


def audit(source):
    templates, repositories = Counter(), Counter()
    problems = []
    count = 0
    suspicious = {"delete", "copyvio", "no permission since", "no license since", "no source since",
                  "disputed", "fair use", "copyright violation", "speedydelete", "حذف", "حذف سريع", "خرق"}
    for path in sorted(source.glob("raw/file-descriptions-v2/*/*.json.gz")):
        envelope = json.loads(gzip.decompress(path.read_bytes()))
        for page in envelope["response"].get("query", {}).get("pages", []):
            revision = (page.get("revisions") or [{}])[0]
            raw = revision.get("slots", {}).get("main", {}).get("content")
            if raw is None:
                raise ValueError("Source file description missing: " + page["title"])
            names = [str(t.name).strip().replace("_", " ").lower() for t in mwparserfromhell.parse(raw).filter_templates()]
            templates.update(names); repositories[path.parent.name] += 1; count += 1
            bad = sorted(set(names) & suspicious)
            if bad:
                problems.append({"title": page["title"], "templates": bad, "revision_id": revision["revid"], "api_url": envelope["api_url"]})
    report = {"description_records": count, "hosting_repository_counts": dict(repositories),
              "template_counts": dict(templates), "active_problem_templates": problems,
              "coverage_note": "Mechanical exact-template-name scan of original hosting-wiki file descriptions. Does not expand templates, independently verify copyright, or guarantee absence of rights disputes."}
    (source / "file-description-template-audit.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    return {k: report[k] for k in ("description_records", "hosting_repository_counts", "active_problem_templates")}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("--source", type=Path, required=True)
    args = parser.parse_args(); print(json.dumps(audit(args.source), ensure_ascii=False, indent=2))
