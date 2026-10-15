"""Reclaim only hash-bound source PDF caches after a builder run has stopped.

Run between builder invocations, never concurrently with a resuming builder.
Image shards must already be remotely verified. A later retry re-downloads the
PDF and checks its frozen hash, including for partially extracted documents.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
from datetime import datetime,timezone

def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda:f.read(8*1024**2),b''):h.update(block)
    return h.hexdigest()

def evict(output, *, builder_stopped=False, completed_single_pass_visits=False):
    if not (builder_stopped or completed_single_pass_visits):
        raise ValueError('Confirm the builder is stopped, or explicitly guarantee these are finished visits in one non-resuming single pass')
    output=Path(output).resolve();cache=output/'private/pdf-cache'
    if cache.is_symlink() or cache.resolve()!=cache:raise ValueError('Source cache must be a real build-owned directory')
    db=sqlite3.connect(f'file:{output/"private/build.sqlite"}?mode=ro',uri=True)
    if db.execute('SELECT count(*) FROM shards WHERE uploaded=0').fetchone()[0]:raise ValueError('Unverified shards remain; source cache retained')
    log=output/'private/input-eviction-ledger.jsonl';reclaimed=0;files=0
    # With the single-pass option the caller guarantees these source visits will
    # not be retried concurrently. SQLite status alone cannot guarantee that.
    for doc,expected,status in db.execute("SELECT document_id,pdf_sha256,status FROM documents WHERE pdf_sha256 IS NOT NULL AND status IN ('complete','partial','error')").fetchall():
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,100}',doc):raise ValueError('Invalid document cache identifier')
        path=cache/(doc+'.pdf')
        if not path.exists():continue
        info=path.lstat()
        if path.is_symlink() or not stat.S_ISREG(info.st_mode) or path.resolve().parent!=cache:raise ValueError('Unsafe source cache path')
        if digest(path)!=expected:raise ValueError('Source cache hash mismatch; review instead of deleting: '+doc)
        current=db.execute('SELECT status,pdf_sha256 FROM documents WHERE document_id=?',(doc,)).fetchone()
        if current!=(status,expected) or current[0]=='processing':continue
        if db.execute('SELECT count(*) FROM shards WHERE uploaded=0').fetchone()[0]:continue
        event={'document_id':doc,'pdf_sha256':expected,'bytes':info.st_size,'document_status':status,
               'protocol':'builder_stopped' if builder_stopped else 'finished_single_pass_visit_no_concurrent_retry',
               'recorded_at':datetime.now(timezone.utc).isoformat()}
        with log.open('a') as f:
            f.write(json.dumps({**event,'event':'planned'})+'\n');f.flush();os.fsync(f.fileno())
            path.unlink()
            f.write(json.dumps({**event,'event':'deleted'})+'\n');f.flush();os.fsync(f.fileno())
        reclaimed+=info.st_size;files+=1
    return {'files_reclaimed':files,'bytes_reclaimed':reclaimed}

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',required=True)
    p.add_argument('--builder-stopped',action='store_true')
    p.add_argument('--completed-single-pass-visits',action='store_true',help='Operator guarantee: terminal source visits are not retried by a concurrent builder')
    a=p.parse_args();print(json.dumps(evict(a.output,builder_stopped=a.builder_stopped,completed_single_pass_visits=a.completed_single_pass_visits)))
