"""Publish an owned, quiet Atlas package with resumable protected commits.

No repository is created here. Every uploaded path is explicit, every commit is
parent-protected, and a version becomes public only after metadata/hash checks.
The Hugging Face API object is intentionally patchable for offline state tests.
"""
from __future__ import annotations

from datetime import datetime, timezone
from contextlib import contextmanager
import fcntl
import hashlib
import json
from pathlib import Path, PurePosixPath
import re

from huggingface_hub import HfApi

REPO = 'Bayanat-Labs/arabic-document-atlas'
BATCH_SIZE = 60
REPORT = 'reports/remote-release-verification.json'
SUMMARY = 'metadata/release-summary.json'
REFERENCE_SUMMARY = 'metadata/reference-pairs-summary.json'


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def _digest(value):
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _json_bytes(value):
    return (json.dumps(value, ensure_ascii=False, indent=2) + '\n').encode()


def _atomic(path, data):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.publisher-tmp')
    with temporary.open('wb') as stream:
        stream.write(data); stream.flush()
        import os
        os.fsync(stream.fileno())
    temporary.replace(path)


def _save(path, value):
    _atomic(path, _json_bytes(value))


def _field(value, name, default=None):
    return value.get(name, default) if isinstance(value, dict) else getattr(value, name, default)


def _safe_path(value):
    if not isinstance(value, str) or not value or '\\' in value or any(c in value for c in '*?[]\r\n\0'):
        raise ValueError('Unsafe package path')
    path = PurePosixPath(value)
    if path.is_absolute() or any(p in ('..', '.', 'private', '.git', '__pycache__') for p in path.parts):
        raise ValueError('Unsafe package path: ' + value)
    return value


def _bytes_facts(data):
    return {'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest(),
            'git_blob_sha1': hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()}


def _file_facts(path):
    size = path.stat().st_size
    sha256 = hashlib.sha256(); git = hashlib.sha1(b'blob ' + str(size).encode() + b'\0')
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024**2), b''):
            sha256.update(block); git.update(block)
    if path.stat().st_size != size: raise ValueError('Local file changed while hashing')
    return {'bytes': size, 'sha256': sha256.hexdigest(), 'git_blob_sha1': git.hexdigest()}


def _manifest(release):
    result = {}
    for path in sorted(release.rglob('*')):
        if path.is_symlink(): raise ValueError('Symlink in frozen release package')
        if path.is_file(): result[_safe_path(path.relative_to(release).as_posix())] = _file_facts(path)
    if not result: raise ValueError('Empty release package')
    return result


def _primary(release):
    records = json.loads((release / 'metadata/shards.json').read_text())['shards']
    result = {}
    for row in records:
        name = _safe_path(row['path'])
        if not name.startswith('data/') or not name.endswith('.parquet') or name in result:
            raise ValueError('Invalid or duplicated primary shard path')
        if not re.fullmatch('[0-9a-f]{64}', str(row.get('sha256', ''))) or type(row.get('bytes')) is not int or row['bytes'] <= 0:
            raise ValueError('Primary shard hash/size missing')
        result[name] = {'sha256': row['sha256'], 'bytes': row['bytes'], 'git_blob_sha1': None}
    if not result: raise ValueError('No primary shard bindings')
    return result


def _expected(local, primary):
    result = dict(primary)
    for name, facts in local.items():
        if name in result and any(result[name][k] != facts[k] for k in ('bytes', 'sha256')):
            raise ValueError('Local primary shard differs from metadata binding')
        result[name] = facts
    return result


def _checksum_bytes(local, primary):
    expected = _expected(local, primary)
    return ''.join(f'{facts["sha256"]}  {name}\n' for name, facts in sorted(expected.items()) if name != 'SHA256SUMS').encode()


def _check_checksums(release, local, primary):
    if (release / 'SHA256SUMS').read_bytes() != _checksum_bytes(local, primary):
        raise ValueError('SHA256SUMS does not bind the exact local/evicted-primary package')


def _head(api):
    return api.repo_info(REPO, repo_type='dataset', files_metadata=True)


def _require_head(api, expected):
    remote = _head(api)
    if remote.sha != expected: raise ValueError('Remote HEAD changed outside the protected publication chain')
    return remote


def _tag(api, version):
    return next((r for r in api.list_repo_refs(REPO, repo_type='dataset').tags if r.name == version), None)


def _remote_facts(remote):
    result={}
    for entry in remote.siblings:
        name=_safe_path(entry.rfilename);size=_field(entry,'size');lfs=_field(entry,'lfs')
        if name in result or type(size) is not int or size<0: raise ValueError('Invalid full remote file facts')
        if lfs:
            value=_field(lfs,'sha256')
            if not re.fullmatch('[0-9a-f]{64}',str(value)): raise ValueError('Remote LFS facts lack SHA256')
            result[name]={'bytes':size,'lfs_sha256':value}
        else:
            value=_field(entry,'blob_id')
            if not re.fullmatch('[0-9a-f]{40}',str(value)): raise ValueError('Remote Git facts lack blob SHA1')
            result[name]={'bytes':size,'git_blob_sha1':value}
    return result


def _verify_entry(entry,facts,name):
    if entry is None or _field(entry,'size')!=facts['bytes']: raise ValueError('Remote file missing or byte-size mismatch: '+name)
    lfs=_field(entry,'lfs')
    if lfs:
        if _field(lfs,'sha256')!=facts['sha256']: raise ValueError('Remote LFS SHA256 mismatch: '+name)
    elif facts.get('git_blob_sha1') is None or _field(entry,'blob_id')!=facts['git_blob_sha1']:
        raise ValueError('Remote Git blob SHA1 mismatch or unavailable evicted content: '+name)


def _verify(api, commit, local, primary, *, exact=True):
    remote = api.repo_info(REPO, repo_type='dataset', revision=commit, files_metadata=True)
    if remote.sha != commit: raise ValueError('Remote revision did not resolve to expected immutable commit')
    indexed = {r.rfilename: r for r in remote.siblings}
    if len(indexed) != len(remote.siblings): raise ValueError('Duplicated remote file metadata')
    expected = _expected(local, primary)
    for name, facts in expected.items():
        _verify_entry(indexed.get(name),facts,name)
    if exact:
        if set(indexed)!=set(expected): raise ValueError('Remote exact data/reference-PDF/package path set differs')
        for prefix, suffix in [('data/', '.parquet'), ('source/reference_pdfs/', '.pdf')]:
            wanted = {p for p in expected if p.startswith(prefix) and p.endswith(suffix)}
            observed = {p for p in indexed if p.startswith(prefix) and p.endswith(suffix)}
            if observed != wanted: raise ValueError('Remote exact data/reference-PDF path set differs')
    return remote


def _local_unchanged(release, pending):
    if _manifest(release) != pending['package_manifest']:
        raise ValueError('Frozen local package changed during/resuming publication')


def _upload(api, release, statepath, state, paths, message):
    pending = state['pending_snapshot']
    _local_unchanged(release, pending)
    before=_require_head(api, pending['last_remote_head'])
    pending['inflight'] = {'paths': paths, 'parent_commit': pending['last_remote_head'],'previous_remote_facts':_remote_facts(before)}
    _save(statepath, state)
    info = api.upload_folder(repo_id=REPO, repo_type='dataset', folder_path=release,
                             allow_patterns=paths, parent_commit=pending['last_remote_head'], commit_message=message)
    commit = info.oid
    if not re.fullmatch('[0-9a-f]{40}', str(commit)): raise ValueError('Upload returned no immutable commit')
    # Persist the known owned outcome before a later verification can fail.
    pending['last_remote_head'] = commit; pending['inflight']['commit'] = commit
    _save(statepath, state)
    _require_head(api, commit)
    return commit


def _require_or_recover_head(api,release,statepath,state,primary):
    """Adopt a lost upload outcome only when the entire intended tree matches."""
    pending=state['pending_snapshot'];remote=_head(api)
    if remote.sha==pending['last_remote_head']: return remote
    intent=pending.get('inflight') or {}
    before=intent.get('previous_remote_facts')
    if intent.get('commit') or not before or intent.get('parent_commit')!=pending['last_remote_head']:
        raise ValueError('Remote HEAD changed outside the protected publication chain')
    _local_unchanged(release,pending)
    observed=_remote_facts(remote);paths=set(intent['paths'])
    if set(observed)!=set(before)|paths: raise ValueError('Unknown upload outcome differs from exact full intended remote file set')
    indexed={r.rfilename:r for r in remote.siblings}
    for name in set(before)-paths:
        if observed[name]!=before[name]: raise ValueError('Unknown upload outcome changed a prior remote file')
    for name in paths: _verify_entry(indexed[name],pending['package_manifest'][name],name)
    owned_paths=set(pending['uploaded_paths'])|paths
    _verify(api,remote.sha,{n:pending['package_manifest'][n] for n in owned_paths},primary,exact=False)
    pending['last_remote_head']=remote.sha
    intent.update(commit=remote.sha,reconciliation='exact_entire_remote_file_set_hashes_and_sizes_match_persisted_upload_intent')
    _save(statepath,state)
    return remote


def _final_plan(release, pending, primary, version):
    local = pending['package_manifest']; expected = _expected(local, primary)
    summary = json.loads((release / SUMMARY).read_text())
    counts = {'local_files_verified': len(local), 'primary_shards_verified': len(primary),
              'community_parquet_files_verified': sum(p.startswith('data/community_text/') and p.endswith('.parquet') for p in expected),
              'reference_parquet_files_verified': sum(p.startswith('data/reference_pairs/') and p.endswith('.parquet') for p in expected),
              'reference_pdf_files_verified': sum(p.startswith('source/reference_pdfs/') and p.endswith('.pdf') for p in expected)}
    report = {'status': 'passed', 'version': version, 'data_commit': pending['last_remote_head'],
              'primary_data_commit_before_documentation': summary['page_data_commit_before_documentation'],
              'verified_at': datetime.now(timezone.utc).isoformat(), 'counts': counts,
              'verified_local_manifest_sha256': _digest(local),
              'exact_data_paths': sorted(p for p in expected if p.startswith('data/') and p.endswith('.parquet')),
              'exact_reference_pdf_paths': sorted(p for p in expected if p.startswith('source/reference_pdfs/') and p.endswith('.pdf')),
              'release_counts_bound_by_verified_hashes': {k:v for k,v in summary.items() if isinstance(v,(int,float)) and not isinstance(v,bool)},
              'scope': 'All initial package files and evicted primary shards were checked by remote byte size and LFS SHA256 or Git blob SHA1 at data_commit. This report, updated flags and checksums are reverified in the subsequent protected final metadata commit before tagging.'}
    summary['remote_community_sha256_verified'] = counts['community_parquet_files_verified'] > 0
    summary['remote_reference_sha256_verified'] = counts['reference_parquet_files_verified'] > 0
    summary['remote_reference_pdf_sha256_verified'] = counts['reference_pdf_files_verified'] > 0
    generated = {SUMMARY: _json_bytes(summary).decode(), REPORT: _json_bytes(report).decode()}
    if REFERENCE_SUMMARY in local:
        reference = json.loads((release / REFERENCE_SUMMARY).read_text())
        if reference.get('validation_passed') is not True or not counts['reference_parquet_files_verified']:
            raise ValueError('Reference summary lacks validated remotely checked reference data')
        reference['upload_sha256_verified'] = True
        generated[REFERENCE_SUMMARY] = _json_bytes(reference).decode()
    final = dict(local)
    for name, content in generated.items(): final[name] = _bytes_facts(content.encode())
    generated['SHA256SUMS'] = _checksum_bytes(final, primary).decode()
    final['SHA256SUMS'] = _bytes_facts(generated['SHA256SUMS'].encode())
    return generated, final


def _materialize(release, pending):
    """Resume even if a crash replaced only some planned generated text files."""
    generated = pending['generated_files']; previous = pending['package_manifest']; final = pending['final_manifest']
    for name, content in generated.items():
        temporary = release / (name + '.publisher-tmp')
        if temporary.exists():
            if temporary.is_symlink() or temporary.read_bytes() != content.encode(): raise ValueError('Unknown generated temporary file')
            temporary.unlink()
    observed = _manifest(release)
    if set(observed) - set(final): raise ValueError('Unexpected local files appeared during finalization')
    for name in set(previous) | set(final):
        facts = observed.get(name)
        allowed = [previous.get(name), final.get(name)] if name in generated else [previous.get(name)]
        if facts not in allowed: raise ValueError('Frozen local file changed outside generated final metadata')
    for name, content in generated.items(): _atomic(release / name, content.encode())
    if _manifest(release) != final: raise ValueError('Generated final metadata differs from persisted plan')


def _finish_visibility(api, statepath, state, commit, public):
    observed=_require_head(api, commit)
    if public and _field(observed,'private',True):
        try: api.update_repo_settings(REPO, repo_type='dataset', private=False)
        except Exception:
            observed = _require_head(api, commit)
            if _field(observed, 'private', True): raise
        observed = _require_head(api, commit)
        if _field(observed, 'private', True): raise ValueError('Public visibility was not confirmed')
    state.update(private=bool(_field(observed,'private',True)),dataset_public=not bool(_field(observed,'private',True)))
    _save(statepath, state)


def _publish(output, version, public):
    output = Path(output).resolve(); release = output / 'release'; statepath = output / 'private/publication.json'
    state = json.loads(statepath.read_text())
    if state.get('repo_id') != REPO or state.get('dataset_created') is not True:
        raise ValueError('Builder-owned repository publication state required')
    if not isinstance(public, bool) or not re.fullmatch(r'v\d+\.\d+\.\d+(?:[-+][A-Za-z0-9.-]+)?', version):
        raise ValueError('Explicit public boolean and version tag required')
    if release.is_symlink(): raise ValueError('Release root must not be a symlink')
    summary = json.loads((release / SUMMARY).read_text())
    if summary.get('version') != version: raise ValueError('Package version mismatch')
    pending=state.get('pending_snapshot')
    local = _manifest(release); primary = _primary(release)
    if not pending or pending.get('phase')!='materialize_final': _check_checksums(release, local, primary)
    api = HfApi(); existing = _tag(api, version)
    if existing:
        commit = existing.target_commit
        owned = state.get('dataset_version') == version and state.get('dataset_commit') == commit
        pending_owned = (pending and pending.get('version') == version and pending.get('commit') == commit
                         and pending.get('phase') in ('tag_pending','complete'))
        if not owned and not pending_owned: raise ValueError('Immutable version tag exists outside owned publication state')
        bound = (pending or {}).get('package_manifest') or state.get('completed_snapshot',{}).get('package_manifest')
        if bound is not None and bound != local: raise ValueError('Local package differs from owned immutable version')
        if REPORT not in local: raise ValueError('Owned tag has no local remote-verification evidence')
        _require_head(api, commit); _verify(api, commit, local, primary)
        state.update(dataset_commit=commit,dataset_version=version,dataset_staged=True)
        if pending_owned: pending['phase']='complete'
        state['completed_snapshot']={'version':version,'commit':commit,'package_manifest':local,'package_manifest_sha256':_digest(local)}
        _finish_visibility(api,statepath,state,commit,public)
        return {'stage':'dataset_snapshot_already_published','version':version,'commit':commit,'public':bool(state.get('dataset_public'))}
    if pending:
        if pending.get('version') != version: raise ValueError('Another version has unfinished publication state')
        if pending.get('schema') != 1: raise ValueError('Unsupported pending snapshot state; explicit reconciliation required')
        if _digest(pending['package_manifest'])!=pending['package_manifest_sha256']: raise ValueError('Pending package manifest digest differs')
        _require_or_recover_head(api,release,statepath,state,primary)
        if pending['phase'] != 'materialize_final': _local_unchanged(release,pending)
    else:
        initial = summary.get('page_data_commit_before_documentation')
        if not re.fullmatch('[0-9a-f]{40}',str(initial)): raise ValueError('Missing frozen primary data commit')
        _require_head(api, initial); _verify(api,initial,{},primary,exact=False)
        pending={'schema':1,'version':version,'phase':'uploading','initial_remote_head':initial,'last_remote_head':initial,
                 'package_manifest':local,'package_manifest_sha256':_digest(local),'uploaded_paths':[],'inflight':None}
        state['pending_snapshot']=pending;_save(statepath,state)
    if pending['phase']=='uploading':
        inflight=pending.get('inflight') or {}
        if inflight.get('commit'):
            _verify(api,pending['last_remote_head'],{p:pending['package_manifest'][p] for p in inflight['paths']},{},exact=False)
            pending['uploaded_paths'].extend(p for p in inflight['paths'] if p not in pending['uploaded_paths'])
            pending['inflight']=None;_save(statepath,state)
        remaining=[p for p in sorted(pending['package_manifest']) if p not in pending['uploaded_paths']]
        for offset in range(0,len(remaining),BATCH_SIZE):
            batch=remaining[offset:offset+BATCH_SIZE]
            commit=_upload(api,release,statepath,state,batch,f'Arabic Document Atlas {version}: package batch {len(pending["uploaded_paths"])//BATCH_SIZE+1}')
            _verify(api,commit,{p:pending['package_manifest'][p] for p in batch},{},exact=False)
            pending['uploaded_paths'].extend(batch);pending['inflight']=None;_save(statepath,state)
        _local_unchanged(release,pending);_require_head(api,pending['last_remote_head'])
        _verify(api,pending['last_remote_head'],pending['package_manifest'],primary)
        generated,final=_final_plan(release,pending,primary,version)
        pending.update(phase='materialize_final',generated_files=generated,final_manifest=final)
        _save(statepath,state)
    if pending['phase']=='materialize_final':
        _require_head(api,pending['last_remote_head']);_materialize(release,pending)
        pending.update(package_manifest=pending['final_manifest'],package_manifest_sha256=_digest(pending['final_manifest']),phase='uploading_final')
        _save(statepath,state)
    if pending['phase']=='uploading_final':
        if (pending.get('inflight') or {}).get('commit'):
            _verify(api,pending['last_remote_head'],{p:pending['package_manifest'][p] for p in pending['generated_files']},{},exact=False)
        else:
            _upload(api,release,statepath,state,sorted(pending['generated_files']),f'Arabic Document Atlas {version}: verified final metadata')
        pending.update(phase='final_verifying',inflight=None);_save(statepath,state)
    if pending['phase']=='final_verifying':
        _local_unchanged(release,pending);_require_head(api,pending['last_remote_head'])
        _verify(api,pending['last_remote_head'],pending['package_manifest'],primary)
        pending.update(phase='tag_pending',commit=pending['last_remote_head']);_save(statepath,state)
    if pending['phase']!='tag_pending': raise ValueError('Unexpected publisher phase')
    commit=pending['commit'];_local_unchanged(release,pending);_require_head(api,commit)
    _verify(api,commit,pending['package_manifest'],primary)
    try: api.create_tag(REPO,repo_type='dataset',tag=version,revision=commit)
    except Exception:
        observed=_tag(api,version)
        if observed is None or observed.target_commit!=commit: raise
    observed=_tag(api,version)
    if observed is None or observed.target_commit!=commit: raise ValueError('Immutable tag target was not confirmed')
    pending['phase']='complete'
    state.update(dataset_commit=commit,dataset_version=version,dataset_staged=True,
                 completed_snapshot={'version':version,'commit':commit,'package_manifest':pending['package_manifest'],
                                     'package_manifest_sha256':pending['package_manifest_sha256']})
    _save(statepath,state);_finish_visibility(api,statepath,state,commit,public)
    return {'stage':'dataset_snapshot','version':version,'commit':commit,'public':bool(state.get('dataset_public'))}


@contextmanager
def _publication_lock(path):
    if path.is_symlink(): raise ValueError('Publisher lock must not be a symlink')
    with path.open('a+b') as handle:
        try: fcntl.flock(handle.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError as error: raise ValueError('Another snapshot publisher is active') from error
        try: yield
        finally: fcntl.flock(handle.fileno(),fcntl.LOCK_UN)


def publish(output,version,public):
    output=Path(output).resolve()
    with _publication_lock(output/'private/publish-snapshot.lock'):
        return _publish(output,version,public)
