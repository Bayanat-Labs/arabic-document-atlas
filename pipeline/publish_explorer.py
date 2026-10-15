"""Publish a verified static viewer of one already-public immutable dataset commit.

Retries reuse state-owned commits/tags after checking the complete artifact. A
pending upload intent permits recovery from an unknown upload response without
creating another commit. This module never adopts an unrelated existing Space.
"""
from __future__ import annotations
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
from huggingface_hub import HfApi

REPO = 'Bayanat-Labs/arabic-document-atlas'
ATTRIBUTES = b'*.jpg filter=lfs diff=lfs merge=lfs -text\n'
ROOT_FILES = {'index.html', 'app.js', 'style.css', 'README.md', 'samples.json',
              'DATA_LICENSE.md', 'ATTRIBUTION.md', 'LICENSE', '.gitattributes'}
COMMIT = re.compile(r'[0-9a-f]{40}')
IMAGE = re.compile(r'images/[0-9a-f]{64}\.jpg')


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def dump(path, value):
    temporary = path.with_suffix('.json.tmp')
    if path.is_symlink() or temporary.is_symlink():
        raise ValueError('Publication state must not be a symlink')
    with temporary.open('w') as stream:
        stream.write(json.dumps(value, indent=2) + '\n')
        stream.flush(); os.fsync(stream.fileno())
    temporary.replace(path)


def _owned_state(path):
    if path.is_symlink() or path.parent.is_symlink():
        raise ValueError('Publication state must be an owned regular file')
    state = json.loads(path.read_text())
    if not isinstance(state, dict) or state.get('repo_id') != REPO or state.get('dataset_created') is not True:
        raise ValueError('Builder-owned publication state required')
    if state.get('space_repo_id', REPO) != REPO:
        raise ValueError('Space ownership belongs to another repository')
    return state


def _dataset_binding(state):
    revision, version = state.get('dataset_commit'), state.get('dataset_version')
    if (state.get('dataset_public') is not True or not isinstance(revision, str)
            or COMMIT.fullmatch(revision) is None or not isinstance(version, str)
            or re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}', version) is None):
        raise ValueError('An already-public immutable dataset commit and version are required')
    return revision, version


def _save(path, state, **changes):
    # Preserve unrelated builder/dataset publication fields and reject a dataset
    # snapshot changing while this explorer publication is in flight.
    current = _owned_state(path)
    if _dataset_binding(current) != _dataset_binding(state):
        raise ValueError('Dataset publication state changed during explorer publication')
    current.update(changes)
    dump(path, current)
    state.clear(); state.update(current)


def _artifact(directory, revision, version):
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError('Static viewer must be an owned directory')
    attributes = directory / '.gitattributes'
    if attributes.is_symlink():
        raise ValueError('Unexpected static viewer symlink')
    if not attributes.exists():
        attributes.write_bytes(ATTRIBUTES)
    elif attributes.read_bytes() != ATTRIBUTES:
        raise ValueError('Unexpected static viewer Git attributes')
    manifest = {}
    for path in sorted(directory.rglob('*')):
        name = path.relative_to(directory).as_posix()
        if path.is_symlink():
            raise ValueError('Unexpected static viewer symlink: ' + name)
        if path.is_dir():
            if name != 'images':
                raise ValueError('Unexpected static viewer directory: ' + name)
            continue
        if not path.is_file() or not (name in ROOT_FILES or IMAGE.fullmatch(name)):
            raise ValueError('Unexpected static viewer file: ' + name)
        data = path.read_bytes()
        manifest[name] = {'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest(),
                          'git_blob_sha1': hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()}
    if not ROOT_FILES.issubset(manifest):
        raise ValueError('Missing required static viewer files')
    samples = json.loads((directory / 'samples.json').read_text())
    if (samples.get('dataset_revision') != revision or samples.get('summary', {}).get('version') != version
            or not isinstance(samples.get('pages'), list) or not samples['pages']):
        raise ValueError('Viewer must use this build’s already-public pinned dataset snapshot and actual examples')
    referenced = set()
    for page in samples['pages']:
        name = page.get('sample_image')
        if (not isinstance(name, str) or IMAGE.fullmatch(name) is None or name not in manifest
                or name in referenced or name != 'images/' + str(page.get('page_id')) + '.jpg'
                or manifest[name]['sha256'] != page.get('image_sha256')):
            raise ValueError('Viewer example image identity/hash differs')
        referenced.add(name)
    if referenced != {name for name in manifest if name.startswith('images/')}:
        raise ValueError('Unexpected or missing viewer example images')
    artifact = hashlib.sha256(canonical({'dataset_commit': revision, 'dataset_version': version, 'files': manifest}).encode()).hexdigest()
    return manifest, artifact


def _verify_remote(info, commit, manifest):
    if info.sha != commit:
        raise ValueError('Remote viewer commit differs from requested immutable commit')
    indexed = {entry.rfilename: entry for entry in info.siblings}
    if len(indexed) != len(info.siblings) or set(indexed) != set(manifest):
        raise ValueError('Remote viewer file set differs from complete local artifact')
    for name, expected in manifest.items():
        entry = indexed[name]
        if entry.size != expected['bytes']:
            raise ValueError('Viewer file size mismatch: ' + name)
        actual = entry.lfs.sha256 if entry.lfs else entry.blob_id
        wanted = expected['sha256'] if entry.lfs else expected['git_blob_sha1']
        if actual != wanted:
            raise ValueError('Viewer file hash mismatch: ' + name)


def _tag(api, version):
    matches = [ref for ref in api.list_repo_refs(REPO, repo_type='space').tags if ref.name == version]
    if len(matches) > 1:
        raise ValueError('Duplicate Space version refs')
    return matches[0].target_commit if matches else None


@contextmanager
def _publication_lock(output):
    path = output / 'private/explorer-publication.lock'
    if path.is_symlink():
        raise ValueError('Explorer publication lock must not be a symlink')
    with path.open('a') as stream:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError('Another explorer publisher is running') from error
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def publish(output):
    output = Path(output).resolve()
    statepath = output / 'private/publication.json'
    state = _owned_state(statepath)
    _dataset_binding(state)
    with _publication_lock(output):
        state = _owned_state(statepath)
        revision, version = _dataset_binding(state)
        return _publish(output, statepath, state, revision, version)


def _publish(output, statepath, state, revision, version):
    directory = output / 'explorer'
    uploaded = False
    manifest, artifact = _artifact(directory, revision, version)
    api = HfApi()
    dataset = api.repo_info(REPO, repo_type='dataset', revision=revision)
    if dataset.private is not False or dataset.sha != revision:
        raise ValueError('The exact pinned dataset commit must already be public')
    exists = api.repo_exists(REPO, repo_type='space')
    if state.get('space_created') is not True:
        if exists:
            raise ValueError('Space exists outside this publication state')
        if any(state.get(key) for key in ('space_commit', 'space_version', 'space_pending_snapshot')):
            raise ValueError('Space publication fields lack owned creation state')
        api.create_repo(REPO, repo_type='space', space_sdk='static', private=True)
        _save(statepath, state, space_created=True, space_repo_id=REPO)
    elif not exists:
        raise ValueError('State-owned Space no longer exists; do not recreate it implicitly')
    existing = _tag(api, version)
    pending = state.get('space_pending_snapshot')
    completed = state.get('space_version') == version and state.get('space_commit')
    if pending and (not isinstance(pending, dict) or pending.get('version') != version
                    or pending.get('artifact_sha256') != artifact or pending.get('dataset_commit') != revision):
        raise ValueError('Pending Space artifact differs; finish its frozen publication first')
    if completed and state.get('space_artifact_sha256') not in (None, artifact):
        raise ValueError('Version is already bound to a different local static artifact')
    owned_commit = pending.get('commit') if pending else state.get('space_commit') if completed else None
    if existing and (not owned_commit or owned_commit != existing):
        raise ValueError('Viewer version tag exists outside this completed/pending publication state')
    if existing:
        commit = existing
    elif owned_commit:
        commit = owned_commit
    else:
        head = api.repo_info(REPO, repo_type='space', files_metadata=True)
        if pending and head.sha != pending['parent_commit']:
            # The upload may have succeeded despite a lost response. Only an
            # exact complete artifact at HEAD can recover this pending intent.
            _verify_remote(head, head.sha, manifest)
            commit = head.sha
        else:
            if state.get('space_commit') and head.sha != state['space_commit']:
                raise ValueError('Owned Space HEAD changed outside publication state')
            intent = {'version': version, 'dataset_commit': revision, 'artifact_sha256': artifact,
                      'parent_commit': head.sha, 'commit': None}
            _save(statepath, state, space_pending_snapshot=intent)
            info = api.upload_folder(repo_id=REPO, repo_type='space', folder_path=directory,
                parent_commit=head.sha, allow_patterns=sorted(manifest), delete_patterns=['*'],
                commit_message='Arabic Document Atlas: static viewer of verified release examples')
            uploaded = True
            commit = info.oid
        pending = {'version': version, 'dataset_commit': revision, 'artifact_sha256': artifact,
                   'parent_commit': pending['parent_commit'] if pending else head.sha, 'commit': commit}
        _save(statepath, state, space_pending_snapshot=pending)
    remote = api.repo_info(REPO, repo_type='space', revision=commit, files_metadata=True)
    _verify_remote(remote, commit, manifest)
    # Re-read both filesystem and state before tagging; publication never certifies
    # files that changed during an upload or a different dataset snapshot.
    if _artifact(directory, revision, version) != (manifest, artifact):
        raise ValueError('Local viewer artifact changed during publication')
    _save(statepath, state)
    if api.repo_info(REPO, repo_type='space').sha != commit:
        raise ValueError('Space HEAD differs from the verified frozen viewer commit')
    if not existing:
        try:
            api.create_tag(REPO, repo_type='space', tag=version, revision=commit)
        except Exception:
            if _tag(api, version) != commit:
                raise
        if _tag(api, version) != commit:
            raise ValueError('Space version tag does not point to verified commit')
    if api.repo_info(REPO, repo_type='space').sha != commit:
        raise ValueError('Space HEAD changed before visibility update')
    if remote.private:
        api.update_repo_settings(REPO, repo_type='space', private=False)
    _save(statepath, state, space_commit=commit, space_public=True, space_version=version,
          space_repo_id=REPO, space_artifact_sha256=artifact, space_dataset_commit=revision,
          space_verified_files=len(manifest), space_pending_snapshot=None)
    result = {'space': f'https://huggingface.co/spaces/{REPO}', 'commit': commit,
              'verified_files': len(manifest), 'public': True, 'reused_commit': not uploaded}
    print(json.dumps(result))
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    publish(parser.parse_args().output)
