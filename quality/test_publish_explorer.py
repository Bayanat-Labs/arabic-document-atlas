"""Static Space publication contracts; original fixtures and a faithful fake Hub.

No network calls, corpus records, uploads or public-visibility changes occur.
"""
from __future__ import annotations
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace as NS
import unittest
from unittest.mock import patch

MODULE = Path(__file__).resolve().parents[1] / 'pipeline/publish_explorer.py'
spec = importlib.util.spec_from_file_location('atlas_publish_explorer_tested', MODULE)
viewer = importlib.util.module_from_spec(spec); spec.loader.exec_module(viewer)
DATASET_COMMIT = 'd' * 40
INITIAL_COMMIT = 'a' * 40


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def git_blob(data):
    return hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()


class FakeAPI:
    """Matches Hub keyword-only upload semantics, immutable commits and git refs."""
    def __init__(self):
        self.space_exists = False
        self.dataset_private = False
        self.dataset_sha = DATASET_COMMIT
        self.space_private = True
        self.head = INITIAL_COMMIT
        self.commits = {self.head: {'.gitattributes': b'Hub initial generated attributes\n', 'README.md': b'Initial Space'}}
        self.tags = {}
        self.calls = []
        self.uploads = []
        self.after_upload = None
        self.after_tag = None
        self.on_visibility = None
        self.upload_unknown_once = False
        self.tag_unknown_once = False

    def repo_info(self, repo_id, *, repo_type=None, revision=None, files_metadata=False):
        assert repo_id == viewer.REPO
        self.calls.append(('info', repo_type, revision, files_metadata))
        if repo_type == 'dataset':
            assert revision == DATASET_COMMIT
            return NS(sha=self.dataset_sha, private=self.dataset_private)
        assert repo_type == 'space' and self.space_exists
        commit = revision or self.head
        files = self.commits[commit]
        siblings = []
        for name, data in sorted(files.items()):
            # Real Hub may store some files in LFS and others in ordinary git.
            siblings.append(NS(rfilename=name, size=len(data),
                lfs=NS(sha256=hashlib.sha256(data).hexdigest()) if name.endswith('.jpg') else None,
                blob_id=git_blob(data)))
        return NS(sha=commit, private=self.space_private, siblings=siblings)

    def repo_exists(self, repo_id, *, repo_type=None):
        assert repo_id == viewer.REPO and repo_type == 'space'
        self.calls.append(('exists',)); return self.space_exists

    def create_repo(self, repo_id, *, repo_type=None, space_sdk=None, private=None):
        assert repo_id == viewer.REPO and repo_type == 'space' and space_sdk == 'static' and private is True
        assert not self.space_exists
        self.calls.append(('create',)); self.space_exists = True
        return NS(repo_id=repo_id)

    def list_repo_refs(self, repo_id, *, repo_type=None):
        assert repo_id == viewer.REPO and repo_type == 'space' and self.space_exists
        self.calls.append(('refs',))
        return NS(tags=[NS(name=name, target_commit=commit) for name, commit in self.tags.items()])

    def upload_folder(self, *, repo_id, repo_type=None, folder_path=None, parent_commit=None,
                      allow_patterns=None, delete_patterns=None, commit_message=None):
        assert repo_id == viewer.REPO and repo_type == 'space'
        if parent_commit != self.head:
            raise ValueError('Hub rejected stale parent_commit')
        assert delete_patterns == ['*']
        names = sorted(p.relative_to(folder_path).as_posix() for p in Path(folder_path).rglob('*') if p.is_file())
        assert allow_patterns == names
        files = {'.gitattributes': self.commits[self.head]['.gitattributes']}
        files.update({name: (Path(folder_path) / name).read_bytes() for name in allow_patterns})
        commit = f'{len(self.uploads) + 1:040x}'
        self.uploads.append({'parent_commit': parent_commit, 'allow_patterns': allow_patterns, 'commit': commit})
        self.calls.append(('upload', commit)); self.commits[commit] = files; self.head = commit
        if self.after_upload: self.after_upload(self)
        if self.upload_unknown_once:
            self.upload_unknown_once = False
            raise TimeoutError('Upload committed, response lost')
        return NS(oid=commit)

    def create_tag(self, repo_id, *, repo_type=None, tag=None, revision=None):
        assert repo_id == viewer.REPO and repo_type == 'space' and revision in self.commits
        self.calls.append(('tag', tag, revision))
        if tag in self.tags: raise ValueError('Tag already exists')
        self.tags[tag] = revision
        if self.after_tag: self.after_tag(self)
        if self.tag_unknown_once:
            self.tag_unknown_once = False
            raise TimeoutError('Tag committed, response lost')

    def update_repo_settings(self, repo_id, *, repo_type=None, private=None):
        assert repo_id == viewer.REPO and repo_type == 'space' and private is False
        self.calls.append(('visibility', private))
        if self.on_visibility: self.on_visibility(self)
        self.space_private = private


class ExplorerPublication(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.output = self.root / 'build'
        self.statepath = self.output / 'private/publication.json'
        self.directory = self.output / 'explorer'
        self.directory.mkdir(parents=True)
        self.state = {'repo_id': viewer.REPO, 'dataset_created': True, 'dataset_public': True,
                      'dataset_commit': DATASET_COMMIT, 'dataset_version': 'v0.1.0', 'builder_field': 'preserve me'}
        write_json(self.statepath, self.state)
        self.image_id = 'f' * 64
        self.image_name = f'images/{self.image_id}.jpg'
        image = self.directory / self.image_name; image.parent.mkdir(); image.write_bytes(b'Original fake JPEG fixture, never published')
        for name in viewer.ROOT_FILES - {'.gitattributes', 'samples.json'}:
            (self.directory / name).write_text('Original fixture: ' + name + '\n')
        write_json(self.directory / 'samples.json', {'dataset_revision': DATASET_COMMIT,
            'summary': {'version': 'v0.1.0'}, 'pages': [{'page_id': self.image_id, 'sample_image': self.image_name,
            'image_sha256': hashlib.sha256(image.read_bytes()).hexdigest()}]})
        self.api = FakeAPI()
        self.mock = patch.object(viewer, 'HfApi', return_value=self.api); self.mocked_api = self.mock.start()
        self.stdout = patch('builtins.print'); self.stdout.start()

    def tearDown(self):
        self.stdout.stop(); self.mock.stop(); self.tmp.cleanup()

    def publish(self): return viewer.publish(self.output)
    def read_state(self): return json.loads(self.statepath.read_text())
    def change_state(self, **changes):
        state = self.read_state(); state.update(changes); write_json(self.statepath, state)
    def mutations(self): return [x for x in self.api.calls if x[0] in ('create', 'upload', 'tag', 'visibility')]

    def test_fresh_publication_verifies_git_lfs_complete_set_before_tag(self):
        result = self.publish(); state = self.read_state()
        self.assertTrue(result['public']); self.assertFalse(result['reused_commit'])
        self.assertEqual(state['space_commit'], self.api.head)
        self.assertEqual(state['space_dataset_commit'], DATASET_COMMIT)
        self.assertEqual(state['space_repo_id'], viewer.REPO)
        self.assertEqual(state['space_verified_files'], 10)
        self.assertIsNone(state['space_pending_snapshot'])
        self.assertEqual(state['builder_field'], 'preserve me')
        self.assertEqual(self.api.tags, {'v0.1.0': self.api.head})
        self.assertEqual((self.directory / '.gitattributes').read_bytes(), viewer.ATTRIBUTES)
        self.assertEqual(set(self.api.commits[self.api.head]), set(self.api.uploads[0]['allow_patterns']))
        tag_index = next(i for i, call in enumerate(self.api.calls) if call[0] == 'tag')
        self.assertTrue(any(call == ('info', 'space', self.api.head, True) for call in self.api.calls[:tag_index]))
        self.assertLess(tag_index, next(i for i, call in enumerate(self.api.calls) if call[0] == 'visibility'))

    def test_repeated_owned_version_reuses_commit_without_upload_or_tag(self):
        first = self.publish(); self.api.calls.clear()
        second = self.publish()
        self.assertEqual(first['commit'], second['commit'])
        self.assertTrue(second['reused_commit'])
        self.assertEqual(self.mutations(), [])
        self.assertEqual(len(self.api.uploads), 1)

    def test_changed_local_artifact_rejected_before_any_new_commit(self):
        self.publish(); self.api.calls.clear(); (self.directory / 'app.js').write_text('Different local artifact')
        with self.assertRaisesRegex(ValueError, 'different local static artifact'):
            self.publish()
        self.assertEqual(self.mutations(), [])

    def test_existing_unowned_tag_rejected_before_upload(self):
        self.api.space_exists = True
        self.change_state(space_created=True, space_repo_id=viewer.REPO)
        self.api.tags['v0.1.0'] = INITIAL_COMMIT
        with self.assertRaisesRegex(ValueError, 'outside this completed/pending'):
            self.publish()
        self.assertEqual(self.mutations(), [])

    def test_existing_space_outside_state_never_adopted(self):
        self.api.space_exists = True
        with self.assertRaisesRegex(ValueError, 'outside this publication state'):
            self.publish()
        self.assertEqual(self.mutations(), [])

    def test_ownership_and_dataset_pin_fail_before_mutations(self):
        cases = [{'repo_id': 'Somebody/else'}, {'dataset_created': False}, {'dataset_public': False},
                 {'dataset_commit': 'main'}, {'space_repo_id': 'Somebody/else'}]
        for changes in cases:
            with self.subTest(changes=changes):
                write_json(self.statepath, {**self.state, **changes})
                with self.assertRaises(ValueError): self.publish()
                self.assertEqual(self.mutations(), [])
        write_json(self.statepath, self.state)
        self.api.dataset_private = True
        with self.assertRaisesRegex(ValueError, 'exact pinned dataset'):
            self.publish()
        self.assertEqual(self.mutations(), [])
        self.api.dataset_private = False; self.api.dataset_sha = 'e' * 40
        with self.assertRaisesRegex(ValueError, 'exact pinned dataset'):
            self.publish()
        self.assertEqual(self.mutations(), [])

    def test_unknown_upload_response_recovers_exact_head_without_reupload(self):
        self.api.upload_unknown_once = True
        with self.assertRaises(TimeoutError): self.publish()
        self.assertIsNone(self.read_state()['space_pending_snapshot']['commit'])
        self.assertNotIn('v0.1.0', self.api.tags)
        result = self.publish()
        self.assertTrue(result['public']); self.assertEqual(len(self.api.uploads), 1)
        self.assertIsNone(self.read_state()['space_pending_snapshot'])

    def test_visibility_failure_reuses_verified_tag_on_retry(self):
        def fail(_): raise RuntimeError('Visibility service temporarily failed')
        self.api.on_visibility = fail
        with self.assertRaises(RuntimeError): self.publish()
        self.assertEqual(self.api.tags['v0.1.0'], self.read_state()['space_pending_snapshot']['commit'])
        self.api.on_visibility = None; self.api.calls.clear()
        self.publish()
        self.assertEqual(len(self.api.uploads), 1)
        self.assertFalse(any(call[0] in ('upload', 'tag') for call in self.api.calls))

    def test_unknown_tag_response_recovers_only_its_verified_commit(self):
        self.api.tag_unknown_once = True
        result = self.publish()
        self.assertTrue(result['public']); self.assertEqual(len(self.api.uploads), 1)

    def test_extra_missing_same_size_corruption_and_wrong_size_never_tagged(self):
        def extra(api): api.commits[api.head]['private/token.txt'] = b'Unwanted fixture'
        def missing(api): del api.commits[api.head]['style.css']
        def git_corrupt(api):
            old = api.commits[api.head]['app.js']; api.commits[api.head]['app.js'] = b'X' * len(old)
        def lfs_corrupt(api):
            old = api.commits[api.head][self.image_name]; api.commits[api.head][self.image_name] = b'X' * len(old)
        def wrong_size(api): api.commits[api.head]['README.md'] += b'x'
        for corrupt in (extra, missing, git_corrupt, lfs_corrupt, wrong_size):
            with self.subTest(corrupt=corrupt.__name__):
                write_json(self.statepath, self.state)
                self.api = FakeAPI(); self.mocked_api.return_value = self.api; self.api.after_upload = corrupt
                with self.assertRaises(ValueError): self.publish()
                self.assertEqual(self.api.tags, {})
                self.assertFalse(any(call[0] == 'visibility' for call in self.api.calls))
                self.assertIsNotNone(self.read_state()['space_pending_snapshot']['commit'])

    def test_tag_race_and_changed_head_never_publish_or_overwrite(self):
        def external_tag(api): api.tags['v0.1.0'] = INITIAL_COMMIT
        self.api.after_upload = external_tag
        with self.assertRaises(ValueError): self.publish()
        self.assertEqual(self.api.tags['v0.1.0'], INITIAL_COMMIT)
        self.assertFalse(any(call[0] == 'visibility' for call in self.api.calls))
        self.api.calls.clear()
        with self.assertRaisesRegex(ValueError, 'outside this completed/pending'):
            self.publish()
        self.assertEqual(self.mutations(), [])
        write_json(self.statepath, self.state); self.api = FakeAPI(); self.mocked_api.return_value = self.api
        self.api.after_tag = lambda api: setattr(api, 'head', INITIAL_COMMIT)
        with self.assertRaisesRegex(ValueError, 'HEAD changed'):
            self.publish()
        self.assertFalse(any(call[0] == 'visibility' for call in self.api.calls))

    def test_local_unexpected_files_symlink_and_bad_samples_rejected(self):
        secret = self.directory / '.env'; secret.write_text('Fake secret fixture')
        with self.assertRaisesRegex(ValueError, 'Unexpected static viewer file'): self.publish()
        secret.unlink(); (self.directory / 'linked-dir').symlink_to(self.root, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'symlink'): self.publish()
        (self.directory / 'linked-dir').unlink()
        samples = json.loads((self.directory / 'samples.json').read_text()); samples['pages'][0]['image_sha256'] = '0' * 64
        write_json(self.directory / 'samples.json', samples)
        with self.assertRaisesRegex(ValueError, 'image identity/hash'): self.publish()
        self.assertEqual(self.mutations(), [])

    def test_dataset_state_change_during_upload_is_not_clobbered_or_tagged(self):
        self.api.after_upload = lambda api: self.change_state(dataset_commit='e' * 40, independently_added='preserve')
        with self.assertRaisesRegex(ValueError, 'Dataset publication state changed'): self.publish()
        self.assertEqual(self.read_state()['dataset_commit'], 'e' * 40)
        self.assertEqual(self.read_state()['independently_added'], 'preserve')
        self.assertEqual(self.api.tags, {})

    def test_existing_tag_is_reverified_in_full_on_retry(self):
        self.publish(); self.api.commits[self.api.head]['unexpected.js'] = b'Unknown remote code'; self.api.calls.clear()
        with self.assertRaisesRegex(ValueError, 'file set differs'): self.publish()
        self.assertEqual(self.mutations(), [])

    def test_local_files_changing_during_upload_never_get_tagged(self):
        self.api.after_upload = lambda api: (self.directory / 'app.js').write_text('Changed during upload')
        with self.assertRaisesRegex(ValueError, 'Local viewer artifact changed'):
            self.publish()
        self.assertEqual(self.api.tags, {})
        self.assertFalse(any(call[0] == 'visibility' for call in self.api.calls))

    def test_new_version_preserves_previous_tag_and_uses_owned_head_parent(self):
        first = self.publish()
        self.change_state(dataset_version='v0.2.0')
        samples = json.loads((self.directory / 'samples.json').read_text())
        samples['summary']['version'] = 'v0.2.0'; write_json(self.directory / 'samples.json', samples)
        second = self.publish()
        self.assertNotEqual(first['commit'], second['commit'])
        self.assertEqual(self.api.uploads[-1]['parent_commit'], first['commit'])
        self.assertEqual(self.api.tags, {'v0.1.0': first['commit'], 'v0.2.0': second['commit']})
        self.assertEqual(self.read_state()['space_version'], 'v0.2.0')

    def test_missing_owned_space_and_symlink_state_fail_without_mutations(self):
        self.change_state(space_created=True, space_repo_id=viewer.REPO)
        with self.assertRaisesRegex(ValueError, 'no longer exists'):
            self.publish()
        target = self.root / 'external-state.json'; target.write_bytes(self.statepath.read_bytes())
        self.statepath.unlink(); self.statepath.symlink_to(target)
        with self.assertRaisesRegex(ValueError, 'owned regular file'):
            self.publish()
        self.assertEqual(self.mutations(), [])


if __name__ == '__main__': unittest.main()
