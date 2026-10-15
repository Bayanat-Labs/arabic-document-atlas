"""Offline publisher transactions; all bytes are original engineering fixtures."""
from __future__ import annotations
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
from types import SimpleNamespace as NS
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('snapshot_publisher_under_test',ROOT/'pipeline/publish_snapshot.py')
p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)


def sha(data):return hashlib.sha256(data).hexdigest()
def blob(data):return hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()


class FakeAPI:
    """Commit-pinned file metadata and optimistic parent guards like the real API."""
    def __init__(self,files):
        self.head='1'*40;self.commits={self.head:dict(files)};self.tags={};self.private=True
        self.uploads=[];self.public_calls=[];self.tag_calls=[];self.attempts=0
        self.fail_before=None;self.fail_after=None;self.tag_mode=None;self.bad={};self.setting_mode=None

    def repo_info(self,repo_id,*,repo_type,revision=None,files_metadata=False):
        assert repo_id==p.REPO and repo_type=='dataset'
        commit=self.tags.get(revision,revision) if revision else self.head
        files=self.commits[commit];siblings=[]
        for name,data in sorted(files.items()):
            lfs=NS(sha256=sha(data)) if name.endswith(('.parquet','.pdf')) else None
            item=NS(rfilename=name,size=len(data),lfs=lfs,blob_id=blob(data))
            if name in self.bad:
                kind=self.bad[name]
                if kind=='lfs':item.lfs=NS(sha256='0'*64)
                elif kind=='git':item.blob_id='0'*40;item.lfs=None
                elif kind=='size':item.size+=1
            siblings.append(item)
        return NS(sha=commit,siblings=siblings,private=self.private)

    def list_repo_refs(self,repo_id,*,repo_type):
        assert repo_id==p.REPO and repo_type=='dataset'
        return NS(tags=[NS(name=k,target_commit=v) for k,v in self.tags.items()])

    def upload_folder(self,*,repo_id,repo_type,folder_path,allow_patterns,parent_commit,commit_message):
        assert repo_id==p.REPO and repo_type=='dataset'
        self.attempts+=1
        if self.fail_before==self.attempts:raise TimeoutError('fixture request failed before remote commit')
        if parent_commit!=self.head:raise ValueError('409 optimistic parent mismatch')
        assert isinstance(allow_patterns,list) and len(allow_patterns)<=60
        assert all(not any(c in name for c in '*?[]') for name in allow_patterns)
        files=dict(self.commits[self.head])
        for name in allow_patterns:files[name]=(Path(folder_path)/name).read_bytes()
        commit=hashlib.sha1((self.head+commit_message+str(self.attempts)).encode()).hexdigest()
        self.commits[commit]=files;self.head=commit
        self.uploads.append({'parent':parent_commit,'commit':commit,'paths':list(allow_patterns)})
        if self.fail_after==self.attempts:raise TimeoutError('fixture commit succeeded but response was lost')
        return NS(oid=commit)

    def create_tag(self,repo_id,*,repo_type,tag,revision):
        assert repo_id==p.REPO and repo_type=='dataset';self.tag_calls.append((tag,revision))
        if self.tag_mode=='before':raise TimeoutError('fixture tag request not committed')
        if tag in self.tags and self.tags[tag]!=revision:raise ValueError('immutable tag exists')
        self.tags[tag]=revision
        if self.tag_mode=='after':raise TimeoutError('fixture tag response lost')
        if self.tag_mode=='interrupt_after':raise KeyboardInterrupt('fixture crash after tag creation')

    def update_repo_settings(self,repo_id,*,repo_type,private):
        assert repo_id==p.REPO and repo_type=='dataset' and private is False
        # Visibility can only follow the final metadata/proof upload and tag.
        assert self.head in self.tags.values()
        files=self.commits[self.head]
        assert json.loads(files[p.REPORT])['status']=='passed'
        assert json.loads(files[p.SUMMARY])['remote_community_sha256_verified'] is True
        self.public_calls.append(self.head)
        if self.setting_mode=='before':raise TimeoutError('fixture setting not changed')
        self.private=False
        if self.setting_mode=='after':raise TimeoutError('fixture setting response lost')

    def external_commit(self,name='external.txt',data=b'outside transaction'):
        files=dict(self.commits[self.head]);files[name]=data
        self.head=hashlib.sha1((self.head+'external').encode()).hexdigest();self.commits[self.head]=files


class Publisher(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.output=Path(self.tmp.name);self.release=self.output/'release';self.release.mkdir()
        self.primary={'data/pages/evicted.parquet':b'Original engineering primary shard fixture; not corpus data'}
        self.api=FakeAPI(self.primary)
        self.state=self.output/'private/publication.json';self.state.parent.mkdir()
        self.state.write_text(json.dumps({'repo_id':p.REPO,'dataset_created':True,'private':True}))
        summary={'version':'v0.1.0','page_data_commit_before_documentation':self.api.head,'unique_image_pages':1,
                 'community_text_records':1,'remote_page_sha256_verified':True}
        files={p.SUMMARY:json.dumps(summary).encode(),
               'metadata/shards.json':json.dumps({'version':'v0.1.0','shards':[{'path':name,'sha256':sha(data),'bytes':len(data)} for name,data in self.primary.items()]}).encode(),
               p.REFERENCE_SUMMARY:json.dumps({'rows':1,'validation_passed':True,'upload_sha256_verified':False}).encode(),
               'data/community_text/fixture.parquet':b'Original engineering community fixture',
               'data/reference_pairs/fixture.parquet':b'Original engineering image/reference fixture',
               'source/reference_pdfs/fixture.pdf':b'%PDF-1.4 original engineering rights-proof fixture',
               'README.md':b'# Original engineering fixture only', '.gitattributes':b'*.parquet filter=lfs diff=lfs merge=lfs -text\n'}
        files.update({f'metadata/fixture-{i:03}.json':json.dumps({'fixture':i}).encode() for i in range(125)})
        for name,data in files.items():
            f=self.release/name;f.parent.mkdir(parents=True,exist_ok=True);f.write_bytes(data)
        self.rechecksum()
        self.api_patch=patch.object(p,'HfApi',lambda:self.api);self.api_patch.start()

    def tearDown(self):self.api_patch.stop();self.tmp.cleanup()
    def rechecksum(self):
        files={**self.primary,**{str(f.relative_to(self.release)):f.read_bytes() for f in self.release.rglob('*') if f.is_file() and f.name!='SHA256SUMS'}}
        (self.release/'SHA256SUMS').write_text(''.join(f'{sha(data)}  {name}\n' for name,data in sorted(files.items())))
    def publish(self,public=False):return p.publish(self.output,'v0.1.0',public)
    def next_package(self,version='v0.2.0'):
        """Model the operator's archived old release and a fresh quiet package."""
        old_version=json.loads((self.release/p.SUMMARY).read_text())['version']
        archived=self.output/'snapshots'/old_version;archived.parent.mkdir(exist_ok=True)
        self.release.replace(archived);shutil.copytree(archived,self.release)
        # Model an additional builder-owned, verified shard before packaging.
        name='data/pages/evicted-next.parquet';data=b'Original next-cohort primary engineering fixture'
        self.api.external_commit(name,data);self.primary={**self.primary,name:data}
        # A fresh release.package has no generated remote verification report.
        (self.release/p.REPORT).unlink()
        summary=json.loads((self.release/p.SUMMARY).read_text())
        summary.update(version=version,page_data_commit_before_documentation=self.api.head,
                       unique_image_pages=2,remote_community_sha256_verified=False,remote_reference_sha256_verified=False)
        (self.release/p.SUMMARY).write_text(json.dumps(summary))
        (self.release/'metadata/shards.json').write_text(json.dumps({'version':version,'shards':[
            {'path':name,'sha256':sha(data),'bytes':len(data)} for name,data in self.primary.items()]}))
        (self.release/'README.md').write_text('# Original next-version engineering fixture')
        self.rechecksum();return archived

    def test_batched_protected_publish_verifies_all_components_before_public(self):
        result=self.publish(public=True)
        self.assertEqual(result['commit'],self.api.head);self.assertFalse(self.api.private)
        self.assertEqual(self.api.tags['v0.1.0'],self.api.head)
        self.assertGreaterEqual(len(self.api.uploads),4)
        parent='1'*40
        for call in self.api.uploads:
            self.assertEqual(call['parent'],parent);parent=call['commit'];self.assertLessEqual(len(call['paths']),60)
        summary=json.loads((self.release/p.SUMMARY).read_text())
        self.assertTrue(summary['remote_community_sha256_verified']);self.assertTrue(summary['remote_reference_sha256_verified'])
        self.assertTrue(summary['remote_reference_pdf_sha256_verified'])
        self.assertTrue(json.loads((self.release/p.REFERENCE_SUMMARY).read_text())['upload_sha256_verified'])
        report=json.loads((self.release/p.REPORT).read_text())
        self.assertEqual(report['data_commit'],self.api.uploads[-2]['commit'])
        self.assertEqual(report['counts']['reference_pdf_files_verified'],1)
        self.assertNotEqual(report['data_commit'],result['commit'])
        sums=(self.release/'SHA256SUMS').read_text();self.assertIn(p.REPORT,sums);self.assertIn('data/pages/evicted.parquet',sums)

    def test_owned_tag_reuses_exact_commit_without_upload(self):
        first=self.publish();calls=len(self.api.uploads)
        second=self.publish(public=True);self.assertEqual(first['commit'],second['commit']);self.assertEqual(len(self.api.uploads),calls)
        self.publish(public=True);self.assertEqual(len(self.api.public_calls),1)

    def test_changed_local_owned_tag_cannot_be_reused(self):
        self.publish();calls=len(self.api.uploads)
        (self.release/'README.md').write_text('Locally changed fixture');self.rechecksum()
        with self.assertRaisesRegex(ValueError,'Local package differs'):self.publish(public=True)
        self.assertEqual(len(self.api.uploads),calls);self.assertTrue(self.api.private)

    def test_failure_before_later_batch_resumes_only_remaining_paths(self):
        self.api.fail_before=2
        with self.assertRaises(TimeoutError):self.publish()
        initial_paths=set(self.api.uploads[0]['paths']);self.api.fail_before=None
        self.publish()
        subsequent_initial=[name for call in self.api.uploads[1:-1] for name in call['paths']]
        self.assertFalse(initial_paths.intersection(subsequent_initial))

    def test_lost_upload_outcome_recovers_only_exact_full_intended_state(self):
        self.api.fail_after=1
        with self.assertRaises(TimeoutError):self.publish()
        first=set(self.api.uploads[0]['paths']);self.api.fail_after=None
        self.publish(public=True)
        self.assertFalse(first.intersection(n for call in self.api.uploads[1:-1] for n in call['paths']))
        self.assertFalse(self.api.private)

    def test_lost_upload_outcome_with_extra_external_change_is_refused(self):
        self.api.fail_after=1
        with self.assertRaises(TimeoutError):self.publish()
        self.api.external_commit();calls=len(self.api.uploads);self.api.fail_after=None
        with self.assertRaisesRegex(ValueError,'exact full intended'):self.publish(public=True)
        self.assertEqual(len(self.api.uploads),calls);self.assertFalse(self.api.tags);self.assertTrue(self.api.private)

    def test_lost_final_metadata_response_recovers_without_second_commit(self):
        initial_files=sum(f.is_file() for f in self.release.rglob('*'))
        self.api.fail_after=(initial_files+p.BATCH_SIZE-1)//p.BATCH_SIZE+1
        with self.assertRaises(TimeoutError):self.publish()
        self.assertEqual(json.loads(self.state.read_text())['pending_snapshot']['phase'],'uploading_final')
        calls=len(self.api.uploads);self.api.fail_after=None
        self.publish(public=True)
        self.assertEqual(len(self.api.uploads),calls);self.assertFalse(self.api.private)

    def test_lost_response_cannot_adopt_changed_prior_file_with_same_names(self):
        self.api.fail_after=1
        with self.assertRaises(TimeoutError):self.publish()
        self.api.external_commit('data/pages/evicted.parquet',b'Changed evicted primary engineering fixture')
        self.api.fail_after=None
        with self.assertRaisesRegex(ValueError,'changed a prior remote file'):self.publish(public=True)
        self.assertFalse(self.api.tags);self.assertTrue(self.api.private)

    def test_external_head_and_local_resume_mutations_are_refused(self):
        self.api.fail_before=2
        with self.assertRaises(TimeoutError):self.publish()
        original=(self.release/'README.md').read_bytes()
        (self.release/'README.md').write_text('Changed after interrupted batch');self.rechecksum()
        with self.assertRaisesRegex(ValueError,'Frozen local package changed'):self.publish()
        (self.release/'README.md').write_bytes(original);self.rechecksum()
        self.api.external_commit()
        with self.assertRaisesRegex(ValueError,'exact full intended'):self.publish()

    def test_corrupt_lfs_git_or_size_never_tags_or_publishes(self):
        for path,kind in [('data/community_text/fixture.parquet','lfs'),('README.md','git'),('source/reference_pdfs/fixture.pdf','size')]:
            with self.subTest(path=path),tempfile.TemporaryDirectory() as tmp:
                # Each case starts from a fresh independent source package/state.
                original=self.output;release=self.release;api=self.api
                import shutil
                new=Path(tmp);shutil.copytree(original/'release',new/'release');shutil.copytree(original/'private',new/'private')
                self.output=new;self.release=new/'release';self.api=FakeAPI(self.primary);self.api.bad[path]=kind
                try:
                    with self.assertRaises(ValueError):self.publish(public=True)
                    self.assertFalse(self.api.tags);self.assertFalse(self.api.public_calls);self.assertTrue(self.api.private)
                finally:self.output=original;self.release=release;self.api=api

    def test_extra_data_or_reference_pdf_paths_fail_exact_set_gate(self):
        for path in ['data/stale.parquet','source/reference_pdfs/stale.pdf']:
            # Both paths are present at the declared initial commit, so the gate
            # detects stale content rather than only a changed HEAD.
            self.api.commits[self.api.head][path]=b'Original stale engineering fixture'
        with self.assertRaisesRegex(ValueError,'exact data/reference-PDF'):self.publish(public=True)
        self.assertFalse(self.api.tags);self.assertFalse(self.api.public_calls)

    def test_tag_response_timeout_is_reconciled_to_exact_target(self):
        self.api.tag_mode='after';result=self.publish(public=True)
        self.assertEqual(self.api.tags['v0.1.0'],result['commit']);self.assertFalse(self.api.private)

    def test_tag_request_timeout_resumes_without_reupload(self):
        self.api.tag_mode='before'
        with self.assertRaises(TimeoutError):self.publish()
        state=json.loads(self.state.read_text());self.assertEqual(state['pending_snapshot']['phase'],'tag_pending')
        calls=len(self.api.uploads);self.api.tag_mode=None
        self.publish(public=True);self.assertEqual(len(self.api.uploads),calls)

    def test_existing_pending_owned_tag_is_adopted_after_process_crash(self):
        self.api.tag_mode='interrupt_after'
        with self.assertRaises(KeyboardInterrupt):self.publish()
        state=json.loads(self.state.read_text());self.assertNotIn('dataset_version',state)
        calls=len(self.api.uploads);self.api.tag_mode=None
        result=self.publish(public=True)
        self.assertEqual(result['commit'],self.api.tags['v0.1.0']);self.assertEqual(len(self.api.uploads),calls)
        self.assertEqual(json.loads(self.state.read_text())['dataset_version'],'v0.1.0')

    def test_foreign_tag_and_unowned_repository_are_refused(self):
        self.api.tags['v0.1.0']=self.api.head
        with self.assertRaisesRegex(ValueError,'outside owned'):self.publish(public=True)
        self.assertFalse(self.api.uploads)
        self.api.tags.clear();state=json.loads(self.state.read_text());state['repo_id']='Someone/else';self.state.write_text(json.dumps(state))
        with self.assertRaisesRegex(ValueError,'Builder-owned'):self.publish()
        self.assertFalse(self.api.uploads)

    def test_partial_final_metadata_materialization_resumes_safely(self):
        atomic=p._atomic;interrupted=False
        def stop(path,data):
            nonlocal interrupted
            atomic(path,data)
            if Path(path).resolve()==(self.release/p.REPORT).resolve() and not interrupted:
                interrupted=True;raise KeyboardInterrupt('fixture crash partway through generated metadata')
        with patch.object(p,'_atomic',stop),self.assertRaises(KeyboardInterrupt):self.publish()
        self.assertEqual(json.loads(self.state.read_text())['pending_snapshot']['phase'],'materialize_final')
        calls=len(self.api.uploads);self.publish(public=True)
        self.assertEqual(len(self.api.uploads),calls+1)

    def test_final_metadata_corruption_prevents_tag_and_public(self):
        self.api.bad[p.REPORT]='git'
        with self.assertRaisesRegex(ValueError,'Remote Git blob'):self.publish(public=True)
        self.assertFalse(self.api.tags);self.assertFalse(self.api.public_calls)

    def test_public_setting_timeout_after_actual_change_is_observed(self):
        self.api.setting_mode='after';result=self.publish(public=True)
        self.assertTrue(result['public']);self.assertEqual(len(self.api.public_calls),1)

    def test_only_one_local_publisher_can_hold_state(self):
        with p._publication_lock(self.output/'private/publish-snapshot.lock'):
            with self.assertRaisesRegex(ValueError,'Another snapshot publisher'):self.publish(public=True)
        self.assertFalse(self.api.uploads)

    def test_second_version_preserves_first_tag_and_completed_proof_and_is_retryable(self):
        first=self.publish(public=True);first_files=copy.deepcopy(self.api.commits[first['commit']])
        completed=json.loads(self.state.read_text())['completed_snapshot'];archived=self.next_package()
        self.assertTrue((archived/p.REPORT).is_file())
        second=p.publish(self.output,'v0.2.0',True)
        self.assertNotEqual(second['commit'],first['commit'])
        self.assertEqual(self.api.tags,{'v0.1.0':first['commit'],'v0.2.0':second['commit']})
        self.assertEqual(self.api.commits[first['commit']],first_files)
        state=json.loads(self.state.read_text())
        self.assertEqual(state['completed_snapshot_history'],[completed])
        self.assertEqual(state['dataset_version'],'v0.2.0');self.assertEqual(state['pending_snapshot']['phase'],'complete')
        calls=len(self.api.uploads);p.publish(self.output,'v0.2.0',True)
        self.assertEqual(len(self.api.uploads),calls)
        self.assertEqual(json.loads(self.state.read_text())['completed_snapshot_history'],[completed])

    def test_interrupted_second_version_resumes_without_retiring_history_twice(self):
        first=self.publish();completed=json.loads(self.state.read_text())['completed_snapshot'];self.next_package()
        self.api.fail_before=self.api.attempts+2
        with self.assertRaises(TimeoutError):p.publish(self.output,'v0.2.0',False)
        state=json.loads(self.state.read_text());self.assertEqual(state['completed_snapshot_history'],[completed])
        self.assertEqual(state['pending_snapshot']['version'],'v0.2.0')
        self.assertEqual(set(state['pending_snapshot']['previous_snapshot_files']),{p.REPORT})
        self.api.fail_before=None;p.publish(self.output,'v0.2.0',False)
        self.assertEqual(self.api.tags['v0.1.0'],first['commit'])
        self.assertEqual(json.loads(self.state.read_text())['completed_snapshot_history'],[completed])

    def test_second_version_refuses_unfinished_mismatched_or_foreign_prior_proof(self):
        self.publish();self.next_package();baseline=json.loads(self.state.read_text());tags=copy.deepcopy(self.api.tags)
        calls=len(self.api.uploads)
        for case in ('unfinished','commit','manifest','digest','completed','tag_missing','tag_changed','history'):
            with self.subTest(case=case):
                state=copy.deepcopy(baseline);self.api.tags=copy.deepcopy(tags)
                if case=='unfinished':state['pending_snapshot']['phase']='tag_pending'
                if case=='commit':state['dataset_commit']='0'*40
                if case=='manifest':state['pending_snapshot']['package_manifest']['README.md']['bytes']+=1
                if case=='digest':state['pending_snapshot']['package_manifest_sha256']='0'*64
                if case=='completed':state['completed_snapshot']['version']='v9.9.9'
                if case=='tag_missing':self.api.tags.pop('v0.1.0')
                if case=='tag_changed':self.api.tags['v0.1.0']='1'*40
                if case=='history':state['completed_snapshot_history']=[{**state['completed_snapshot'],'commit':'0'*40}]
                self.state.write_text(json.dumps(state));before=self.state.read_bytes()
                with self.assertRaisesRegex(ValueError,'unfinished|completed|Previous|history'):
                    p.publish(self.output,'v0.2.0',True)
                self.assertEqual(self.state.read_bytes(),before);self.assertEqual(len(self.api.uploads),calls)
                self.assertNotIn('v0.2.0',self.api.tags)

    def test_second_version_requires_new_initial_head_and_refuses_foreign_new_tag(self):
        self.publish();self.next_package();state=self.state.read_bytes();calls=len(self.api.uploads)
        self.api.tags['v0.2.0']=self.api.head
        with self.assertRaisesRegex(ValueError,'outside owned'):p.publish(self.output,'v0.2.0',True)
        self.assertEqual(self.state.read_bytes(),state);self.assertEqual(len(self.api.uploads),calls)
        self.api.tags.pop('v0.2.0');self.api.external_commit('README.md',b'Concurrent outside engineering edit')
        with self.assertRaisesRegex(ValueError,'HEAD changed'):p.publish(self.output,'v0.2.0',True)
        self.assertEqual(self.state.read_bytes(),state);self.assertEqual(len(self.api.uploads),calls)

    def test_second_version_does_not_allow_changed_prior_verification_report(self):
        self.publish();self.next_package()
        # This HEAD is explicitly bound by the new package, but the temporary
        # old report must still match its completed version's exact hash.
        self.api.external_commit(p.REPORT,b'Changed previous verification evidence')
        summary=json.loads((self.release/p.SUMMARY).read_text());summary['page_data_commit_before_documentation']=self.api.head
        (self.release/p.SUMMARY).write_text(json.dumps(summary));self.rechecksum()
        before=self.state.read_bytes();calls=len(self.api.uploads)
        with self.assertRaisesRegex(ValueError,'Remote.*mismatch'):p.publish(self.output,'v0.2.0',True)
        self.assertNotIn('v0.2.0',self.api.tags);self.assertFalse(self.api.public_calls)
        self.assertEqual(self.state.read_bytes(),before);self.assertEqual(len(self.api.uploads),calls)


if __name__=='__main__':unittest.main()
