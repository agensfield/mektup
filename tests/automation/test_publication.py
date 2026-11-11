"""Exercise publication gates against disposable Git repositories and fake GitHub."""

import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
import auto_codex_release as auto
import release_metadata as metadata
import verify_published_release as verify


class Publication(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / 'repo'
        self.root.mkdir()
        for name in metadata.FILES:
            destination = self.root / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text((ROOT / name).read_text())
        self.previous_cwd = Path.cwd()
        os.chdir(self.root)
        self.addCleanup(os.chdir, self.previous_cwd)
        self.git('init', '-b', 'main')
        self.git('config', 'user.name', 'Publication fixture')
        self.git('config', 'user.email', 'fixture@example.invalid')
        self.git('add', '.')
        self.git('commit', '-m', 'fixture source')
        self.source = self.git('rev-parse', 'HEAD')
        remote = Path(self.tmp.name) / 'origin.git'
        subprocess.run(['git', 'init', '--bare', str(remote)], check=True, capture_output=True)
        self.git('remote', 'add', 'origin', str(remote))
        self.git('push', 'origin', 'main')
        current = metadata.stable(metadata.tested(self.root)[-1])
        self.version = f'{current[0]}.{current[1] + 1}.0'
        self.run = {'conclusion': 'success', 'path': auto.QUALIFIER, 'head_branch': 'main',
                    'head_repository': {'full_name': auto.REPO}, 'event': 'schedule',
                    'head_sha': self.source, 'html_url': 'https://github.com/agensfield/mektup/actions/runs/100'}
        self.receipt = {'schema': 'mektup/codex-qualification/v1', 'status': 'PASS',
                        'qualified': True, 'checksPassed': True, 'sourceDirty': False,
                        'mektupSourceCommit': self.source, 'codexVersion': self.version,
                        'codexAssetSha256': 'sha256:' + 'a' * 64, 'platform': 'linux/x86_64'}
        self.official = {'tag_name': 'rust-v' + self.version, 'draft': False, 'prerelease': False,
                         'assets': [{'name': 'codex-x86_64-unknown-linux-musl.tar.gz',
                                     'digest': self.receipt['codexAssetSha256']}]}
        self.pr = None
        self.calls = []
        self.fail_phase = None
        self.prior, self.release, self.expected = metadata.render(self.root, self.version, 100, self.receipt)

    def git(self, *args):
        return subprocess.check_output(['git', *args], text=True, stderr=subprocess.DEVNULL).strip()

    def api(self, route):
        if route == 'actions/runs/100':
            return self.run
        if route == 'releases/latest':
            return {'tag_name': 'v' + self.prior}
        raise AssertionError(route)

    def gh(self, *args):
        if args[:2] == ('run', 'download'):
            folder = Path(args[args.index('--dir') + 1])
            folder.mkdir()
            (folder / 'receipt.json').write_text(json.dumps(self.receipt))
            return ''
        if args[0] == 'api' and args[1].startswith('repos/openai/codex/'):
            return json.dumps(self.official)
        if args[:2] == ('pr', 'list'):
            branch = args[args.index('--head') + 1]
            return json.dumps([self.pr] if self.pr and self.pr['branch'] == branch else [])
        if args[:2] == ('pr', 'create'):
            self.pr = {'branch': args[args.index('--head') + 1], 'number': 1, 'state': 'OPEN', 'headRefOid': self.git('rev-parse', 'HEAD'),
                       'baseRefOid': self.source, 'headRepositoryOwner': {'login': 'agensfield'},
                       'mergeCommit': None}
            return 'https://github.com/agensfield/mektup/pull/1'
        if args[:2] == ('pr', 'merge'):
            self.assertEqual(args[args.index('--match-head-commit') + 1], self.pr['headRefOid'])
            head = self.pr['headRefOid']
            self.git('checkout', 'main')
            self.git('merge', '--no-ff', head, '-m', 'fixture merge')
            self.git('push', 'origin', 'main')
            self.pr.update(state='MERGED', mergeCommit={'oid': self.git('rev-parse', 'HEAD')})
            return ''
        if args[:2] == ('pr', 'view'):
            return json.dumps(self.pr)
        if args[:2] == ('release', 'edit'):
            self.assertIn('Host installation is outside', Path(args[-1]).read_text())
            return ''
        raise AssertionError(args)

    def workflow(self, name, ref, sha, correlation, ordinary_event, **inputs):
        phase = 'pr' if correlation.endswith('-pr') else 'main' if correlation.endswith('-main') else 'release'
        self.calls.append(phase)
        if phase != 'release' and 'release' not in self.calls[:-1]:
            self.assertEqual(self.git('ls-remote', '--tags', 'origin'), '')
        else:
            for tag in ('v' + self.release, 'go/v' + self.release):
                self.assertEqual(self.git('rev-parse', tag + '^{commit}'), sha)
        if phase == self.fail_phase:
            raise RuntimeError('fixture failed ' + phase)
        return 'https://github.com/agensfield/mektup/actions/runs/200'

    def execute(self):
        with patch.object(auto, 'gh', side_effect=self.gh), patch.object(auto, 'api', side_effect=self.api), \
             patch.object(auto, 'workflow', side_effect=self.workflow):
            auto.main(100)

    def test_complete_publication_and_exact_patch(self):
        self.execute()
        self.assertEqual(self.calls, ['pr', 'main', 'release'])
        for name, content in self.expected.items():
            self.assertEqual((self.root / name).read_text(), content)
        self.assertIn(self.version, metadata.tested(self.root))

    def test_completed_publication_can_resume_without_moving_tags(self):
        self.execute()
        refs = self.git('ls-remote', '--tags', 'origin')
        merged = self.git('rev-parse', 'HEAD')
        self.execute()
        self.assertEqual(self.git('ls-remote', '--tags', 'origin'), refs)
        self.assertEqual(self.git('rev-parse', 'HEAD'), merged)

    def test_new_qualified_source_gets_distinct_pr_after_old_source_failed(self):
        self.fail_phase = 'pr'
        with self.assertRaises(RuntimeError):
            self.execute()
        old_pr = self.pr
        old_pr['state'] = 'CLOSED'
        self.git('checkout', 'main')
        (self.root / 'compiler-version.txt').write_text('patched compiler')
        self.git('add', 'compiler-version.txt')
        self.git('commit', '-m', 'patch compiler')
        self.git('push', 'origin', 'main')
        self.source = self.git('rev-parse', 'HEAD')
        self.run['head_sha'] = self.source
        self.receipt['mektupSourceCommit'] = self.source
        self.fail_phase = None
        self.execute()
        self.assertNotEqual(self.pr['branch'], old_pr['branch'])
        self.assertEqual(old_pr['state'], 'CLOSED')
        self.assertEqual(self.pr['state'], 'MERGED')

    def test_failed_pr_ci_cannot_merge_or_tag(self):
        self.fail_phase = 'pr'
        with self.assertRaisesRegex(RuntimeError, 'failed pr'):
            self.execute()
        self.assertEqual(self.pr['state'], 'OPEN')
        self.assertEqual(self.git('ls-remote', '--tags', 'origin'), '')

    def test_failed_main_ci_cannot_tag(self):
        self.fail_phase = 'main'
        with self.assertRaisesRegex(RuntimeError, 'failed main'):
            self.execute()
        self.assertEqual(self.pr['state'], 'MERGED')
        self.assertEqual(self.git('ls-remote', '--tags', 'origin'), '')

    def test_merged_pr_recovery_reuses_exact_commit(self):
        self.fail_phase = 'main'
        with self.assertRaises(RuntimeError):
            self.execute()
        merged = self.git('rev-parse', 'HEAD')
        self.fail_phase = None
        self.execute()
        self.assertEqual(self.git('rev-parse', 'v' + self.release + '^{commit}'), merged)
        self.assertEqual(self.calls, ['pr', 'main', 'main', 'release'])

    def test_modified_candidate_cannot_be_released(self):
        self.fail_phase = 'pr'
        with self.assertRaises(RuntimeError):
            self.execute()
        (self.root / 'README.md').write_text('unexpected runtime instructions\n')
        self.git('add', 'README.md')
        self.git('commit', '-m', 'foreign patch')
        self.pr['headRefOid'] = self.git('rev-parse', 'HEAD')
        with self.assertRaisesRegex(ValueError, 'exactly one patch'):
            self.execute()
        self.assertEqual(self.git('ls-remote', '--tags', 'origin'), '')

    def test_stale_source_cannot_open_release_pr(self):
        (self.root / 'other').write_text('new source')
        self.git('add', 'other')
        self.git('commit', '-m', 'advance main')
        self.git('push', 'origin', 'main')
        with self.assertRaisesRegex(ValueError, 'main advanced'):
            self.execute()
        self.assertIsNone(self.pr)

    def test_already_tested_version_is_noop(self):
        self.receipt['codexVersion'] = metadata.tested(self.root)[-1]
        self.official['tag_name'] = 'rust-v' + self.receipt['codexVersion']
        self.execute()
        self.assertEqual(self.calls, [])
        self.assertIsNone(self.pr)

    def test_wrong_immutable_tag_is_never_replaced(self):
        self.git('tag', 'v' + self.release, self.source)
        self.git('push', 'origin', 'refs/tags/v' + self.release)
        with self.assertRaisesRegex(ValueError, 'points elsewhere'):
            auto.ensure_tags(self.release, 'b' * 40)
        self.assertEqual(self.git('rev-parse', 'v' + self.release), self.source)
        self.assertNotIn('go/v', self.git('ls-remote', '--tags', 'origin'))

    def test_receipt_trust_boundaries(self):
        auto.validate_receipt(self.run, self.receipt, self.official)
        for key, value in [('sourceDirty', True), ('qualified', False), ('checksPassed', False),
                           ('mektupSourceCommit', 'b' * 40), ('status', 'DEVELOPMENT_PASS')]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                auto.validate_receipt(self.run, dict(self.receipt, **{key: value}), self.official)
        for key, value in [('head_branch', 'foreign'), ('path', 'untrusted.yml'),
                           ('head_repository', {'full_name': 'outsider/fork'}), ('event', 'pull_request')]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                auto.validate_receipt(dict(self.run, **{key: value}), self.receipt, self.official)
        wrong = copy.deepcopy(self.official)
        wrong['assets'][0]['digest'] = 'sha256:' + 'b' * 64
        with self.assertRaises(ValueError):
            auto.validate_receipt(self.run, self.receipt, wrong)

    def test_correlated_dispatch_wins_over_newer_approval_required_pr_run(self):
        runs = [
            {'id': 10, 'head_sha': 'a', 'head_branch': 'automation/codex-x',
             'event': 'workflow_dispatch', 'display_title': 'CI qualification-100-pr',
             'status': 'in_progress', 'conclusion': None},
            {'id': 11, 'head_sha': 'a', 'head_branch': 'automation/codex-x',
             'event': 'pull_request', 'display_title': 'CI automation/codex-x',
             'status': 'completed', 'conclusion': 'action_required'},
        ]
        selected = auto.matching_run(runs, 'a', 'CI qualification-100-pr',
                                     'automation/codex-x', 'pull_request')
        self.assertEqual(selected['id'], 10)

    def test_approval_required_pr_run_does_not_suppress_dispatch(self):
        runs = [{'id': 11, 'head_sha': 'a', 'head_branch': 'automation/codex-x',
                 'event': 'pull_request', 'display_title': 'CI automation/codex-x',
                 'status': 'completed', 'conclusion': 'action_required'}]
        self.assertIsNone(auto.matching_run(runs, 'a', 'CI qualification-100-pr',
                                            'automation/codex-x', 'pull_request'))

    def test_failed_correlated_dispatch_cannot_be_hidden_by_newer_ordinary_success(self):
        runs = [
            {'id': 10, 'head_sha': 'a', 'head_branch': 'main',
             'event': 'workflow_dispatch', 'display_title': 'CI qualification-100-main',
             'status': 'completed', 'conclusion': 'failure'},
            {'id': 11, 'head_sha': 'a', 'head_branch': 'main',
             'event': 'push', 'display_title': 'CI main',
             'status': 'completed', 'conclusion': 'success'},
        ]
        selected = auto.matching_run(runs, 'a', 'CI qualification-100-main', 'main', 'push')
        self.assertEqual(selected['id'], 10)

    def test_exact_run_correlation(self):
        runs = [{'id': 1, 'head_sha': 'a', 'head_branch': 'main', 'event': 'workflow_dispatch',
                 'display_title': 'CI qualification-100-main'},
                {'id': 2, 'head_sha': 'a', 'head_branch': 'other', 'event': 'push', 'display_title': ''}]
        self.assertEqual(auto.matching_run(runs, 'a', 'CI qualification-100-main', 'main', 'push')['id'], 1)
        self.assertIsNone(auto.matching_run(runs, 'b', 'CI qualification-100-main', 'main', 'push'))


class PublishedArtifacts(unittest.TestCase):
    def test_manifest_requires_complete_unique_verified_artifacts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            lines = []
            for system in ('darwin', 'linux'):
                for arch in ('amd64', 'arm64'):
                    name = f'mektup_1.0.19_{system}_{arch}.tar.gz'
                    for entry in (name, name + '.sbom.json'):
                        (root / entry).write_bytes(entry.encode())
                        lines.append(hashlib.sha256(entry.encode()).hexdigest() + '  ' + entry)
            (root / 'checksums.txt').write_text('\n'.join(lines))
            self.assertEqual(len(verify.validate_manifest(root, '1.0.19')), 8)
            (root / 'checksums.txt').write_text('\n'.join(lines + [lines[0]]))
            with self.assertRaises(ValueError):
                verify.validate_manifest(root, '1.0.19')
            (root / 'checksums.txt').write_text('\n'.join(lines[:-1]))
            with self.assertRaises(ValueError):
                verify.validate_manifest(root, '1.0.19')
            (root / 'checksums.txt').write_text('\n'.join(lines))
            (root / lines[0].split()[1]).write_bytes(b'tampered')
            with self.assertRaises(ValueError):
                verify.validate_manifest(root, '1.0.19')


if __name__ == '__main__':
    unittest.main()
