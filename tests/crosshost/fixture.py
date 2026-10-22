#!/usr/bin/env python3
"""Opt-in two-host black-box acceptance. Python stdlib; never uses user daemons.

Workers own a fake provider, Codex daemon, private configuration, identities and
journal. Fixed-command SSH wrappers select these fixtures on the other host.
The encrypted SSH hops are real; ambient production endpoint selection is not
part of this acceptance test. No request data controls an executable or root.
"""
import argparse
import base64
import hashlib
import http.server
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time


def run(argv, **kwargs):
    return subprocess.run(argv, check=True, text=True, capture_output=True, **kwargs).stdout


def write_private(path, value):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_text(value)
    path.chmod(0o600)


def events(result):
    if result['code']:
        raise RuntimeError(json.dumps(result, ensure_ascii=False))
    return [json.loads(line) for line in result['stdout'].splitlines() if line.strip()]


def find_key(value, key):
    if isinstance(value, dict):
        if key in value:
            return value[key]
        for child in value.values():
            found = find_key(child, key)
            if found is not None:
                return found
    if isinstance(value, list):
        for child in value:
            found = find_key(child, key)
            if found is not None:
                return found
    return None


def native_messages(value):
    if isinstance(value, dict):
        if value.get('type') == 'userMessage':
            yield value
        else:
            for child in value.values():
                yield from native_messages(child)
    elif isinstance(value, list):
        for child in value:
            yield from native_messages(child)


def exact_native(host, uri, message, body):
    deadline = time.monotonic() + 10
    while True:
        history = host.cli('thread', 'items', uri, '--limit', '100')
        matches = []
        for item in native_messages(history):
            text = ''.join(part.get('text', '') for part in item.get('content', []))
            if f'message-id: "{message}"\n' in text:
                matches.append((item, text))
        if matches:
            assert len(matches) == 1, matches
            item, text = matches[0]
            assert item.get('clientMessageId', item.get('clientId')) == message, item
            header, actual = text.split('---\n', 1)
            assert actual == body, (actual, body)
            assert f'payload-sha256: "sha256:{hashlib.sha256(body.encode()).hexdigest()}"' in header
            return {'nativeItemId': item['id'], 'exactBody': True, 'count': len(matches)}
        if time.monotonic() > deadline:
            raise AssertionError(('native message not found', message, history))
        time.sleep(.1)


class Provider(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        size = int(self.headers.get('Content-Length', 0))
        request = self.rfile.read(size)
        with open(self.server.root / 'provider.jsonl', 'ab') as log:
            log.write(request + b'\n')
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.end_headers()
        item = {'type': 'message', 'role': 'assistant', 'id': 'fixture-ready',
                'content': [{'type': 'output_text', 'text': 'ready'}]}
        for event in [
            {'type': 'response.output_item.done', 'item': item},
            {'type': 'response.completed', 'response': {'id': 'fixture-response',
             'usage': {'input_tokens': 0, 'input_tokens_details': None,
                       'output_tokens': 0, 'output_tokens_details': None, 'total_tokens': 0}}},
        ]:
            self.wfile.write(('data: ' + json.dumps(event) + '\n\n').encode())


class NativeClient:
    """Minimal RFC6455 client for the fixture's local Unix socket only."""
    def __init__(self, filename):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(20)
        self.sock.connect(str(filename))
        key = base64.b64encode(os.urandom(16)).decode()
        request = f'GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n'
        self.sock.sendall(request.encode())
        response = bytearray()
        while not response.endswith(b'\r\n\r\n'):
            response.extend(self.sock.recv(1))
        assert response.startswith(b'HTTP/1.1 101'), response
        self.file = self.sock.makefile('rb')
        self.sequence = 0

    def send(self, value, opcode=1):
        data = json.dumps(value).encode() if opcode == 1 else value
        mask = os.urandom(4)
        size = len(data)
        length = bytes([size | 128]) if size < 126 else b'\xfe' + struct.pack('!H', size)
        self.sock.sendall(bytes([128 | opcode]) + length + mask + bytes(v ^ mask[i % 4] for i, v in enumerate(data)))

    def receive(self):
        message = bytearray()
        while True:
            first, second = self.file.read(2)
            size = second & 127
            if size == 126:
                size = struct.unpack('!H', self.file.read(2))[0]
            elif size == 127:
                size = struct.unpack('!Q', self.file.read(8))[0]
            payload = self.file.read(size)
            opcode = first & 15
            if opcode == 9:
                self.send(payload, 10)
                continue
            if opcode == 8:
                raise EOFError('native socket closed')
            message.extend(payload)
            if first & 128:
                return json.loads(message)

    def rpc(self, method, params):
        self.sequence += 1
        self.send({'id': self.sequence, 'method': method, 'params': params})
        while True:
            answer = self.receive()
            if answer.get('id') == self.sequence:
                if 'error' in answer:
                    raise RuntimeError(answer)
                return answer['result']

    def drain(self):
        try:
            while True:
                self.receive()
        except (OSError, EOFError, ValueError):
            pass


def worker(root):
    root = Path(root).resolve()
    env = {k: v for k, v in os.environ.items() if not k.startswith(('CODEX_', 'MEKTUP_', 'HERDR_'))}
    env.update(CODEX_HOME=str(root / 'codex'), XDG_CONFIG_HOME=str(root / 'config'),
               XDG_DATA_HOME=str(root / 'data'), XDG_STATE_HOME=str(root / 'state'),
               XDG_CACHE_HOME=str(root / 'cache'), PATH=str(root / 'bin') + ':' + os.environ['PATH'])
    write_private(root / 'env.json', json.dumps(env))
    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Provider)
    server.root = root
    threading.Thread(target=server.serve_forever, daemon=True).start()
    config = f'''thread_unload_delay_secs = 1
model = "mock-model"
approval_policy = "never"
sandbox_mode = "read-only"
model_provider = "fixture"
[model_providers.fixture]
name = "Isolated two-host acceptance"
base_url = "http://127.0.0.1:{server.server_port}/v1"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
'''
    write_private(root / 'codex/config.toml', config)
    control = root / 'codex/app-server-control'
    control.mkdir(mode=0o700)
    socket = root / 'daemon.sock'
    (control / 'app-server-control.sock').symlink_to(socket)
    codex = shutil.which('codex')
    log = open(root / 'daemon.log', 'w')
    daemon = subprocess.Popen([codex, 'app-server', '--listen', 'unix://' + str(socket)],
                              env=env, stdout=log, stderr=log)
    waiters = []
    seed = None
    try:
        for _ in range(200):
            if socket.exists():
                break
            if daemon.poll() is not None:
                raise RuntimeError((root / 'daemon.log').read_text())
            time.sleep(.05)
        else:
            raise RuntimeError('isolated daemon did not create socket')
        seed = NativeClient(socket)
        seed.rpc('initialize', {'clientInfo': {'name': 'mektup-acceptance', 'version': '1.0.0'},
                              'capabilities': {'experimentalApi': True}})
        seed.send({'method': 'initialized'})
        thread = seed.rpc('thread/start', {'model': 'mock-model', 'cwd': str(root)})['thread']['id']
        seed.rpc('turn/start', {'threadId': thread, 'input': [{'type': 'text', 'text': 'fixture seed'}]})
        seed.sock.settimeout(None)
        threading.Thread(target=seed.drain, daemon=True).start()
        print(json.dumps({'ready': True, 'codex': run([codex, '--version']).strip(),
                          'root': str(root), 'pid': daemon.pid, 'threadId': thread}), flush=True)
        for line in sys.stdin:
            req = json.loads(line)
            try:
                op = req.get('op', 'cli')
                if op == 'stop':
                    print('{}', flush=True)
                    return
                if op == 'config':
                    write_private(root / 'config/mektup/endpoints.json', json.dumps(req['value']))
                    answer = {}
                elif op == 'files':
                    answer = {str(p.relative_to(root)): p.read_text(errors='replace')
                              for p in root.rglob('*') if p.is_file() and p.suffix in ('.log', '.jsonl')}
                elif op == 'stop_waiters':
                    for waiter in waiters:
                        waiter.terminate()
                        waiter.communicate(timeout=5)
                    waiters.clear()
                    answer = {}
                elif op == 'fault':
                    write_private(root / 'drop-commit-once', 'armed')
                    answer = {}
                else:
                    cli_env = dict(env)
                    cli_env['CODEX_THREAD_ID'] = req.get('thread', '')
                    argv = [str(root / 'mektup'), '--json'] + req['args']
                    if op == 'waiter':
                        waiters.append(subprocess.Popen(argv, env=cli_env, stdout=subprocess.PIPE,
                                                        stderr=subprocess.PIPE, text=True))
                        answer = {'started': True}
                    else:
                        result = subprocess.run(argv, env=cli_env, text=True, capture_output=True, timeout=60)
                        answer = {'code': result.returncode, 'stdout': result.stdout, 'stderr': result.stderr}
                print(json.dumps(answer), flush=True)
            except Exception as exc:
                print(json.dumps({'exception': str(exc)}), flush=True)
    finally:
        if seed:
            seed.sock.close()
        for child in waiters + [daemon]:
            if child.poll() is None:
                child.terminate()
            try:
                child.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.communicate()
        server.shutdown()
        log.close()


def dispatch(root, args):
    allowed = (['codex', 'app-server', 'proxy'], ['mektup', 'control', 'preflight'],
               ['mektup', 'control', 'receive'])
    if args not in allowed:
        raise RuntimeError('fixture dispatcher rejects non-fixed command')
    root = Path(root)
    env = json.loads((root / 'env.json').read_text())
    with open(root / 'ssh-transport.jsonl', 'a') as transport:
        transport.write(json.dumps({'argv': args, 'pid': os.getpid()}) + '\n')
    if args[0] == 'codex':
        executable = shutil.which('codex', path=os.environ['PATH'])
        argv = [executable, 'app-server', 'proxy', '--sock', str(root / 'daemon.sock')]
    else:
        argv = [str(root / 'mektup')] + args[1:]
        if args[-1] == 'receive':
            incoming = sys.stdin.buffer.read()
            request = json.loads(incoming)
            marker = root / 'drop-commit-once'
            if request.get('operation') == 'commit' and marker.exists():
                marker.unlink()
                result = subprocess.run(argv, env=env, input=incoming, capture_output=True)
                write_private(root / 'injected-loss.jsonl', json.dumps({
                    'operation': 'commit', 'exit': result.returncode,
                    'discardedResponse': result.stdout.decode(),
                    'stderr': result.stderr.decode()}))
                # The receiver completed, but no response reaches the caller.
                os._exit(255)
            result = subprocess.run(argv, env=env, input=incoming)
            sys.exit(result.returncode)
    os.execve(argv[0], argv, env)


class Host:
    def __init__(self, root, host=None):
        self.root, self.host = root, host
        argv = ['python3', root + '/fixture.py', '--worker', root]
        if host:
            argv = ['/usr/bin/ssh', '-o', 'BatchMode=yes', '--', host, shlex.join(argv)]
        self.proc = subprocess.Popen(argv, text=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        try:
            self.info = json.loads(self.proc.stdout.readline())
            assert self.info['ready'], self.info
        except BaseException:
            self.proc.send_signal(signal.SIGINT)
            self.proc.wait(timeout=15)
            raise

    def request(self, **request):
        self.proc.stdin.write(json.dumps(request) + '\n')
        self.proc.stdin.flush()
        line = self.proc.stdout.readline()
        if not line:
            raise RuntimeError('fixture worker exited')
        answer = json.loads(line)
        if 'exception' in answer:
            raise RuntimeError(answer['exception'])
        return answer

    def cli(self, *args, thread=''):
        return events(self.request(args=list(args), thread=thread))

    def stop(self):
        if self.proc.poll() is None:
            try:
                self.request(op='stop')
            finally:
                self.proc.stdin.close()
                self.proc.wait(timeout=15)


def install_wrapper(root, peer_root, peer_host, remote=None):
    # Values derive only from validated host names and mktemp roots, never messages.
    fixed = ['/usr/bin/ssh', '-o', 'BatchMode=yes', '--', peer_host,
             'python3', peer_root + '/fixture.py', '--dispatch', peer_root]
    script = '#!/usr/bin/env python3\nimport os,sys\n'
    script += 'allowed = (["codex","app-server","proxy"], ["mektup","control","receive"], ["mektup","control","preflight"])\n'
    script += f'assert sys.argv[1:3] == ["--", {peer_host!r}], sys.argv\n'
    script += 'assert sys.argv[3:] in allowed, sys.argv\n'
    script += f'argv = {fixed!r} + sys.argv[3:]\nos.execv(argv[0], argv)\n'
    if remote:
        run(['/usr/bin/ssh', '--', remote, f'mkdir -m 700 -p {shlex.quote(root + "/bin")}; cat > {shlex.quote(root + "/bin/ssh")}; chmod 700 {shlex.quote(root + "/bin/ssh")}'], input=script)
    else:
        write_private(Path(root) / 'bin/ssh', script)
        (Path(root) / 'bin/ssh').chmod(0o700)


def main(args):
    if any(not x or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-' for c in x)
           for x in (args.remote_host, args.return_host)):
        raise ValueError('use simple SSH config aliases for fixture hosts')
    # macOS sockaddr_un has a short path limit; avoid its long default TMPDIR.
    local = str(Path(tempfile.mkdtemp(prefix='mektup-xh-', dir='/tmp')).resolve())
    remote = run(['/usr/bin/ssh', '-o', 'BatchMode=yes', '--', args.remote_host,
                  'mktemp -d /tmp/mektup-xh-XXXXXXXX']).strip()
    assert remote.startswith('/tmp/mektup-xh-') and remote.count('/') == 2
    workers = []
    report = {'schema': 'mektup/cross-host-acceptance/v1', 'status': 'FAIL',
              'environmentSelection': 'fixture-only fixed-command SSH wrappers',
              'transport': 'real OpenSSH in both directions', 'checks': [],
              'binaries': {name: hashlib.sha256(Path(filename).read_bytes()).hexdigest()
                           for name, filename in [('local', args.local_binary),
                                                  ('remote', args.remote_binary)]}}
    try:
        shutil.copyfile(args.local_binary, local + '/mektup')
        Path(local + '/mektup').chmod(0o700)
        shutil.copyfile(__file__, local + '/fixture.py')
        run(['/usr/bin/scp', '-q', args.remote_binary, args.remote_host + ':' + remote + '/mektup'])
        run(['/usr/bin/scp', '-q', __file__, args.remote_host + ':' + remote + '/fixture.py'])
        run(['/usr/bin/ssh', '--', args.remote_host, 'chmod 700 ' + remote + '/mektup'])
        install_wrapper(local, remote, args.remote_host)
        install_wrapper(remote, local, args.return_host, args.remote_host)
        a = Host(local)
        workers.append(a)
        b = Host(remote, args.remote_host)
        workers.append(b)
        report['hosts'] = [a.info, b.info]
        ids = [find_key(h.cli('endpoint', 'show', 'local'), 'id') for h in workers]
        assert all(ids) and ids[0] != ids[1], ids
        threads = [h.info['threadId'] for h in workers]
        assert all(threads), threads
        report['endpointIds'] = ids
        uris = [f'codex://{ep}/thread/{thread}' for ep, thread in zip(ids, threads)]
        configs = [{'version': 1, 'endpoints': [{'id': ids[1-i], 'alias': 'peer',
                    'route': {'kind': 'ssh', 'sshHost': [args.remote_host, args.return_host][i]},
                    'herdr': 'disabled'}]} for i in range(2)]
        for h, config in zip(workers, configs):
            h.request(op='config', value=config)
        for case, i in enumerate([0, 1, 0]):
            print(f'checking {("local", "remote")[i]} request origin', file=sys.stderr, flush=True)
            sender, receiver = workers[i], workers[1-i]
            body = f'exact request {case}: İstanbul 👋\nquotes " \' $() `literal`\n'
            reply_body = f'exact reply {case}: yanıt ✓\nlast line\n'
            sent = sender.cli('send', uris[1-i], body, '--request-reply', thread=threads[i])
            message = find_key(sent, 'messageId')
            assert message, sent
            sender.request(op='waiter', args=['wait', message, '--timeout', '30s'], thread=threads[i])
            time.sleep(.5)
            sender.request(op='stop_waiters')
            if case == 2:
                sender.request(op='fault')
            reply_result = receiver.request(args=['reply', message, reply_body], thread=threads[1-i])
            if case == 2:
                assert reply_result['code'] == 4 and 'reply_outcome_unknown' in reply_result['stdout'], reply_result
                replied = [json.loads(line) for line in reply_result['stdout'].splitlines()]
                assert find_key(replied, 'effectState') == 'outcome_unknown', replied
                assert find_key(replied, 'receiptId'), replied
            else:
                replied = events(reply_result)
            reply = find_key(replied, 'messageId')
            assert reply, replied
            waited = sender.cli('wait', message, '--timeout', '15s', thread=threads[i])
            assert reply in json.dumps(waited), waited
            # A new CLI invocation reopens the original journal after interrupted waiter.
            again = sender.cli('wait', message, '--timeout', '1s', thread=threads[i])
            assert reply in json.dumps(again), again
            request_native = exact_native(receiver, uris[1-i], message, body)
            reply_native = exact_native(sender, uris[i], reply, reply_body)
            # An explicit second reply is a new message by design. Recover by
            # observation; never resend an operation with an unknown outcome.
            reconciled = None
            if case == 2:
                reconciled = receiver.cli('receipt', 'reconcile', find_key(replied, 'receiptId'))
                assert find_key(reconciled, 'state') == 'accepted', reconciled
            exact_native(sender, uris[i], reply, reply_body)
            report['checks'].append({'origin': ['local', 'remote'][i], 'request': message,
                                     'reply': reply, 'waiterRestart': True,
                                     'commitResponseDropped': case == 2,
                                     'reconciled': reconciled,
                                     'requestNative': request_native, 'replyNative': reply_native,
                                     'send': sent, 'replyReceipt': replied, 'wait': waited})
        # Missing reverse endpoint must reject before submitting the body.
        b.request(op='config', value={'version': 1, 'endpoints': []})
        denied_body = 'MUST NOT BE DELIVERED missing return route'
        denied = a.request(args=['send', uris[1], denied_body, '--request-reply'], thread=threads[0])
        assert denied['code'] != 0 and 'reply_route_unavailable' in denied['stdout'], denied
        denied_events = [json.loads(line) for line in denied['stdout'].splitlines()]
        assert find_key(denied_events, 'effectState') == 'not_sent', denied_events
        denied_history = b.cli('thread', 'items', uris[1], '--limit', '100')
        assert denied_body not in json.dumps(denied_history), denied_history
        report['missingReturnRoute'] = denied
        loss = json.loads(a.request(op='files')['injected-loss.jsonl'])
        assert loss['exit'] == 0 and loss['discardedResponse'], loss
        report['lostCommitResponse'] = {'receiverCommitted': True, 'recoveredWithoutDuplicate': True}
        report['status'] = 'PASS'
    except Exception as exc:
        report['error'] = str(exc)
        raise
    finally:
        for i, h in enumerate(workers):
            try:
                logs = h.request(op='files')
                if args.report_dir:
                    dest = Path(args.report_dir)
                    dest.mkdir(parents=True, exist_ok=True)
                    write_private(dest / f'host-{i}-logs.json', json.dumps(logs, ensure_ascii=False, indent=2))
            finally:
                h.stop()
        shutil.rmtree(local)
        run(['/usr/bin/ssh', '--', args.remote_host, 'rm -rf -- ' + shlex.quote(remote)])
        report['cleanup'] = {'workersExited': all(h.proc.poll() == 0 for h in workers), 'rootsRemoved': True}
        if args.report_dir:
            write_private(Path(args.report_dir) / 'receipt.json', json.dumps(report, ensure_ascii=False, indent=2))
    output = report if not args.report_dir else {key: report[key] for key in
             ('schema', 'status', 'environmentSelection', 'transport', 'binaries')}
    print(json.dumps(output, ensure_ascii=False))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--worker')
    parser.add_argument('--dispatch')
    parser.add_argument('--local-binary')
    parser.add_argument('--remote-binary')
    parser.add_argument('--remote-host', default='devbox')
    parser.add_argument('--return-host', default='seele')
    parser.add_argument('--report-dir')
    parsed, rest = parser.parse_known_args()
    if parsed.worker:
        worker(parsed.worker)
    elif parsed.dispatch:
        dispatch(parsed.dispatch, rest)
    else:
        main(parsed)
