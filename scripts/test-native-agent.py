#!/usr/bin/env python3
"""Exercise real Cayrn host/SDK wire and LFE agent; all state is temporary."""
import argparse
import json
import os
from pathlib import Path
import signal
import concurrent.futures
import pty
import select
import fcntl
import termios
import socket
import struct
import subprocess
import tempfile
import time
import uuid
import sqlite3

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--agent-dir', type=Path, required=True)
parser.add_argument('--xrepl', type=Path)
args = parser.parse_args()
ROOT = Path(__file__).resolve().parent.parent


class Client:
    def __init__(self, path):
        self.socket = socket.socket(socket.AF_UNIX)
        self.socket.settimeout(5)
        self.socket.connect(str(path))

    def call(self, **request):
        identifier = str(uuid.uuid4())
        payload = json.dumps(dict(version=1, id=identifier, request=request)).encode()
        self.socket.sendall(struct.pack('>I', len(payload)) + payload)
        def exact(size):
            output = b''
            while len(output) < size:
                chunk = self.socket.recv(size - len(output))
                if not chunk:
                    raise EOFError('host disconnected')
                output += chunk
            return output
        reply = json.loads(exact(struct.unpack('>I', exact(4))[0]))
        assert reply['version'] == 1 and reply['id'] == identifier
        return reply['response']

    def close(self):
        self.socket.close()


def wait_socket(path, process):
    for _ in range(200):
        if path.exists():
            probe = socket.socket(socket.AF_UNIX)
            try:
                probe.settimeout(.05)
                probe.connect(str(path))
                return
            except (ConnectionRefusedError, FileNotFoundError, socket.timeout):
                pass
            finally:
                probe.close()
        assert process.poll() is None, f'process stopped: {process.returncode}'
        time.sleep(.025)
    raise TimeoutError(str(path))


def stop(process, sig=signal.SIGTERM):
    if process.poll() is None:
        process.send_signal(sig)
        try:
            process.wait(timeout=6)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise AssertionError('shutdown exceeded six seconds')


with tempfile.TemporaryDirectory(prefix='cayrn-native-', dir='/private/tmp') as folder:
    directory = Path(folder)
    endpoint = directory / 'agent.sock'
    state = directory / 'state'
    processes = []
    with (directory / 'agent.log').open('w+') as agent_log, (directory / 'host.log').open('w+') as host_log:
        try:
            agent = subprocess.Popen([str(args.agent_dir.resolve() / 'bin/cayrn-lfe'), str(endpoint)], stdout=agent_log, stderr=agent_log)
            processes.append(agent)
            wait_socket(endpoint, agent)
            def native_negative(first, second=None, malformed=None):
                connection = socket.socket(socket.AF_UNIX)
                connection.settimeout(3)
                connection.connect(str(endpoint))
                def send(value):
                    payload = json.dumps(value).encode()
                    connection.sendall(struct.pack('>I', len(payload)) + payload)
                try:
                    if malformed is not None:
                        connection.sendall(malformed)
                    else:
                        send(first)
                        if second:
                            header = connection.recv(4)
                            size = struct.unpack('>I', header)[0]
                            data = b''
                            while len(data) < size:
                                data += connection.recv(size - len(data))
                            assert json.loads(data)['kind'] == 'ready'
                            send(second)
                    try:
                        assert connection.recv(1) == b'', 'invalid native command was accepted'
                    except ConnectionResetError:
                        pass
                finally:
                    connection.close()
            native_negative(dict(kind='hello', version=999, session='bad', generation='g', authority='a'))
            native_negative(None, malformed=struct.pack('>I', 1) + b'{')
            native_negative(None, malformed=struct.pack('>I', 262145))
            for field in ['generation', 'authority']:
                identity = str(uuid.uuid4())
                hello = dict(kind='hello', version=1, session=identity, generation='g', authority='a')
                dispatch = dict(kind='dispatch', operation='invalid', generation='g', authority='a', source='42')
                dispatch[field] = 'stale'
                native_negative(hello, dispatch)
            assert agent.poll() is None
            print('PASS: native malformed/oversized frames, incompatible version and stale generation/authority fail closed', flush=True)
            def host_start():
                host = subprocess.Popen([str(ROOT / 'bin/cayrn'), 'serve-agent', '--state-dir', str(state), '--agent-socket', str(endpoint)], stdout=host_log, stderr=host_log)
                processes.append(host)
                wait_socket(state / 'cayrn.sock', host)
                return host
            host = host_start()
            terminal = Client(state / 'cayrn.sock')
            editor = Client(state / 'cayrn.sock')
            session = terminal.call(op='create')['data']
            def submission(source, operation=None):
                return dict(session=session['id'], generation=session['generation'], operation=operation or str(uuid.uuid4()), client='test', source=source)
            def evaluate(source, operation=None, client=terminal):
                sub = submission(source, operation)
                admitted = client.call(op='submit', submission=sub)
                assert admitted['type'] == 'operation', admitted
                for _ in range(400):
                    result = client.call(op='inspect_operation', session=session['id'], operation=sub['operation'])['data']
                    if result['phase'] not in ('queued', 'dispatched'):
                        return sub, result
                    time.sleep(.005)
                raise TimeoutError(source)
            def transcript(client):
                after = '0'
                events = []
                while True:
                    reply = client.call(op='read', session=session['id'], after=after, limit=3)['data']
                    events.extend(reply['events'])
                    after = reply['next']
                    if after == reply['head']:
                        return events
            assert evaluate('(set cayrn-probe 41)')[1]['value'] == '41'
            assert evaluate('(+ cayrn-probe 1)', client=editor)[1]['value'] == '42'
            sub, result = evaluate('(set cayrn-probe (+ cayrn-probe 1))', 'once')
            assert result['value'] == '42'
            assert editor.call(op='submit', submission=sub)['data'] == result
            changed = dict(sub, source='(set cayrn-probe 999)')
            assert editor.call(op='submit', submission=changed)['data']['code'] == 'conflict'
            assert evaluate('cayrn-probe')[1]['value'] == '42'
            # Lose the acceptance reply deliberately; same operation ID must not rerun.
            lost = Client(state / 'cayrn.sock')
            lost_sub = submission('(set cayrn-probe (+ cayrn-probe 1))', 'lost-reply')
            payload = json.dumps(dict(version=1, id='lost', request=dict(op='submit', submission=lost_sub))).encode()
            lost.socket.sendall(struct.pack('>I', len(payload)) + payload)
            lost.close()
            time.sleep(.05)
            assert editor.call(op='submit', submission=lost_sub)['type'] == 'operation'
            assert evaluate('cayrn-probe')[1]['value'] == '43'
            _, output = evaluate('(progn (io:format "hello ~ts" (list "世界")) cayrn-probe)')
            assert output['phase'] == 'succeeded'
            before = transcript(terminal)
            assert any(e['event'].get('text') == 'hello 世界' for e in before)
            assert before == transcript(editor)
            assert evaluate('(/ 1 0)')[1]['phase'] == 'failed'
            assert evaluate('(')[1]['phase'] == 'failed'
            assert evaluate('cayrn-probe')[1]['value'] == '43'
            assert 'enotsup' in evaluate('(io:get_line "input?")')[1]['value']
            terminal.close(); editor.close()
            reconnect = Client(state / 'cayrn.sock')
            assert evaluate('cayrn-probe', client=reconnect)[1]['value'] == '43'
            peer_session = reconnect.call(op='create')['data']
            slow = submission('(progn (timer:sleep 1500) cayrn-probe)')
            assert reconnect.call(op='submit', submission=slow)['type'] == 'operation'
            peer_sub = dict(session=peer_session['id'], generation=peer_session['generation'], operation='peer', client='peer', source='(+ 20 22)')
            reconnect.call(op='submit', submission=peer_sub)
            for _ in range(100):
                peer_result = reconnect.call(op='inspect_operation', session=peer_session['id'], operation='peer')['data']
                if peer_result['phase'] == 'succeeded':
                    break
                time.sleep(.005)
            assert peer_result['value'] == '42'
            assert reconnect.call(op='inspect_operation', session=session['id'], operation=slow['operation'])['data']['phase'] == 'dispatched'
            for _ in range(400):
                if reconnect.call(op='inspect_operation', session=session['id'], operation=slow['operation'])['data']['phase'] == 'succeeded':
                    break
                time.sleep(.005)
            else:
                raise AssertionError('slow evaluation failed to complete')
            print('PASS: busy native evaluator leaves peer context and inspection responsive', flush=True)
            delayed, _ = evaluate('(progn (spawn (lambda () (timer:sleep 150) (io:format "LATE-ORIGIN" ()))) (quote scheduled))', client=reconnect)
            following, _ = evaluate('(timer:sleep 400)', client=reconnect)
            late_events = transcript(reconnect)
            late = next(e for e in late_events if e['event'].get('text') == 'LATE-ORIGIN')
            assert late['event']['operation'] == delayed['operation']
            assert int(late['sequence']) < int(next(e['sequence'] for e in late_events if e['event'].get('kind') == 'outcome' and e['event'].get('operation') == following['operation']))
            observer = Client(state / 'cayrn.sock')
            after = late_events[-1]['sequence']
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                waiting = pool.submit(observer.call, op='follow', session=session['id'], after=after, limit=64, wait_ms=1000)
                evaluate('(+ cayrn-probe 1)', client=reconnect)
                follow_page = waiting.result(timeout=4)['data']
                assert int(follow_page['events'][0]['sequence']) == int(after) + 1
            observer.close()
            print('PASS: replay/follow arrival and late output preserve contiguous cursors and original operation identity', flush=True)
            if args.xrepl:
                binary = str(args.xrepl.resolve())
                head = transcript(reconnect)[-1]['sequence']
                run = subprocess.run([binary, '--cayrn', str(state), '--session', session['id'], '--after', head, '--cayrn-json'], input='cayrn-probe\n', text=True, capture_output=True, timeout=8)
                assert run.returncode == 0, run.stderr
                records = [json.loads(line) for line in run.stdout.splitlines()]
                assert records[0]['session'] == session['id']
                assert any(e.get('event', {}).get('value') == '43' for e in records)
                sequences = [int(e['sequence']) for e in records if 'sequence' in e]
                assert sequences == list(range(int(head) + 1, int(head) + 1 + len(sequences)))
                master, slave = pty.openpty()
                fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack('HHHH', 30, 120, 0, 0))
                interactive = subprocess.Popen([binary, '--cayrn', str(state), '--session', session['id'], '--after', transcript(reconnect)[-1]['sequence']], stdin=slave, stdout=slave, stderr=slave, env=dict(os.environ, TERM='xterm-256color'))
                os.close(slave)
                screen = b''
                def collect(seconds):
                    output = b''
                    deadline = time.monotonic() + seconds
                    while time.monotonic() < deadline:
                        readable, _, _ = select.select([master], [], [], max(0, deadline - time.monotonic()))
                        if not readable:
                            break
                        try:
                            chunk = os.read(master, 65536)
                        except OSError:
                            break
                        output += chunk
                        if b'\x1b[6n' in chunk:
                            os.write(master, b'\x1b[1;1R')
                    return output
                try:
                    screen += collect(.8)
                    os.write(master, b'(+ cayrn-probe ')
                    screen += collect(.2)
                    evaluate('(progn (io:format "EDIT-BUFFER" ()) 0)', client=reconnect)
                    during = collect(.3)
                    assert b'EDIT-BUFFER' not in during, during
                    os.write(master, b'1)\r')
                    screen += during + collect(1.2)
                    assert b'EDIT-BUFFER' in screen and b'Succeeded] 44' in screen, screen
                    assert any(e['event'].get('submission', {}).get('source') == '(+ cayrn-probe 1)' for e in transcript(reconnect))
                    os.write(master, b':quit\r')
                    screen += collect(.5)
                    interactive.wait(timeout=4)
                    assert interactive.returncode == 0, screen
                    assert evaluate('cayrn-probe', client=reconnect)[1]['value'] == '43'
                finally:
                    stop(interactive)
                    os.close(master)
                print('PASS: actual xrepl binary shares session; PTY draft survives remote output; EOF/quit only detaches', flush=True)
                control_session = reconnect.call(op='create')['data']
                master, slave = pty.openpty()
                fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack('HHHH', 30, 120, 0, 0))
                def controlling_terminal():
                    os.setsid()
                    fcntl.ioctl(0, termios.TIOCSCTTY, 0)
                interactive = subprocess.Popen([binary, '--cayrn', str(state), '--session', control_session['id']], stdin=slave, stdout=slave, stderr=slave, env=dict(os.environ, TERM='xterm-256color'), preexec_fn=controlling_terminal)
                os.close(slave)
                try:
                    screen = collect(.8)
                    os.write(master, b'(progn (io:format "CTRL-C-START" ()) (timer:sleep 10000) 42)\r')
                    screen += collect(.8)
                    assert b'] CTRL-C-START' in screen, screen
                    os.write(master, b'\x03')
                    screen += collect(.8)
                    assert b'Cancelled]' in screen, screen
                    assert reconnect.call(op='inspect_session', session=control_session['id'])['data']['state'] == 'state_lost'
                    os.write(master, b':quit\r')
                    screen += collect(.4)
                    interactive.wait(timeout=4)
                    assert interactive.returncode == 0, screen
                    assert evaluate('cayrn-probe', client=reconnect)[1]['value'] == '43'
                finally:
                    stop(interactive)
                    os.close(master)
                print('PASS: real PTY Ctrl-C routes session interruption while evaluating; shared peer remains usable', flush=True)

            primary = session
            def wait_for(predicate, label, seconds=5):
                deadline = time.monotonic() + seconds
                while time.monotonic() < deadline:
                    if predicate():
                        return
                    time.sleep(.01)
                raise AssertionError(label)
            session = reconnect.call(op='create')['data']
            marker = directory / 'cancel-marker'
            interrupted = submission('(progn (io:format "INTERRUPT-START" ()) (timer:sleep 1000) (file:write_file "' + str(marker) + '" "bad"))', 'interrupt')
            reconnect.call(op='submit', submission=interrupted)
            wait_for(lambda: any(e['event'].get('text') == 'INTERRUPT-START' for e in transcript(reconnect)), 'native evaluation did not start')
            receipt = reconnect.call(op='cancel', session=session['id'], operation='interrupt')
            assert receipt['data']['disposition'] == 'interrupt_requested_session_destructive'
            wait_for(lambda: reconnect.call(op='inspect_session', session=session['id'])['data']['state'] == 'state_lost', 'interrupt state loss not observed')
            assert reconnect.call(op='inspect_operation', session=session['id'], operation='interrupt')['data']['phase'] == 'cancelled'
            time.sleep(1.1)
            assert not marker.exists()
            assert agent.poll() is None
            session = primary
            assert evaluate('cayrn-probe', client=reconnect)[1]['value'] == '43'
            print('PASS: interruption confirmed after evaluator death, environment discarded, peer session and BEAM survive', flush=True)
            session = reconnect.call(op='create')['data']
            archive_id = session['id']
            closing = submission('(progn (spawn (lambda () (timer:sleep 700) (io:format "AFTER-BARRIER" ()))) (io:format "CLOSE-START" ()) (timer:sleep 3000) 42)', 'close-active')
            reconnect.call(op='submit', submission=closing)
            wait_for(lambda: any(e['event'].get('text') == 'CLOSE-START' for e in transcript(reconnect)), 'close evaluation did not start')
            assert reconnect.call(op='close', session=session['id'])['data']['state'] == 'closing'
            wait_for(lambda: reconnect.call(op='inspect_session', session=session['id'])['data']['state'] == 'closed', 'native producer/capture barrier incomplete')
            sealed = transcript(reconnect)
            assert sealed[-1]['event']['kind'] == 'session_closed'
            assert reconnect.call(op='inspect_operation', session=session['id'], operation='close-active')['data']['phase'] == 'cancelled'
            assert reconnect.call(op='close', session=session['id'])['data']['state'] == 'closed'
            assert reconnect.call(op='submit', submission=submission('42'))['type'] == 'error'
            time.sleep(.8)
            assert transcript(reconnect) == sealed
            session = primary
            assert evaluate('cayrn-probe', client=reconnect)[1]['value'] == '43'
            print('PASS: busy close drains/seals managed capture, repeated close is idempotent, archive is immutable and peer survives', flush=True)
            for boundary in ['output', 'dispatch_intent']:
                session = reconnect.call(op='create')['data']
                marker = directory / ('storage-marker-' + boundary)
                with sqlite3.connect(state / 'journal.sqlite3') as database:
                    database.execute("CREATE TRIGGER injected_failure BEFORE INSERT ON events WHEN json_extract(NEW.json, '$.event.kind')='" + boundary + "' BEGIN SELECT RAISE(ABORT, 'injected storage failure'); END")
                failed = submission('(progn (io:format "STORE-BOUNDARY" ()) (file:write_file "' + str(marker) + '" "bad"))', 'store-failure')
                try:
                    assert reconnect.call(op='submit', submission=failed)['type'] == 'operation'
                    wait_for(lambda: reconnect.call(op='inspect_session', session=session['id'])['data']['state'] == 'state_lost', 'storage fault did not freeze dispatch')
                    phase = reconnect.call(op='inspect_operation', session=session['id'], operation='store-failure')['data']['phase']
                    assert phase == ('unknown' if boundary == 'output' else 'cancelled'), phase
                    time.sleep(.1)
                    assert not marker.exists()
                    assert reconnect.call(op='submit', submission=failed)['data']['phase'] == phase
                    assert not any(e['event'].get('text') == 'STORE-BOUNDARY' for e in transcript(reconnect))
                finally:
                    with sqlite3.connect(state / 'journal.sqlite3') as database:
                        database.execute('DROP TRIGGER injected_failure')
                session = primary
                assert evaluate('cayrn-probe', client=reconnect)[1]['value'] == '43'
            print('PASS: injected capture/dispatch transaction failures give no false ACK, post-ACK effect or repeat execution', flush=True)
            session = reconnect.call(op='create')['data']
            crashed = submission('(erlang:exit (self) (quote kill))', 'evaluator-crash')
            assert reconnect.call(op='submit', submission=crashed)['type'] == 'operation'
            wait_for(lambda: reconnect.call(op='inspect_session', session=session['id'])['data']['state'] == 'state_lost', 'unexpected evaluator death not recorded')
            assert reconnect.call(op='inspect_operation', session=session['id'], operation='evaluator-crash')['data']['phase'] == 'unknown'
            session = primary
            assert evaluate('cayrn-probe', client=reconnect)[1]['value'] == '43'
            print('PASS: unexpected native evaluator death quarantines one context and preserves peer state', flush=True)
            uncertain_session = reconnect.call(op='create')['data']
            session = uncertain_session
            effect = directory / 'once-effect'
            uncertain = submission('(progn (file:write_file "' + str(effect) + '" "x" (list (quote append))) (io:format "EFFECT-COMMITTED" ()) (timer:sleep 10000) 42)', 'uncertain')
            reconnect.call(op='submit', submission=uncertain)
            wait_for(lambda: any(e['event'].get('text') == 'EFFECT-COMMITTED' for e in transcript(reconnect)), 'pre-crash effect not observed')
            assert effect.read_text() == 'x'
            session = primary
            print('PASS: real LFE bindings, two clients, lost reply dedup/conflict, Unicode/errors/input and last-client reconnect', flush=True)
            # Agent connection is separate from public client connections.
            stop(host, signal.SIGKILL)
            assert host.returncode == -signal.SIGKILL
            host = host_start()
            restarted = Client(state / 'cayrn.sock')
            assert restarted.call(op='inspect_session', session=session['id'])['data']['state'] == 'state_lost'
            assert restarted.call(op='inspect_operation', session=session['id'], operation='once')['data']['value'] == '42'
            assert transcript(restarted)[:len(before)] == before
            assert agent.poll() is None
            assert restarted.call(op='inspect_session', session=archive_id)['data']['state'] == 'closed'
            assert restarted.call(op='inspect_operation', session=uncertain_session['id'], operation='uncertain')['data']['phase'] == 'unknown'
            assert restarted.call(op='submit', submission=uncertain)['data']['phase'] == 'unknown'
            assert effect.read_text() == 'x'
            print('PASS: host restart preserves committed transcript, marks old context lost, leaves shared BEAM alive', flush=True)
            fresh = restarted.call(op='create')['data']
            doomed = dict(session=fresh['id'], generation=fresh['generation'], operation='agent-crash', client='failure-test', source='(progn (io:format "NODE-CRASH-START" ()) (timer:sleep 10000) 42)')
            restarted.call(op='submit', submission=doomed)
            session = fresh
            wait_for(lambda: any(e['event'].get('text') == 'NODE-CRASH-START' for e in transcript(restarted)), 'agent crash operation did not start')
            stop(agent, signal.SIGKILL)
            wait_for(lambda: restarted.call(op='inspect_session', session=fresh['id'])['data']['state'] == 'state_lost', 'agent/node loss did not freeze execution')
            assert restarted.call(op='inspect_operation', session=fresh['id'], operation='agent-crash')['data']['phase'] == 'unknown'
            assert restarted.call(op='submit', submission=doomed)['data']['phase'] == 'unknown'
            assert restarted.call(op='inspect_session', session=archive_id)['data']['state'] == 'closed'
            print('PASS: whole agent/node failure exposes its shared failure domain without affecting sealed archives or retrying code', flush=True)
            stop(host, signal.SIGINT)
            assert host.returncode == 0
        except BaseException:
            for log in (agent_log, host_log):
                log.flush(); log.seek(0); print(log.read()[-4000:])
            raise
        finally:
            for process in reversed(processes):
                stop(process)
