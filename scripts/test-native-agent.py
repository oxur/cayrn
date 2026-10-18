#!/usr/bin/env python3
"""Exercise real Cayrn host/SDK wire and LFE agent; all state is temporary."""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import tempfile
import time
import uuid

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--agent-dir', type=Path, required=True)
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
            return
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
            print('PASS: real LFE bindings, two clients, lost reply dedup/conflict, Unicode/errors/input and last-client reconnect', flush=True)
            # Agent connection is separate from public client connections.
            stop(host, signal.SIGINT)
            assert host.returncode == 0
            host = host_start()
            restarted = Client(state / 'cayrn.sock')
            assert restarted.call(op='inspect_session', session=session['id'])['data']['state'] == 'state_lost'
            assert restarted.call(op='inspect_operation', session=session['id'], operation='once')['data']['value'] == '42'
            assert transcript(restarted)[:len(before)] == before
            assert agent.poll() is None
            print('PASS: host restart preserves committed transcript, marks old context lost, leaves shared BEAM alive', flush=True)
            stop(host, signal.SIGINT)
        except BaseException:
            for log in (agent_log, host_log):
                log.flush(); log.seek(0); print(log.read()[-4000:])
            raise
        finally:
            for process in reversed(processes):
                stop(process)
