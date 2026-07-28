"""Stale-broker resilience (user report: one wedged danvasd on WSL2 poisoned
every serve() with an undiagnosable websocket timeout until pkill).

The spawn path must never equate TCP-accept with readiness: it probes the
hub's own HTTP routes (/__health__, or /__templates__ on older brokers), and
when its freshly spawned danvasd dies on a bind conflict it either attaches
to a LIVE surviving hub (the designed owner-restart path) or raises a
diagnosis naming the squatter — never dials a dead socket and times out.
"""

import http.server
import json
import os
import socket
import subprocess
import threading
import time
import urllib.request

import pytest

import danvas
from danvas.remote import (_BrokerUnavailable, _port_accepts, _probe_hub,
                           serve_via_broker)

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _danvasd():
    exe = os.environ.get("DANVASD")
    if exe and os.path.isfile(exe):
        return exe
    name = "danvasd.exe" if os.name == "nt" else "danvasd"
    for rel in ("broker/target/release", "broker/target/debug"):
        p = os.path.join(_ROOT, rel, name)
        if os.path.isfile(p):
            return p
    return None


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture()
def hub():
    binary = _danvasd()
    if binary is None:
        pytest.skip("danvasd not built")
    port = _free_port()
    proc = subprocess.Popen([binary, "--port", str(port)],
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    deadline = time.time() + 15
    while time.time() < deadline:
        if _probe_hub(port) == "danvasd":
            break
        time.sleep(0.1)
    else:
        proc.kill()
        pytest.skip("danvasd never became ready")
    yield port
    proc.kill()
    proc.wait(timeout=10)


def test_health_endpoint_identifies_the_hub(hub):
    with urllib.request.urlopen(
            f"http://127.0.0.1:{hub}/__health__", timeout=5) as resp:
        body = json.load(resp)
    assert "danvasd" in body and body["run_id"]


def test_probe_classifies_live_hub(hub):
    assert _probe_hub(hub) == "danvasd"


def test_probe_classifies_free_port():
    assert _probe_hub(_free_port(), timeout=0.5) is None


def test_probe_classifies_silent_squatter():
    # Accepts TCP (listen backlog) but never speaks — the wedged-broker
    # shape. The old bare create_connection probe called this "ready".
    lst = socket.socket()
    lst.bind(("127.0.0.1", 0))
    lst.listen(1)
    port = lst.getsockname()[1]
    try:
        assert _port_accepts(port)
        assert _probe_hub(port, timeout=0.5) is None
    finally:
        lst.close()


def test_probe_classifies_foreign_http_server():
    httpd = http.server.HTTPServer(
        ("127.0.0.1", 0), http.server.BaseHTTPRequestHandler)
    port = httpd.server_address[1]
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    try:
        assert _probe_hub(port, timeout=1.0) == "other"
    finally:
        httpd.shutdown()


def test_serve_attaches_to_surviving_hub(hub):
    # A danvasd from a "previous session" already owns the port. serve()'s
    # own spawn loses the bind race and must ATTACH (owner restart is a
    # feature: the UI survives), not dial blind or die.
    canvas = danvas.Canvas()
    canvas.label("greet", "hello")
    serve_via_broker(canvas, port=hub, open_browser=False, block=False)
    try:
        assert canvas._serving
        assert canvas._broker.proc is None      # attached, spawned nothing
    finally:
        canvas._broker.stop()


def test_serve_diagnoses_silent_squatter():
    if _danvasd() is None:
        pytest.skip("danvasd not built")
    lst = socket.socket()
    lst.bind(("127.0.0.1", 0))
    lst.listen(1)
    port = lst.getsockname()[1]
    canvas = danvas.Canvas()
    try:
        with pytest.raises(_BrokerUnavailable, match="already in use"):
            serve_via_broker(canvas, port=port, open_browser=False,
                             block=False)
    finally:
        lst.close()


def test_serve_diagnoses_foreign_server():
    if _danvasd() is None:
        pytest.skip("danvasd not built")
    httpd = http.server.HTTPServer(
        ("127.0.0.1", 0), http.server.BaseHTTPRequestHandler)
    port = httpd.server_address[1]
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    canvas = danvas.Canvas()
    try:
        with pytest.raises(_BrokerUnavailable, match="not a danvas hub"):
            serve_via_broker(canvas, port=port, open_browser=False,
                             block=False)
    finally:
        httpd.shutdown()


def test_source_client_connect_waits_again_on_retry():
    # connect() on an already-running client must WAIT again (the socket
    # loop redials in the background) — returning immediately made every
    # retry loop a no-op.
    from danvas.source import SourceClient
    client = SourceClient(f"127.0.0.1:{_free_port()}", label="host")
    with pytest.raises(TimeoutError):
        client.connect(timeout=0.4)
    t0 = time.time()
    with pytest.raises(TimeoutError):
        client.connect(timeout=0.4)
    assert time.time() - t0 >= 0.3      # second call actually waited
    client.close()


def test_connect_failure_probes_before_teardown(monkeypatch):
    # The failure diagnostic must inspect the hub BEFORE closing/terminating
    # anything (user report: probing after proc.terminate() autopsied a
    # corpse, so every failure read "hub stopped responding" and pointed
    # away from the real cause).
    import danvas.remote as remote_mod

    calls = []

    class FakeProc:
        pid = 1
        stderr = None
        def poll(self): return None
        def terminate(self): calls.append("terminate")

    monkeypatch.setattr("subprocess.Popen", lambda *a, **k: FakeProc())
    monkeypatch.setattr(remote_mod, "_find_danvasd", lambda: "/fake/danvasd")
    probes = iter(["danvasd", "danvasd"])   # ready-probe, then diagnosis
    monkeypatch.setattr(remote_mod, "_probe_hub",
                        lambda *a, **k: (calls.append("probe"),
                                         next(probes, "danvasd"))[1])

    def timeout_connect(self, timeout=10.0):
        raise TimeoutError("nope")
    monkeypatch.setattr(remote_mod.SourceClient, "connect", timeout_connect)
    monkeypatch.setattr(remote_mod.SourceClient, "close",
                        lambda self: calls.append("close"))
    monkeypatch.setenv("DANVAS_CONNECT_TIMEOUT", "0.5")

    canvas = danvas.Canvas()
    with pytest.raises(TimeoutError, match="CPU-bound thread"):
        remote_mod.serve_via_broker(canvas, port=1, open_browser=False,
                                    block=False)
    diag = calls[calls.index("close") - 1] if "close" in calls else None
    assert diag == "probe", f"probe must precede teardown, got {calls}"
    assert calls.index("probe") < calls.index("terminate")
