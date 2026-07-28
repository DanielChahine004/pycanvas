"""The language-neutral component templates + serve(broker=True).

components.json is what lets a non-Python SDK author NATIVE panels (the
register frame's React source + data defaults, extracted from the real
components). serve(broker=True) is the transplant: danvasd owns the port,
the Python process dials in as the host source.
"""

import asyncio
import json
import os
import socket
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import danvas
from danvas.source import SourceClient, _templates


def test_committed_asset_is_fresh():
    import gen_component_templates as gen
    committed = json.load(open(gen.OUT_PATH, encoding="utf-8"))
    assert committed == gen.build(), (
        "danvas/templates/components.json is stale — run "
        "python scripts/gen_component_templates.py")


def test_templates_match_the_real_components():
    tpl = _templates()["slider"]
    real = danvas.Slider().register_props_for(None, None)
    assert tpl["component"] == "React"
    assert tpl["props"]["source"] == real["source"]      # same JSX mounts
    assert tpl["data"]["max"] == 100


def test_register_template_builds_a_native_register():
    c = SourceClient(":8000", label="rig")
    sent = []
    c._send = lambda m: sent.append(m)
    c.register_template("temp", "slider", min=0, max=60, value=20, x=40, y=50)
    reg = sent[-1]
    assert reg["type"] == "register"
    assert reg["component"] == "React"                   # renders natively
    assert reg["name"] == "temp" and (reg["x"], reg["y"]) == (40, 50)
    blob = json.loads(reg["props"]["data"])
    assert (blob["min"], blob["max"], blob["value"]) == (0, 60, 20)
    assert "source" in reg["props"]                      # the mounting JSX


# -- serve(broker=True): the binary owns the port, Python dials in ---------------

def _danvasd():
    from danvas.remote import _find_danvasd
    return _find_danvasd()


@pytest.mark.skipif(_danvasd() is None, reason="danvasd binary not built")
def test_serve_broker_end_to_end():
    from websockets.asyncio.client import connect as ws_connect

    port = None
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()

    canvas = danvas.Canvas()
    servo = canvas.slider("servo", min=0, max=180, default=90)
    status = canvas.label("status", "idle")
    got = []
    servo.on_change(lambda v: (got.append(v), status.update(f"at {v}")))

    canvas.serve(broker=True, port=port, open_browser=False, block=False)
    try:
        # the broker serves the frontend...
        import urllib.request
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=5) as r:
            assert r.status == 200

        async def go():
            async with ws_connect(f"ws://127.0.0.1:{port}/ws",
                                  max_size=None) as ws:
                reg = None
                while reg is None:
                    m = json.loads(await asyncio.wait_for(ws.recv(), 5))
                    if m.get("type") == "register" and m.get("name") == "servo":
                        reg = m
                assert reg["owner"] == "host"
                # a browser input reaches the Python handler through the broker
                await ws.send(json.dumps({"type": "input", "id": reg["id"],
                                          "payload": {"value": 42}}))
                deadline = time.monotonic() + 5
                while not got and time.monotonic() < deadline:
                    await asyncio.sleep(0.05)
                assert got == [42]
                # ...and the handler's update comes back out to the browser
                while True:
                    m = json.loads(await asyncio.wait_for(ws.recv(), 5))
                    if m.get("type") == "update" and "at 42" in json.dumps(m):
                        break
        asyncio.run(asyncio.wait_for(go(), timeout=30))
    finally:
        canvas._broker.stop()


@pytest.mark.skipif(_danvasd() is None, reason="danvasd binary not built")
def test_serve_broker_forwards_view_background_and_merge_server():
    # Tier-1 parity: serve() features that ride the host source or the broker's
    # CLI must land through the broker just as they do embedded — the initial
    # view (folded into welcome), background producer loops, and merge_server=.
    from websockets.asyncio.client import connect as ws_connect

    s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()

    canvas = danvas.Canvas()
    beat = canvas.label("beat", "waiting")

    @canvas.background
    def pulse():
        beat.update("alive")   # a producer loop must run in the serving process

    canvas.serve(broker=True, port=port, open_browser=False, block=False,
                 view={"locked": True, "zoom": 2.0},
                 merge_server="127.0.0.1:9999")
    try:
        async def go():
            async with ws_connect(f"ws://127.0.0.1:{port}/ws", max_size=None,
                                  max_queue=None) as ws:
                welcome = json.loads(await asyncio.wait_for(ws.recv(), 5))
                assert welcome["type"] == "welcome"
                # serve(view=...) folded into the broker's welcome
                assert (welcome.get("view") or {}).get("locked") is True
                assert (welcome.get("view") or {}).get("zoom") == 2.0
                # serve(merge_server=...) advertised for the "Merge…" button
                assert welcome.get("mergeServer") == "127.0.0.1:9999"
                # UI gating: a private local bind (no password, no tunnel)
                # defaults the Inspector/graveyard/cursors/hosting button ON and
                # advertises no auth — the same truth-table as the embedded hub.
                assert welcome.get("uiInspector") is True
                assert welcome.get("uiGraveyard") is True
                assert welcome.get("cursors") is True
                assert welcome.get("uiHosting") is True
                assert welcome.get("auth") is False
                # the background loop ran (through the broker) and its content
                # reaches the browser — either as a live update (browser already
                # connected) or folded into the replayed register (browser
                # joined after the worker fired). Either proves _start_background
                # ran and its frames crossed the hub.
                end = time.monotonic() + 5
                while time.monotonic() < end:
                    m = json.loads(await asyncio.wait_for(ws.recv(), 5))
                    if m.get("type") == "update" and "alive" in json.dumps(m):
                        return
                    if (m.get("type") == "register" and m.get("name") == "beat"
                            and "alive" in (m.get("props", {}).get("data") or "")):
                        return
                raise AssertionError("background update never arrived")
        asyncio.run(asyncio.wait_for(go(), timeout=30))
    finally:
        canvas._broker.stop()


@pytest.mark.skipif(_danvasd() is None, reason="danvasd binary not built")
def test_serve_broker_on_request_round_trips():
    # Tier-2 parity: canvas.request() from the browser must reach the owner's
    # on_request handler through the broker and the reply must route back to
    # exactly the asker (the broker's pending-request routing).
    from websockets.asyncio.client import connect as ws_connect

    s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()

    canvas = danvas.Canvas()
    btn = canvas.button("go")

    @btn.on_request()
    def _(req):
        return {"echo": req.get("n", 0) * 2}

    canvas.serve(broker=True, port=port, open_browser=False, block=False)
    try:
        async def go():
            async with ws_connect(f"ws://127.0.0.1:{port}/ws", max_size=None,
                                  max_queue=None) as ws:
                reg = None
                while reg is None:
                    m = json.loads(await asyncio.wait_for(ws.recv(), 5))
                    if m.get("type") == "register" and m.get("name") == "go":
                        reg = m
                await ws.send(json.dumps({"type": "request", "id": reg["id"],
                                          "reqId": "r1", "data": {"n": 21}}))
                while True:
                    m = json.loads(await asyncio.wait_for(ws.recv(), 5))
                    if m.get("type") == "response" and m.get("reqId") == "r1":
                        assert m["result"] == {"echo": 42}
                        return
        asyncio.run(asyncio.wait_for(go(), timeout=30))
    finally:
        canvas._broker.stop()


@pytest.mark.skipif(_danvasd() is None, reason="danvasd binary not built")
def test_serve_broker_presence_populates_viewers():
    # Tier-2 parity: the broker owns the roster; the host mirrors presence so
    # canvas.viewers reflects the browser audience (and excludes the host's own
    # dial-in, matching the embedded server's semantics).
    from websockets.asyncio.client import connect as ws_connect

    s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()
    canvas = danvas.Canvas(); canvas.label("x", "hi")
    canvas.serve(broker=True, port=port, open_browser=False, block=False)
    try:
        async def go():
            async with ws_connect(f"ws://127.0.0.1:{port}/ws?vname=alice",
                                  max_size=None, max_queue=None):
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    names = {v.get("name") for v in canvas.viewers}
                    if "alice" in names:
                        assert "host" not in names   # host isn't its own viewer
                        return
                    await asyncio.sleep(0.1)
                raise AssertionError(f"alice never appeared; saw {canvas.viewers}")
        asyncio.run(asyncio.wait_for(go(), timeout=30))
    finally:
        canvas._broker.stop()


@pytest.mark.skipif(_danvasd() is None, reason="danvasd binary not built")
def test_serve_broker_on_draw_fires():
    # Tier-2 tail: free-form ink drawn in the browser reaches canvas.on_draw
    # through the broker (danvasd fans hub-native ink to sources too).
    from websockets.asyncio.client import connect as ws_connect

    s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()
    canvas = danvas.Canvas(); canvas.label("x", "hi")
    seen = []
    canvas.on_draw(lambda ev: seen.append(ev))
    canvas.serve(broker=True, port=port, open_browser=False, block=False)
    try:
        async def go():
            async with ws_connect(f"ws://127.0.0.1:{port}/ws", max_size=None,
                                  max_queue=None) as ws:
                # wait past welcome, then draw a bare (hub-native) record
                await asyncio.sleep(0.4)
                await ws.send(json.dumps({"type": "draw", "diff": {
                    "added": {"ink1": {"id": "ink1", "x": 3, "props": {}}},
                    "updated": {}, "removed": {}}}))
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    if any(ev["added"] for ev in seen):
                        return
                    await asyncio.sleep(0.1)
                raise AssertionError("on_draw never fired")
        asyncio.run(asyncio.wait_for(go(), timeout=30))
    finally:
        canvas._broker.stop()


@pytest.mark.skipif(_danvasd() is None, reason="danvasd binary not built")
def test_serve_broker_graveyard_and_restore():
    # Tier-2 tail: a browser deleting (and restoring) a managed panel routes to
    # the owner through the broker and toggles its graveyard state as embedded.
    from websockets.asyncio.client import connect as ws_connect

    s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()
    canvas = danvas.Canvas(); panel = canvas.label("doomed", "here")
    canvas.serve(broker=True, port=port, open_browser=False, block=False)
    try:
        async def go():
            async with ws_connect(f"ws://127.0.0.1:{port}/ws", max_size=None,
                                  max_queue=None) as ws:
                reg = None
                while reg is None:
                    m = json.loads(await asyncio.wait_for(ws.recv(), 5))
                    if m.get("type") == "register" and m.get("name") == "doomed":
                        reg = m
                await ws.send(json.dumps({"type": "graveyard", "id": reg["id"]}))
                end = time.monotonic() + 5
                while not getattr(panel, "_graveyarded", False) and time.monotonic() < end:
                    await asyncio.sleep(0.05)
                assert getattr(panel, "_graveyarded", False), "graveyard didn't reach owner"
                await ws.send(json.dumps({"type": "restore", "id": reg["id"]}))
                end = time.monotonic() + 5
                while getattr(panel, "_graveyarded", False) and time.monotonic() < end:
                    await asyncio.sleep(0.05)
                assert not getattr(panel, "_graveyarded", False), "restore didn't reach owner"
        asyncio.run(asyncio.wait_for(go(), timeout=30))
    finally:
        canvas._broker.stop()


@pytest.mark.skipif(_danvasd() is None, reason="danvasd binary not built")
def test_serve_broker_snapshot_round_trips():
    # Tier-2 tail: canvas.save()/screenshot() round-trip through the broker — the
    # host's get_snapshot reaches a browser and the browser's reply routes back.
    import threading
    from websockets.asyncio.client import connect as ws_connect

    s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()
    canvas = danvas.Canvas(); canvas.label("x", "hi")
    canvas.serve(broker=True, port=port, open_browser=False, block=False)
    try:
        async def go():
            async with ws_connect(f"ws://127.0.0.1:{port}/ws", max_size=None,
                                  max_queue=None) as ws:
                await asyncio.sleep(0.3)               # settle presence
                result = {}
                def ask():
                    result["doc"] = canvas._bridge.request_snapshot(timeout=8)
                t = threading.Thread(target=ask, daemon=True); t.start()
                # play the browser: answer the get_snapshot with a document
                while True:
                    m = json.loads(await asyncio.wait_for(ws.recv(), 5))
                    if m.get("type") == "get_snapshot":
                        await ws.send(json.dumps({"type": "snapshot",
                                                  "reqId": m["reqId"],
                                                  "data": {"records": [1, 2, 3]}}))
                        break
                t.join(8)
                assert result.get("doc") == {"records": [1, 2, 3]}
        asyncio.run(asyncio.wait_for(go(), timeout=30))
    finally:
        canvas._broker.stop()


@pytest.mark.skipif(_danvasd() is None, reason="danvasd binary not built")
def test_serve_broker_inspector_toggle_spawns_panel():
    # Tier-2 tail: the toolbar Inspector toggle routes to the owner through the
    # broker and spawns the Inspector panel (which then appears to browsers).
    from websockets.asyncio.client import connect as ws_connect

    s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()
    canvas = danvas.Canvas(); canvas.label("x", "hi")
    # private local bind -> Inspector defaults ON
    canvas.serve(broker=True, port=port, open_browser=False, block=False)
    try:
        async def go():
            async with ws_connect(f"ws://127.0.0.1:{port}/ws", max_size=None,
                                  max_queue=None) as ws:
                await asyncio.sleep(0.3)
                await ws.send(json.dumps({"type": "ui", "action": "toggle_inspector",
                                          "center": {"x": 200, "y": 200}}))
                end = time.monotonic() + 5
                while time.monotonic() < end:
                    m = json.loads(await asyncio.wait_for(ws.recv(), 5))
                    if (m.get("type") == "register"
                            and "__ui_inspector__" in json.dumps(m)):
                        return
                raise AssertionError("Inspector panel never registered")
        asyncio.run(asyncio.wait_for(go(), timeout=30))
    finally:
        canvas._broker.stop()


# -- the all-Rust stack: danvasd serves, a Rust program authors the canvas -------

def _rust_canvas_exe():
    root = os.path.join(os.path.dirname(__file__), "..")
    exe = "rust_canvas.exe" if os.name == "nt" else "rust_canvas"
    for profile in ("debug", "release"):
        p = os.path.abspath(os.path.join(root, "broker", "target", profile,
                                         "examples", exe))
        if os.path.exists(p):
            return p
    return None


@pytest.mark.skipif(_danvasd() is None or _rust_canvas_exe() is None,
                    reason="rust binaries not built")
def test_canvas_authored_in_rust_no_python_serving():
    import subprocess
    import urllib.request
    from websockets.asyncio.client import connect as ws_connect

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    broker = subprocess.Popen([_danvasd(), "--port", str(port)])
    rust = None
    try:
        deadline = time.time() + 15
        while time.time() < deadline:
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
                break
            except OSError:
                time.sleep(0.1)
        rust = subprocess.Popen([_rust_canvas_exe(), "--port", str(port)])

        async def go():
            async with ws_connect(f"ws://127.0.0.1:{port}/ws",
                                  max_size=None) as ws:
                reg = None
                end = time.monotonic() + 10
                while reg is None and time.monotonic() < end:
                    m = json.loads(await asyncio.wait_for(ws.recv(), 10))
                    if m.get("type") == "register" and m.get("name") == "servo":
                        reg = m
                assert reg is not None
                assert reg["owner"] == "rust-canvas"      # authored in Rust
                assert reg["component"] == "React"        # renders natively
                blob = json.loads(reg["props"]["data"])
                assert blob["max"] == 180                 # template + overrides
                # browser drags the Rust slider -> Rust computes -> label updates
                await ws.send(json.dumps({"type": "input", "id": reg["id"],
                                          "payload": {"value": 77}}))
                while True:
                    m = json.loads(await asyncio.wait_for(ws.recv(), 10))
                    if (m.get("type") == "update"
                            and "computed in rust" in json.dumps(m)
                            and "77" in json.dumps(m)):
                        break
        asyncio.run(asyncio.wait_for(go(), timeout=40))
        # the frontend itself is served by the Rust broker
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=5) as r:
            assert r.status == 200
    finally:
        if rust is not None:
            rust.kill()
        broker.kill()


# -- the default flip: serve() prefers the broker ---------------------------------

def test_serve_resolves_to_broker(monkeypatch):
    # serve() always routes through the broker — there is no embedded fallback.
    import danvas.remote as remote_mod
    calls = {}

    def fake_serve_via_broker(canvas, **kw):
        calls.update(kw)
        return canvas
    monkeypatch.setattr(remote_mod, "serve_via_broker", fake_serve_via_broker)
    # Force the broker to "resolve" regardless of whether a real binary is
    # present in this environment (the broker CI has one, a pure compat run
    # doesn't) — we're testing serve()'s routing, not binary discovery.
    monkeypatch.setattr(remote_mod, "_find_danvasd", lambda: "/fake/danvasd")

    c = danvas.Canvas()
    out = c.serve(port=1234, open_browser=False, block=False)
    assert out is c and calls["port"] == 1234                   # broker path
    # even a feature that used to force the embedded server (persist=) now rides
    # the broker — serve_via_broker is still the one call.
    calls.clear()
    danvas.Canvas().serve(persist=True, open_browser=False, block=False)
    assert calls.get("persist") is True


def test_serve_raises_when_broker_wont_launch(monkeypatch):
    # A found-but-unlaunchable binary (wrong arch, corrupt) surfaces as
    # _BrokerUnavailable — there is no in-process fallback to swallow it.
    import danvas.remote as remote_mod
    from danvas.remote import _BrokerUnavailable

    def boom(canvas, **kw):
        raise _BrokerUnavailable("danvasd exited on startup (code 1)")
    monkeypatch.setattr(remote_mod, "serve_via_broker", boom)
    monkeypatch.setattr(remote_mod, "_find_danvasd", lambda: "/fake/danvasd")

    import pytest as _pytest
    c = danvas.Canvas()
    with _pytest.raises(_BrokerUnavailable):
        c.serve(open_browser=False, block=False)


def test_serve_desktop_routes_through_broker(monkeypatch):
    # desktop=True must reach serve_via_broker with desktop=True (a native
    # window pointed at the broker's URL — a pure client-side retarget).
    import danvas.remote as remote_mod
    calls = {}
    monkeypatch.setattr(remote_mod, "_find_danvasd", lambda: "/fake/danvasd")
    monkeypatch.setattr(remote_mod, "serve_via_broker",
                        lambda canvas, **kw: (calls.update(kw), canvas)[1])
    c = danvas.Canvas()
    c.serve(desktop=True, open_browser=False, block=False)
    assert calls.get("desktop") is True
    assert "window_title" in calls and "window_size" in calls


def test_serve_via_broker_desktop_opens_window_at_broker_url(monkeypatch):
    # desktop=True opens the native window pointed at the BROKER's url. Stub
    # pywebview to capture that url (a real window can't run headless) and
    # stub the danvasd spawn so nothing external launches.
    import sys as _sys
    import types
    import danvas.remote as remote_mod

    captured = {}
    fake_webview = types.ModuleType("webview")
    fake_webview.create_window = lambda title, url, **k: captured.update(
        title=title, url=url)
    fake_webview.start = lambda: None
    monkeypatch.setitem(_sys.modules, "webview", fake_webview)
    monkeypatch.setattr(remote_mod, "_find_danvasd", lambda: "/fake/danvasd")

    class FakeProc:
        pid = 123
        def poll(self): return None
        def terminate(self): pass
    monkeypatch.setattr("subprocess.Popen", lambda *a, **k: FakeProc())
    # readiness is now an identity probe of the hub's HTTP routes (not a
    # bare TCP connect) — stub the probe to say "a danvasd answers"
    monkeypatch.setattr(remote_mod, "_probe_hub", lambda *a, **k: "danvasd")
    monkeypatch.setattr(remote_mod.SourceClient, "connect", lambda self, **k: self)

    c = danvas.Canvas()
    remote_mod.serve_via_broker(c, port=9911, open_browser=False,
                                desktop=True, window_title="MyApp")
    assert captured["url"] == "http://127.0.0.1:9911"    # window -> broker
    assert captured["title"] == "MyApp"


def test_serve_tunnel_opens_python_owned_tunnel_to_broker_port(monkeypatch):
    # tunnel=True through the broker: Python opens a tunnel to danvasd's port
    # (a client-side concern, like the hot-reload monitor tunnels the worker).
    import danvas.remote as remote_mod
    import types, sys as _sys
    opened = {}

    class FakeTunnel:
        url = "https://x.trycloudflare.com"
        def stop(self): opened["stopped"] = True
    fake_tunnel_mod = types.ModuleType("danvas.tunnel")
    fake_tunnel_mod.open_tunnel = lambda port, provider="cloudflared": (
        opened.update(port=port), FakeTunnel())[1]
    # tunnel=True now defaults to the detached keeper (URL stable across
    # restarts); tunnel="ephemeral" is the in-process open_tunnel path.
    fake_tunnel_mod.ensure_tunnel = fake_tunnel_mod.open_tunnel
    monkeypatch.setitem(_sys.modules, "danvas.tunnel", fake_tunnel_mod)
    monkeypatch.setattr(remote_mod, "_find_danvasd", lambda: "/fake/danvasd")

    class FakeProc:
        pid = 1; 
        def poll(self): return None
        def terminate(self): pass
    monkeypatch.setattr("subprocess.Popen", lambda *a, **k: FakeProc())
    monkeypatch.setattr(remote_mod, "_probe_hub", lambda *a, **k: "danvasd")
    monkeypatch.setattr(remote_mod.SourceClient, "connect", lambda self, **k: self)

    c = danvas.Canvas()
    remote_mod.serve_via_broker(c, port=8080, open_browser=False, block=False,
                                tunnel=True)
    assert opened["port"] == 8080                 # tunnel -> the broker's port
    assert c._broker.tunnel is not None
    c._broker.stop()
    assert opened.get("stopped") is True          # torn down with the broker


def test_danvasd_hosting_button_lan_share(monkeypatch):
    # The 🌐 hosting button through the broker: danvasd emits uiHosting on a
    # loopback bind, and host_lan binds a live LAN listener that actually
    # serves. (danvasd-specific parity with the Canvas embedded server, not a
    # both-hubs conformance item — the merge hub has no hosting button.)
    from danvas.remote import _find_danvasd
    binary = _find_danvasd()
    if binary is None:
        import pytest as _pytest
        _pytest.skip("danvasd binary not built")
    import socket, subprocess, time, json, asyncio, urllib.request
    from websockets.asyncio.client import connect as ws_connect
    s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()
    proc = subprocess.Popen([binary, "--port", str(port)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(60):
            try:
                socket.create_connection(("127.0.0.1", port), 0.5).close(); break
            except OSError: time.sleep(0.1)

        async def go():
            async with ws_connect(f"ws://127.0.0.1:{port}/ws", max_size=None) as ws:
                w = json.loads(await asyncio.wait_for(ws.recv(), 3))
                assert w.get("uiHosting") is True                    # button on
                assert (w.get("hosting") or {}).get("local")
                await ws.send(json.dumps({"type": "ui", "action": "host_lan"}))
                lan = None
                end = asyncio.get_event_loop().time() + 8
                while asyncio.get_event_loop().time() < end:
                    m = json.loads(await asyncio.wait_for(ws.recv(), 3))
                    if m.get("type") == "hosting" and m.get("lan"):
                        lan = m["lan"]; break
                assert lan and lan.startswith("http://")             # LAN url
                with urllib.request.urlopen(lan, timeout=4) as r:
                    assert r.status == 200                           # it serves
                await ws.send(json.dumps({"type": "ui", "action": "host_lan_off"}))
                end = asyncio.get_event_loop().time() + 5
                off = False
                while asyncio.get_event_loop().time() < end:
                    m = json.loads(await asyncio.wait_for(ws.recv(), 3))
                    if m.get("type") == "hosting" and m.get("lan") is None:
                        off = True; break
                assert off
        asyncio.run(asyncio.wait_for(go(), timeout=40))
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


def test_custom_factory_exposes_behavior_kwargs():
    # keep_mounted (and friends) used to ride **place invisibly — exactly the
    # kwarg you need when writing a stateful custom panel was absent from the
    # signature and autocomplete. Now they're named parameters end to end.
    import inspect
    sig = inspect.signature(danvas.Canvas.custom)
    for kwarg in ("keep_mounted", "forward_wheel", "themed", "permissions"):
        assert kwarg in sig.parameters, kwarg
    c = danvas.Canvas()
    p = c.custom(html="<b>x</b>", keep_mounted=True, forward_wheel=False)
    assert p._keep_mounted is True and p._forward_wheel is False
