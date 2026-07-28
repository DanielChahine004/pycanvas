"""The native Canvas API over a dial-in connection: ``danvas.connect(url)``.

``SourceClient`` is the wire-level reference (the executable spec for non-
Python SDKs). This module is the ergonomic layer on top: a **RemoteCanvas**
that *is* a :class:`~danvas.canvas.Canvas` — same factories, same component
objects, same handler threading — whose bridge ships frames up one dial-in
socket instead of fanning out to browsers of its own::

    import danvas

    canvas = danvas.connect("127.0.0.1:8000", label="rig")   # any served canvas
    servo = canvas.slider("servo", min=0, max=180)           # native factory

    @servo.on_change                                          # native handlers
    def _(v):
        print("browser set", v)

    servo.min = 10                                            # native setters
    canvas["servo"].color = (255, 0, 0)                       # native lookup

Because the components are the *real* danvas components bound to a socket-
backed bridge, everything a normal script does — ``update()``, live property
setters, ``set_layout``, ``threaded=True`` handlers, containers — works
unchanged; the only difference is where the frames go. The hub composes these
panels for every viewer, routes interactions back here, and holds them frozen
(retention) if this process dies.

Panels owned by OTHER processes are reachable through the shared property
plane: ``canvas.shared`` mirrors the whole canvas this process joined
(``{id: {"component", "props", "state", ...}}``), ``canvas.set_props(id, ...)``
writes any panel's properties, and ``canvas.subscribe(id, fn)`` reacts to any
panel's input events. What doesn't exist here: ``serve()`` (a RemoteCanvas
joins a canvas, it doesn't host one) and binary media (the merge fabric
doesn't relay it).
"""

import logging
import os
import warnings

from .bridge import Bridge
from .canvas import Canvas
from .source import SourceClient

_log = logging.getLogger("danvas")


class _RemoteBridge(Bridge):
    """A Bridge whose every outbound path is one dial-in socket.

    The component layer talks to the bridge through a thin waist (broadcast /
    conflated / per-viewer sends / ``register_live`` / the ``_emit`` tail);
    re-pointing that waist at ``SourceClient._send`` makes the entire native
    component stack remote-transparent. Per-viewer scoping (roles/client
    overlays) has no meaning on a single upstream pipe — the hub applies its
    own role filtering to its own viewers — so those sends collapse to the
    shared frame.
    """

    def __init__(self, client):
        super().__init__()
        self._client = client
        # Panels made here are owned (handler-executed) by this process; the
        # hub re-stamps them with our dial-in label on the composed canvas.
        self._owner_label = client.label

    # -- every outbound path becomes "send it up the socket" ------------------
    def _emit(self, targets, msg):
        self._client._send(msg)

    def broadcast(self, msg, exclude=None, roles=None):
        self._client._send(msg)

    def send_to_client(self, viewer_id, msg):
        self._client._send(msg)

    def send_to_role(self, role, msg):
        self._client._send(msg)

    def broadcast_conflated(self, comp_id, *, msg=None, data=None, exclude=None,
                            **kw):
        # The hub coalesces per-browser on its side; up here there is exactly
        # one pipe, so just send — text or media alike.
        if msg is not None:
            self._client._send(msg)
        elif data is not None:
            self._client._send_binary(data)

    def broadcast_binary(self, data, exclude=None, roles=None):
        # Media rides the same envelope up; the hub rewrites the id in-place
        # and relays to browsers (video/audio through a hub works).
        self._client._send_binary(data)

    def _relay_layout(self, comp, geom, ws):
        # The hub folds a browser's move/resize into its replay cache and
        # relays it to the other browsers itself ("the owner doesn't echo
        # layout back" — broker/src/main.rs). Echoing from here would reach
        # the MOVER too (this pipe can't exclude a viewer), and a stale echo
        # mid-drag snaps the panel backwards.
        pass

    def register_live(self, component, only_roles=None):
        self._client._send(self.register_message(component))
        state = component.state_payload()
        if state:
            self._client._send({"type": "update", "id": component.id,
                                "payload": state})


class RemoteHandle:
    """A live proxy for a panel ANOTHER process owns, resolved by name.

    Reads come from the connection's converging mirror; writes go through the
    shared property plane (they apply at the owner's real setters); events come
    via subscription. So ``canvas["servo"].max = 90`` and
    ``canvas["go"].on_click(fn)`` work whether the panel is local or lives in
    a peer process — the danvas name is the cross-process identity.
    """

    __slots__ = ("_canvas", "id", "name")

    def __init__(self, canvas, panel_id, name):
        object.__setattr__(self, "_canvas", canvas)
        object.__setattr__(self, "id", panel_id)
        object.__setattr__(self, "name", name)

    # -- reads: the mirror (eventually consistent, like any replica) ----------
    def _entry(self):
        return self._canvas._client.panels.get(self.id) or {}

    def __getattr__(self, key):
        entry = self._entry()
        state = entry.get("state") or {}
        if key in state:
            return state[key]
        props = entry.get("props") or {}
        if key in props:
            return props[key]
        if key in entry:
            return entry[key]
        raise AttributeError(
            f"remote panel {self.name!r} has no visible property {key!r} "
            "(reads come from the replica; only streamed state is readable)")

    # -- writes: the shared property plane -------------------------------------
    def __setattr__(self, key, value):
        self._canvas.set_props(self.id, **{key: value})

    def set_props(self, **props):
        self._canvas.set_props(self.id, **props)
        return self

    def update(self, value=None, **props):
        """Content-verb parity with the native object: ``handle.update("ready")``
        does what the owner's ``label.update("ready")`` does — the value routes
        to the owner's ``update()`` (silent, like any programmatic update);
        keyword props ride along as property writes."""
        if value is not None:
            props["value"] = value
        self._canvas.set_props(self.id, **props)
        return self

    def set_layout(self, **layout):
        self._canvas.set_props(self.id, **layout)
        return self

    # -- events: subscription (the owner's handlers keep running too) ----------
    def on_input(self, fn=None):
        """The raw event feed: ``fn(payload)`` for every input on this panel."""
        return self._canvas.subscribe(self.id, fn)

    def on_click(self, fn=None):
        """Button-flavoured sugar: ``fn()`` per click — alongside (not instead
        of) whatever handler the owning process registered."""
        if fn is None:
            return lambda f: self.on_click(f)
        self._canvas.subscribe(self.id, lambda _p, f=fn: f())
        return fn

    def on_change(self, fn=None):
        """Value-control sugar: ``fn(value)`` per committed change."""
        if fn is None:
            return lambda f: self.on_change(f)
        self._canvas.subscribe(
            self.id, lambda p, f=fn: f(p.get("value", p) if isinstance(p, dict) else p))
        return fn

    def __repr__(self):
        entry = self._entry()
        return (f"<RemoteHandle {self.name!r} ({entry.get('component')}) "
                f"id={self.id}>")


class RemoteCanvas(Canvas):
    """A Canvas that joins a served canvas instead of serving one.

    Built by :func:`danvas.connect`. Everything panel-shaped is inherited from
    :class:`Canvas`; the differences are the socket-backed bridge, replay of
    this process's panels on every (re)connect, and the shared-plane accessors
    for panels other processes own.
    """

    def __init__(self, url, label="python", password=None):
        super().__init__()
        client = SourceClient(url, label=label, password=password)
        self._client = client
        self._bridge = _RemoteBridge(client)
        self._bridge._canvas = self
        # Replay on every (re)connect: same shape as Bridge.handle_connection's
        # replay to a fresh browser — register + current state per panel.
        client._replay_hook = self._replay_frames
        # Interactions the hub routes to this source's panels: dispatch through
        # the real bridge machinery, so handler modes (inline/threaded/
        # dedicated/async) and the authoritative state echo work unchanged.
        client.on_frame(self._on_hub_frame)
        # Binary INPUT the hub routes to this source's panels (sendBinary /
        # camera / mic relays): through the real dispatch, like JSON input.
        client._binary_hook = lambda data: _hub_binary(self._bridge, data)
        # Live-announce inserts from the start: Canvas gates register_live on
        # _serving, and for a RemoteCanvas the dial-in session IS the serving
        # state. Frames sent before connect() drop at the (socket-less) client
        # and the on-connect replay reconstructs them, so it's always safe.
        self._serving = True

    # -- lifecycle -------------------------------------------------------------
    # Named dial() rather than connect(): Canvas.connect(a, b) is the ARROW
    # verb, inherited and fully functional here (an arrow between this
    # process's panels rides the socket like any frame) — the session verb
    # must not shadow it.
    def dial(self, timeout=10.0):
        self._client.connect(timeout=timeout)
        return self

    def close(self):
        self._client.close()

    def serve(self, *a, **kw):
        raise RuntimeError(
            "a RemoteCanvas joins an already-served canvas; open the host "
            "canvas's URL to view it (or use danvas.Canvas().serve() to host)")

    # -- replay / inbound -------------------------------------------------------
    def _replay_frames(self):
        """The frames that reconstruct this process's panels on the hub."""
        for comp in self._bridge._components.values():
            yield self._bridge.register_message(comp)
            state = comp.state_payload()
            if state:
                yield {"type": "update", "id": comp.id, "payload": state}
        for arrow in self._arrows:
            yield arrow.register_message()

    def _on_hub_frame(self, msg):
        _dispatch_hub_frame(self._bridge, msg)

    # -- cross-process lookup: the danvas name is the identity ------------------
    def __getitem__(self, name):
        """``canvas["name"]`` resolves panels this process owns (the native
        component object) AND panels any peer owns (a :class:`RemoteHandle`
        proxying reads/writes/events over the wire). Own panels win a name
        collision; foreign names resolve through the connection's mirror —
        waiting briefly (≤2 s) for it to converge, so a lookup right after
        :func:`danvas.connect` doesn't race the hub's initial replay."""
        import time as _time
        try:
            return super().__getitem__(name)
        except KeyError:
            deadline = _time.monotonic() + 2.0
            while True:
                panel_id = self._client.find(name)
                if panel_id is not None:
                    return RemoteHandle(self, panel_id, name)
                if _time.monotonic() >= deadline:
                    raise
                _time.sleep(0.05)

    def __contains__(self, name):
        return super().__contains__(name) or self._client.find(name) is not None

    @property
    def sources(self):
        """The processes contributing panels to the canvas this one joined:
        ``{owner_label: panel_count}``, derived from the replica ("host" is
        the serving canvas itself; this process's label covers its own)."""
        counts = {}
        for entry in self._client.panels.values():
            owner = entry.get("owner")
            if owner:
                counts[owner] = counts.get(owner, 0) + 1
        for comp in self._bridge._components.values():
            counts[self._client.label] = counts.get(self._client.label, 0) + 1
        return counts

    # -- the shared plane (panels other processes own) --------------------------
    @property
    def shared(self):
        """Live mirror of the canvas this process joined: ``{id: entry}`` with
        ``entry = {"component", "props", "state", x/y/w/h}`` — every panel the
        connection may see, whoever owns it. Eventually consistent."""
        return self._client.panels

    def set_props(self, panel_id, **props):
        """Write any panel's properties by id (see :attr:`shared` for ids) —
        applies at the owning process through its real setters."""
        self._client.set_props(panel_id, **props)
        return self

    def subscribe(self, panel_id, fn=None):
        """React to any panel's input events without owning it."""
        return self._client.subscribe(panel_id, fn)

    def unsubscribe(self, panel_id):
        self._client.unsubscribe(panel_id)
        return self


def _dispatch_hub_frame(bridge, msg):
    """Route one hub frame at a source-side bridge: interactions on this
    process's panels go through the real dispatch machinery (handler threading
    modes and the authoritative echo included)."""
    kind = msg.get("type")
    if kind == "welcome":
        # Learn our own dial-in viewer id so the mirrored roster can exclude it
        # — canvas.viewers is the *browser* audience, as under the embedded
        # server (where the host isn't itself a viewer).
        bridge._own_viewer_id = (msg.get("you") or {}).get("id")
        return
    if kind == "file_pull":
        # The hub is asking for a download token's bytes on a browser's
        # behalf (this process owns them). Reply file_meta + a FILE binary
        # envelope; decline tokens that aren't ours (another source's, or
        # expired). Role-gated tokens are declined over a hub — the hub
        # can't verify the per-token role, so fail closed.
        import json as _json
        req = msg.get("reqId")
        item = bridge.take_download(msg.get("token"))
        if item is None or item[2] is not None:
            bridge.broadcast({"type": "file_meta", "reqId": req, "ok": False})
            return
        filename, source, _role = item
        try:
            data = (bytes(source) if isinstance(source, (bytes, bytearray))
                    else open(source, "rb").read())
        except OSError:
            bridge.broadcast({"type": "file_meta", "reqId": req, "ok": False})
            return
        bridge.broadcast({"type": "file_meta", "reqId": req, "ok": True,
                          "filename": filename})
        rid = str(req).encode()
        bridge.broadcast_binary(bytes([6, len(rid)]) + rid + data)
        return
    if kind == "file_push":
        # An upload is arriving for one of this process's endpoints: the FILE
        # bytes follow on the same socket; stash the meta until they land.
        pushes = getattr(bridge, "_pending_pushes", None)
        if pushes is None:
            pushes = bridge._pending_pushes = {}
        pushes[msg.get("reqId")] = msg
        return
    if kind == "presence":
        # The broker owns the roster; mirror it so canvas.viewers works the same
        # as under the embedded server. Keyed by viewer id (the embedded server
        # keys by socket, but canvas.viewers only reads .values(), and broker-
        # routed frames carry no socket). Any per-viewer cursor the broker
        # streams is merged in so canvas.viewers[i]["cursor"] survives a roster
        # refresh.
        prev = {v.get("id"): v for v in bridge._viewers.values()}
        own = getattr(bridge, "_own_viewer_id", None)
        merged = {}
        for v in (msg.get("viewers") or []):
            vid = v.get("id")
            if vid == own:
                continue                       # the host isn't its own viewer
            cur = prev.get(vid, {}).get("cursor")
            if cur is not None and "cursor" not in v:
                v = {**v, "cursor": cur}
            merged[vid] = v
        bridge._viewers = merged
        # Roster diff -> the on_connect/on_disconnect taps. The broker owns
        # the joins (there is no per-viewer socket on this side), so the
        # roster refresh is where a join/leave becomes visible; same off-loop
        # dispatch contract as the embedded server's connect path. Browsers
        # already on the canvas when this source dials in appear in the first
        # roster and fire on_connect too — they ARE new to this process.
        for vid in merged.keys() - prev.keys():
            if bridge._connect_taps:
                bridge._dispatch.submit(
                    lambda v=dict(merged[vid]): bridge._tap_connect(v))
        for vid in prev.keys() - merged.keys():
            if bridge._disconnect_taps:
                bridge._dispatch.submit(
                    lambda v=dict(prev[vid]): bridge._tap_disconnect(v))
        return
    if kind == "cursor":
        # High-rate pointer report for one viewer: fold it onto that viewer's
        # roster entry (canvas.viewers[i]["cursor"]).
        v = bridge._viewers.get(msg.get("id"))
        if v is not None:
            v["cursor"] = msg.get("pos") or msg.get("cursor")
        return
    if kind == "chat":
        # The broker relays + replays chat among browsers itself; deliver it to
        # any Python chat sinks (canvas.on_chat) too, so a canvas can react to
        # what viewers say the same way it does embedded.
        for sink in list(getattr(bridge, "_chat_sinks", [])):
            try:
                sink(msg)
            except Exception:
                import traceback as _tb
                _tb.print_exc()
        return
    if kind == "draw":
        # Free-form ink the broker relays among browsers is delivered here too,
        # so the canonical record set stays in step and canvas.on_draw observers
        # fire — the same fold + tap the embedded server does. (The broker owns
        # relay/replay; we only mirror state and notify, we don't re-broadcast.)
        bridge._apply_draw(msg.get("diff") or {})
        return
    if kind == "graveyard":
        # The broker routed a browser's delete of one of our objects (stripped to
        # our own id): graveyard it (restorable, off the live canvas) as embedded.
        cid = msg.get("id")
        if (bridge._components.get(cid) or bridge._shapes.get(cid)
                or bridge._arrows.get(cid)) is not None:
            bridge._graveyard(cid)
        return
    if kind == "restore":
        bridge._restore_object(msg.get("id"))
        return
    if kind in ("snapshot", "image"):
        # A browser answered canvas.save()/screenshot(): hand the reply to the
        # blocked waiter (correlated by reqId), exactly as the embedded server.
        waiter = bridge._snapshot_waiters.get(msg.get("reqId"))
        if waiter is not None:
            waiter["data"] = msg.get("data")
            if kind == "image":
                waiter["error"] = msg.get("error")
            waiter["event"].set()
        return
    if kind == "ui":
        # A native-UI request routed to the owner (the broker keeps hosting
        # actions to itself). Today that's the toolbar Inspector toggle, gated
        # by the same flag advertised in welcome — spawn/close it on the canvas.
        if (msg.get("action") == "toggle_inspector"
                and getattr(bridge, "_ui_inspector", False)
                and bridge._canvas is not None):
            try:
                bridge._canvas._toggle_ui_inspector(at=msg.get("center"))
            except Exception:
                import traceback as _tb
                _tb.print_exc()
        return
    comp = bridge._components.get(msg.get("id"))
    if comp is None:
        # A Python-managed shape (canvas.geo/text/line/…) moved/resized in the
        # browser: store the new geometry (it replays on reload) and relay it to
        # the other clients, exactly as the embedded server does — the owner
        # isn't a component, so it wouldn't be found above.
        if kind == "layout":
            shape = bridge._shapes.get(msg.get("id"))
            if shape is not None:
                bridge._apply_shape_layout(shape, dict(msg), None)
        return
    if kind == "input":
        payload = msg.get("payload") or {}
        bridge._dispatch.submit(
            lambda c=comp, p=payload: bridge._dispatch_input(c, p, None))
    elif kind == "layout":
        bridge._dispatch.submit(
            lambda c=comp, m=dict(msg): bridge._dispatch_layout(c, m, None))
    elif kind == "request":
        # A panel's canvas.request(data): the broker routed the browser's
        # request to us (the owner). Answer it on the dispatch machinery and
        # reply with a `response` correlated by reqId — the broker fans it back
        # to exactly the asker (the same pending-request routing a browser gets
        # from the embedded server). The broker already gated operability, so
        # dispatch straight to the handler; the requester's viewer identity
        # isn't carried over the hub, so on_request's second arg is empty.
        bridge._dispatch.submit(
            lambda c=comp, r=msg.get("reqId"), d=msg.get("data"):
            bridge._dispatch_request(c, r, d, None))
    elif kind == "set_props":
        # The shared property plane, hub-routed: the broker delivered another
        # peer's write on OUR panel (id already stripped). Apply through the
        # component's real setters — the same _apply_props the embedded server
        # used, so validation and the echoed broadcast are one code path. The
        # broker gated roles; over a hub the writer is authoritative, so only
        # a hard lock refuses (PROTOCOL.md § the shared property plane).
        if not getattr(comp, "locked", False):
            bridge._dispatch.submit(
                lambda c=comp, p=dict(msg.get("props") or {}):
                bridge._apply_props(c, p))


# -- serve(broker=True): the binary broker serves; this process is a source --

def _hub_binary(bridge, data):
    """Route a binary envelope from the hub: FILE completes a pending upload
    push; everything else is binary INPUT for this process's panels."""
    import json as _json
    import os as _os
    if len(data) >= 2 and data[0] == 6:                    # FILE
        req = bytes(data[2:2 + data[1]]).decode("utf-8", "replace")
        pushes = getattr(bridge, "_pending_pushes", {})
        push = pushes.pop(req, None)
        if push is None:
            return
        payload = bytes(data[2 + data[1]:])

        def _ack(ok, **extra):
            bridge.broadcast({"type": "file_ack", "reqId": req,
                              "ok": ok, **extra})

        comp = bridge.upload_component(push.get("token"))
        # Unknown endpoint (another source's) or role-gated: decline —
        # the hub can't verify per-endpoint roles, so fail closed.
        if comp is None or getattr(comp, "_role", None) is not None:
            _ack(False)
            return
        max_size = getattr(comp, "_max_size", None)
        if max_size and len(payload) > max_size:
            _ack(False, error="file too large")
            return
        filename = _os.path.basename(push.get("name") or "upload.bin")
        dest = getattr(comp, "_dest", None)
        info = {"name": filename, "size": len(payload),
                "content_type": push.get("content_type")
                or "application/octet-stream",
                "data": None if dest else payload, "path": None}
        if dest:
            from ._net import _safe_upload_path
            try:
                target = _safe_upload_path(dest, filename)
                with open(target, "wb") as f:
                    f.write(payload)
                info["path"] = target
                info["name"] = _os.path.basename(target)
            except Exception:
                _ack(False, error="write failed")
                return
        bridge.deliver_upload(comp, info, viewer=None)
        _ack(True, name=info["name"], size=info["size"])
        return
    bridge._on_binary_input(None, data)


def _find_danvasd():
    """Locate the danvasd binary, in priority order: ``$DANVASD`` (explicit
    override), the copy bundled in the installed wheel (``danvas/_bin/``), a
    local cargo build (dev checkout), then ``PATH``.

    The bundled path is why a platform wheel makes ``serve()`` broker-by-
    default with no download: pip picks the wheel for the user's OS, the
    binary rides inside it, and this finds it offline. A pure ``py3-none-any``
    install (other platforms) has no ``_bin/`` and falls through to the
    embedded server."""
    import shutil
    exe = "danvasd.exe" if os.name == "nt" else "danvasd"
    cand = os.environ.get("DANVASD")
    if cand and os.path.exists(cand):
        return cand
    bundled = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_bin", exe)
    if os.path.exists(bundled):
        return bundled
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for profile in ("release", "debug"):
        p = os.path.join(here, "broker", "target", profile, exe)
        if os.path.exists(p):
            return p
    return shutil.which("danvasd")


class _BrokerUnavailable(RuntimeError):
    """The broker binary is absent or won't launch. serve() is broker-only, so
    this propagates to the caller (there is no in-process fallback)."""


_STALE_HINT = (
    "A broker from an earlier session may be wedged on this port (common "
    "after a hard kill — e.g. WSL2 surviving a closed terminal). Fix: kill "
    "it and relaunch —\n"
    "    pkill -9 -f danvasd        (Windows: taskkill /f /im danvasd.exe)\n"
    "— or serve on a fresh port: canvas.serve(port=...)")


def _port_accepts(port, host="127.0.0.1", timeout=0.5):
    """Does anything at all accept TCP on ``port``? (Identity-blind — pair
    with :func:`_probe_hub` to tell a live hub from a squatter.)"""
    import socket as _socket
    try:
        _socket.create_connection((host, port), timeout=timeout).close()
        return True
    except OSError:
        return False


def _probe_hub(port, timeout=1.0, host="127.0.0.1"):
    """What is actually listening on ``port``? Returns ``"danvasd"`` (a live
    danvas hub — /__health__ answers, or /__templates__ on pre-0.6.5
    brokers), ``"other"`` (something responds to HTTP but isn't a danvas
    hub), or ``None`` (nothing answers HTTP: free port, a wedged broker
    whose accept loop is dead, or a non-HTTP service).

    A bare TCP connect can NOT stand in for this — a hung danvasd still
    accepts TCP, which is exactly how one stale broker used to poison
    every subsequent serve() with an undiagnosable websocket timeout.
    """
    import http.client
    for path in ("/__health__", "/__templates__"):
        try:
            conn = http.client.HTTPConnection(host, port, timeout=timeout)
            try:
                conn.request("GET", path)
                resp = conn.getresponse()
                body = resp.read(2048)
            finally:
                conn.close()
        except Exception:
            return None          # no HTTP response at all
        if resp.status == 200:
            if path == "/__health__":
                return "danvasd" if b"danvasd" in body else "other"
            return "danvasd"     # /__templates__ only exists on danvasd
        # non-200: keep probing the fallback path, then classify as other
    return "other"


class BrokerHandle:
    """The running broker behind serve(broker=True): stop() ends it."""

    def __init__(self, proc, client, tunnel=None):
        self.proc = proc
        self.client = client
        self.tunnel = tunnel

    def stop(self):
        def _end_proc():
            if not self.proc:
                return
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except Exception:
                self.proc.kill()

        for closer in (lambda: self.client.close(),
                       lambda: self.tunnel and self.tunnel.stop(),
                       _end_proc):
            try:
                closer()
            except Exception:
                pass


def serve_via_broker(canvas, port=8000, open_browser=True, block=True,
                     password=None, passwords=None, host="127.0.0.1",
                     existing_port=None, persist=False, desktop=False,
                     window_title="danvas", window_size=(1200, 800),
                     tunnel=False, tunnel_provider="cloudflared",
                     merge_server=None, self_url=None,
                     ui_inspector=False, ui_graveyard=False, cursors=False,
                     ui_hosting=None):
    """EXPERIMENTAL: serve this canvas THROUGH the danvasd binary.

    The broker owns the port (frontend, browsers, retention, ledger, merging);
    this Python process dials in as the ``host`` source — so it can crash and
    restart while the UI survives. The existing bridge is transplanted onto
    the socket (class-swap onto :class:`_RemoteBridge`), so components,
    handlers, and live setters work unchanged.

    Feature parity: panels/handlers/live setters, arrows, ink, media, auth,
    ``persist=``, tunnels, desktop, hot reload, the hosting button, native
    merging, background workers, ``view=``, ``debug=``, ``merge_server=``, the
    UI-affordance gating, ``on_request`` replies, presence/cursors →
    ``canvas.viewers``, managed-shape moves, chat sinks, ``on_draw``, the panel
    graveyard/restore, ``save()``/``screenshot()`` round-trips, and the toolbar
    Inspector all cross the hub — the broker is the universal serving path.
    """
    import subprocess
    import time as _time
    import webbrowser

    attached = False   # dialed a hub this call didn't spawn (survivor reuse)
    if existing_port is not None:
        # A broker is already running (the hot-reload monitor owns it) — dial
        # into it instead of spawning our own. The UI lives in that danvasd, so
        # this process restarting (an edit) never drops the browser: retention
        # holds the panels while we re-dial.
        port = existing_port
        proc = None
    else:
        binary = _find_danvasd()
        if binary is None:
            raise _BrokerUnavailable(
                "danvasd (the serving binary) was not found. Serving is "
                "broker-only -- there is no in-process Python server. Fix by "
                "one of:\n"
                "  - install a platform wheel that bundles it: pip install danvas\n"
                "  - point $DANVASD at a danvasd binary\n"
                "  - build it from a checkout: cargo build --release "
                "--manifest-path broker/Cargo.toml\n"
                "(a binary-less install can still danvas.connect() into a "
                "canvas that is already being served.)")
        cmd = [binary, "--port", str(port), "--host", str(host or "127.0.0.1")]
        if password:
            cmd += ["--password", str(password)]
        if merge_server:
            # serve(merge_server=): the broker advertises it in welcome so the
            # UI shows a "Merge…" button; self_url is the address that server
            # dials back to reach this canvas.
            cmd += ["--merge-server", str(merge_server)]
            if self_url:
                cmd += ["--self-url", str(self_url)]
        # UI-affordance gating (Inspector/graveyard/cursor reporting/hosting
        # button): the owner resolved these against the bind; pass the decision
        # so the broker's welcome matches the embedded server's gating.
        _b = lambda v: "1" if v else "0"
        cmd += ["--ui-inspector", _b(ui_inspector),
                "--ui-graveyard", _b(ui_graveyard),
                "--cursors", _b(cursors)]
        if ui_hosting is not None:
            cmd += ["--ui-hosting", _b(ui_hosting)]
        env = dict(os.environ)
        if passwords:
            # Role logins ride the env contract both hubs share.
            env["DANVAS_ROLE_PASSWORDS"] = ",".join(
                f"{r}={pw}" for r, pw in passwords.items())
        # Owned spawn: the broker dies with this process no matter HOW this
        # process dies (atexit + a Windows kill-on-close job / Linux
        # PDEATHSIG) — an IDE stop button or a Ctrl+C landing in user code
        # must not leave a stray danvasd holding the port.
        import collections
        import subprocess as _subprocess
        import sys as _sys
        import threading as _threading
        from ._procown import spawn_owned
        # stderr is piped and teed: the user still sees danvasd's own words
        # live on this console, AND a startup failure can quote them in the
        # exception (a bind-conflict line is the definitive "port taken"
        # signal — port probing can lie, e.g. a squatter whose backlog our
        # own probes filled stops accepting).
        proc = spawn_owned(cmd, env=env, stderr=_subprocess.PIPE,
                           text=True, encoding="utf-8", errors="replace")
        err_tail = collections.deque(maxlen=20)

        def _drain():
            pipe = getattr(proc, "stderr", None)   # absent on test fakes
            for line in pipe or ():
                err_tail.append(line.rstrip())
                _sys.stderr.write(line)
        _threading.Thread(target=_drain, daemon=True).start()

        def _bind_conflict():
            tail = " ".join(err_tail)
            return any(m in tail for m in (
                "os error 10048", "os error 98", "Address already in use",
                "already in use", "each socket address"))

        # Readiness = the hub ANSWERS (an HTTP round-trip on its own routes),
        # never just TCP accept: a stale broker from a dead session still
        # accepts TCP, so the old bare-connect probe waved through wedged
        # brokers and every serve() after one hard kill timed out dialing a
        # hub that would never complete a websocket handshake.
        deadline = _time.time() + 15
        while _time.time() < deadline:
            if proc.poll() is not None:
                # Our spawn is gone: the port is held by someone else
                # (danvasd binds strictly and exits) or the binary won't run.
                _time.sleep(0.2)   # let the stderr drain catch its last words
                kind = _probe_hub(port)
                if kind == "danvasd":
                    # A live hub from a previous session still owns the port.
                    # Attaching IS the designed restart path (the UI survives
                    # the owner dying); the dial-in below re-registers this
                    # canvas and retention swaps the panels over.
                    print(f"[danvas] attaching to the danvasd already on "
                          f"port {port} (previous session's hub)")
                    proc = None
                    attached = True
                    break
                said = ("\ndanvasd said: " + " | ".join(err_tail)
                        if err_tail else "")
                if kind == "other":
                    raise _BrokerUnavailable(
                        f"danvasd exited on startup (code {proc.returncode}): "
                        f"port {port} is already taken by something that is "
                        "not a danvas hub. Pick another port "
                        f"(canvas.serve(port=...)) or stop that process."
                        f"{said}")
                if _bind_conflict() or _port_accepts(port):
                    raise _BrokerUnavailable(
                        f"danvasd exited on startup (code {proc.returncode}): "
                        f"port {port} is already in use by something that "
                        f"never answers as a danvas hub.\n{_STALE_HINT}"
                        f"{said}")
                # Port looks free yet danvasd died: wrong arch, corrupt
                # binary, missing lib.
                raise _BrokerUnavailable(
                    f"danvasd exited on startup (code {proc.returncode})."
                    f"{said}")
            if _probe_hub(port, timeout=0.5) == "danvasd":
                # A survivor answers exactly like our own spawn while our
                # doomed bind is still in flight — give the spawn a beat to
                # lose the race so the exit branch above classifies it.
                _time.sleep(0.2)
                if proc.poll() is not None:
                    continue
                break
            _time.sleep(0.1)
        else:
            proc.terminate()
            raise _BrokerUnavailable(
                f"danvasd never became ready on port {port} within 15s")

    bridge = canvas._bridge
    login = password or (next(iter(passwords.values())) if passwords else None)
    client = SourceClient(f"127.0.0.1:{port}", label="host", password=login)
    # Transplant: the existing bridge (components already bound to it) becomes
    # socket-backed in place — every outbound path now rides the dial-in.
    bridge.__class__ = _RemoteBridge
    bridge._client = client

    def _replay():
        for comp in bridge._components.values():
            yield bridge.register_message(comp)
            state = comp.state_payload()
            if state:
                yield {"type": "update", "id": comp.id, "payload": state}
        for arrow in canvas._arrows:
            yield arrow.register_message()

    client._replay_hook = _replay
    client.on_frame(lambda m: _dispatch_hub_frame(bridge, m))
    client._binary_hook = lambda data: _hub_binary(bridge, data)
    # persist= is an OWNER-side feature (the owner serialises its own canvas
    # state) — it works the same through the broker. Restore BEFORE connecting
    # so the saved layout/values ride the initial replay (no default->restored
    # flicker); the autosave arms on hub-routed layout/input the same way.
    # (Free-form drawings are hub-native, so ink doesn't round-trip through the
    # broker — layout + input values do, which is the bulk of persist's value.)
    if persist:
        try:
            canvas._persist_setup(persist)
        except Exception:
            import traceback as _tb
            _tb.print_exc()
    # The client's socket loop keeps redialing in the background; connect()
    # only decides how long to WAIT for the first success. One 10s wait used
    # to be the single roll of the dice — retry within a budget, and when it
    # still fails say what's actually wrong (a hub that accepts TCP but never
    # completes the websocket handshake is a stale broker, not a network
    # blip) instead of a bare timeout.
    budget = float(os.environ.get("DANVAS_CONNECT_TIMEOUT", 20))
    connect_deadline = _time.time() + budget
    while True:
        try:
            client.connect(timeout=min(7.0, max(
                1.0, connect_deadline - _time.time())))
            break
        except TimeoutError:
            if _time.time() < connect_deadline:
                continue
            # Diagnose FIRST, tear down after: probing a hub we just
            # terminated can only ever report a corpse (the old order
            # misdiagnosed every failure as "hub stopped responding").
            state = _probe_hub(port)
            client.close()
            if proc is not None:
                proc.terminate()
            if state == "danvasd":
                detail = (
                    "the hub is alive and answering HTTP, but this "
                    "process never completed the websocket dial-in. "
                    "Likely causes: a password mismatch; a loopback "
                    "firewall; or a CPU-bound thread started before "
                    "serve() starving the handshake thread (the dial-in "
                    "runs on a background thread this process must get "
                    "to schedule)")
            elif state is None and _port_accepts(port):
                detail = ("something on the port accepts connections but "
                          f"never answers.\n{_STALE_HINT}")
            else:
                detail = "the hub stopped responding while connecting"
            raise TimeoutError(
                f"could not reach the hub at ws://127.0.0.1:{port}/ws "
                f"within {budget:g}s (override with $DANVAS_CONNECT_TIMEOUT)"
                f": {detail}") from None
    canvas._serving = True
    # Background producer loops (a camera feed, a sensor stream) run in the
    # serving process — their frames now ride the dial-in through the
    # transplanted bridge, exactly as they'd ride the embedded server.
    canvas._start_background()
    # serve(view=...): the host source folds its view delta into the broker's
    # hub view, which the broker bakes into every browser's welcome (the same
    # `view` frame a source sends over the wire — see the conformance contract).
    view = getattr(canvas._bridge, "_view", None)
    if view:
        client._send({"type": "view", "view": view})
    if existing_port is not None or attached:
        # An already-running broker (the hot-reload monitor's, or a previous
        # session's survivor we attached to) was spawned without knowing this
        # serve()'s kwargs — deliver the resolved UI-affordance gating now,
        # before browsers typically connect.
        config = {"type": "serve_config", "uiInspector": bool(ui_inspector),
                  "uiGraveyard": bool(ui_graveyard), "cursors": bool(cursors)}
        if ui_hosting is not None:
            config["uiHosting"] = bool(ui_hosting)
        client._send(config)
    # Pre-serve canvas.merge(url) calls: the broker owns merging, so replay them
    # as merge frames instead of standing up the Python merge host. A known
    # password uses merge_auth (dials with the session cookie); otherwise
    # merge_add composes an open target.
    pending = getattr(canvas, "_pending_merges", None)
    if pending:
        for url, pw, _offset in pending:
            frame = {"type": "merge_auth" if pw else "merge_add", "uri": url}
            if pw:
                frame["password"] = pw
            client._send(frame)
        canvas._pending_merges = []
    tunnel_handle = None
    if tunnel:
        # A tunnel is a Python-side concern (pycloudflared) — point it at the
        # broker's port. By default the tunnel lives in a detached KEEPER
        # process, so the public URL is stable across script restarts (a
        # quick tunnel mints a new URL per process — owning it here meant
        # every code iteration invalidated the link you'd shared). Pass
        # tunnel="ephemeral" for the old die-with-this-script behavior.
        from .tunnel import ensure_tunnel, open_tunnel
        try:
            if tunnel == "ephemeral":
                tunnel_handle = open_tunnel(port, provider=tunnel_provider)
                extra = ""
            else:
                tunnel_handle = ensure_tunnel(port, provider=tunnel_provider)
                extra = ("   (stable across restarts; stop with: python -m "
                         f"danvas.tunnel --stop --port {port})")
            # flush: THE line people tail in detached logs/CI — it must not
            # sit in Python's block-buffered stdout while the app runs.
            print(f"[danvas] public URL: {tunnel_handle.url}"
                  "   <- share this; served by danvasd behind it" + extra,
                  flush=True)
        except Exception as exc:  # noqa: BLE001
            warnings.warn(f"tunnel failed to start ({exc}); serving locally")
    canvas._broker = BrokerHandle(proc, client, tunnel_handle)
    url = f"http://127.0.0.1:{port}"
    if proc is not None:
        print(f"[danvas] serving via danvasd at {url}"
              f"  (broker pid {proc.pid}; UI survives this process)",
              flush=True)

    if desktop:
        # Native window instead of a browser — the same webview.create_window
        # the embedded path uses, just pointed at the broker's URL. pywebview
        # must drive on the main thread (the dial-in socket is a bg thread, so
        # that's fine); the window blocks until closed, then we stop the broker.
        try:
            import webview
        except ImportError:
            warnings.warn("pywebview is not installed; opening in the browser "
                          "instead. Install: pip install 'danvas[desktop]'")
            webbrowser.open(url)
        else:
            try:
                w, h = window_size
                webview.create_window(window_title, url,
                                      width=int(w), height=int(h))
                webview.start()
            finally:
                canvas._broker.stop()
            return canvas

    if open_browser:
        webbrowser.open(url)
    if not block:
        return canvas
    try:
        while True:
            _time.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        canvas._broker.stop()
    return canvas


def connect(url, label="python", password=None, timeout=10.0):
    """Join a running canvas as a peer process: ``danvas.connect(url)``.

    Returns a connected :class:`RemoteCanvas` — the normal danvas API, with the
    frames going to the serving canvas instead of to browsers of this
    process's own. (On the returned canvas, ``connect(a, b)`` keeps its normal
    danvas meaning: an arrow between two panels.)
    """
    return RemoteCanvas(url, label=label, password=password).dial(timeout)
