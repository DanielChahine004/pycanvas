"""Dial-in source client: put panels on a running canvas from *outside* it.

The reference implementation of the protocol's dial-in role (PROTOCOL.md §
dial-in sources) — deliberately small, because it is also the executable spec
for what a Rust/C++/MATLAB SDK implements: a plain WebSocket **client** that
connects to a serving canvas (any ``canvas.serve()`` is a hub by default),
registers panels, streams updates, and receives the interactions browsers make
on them. No server, no framework — a process that can open a socket can be on
the canvas::

    from danvas import SourceClient

    src = SourceClient("127.0.0.1:8000", label="telemetry")
    src.connect()
    src.register("temp", "Slider", props={"min": 0, "max": 100, "value": 20},
                 x=40, y=40, w=320, h=90)

    @src.on_input("temp")
    def moved(payload):
        print("browser set", payload)

    while True:
        src.update("temp", value=read_sensor())
        time.sleep(0.5)

Identity is the ``label``: reconnecting under the same label replaces the
previous life's panels (the hub tears the stale ones down and this client
re-replays its registers). A password-protected canvas takes ``password=`` —
the client runs the same ``/__auth__`` flow a browser does and connects with
the session cookie. The connection also *receives* the hub's full state
(register/update frames for every panel the login role may see) — tap it with
:meth:`on_frame` to observe the canvas you joined.

Threading: the socket runs on a background asyncio loop; ``register`` /
``update`` / ``remove`` are thread-safe and non-blocking. Handlers run on one
ordered dispatch thread (a slow handler delays later events, not the socket).
"""

import asyncio
import json
import queue
import threading
import traceback

from ._dialin import _authenticate, _parse_source

_HEARTBEAT_S = 10.0     # the hub reaps connections silent for ~30s
_RECONNECT_S = 1.0

_TEMPLATES = None


def _templates():
    """The language-neutral built-in panel templates (lazy-loaded)."""
    global _TEMPLATES
    if _TEMPLATES is None:
        import os
        path = os.path.join(os.path.dirname(__file__),
                            "templates", "components.json")
        with open(path, encoding="utf-8") as f:
            _TEMPLATES = json.load(f)["templates"]
    return _TEMPLATES


class SourceClient:
    """One dial-in connection to a hub, owning the panels it registers."""

    def __init__(self, url, label="source", password=None):
        ws_uri, http_parts, _lbl = _parse_source(url)
        self._http_parts = http_parts
        self._password = password
        self.label = label
        # ?source=1 marks the connection as a dial-in source to the hub;
        # vname labels it in the viewer roster.
        sep = "&" if "?" in ws_uri else "?"
        self._uri = f"{ws_uri}{sep}source=1&label={label}&vname={label}"
        # Everything this source has declared, for replay on (re)connect —
        # the client-side twin of the hub's upstream cache.
        self._registers = {}      # cid -> register msg
        self._updates = {}        # cid -> accumulated update payload
        self._input_handlers = {} # cid -> [fn]
        self._layout_handlers = {}# cid -> [fn]
        self._frame_taps = []
        self._subs = set()        # panel ids we subscribed to (re-sent on reconnect)
        # Optional replacement for the built-in replay: a callable yielding the
        # frames that reconstruct this source on a fresh connection (used by
        # RemoteCanvas, whose components — not this cache — hold the truth).
        self._replay_hook = None
        # Optional receiver for inbound BINARY envelopes (browser->owner INPUT
        # routed by the hub); RemoteCanvas wires it into the bridge dispatch.
        self._binary_hook = None
        # Local mirror of the canvas we joined: id -> {"component", "props",
        # "state", ...geometry} folded from the hub's register/update stream.
        # Eventually consistent (updated on the dispatch thread) — the replica
        # every participant holds in the shared-document model.
        self.panels = {}
        self._loop = None
        self._thread = None
        self._sock = None
        self._connected = threading.Event()
        self._closing = False
        # Ordered handler dispatch off the socket loop, like the Canvas's own
        # dispatch thread: a slow handler must never stall the wire.
        self._events = queue.Queue()
        self._dispatcher = threading.Thread(target=self._dispatch_loop, daemon=True)

    # -- public API -----------------------------------------------------------
    def connect(self, timeout=10.0):
        """Dial the hub (background thread; auto-reconnects until :meth:`close`).

        Returns once the first connection is up, or raises ``TimeoutError``.
        """
        if self._thread is None:
            self._dispatcher.start()
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
        # Wait even on a repeat call: the socket loop redials in the
        # background, so calling connect() again means "give it more time",
        # not a no-op — that's what lets a caller retry within a budget.
        if not self._connected.wait(timeout):
            raise TimeoutError(f"could not reach the hub at {self._uri}")
        return self

    def register(self, cid, component, props=None, **place):
        """Declare a panel: ``component`` is the panel type name the frontend
        mounts (``"Slider"``, ``"Label"``, ``"React"``, …), ``props`` its
        constructor payload, ``**place`` any placement keys (x/y/w/h/...)."""
        msg = {"type": "register", "id": cid, "component": component,
               "props": dict(props or {})}
        msg.update({k: v for k, v in place.items() if v is not None})
        self._registers[cid] = msg
        self._updates.pop(cid, None)   # a re-register resets accumulated state
        self._send(msg)
        return self

    def register_template(self, cid, kind, name=None, x=None, y=None,
                          w=None, h=None, rel=None, **data):
        """Register a NATIVE built-in panel from the language-neutral template
        asset (``danvas/templates/components.json``): ``kind`` is one of
        slider/label/button/toggle/text_field/markdown; ``**data`` overrides
        its data defaults (min/max/value/text/options/...). This is the move a
        non-Python SDK makes to put a real, rendering slider on the canvas —
        the template carries the React source the frontend mounts::

            src.register_template("temp", "slider", min=0, max=100, value=20,
                                  x=40, y=40)
        """
        tpl = _templates()[kind]
        props = dict(tpl["props"])
        blob = dict(tpl["data"])
        blob.update(data)
        props["data"] = json.dumps(blob)
        props["label"] = name or cid
        if w is not None:
            props["w"] = w
        if h is not None:
            props["h"] = h
        # rel rides the register frame itself (PROTOCOL.md § relative
        # placement): {"kind": "below"|..., "anchor": <cid>, "gap": px} — a
        # rel-aware hub's frontend places and cascades it.
        return self.register(cid, tpl["component"], props=props,
                             name=name or cid, x=x, y=y, rel=rel)

    def update(self, cid, **payload):
        """Stream new state for a registered panel."""
        self._updates.setdefault(cid, {}).update(payload)
        self._send({"type": "update", "id": cid, "payload": payload})
        return self

    def remove(self, cid):
        """Withdraw a panel."""
        self._registers.pop(cid, None)
        self._updates.pop(cid, None)
        self._send({"type": "remove", "id": cid})
        return self

    def on_input(self, cid, fn=None):
        """Handle a browser operating this source's panel ``cid``:
        ``fn(payload)``. Usable as a decorator (``@src.on_input("temp")``)."""
        if fn is None:
            return lambda f: self.on_input(cid, f)
        self._input_handlers.setdefault(cid, []).append(fn)
        return fn

    def on_layout(self, cid, fn=None):
        """Handle a browser moving/resizing panel ``cid``: ``fn(msg)`` with the
        raw layout frame (x/y/w/h/rotation as present)."""
        if fn is None:
            return lambda f: self.on_layout(cid, f)
        self._layout_handlers.setdefault(cid, []).append(fn)
        return fn

    def set_props(self, cid, **props):
        """Write properties of ANY panel on the canvas — including panels other
        processes own (``cid`` as it appears in :attr:`panels`). The write
        applies at the panel's owner through its real setters (last-writer-wins
        on races); a hard-locked panel refuses. Placement keys (x/y/w/h/
        rotation/opacity) work too."""
        self._send({"type": "set_props", "id": cid, "props": dict(props)})
        return self

    def subscribe(self, cid, fn=None):
        """Receive ``cid``'s input events even though another process owns it
        — this is how "the click runs on *this* process" works: the owner's
        handlers are untouched; you react in parallel. Optionally pass/decorate
        a handler (sugar for ``subscribe(cid)`` + ``on_input(cid, fn)``)."""
        self._subs.add(cid)
        self._send({"type": "subscribe", "id": cid})
        if fn is not None:
            self.on_input(cid, fn)
            return fn
        return lambda f: self.on_input(cid, f)

    def unsubscribe(self, cid):
        self._subs.discard(cid)
        self._send({"type": "unsubscribe", "id": cid})
        return self

    def find(self, name):
        """Resolve a panel's wire id from its Python-side ``name=`` (register
        frames carry it), or ``None``. If two sources used the same name, the
        first registered wins — qualify names across processes if that bites."""
        for cid, entry in self.panels.items():
            if entry.get("name") == name:
                return cid
        return None

    def on_frame(self, fn):
        """Tap every frame the hub sends this connection — including the hub
        canvas's own register/update stream, i.e. read access to the canvas."""
        self._frame_taps.append(fn)
        return fn

    def close(self):
        """Stop reconnecting and drop the connection (the hub then applies its
        offline policy — retain-freeze by default)."""
        self._closing = True
        loop, sock = self._loop, self._sock
        if loop is not None and sock is not None:
            asyncio.run_coroutine_threadsafe(sock.close(), loop)

    # -- wire (background loop) -----------------------------------------------
    def _send(self, msg):
        loop, sock = self._loop, self._sock
        if loop is None or sock is None:
            return  # not connected yet — replay on connect covers it
        asyncio.run_coroutine_threadsafe(sock.send(json.dumps(msg)), loop)

    def _send_binary(self, data):
        """Ship one binary envelope up (media frames; not replayed — streams
        are transient by the protocol's own rule)."""
        loop, sock = self._loop, self._sock
        if loop is None or sock is None:
            return
        asyncio.run_coroutine_threadsafe(sock.send(bytes(data)), loop)

    def _run(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._session_loop())
        finally:
            self._loop.close()

    async def _session_loop(self):
        from websockets.asyncio.client import connect as ws_connect
        while not self._closing:
            try:
                headers = None
                if self._password is not None:
                    cookie = await asyncio.get_event_loop().run_in_executor(
                        None, _authenticate, self._http_parts, self._password)
                    if not cookie:
                        raise ConnectionError("hub rejected the password")
                    headers = {"Cookie": f"pc_session={cookie}"}
                async with ws_connect(self._uri, max_size=None,
                                      additional_headers=headers) as sock:
                    self._sock = sock
                    await self._replay(sock)
                    self._connected.set()
                    heart = asyncio.ensure_future(self._heartbeat(sock))
                    try:
                        async for raw in sock:
                            self._on_raw(raw)
                    finally:
                        heart.cancel()
            except asyncio.CancelledError:
                raise
            except Exception:
                if self._closing:
                    break
            finally:
                self._sock = None
            if not self._closing:
                await asyncio.sleep(_RECONNECT_S)

    async def _replay(self, sock):
        """(Re)declare everything on a fresh connection — the source-side twin
        of the hub's replay-on-connect: registers first, then accumulated
        state, so a hub restart heals without user code doing anything."""
        if self._replay_hook is not None:
            for msg in self._replay_hook():
                await sock.send(json.dumps(msg))
        else:
            for msg in self._registers.values():
                await sock.send(json.dumps(msg))
            for cid, payload in self._updates.items():
                await sock.send(json.dumps(
                    {"type": "update", "id": cid, "payload": payload}))
        for cid in self._subs:
            await sock.send(json.dumps({"type": "subscribe", "id": cid}))

    async def _heartbeat(self, sock):
        while True:
            await asyncio.sleep(_HEARTBEAT_S)
            await sock.send(json.dumps({"type": "heartbeat"}))

    # -- inbound --------------------------------------------------------------
    def _on_raw(self, raw):
        if isinstance(raw, (bytes, bytearray)):
            self._events.put(bytes(raw))   # binary envelope -> hook, in order
            return
        try:
            msg = json.loads(raw)
        except (ValueError, TypeError):
            return
        self._events.put(msg)

    def _dispatch_loop(self):
        while True:
            msg = self._events.get()
            try:
                if isinstance(msg, bytes):
                    if self._binary_hook is not None:
                        self._binary_hook(msg)
                else:
                    self._handle(msg)
            except Exception:
                traceback.print_exc()

    def _handle(self, msg):
        """Route one hub frame: interactions on our panels to their handlers,
        everything to the frame taps. Split out for direct unit testing."""
        for tap in list(self._frame_taps):
            try:
                tap(msg)
            except Exception:
                traceback.print_exc()
        kind = msg.get("type")
        cid = msg.get("id")
        if kind == "input":
            for fn in self._input_handlers.get(cid, []):
                fn(msg.get("payload"))
        elif kind == "layout":
            for fn in self._layout_handlers.get(cid, []):
                fn(msg)
        elif kind == "register":
            # Fold the hub's canvas stream into the local mirror.
            self.panels[cid] = {
                "component": msg.get("component"),
                "name": msg.get("name"),
                "owner": msg.get("owner"),
                "props": dict(msg.get("props") or {}),
                "state": {},
                **{k: msg[k] for k in ("x", "y", "w", "h") if k in msg},
            }
        elif kind == "update":
            entry = self.panels.get(cid)
            if entry is not None and isinstance(msg.get("payload"), dict):
                entry["state"].update(msg["payload"])
        elif kind == "remove":
            self.panels.pop(cid, None)
