"""Bidirectional state sync between Python components and the browser.

A single WebSocket connection carries all components, multiplexed by id.
``broadcast`` is thread-safe: user threads call ``component.update(...)`` which
schedules the actual send onto the server's asyncio event loop.
"""

import asyncio
import copy
import inspect
import itertools
import json
import logging
import math
import os
import random
import re
import secrets
import sys
import threading
import time
import traceback
import uuid
import warnings

_log = logging.getLogger("danvas")
from collections import deque

try:
    from fastapi import WebSocketDisconnect
except ImportError:
    # The server stack (danvas[hub]) isn't installed — this is a light
    # client/broker install. WebSocketDisconnect is only used by the embedded
    # WebSocket handler below, which such an install never reaches (it dials
    # into danvasd instead of running the FastAPI app), so a stand-in that keeps
    # the name defined is enough.
    class WebSocketDisconnect(Exception):  # noqa: N818
        pass

from ._flags import LAYOUT_FLAGS
from ._protocol import BINARY_FRAME_CODES, PROTOCOL_VERSION
from .kernel import AsyncKernel, Kernel, spawn

# JSON codec for the wire. orjson, when installed, encodes our frames ~10x
# faster than stdlib json (it's the dominant per-broadcast CPU cost on the
# single event-loop thread, so it directly caps fan-out rate); we fall back to
# stdlib transparently when it isn't, so it's a free speedup that asks nothing
# of the user. ``OPT_NON_STR_KEYS`` matches json.dumps' coercion of int/float
# dict keys to strings (a payload like ``{1: ...}`` doesn't raise); orjson also
# serialises NaN/Infinity as ``null``, which is what the browser's JSON.parse
# requires anyway (stdlib json emits bare ``NaN``, which JSON.parse rejects), so
# the swap is strictly safer on that edge, not just faster. orjson returns
# bytes; we decode once for ``send_text`` (still ~10x ahead, decode included).
try:
    import orjson as _orjson

    def _dumps(obj):
        return _orjson.dumps(obj, option=_orjson.OPT_NON_STR_KEYS).decode()

    def _loads(raw):
        return _orjson.loads(raw)
except ImportError:  # pragma: no cover - exercised only without orjson installed
    def _dumps(obj):
        return json.dumps(obj)

    def _loads(raw):
        return json.loads(raw)


# Tokens that mark a mobile/tablet browser in the User-Agent string. Used only
# to classify a viewer as "mobile" vs "desktop" for layout adaptation — it's a
# best-effort, client-reported, spoofable signal (and iPadOS reports a desktop
# UA), so it's presentation-only, never an authorization input.
_MOBILE_UA_RE = re.compile(
    r"Mobi|Android|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini|"
    r"Windows Phone|webOS",
    re.I,
)


def _device_from_ua(user_agent):
    """Classify a User-Agent as ``"mobile"`` or ``"desktop"`` (best effort)."""
    if user_agent and _MOBILE_UA_RE.search(user_agent):
        return "mobile"
    return "desktop"


def _diag(msg):
    """Print a server-thread diagnostic line without tripping the Jupyter kernel.

    These lines (viewer connected / disconnected) fire from the asyncio server
    thread, not from a cell. Inside an ipykernel, ``sys.stdout`` is an
    ``OutStream`` that tags every write with the *currently executing* cell's
    parent header and ships it over iopub; a write from a background thread
    therefore gets attributed to whatever cell ran last. During "Run All" that
    cell has already finished, so VS Code's Jupyter extension tries to attach
    the output to a disposed execution and throws "notebook controller is
    DISPOSED", killing every remaining queued cell. Writing to the original
    ``sys.__stdout__`` (the kernel's real fd, surfaced in the kernel log) skips
    the per-cell redirection. In a plain script ``__stdout__ is stdout``, so
    behaviour there is unchanged.
    """
    stream = sys.__stdout__
    if stream is None:  # e.g. pythonw / detached stdout — nothing to write to
        return
    try:
        stream.write(msg + "\n")
        stream.flush()
    except (ValueError, OSError):
        pass  # stream closed mid-shutdown; a diagnostic line isn't worth raising

# Friendly auto-generated identities for connecting viewers (editable in the UI).
_VIEWER_ANIMALS = ["Fox", "Owl", "Bear", "Wolf", "Hawk", "Lynx", "Otter",
                   "Crane", "Seal", "Moth", "Newt", "Wren", "Stoat", "Vole"]
_VIEWER_COLORS = ["#ef4444", "#f59e0b", "#10b981", "#3b82f6", "#8b5cf6",
                  "#ec4899", "#14b8a6", "#f97316"]

# An idle browser sends a heartbeat every ~10s (see frontend bridge.js). A
# connection silent for longer than this is treated as dead and reaped, so the
# viewer count can't stay inflated by a hard-dropped tab (the WS keepalive ping
# is disabled server-side; see server.py).
_HEARTBEAT_TIMEOUT = float(os.environ.get("DANVAS_HEARTBEAT_TIMEOUT", "30"))
_REAP_INTERVAL = min(10.0, max(0.5, _HEARTBEAT_TIMEOUT / 3))

# Binary-frame type codes. High-rate media rides a binary WebSocket frame instead
# of base64-in-JSON: a 2-byte header (``[type][id-length]``) plus the id, then the
# raw payload, so the browser feeds bytes straight into a Blob/ArrayBuffer with no
# base64 decode or JSON parse. Control messages (register/update/layout/chat/...)
# stay JSON: they're low-rate and self-describing, so binary would cost
# readability for no real throughput. The codes are sourced from the canonical
# danvas/_protocol.py (the same definition the frontend's protocol.generated.js
# is rendered from), so the two sides can't drift.
BINARY_VIDEO = BINARY_FRAME_CODES["VIDEO"]   # JPEG-encoded frame bytes
BINARY_AUDIO = BINARY_FRAME_CODES["AUDIO"]   # little-endian int16 PCM (interleaved)
BINARY_CUSTOM = BINARY_FRAME_CODES["CUSTOM"]  # opaque -> Custom.push_binary -> canvas.onPush
BINARY_REACT = BINARY_FRAME_CODES["REACT"]   # opaque -> React.push_binary -> canvas.onFrame
BINARY_INPUT = BINARY_FRAME_CODES["INPUT"]   # browser -> Python (canvas.sendBinary -> @on_binary)
BINARY_FILE = BINARY_FRAME_CODES["FILE"]     # hub <-> owner file transfer (id = reqId)


def encode_binary_frame(type_code, comp_id, payload):
    """Pack a component binary frame: ``[type][idLen][id bytes][payload]``.

    ``comp_id`` is ascii-safe (component ids are code-defined names/uuids) and
    capped at 255 bytes so its length fits one header byte. ``payload`` is raw
    ``bytes`` (e.g. JPEG-encoded frame data).
    """
    cid = comp_id.encode("utf-8")[:255]
    return bytes((type_code, len(cid))) + cid + payload


class Bridge:
    def __init__(self):
        self._components = {}  # id -> BaseComponent
        self._arrows = {}  # id -> Arrow
        self._shapes = {}   # id -> BaseShape (geo/text/note/draw/line/frame/highlight)
        # Identity of this server *run*. Component ids are minted fresh every
        # run, so a browser whose socket reconnects to a new run (re-running the
        # script, a crash, a hot reload) still shows the previous run's panels —
        # stacked exactly on top of the new ones, dead to input. The run id rides
        # the welcome frame; the frontend clears every managed shape when it sees
        # the id change, so a stale page heals itself on reconnect.
        self._run_id = uuid.uuid4().hex[:8]
        # Wire observers (canvas.on_frame / serve(debug=True)): each is called
        # as fn(direction, msg) with direction "out" (Python -> browser) or "in"
        # (browser -> Python) for every JSON frame and a summary of every binary
        # frame. ``_tap_guard`` makes taps reentrancy-safe: anything a tap itself
        # sends (e.g. updating a debug panel) is not re-tapped, so a tap that
        # drives a component can't recurse.
        self._frame_taps = []
        # Observers of handler dispatch (canvas.on_dispatch): fired as each input/
        # layout handler is queued, starts, and finishes, so a tap can render a
        # live execution trace. Empty by default — instrumentation in
        # _dispatch_callbacks is fully skipped when no tap is registered, so an
        # untapped canvas pays nothing. ``_trace_ids`` hands each browser action a
        # correlation id shared by all the handlers it fans out to (next() on an
        # itertools.count is atomic, so threaded handlers can read it safely).
        self._dispatch_taps = []
        self._trace_ids = itertools.count(1)
        # When True, dispatch tracing also follows each handler *into* the user's
        # own functions (canvas.trace_calls), via a sys.setprofile probe — off by
        # default because that probe costs more than the shallow handler trace.
        self._trace_deep = False
        # Always-on history: once serving, every handler dispatch is recorded into
        # a bounded ring of the most recent actions (newest evicts oldest), so a
        # trace panel opened later — or canvas.trace_history() — can show what
        # already happened. Armed by serve(); shallow (handler-level) unless deep
        # tracing is also on. _trace_history maps trace id -> assembled action.
        self._trace_recording = False
        self._trace_history = {}
        self._trace_history_limit = 50
        self._trace_lock = threading.Lock()
        # Observers of viewer cursor moves (canvas.on_cursor). Kept separate from
        # frame taps: cursors are high-rate and intentionally off the wire-tap
        # path, so they neither flood debug logs nor pay the frame-tap guard.
        self._cursor_taps = []
        # Observers of viewer connections (canvas.on_connect). Fired once per
        # join with the viewer dict, off the event loop, so a handler can adapt
        # the canvas to that viewer (e.g. a mobile layout via set_layout(
        # client_id=...)) without blocking the connect path.
        self._connect_taps = []
        # Observers of viewer disconnections (canvas.on_disconnect) — the
        # symmetric twin of _connect_taps, fired once per leave with the
        # departed viewer's last-known dict.
        self._disconnect_taps = []
        # Observers of ephemeral drawing changes (canvas.on_draw).  Each is
        # called off the event loop with a dict {added, updated, removed}.
        self._draw_taps = []
        self._tap_guard = threading.local()
        self._connections = set()
        # Connections that asked (via ?proxy=1) not to be excluded from a change's
        # own echo. A normal browser is the "mover" of its own input and already
        # applied it locally, so it's excluded from the state echo; a merge server
        # connects as a proxy for many browsers, so it NEEDS the echo (to keep its
        # replay cache current and fan the change to its other viewers).
        self._proxy_conns = set()
        # The label register frames carry as "owner" — who runs this panel's
        # handlers. "host" for a serving canvas; a RemoteCanvas overrides it
        # with its dial-in label.
        self._owner_label = "host"
        # Live hosting controls (the 🌐 button): whether the button is shown
        # (defaults on only for a private local bind, like the Inspector), and
        # the current exposure state broadcast to viewers. ``_app`` is the
        # FastAPI app (set by server.create_app) so a LAN listener can be added
        # to the SAME app live; the handles keep the extra listener/tunnel
        # stoppable and idempotent.
        self._ui_hosting = False
        self._hosting = {"host": None, "port": None, "lan": None,
                         "tunnel": None, "busy": None, "error": None}
        self._app = None
        self._lan_server = None
        self._tunnel_handle = None
        # Dial-in source connections (?source=1): processes, not browsers. They
        # are *authoritative* peers on the shared property plane (set_props) —
        # only a hard lock stops them — where a browser passes the same
        # role/operable gate as input.
        self._source_conns = set()
        # Input-event subscriptions: comp id -> connections that asked to
        # RECEIVE this panel's input frames too (any process may react to a
        # panel it doesn't own; the owner's handlers are untouched).
        self._input_subs = {}
        self._any_connected = threading.Event()  # set while ≥1 client is connected
        # One asyncio.Lock per live connection. The websockets legacy protocol
        # forbids concurrent writes (its drain() has no internal lock — two
        # coroutines draining a flow-control-paused socket trip an assertion), so
        # every send to a given socket is serialized through its lock. Without
        # this, a high-rate feed (e.g. 30fps video) overlaps sends and crashes.
        self._send_locks = {}  # ws -> asyncio.Lock
        self._loop = None
        self._snapshot_waiters = {}  # reqId -> {"event": Event, "data": ...}
        self._loaded_doc = None  # last full document loaded, replayed on connect
        # Live free-form drawings (the records a *user* draws, not danvas
        # panels) keyed by record id. Browsers relay their changes as `draw`
        # diffs; we accumulate the canonical set here, fan it out to the other
        # browsers, and replay it to anyone who connects later.
        self._drawings = {}  # record id -> drawing record
        # Reflow requests from col/row.refit() — keyed by container id so each
        # column's latest reflow supersedes its previous one. Replayed on connect
        # so auto-height panels stay correctly stacked for every joining client.
        self._reflows = {}  # container key -> reflow message
        # Container tree registrations (from canvas.column/row/container).
        # Keyed by container key; replayed on connect so the frontend's
        # auto-repack is armed for every joining client.
        self._containers = {}   # key -> container_sync message
        # Maps component id -> Container object so _dispatch_layout can notify
        # the owning container when a panel's height changes.
        self._panel_in_container = {}  # comp_id -> Container
        # Optional callback fired (no args) whenever canvas state the user can
        # mutate from the browser changes -- a panel moved/resized (``layout``)
        # or a free-form drawing edited (``draw``). serve(persist=...) sets this
        # to a debounced autosave; ``None`` (the default) means nobody is
        # listening and the notify is a cheap no-op. May be invoked from either
        # the event-loop thread (draw) or a dispatch thread (layout), so the
        # listener must be thread-safe.
        self._on_mutation = None
        # Graveyard: panels the user deleted in the canvas (but Python still owns).
        # Keyed by component id so lookup is O(1). When uiGraveyard is enabled
        # in serve(), the frontend shows a toolbar button that toggles a floating
        # graveyard panel; restore requests arrive as {type:"restore"} messages.
        self._graveyarded = {}   # comp_id -> comp
        self._ui_graveyard = False  # set by serve() / canvas.py
        # Per-connection viewer identity (id / display name / color) for the
        # presence roster and chat. ``_last_seen`` tracks each socket's most
        # recent inbound message so the reaper can drop silent (dead) ones.
        self._viewers = {}     # ws -> {"id", "name", "color"}
        self._last_seen = {}   # ws -> monotonic timestamp of last message
        self._chat_seq = 0     # monotonic id for chat messages
        self._chat_history = deque(maxlen=100)  # recent chat, replayed on join
        # Components that want to observe chat (the Chat panel's Python handle).
        self._chat_sinks = []
        # Back-reference to the owning Canvas and whether the native UI may spawn
        # an ephemeral Inspector. Set by Canvas (``_canvas`` in __init__,
        # ``_ui_inspector`` in serve); the flag is advertised to each browser in
        # the welcome frame so the button only shows where it's allowed.
        self._canvas = None
        self._ui_inspector = False
        # True when serve() was given a password/passwords. Advertised in the
        # welcome frame so the frontend shows a sign-out button (POST-less nav to
        # /__logout__); no auth means there's nothing to sign out of.
        self._auth = False
        # Optional host note shown on the password page (serve(login_message=...));
        # read by server.create_app. None = the default prompt only.
        self._login_message = None
        # serve(merge_server=...): the standing merge server this canvas offers a
        # "Merge…" button for, and this canvas's own reachable address (the source
        # the button pre-seeds). Both ride the welcome frame; None = no button.
        self._merge_server = None
        self._self_url = None
        # Optional merge capability (danvas.merge._MergeHost): set by
        # serve(merge=True) or the dedicated Merge server, it lets this bridge
        # compose *other* canvases in alongside its own components. ``None`` =
        # merging off (the common case for a plain canvas is that it's on, since
        # serve(merge=) defaults True, but the host stays inert until a source is
        # added). Driven from handle_connection / _on_message via three hooks.
        self._merge = None
        # When True, browsers report their pointer position (in canvas/page
        # coords) so Python can read it off the roster as ``viewer["cursor"]``.
        # Advertised in the welcome frame; gated like ``_ui_inspector`` (default
        # on only for a private local bind) since it's viewer telemetry.
        self._cursors = False
        # True when this process is a hot-reload restart (serve sets it). It rides
        # the welcome frame so a reconnecting browser — whose page never reloaded,
        # only its socket — drops the previous run's panels before this run's are
        # replayed. Without it, panels (which get fresh ids each run) pile up: the
        # old shapes linger beside the new ones. See serve(hot_reload=True).
        self._reload = False
        # Optional viewport/navigation config (initial camera, zoom limits, pan/
        # zoom lock, UI chrome visibility). Sent to each browser in `welcome` and
        # applied to the canvas on connect. ``None`` leaves every default in place.
        self._view = None
        # Per-client view state: viewer_id -> view_dict. When set, overrides the
        # global _view for that specific client (merged with global defaults).
        self._view_per_client = {}
        # Per-role view state: role -> view_dict. Applied to a viewer on connect
        # by their login role. Precedence is global < per-role < per-client, so a
        # client-specific override still wins. Set via canvas.set_view(roles=...).
        self._view_per_role = {}
        # Shared React assets (canvas.define / canvas.style): JSX component sources
        # made available by name in every React panel's compile scope, and a single
        # global stylesheet injected once into the page <head>. Replayed to each
        # browser on connect (before panels register, so a panel mounts with them
        # present) and broadcast live when changed. ``_shared_components`` is
        # name -> JSX source; ``_shared_styles`` is the concatenated global CSS.
        self._shared_components = {}
        self._shared_styles = ""
        # Conflated ("latest" queue policy) send state. For components that opt
        # out of FIFO, we keep only the newest pending value per (socket,
        # component, channel) and a flag marking whether a sender is draining it,
        # so a fast producer can't pile a backlog onto a slow client. Guarded by a
        # plain lock since producers are user threads and the sender is the loop.
        self._conflate_pending = {}   # (ws, comp_id, kind) -> (kind, msg|bytes)
        self._conflate_active = set()  # (ws, comp_id, kind) with a live sender
        self._conflate_lock = threading.Lock()
        # Pending file downloads (the Download panel). Maps an unguessable token
        # to ``(filename, source, expiry)`` where ``source`` is a filesystem path
        # or ``bytes``; the ``/__download__/<token>`` HTTP route streams it. Kept
        # for a TTL window (not single-use, so a HEAD+GET or a retry both work)
        # then purged so the table can't grow without bound. Guarded by a plain
        # lock: tokens are minted on the input-dispatch thread and consumed on the
        # event loop.
        self._downloads = {}        # token -> (filename, source, expiry monotonic)
        self._download_lock = threading.Lock()
        # Upload targets (the Upload panel). Maps a panel's unguessable token to
        # the component, so the ``/__upload__/<token>`` route can find which panel
        # an incoming file belongs to and fire its callbacks. Tokens are stable
        # for the panel's lifetime (the button is reused), unlike one-off download
        # tokens. Plain dict: writes happen on the loop/insert thread, reads on the
        # event loop; both are cheap and a stale entry is harmless.
        self._uploads = {}          # token -> Upload component
        # User input/layout callbacks (``on_change``/``on_layout`` and the
        # component routers) run here, on a single FIFO worker thread, instead of
        # on the asyncio event loop. A slow or blocking callback (a sleep, an HTTP
        # call, heavy compute -- exactly what "drag slider -> move robot" handlers
        # do) would otherwise freeze the loop and stall rendering and every other
        # viewer. One ordered thread preserves per-message order (so a slider drag
        # settles on its last value) while keeping the loop free. Lazy: no thread
        # until the first inbound message.
        self._dispatch = Kernel()

    # -- wire observation ------------------------------------------------------
    def add_frame_tap(self, fn):
        """Register ``fn(direction, msg)`` to observe every WebSocket frame.

        ``direction`` is ``"out"`` (Python -> browser) or ``"in"`` (browser ->
        Python); ``msg`` is the frame's dict (binary media frames arrive as a
        ``{"type": "binary", ...}`` summary). Taps observe; they must not block
        (they run inline on the sending/receiving path). A tap may safely drive
        components — frames a tap itself causes are not re-tapped.
        """
        self._frame_taps.append(fn)
        return fn

    def remove_frame_tap(self, fn):
        if fn in self._frame_taps:
            self._frame_taps.remove(fn)

    def add_dispatch_tap(self, fn):
        """Register ``fn(event)`` to observe handler dispatch (on_dispatch).

        Fires as each registered input/layout handler is queued, runs, and
        finishes, so a tap can build a live execution trace. ``event`` is a dict:

        * ``trace`` — an id shared by every handler one browser action fans out to;
        * ``seq`` — that handler's position within the action (0-based);
        * ``comp`` — the component's name;
        * ``event`` — the kind of trigger (e.g. ``"click"``/``"input"``/``"layout"``);
        * ``handler`` — the handler's name plus source location (``file:line``),
          so the anonymous ``def _`` handlers stay distinguishable;
        * ``mode`` — ``"inline"`` / ``"threaded"`` / ``"dedicated"``;
        * ``phase`` — ``"queued"`` then ``"start"`` then ``"done"`` (or ``"error"``);
        * ``t`` — a ``time.perf_counter()`` stamp; ``dur_ms`` (and ``error`` repr)
          on the terminal event.

        ``start``/``done`` fire on whatever thread the handler's dispatch mode
        runs it on, so a ``threaded=True`` handler reports its run *concurrently*
        with the others — the tap may be called from several threads at once, so
        keep it cheap and thread-safe.
        """
        self._dispatch_taps.append(fn)
        return fn

    def remove_dispatch_tap(self, fn):
        if fn in self._dispatch_taps:
            self._dispatch_taps.remove(fn)

    def _next_trace_id(self):
        return next(self._trace_ids)

    def _emit_dispatch(self, event):
        """Record a dispatch-trace event (when recording) and fan it out to the
        taps; a broken tap never breaks (or stalls) the handler it observes."""
        if self._trace_recording:
            self._record_dispatch(event)
        for fn in list(self._dispatch_taps):
            try:
                fn(event)
            except Exception:
                traceback.print_exc()

    def _record_dispatch(self, event):
        """Fold one event into the bounded action-history ring.

        Pairs ``start`` with its later ``done``/``error`` by ``fid`` to build each
        action's frame list (handler/nested call, with depth, mode, status and
        duration). Called from the dispatch thread and from handler threads, so it
        locks. Per-action frames are capped too, so a runaway-recursive handler
        under deep tracing can't grow one entry without bound."""
        tid = event.get("trace")
        if tid is None:
            return
        phase = event.get("phase")
        with self._trace_lock:
            hist = self._trace_history
            rec = hist.get(tid)
            if rec is None:
                rec = {"trace": tid, "comp": event.get("comp"),
                       "event": event.get("event"), "frames": [], "_byfid": {}}
                hist[tid] = rec
                while len(hist) > self._trace_history_limit:
                    del hist[next(iter(hist))]      # evict the oldest action
            if phase == "start":
                if len(rec["frames"]) < 2000:
                    frame = {"fid": event.get("fid"), "depth": event.get("depth"),
                             "handler": event.get("handler"),
                             "mode": event.get("mode"),
                             "status": "running", "dur_ms": None}
                    rec["frames"].append(frame)
                    rec["_byfid"][event.get("fid")] = frame
            elif phase in ("done", "error"):
                frame = rec["_byfid"].get(event.get("fid"))
                if frame is not None:
                    frame["status"] = "error" if phase == "error" else "done"
                    frame["dur_ms"] = event.get("dur_ms")
                    if phase == "error":
                        frame["error"] = event.get("error")

    def _trace_history_snapshot(self):
        """A JSON-serialisable copy of the recorded actions, oldest → newest.
        Strips the internal fid index and copies frames so callers can't mutate
        the live buffer."""
        with self._trace_lock:
            return [
                {"trace": rec["trace"], "comp": rec["comp"],
                 "event": rec["event"], "frames": [dict(f) for f in rec["frames"]]}
                for rec in self._trace_history.values()
            ]

    def add_cursor_tap(self, fn):
        """Register ``fn(viewer)`` to observe viewer cursor moves (on_cursor).

        ``viewer`` is a snapshot dict with ``id``/``name``/``color`` and the new
        ``cursor`` (``{"x", "y"}`` in canvas coords). Runs off the event loop on
        the input-dispatch thread, but it is high-rate — keep it cheap.
        """
        self._cursor_taps.append(fn)
        return fn

    def remove_cursor_tap(self, fn):
        if fn in self._cursor_taps:
            self._cursor_taps.remove(fn)

    def _tap_cursor(self, viewer):
        for fn in list(self._cursor_taps):
            try:
                fn(viewer)
            except Exception:
                traceback.print_exc()

    def add_connect_tap(self, fn):
        """Register ``fn(viewer)`` to fire once when a viewer connects (on_connect).

        ``viewer`` is a snapshot dict (``id``/``name``/``color``/``cursor``/
        ``device``/``role``). Runs off the event loop on the dispatch thread, so
        a handler may safely drive the canvas (e.g. ``set_layout(client_id=...)``
        to adapt the layout to that viewer's device).
        """
        self._connect_taps.append(fn)
        return fn

    def remove_connect_tap(self, fn):
        if fn in self._connect_taps:
            self._connect_taps.remove(fn)

    def _tap_connect(self, viewer):
        for fn in list(self._connect_taps):
            try:
                fn(viewer)
            except Exception:
                traceback.print_exc()

    def add_disconnect_tap(self, fn):
        """Register ``fn(viewer)`` to fire once when a viewer leaves (on_disconnect).

        ``viewer`` is the departed viewer's last-known snapshot dict (same shape
        as on_connect). Runs off the event loop on the dispatch thread; the
        viewer is already gone from the roster, so use it to release per-viewer
        resources or log the session, not to message that viewer.
        """
        self._disconnect_taps.append(fn)
        return fn

    def remove_disconnect_tap(self, fn):
        if fn in self._disconnect_taps:
            self._disconnect_taps.remove(fn)

    def _tap_disconnect(self, viewer):
        for fn in list(self._disconnect_taps):
            try:
                fn(viewer)
            except Exception:
                traceback.print_exc()

    def _tap_frame(self, direction, msg):
        """Hand one frame to every tap, guarding against tap-driven recursion."""
        if not self._frame_taps or getattr(self._tap_guard, "active", False):
            return
        self._tap_guard.active = True
        try:
            for fn in list(self._frame_taps):
                try:
                    fn(direction, msg)
                except Exception:
                    traceback.print_exc()
        finally:
            self._tap_guard.active = False

    def _tap_binary(self, data):
        """Report a binary media frame to taps as a small JSON-able summary."""
        if not self._frame_taps:
            return
        try:  # header: [type][idLen][id bytes][payload] (see encode_binary_frame)
            kind = {BINARY_VIDEO: "video", BINARY_AUDIO: "audio"}.get(data[0])
            cid = data[2:2 + data[1]].decode("utf-8", "replace")
            self._tap_frame("out", {"type": "binary", "id": cid,
                                    "media": kind, "bytes": len(data)})
        except Exception:
            pass

    def _on_binary_input(self, ws, data):
        """Route an inbound binary frame (browser → Python) to the right component.

        Frame layout: ``[type][idLen][id bytes][payload]`` — the same envelope as
        outbound frames. Currently only ``BINARY_INPUT`` (type 5) is valid here;
        other codes are silently ignored (they are server→browser-only directions).
        """
        if len(data) < 2:
            return
        type_code = data[0]
        id_len = data[1]
        if len(data) < 2 + id_len:
            return
        comp_id = data[2:2 + id_len].decode("utf-8", "replace")
        payload = data[2 + id_len:]
        if self._frame_taps:
            self._tap_frame("in", {"type": "binary", "id": comp_id,
                                   "bytes": len(data)})
        # Merge plane: a dial-in source's MEDIA frames relay to the composed
        # canvas (id rewritten in-envelope); a viewer's binary INPUT on a
        # merged (namespaced) panel routes to the owning source. Bare-id
        # INPUT on this canvas's own panels falls through to normal dispatch.
        if self._merge is not None:
            up = self._merge._dialins.get(ws)
            if up is not None and type_code != BINARY_INPUT:
                self._merge.ingest_binary(up, data)
                return
            if self._merge.route_binary(ws, data):
                return
        if type_code == BINARY_INPUT:
            comp = self._components.get(comp_id)
            if comp is not None:
                # Same authorization as JSON input: binary frames are user
                # interaction too (canvas.sendBinary / camera / mic relays).
                if not self._may_operate(comp, self._viewer_of(ws)):
                    _log.debug("dropped binary input for %s: viewer not "
                               "authorized", comp_id)
                    return
                self._dispatch.submit(
                    lambda c=comp, d=payload: self._dispatch_binary_input(c, d, ws)
                )

    def _dispatch_binary_input(self, comp, data, ws):
        """Call a component's binary-input handler off the event loop."""
        handle = getattr(comp, "_receive_binary", None)
        if handle is not None:
            handle(data, self._viewers.get(ws, {}))

    # -- wiring --------------------------------------------------------------
    def add_component(self, component):
        self._components[component.id] = component

    def remove_component(self, component_id):
        """Forget a component and tell connected clients to drop its panel."""
        self._components.pop(component_id, None)
        # Drop any upload token pointing at this component so the map can't grow.
        for tok in [t for t, c in self._uploads.items()
                    if getattr(c, "id", None) == component_id]:
            self._uploads.pop(tok, None)
        # If the panel belonged to a Container, remove it and repack.
        container = self._panel_in_container.pop(component_id, None)
        if container is not None:
            container._children = [
                c for c in container._children
                if not (hasattr(c, "id") and c.id == component_id)
            ]
            root = container._root()
            if root._x is not None and root._y is not None:
                root._sync()
        self.broadcast({"type": "remove", "id": component_id})

    def reorder_component(self, component_id, op):
        """Restack a record (front/back/forward/backward) on every live client.

        Panels, managed shapes, and arrows all live in the browser's ONE
        global z-order, so the same ``order`` frame restacks any of them —
        ``component_id`` is looked up across all three registries.

        ``front``/``back`` also move the record to the end/start of its replay
        registry, so a client that connects or reloads rebuilds things in the
        new stacking order (later-registered records sit on top). ``forward`` /
        ``backward`` are a live one-step nudge only: the canvas's overlap-aware
        step has no faithful registry equivalent, so it isn't persisted across
        reload.
        """
        if op in ("front", "back"):
            for reg in (self._components, self._shapes, self._arrows):
                obj = reg.pop(component_id, None)
                if obj is None:
                    continue
                if op == "front":
                    reg[component_id] = obj    # last in -> top of stack
                else:
                    rest = dict(reg)           # first in -> bottom, in place
                    reg.clear()
                    reg[component_id] = obj
                    reg.update(rest)
                break
        self.broadcast({"type": "order", "id": component_id, "op": op})

    def add_arrow(self, arrow):
        """Store an ``Arrow`` and broadcast its register message to live clients.

        The object (not a snapshot) is kept so reconnecting clients replay the
        arrow with its current props after their panels are recreated.
        """
        self._arrows[arrow.id] = arrow
        self.broadcast(arrow.register_message())

    def remove_arrow(self, arrow_id):
        """Forget an arrow and tell connected clients to drop it."""
        self._arrows.pop(arrow_id, None)
        self.broadcast({"type": "remove", "id": arrow_id})

    def add_shape(self, shape):
        """Store a managed shape and broadcast its register message."""
        self._shapes[shape.id] = shape
        shape._bridge = self
        self.broadcast(shape.register_message())

    def remove_shape(self, shape_id):
        """Forget a managed shape and tell connected clients to drop it.

        Reuses the existing ``remove`` message type; ``removeComponent`` on the
        JS side handles ``managedIds.delete`` + ``editor.deleteShape`` for any
        id, including non-panel shapes.
        """
        self._shapes.pop(shape_id, None)
        self.broadcast({"type": "remove", "id": shape_id})

    def add_draw_tap(self, fn):
        """Register ``fn`` as an ephemeral-drawing observer (canvas.on_draw)."""
        self._draw_taps.append(fn)
        return fn

    def remove_draw_tap(self, fn):
        """Remove a draw observer registered with :meth:`add_draw_tap`."""
        try:
            self._draw_taps.remove(fn)
        except ValueError:
            pass

    def _tap_draw(self, diff):
        """Fire draw observers off the event loop with structured DrawingShape lists."""
        from .shapes import DrawingShape
        added = [DrawingShape(r, self) for r in (diff.get("added") or {}).values()]
        updated = [
            DrawingShape(p[1] if isinstance(p, (list, tuple)) else p, self)
            for p in (diff.get("updated") or {}).values()
        ]
        removed = list((diff.get("removed") or {}).keys())
        event = {"added": added, "updated": updated, "removed": removed}
        for fn in list(self._draw_taps):
            try:
                fn(event)
            except Exception:
                traceback.print_exc()

    def store_reflow(self, msg):
        """Persist a reflow message so connecting clients receive it on join.

        Each column/row container has a stable ``key`` (``id(container)``); a
        later ``refit()`` call on the same container replaces the earlier one,
        so only the most-recent layout for each container is replayed.
        """
        self._reflows[msg["key"]] = msg

    def store_container(self, msg):
        """Persist a container_sync message so connecting clients receive it on join.

        Keyed by container key so a later :meth:`~_layout.Container.reflow`
        replaces the earlier registration with the updated member list and
        positions.
        """
        self._containers[msg["key"]] = msg

    def set_loop(self, loop):
        self._loop = loop
        loop.create_task(self._reap_loop())

    # -- live hosting (the 🌐 button + canvas.expose()) -----------------------
    def hosting_state(self):
        """The exposure snapshot broadcast to viewers (and sent in welcome)."""
        h = self._hosting
        local = f"http://127.0.0.1:{h['port']}" if h.get("port") else None
        return {"local": local, "lan": h.get("lan"), "tunnel": h.get("tunnel"),
                "busy": h.get("busy"), "error": h.get("error")}

    def _broadcast_hosting(self):
        self.broadcast({"type": "hosting", **self.hosting_state()})

    async def _hosting_action(self, action):
        """Perform one exposure change (idempotent) and keep viewers posted.

        Runs on the event loop; the slow parts (tunnel spawn) hop to an
        executor. Errors land in the broadcast state instead of raising — the
        flyout shows them, and the canvas keeps serving regardless.
        """
        self._hosting["error"] = None
        self._hosting["busy"] = action
        self._broadcast_hosting()
        try:
            if action == "host_lan":
                await self._expose_lan()
            elif action == "host_tunnel":
                await self._open_tunnel()
            elif action == "host_lan_off":
                await self._close_lan()
            elif action == "host_tunnel_off":
                await self._close_tunnel()
        except Exception as exc:
            self._hosting["error"] = str(exc) or type(exc).__name__
            _log.warning("hosting: %s failed: %s", action, exc)
        finally:
            self._hosting["busy"] = None
            self._broadcast_hosting()

    async def _expose_lan(self):
        """Add a second listener for the SAME app on the LAN address, live.

        The original 127.0.0.1 bind stays; the LAN IP is a distinct address so
        the same port binds cleanly. ``_serve`` is used instead of ``serve()``
        because the latter's capture_signals would steal Ctrl+C from the
        primary server when this loop runs on the main thread.
        """
        if self._hosting.get("lan"):
            return
        if self._app is None or not self._hosting.get("port"):
            raise RuntimeError("hosting state not initialised (serve() first)")
        from . import server as _server
        import uvicorn
        ip = _server._lan_ip()
        if not ip:
            raise RuntimeError("no LAN address found on this machine")
        port = self._hosting["port"]
        sock = _server._make_server_socket(ip, port)
        srv = uvicorn.Server(uvicorn.Config(
            self._app, log_level="warning", **_server._ws_opts(False)))
        serve = getattr(srv, "_serve", None) or srv.serve
        self._loop.create_task(serve([sock]))
        self._lan_server = srv
        self._hosting["lan"] = f"http://{ip}:{port}"
        _diag(f"[danvas] canvas now also on the LAN: http://{ip}:{port}")

    async def _open_tunnel(self):
        """Open a public HTTPS tunnel to the local port, live (blocking spawn
        off-loop). Needs the [tunnel] extra the first time (downloads
        cloudflared)."""
        if self._hosting.get("tunnel"):
            return
        if not self._hosting.get("port"):
            raise RuntimeError("hosting state not initialised (serve() first)")
        from .tunnel import open_tunnel
        handle = await self._loop.run_in_executor(
            None, open_tunnel, self._hosting["port"])
        self._tunnel_handle = handle
        self._hosting["tunnel"] = handle.url
        _diag(f"[danvas] canvas now public: {handle.url}")

    async def _close_lan(self):
        """Drop the extra LAN listener (the loopback bind is untouched). LAN
        viewers' sockets close; their tabs show the disconnect banner and stop
        being able to reconnect."""
        srv = self._lan_server
        self._lan_server = None
        self._hosting["lan"] = None
        if srv is not None:
            srv.should_exit = True   # its serve task drains and exits
        _diag("[danvas] LAN sharing stopped (local URL still up)")

    async def _close_tunnel(self):
        """Tear the public tunnel down (blocking process stop, off-loop)."""
        handle = self._tunnel_handle
        self._tunnel_handle = None
        self._hosting["tunnel"] = None
        if handle is not None:
            await self._loop.run_in_executor(None, handle.stop)
        _diag("[danvas] public tunnel closed")

    def register_message(self, component, role=None, client_id=None):
        """Build the ``register`` message for a component, including placement.

        ``role``/``client_id`` identify the connecting viewer so a component with
        per-viewer prop overlays (see :meth:`React.update` ``roles=``) replays the
        slice that viewer should see; omit them for the shared props.
        """
        msg = {
            "type": "register",
            "id": component.id,
            "component": component.component,
            "props": component.register_props_for(role, client_id),
        }
        # The Python-side identity, carried on the wire so peer processes can
        # resolve panels by the same name= the owning script uses (cross-
        # process canvas["name"]). Additive: the frontend ignores it.
        name = getattr(component, "name", None)
        if name:
            msg["name"] = name
        # The role allowlist, declared on the wire so a HUB can enforce
        # egress for panels it relays (the owner still enforces for its own
        # viewers). Empty/absent = visible to everyone. Additive field.
        roles = list(getattr(component, "_roles", []) or [])
        if roles:
            msg["roles"] = roles
        # Which process executes this panel's handlers: the serving canvas's
        # own panels say "host"; a RemoteCanvas stamps its dial-in label; a hub
        # re-stamps relayed panels with the source's label. Purely descriptive
        # (routing is by connection, not by this field).
        msg["owner"] = self._owner_label
        pos = getattr(component, "_position", None)
        if pos is not None:
            msg["x"], msg["y"] = pos
        rot = getattr(component, "_rotation", None)
        if rot is not None:
            msg["rotation"] = math.radians(rot)
        op = getattr(component, "_opacity", 1.0)
        if op != 1.0:
            msg["opacity"] = op
        # Lock/chrome flags: send each only when it differs from its default
        # (e.g. locked=True, or draggable=False as movable=False), matching
        # set_layout's payload and the frontend's lockMeta. The Python names
        # (draggable/operable/grabbable) map to the wire keys
        # (movable/interactive/selectable) via the LAYOUT_FLAGS table, the single
        # source of truth shared with base.py and canvas.py.
        for flag in LAYOUT_FLAGS.values():
            value = getattr(component, flag.attr, flag.default)
            if value != flag.default:
                msg[flag.wire] = value
        fc = getattr(component, "_frame_color", None)
        if fc is not None:
            msg["frameColor"] = fc
        # Relative placement rides the register frame (PROTOCOL.md): the
        # frontend places when x/y is absent and owns the cascade either way.
        rel = getattr(component, "_rel", None)
        if rel:
            msg["rel"] = rel
        # Keep wheel local to the panel content (no canvas zoom) — sent only when
        # opted out of the default. Custom handles this inside its iframe instead.
        if not getattr(component, "_forward_wheel", True):
            msg["wheelLocal"] = True
        # Per-viewer layout overlay (set_layout(roles=) / a drag written to the
        # viewer's own layer): merge it on top of the base geometry. x/y/rotation
        # and the lock flags are top-level register fields; w/h are shape props.
        overlay = (component._layout_overlay_for(role, client_id)
                   if hasattr(component, "_layout_overlay_for") else {})
        if overlay:
            if "x" in overlay:
                msg["x"] = overlay["x"]
            if "y" in overlay:
                msg["y"] = overlay["y"]
            if "rotation" in overlay:
                msg["rotation"] = math.radians(overlay["rotation"])
            if "opacity" in overlay:
                msg["opacity"] = float(overlay["opacity"])
            if "w" in overlay:
                msg["props"]["w"] = overlay["w"]
            if "h" in overlay:
                msg["props"]["h"] = overlay["h"]
            for name, flag in LAYOUT_FLAGS.items():
                if name in overlay:
                    msg[flag.wire] = bool(overlay[name])
        return msg

    def register_live(self, component, only_roles=None):
        """Push a newly-added component to connected clients who may see it.

        Role-aware: clients whose role is not in the component's ``_roles``
        list are skipped. Used for components inserted after the server is
        already running (e.g. from a Jupyter cell). Fresh connections still get
        the full replay via ``handle_connection``; this covers live clients.

        ``only_roles`` (a set/list of role names) further narrows the push to
        viewers in those roles — used by :meth:`BaseComponent.add_role` so newly
        allowed roles get the panel without re-registering it to viewers who
        already had it.
        """
        roles = getattr(component, "_roles", [])
        lock_for = getattr(component, "_lock_for", [])
        state = component.state_payload()
        if self._loop is None:
            return
        # Build the register frame once when the panel has no per-viewer overlays
        # (the common case) — only re-derive per viewer when there's a role/client
        # override to merge, so a fan-out to N viewers isn't N identical builds.
        has_overlays = component._has_viewer_overlays()
        shared_reg_text = (None if has_overlays
                           else _dumps(self.register_message(component)))
        # Pre-encode shared messages once (state and lock are viewer-independent).
        state_text = (_dumps({"type": "update", "id": component.id, "payload": state})
                      if state else None)
        locked_text = (_dumps({"type": "update", "id": component.id,
                                "payload": {"operable": False}})
                       if lock_for else None)
        # Collect the eligible (ws, reg_text) pairs — reg may differ per viewer
        # when overlays are present, but is otherwise the same encoded string.
        eligible = []
        locked_sockets = []
        for ws, viewer in list(self._viewers.items()):
            role = viewer.get("role")
            if roles and role not in roles:
                continue
            if only_roles is not None and role not in only_roles:
                continue
            reg_text = shared_reg_text if shared_reg_text is not None else _dumps(
                self.register_message(component, role=role, client_id=viewer.get("id")))
            eligible.append((ws, reg_text))
            if locked_text and role in lock_for:
                locked_sockets.append(ws)
        if not eligible:
            return
        # All sends are batched into one coroutine — a single cross-thread hop
        # instead of N×3, with per-socket sends run concurrently via gather.
        asyncio.run_coroutine_threadsafe(
            self._register_live_fanout(eligible, state_text, locked_sockets, locked_text),
            self._loop,
        )

    # -- connection lifecycle (runs in the event loop) -----------------------
    async def handle_connection(self, ws, role=None):
        await ws.accept()
        self._connections.add(ws)
        self._any_connected.set()
        self._send_locks[ws] = asyncio.Lock()
        self._last_seen[ws] = time.monotonic()
        # A reconnecting browser re-sends its identity (id/name/color) as query
        # params so a flapping tab keeps one viewer instead of churning new ones.
        # ``role`` still comes from the trusted session, never the client.
        qp = getattr(ws, "query_params", {})
        requested = {"id": qp.get("vid"), "name": qp.get("vname"),
                     "color": qp.get("vcolor")}
        # A merge server connects with ?proxy=1 so its own input echoes reach it
        # (it fronts many browsers; see _proxy_conns / _dispatch_input).
        if qp.get("proxy"):
            self._proxy_conns.add(ws)
        # A dial-in source (?source=1) is a process peer, not a browser — track
        # it so the shared property plane can treat it as authoritative.
        if qp.get("source"):
            self._source_conns.add(ws)
        # Classify the connecting device from the handshake User-Agent (no client
        # cooperation needed) so a handler can adapt the layout to mobile.
        headers = getattr(ws, "headers", {})
        # A process peer is roster-visible by design, but marked so the UI
        # counts humans separately (the badge must not say "2 viewers" to a
        # solo user whose own program is peer #2).
        device = ("process" if qp.get("source")
                  else _device_from_ua(headers.get("user-agent")))
        viewer = self._make_viewer(role=role, requested=requested, device=device)
        self._viewers[ws] = viewer
        self._broadcast_roster()  # tell everyone a viewer joined
        try:
            # Tell this client who it is, so it can label its own chat messages
            # and prefill the editable name field.
            view_for_client = self._view_for(viewer["id"], role)
            await self._send(ws, {"type": "welcome", "you": viewer,
                                  "protocol": PROTOCOL_VERSION,
                                  # This hub serves the rel-aware frontend
                                  # (same dist as danvasd), so SDKs may hand
                                  # relative placement to it (PROTOCOL.md).
                                  "features": ["rel"],
                                  "uiInspector": self._ui_inspector,
                                  "uiGraveyard": self._ui_graveyard,
                                  "uiHosting": self._ui_hosting,
                                  "hosting": self.hosting_state(),
                                  "auth": self._auth,
                                  "cursors": self._cursors,
                                  "view": view_for_client,
                                  "runId": self._run_id,
                                  "reload": self._reload,
                                  "mergeHost": self._merge is not None,
                                  "mergeServer": self._merge_server,
                                  "selfUrl": self._self_url})
            # Replay the shared React assets (canvas.define / canvas.style) before
            # any panel registers, so a React panel mounts with its shared
            # components and the global stylesheet already in place.
            if self._shared_components or self._shared_styles:
                await self._send(ws, self.shared_message())
            # Replay recent chat so a fresh viewer sees the conversation so far.
            for entry in self._chat_history:
                await self._send(ws, entry)
            # Replay full state to the freshly connected client, filtered by role.
            # Encode each frame once as text (using _send_text) rather than
            # letting _send re-encode the same dict per viewer when multiple
            # clients connect concurrently. Per-viewer overlays still rebuild
            # the register message but encode it once for this viewer.
            for comp in self._components.values():
                roles = getattr(comp, "_roles", [])
                if roles and role not in roles:
                    continue  # this panel is not visible to this role
                await self._send_text(ws, _dumps(self.register_message(
                    comp, role=role, client_id=viewer["id"])))
                state = comp.state_payload()
                if state:
                    await self._send_text(
                        ws, _dumps({"type": "update", "id": comp.id, "payload": state})
                    )
                lock_for = getattr(comp, "_lock_for", [])
                if lock_for and role in lock_for:
                    await self._send_text(
                        ws, _dumps({"type": "update", "id": comp.id,
                                    "payload": {"operable": False}})
                    )
            # Replay managed shapes (geo, text, note, draw, line, frame,
            # highlight) before arrows, so an arrow may bind to a shape as well as
            # a panel — both its endpoints now exist by the time it registers.
            for shape in self._shapes.values():
                await self._send(ws, shape.register_message())
            # Arrows bind to panels/shapes, so replay them after both exist.
            for arrow in self._arrows.values():
                await self._send(ws, arrow.register_message())
            # Replay stored reflows so auto-height columns/rows are stacked at
            # real browser-measured heights for every joining client.
            for reflow in self._reflows.values():
                await self._send(ws, reflow)
            # Replay container registrations so the frontend's auto-repack is
            # armed for every joining client (child containers before parents,
            # matching the order store_container receives them).
            for container_msg in self._containers.values():
                await self._send(ws, container_msg)
            # Replay current graveyard list so a freshly connected client sees
            # panels deleted before it joined.
            if self._graveyarded:
                await self._send(ws, self._graveyard_message())
            # Replay the live free-form drawings as a single "added" diff so a
            # fresh (or reloaded) browser sees what others have drawn.
            if self._drawings:
                _log.info("replaying %d drawing record(s) to new viewer", len(self._drawings))
                await self._send(ws, {
                    "type": "draw",
                    "diff": {"added": self._drawings, "updated": {}, "removed": {}},
                })
            # If a full canvas was loaded, replay it last so reloads keep it
            # (it replaces the document, incl. any user drawings it contained).
            if self._loaded_doc is not None:
                await self._send(
                    ws, {"type": "load_snapshot", "data": self._loaded_doc}
                )
            # One diagnostic line per connection, so "nothing happens" debugging
            # starts with evidence: the viewer reached the server and what state
            # it was seeded with.
            _diag(f"[danvas] viewer '{viewer['name']}' connected "
                  f"(replayed {len(self._components)} panels, "
                  f"{len(self._arrows)} arrows, {len(self._shapes)} shapes)")

            # Fire on_connect observers off the loop (a snapshot, like cursor
            # taps) once the client has its initial state — so a handler that
            # adapts the layout (set_layout(client_id=...)) lands as a live
            # update on top of what was just replayed.
            if self._connect_taps:
                self._dispatch.submit(lambda v=dict(viewer): self._tap_connect(v))

            # Merge hub: seed this browser's composed sources (from ?sources= or the
            # default set) AFTER the canvas's own state, so the hub's own panels land
            # first. Inert unless serve(merge=True) / the Merge server enabled it.
            if self._merge is not None:
                self._merge.on_connect(ws, qp)

            while True:
                msg = await ws.receive()
                if msg["type"] == "websocket.disconnect":
                    raise WebSocketDisconnect(msg.get("code", 1000))
                raw_bytes = msg.get("bytes")
                raw_text = msg.get("text")
                if raw_bytes:
                    self._on_binary_input(ws, raw_bytes)
                elif raw_text:
                    # Merge-plane frames (merge control, a merged panel's interaction
                    # or ink) are handled by the hub; everything else — incl. the
                    # hub's OWN panels — falls through to the normal dispatch.
                    if self._merge is not None and await self._merge.route(ws, raw_text):
                        continue
                    self._on_message(ws, raw_text)
        except WebSocketDisconnect:
            pass
        except Exception:
            # Same background-thread hazard as _diag: route the trace to the
            # kernel's real stderr so it can't be misattributed to a finished
            # cell and dispose the controller mid "Run All".
            if sys.__stderr__ is not None:
                traceback.print_exc(file=sys.__stderr__)
        finally:
            if self._merge is not None:
                self._merge.on_disconnect(ws)
            self._connections.discard(ws)
            self._proxy_conns.discard(ws)
            self._source_conns.discard(ws)
            for subs in self._input_subs.values():
                subs.discard(ws)
            if not self._connections:
                self._any_connected.clear()
            self._send_locks.pop(ws, None)
            self._drop_conflate(ws)
            gone = self._viewers.pop(ws, None)
            # Viewer ids are minted fresh per connection and never reused, so a
            # per-client view override for a departed viewer can never apply
            # again — drop it so the map doesn't grow unbounded.
            if gone is not None:
                self._view_per_client.pop(gone["id"], None)
                # Tell peers to drop this viewer's rendered cursor.
                if self._cursors:
                    self.broadcast({"type": "cursor_gone", "id": gone["id"]})
                _diag(f"[danvas] viewer '{gone['name']}' disconnected")
                # Fire on_disconnect observers off the loop (a snapshot, the
                # symmetric twin of the on_connect tap), so a handler can release
                # per-viewer resources or log the session without blocking teardown.
                if self._disconnect_taps:
                    self._dispatch.submit(
                        lambda v=dict(gone): self._tap_disconnect(v))
            self._last_seen.pop(ws, None)
            self._broadcast_roster()  # tell everyone a viewer left

    def _make_viewer(self, role=None, requested=None, device="desktop"):
        """Mint a viewer identity (id + friendly editable name + color + role).

        ``requested`` carries a browser-supplied id/name/color from a reconnect
        (see handle_connection): when its id looks valid it's reused so a tab that
        flaps keeps a single, stable identity instead of being renamed each time.
        These three fields are client-reported either way (only ``role`` is
        trusted), so honouring them changes no trust boundary. ``device`` is the
        connection's classified device (``"mobile"``/``"desktop"``) for layout
        adaptation — also client-reported (from the User-Agent), so attribution
        only, never authorization.
        """
        rid = (requested or {}).get("id")
        if rid and rid.isalnum() and len(rid) <= 32:
            rname = ((requested or {}).get("name") or "").strip()[:40]
            rcolor = (requested or {}).get("color") or ""
            color = (rcolor if len(rcolor) == 7 and rcolor[0] == "#"
                     and all(c in "0123456789abcdefABCDEF" for c in rcolor[1:])
                     else random.choice(_VIEWER_COLORS))
            return {"id": rid, "name": rname or random.choice(_VIEWER_ANIMALS),
                    "color": color, "cursor": None, "device": device,
                    "role": role}
        existing = {v["name"] for v in self._viewers.values()}
        animal = random.choice(_VIEWER_ANIMALS)
        name = animal
        n = 2
        while name in existing:  # keep auto-names distinct; user can rename
            name = f"{animal} {n}"
            n += 1
        color = random.choice(_VIEWER_COLORS)
        # ``cursor`` is the viewer's last-known pointer position in canvas/page
        # coords (``{"x", "y"}``), or None until they move it (and only ever
        # populated when cursor reporting is enabled). Read it via canvas.viewers.
        # ``role`` is the access level granted at login (None when no passwords set).
        return {"id": uuid.uuid4().hex[:8], "name": name, "color": color,
                "cursor": None, "device": device, "role": role}

    def _broadcast_roster(self):
        """Push the live-viewer roster (and count) to every connected browser.

        Carries the full list of viewers (id / name / color) so the UI can show
        who's here and the chat can colour names; ``count`` is kept for the
        presence badge. Sent on every join/leave/rename. Safe before the loop
        exists (broadcast no-ops then).
        """
        viewers = list(self._viewers.values())
        self.broadcast({"type": "presence", "count": len(viewers),
                        "viewers": viewers})

    # -- chat / identity (browser <-> browser, relayed through the server) ----
    def _rename_viewer(self, ws, name):
        """Apply a viewer's editable display name, then re-broadcast the roster."""
        viewer = self._viewers.get(ws)
        if viewer is None:
            return
        clean = (name or "").strip()[:24]
        if not clean:
            return
        viewer["name"] = clean
        self._broadcast_roster()

    def _handle_chat(self, ws, text):
        """Stamp a chat line with the sender's identity and fan it out to all.

        The identity is taken from the server's record of the socket (not the
        client's claim), so a name can't be spoofed. The message is appended to
        the replay history and also delivered to any Python ``Chat`` sinks.
        """
        viewer = self._viewers.get(ws)
        if viewer is None:
            return
        body = (text or "").strip()
        if not body:
            return
        self._chat_seq += 1
        entry = {
            "type": "chat",
            "msgId": self._chat_seq,
            "id": viewer["id"],
            "name": viewer["name"],
            "color": viewer["color"],
            "text": body[:2000],
            "ts": time.time(),
        }
        self._chat_history.append(entry)
        self.broadcast(entry)
        self._fan_out_chat(entry)

    def _fan_out_chat(self, entry):
        """Deliver a chat entry to every Python sink (off ``_handle_chat`` /
        ``post_chat``). Sinks run inline on the event loop unless marked
        ``threaded=True`` (Chat.on_message), which runs them on their own thread
        so a slow one can't stall the canvas."""
        for sink in self._chat_sinks:
            if getattr(sink, "_danvas_threaded", False):
                spawn(lambda s=sink: s(entry), name="danvas-chat-sink")
            else:
                try:
                    sink(entry)
                except Exception:
                    traceback.print_exc()

    def post_chat(self, text, name="host", color="#64748b"):
        """Inject a chat message from Python (e.g. a system/host announcement)."""
        body = (text or "").strip()
        if not body:
            return
        self._chat_seq += 1
        entry = {
            "type": "chat", "msgId": self._chat_seq, "id": "host",
            "name": name, "color": color, "text": body[:2000], "ts": time.time(),
        }
        self._chat_history.append(entry)
        self.broadcast(entry)
        self._fan_out_chat(entry)

    def add_chat_sink(self, fn):
        """Register a callback fired with every chat entry (Chat panel handle)."""
        self._chat_sinks.append(fn)

    def remove_chat_sink(self, fn):
        if fn in self._chat_sinks:
            self._chat_sinks.remove(fn)

    # -- file downloads (Download panel <-> /__download__ route) --------------
    def register_download(self, filename, source, ttl=300, role=None):
        """Stash ``source`` under a fresh token and return it (any-thread safe).

        ``source`` is a filesystem path or ``bytes``; the ``/__download__/<token>``
        route streams it to the browser as an attachment named ``filename``. The
        token is unguessable and expires after ``ttl`` seconds, so a leaked URL
        can't be replayed indefinitely. ``role`` (when set) restricts the download
        to viewers holding that login role — the route rejects anyone else even
        though the URL is otherwise behind the shared auth gate. Expired tokens are
        purged opportunistically on each register/consume.
        """
        token = secrets.token_urlsafe(24)
        with self._download_lock:
            self._purge_downloads()
            self._downloads[token] = (filename, source, time.monotonic() + ttl, role)
        return token

    def take_download(self, token):
        """Resolve a download token to ``(filename, source, role)`` or ``None``.

        Returns ``None`` if the token is unknown or has expired. ``role`` is the
        login role required to fetch it (``None`` = any authorised viewer); the
        route enforces it. Not single-use: the entry stays until its TTL lapses, so
        a browser that issues a HEAD then GET (or retries) still succeeds.
        """
        with self._download_lock:
            self._purge_downloads()
            item = self._downloads.get(token)
        if item is None:
            return None
        filename, source, _exp, role = item
        return filename, source, role

    def _purge_downloads(self):
        """Drop expired download tokens. Call with ``_download_lock`` held."""
        now = time.monotonic()
        expired = [t for t, (_, _, exp, _r) in self._downloads.items() if exp <= now]
        for t in expired:
            self._downloads.pop(t, None)

    # -- file uploads (Upload panel <-> /__upload__ route) -------------------
    def register_upload(self, token, component):
        """Bind an upload ``token`` to the panel that receives its files."""
        self._uploads[token] = component

    def upload_component(self, token):
        """Resolve an upload token to its panel, or ``None`` if unknown."""
        return self._uploads.get(token)

    def deliver_upload(self, component, info, viewer=None):
        """Fire a panel's upload handler off the event loop (any-thread safe).

        ``info`` is the server-built file dict (``name``/``size``/``content_type``
        and one of ``data``/``path``); ``viewer`` is the uploader's identity (see
        :meth:`resolve_viewer`). Runs on the input-dispatch thread, like every
        other user callback, so a slow handler can't stall rendering.
        """
        self._dispatch.submit(
            lambda: component._receive_upload(info, viewer or {})
        )

    def resolve_viewer(self, viewer_id, role=None):
        """Build the uploader identity dict for an upload handler.

        Returns the same shape as the viewer dict handed to in-canvas handlers
        (``id``/``name``/``color``/``cursor``/``role`` — every key always
        present), so handler code can read it uniformly. ``role`` is the
        server-trusted access level from the HTTP auth session — always
        meaningful (``None`` when no passwords are set) and safe to gate on. The
        rest are attribution-only: an upload arrives over HTTP carrying the
        browser's self-reported roster id, so when it matches a *currently
        connected* viewer that viewer's live ``id``/``name``/``color``/``cursor``
        are merged in; a stale or forged id (or an uploader who already
        disconnected) simply leaves them ``None``. Because name/colour are read
        from the server roster — never the client — the only thing a client can
        claim is the id of a viewer who is actually online. Don't trust
        ``id``/``name``/``color`` for authorization; use ``role``.
        """
        info = {"id": None, "name": None, "color": None,
                "cursor": None, "device": None, "role": role}
        if viewer_id:
            for v in list(self._viewers.values()):
                if v.get("id") == viewer_id:
                    info["id"] = v.get("id")
                    info["name"] = v.get("name")
                    info["color"] = v.get("color")
                    info["cursor"] = v.get("cursor")
                    info["device"] = v.get("device")
                    break
        return info

    async def _reap_loop(self):
        """Drop connections that have gone silent past the heartbeat deadline.

        Browsers send a periodic heartbeat; one that stops (a hard-closed or
        network-dropped tab) is closed here so the viewer count and roster don't
        stay inflated — the WS keepalive ping is disabled (see server.py), so
        without this a dead socket lingers until the next failed send.
        """
        while True:
            await asyncio.sleep(_REAP_INTERVAL)
            try:
                now = time.monotonic()
                dead = [ws for ws in list(self._connections)
                        if now - self._last_seen.get(ws, now) > _HEARTBEAT_TIMEOUT]
                for ws in dead:
                    try:
                        await ws.close(code=1001)
                    except Exception:
                        pass
                    # handle_connection's finally normally cleans up, but force
                    # it here too in case the receive loop is wedged.
                    self._connections.discard(ws)
                    self._send_locks.pop(ws, None)
                    self._drop_conflate(ws)
                    gone = self._viewers.pop(ws, None)
                    if gone is not None:
                        self._view_per_client.pop(gone["id"], None)
                        self._last_seen.pop(ws, None)
                        self._broadcast_roster()
            except Exception:
                traceback.print_exc()

    # -- inbound authorization -------------------------------------------------
    # The browser hides/disables what a viewer may not touch, but a hand-crafted
    # frame doesn't go through the browser's UI — so the same rules are enforced
    # here, where the trusted session role lives. Everything a viewer may not
    # see, they may not address; controls locked *for* them stay locked against
    # forged input too. Drops are logged at debug level (a malicious client
    # could otherwise spam the console).

    def _viewer_of(self, ws):
        return self._viewers.get(ws) or {}

    def _may_see(self, comp, viewer):
        """Whether this viewer's role may see ``comp`` at all — the same rule
        the register replay applies (empty ``_roles`` means everyone)."""
        roles = getattr(comp, "_roles", None)
        return not roles or viewer.get("role") in roles

    def _effective_flag(self, comp, viewer, name):
        """A lock/chrome flag as this viewer's browser renders it: the shared
        base value overlaid by any per-role / per-client ``set_layout``
        override (precedence shared < role < client, like everything else)."""
        flag = LAYOUT_FLAGS[name]
        value = getattr(comp, flag.attr, flag.default)
        overlay = (comp._layout_overlay_for(viewer.get("role"), viewer.get("id"))
                   if hasattr(comp, "_layout_overlay_for") else {})
        if name in overlay:
            value = bool(overlay[name])
        return value

    def _may_operate(self, comp, viewer):
        """Server-side twin of the browser's interaction gating for ``input`` /
        ``request`` / binary-input frames.

        False when the viewer can't see the panel (role filter), when the panel
        is fully ``locked`` or ``operable=False`` for them (base or overlay),
        or when their role is in the panel's ``lock_for`` list. The layout
        message path deliberately checks only visibility — move/resize frames
        also carry machine-generated reports (auto-flow slots, content-fit
        heights, container repacks) that must keep flowing for panels whose
        *user* gestures are locked.
        """
        if not self._may_see(comp, viewer):
            return False
        if self._effective_flag(comp, viewer, "locked"):
            return False
        if viewer.get("role") in getattr(comp, "_lock_for", ()):
            return False
        return self._effective_flag(comp, viewer, "operable")

    # Placement keys a set_props petition routes through set_layout (one live
    # message, same as a drag) rather than individual property assignment.
    _LAYOUT_PROP_KEYS = ("x", "y", "w", "h", "rotation", "opacity")

    def _apply_props(self, comp, props):
        """Apply a ``set_props`` petition through the component's own setters.

        Runs on the dispatch thread (a slow/odd setter can't stall the wire).
        Placement keys go through ``set_layout`` in one message; anything else
        must be a *settable property* on the component class — the write then
        takes the exact code path ``slider.min = 5`` takes in the owner's own
        script, so validation and the live browser broadcast come for free. A
        key with no writable property, or a value its setter rejects, is
        dropped with a log line — the owner's echoed state remains canonical,
        which is what keeps every replica convergent.
        """
        layout = {k: props.pop(k) for k in list(props)
                  if k in self._LAYOUT_PROP_KEYS or k in LAYOUT_FLAGS}
        if layout:
            try:
                comp.set_layout(**layout)
            except Exception:
                _log.debug("set_props: layout %r rejected for %s",
                           layout, comp.id, exc_info=True)
        for k, v in props.items():
            prop = getattr(type(comp), k, None)
            if isinstance(prop, property) and prop.fset is not None:
                try:
                    setattr(comp, k, v)
                except Exception:
                    _log.debug("set_props: %s.%s = %r rejected",
                               type(comp).__name__, k, v, exc_info=True)
            elif k == "value" and callable(getattr(comp, "update", None)):
                # The canonical content key: a peer's `panel.value = ...` takes
                # the same path the owner's own `panel.update(...)` does —
                # replace the content, broadcast, and (like any programmatic
                # update) do NOT fire on_change. This is what lets a remote
                # process rewrite a Label's text or set a Slider's position.
                try:
                    comp.update(v)
                except Exception:
                    _log.debug("set_props: %s.update(%r) rejected",
                               type(comp).__name__, v, exc_info=True)
            else:
                _log.debug("set_props: %s has no writable property %r",
                           type(comp).__name__, k)

    def _on_message(self, ws, raw):
        try:
            msg = _loads(raw)
        except (ValueError, TypeError):
            return
        # Any inbound frame proves the socket is alive — refresh its deadline.
        self._last_seen[ws] = time.monotonic()
        kind = msg.get("type")
        if kind == "heartbeat":
            return  # liveness only; timestamp already refreshed above
        if kind == "cursor":
            # High-rate pointer telemetry: return *before* the frame tap so cursor
            # spam never floods debug logs or on_frame taps. Already throttled +
            # dead-banded client-side; the server conflates per sender per viewer.
            if self._cursors:
                v = self._viewers.get(ws)
                x, y = msg.get("x"), msg.get("y")
                if v is not None and isinstance(x, (int, float)) \
                        and isinstance(y, (int, float)):
                    x, y = float(x), float(y)
                    # 1) Store newest on the roster entry for Python to read.
                    v["cursor"] = {"x": x, "y": y}
                    # 2) Relay to *other* viewers for peer rendering, tagged with
                    #    the sender's identity/colour. Conflated per sender (a slow
                    #    viewer only gets the latest); tap=False keeps the relay off
                    #    the wire-debug path.
                    self.broadcast_conflated(
                        f"cursor:{v['id']}", exclude=ws, tap=False,
                        msg={"type": "cursor", "id": v["id"], "x": x, "y": y,
                             "color": v["color"], "name": v["name"]},
                    )
                    # 3) Fan out to Python cursor observers off the loop thread.
                    if self._cursor_taps:
                        self._dispatch.submit(
                            lambda vv=dict(v): self._tap_cursor(vv)
                        )
            return
        self._tap_frame("in", msg)
        if kind == "set_name":
            self._rename_viewer(ws, msg.get("name"))
            return
        if kind == "chat":
            self._handle_chat(ws, msg.get("text"))
            return
        if kind == "ui":
            # Native-UI request (e.g. the toolbar Inspector toggle). Gated by the
            # same flag advertised in `welcome`, and only ever touches the canvas
            # when one is attached (the merge host has none and ignores it).
            # Live hosting actions (the 🌐 flyout): gated by the same
            # private-bind default as the button itself; each is idempotent
            # and reports through the broadcast hosting state.
            if msg.get("action") in ("host_lan", "host_tunnel",
                                     "host_lan_off", "host_tunnel_off"):
                if self._ui_hosting:
                    self._loop.create_task(
                        self._hosting_action(msg["action"]))
                return
            if self._ui_inspector and self._canvas is not None:
                if msg.get("action") == "toggle_inspector":
                    # Open it centred in this viewer's current view — the browser
                    # sends its viewport centre with the toggle (falls back to a
                    # fixed spot when it isn't available).
                    try:
                        self._canvas._toggle_ui_inspector(at=msg.get("center"))
                    except Exception:
                        traceback.print_exc()
            return
        if kind == "input":
            comp = self._components.get(msg.get("id"))
            if comp is not None:
                # Authorize before dispatch: the browser disables locked controls,
                # but a forged frame doesn't go through the browser's UI.
                if not self._may_operate(comp, self._viewer_of(ws)):
                    _log.debug("dropped input for %s: viewer not authorized",
                               msg.get("id"))
                    return
                payload = msg.get("payload") or {}
                # Run the (user-authored) handler on the dispatch thread, never on
                # the event loop -- a blocking callback can't stall rendering or
                # other viewers. The state echo happens there too, after handling.
                self._dispatch.submit(
                    lambda c=comp, p=payload: self._dispatch_input(c, p, ws)
                )
                # Event subscription fan-out: any connection that subscribed to
                # this panel's inputs gets a copy of the raw event (so another
                # process can react to a panel it doesn't own). The originator
                # is excluded — it already knows what it sent.
                for sub in list(self._input_subs.get(msg.get("id"), ())):
                    if sub is not ws:
                        self._loop.create_task(self._safe_send(
                            sub, {"type": "input", "id": msg.get("id"),
                                  "payload": payload}))
        elif kind == "set_props":
            # The shared property plane: ANY peer may write ANY panel's
            # properties, Figma-style — the write applies through the owner's
            # real setters (validation + live broadcast for free), so ordering-
            # through-the-owner is the last-writer-wins. A browser passes the
            # same gate as input; a process peer (dial-in source / merge proxy)
            # is authoritative and is stopped only by a hard lock.
            comp = self._components.get(msg.get("id"))
            if comp is not None:
                viewer = self._viewer_of(ws)
                if ws in self._source_conns or ws in self._proxy_conns:
                    allowed = not self._effective_flag(comp, viewer, "locked")
                else:
                    allowed = self._may_operate(comp, viewer)
                props = msg.get("props")
                if allowed and isinstance(props, dict) and props:
                    self._dispatch.submit(
                        lambda c=comp, p=dict(props): self._apply_props(c, p))
                elif not allowed:
                    _log.debug("dropped set_props for %s: not permitted",
                               msg.get("id"))
        elif kind == "subscribe":
            # Ask to receive a panel's input events (in addition to its owner).
            # Visibility-gated: you can't tap a panel your role can't see.
            comp = self._components.get(msg.get("id"))
            if comp is not None and self._may_see(comp, self._viewer_of(ws)):
                self._input_subs.setdefault(msg.get("id"), set()).add(ws)
        elif kind == "unsubscribe":
            subs = self._input_subs.get(msg.get("id"))
            if subs is not None:
                subs.discard(ws)
        elif kind == "layout":
            # User moved/resized a panel or a managed shape; sync Python's state.
            cid = msg.get("id")
            comp = self._components.get(cid)
            if comp is not None:
                # Visibility gate only (see _may_operate for why the lock flags
                # aren't enforced here): a viewer who can't see the panel has no
                # legitimate layout frames for it — its shape never registered
                # in their browser.
                if not self._may_see(comp, self._viewer_of(ws)):
                    _log.debug("dropped layout for %s: viewer not authorized", cid)
                    return
                self._dispatch.submit(
                    lambda c=comp, m=msg: self._dispatch_layout(c, m, ws)
                )
            else:
                # A Python-managed shape (canvas.geo/text/line/…) was moved/resized
                # in the browser — store the new geometry so it replays on reload.
                shape = self._shapes.get(cid)
                if shape is not None:
                    self._apply_shape_layout(shape, msg, ws)
        elif kind == "graveyard":
            # User deleted a danvas-managed object (panel / shape / arrow). All go
            # to the graveyard (restorable) and off the live canvas — anything
            # placed programmatically is treated uniformly, just like panels.
            # Role-gated like input: a viewer can't delete a panel they can't see
            # (shapes/arrows carry no roles and pass the check trivially).
            cid = msg.get("id")
            target = (self._components.get(cid) or self._shapes.get(cid)
                      or self._arrows.get(cid))
            if target is not None and not self._may_see(target, self._viewer_of(ws)):
                _log.debug("dropped graveyard for %s: viewer not authorized", cid)
                return
            self._graveyard(cid)
        elif kind == "restore":
            # User clicked Restore in the graveyard panel; bring the object back to
            # the live canvas (route by the kind recorded when it was graveyarded).
            # Same role gate as graveyard — an unauthorized viewer can't resurrect
            # a panel their role may not see.
            comp = self._graveyarded.get(msg.get("id"))
            if comp is not None and not self._may_see(comp, self._viewer_of(ws)):
                _log.debug("dropped restore for %s: viewer not authorized",
                           msg.get("id"))
                return
            self._restore_object(msg.get("id"))
        elif kind == "draw":
            # A browser relayed a free-form drawing change. Fold it into the
            # canonical record set and relay it to the *other* browsers so every
            # open view converges. Exclude the sender (``exclude=ws``): it already
            # applied the edit locally, and echoing the diff back is harmful while
            # a record is being actively edited — a text shape mid-typing sends a
            # diff per keystroke, and over network latency the echo of an earlier
            # keystroke arrives after newer ones and `applyDiff` reverts them (the
            # cursor jumps / characters vanish). Instant on localhost, so it only
            # bit non-host devices. (Replay to fresh clients still uses _drawings.)
            # A read_only view blocks the drawing tools in the browser; enforce
            # the same rule here so a forged draw frame from a read-only viewer
            # (by role, client, or the global view) is dropped too.
            viewer = self._viewer_of(ws)
            view = self._view_for(viewer.get("id"), viewer.get("role")) or {}
            if view.get("read_only"):
                _log.debug("dropped draw: viewer's view is read_only")
                return
            diff = msg.get("diff") or {}
            self._apply_draw(diff)
            self.broadcast({"type": "draw", "diff": diff}, exclude=ws)
        elif kind == "request":
            # A panel's ``canvas.request(data)`` — the awaitable twin of input.
            # Answer it off the loop (a slow handler can't stall rendering) and
            # reply correlated by reqId. Authorized like input: request *is*
            # interaction, so a locked/invisible panel rejects it (the error
            # reply keeps the panel's Promise from hanging).
            comp = self._components.get(msg.get("id"))
            if comp is not None:
                if not self._may_operate(comp, self._viewer_of(ws)):
                    _log.debug("dropped request for %s: viewer not authorized",
                               msg.get("id"))
                    self._reply(msg.get("reqId"),
                                error="not authorized for this panel", ws=ws)
                    return
                self._dispatch.submit(
                    lambda c=comp, r=msg.get("reqId"), d=msg.get("data"):
                    self._dispatch_request(c, r, d, ws)
                )
        elif kind == "panel_error":
            comp = self._components.get(msg.get("id"))
            message = msg.get("message", "unknown error")
            if comp is not None:
                self._dispatch.submit(
                    lambda c=comp, m=message: c._dispatch_error(m)
                )
            else:
                label = msg.get("id", "?")
                print(f"\033[31m[panel error] {label}: {message}\033[0m", file=sys.stderr)
        elif kind == "snapshot":
            # Reply to a request_snapshot; hand the document to the waiter.
            waiter = self._snapshot_waiters.get(msg.get("reqId"))
            if waiter is not None:
                waiter["data"] = msg.get("data")
                waiter["event"].set()
        elif kind == "image":
            # Reply to a request_image (canvas.screenshot); base64 PNG or error.
            waiter = self._snapshot_waiters.get(msg.get("reqId"))
            if waiter is not None:
                waiter["data"] = msg.get("data")
                waiter["error"] = msg.get("error")
                waiter["event"].set()

    def _dispatch_input(self, comp, payload, ws):
        """Run a component's input handler (off the loop) and echo its state.

        Called on the dispatch thread. Passes the viewer dict (which includes
        ``role``) to ``_handle_input`` so callbacks can inspect who triggered
        the action. Echoes resulting state to other clients.
        """
        viewer = self._viewers.get(ws, {})
        comp._handle_input(payload, viewer)
        state = comp.state_payload()
        if state:
            # Echo the resulting state to the other clients. A normal browser (the
            # mover) already applied it locally, so it's excluded; a proxy (merge
            # server) is NOT excluded — it needs the echo to keep its cache current
            # and relay to its own viewers.
            echo_exclude = None if ws in self._proxy_conns else ws
            self.broadcast(
                {"type": "update", "id": comp.id, "payload": state},
                exclude=echo_exclude,
                roles=getattr(comp, "_roles", None) or None,
            )
        # A committed input is user-set state too (Slider/Toggle/TextField persist
        # their value via _layout). Arm the same debounced autosave a drag/draw
        # does, so serve(persist=) captures it even without a later layout change
        # or a clean shutdown. No-op when persistence is off (_on_mutation None).
        self._notify_mutation()

    def _dispatch_request(self, comp, req_id, data, ws=None):
        """Answer a panel's ``canvas.request`` (off the loop) and reply by reqId.

        Runs the component's request handler on the dispatch thread, then sends a
        ``response`` correlated by ``reqId`` to the requesting socket — the tab
        resolves its Promise. The requester's viewer identity is passed through
        so an ``on_request`` handler that declares a second parameter learns who
        asked. A panel with no request handler, a handler that raises, or a
        return value that isn't JSON-serialisable all come back as an ``error``
        (rejecting the Promise) rather than hanging the caller.

        An ``async def`` request handler returns a coroutine; it is run on the
        shared :class:`~danvas.kernel.AsyncKernel` loop (freeing the dispatch
        thread while it awaits) and the reply is sent when it completes.
        """
        handle = getattr(comp, "_handle_request", None)
        if handle is None:
            self._reply(req_id, error="this panel does not accept requests", ws=ws)
            return
        viewer = self._viewers.get(ws, {})
        try:
            result = handle(data, viewer)
        except Exception as exc:
            traceback.print_exc()
            self._reply(req_id, error=repr(exc), ws=ws)
            return
        if inspect.iscoroutine(result):
            def _finish(fut, req_id=req_id, ws=ws):
                try:
                    value = fut.result()
                    json.dumps(value)  # non-serialisable reply -> clean error
                except Exception as exc:
                    traceback.print_exc()
                    self._reply(req_id, error=repr(exc), ws=ws)
                    return
                self._reply(req_id, result=value, ws=ws)
            AsyncKernel.get().submit(result).add_done_callback(_finish)
            return
        try:
            json.dumps(result)  # surface a non-serialisable reply as a clean error
        except Exception as exc:
            traceback.print_exc()
            self._reply(req_id, error=repr(exc), ws=ws)
            return
        self._reply(req_id, result=result, ws=ws)

    def _reply(self, req_id, result=None, error=None, ws=None):
        """Send a ``response`` for ``req_id`` to the requesting socket.

        The reqId is minted by the requesting tab, so the response is
        meaningless — and private — to every other viewer; earlier builds
        broadcast it (other tabs ignored it by reqId), which leaked one viewer's
        reply to all connected sockets. ``ws=None`` (no known requester, e.g. a
        caller poking the method directly) falls back to broadcast.
        """
        msg = {"type": "response", "reqId": req_id}
        if error is not None:
            msg["error"] = error
        else:
            msg["result"] = result
        if ws is not None:
            self._tap_frame("out", msg)
            self._emit((ws,), msg)
        else:
            self.broadcast(msg)

    def _relay_layout(self, comp, geom, ws):
        """Relay a user move/resize to the OTHER clients (mover excluded).

        Overridden by the hub-backed bridge to a no-op: the hub folds a
        browser's layout into its replay cache and keeps the other browsers
        in step itself, so an owner echo would reach the mover — with the
        owner busy (a tight user loop), that echo arrives stale mid-drag and
        snaps the panel backwards (the jitter this method's split fixed).
        """
        self.broadcast({"type": "update", "id": comp.id, "payload": geom},
                       exclude=ws,
                       roles=getattr(comp, "_roles", None) or None)

    def _dispatch_layout(self, comp, msg, ws=None):
        """Apply a user move/resize (off the loop) and relay the new geometry.

        Relays to the *other* clients (a second browser, or a merge host) as an
        ``update`` -- the server->browser form the frontend applies. The fields
        already carry the wire units the frontend expects (canvas x/y, radian
        rotation). The mover's viewer identity is threaded through so an
        ``on_layout`` handler with a second parameter learns who rearranged it.

        ``exclude=ws`` keeps the mover from receiving its own geometry back: it
        already applied the gesture locally, so echoing it is redundant traffic
        and — the same hazard that bit free-form draw sync — a latent stale-
        overwrite were these ever sent mid-gesture (they're debounced to settle
        today, so it isn't a live bug, but excluding the sender is the right shape).
        """
        old_h = comp.h
        # Auto-flow messages (auto=True) are browser-side masonry placements.
        # If Python already has an explicit position for this component (set via
        # set_layout / deferred relative placement), the auto position must not
        # overwrite it — Python's relative layout wins. Strip x/y from the msg
        # before _apply_remote_layout so only size updates propagate.
        effective_msg = msg
        if msg.get("auto") and comp.x is not None:
            effective_msg = {k: v for k, v in msg.items() if k not in ("x", "y", "auto")}
        comp._apply_remote_layout(effective_msg, self._viewers.get(ws, {}))
        geom = {k: msg[k] for k in ("x", "y", "w", "h", "rotation", "autoH", "autoW")
                if msg.get(k) is not None}
        if geom:
            self._relay_layout(comp, geom, ws)
        if "h" in msg and old_h is not None and comp.h != old_h:
            dh = comp.h - old_h
            # Auto-height measurement arrives with h only (no x/y from a drag).
            # Track the settled h in _initial_layout so reset_layout() restores
            # the cascade-correct position, not the raw default_h placeholder.
            if "x" not in msg and "y" not in msg:
                il = getattr(comp, "_initial_layout", None)
                if il is not None:
                    il["h"] = comp.h
            self._cascade_height(comp, dh)
            # If the panel lives in a Container, repack the whole tree from the
            # root so siblings (e.g. a status bar below a growing log) shift
            # automatically.  This is a reliable Python-side fallback: the
            # browser's autoRepackForPanel handles the same case, but the
            # Python path guarantees correctness even on reconnect when the
            # frontend's panelToContainer index hasn't been seeded yet.
            container = self._panel_in_container.get(comp.id)
            if container is not None:
                root = container._root()
                if root._x is not None and root._y is not None:
                    root._sync()
        self._notify_mutation()

    def _apply_shape_layout(self, shape, msg, ws=None):
        """Apply a user move/resize of a Python-managed shape and relay it.

        Mirrors :meth:`_dispatch_layout` for non-panel shapes (canvas.geo/text/
        line/…): store the new geometry on the shape object so register_message
        replays it to a reloading/joining client, and relay it to the *other*
        live clients as a ``shape_update`` (the mover already applied it locally).
        ``x``/``y``/``rotation`` move the shape; ``w``/``h`` (when sent) resize it.
        """
        top = {}
        for k in ("x", "y", "rotation"):
            v = msg.get(k)
            if v is not None:
                setattr(shape, k, v)
                top[k] = v
        props = {}
        for k in ("w", "h"):
            v = msg.get(k)
            if v is not None:
                shape._props[k] = v
                props[k] = v
        if top or props:
            relay = {"type": "shape_update", "id": shape.id}
            relay.update(top)
            if props:
                relay["props"] = props
            self.broadcast(relay, exclude=ws)
        self._notify_mutation()

    def _cascade_height(self, comp, dh):
        """When comp's height changes by dh, shift all panels anchored below= it.

        Deps whose register frame carries `rel` are skipped: every serving
        path ships the rel-aware frontend, which re-settles those chains
        browser-side (one implementation, no wire round-trip). Only the
        legacy two-anchor placements still cascade here.
        """
        for dep, _gap in getattr(comp, "_below_deps", []):
            if getattr(dep, "_rel", None):
                continue
            if dep.id in self._components and dep.y is not None:
                self._move_y(dep, dh)

    def _graveyard_message(self):
        items = [
            {"id": c.id, "label": c._props.get("label") or c.name}
            for c in self._graveyarded.values()
        ]
        return {"type": "graveyard_update", "items": items}

    def _broadcast_graveyard(self):
        self.broadcast(self._graveyard_message())

    def _refresh_graveyard(self):
        self._broadcast_graveyard()

    def _restore_object(self, comp_id):
        """Bring a graveyarded object back to the live canvas, by its kind.

        The twin of :meth:`_graveyard`; shared by the embedded ``restore`` frame
        handler and the broker's source-side dispatcher so both resurrect an
        object the same way (re-register a shape/arrow, re-show a panel).
        """
        comp = self._graveyarded.pop(comp_id, None)
        if comp is None:
            return
        comp._graveyarded = False
        gk = getattr(comp, "_grave_kind", "panel")
        if gk == "shape":
            self._shapes[comp.id] = comp
            self.broadcast(comp.register_message())
        elif gk == "arrow":
            self._arrows[comp.id] = comp
            self.broadcast(comp.register_message())
        else:
            comp._visible = True
            self.register_live(comp)
        self._notify_mutation()  # persist the restore (serve(persist=))
        self._dispatch.submit(self._refresh_graveyard)

    def _graveyard(self, comp_id):
        """Handle a browser delete of any danvas-managed object.

        The object is kept (so the graveyard toolbar can offer Restore) but taken
        off the live canvas. Panels stay in ``_components`` flagged
        ``_graveyarded`` (callbacks/state intact); managed shapes/arrows are
        popped from ``_shapes``/``_arrows`` so the reconnect replay skips them.
        ``_grave_kind`` records which set to restore into. Ephemeral dev panels —
        the Inspector and the dispatch-trace panel — opt out via ``_ephemeral``:
        deleting one just *closes* it so it doesn't pile up in the graveyard.
        """
        comp = self._components.get(comp_id)
        if comp is not None:
            if getattr(comp, "_ephemeral", False):
                if self._canvas is not None:
                    self._canvas.remove(comp)
                else:
                    self.remove_component(comp.id)
                return
            if getattr(comp, "_graveyarded", False):
                return
            comp._graveyarded = True
            comp._visible = False
            comp._grave_kind = "panel"
            self._graveyarded[comp.id] = comp
            self._notify_mutation()  # persist the deletion (serve(persist=))
            self._dispatch.submit(self._refresh_graveyard)
            return
        # Managed shape / arrow: pop from the active set (so the reconnect replay
        # skips it) but keep the object for Restore.
        obj = self._shapes.pop(comp_id, None)
        kind = "shape"
        if obj is None:
            obj = self._arrows.pop(comp_id, None)
            kind = "arrow"
        if obj is not None:
            obj._graveyarded = True
            obj._grave_kind = kind
            self._graveyarded[comp_id] = obj
            self._notify_mutation()  # persist the deletion (serve(persist=))
            self._dispatch.submit(self._refresh_graveyard)

    def _move_y(self, comp, dh):
        """Shift comp's y by dh and propagate to every panel whose y derives from comp's."""
        new_y = comp.y + dh
        comp._store_base_layout({"y": new_y})
        # Track the cascade-settled y so reset_layout() restores the correct
        # position rather than the raw insert-time placeholder.
        il = getattr(comp, "_initial_layout", None)
        if il is not None and il.get("y") is not None:
            il["y"] = new_y
        self.broadcast({"type": "update", "id": comp.id, "payload": {"y": new_y}})
        # rel-carrying deps are the frontend's to cascade (see _cascade_height).
        for dep, _gap in getattr(comp, "_below_deps", []):
            if getattr(dep, "_rel", None):
                continue
            if dep.id in self._components and dep.y is not None:
                self._move_y(dep, dh)
        for dep, _gap in getattr(comp, "_right_of_deps", []):
            if getattr(dep, "_rel", None):
                continue
            if dep.id in self._components and dep.y is not None:
                self._move_y(dep, dh)

    async def _send(self, ws, msg):
        """Serialize and send one frame to a single socket.

        For a fan-out to many sockets go through :meth:`_emit`, which encodes the
        frame *once* and hands the same text to every recipient via
        :meth:`_send_text`; broadcasting a dict through here would re-encode it
        per socket.
        """
        await self._send_text(ws, _dumps(msg))

    async def _send_text(self, ws, text):
        """Send an already-serialized JSON frame, serialized against any other
        send to this socket.

        The shared tail of :meth:`_send` and the broadcast fan-out, so a frame
        delivered to N sockets is JSON-encoded once rather than N times.
        """
        if ws not in self._connections:
            return  # connection already torn down
        # Lazily create the per-connection lock so any code path that registers a
        # connection (incl. subclasses overriding handle_connection, e.g. the
        # merge host) gets serialized sends without having to know about it. Safe
        # without a guard: there's no await between get and setdefault.
        lock = self._send_locks.get(ws)
        if lock is None:
            lock = self._send_locks.setdefault(ws, asyncio.Lock())
        async with lock:
            await ws.send_text(text)

    async def _send_bytes(self, ws, data):
        """Send one binary frame, serialized against any other send (text or
        binary) to this socket — the websockets drain forbids overlapping
        writes, so binary media must share the same per-connection lock."""
        if ws not in self._connections:
            return
        lock = self._send_locks.get(ws)
        if lock is None:
            lock = self._send_locks.setdefault(ws, asyncio.Lock())
        async with lock:
            await ws.send_bytes(data)

    # -- outbound (thread-safe) ----------------------------------------------
    def _emit(self, targets, msg):
        """Schedule ``msg`` to each websocket in ``targets`` — the shared tail of
        :meth:`broadcast` / :meth:`send_to_role` / :meth:`send_to_client`.

        The frame is JSON-encoded *once* here and the same text handed to every
        recipient, so a broadcast to N viewers pays a single encode rather than
        one per socket. It is also scheduled onto the loop with a *single*
        cross-thread hop (:meth:`_fanout_text`), not one per socket: the
        thread-safe handoff (``run_coroutine_threadsafe``) is far costlier than an
        in-loop task, and it was the real ceiling on fan-out — total sends/sec
        stayed flat (~13k) however many viewers connected. A no-op before the loop
        exists (replay carries the state on connect); each send is wrapped in
        :meth:`_safe_send_text` so a dead socket is dropped rather than raising.
        ``targets`` is materialised by the caller (a snapshot of the
        connection/viewer map) so a concurrent connect/disconnect can't mutate it
        mid-iteration.
        """
        if self._loop is None:
            return
        targets = list(targets)
        if not targets:
            return
        text = _dumps(msg)
        asyncio.run_coroutine_threadsafe(
            self._fanout_text(targets, text), self._loop)

    async def _register_live_fanout(self, eligible, state_text, locked_sockets, locked_text):
        """Deliver register + optional state/lock frames for a register_live batch.

        All sends run concurrently as gather tasks so one backpressured socket
        can't stall others. Called via a single run_coroutine_threadsafe so
        N viewers × 3 messages cost one cross-thread hop instead of N×3.
        ``eligible`` is a list of ``(ws, reg_text)`` pairs; ``state_text`` and
        ``locked_text`` are pre-encoded and viewer-independent.
        """
        coros = [self._safe_send_text(ws, reg_text) for ws, reg_text in eligible]
        if state_text:
            coros += [self._safe_send_text(ws, state_text) for ws, _ in eligible]
        if locked_text:
            coros += [self._safe_send_text(ws, locked_text) for ws in locked_sockets]
        if coros:
            await asyncio.gather(*coros, return_exceptions=True)

    async def _fanout_text(self, targets, text):
        """Deliver one already-encoded frame to every target, concurrently.

        Spawns the per-socket sends as cheap in-loop tasks (via ``gather``) so a
        slow/backpressured socket can't hold up a fast one — the same per-socket
        independence the old one-task-per-socket scheduling had, but reached with
        a single cross-thread hop instead of N. Exceptions are swallowed per
        socket inside :meth:`_safe_send_text`; ``return_exceptions`` is belt-and-
        braces so one failure can't cancel the rest.
        """
        if len(targets) == 1:
            await self._safe_send_text(targets[0], text)
        else:
            await asyncio.gather(
                *(self._safe_send_text(ws, text) for ws in targets),
                return_exceptions=True)

    def broadcast(self, msg, exclude=None, roles=None):
        """Send ``msg`` to every connected client. Safe to call from any thread.

        ``exclude`` skips one connection (the originator of a change), used to
        avoid echoing a browser's own input straight back to it.

        ``roles`` (a non-empty list of role names) restricts delivery to viewers
        connected under one of those roles — the egress twin of the register
        replay's role filter, so a role-restricted panel's live updates never
        cross the wire to a viewer who may not see the panel. ``None``/empty
        means everyone (the default, and the meaning of an empty ``_roles``).
        """
        if self._loop is None:
            return  # not serving yet; connection replay will carry the state
        self._tap_frame("out", msg)
        self._emit(self._role_targets(roles, exclude), msg)

    def _role_targets(self, roles, exclude=None):
        """The connections a role-filtered send goes to (all, when ``roles`` is
        falsy). Snapshots the maps so a concurrent connect/disconnect on the
        loop thread can't mutate them mid-iteration."""
        if not roles:
            return [ws for ws in list(self._connections) if ws is not exclude]
        connected = self._connections
        return [ws for ws, v in list(self._viewers.items())
                if ws is not exclude and ws in connected
                and v.get("role") in roles]

    async def _safe_send(self, ws, msg):
        try:
            await self._send(ws, msg)
        except Exception:
            self._connections.discard(ws)
            self._send_locks.pop(ws, None)

    async def _safe_send_text(self, ws, text):
        """:meth:`_safe_send` for an already-serialized frame (broadcast fan-out)."""
        try:
            await self._send_text(ws, text)
        except Exception:
            self._connections.discard(ws)
            self._send_locks.pop(ws, None)

    def broadcast_binary(self, data, exclude=None, roles=None):
        """Send a pre-encoded binary frame to every client. Any-thread safe.

        Mirrors :meth:`broadcast` but for ``bytes`` (high-rate media), including
        the ``roles`` egress filter. A client that hasn't mounted the target
        panel yet simply has no handler for the frame and drops it — the next
        frame lands once it's ready.
        """
        if self._loop is None:
            return
        self._tap_binary(data)
        targets = self._role_targets(roles, exclude)
        if targets:
            asyncio.run_coroutine_threadsafe(
                self._fanout_bytes(targets, data), self._loop)

    async def _fanout_bytes(self, targets, data):
        """:meth:`_fanout_text` for a binary frame — one cross-thread hop, then
        concurrent per-socket sends."""
        if len(targets) == 1:
            await self._safe_send_binary(targets[0], data)
        else:
            await asyncio.gather(
                *(self._safe_send_binary(ws, data) for ws in targets),
                return_exceptions=True)

    async def _safe_send_binary(self, ws, data):
        try:
            await self._send_bytes(ws, data)
        except Exception:
            self._connections.discard(ws)
            self._send_locks.pop(ws, None)

    def send_to_client(self, viewer_id, msg):
        """Send ``msg`` to the one client with this viewer id. Any-thread safe.

        A no-op if no live connection carries that id (it has disconnected, or
        the id is stale). The viewer map is snapshotted before scanning so a
        concurrent connect/disconnect on the loop thread can't trip a
        "dict changed size" error -- mirrors :meth:`broadcast`.
        """
        ws = next((s for s, v in list(self._viewers.items())
                   if v.get("id") == viewer_id), None)
        self._emit((ws,) if ws is not None else (), msg)

    def send_to_role(self, role, msg):
        """Send ``msg`` to every connected client whose login role matches.

        Any-thread safe; a no-op when no one is connected under ``role``. The
        viewer map is snapshotted before scanning, like :meth:`send_to_client`.
        """
        self._emit([ws for ws, v in list(self._viewers.items())
                    if v.get("role") == role], msg)

    def _view_for(self, viewer_id, role):
        """Merge the view layers that apply to one viewer, newest layer wins.

        Precedence is global (:attr:`_view`) < per-role < per-client, so a
        client-specific override beats a role default beats the global view.
        Returns ``None`` when no layer is set (the welcome frame treats a missing
        view as "leave the canvas's defaults").
        """
        merged, have = {}, False
        for layer in (self._view, self._view_per_role.get(role),
                      self._view_per_client.get(viewer_id)):
            if layer:
                merged.update(layer)
                have = True
        return merged if have else None

    @staticmethod
    def _merge_update(existing, new_msg):
        """Fold a new update message into a pending one, newest value per key.

        Merging (not replacing) keeps partial updates from being lost: a pending
        ``set_layout(x=1)`` followed by ``set_layout(w=5)`` ends up carrying both.
        Top-level fields other than ``payload`` take the newest message's value.
        """
        if existing is None:
            return {**new_msg, "payload": dict(new_msg.get("payload") or {})}
        ep = existing.setdefault("payload", {})
        npl = new_msg.get("payload") or {}
        # ``data_patch`` carries only the keys that changed (React.update). A plain
        # replace would drop the earlier patch's keys when two updates conflate, so
        # accumulate them: shared key/value props, newest value wins.
        if isinstance(npl.get("data_patch"), dict) and isinstance(ep.get("data_patch"), dict):
            merged_patch = {**ep["data_patch"], **npl["data_patch"]}
            ep.update(npl)
            ep["data_patch"] = merged_patch
        else:
            ep.update(npl)
        for k, v in new_msg.items():
            if k != "payload":
                existing[k] = v
        return existing

    @staticmethod
    def _merge_live(existing, new_msg):
        """Coalesce a LivePlot stream frame into the pending one (see
        ``broadcast_conflated`` ``coalesce=``).

        A full ``plot`` snapshot supersedes whatever is pending (it is the whole
        current figure, e.g. after a new trace / clear / smoothing change). An
        ``plot_extend`` delta is *appended*: folded onto a pending snapshot's
        arrays, or concatenated onto a pending delta — so the single pending
        frame always represents every sample since the last send, in order.
        """
        new_payload = new_msg.get("payload") or {}
        if existing is None:
            if "plot" in new_payload:
                # Nothing pending yet and a full snapshot is arriving: snapshots
                # are never mutated in place (only extend deltas are), so skip
                # the copy on first store.
                return {**new_msg, "payload": new_payload}
            # plot_extend with nothing pending: copy so that a later
            # _coalesce_extend doesn't mutate the caller's nested arrays.
            return {**new_msg, "payload": copy.deepcopy(new_payload)}
        if "plot" in new_payload:
            # A full snapshot supersedes the pending frame; deep-copy so that
            # later _append_extend_to_snapshot calls don't alias the
            # component's own buffer or an already-queued frame.
            return {**new_msg, "payload": copy.deepcopy(new_payload)}
        ext = new_payload.get("plot_extend")
        if ext is None:
            return existing  # unrecognised frame; leave the pending one intact
        pending = existing.get("payload") or {}
        if "plot" in pending:
            Bridge._append_extend_to_snapshot(pending["plot"], ext)
        elif "plot_extend" in pending:
            Bridge._coalesce_extend(pending["plot_extend"], ext)
        return existing

    @staticmethod
    def _coalesce_extend(acc, new):
        """Concatenate one ``plot_extend`` delta onto an accumulating one, by
        trace index, trimming each trace to the rolling ``max`` if set."""
        pos = {ti: k for k, ti in enumerate(acc["indices"])}
        for j, ti in enumerate(new["indices"]):
            if ti in pos:
                k = pos[ti]
                acc["x"][k] = acc["x"][k] + new["x"][j]
                acc["y"][k] = acc["y"][k] + new["y"][j]
            else:
                pos[ti] = len(acc["indices"])
                acc["indices"].append(ti)
                acc["x"].append(list(new["x"][j]))
                acc["y"].append(list(new["y"][j]))
        mx = new.get("max")
        if mx is not None:
            acc["max"] = mx
            for k in range(len(acc["x"])):
                if len(acc["x"][k]) > mx:
                    acc["x"][k] = acc["x"][k][-mx:]
                    acc["y"][k] = acc["y"][k][-mx:]

    @staticmethod
    def _append_extend_to_snapshot(plot, ext):
        """Append a ``plot_extend`` delta's points onto a pending full snapshot's
        trace arrays, so a snapshot waiting to be sent stays current."""
        data = plot.get("data") or []
        mx = ext.get("max")
        for j, ti in enumerate(ext["indices"]):
            if 0 <= ti < len(data):
                tr = data[ti]
                tr["x"] = list(tr.get("x") or []) + ext["x"][j]
                tr["y"] = list(tr.get("y") or []) + ext["y"][j]
                if mx is not None and len(tr["x"]) > mx:
                    tr["x"] = tr["x"][-mx:]
                    tr["y"] = tr["y"][-mx:]

    def broadcast_conflated(self, comp_id, *, msg=None, data=None, exclude=None,
                            tap=True, coalesce=False, roles=None):
        """Broadcast an update under the ``latest`` queue policy.

        Keeps only the most recent pending value per viewer for this component,
        dropping stale ones: dict updates merge newest-per-key (so partial
        updates survive), binary frames replace wholesale. The per-viewer backlog
        is bounded to one in-flight send plus one pending value, so a fast
        producer (e.g. a camera) can't accumulate latency on a slow client.

        ``coalesce=True`` switches the merge from *replace* to *append* for
        LivePlot stream frames (:meth:`_merge_live`): instead of dropping the
        stale pending frame, the new points are folded into it, so a client that
        falls behind a fast producer gets one catch-up frame carrying every point
        it missed — no backlog (the frame rate self-throttles to what the client
        can render) and no loss (every sample is delivered, in order). This is
        what lets a 75 push/s stream stay live without queuing 75 redraws/s at a
        client that can only paint a handful.

        Pass exactly one of ``msg`` (a dict to JSON-send) or ``data`` (bytes).
        ``roles`` applies the same egress filter as :meth:`broadcast`.
        """
        if self._loop is None:
            return
        # tap=False suppresses wire-debug observation for high-rate internal
        # relays (cursor positions) that would otherwise flood on_frame/debug.
        if tap:
            if data is not None:
                self._tap_binary(data)
            else:
                self._tap_frame("out", msg)
        kind = "bin" if data is not None else "msg"
        for ws in self._role_targets(roles, exclude):
            key = (ws, comp_id, kind)
            with self._conflate_lock:
                if kind == "bin":
                    self._conflate_pending[key] = ("bin", data)
                else:
                    prev = self._conflate_pending.get(key)
                    merge = self._merge_live if coalesce else self._merge_update
                    merged = merge(prev[1] if prev else None, msg)
                    self._conflate_pending[key] = ("msg", merged)
                if key in self._conflate_active:
                    continue  # a sender is draining this slot; it'll see the latest
                self._conflate_active.add(key)
            asyncio.run_coroutine_threadsafe(
                self._conflated_sender(key), self._loop
            )

    async def _conflated_sender(self, key):
        """Drain a conflated slot until empty, sending only its latest value.

        New values that land while a send is in flight overwrite/merge the slot,
        so intermediate ones are skipped rather than queued.
        """
        ws = key[0]
        while True:
            with self._conflate_lock:
                item = self._conflate_pending.pop(key, None)
                if item is None:
                    self._conflate_active.discard(key)
                    return
            kind, val = item
            try:
                if kind == "bin":
                    await self._send_bytes(ws, val)
                else:
                    await self._send(ws, val)
            except Exception:
                self._connections.discard(ws)
                self._send_locks.pop(ws, None)
                with self._conflate_lock:
                    self._conflate_pending.pop(key, None)
                    self._conflate_active.discard(key)
                return

    def _drop_conflate(self, ws):
        """Forget any conflated state for a closed connection."""
        with self._conflate_lock:
            for key in [k for k in self._conflate_pending if k[0] is ws]:
                self._conflate_pending.pop(key, None)
            for key in [k for k in self._conflate_active if k[0] is ws]:
                self._conflate_active.discard(key)

    def _apply_draw(self, diff):
        """Fold a record-store diff into the canonical free-form record set.

        ``added``/``updated`` carry full records (``updated`` as ``[from, to]``
        pairs, of which we keep the new value); ``removed`` carries the dropped
        records by id. This mirrors the frontend's store applyDiff on the wire so
        the server's cache stays in step with every browser.
        """
        for rid, rec in (diff.get("added") or {}).items():
            self._drawings[rid] = rec
        for rid, pair in (diff.get("updated") or {}).items():
            if isinstance(pair, (list, tuple)) and len(pair) == 2:
                self._drawings[rid] = pair[1]
        for rid in (diff.get("removed") or {}):
            self._drawings.pop(rid, None)
        _log.debug("draw sync: %d total records stored", len(self._drawings))
        self._notify_mutation()
        if self._draw_taps:
            self._dispatch.submit(lambda d=dict(diff): self._tap_draw(d))

    def _notify_mutation(self):
        """Fire the optional ``_on_mutation`` listener (set by serve(persist=)).

        Swallows the listener's exceptions so a failing autosave can never break
        layout/draw sync -- the wire path must keep flowing regardless.
        """
        cb = self._on_mutation
        if cb is not None:
            try:
                cb()
            except Exception:
                traceback.print_exc()

    def _panel_shape_ids(self):
        """Shape ids of every danvas-managed entity (panels, arrows, shapes).

        The frontend keys all shapes as ``shape:<id>``; these are the ones we
        own and want to exclude from a saved canvas, leaving only the user's
        free-form drawings.
        """
        return (
            [f"shape:{cid}" for cid in self._components]
            + [f"shape:{aid}" for aid in self._arrows]
            + [f"shape:{sid}" for sid in self._shapes]
        )

    # -- user-drawing snapshot (request/response with the browser) ------------
    def _warn_if_blocking_on_dispatch(self, what):
        """Warn when a blocking browser round-trip runs on the dispatch thread.

        :meth:`request_snapshot` / :meth:`request_image` block the calling thread
        until a browser replies. Called from inside an input handler — which runs
        on the shared dispatch thread (see :attr:`_dispatch`) — that blocks every
        *other* panel's handler and input echo for the whole round-trip. It is not
        a deadlock (the reply still arrives on the event loop and releases the
        wait), but it freezes the canvas's responsiveness, so steer the caller to
        do the slow work off that thread. The default warnings filter shows this
        once per call site, so a handler that does it every time isn't spammed.
        """
        if self._dispatch.is_current_thread():
            warnings.warn(
                f"{what} was called from inside an input handler (the shared "
                "dispatch thread); blocking there stalls every other panel's "
                "handlers until the browser replies. Run it from a handler "
                "marked threaded=True (or your own background thread) instead.",
                stacklevel=3,
            )

    def request_snapshot(self, timeout=5.0):
        """Ask a connected browser for the user's free-form drawings.

        Returns the canvas "content" (shapes/bindings/assets) for everything on the
        canvas *except* the danvas panels and connector arrows — those are
        recreated from Python code, not persisted. The browser is the source of
        truth for free-form drawings, so this round-trips over the socket and
        blocks the calling thread until a reply arrives (or ``timeout`` elapses).
        Requires at least one open client.
        """
        self._warn_if_blocking_on_dispatch("canvas.save()")
        if not (self._connections or self._viewers):
            raise RuntimeError("no connected browser to read the canvas from")
        req_id = uuid.uuid4().hex
        waiter = {"event": threading.Event(), "data": None}
        self._snapshot_waiters[req_id] = waiter
        try:
            self.broadcast({
                "type": "get_snapshot",
                "reqId": req_id,
                "panelIds": self._panel_shape_ids(),
            })
            if not waiter["event"].wait(timeout):
                raise TimeoutError("timed out waiting for the canvas snapshot")
            return waiter["data"]
        finally:
            self._snapshot_waiters.pop(req_id, None)

    def request_image(self, shape_ids, timeout=10.0):
        """Ask a connected browser to render ``shape_ids`` to a PNG.

        Round-trips over the socket like :meth:`request_snapshot` and blocks the
        calling thread until the browser replies (or ``timeout`` elapses).
        ``shape_ids`` empty means the whole page. Returns raw PNG bytes; requires
        at least one open client (the browser is the only thing that can render).
        """
        self._warn_if_blocking_on_dispatch("canvas.screenshot()")
        if not (self._connections or self._viewers):
            raise RuntimeError("no connected browser to capture from")
        req_id = uuid.uuid4().hex
        waiter = {"event": threading.Event(), "data": None, "error": None}
        self._snapshot_waiters[req_id] = waiter
        try:
            self.broadcast({
                "type": "get_image",
                "reqId": req_id,
                "shapeIds": list(shape_ids),
            })
            if not waiter["event"].wait(timeout):
                raise TimeoutError("timed out waiting for the screenshot")
            if waiter["error"] or not waiter["data"]:
                raise RuntimeError(
                    f"screenshot failed: {waiter['error'] or 'no image returned'}")
            import base64
            return base64.b64decode(waiter["data"])
        finally:
            self._snapshot_waiters.pop(req_id, None)

    # -- shared React assets (canvas.define / canvas.style) -------------------
    def shared_message(self):
        """The ``shared`` frame: every defined component source + the global CSS."""
        return {"type": "shared",
                "components": dict(self._shared_components),
                "styles": self._shared_styles}

    def broadcast_shared(self):
        """Push the current shared components/styles to every connected browser."""
        self.broadcast(self.shared_message())

    def load_snapshot(self, data):
        """Push saved user drawings to connected browsers (merged onto the page).

        The content is *added* to the live canvas, so the code-created panels
        stay put and the drawings reappear on top of them. Remembered so a
        client that connects (or reloads) later is sent the same drawings,
        making them survive page reloads.
        """
        self._loaded_doc = data
        self.broadcast({"type": "load_snapshot", "data": data})