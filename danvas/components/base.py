"""Base class shared by all danvas components."""

import functools
import inspect
import math
import os
import threading
import time
import traceback
import warnings

from .. import _trace
from .._flags import LAYOUT_FLAGS
from ..kernel import AsyncKernel, DedicatedKernel, spawn
from . import _theme


def _mark_threaded(fn):
    """Tag a callback so the dispatcher runs it on its own thread (``spawn``).

    Sets a marker attribute on the callable. Bound methods and builtins can't
    take attributes, so those fall back to a thin ``functools.wraps`` wrapper
    (which preserves the signature, so the viewer-arity detection still works).
    """
    try:
        fn._danvas_threaded = True
        return fn
    except (AttributeError, TypeError):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)
        wrapper._danvas_threaded = True
        return wrapper


_HANDLER_QUEUES = ("fifo", "latest")


def _run_handler(fn, args, wait=False):
    """Invoke a handler, transparently supporting ``async def``.

    Detection is on the *returned* coroutine, not ``iscoroutinefunction`` on the
    callable — the routing/tracing wrappers around a handler (``_with_fields``,
    ``_traced``) are plain functions that *return* the inner coroutine, so only
    the result reliably reveals an async handler.

    A coroutine is run on the shared :class:`~danvas.kernel.AsyncKernel` loop.
    ``wait=True`` blocks until it finishes and re-raises its exception (used by
    the ``threaded``/``dedicated`` modes, whose thread *is* the concurrency /
    serialisation unit); ``wait=False`` schedules it and returns immediately
    with failures logged (the inline mode — this is what frees the shared
    dispatch thread while an async handler awaits).
    """
    result = fn(*args)
    if inspect.iscoroutine(result):
        kernel = AsyncKernel.get()
        if wait:
            kernel.submit(result).result()
        else:
            kernel.spawn(result)
        return None
    return result


def _mark_dedicated(fn, queue_mode="fifo"):
    """Tag a callback for a :class:`~danvas.kernel.DedicatedKernel`.

    Like :func:`_mark_threaded` but for handlers that should run on a
    persistent per-handler thread rather than a freshly spawned one per call.
    The kernel is created lazily on first dispatch and lives for the app's
    lifetime. ``queue_mode`` sets the backpressure policy on that thread's
    own queue (``"fifo"`` or ``"latest"`` — see :class:`DedicatedKernel`).
    """
    if queue_mode not in _HANDLER_QUEUES:
        raise ValueError(
            f"queue must be one of {_HANDLER_QUEUES}, got {queue_mode!r}"
        )
    try:
        fn._danvas_dedicated = True
        fn._danvas_handler_queue = queue_mode
        return fn
    except (AttributeError, TypeError):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)
        wrapper._danvas_dedicated = True
        wrapper._danvas_handler_queue = queue_mode
        return wrapper


# The danvas package directory, used to tell a *user's* handler from danvas's
# own internal callbacks (e.g. the layout ``_deferred`` Canvas.insert registers).
# The dispatch trace reports only user handlers — package internals are noise.
_DANVAS_PKG_DIR = os.path.normcase(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _unwrap(cb):
    """Follow ``__wrapped__`` to the user's function under a ``functools.wraps``
    shim (danvas wraps only un-attributable callables — see ``_mark_threaded``)."""
    fn = cb
    seen = set()
    while hasattr(fn, "__wrapped__") and id(fn) not in seen:
        seen.add(id(fn))
        fn = fn.__wrapped__
    return fn


_USER_FILE_CACHE = {}


def _is_user_handler(cb):
    """True when ``cb`` is defined in the user's code rather than inside danvas.

    Keeps the dispatch trace shallow: only the handlers the user wrote show up,
    not danvas's own internal callbacks (layout deferral, inspector wiring, …).
    Callables with no inspectable code (builtins/C) are treated as non-user. The
    per-file verdict is memoised — this runs on every dispatch once recording is
    armed, and path normalisation isn't free."""
    code = getattr(_unwrap(cb), "__code__", None)
    if code is None:
        return False
    fname = code.co_filename
    verdict = _USER_FILE_CACHE.get(fname)
    if verdict is None:
        path = os.path.normcase(os.path.abspath(fname))
        verdict = not path.startswith(_DANVAS_PKG_DIR + os.sep)
        _USER_FILE_CACHE[fname] = verdict
    return verdict


def _handler_label(cb):
    """A human label for a handler in a dispatch trace: its qualified name plus
    source location (``file:line``), so the anonymous ``def _`` handlers — all
    named ``_`` — stay distinguishable."""
    fn = _unwrap(cb)
    name = (getattr(fn, "__qualname__", None)
            or getattr(fn, "__name__", None) or repr(fn))
    code = getattr(fn, "__code__", None)
    if code is None:
        return name
    return f"{name} ({os.path.basename(code.co_filename)}:{code.co_firstlineno})"


class HandlerInfo:
    """One registered handler, as :attr:`BaseComponent.handlers` reports it.

    ``fn`` is the original function (unwrapped from any dispatch-mode
    wrapper), so it can be called or inspected; the repr is the readable
    summary the Inspector's detail view shows.
    """

    __slots__ = ("fn", "name", "mode", "queue")

    def __init__(self, fn, name, mode, queue):
        self.fn = fn
        self.name = name
        self.mode = mode          # "inline" | "threaded" | "dedicated"
        self.queue = queue        # backpressure policy; dedicated mode only

    def __repr__(self):
        q = f"/{self.queue}" if self.queue else ""
        return f"<handler {self.name} [{self.mode}{q}]>"


def _describe_handlers(callbacks):
    """The :class:`HandlerInfo` list for one handler store."""
    out = []
    for cb in callbacks:
        dedicated = getattr(cb, "_danvas_dedicated", False)
        out.append(HandlerInfo(
            fn=_unwrap(cb),
            name=_handler_label(cb),
            mode=("dedicated" if dedicated
                  else "threaded" if getattr(cb, "_danvas_threaded", False)
                  else "inline"),
            queue=getattr(cb, "_danvas_handler_queue", "fifo")
                  if dedicated else None,
        ))
    return out


class BaseComponent:
    """A bidirectional canvas component.

    Subclasses set ``component`` (the type string sent to the browser) and
    implement :meth:`update`. State (``_value``) is written by the asyncio
    WebSocket handler thread and read from user threads, so access is guarded
    by a per-component lock.
    """

    component = "Base"

    # Default panel size in pixels, mirroring the frontend's getDefaultProps so
    # ``comp.w``/``comp.h`` always read a real number (and can be incremented)
    # even when size isn't given to ``insert``. Override per component.
    default_w = 240
    default_h = 96

    # Send-queue policy under backpressure (a slow/late browser). Any component
    # may pass ``queue=`` to choose how its own updates behave when they outpace
    # the connection:
    #   "fifo"   -> every update is delivered in order, nothing dropped (default;
    #               right for controls/labels where each value matters).
    #   "latest" -> keep only the newest pending value per viewer, dropping stale
    #               ones (right for live media/telemetry; VideoFeed's default).
    # Dict updates under "latest" merge newest-per-key so partial updates (e.g.
    # set_layout) aren't lost; binary frames replace wholesale.
    _QUEUE_POLICIES = ("fifo", "latest")

    def __init__(self, name=None, label=None, queue="fifo", **props):
        if queue not in self._QUEUE_POLICIES:
            raise ValueError(
                f"queue must be one of {self._QUEUE_POLICIES}, got {queue!r}"
            )
        self._queue = queue
        self.id = None
        # ``name`` is the unique identity / ``canvas.<name>`` handle (Canvas.insert
        # may still override it). ``label`` is only the caption shown on the panel
        # and defaults to the name, so naming a component is enough to caption it.
        self.name = name
        self._props = dict(props)
        caption = label if label is not None else name
        if caption is not None:
            self._props["label"] = caption
        # Ensure size is always present so w/h read/increment without surprises.
        self._props.setdefault("w", self.default_w)
        self._props.setdefault("h", self.default_h)
        self._value = None
        self._callbacks = []
        self._layout_callbacks = []
        self._error_callbacks = []
        # Dedicated-handler kernels: id(callback) -> DedicatedKernel. Created
        # lazily on first dispatch so no thread is started for a handler that
        # never fires. Keyed by id(cb) because the callback objects are the
        # stable identity. Only written/read from the dispatch thread.
        self._dedicated_kernels = {}
        # Role-based access: _roles limits which viewer roles see this panel;
        # empty means all roles. _lock_for makes the panel non-interactive for
        # the listed roles on connect (operable=False sent per-client).
        self._roles = []
        self._lock_for = []
        self._bridge = None
        # The owning Canvas, set by insert(); used by panels that reach canvas-level
        # primitives (e.g. Download -> canvas.serve_bytes). None until inserted.
        self._canvas = None
        # True once the component has been inserted on the canvas and is
        # currently visible to browsers. Set to False before insert and by
        # canvas.hide(); restored to True by canvas.unhide(). While False,
        # _send_update and _send_binary are no-ops so a hidden panel never
        # sends live updates to the browser.
        self._visible = False
        self._lock = threading.Lock()
        # Optional canvas placement (x, y) in canvas coordinates; None = let the
        # frontend auto-cascade. Set by Canvas.insert. Width/height are passed
        # through register_props instead (they are real shape props).
        self._position = None
        # Panels placed below= or right_of= this one; populated by Canvas.insert
        # so height/y changes cascade through the layout chain automatically.
        self._below_deps = []
        self._right_of_deps = []
        # Rotation in degrees (clockwise). Defaults to 0 (unrotated) so it can be
        # read and incremented. Like position, it is a top-level shape field.
        self._rotation = 0
        # Opacity: 0.0 = fully transparent, 1.0 = fully opaque. The shape's top-level
        # field (same tier as x/y/rotation). Omitted from messages at the default
        # so existing wire protocol is unchanged.
        self._opacity = 1.0
        # Lock / chrome flags, all defaulted from the single table in _flags.py:
        # ``locked`` (full lock, top-level isLocked); ``draggable`` /
        # ``resizable`` / ``operable`` / ``grabbable`` (interaction-preserving
        # locks carried in the shape's ``meta``); ``frame`` (the card
        # chrome). See danvas/_flags.py for the per-flag semantics, the wire
        # keys, and the property docstrings generated at the bottom of this file.
        for _flag in LAYOUT_FLAGS.values():
            setattr(self, _flag.attr, _flag.default)
        # Per-viewer layout overlays (the layout twin of React's prop overlays):
        # role / client id -> {x, y, w, h, rotation, <flag name>}. Merged onto the
        # base geometry for that viewer (precedence shared < role < client), so
        # ``set_layout(roles=...)`` and a user's drag-to-their-own-layer persist
        # and replay on reconnect.
        self._role_layout = {}
        self._client_layout = {}

    # -- wiring (called by Canvas.insert) ------------------------------------
    def _bind(self, component_id, bridge):
        self.id = component_id
        self._bridge = bridge
        self._visible = True

    # -- read ----------------------------------------------------------------
    @property
    def visible(self):
        """``True`` when this panel is currently shown on the canvas.

        ``False`` before :meth:`~danvas.Canvas.insert`, after
        :meth:`~danvas.Canvas.hide` or :meth:`~danvas.Canvas.remove`, and
        while the panel is in the graveyard (user-deleted from the UI).
        Check this to distinguish inserted-and-visible from inserted-but-hidden.
        """
        return self._visible

    @visible.setter
    def visible(self, shown):
        # `panel.visible = False` is canvas.hide(panel); True is unhide —
        # reversible (state/callbacks intact), same as calling the methods.
        canvas = getattr(self._bridge, "_canvas", None) \
            if self._bridge is not None else None
        if canvas is None:
            raise RuntimeError(
                "panel isn't on a canvas yet — insert it before setting visible")
        if bool(shown) == self._visible:
            return
        (canvas.unhide if shown else canvas.hide)(self)

    @property
    def owner(self):
        """Which process executes this panel's handlers, as its peers see it:
        the bridge's owner label (``"host"`` for a serving canvas, the dial-in
        label for a :func:`danvas.connect` canvas), or ``None`` before insert.
        The cross-process twin is ``RemoteHandle.owner`` — so
        ``canvas["x"].owner`` answers the same question for local and foreign
        panels alike."""
        if self._bridge is None:
            return None
        return getattr(self._bridge, "_owner_label", "host")

    @property
    def label(self):
        """The card title shown on the panel header."""
        return self._props.get("label")

    @label.setter
    def label(self, value):
        self._props["label"] = value
        self._send_update({"label": value})

    @property
    def value(self):
        with self._lock:
            return self._value

    @property
    def queue(self):
        """This component's send-queue policy (``"fifo"`` or ``"latest"``).

        Settable on any component so its backpressure behaviour can be chosen
        without a constructor argument, e.g. ``plot.queue = "latest"`` to drop
        stale telemetry for slow viewers. See the class docstring for semantics.
        """
        return self._queue

    @queue.setter
    def queue(self, policy):
        if policy not in self._QUEUE_POLICIES:
            raise ValueError(
                f"queue must be one of {self._QUEUE_POLICIES}, got {policy!r}"
            )
        self._queue = policy

    # -- role-based visibility -----------------------------------------------
    @property
    def roles(self):
        """The viewer roles allowed to see this panel (``[]`` means all roles).

        This is the live form of the ``roles=`` argument on the factory
        (``canvas.react(..., roles=[...])``). Read-only; use :meth:`add_role` /
        :meth:`remove_role` to change it so connected viewers update too.
        """
        return list(self._roles)

    def add_role(self, *roles):
        """Allow these viewer roles to see the panel, live.

        Appends to the role allowlist; roles already present are ignored. A
        viewer currently connected under a newly added role is sent the panel
        immediately, and later connections get it via the normal replay — so a
        panel can be revealed to a role created after the server started (e.g. a
        team whose password the admin just set). Returns ``self``.
        """
        added = [r for r in roles if r not in self._roles]
        self._roles.extend(added)
        if added and self._bridge is not None:
            self._bridge.register_live(self, only_roles=set(added))
        return self

    def remove_role(self, *roles):
        """Disallow these viewer roles from seeing the panel, live.

        Removes them from the allowlist and tells any viewer currently connected
        under a removed role to drop the panel. Roles not present are ignored.
        Note that emptying the allowlist entirely means "visible to all roles",
        so removing the last role *shows* the panel to everyone rather than
        hiding it — keep at least one role (or re-add ``roles=``) to stay
        restricted. Returns ``self``.
        """
        removed = [r for r in roles if r in self._roles]
        for r in removed:
            self._roles.remove(r)
        # Only drop it live while the panel is still role-restricted; if the
        # allowlist is now empty the panel is visible to everyone, so those
        # viewers should keep it.
        if removed and self._roles and self._bridge is not None:
            for r in removed:
                self._bridge.send_to_role(r, {"type": "remove", "id": self.id})
        return self

    # -- layout (read public state; writes move/resize live) -----------------
    @property
    def x(self):
        return self._position[0] if self._position else None

    @x.setter
    def x(self, value):
        self.set_layout(x=value)

    @property
    def y(self):
        return self._position[1] if self._position else None

    @y.setter
    def y(self, value):
        self.set_layout(y=value)

    @property
    def w(self):
        return self._props.get("w")

    @w.setter
    def w(self, value):
        # ``comp.w = "auto"`` is the live form of ``w="auto"`` at insert: fit the
        # width to the content's natural width rather than shipping the literal
        # string to the frontend. Only Custom-based panels can measure their
        # content, so the base implementation explains why it can't.
        if value == "auto":
            self._set_auto_w()
        else:
            self._set_fixed_w(value)

    def _set_auto_w(self):
        """Turn on content-fit width (``w="auto"``).

        Overridden by :class:`~danvas.Custom`, the only panel whose content is
        measurable in the browser. The base panel can't fit its width, so this
        warns and leaves the width as-is.
        """
        warnings.warn(
            f"w='auto' is only supported on Custom-based panels; "
            f"{type(self).__name__} keeps its current width", stacklevel=3,
        )

    def _set_fixed_w(self, value):
        """Set an explicit pixel width. Custom overrides this to also leave
        auto-width mode, so a numeric assignment after ``w="auto"`` sticks."""
        self.set_layout(w=value)

    @property
    def h(self):
        """The panel height in canvas units.

        For an ``h="auto"`` panel this is the DEFAULT placeholder until a
        browser renders the content, measures it, and reports back — then it
        becomes the settled height (kept current across content changes).
        Before that report the number is a guess: don't do absolute-position
        arithmetic against it — anchor with ``below=``/``above=`` instead,
        whose cascade is resolved browser-side after real measurement.
        """
        return self._props.get("h")

    @h.setter
    def h(self, value):
        # ``comp.h = "auto"`` is the live form of ``h="auto"`` at insert: fit the
        # height to the rendered content rather than shipping the literal string
        # to the frontend (which expects a number). Only Custom-based panels can
        # measure their content, so the base implementation just explains why it
        # can't and leaves the height unchanged.
        if value == "auto":
            self._set_auto_h()
        else:
            self._set_fixed_h(value)

    def _set_auto_h(self):
        """Turn on content-fit height (``h="auto"``).

        Overridden by :class:`~danvas.Custom` (and its subclasses — markdown,
        table, image, …), the only panels whose content is measurable in the
        browser. The base panel has a fixed height it can't fit, so this warns
        and leaves the height as-is rather than failing or silently breaking.
        """
        warnings.warn(
            f"h='auto' is only supported on Custom-based panels (custom, "
            f"markdown, table, image, …); {type(self).__name__} keeps its "
            f"current height", stacklevel=3,
        )

    def _set_fixed_h(self, value):
        """Set an explicit pixel height. Custom-based panels override this to
        also leave auto-height mode, so a numeric assignment after ``h="auto"``
        sticks instead of being overridden by the iframe's fit loop."""
        self.set_layout(h=value)

    @property
    def rotation(self):
        """Rotation in degrees (clockwise); 0 if unrotated."""
        return self._rotation

    @rotation.setter
    def rotation(self, value):
        self.set_layout(rotation=value)

    @property
    def opacity(self):
        """Panel opacity: 0.0 = fully transparent, 1.0 = fully opaque."""
        return self._opacity

    @opacity.setter
    def opacity(self, value):
        self.set_layout(opacity=value)

    # The lock/chrome flag properties (``locked``, ``draggable``, ``resizable``,
    # ``operable``, ``grabbable``, ``frame``) are generated from LAYOUT_FLAGS at
    # the bottom of this module — one read-back property + a setter that routes
    # through set_layout for each. See danvas/_flags.py.

    # -- registration / initial sync ----------------------------------------
    def register_props(self):
        """Props sent in the ``register`` message to build the shape."""
        return dict(self._props)

    def register_props_for(self, role=None, client_id=None):
        """Register props for one connecting viewer.

        The base ignores ``role``/``client_id`` and returns the shared props;
        components with per-viewer prop overlays (e.g. :meth:`React.update` with
        ``roles=``) override this so a reconnecting viewer replays the slice it
        should see (precedence shared < role < client, mirroring ``set_view``).
        """
        return self.register_props()

    def state_payload(self):
        """Current state pushed right after register (None = nothing)."""
        return None

    # -- persistence (save / load / serve(persist=)) -------------------------
    def _persist_state(self):
        """A JSON-able snapshot of this panel's *restorable* state, or ``{}``.

        The default persists nothing: a panel's content is normally reproduced
        by re-running the code that built it (panels are code). Input controls
        whose *value the user sets* (Slider/Toggle/TextField) override this via
        :class:`_ValuePersist`, so that value survives a restart. Captured by
        ``Canvas._layout`` and replayed by ``_restore_layout`` — so :meth:`save`,
        :meth:`load`, and ``serve(persist=)`` all carry it through one code path.
        """
        return {}

    def _restore_state(self, saved):
        """Apply a dict produced by :meth:`_persist_state` (default: no-op)."""
        return

    def persist(self, get, set):
        """Opt a custom panel into ``save``/``load``/``serve(persist=)`` state.

        ``get()`` returns a JSON-able snapshot of the panel's *restorable* state;
        ``set(snapshot)`` re-applies it on load / a restart — restore your Python
        state **and** push it back to the panel so the UI reflects it again
        (typically with ``update()``/``push()``). The built-in input controls
        (Slider/Toggle/TextField) persist their value automatically; this is the
        *same* mechanism, exposed so a hand-built ``react()``/``custom()`` panel
        can match them — its value survives a restart instead of resetting.

        Matched across runs by the panel's ``name``, so keep it stable. The
        snapshot must be JSON-serialisable; a restore that raises is logged and
        skipped (a bad/obsolete snapshot never breaks a load). Returns the panel,
        so it chains off the factory::

            state = {"value": 50}                       # the panel's Python state
            slider = canvas.react(MY_SLIDER_JSX, props={"value": 50})
            @slider.on_message
            def _(m): state["value"] = m["value"]       # browser -> Python
            slider.persist(lambda: state,               # -> the saved snapshot
                           lambda s: (state.update(s),  # <- restore Python state
                                      slider.update(**s)))  # <- and the panel UI
        """
        if not callable(get) or not callable(set):
            raise TypeError("persist(get, set) takes two callables")

        def _persist():
            return {"state": get()}

        def _restore(saved):
            if not isinstance(saved, dict) or "state" not in saved:
                return
            try:
                set(saved["state"])
            except Exception:
                # An obsolete/invalid snapshot must never break a load — skip it,
                # loudly (same policy as the built-in _ValuePersist restore).
                traceback.print_exc()

        # Override the per-instance hooks the save/load/serve(persist=) path calls.
        self._persist_state = _persist
        self._restore_state = _restore
        return self

    # -- write (Python -> browser) -------------------------------------------
    def update(self, *args, **kwargs):  # pragma: no cover - overridden
        raise NotImplementedError

    def _send_update(self, payload):
        if self._bridge is None or not self._visible:
            return
        msg = {"type": "update", "id": self.id, "payload": payload}
        # A role-restricted panel's live updates go only to viewers whose role may
        # see it — the same filter its register replay applies. Without this the
        # content still crossed the wire to every authenticated socket (dropped
        # client-side for lack of a registered shape, but readable in devtools).
        roles = self._roles or None
        if self._queue == "latest":
            # Drop stale pending updates; merge newest-per-key (see bridge).
            self._bridge.broadcast_conflated(self.id, msg=msg, roles=roles)
        else:
            self._bridge.broadcast(msg, roles=roles)

    def _send_update_to(self, payload, role=None, client_id=None):
        """Send an update to specific viewers only, leaving broadcast state alone.

        Routes by login ``role`` (a name or list, matching ``serve(passwords=)``)
        and/or ``client_id`` (an id from ``canvas.viewers``). Backs the
        per-recipient writes (e.g. :meth:`React.update_for`); a no-op before the
        server is running, like the other sends.
        """
        if self._bridge is None or not self._visible:
            return
        msg = {"type": "update", "id": self.id, "payload": payload}
        if client_id is not None:
            self._bridge.send_to_client(client_id, msg)
        if role is not None:
            for r in ([role] if isinstance(role, str) else role):
                self._bridge.send_to_role(r, msg)

    # Subclasses that stream binary frames (VideoFeed, AudioFeed, Custom, React)
    # set this to their BINARY_* constant so push_binary can use it without
    # each subclass reimplementing the one-liner.
    BINARY_TYPE = None

    def push_binary(self, data):
        """Stream raw bytes to the browser on a binary WebSocket frame.

        ``data`` may be ``bytes``, ``bytearray``, or ``memoryview``. Honours the
        panel's ``queue`` policy: ``"latest"`` drops stale pending frames for a
        slow viewer; ``"fifo"`` delivers every frame in order.

        Subclasses set :attr:`BINARY_TYPE` to the appropriate
        ``BINARY_VIDEO`` / ``BINARY_AUDIO`` / ``BINARY_CUSTOM`` /
        ``BINARY_REACT`` constant from :mod:`danvas.bridge`.
        """
        self._send_binary(self.BINARY_TYPE, bytes(data))

    def _send_binary(self, type_code, payload):
        """Push raw bytes to the browser as a binary frame, keyed by this id.

        For high-rate media (e.g. video frames): the payload skips base64/JSON
        and is fed straight into a Blob/ArrayBuffer on the frontend. ``payload``
        must be ``bytes``; ``type_code`` selects the frontend handler. Under the
        ``latest`` queue policy a stale pending frame is dropped in favour of the
        newest, so a fast feed can't back up a slow viewer.
        """
        if self._bridge is None or not self._visible:
            return
        from ..bridge import encode_binary_frame
        frame = encode_binary_frame(type_code, self.id, payload)
        # Same role egress filter as _send_update: a restricted panel's media
        # frames reach only the viewers who may see the panel.
        roles = self._roles or None
        if self._queue == "latest":
            self._bridge.broadcast_conflated(self.id, data=frame, roles=roles)
        else:
            self._bridge.broadcast_binary(frame, roles=roles)

    # -- live layout (Python -> browser) -------------------------------------
    def move(self, x, y):
        """Move this panel to ``(x, y)`` in canvas coordinates, live."""
        self.set_layout(x=x, y=y)

    def resize(self, w=None, h=None):
        """Resize this panel, live. Either dimension may be omitted."""
        self.set_layout(w=w, h=h)

    def rotate(self, degrees):
        """Rotate this panel to ``degrees`` (clockwise), live."""
        self.set_layout(rotation=degrees)

    def lock(self):
        """Fully lock the panel (no move, resize, or interaction), live."""
        self.set_layout(locked=True)

    def unlock(self):
        """Release a full lock so the panel responds normally again, live."""
        self.set_layout(locked=False)

    def pin(self):
        """Pin in place and fix size, but keep controls interactive, live.

        Shorthand for ``set_layout(draggable=False, resizable=False)`` — unlike
        :meth:`lock`, sliders and buttons on the panel still work.
        """
        self.set_layout(draggable=False, resizable=False)

    def unpin(self):
        """Allow dragging and resizing again, live."""
        self.set_layout(draggable=True, resizable=True)

    # -- stacking order (z-index) --------------------------------------------
    def to_front(self):
        """Raise this panel above every other panel, live.

        Persists across reload: a reconnecting client rebuilds the panel on top.
        """
        self._send_order("front")

    def to_back(self):
        """Lower this panel beneath every other panel, live.

        Persists across reload: a reconnecting client rebuilds the panel at the
        bottom of the stack.
        """
        self._send_order("back")

    def forward(self):
        """Raise this panel one step up the stack, live.

        A single overlap-aware nudge; not persisted across a
        reload — use :meth:`to_front` for a durable change.
        """
        self._send_order("forward")

    def backward(self):
        """Lower this panel one step down the stack, live.

        A single overlap-aware nudge; not persisted across a
        reload — use :meth:`to_back` for a durable change.
        """
        self._send_order("backward")

    def _send_order(self, op):
        if self._bridge is None:
            return
        self._bridge.reorder_component(self.id, op)

    def set_layout(self, x=None, y=None, w=None, h=None, rotation=None,
                   opacity=None,
                   locked=None, draggable=None, resizable=None, operable=None,
                   grabbable=None, frame=None, frame_color=None, *,
                   roles=None, client_id=None):
        """Update position, size, rotation and/or lock state in one live message.

        Any argument left as ``None`` is unchanged. ``x``/``y`` are the canvas
        position, ``rotation`` (degrees) the angle, ``w``/``h`` the size.
        ``locked`` is a full lock (blocks interaction *and* programmatic updates);
        ``draggable``/``resizable``/``operable``/``grabbable`` are
        interaction-preserving locks carried in the shape's ``meta``
        (``operable=False`` makes controls inert to the user while value updates
        keep rendering); ``frame`` toggles the card chrome.

        Scope it to specific viewers with ``roles=`` and/or ``client_id=`` (just
        like :meth:`React.update`): the change is stored as a per-viewer layout
        *overlay* on the shared geometry (precedence shared < role < client) and
        pushed to just those viewers — it persists and replays on reconnect, so a
        role can have its own placement/size. Omit both to set the shared layout
        for everyone. A user dragging/resizing a panel writes back to whichever
        layer their layout currently comes from (their client/role overlay if any,
        else the shared base), so hand-arranged layouts stick. Returns ``self``.
        """
        fields = {}
        for key, val in (("x", x), ("y", y), ("w", w), ("h", h),
                         ("rotation", rotation), ("opacity", opacity),
                         ("locked", locked), ("draggable", draggable),
                         ("resizable", resizable), ("operable", operable),
                         ("grabbable", grabbable), ("frame", frame),
                         ("frameColor", frame_color)):
            if val is not None:
                fields[key] = val
        if not fields:
            return self
        payload = self._layout_payload(fields)
        if roles is None and client_id is None:
            self._store_base_layout(fields)
            if payload:
                self._send_update(payload)
            return self
        if roles is not None:
            for r in ([roles] if isinstance(roles, str) else roles):
                self._role_layout.setdefault(r, {}).update(fields)
                self._send_update_to(payload, role=r)
        if client_id is not None:
            self._client_layout.setdefault(client_id, {}).update(fields)
            self._send_update_to(payload, client_id=client_id)
        return self

    @property
    def color(self):
        """The accent color of this panel's canvas frame (hex string, or None)."""
        return getattr(self, "_frame_color", None)

    @color.setter
    def color(self, value):
        fc = _theme.accent_hex(value) if value is not None else None
        self._frame_color = fc
        # set_layout treats None as "leave unchanged" — the sentinel would
        # swallow a clear, leaving Python reading None while browsers keep
        # the tint forever. "" is the wire's actual clearing value (a falsy
        # frameColor drops the tint), so translate here.
        self.set_layout(frame_color=fc if fc is not None else "")

    def _init_color(self, color):
        """Store the accent color supplied at construction.

        Called from each component's ``__init__`` instead of repeating the
        ``_theme.accent_hex`` expression inline.  The bridge reads
        ``_frame_color`` at registration time to tint the panel's canvas frame;
        live changes go through the :attr:`color` setter.
        """
        self._frame_color = _theme.accent_hex(color) if color is not None else None

    def _layout_payload(self, fields):
        """Normalised layout ``fields`` -> the wire ``update`` payload (rotation
        to radians for the wire, flag names to their wire keys)."""
        payload = {}
        for key in ("x", "y", "w", "h"):
            if key in fields:
                payload[key] = fields[key]
        if "rotation" in fields:
            payload["rotation"] = math.radians(fields["rotation"])
        if "opacity" in fields:
            payload["opacity"] = float(fields["opacity"])
        for name in LAYOUT_FLAGS:
            if name in fields:
                payload[LAYOUT_FLAGS[name].wire] = bool(fields[name])
        if "frameColor" in fields:
            payload["frameColor"] = fields["frameColor"]
        return payload

    def _store_base_layout(self, fields):
        """Write normalised layout ``fields`` into the shared base state, so they
        replay for every viewer (the position fill-in matches the old inline
        behaviour: a lone x or y keeps the other coordinate)."""
        if "x" in fields or "y" in fields:
            prev_x, prev_y = self._position or (None, None)
            new_x = fields.get("x", prev_x)
            new_y = fields.get("y", prev_y)
            if new_x is not None and new_y is not None:
                self._position = (new_x, new_y)
        if "w" in fields:
            self._props["w"] = fields["w"]
        if "h" in fields:
            self._props["h"] = fields["h"]
        if "rotation" in fields:
            self._rotation = fields["rotation"]
        if "opacity" in fields:
            self._opacity = float(fields["opacity"])
        for name in LAYOUT_FLAGS:
            if name in fields:
                setattr(self, LAYOUT_FLAGS[name].attr, bool(fields[name]))

    def _has_viewer_overlays(self):
        """True when this panel has any per-role/per-client override, so its
        register frame must be built per viewer rather than shared once (see
        ``Bridge.register_live``). Subclasses with prop overlays extend this."""
        return bool(self._role_layout or self._client_layout)

    def _layout_overlay_for(self, role=None, client_id=None):
        """The per-viewer layout override (role then client merged), or ``{}``.
        Used by the bridge to replay a viewer's own placement/size on connect."""
        overlay = {}
        if role is not None:
            for r in ([role] if isinstance(role, str) else role):
                overlay.update(self._role_layout.get(r, {}))
        if client_id is not None:
            overlay.update(self._client_layout.get(client_id, {}))
        return overlay

    # -- layout read-back (browser -> Python) --------------------------------
    def on_layout(self, fn):
        """Decorator: callback fired when the user moves/resizes this panel.

        Called with the component after its stored geometry has been updated
        from the browser. Use it to react to (or persist) hand-arranged layouts.
        """
        self._layout_callbacks.append(fn)
        return fn

    def on_error(self, fn):
        """Decorator: register a handler called when a JS error occurs in this panel.

        The handler receives the error message string::

            @panel.on_error
            def handle(msg):
                log_error(panel.name, msg)

        When at least one handler is registered, the default stderr print is
        suppressed — the handler owns the error. With no handlers registered,
        errors are printed to stderr as before.
        """
        self._error_callbacks.append(fn)
        return fn

    def _dispatch_error(self, message):
        """Fire error callbacks, or fall back to stderr if none are registered.

        Called by the bridge on receipt of a ``panel_error`` message from the
        browser (JS runtime errors, unhandled rejections, React error boundaries).
        """
        if self._error_callbacks:
            for cb in self._error_callbacks:
                try:
                    cb(message)
                except Exception:
                    traceback.print_exc()
        else:
            import sys as _sys
            print(
                f"\033[31m[panel error] {self.name}: {message}\033[0m",
                file=_sys.stderr,
            )

    def _apply_remote_layout(self, msg, viewer=None):
        """Update stored geometry from a user drag/resize in the browser.

        Writes back to the layer this viewer's layout currently comes from — their
        per-client overlay, else their per-role overlay, else the shared base — so
        a hand-arranged layout sticks, and in a role-based canvas a drag rearranges
        only the dragger's role rather than everyone's. Does not broadcast (the
        change already happened in that browser). ``rotation`` arrives in radians
        (radians on the wire) and is stored as degrees, matching the rest of the Python API.
        ``on_layout`` handlers fire with the component, plus the mover's ``viewer``
        when they declare a second parameter.
        """
        fields = {}
        x = msg.get("x")
        y = msg.get("y")
        if x is not None and y is not None:
            fields["x"], fields["y"] = x, y
        if msg.get("w") is not None:
            fields["w"] = msg["w"]
        if msg.get("h") is not None:
            fields["h"] = msg["h"]
        if msg.get("rotation") is not None:
            fields["rotation"] = math.degrees(msg["rotation"])
        # A manual height/width drag in the browser pins a content-fit panel: the
        # frontend flips autoH/autoW off and reports it here so the content fit
        # stops re-asserting itself (and the flag round-trips to other viewers).
        if "autoH" in msg and hasattr(self, "_auto_h"):
            self._auto_h = bool(msg["autoH"])
        if "autoW" in msg and hasattr(self, "_auto_w"):
            self._auto_w = bool(msg["autoW"])
        viewer = viewer or {}
        role = viewer.get("role")
        cid = viewer.get("id")
        if cid is not None and cid in self._client_layout:
            self._client_layout[cid].update(fields)
        elif role is not None and role in self._role_layout:
            self._role_layout[role].update(fields)
        else:
            self._store_base_layout(fields)
        self._dispatch_callbacks(self._layout_callbacks, (self,), viewer,
                                 event="layout")

    # -- input (browser -> Python) -------------------------------------------
    def _register_callback(self, store, fn, threaded, dedicated, queue):
        """Append *fn* to *store* with the requested dispatch mode.

        Shared by every on_* decorator (on_click, on_select, on_message, …) so
        the threaded/dedicated/queue branching lives in exactly one place.
        Returns the original *fn* (unwrapped) so decorators can chain cleanly.
        """
        if threaded and dedicated:
            raise ValueError("threaded and dedicated are mutually exclusive")
        if dedicated:
            store.append(_mark_dedicated(fn, queue))
        elif threaded:
            store.append(_mark_threaded(fn))
        else:
            store.append(fn)
        return fn

    # What the `_callbacks` store means in `handlers` — Button says "click".
    _change_trigger = "change"

    def _handler_sources(self):
        """(trigger, callbacks) pairs feeding :attr:`handlers`; subclasses
        with extra stores extend this via ``yield from super()...``."""
        yield (self._change_trigger, self._callbacks)
        yield ("layout", self._layout_callbacks)

    @property
    def handlers(self):
        """What this panel is wired to trigger: ``{trigger: [HandlerInfo]}``.

        Every registered handler, keyed by what fires it (``"change"`` /
        ``"click"`` / ``"on:<event>"`` / ``"message"`` / ``"binary"`` /
        ``"request:<event>"`` / ``"select"`` / …), each entry carrying the
        original function (``.fn``), a ``file:line``-qualified name, and its
        dispatch mode. Read-only introspection — registration still goes
        through the ``on_*`` decorators. Triggers with no handlers are
        omitted, so an unwired panel reads ``{}``. Also how the Inspector's
        detail view shows a panel's wiring.
        """
        out = {}
        for trigger, fns in self._handler_sources():
            if fns:
                out.setdefault(trigger, []).extend(_describe_handlers(fns))
        return out

    def on_change(self, fn=None, *, threaded=False, dedicated=False, queue="fifo"):
        """Decorator: register a callback fired on input from the browser.

        **Default** — runs on a shared dispatch thread. Fast handlers (state
        updates, canvas calls) belong here; a slow one delays the handlers
        queued behind it.

        **``threaded=True``** — spawns a new daemon thread *per call*. Keeps
        the shared dispatch thread free for other handlers. Right for
        occasional slow work (an HTTP call, a ``time.sleep``). The handler
        may run concurrently with itself if calls arrive faster than it
        finishes, so guard any shared state you write.

        **``dedicated=True``** — launches one persistent daemon thread for
        *this handler only*, started on its first invocation. All calls are
        routed to that thread's own queue, so the handler is always serialised
        (no concurrent self-calls) and the shared dispatch thread is never
        blocked. Right for handlers that fire rapidly and do non-trivial work::

            @speed.on_change(dedicated=True, queue="latest")
            def _(v):
                result = heavy_compute(v)   # own thread; only the latest drag fires
                status.update(result)

        ``queue`` controls backpressure on the dedicated thread's queue:

        - ``"fifo"`` (default) — every call is queued and run in order.
        - ``"latest"`` — only the most recent *pending* call is kept; the
          thread runs to completion first, then picks up only the latest one,
          dropping any that piled up in between.

        ``threaded`` and ``dedicated`` are mutually exclusive.

        **``async def`` handlers** are supported on every ``on_*`` registration:
        the coroutine runs on a shared asyncio loop (its own daemon thread, not
        the server's), so an awaiting handler never blocks the dispatch thread
        and many async handlers can be in flight at once::

            @fetch.on_click
            async def _():
                async with httpx.AsyncClient() as client:
                    table.update((await client.get(url)).json())

        Combined with ``dedicated=True`` the coroutine is *awaited to
        completion* on that handler's own thread, so it stays serialised;
        with ``threaded=True`` each call awaits on its own thread.
        """
        def register(f):
            return self._register_callback(self._callbacks, f, threaded, dedicated, queue)
        return register(fn) if fn is not None else register

    @staticmethod
    def _accepts_viewer(fn, n_call_args):
        """Whether ``fn`` wants a trailing ``viewer`` beyond the ``n_call_args``
        danvas already supplies.

        A handler opts into the viewer dict by declaring a parameter for it,
        detected two ways — and we *deliberately* ignore an unrelated default
        argument so it is never mistaken for the viewer slot:

        - a **required** positional parameter beyond ``n_call_args`` (the common
          ``def _(value, viewer): ...`` — no default, so it must be filled), or
        - a positional parameter literally named ``viewer`` beyond ``n_call_args``
          (so ``def _(value, viewer=None): ...`` still receives it).

        A *defaulted* parameter that isn't named ``viewer`` — e.g. the standard
        loop-capture idiom ``def handle(msg, stage_id=sid): ...`` — is the
        caller's own argument and is left alone, so danvas won't clobber it with
        the viewer. Unintrospectable callables (some builtins/C funcs) report
        False. Shared by the fire-and-forget callback path and the single-answer
        request path so both detect arity the same way.
        """
        try:
            params = [
                p for p in inspect.signature(fn).parameters.values()
                if p.kind in (inspect.Parameter.POSITIONAL_ONLY,
                              inspect.Parameter.POSITIONAL_OR_KEYWORD)
            ]
        except (ValueError, TypeError):
            return False
        # A required (no-default) param beyond what we supply: the handler is
        # asking for the viewer slot.
        required = [p for p in params if p.default is inspect.Parameter.empty]
        if len(required) > n_call_args:
            return True
        # Otherwise honour an explicit ``viewer`` param (even defaulted), but
        # never an unrelated default argument (the loop-capture footgun).
        return any(p.name == "viewer" for p in params[n_call_args:])

    @staticmethod
    def _traced(bridge, cb, meta, traceable=None):
        """Wrap ``cb`` to emit dispatch-trace events around its run.

        Shallow (``traceable is None``): emit the handler's own ``start`` then
        ``done``/``error`` (all at ``depth`` 0). Deep (``traceable`` given): a
        ``sys.setprofile`` probe emits ``start``/``done`` for the handler *and*
        the user-code calls it makes, each depth-tagged, and this wrapper adds
        only the handler-level ``error`` on exception.

        The wrapper runs wherever the dispatch mode places it — inline, a spawned
        thread, or a dedicated kernel — so the stamps reflect the *actual*
        execution and concurrent (``threaded=True``) handlers report overlapping
        runs. It swallows the handler's exception (after emitting ``error`` and
        printing the traceback) so the inline/spawn/kernel error handling doesn't
        also log it.

        An ``async def`` handler returns a coroutine from ``cb(*args)`` without
        doing its work yet; ``run`` then returns a *wrapper* coroutine that emits
        ``done``/``error`` when the awaited handler actually finishes, so the
        trace reports the real duration rather than the microseconds it took to
        create the coroutine. (Deep tracing can't follow onto the async loop —
        its ``sys.setprofile`` probe is per-thread — so the awaited part is
        traced shallowly.)"""
        def run(*args):
            t0 = time.perf_counter()
            if traceable is None:
                bridge._emit_dispatch({**meta, "phase": "start", "depth": 0,
                                       "t": t0})
            try:
                if traceable is None:
                    result = cb(*args)
                else:
                    result = _trace.run_calls(bridge, cb, args, meta, traceable)
            except Exception as exc:
                bridge._emit_dispatch({
                    **meta, "phase": "error", "depth": 0,
                    "t": time.perf_counter(),
                    "dur_ms": (time.perf_counter() - t0) * 1000.0,
                    "error": repr(exc)})
                traceback.print_exc()
                return None
            if inspect.iscoroutine(result):
                async def traced_coro():
                    try:
                        await result
                    except Exception as exc:
                        bridge._emit_dispatch({
                            **meta, "phase": "error", "depth": 0,
                            "t": time.perf_counter(),
                            "dur_ms": (time.perf_counter() - t0) * 1000.0,
                            "error": repr(exc)})
                        traceback.print_exc()
                        return
                    bridge._emit_dispatch({
                        **meta, "phase": "done", "depth": 0,
                        "t": time.perf_counter(),
                        "dur_ms": (time.perf_counter() - t0) * 1000.0})
                return traced_coro()
            if traceable is None:
                bridge._emit_dispatch({
                    **meta, "phase": "done", "depth": 0,
                    "t": time.perf_counter(),
                    "dur_ms": (time.perf_counter() - t0) * 1000.0})
            return None
        return run

    def _dispatch_callbacks(self, callbacks, call_args, viewer, event=None):
        """Call each callback, appending viewer as a final arg when its signature accepts it.

        Detects whether the callback has more explicit positional parameters
        than ``call_args`` supplies — if so, passes ``viewer`` (a dict with
        ``id``, ``name``, ``color``, ``role``) as the extra argument. Backwards
        compatible: existing one-arg handlers are never changed.

        Three dispatch paths, chosen by marker attributes set at registration:

        - ``_danvas_dedicated`` — routes to a per-handler :class:`DedicatedKernel`
          (created lazily here on first dispatch, keyed by ``id(cb)``). The
          kernel's own queue serialises calls to this handler without blocking
          the shared dispatch thread.
        - ``_danvas_threaded`` — spawns a fresh daemon thread per call via
          :func:`spawn`. Keeps the dispatch thread free; may run concurrently.
        - *(default)* — called inline on the shared dispatch thread; FIFO and
          blocking (a slow handler stalls the queue behind it).
        """
        # Dispatch tracing (canvas.on_dispatch): only when a tap is registered —
        # an untapped canvas takes the original, uninstrumented path below and
        # pays nothing. When tapping, every handler this trigger fans out to
        # shares one ``trace`` id, and each is wrapped to emit start/done/error.
        bridge = getattr(self, "_bridge", None)
        # Instrument when a live tap is watching *or* history is being recorded
        # (armed at serve). Fully skipped otherwise, so an unserved/untapped
        # canvas pays nothing.
        active = (getattr(bridge, "_dispatch_taps", None)
                  or getattr(bridge, "_trace_recording", False))
        # Deep tracing (canvas.trace_calls) also records the user-code calls each
        # handler makes; opt-in because the sys.setprofile probe has real cost.
        deep = bool(getattr(bridge, "_trace_deep", False))
        # The action's correlation id is minted lazily on the first *user* handler,
        # so a dispatch of only danvas-internal callbacks consumes nothing.
        trace_id = None
        seq = 0

        for cb in callbacks:
            args = (*call_args, viewer or {}) \
                if self._accepts_viewer(cb, len(call_args)) else call_args
            dedicated = getattr(cb, "_danvas_dedicated", False)
            threaded = getattr(cb, "_danvas_threaded", False)
            run = cb
            if active and _is_user_handler(cb):
                if trace_id is None:
                    trace_id = bridge._next_trace_id()
                meta = {
                    "trace": trace_id, "seq": seq,
                    # A per-handler frame id so a consumer pairs start↔done even
                    # when threaded handlers of one action run concurrently; the
                    # deep tracer mints its own fids for the nested calls.
                    "fid": f"{trace_id}:{seq}",
                    "comp": getattr(self, "name", None) or getattr(self, "id", None),
                    "event": event or "input",
                    "handler": _handler_label(cb),
                    "mode": ("dedicated" if dedicated
                             else "threaded" if threaded else "inline"),
                }
                seq += 1
                bridge._emit_dispatch({**meta, "phase": "queued", "depth": 0,
                                       "t": time.perf_counter()})
                run = self._traced(bridge, cb, meta,
                                   _trace.is_user_code if deep else None)
            # Every mode goes through _run_handler so an ``async def`` handler
            # works under all three. threaded/dedicated *wait* on the coroutine —
            # their thread is the concurrency/serialisation unit, so a dedicated
            # async handler stays serialised and a threaded one keeps its
            # run-to-completion-per-thread semantics. The inline default
            # schedules it and returns, freeing the shared dispatch thread while
            # the handler awaits (its failures are logged by AsyncKernel.spawn).
            if dedicated:
                k = self._dedicated_kernels.get(id(cb))
                if k is None:
                    mode = getattr(cb, "_danvas_handler_queue", "fifo")
                    k = DedicatedKernel(mode=mode)
                    self._dedicated_kernels[id(cb)] = k
                k.submit(lambda c=run, a=args: _run_handler(c, a, wait=True))
            elif threaded:
                # Run on its own daemon thread so a slow handler doesn't hold up
                # the rest; spawn() logs any exception (default-arg capture so
                # the loop variable isn't shared across iterations).
                spawn(lambda c=run, a=args: _run_handler(c, a, wait=True),
                      name=f"danvas-handler-{self.name}")
            else:
                try:
                    _run_handler(run, args)
                except Exception:
                    traceback.print_exc()

    def _handle_input(self, payload, viewer=None):
        if "value" in payload:
            with self._lock:
                self._value = payload["value"]
        self._dispatch_callbacks(self._callbacks, (self.value,), viewer)


def _flag_property(name, flag):
    """Build the read/write property for one lock/chrome flag.

    The getter returns the backing attribute; the setter routes through
    :meth:`BaseComponent.set_layout` so the change is broadcast live and the
    stored state stays consistent.
    """
    def getter(self):
        return getattr(self, flag.attr)

    def setter(self, value):
        self.set_layout(**{name: bool(value)})

    return property(getter, setter, doc=flag.doc)


# Attach the flag properties declared in _flags.py onto BaseComponent. Defining
# them here (rather than by hand) keeps the wire key, default, and docstring in
# one table — adding a flag is a single entry there.
for _name, _flag in LAYOUT_FLAGS.items():
    setattr(BaseComponent, _name, _flag_property(_name, _flag))


class _ValuePersist:
    """Mixin: persist/restore the user-set ``value`` of an input control.

    Mixed into the controls whose value is *user* state worth surviving a
    restart (Slider/Toggle/TextField), as opposed to content panels whose state
    is reproduced by re-running the code that filled them. ``save``/``load`` and
    ``serve(persist=)`` carry it via ``Canvas._layout``/``_restore_layout``.

    Restore goes through the component's own ``update()`` — a Python→browser
    push that sets the value and replays it on reconnect but does **not** fire
    ``on_change`` (that path is only triggered by browser input), so a restored
    value lands silently, with no spurious startup callbacks.
    """

    @property
    def value(self):
        with self._lock:
            return self._value

    @value.setter
    def value(self, v):
        # Write = the component's own update(): pushed to browsers, replayed
        # on reconnect, and silent (a programmatic set never fires on_change —
        # that path belongs to browser input). `slider.value = 3` is the
        # symmetric twin of the .min/.max/.step live setters.
        self.update(v)

    def _persist_state(self):
        v = self.value
        return {"value": v} if v is not None else {}

    def _restore_state(self, saved):
        if not saved:
            return
        v = saved.get("value")
        if v is None:
            return
        try:
            self.update(v)
        except Exception:
            # A value that no longer fits (e.g. a Toggle option the code
            # dropped) must never break a load/restore — skip it, loudly.
            traceback.print_exc()