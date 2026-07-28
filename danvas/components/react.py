"""React: a user-authored React component rendered as a native canvas panel.

The native counterpart to :class:`Custom`. Where ``Custom`` renders arbitrary
HTML in a *sandboxed iframe* (isolated, no theme or bridge access), ``React``
takes JSX *source* and mounts it as an ordinary React subtree **inside the
panel** — so it inherits the canvas theme, dark mode, and selection chrome, and
talks to Python directly with no postMessage hop. The JSX is compiled in the
browser at runtime (Sucrase, lazily loaded), so users author components from
Python with no ``npm`` build.

The component must be named ``Component`` and receives three props:

  * ``canvas`` — the bridge handle (see below);
  * ``value``  — the latest :meth:`push` data: Python → panel, no reload. This is
    the *simple* receive channel — reading ``value`` re-renders the component, so
    it's right for occasional state (a status, a row of numbers). For a high-rate
    stream (video, telemetry), use ``canvas.onFrame`` instead, which delivers each
    push WITHOUT a re-render. The two are one channel with two ends: registering an
    ``onFrame`` subscriber routes pushes there and ``value`` then stops updating —
    that is the deliberate opt-out of per-push re-rendering, not a conflict. Pick
    one end by how fast the data arrives;
  * ``props``  — the dict from :meth:`update` / the ``props=`` arg: Python → panel,
    replayed on reconnect.

The ``canvas`` handle exposes:

  * ``send(data)`` — panel → Python, routed to your ``@on`` / ``on_message`` handlers;
  * ``request(data)`` — the **awaitable** twin of ``send``: returns a Promise that
    resolves with the return value of the matching :meth:`on_request` handler
    (``const r = await canvas.request({event:'…', …})``);
  * ``sendBinary(buf)`` — send an ``ArrayBuffer`` *up* to Python (zero-copy) →
    ``@panel.on_binary``. Binary is bidirectional on a React panel — this is the up
    direction, :meth:`push_binary` / ``onFrame`` the down;
  * ``onFrame(cb)`` — the streaming end of the receive channel: subscribe (in a
    ``useEffect``) to the :meth:`push` / :meth:`push_binary` stream WITHOUT
    re-rendering; ``cb`` gets each value (an ``ArrayBuffer`` for binary). While an
    ``onFrame`` subscriber is registered the ``value`` prop stays put (pushes go
    here instead) — so reach for ``onFrame`` for fast streams and ``value`` for
    simple state, not both at once;
  * ``paintFrame(canvasEl, {onActive})`` — the image-frame fast path: for a panel
    streaming encoded image bytes (JPEG/PNG/WebP via :meth:`push_binary`), paints
    each frame to a ``<canvas>`` decoded **off the main thread**
    (``createImageBitmap`` + GPU ``bitmaprenderer``), coalescing bursts to the
    newest frame. Returns an unsubscribe; style the ``<canvas>`` with CSS
    (``object-fit``) for layout. Built-in VideoFeed uses this;
  * ``viewport(cb)`` — ``cb`` is called now and on every camera move with the live
    ``{ x, y, zoom }`` of the canvas centre (the numbers ``serve(view=…)`` takes);
  * ``setView({ x, y, zoom })`` — the write-twin of ``viewport``: pan/zoom the canvas
    to centre a point (any subset of the keys; omitted axes stay put);
  * ``chat`` — the canvas-wide shared room: ``send(text)``, ``setName(name)``,
    ``history()``, ``subscribe(cb)`` (returns an unsubscribe), ``identity(cb)``.

``React`` (with hooks) is in scope as ``React``; libraries named in ``scope=[…]`` are
in scope as ``libs`` (e.g. ``const d3 = libs.d3``).

    counter = canvas.react('''
      function Component({ canvas, value, props }) {
        const [n, setN] = React.useState(0)
        return <button onClick={() => { setN(n + 1); canvas.send({ clicks: n + 1 }) }}>
          {props.label}: {n}
        </button>
      }
    ''', props={"label": "Taps"})

    @counter.on_message
    def _(msg): print(msg)        # {'clicks': 3}
"""

import json
import sys
import traceback
import re

from ..bridge import BINARY_REACT
from . import _theme
from .base import BaseComponent
from ._routing import _EventRouter


class React(_EventRouter, BaseComponent):
    component = "React"
    BINARY_TYPE = BINARY_REACT

    default_w = 380
    default_h = 320

    def __init__(self, source=None, path=None, jsx=None, css=None, css_path=None,
                 name="react", label=None, w=None, h=None, color=None, props=None,
                 scope=None, event_key="event", queue="fifo",
                 wasm=None, wasm_path=None, forward_wheel=True):
        size = {k: v for k, v in (("w", w), ("h", h)) if v is not None}
        super().__init__(name=name, label=label, queue=queue, **size)
        self._path = path   # remembered so watch() can reload it
        if path is not None:
            with open(path, "r", encoding="utf-8") as f:
                source = f.read()
        # Two ways in: ``source`` is a complete component (must define
        # ``function Component``); ``jsx`` is just the markup expression, which
        # — with optional ``css`` — is composed into a Component under the hood.
        if source is not None and jsx is not None:
            raise ValueError("pass either source= (a full Component) or jsx= "
                             "(markup to be wrapped), not both")
        # ``css_path`` is the css= twin of ``path=``: load the stylesheet from a
        # file so a panel can keep both halves in sibling files (see canvas.react).
        if css is not None and css_path is not None:
            raise ValueError("pass either css= or css_path=, not both")
        if css_path is not None:
            with open(css_path, "r", encoding="utf-8") as f:
                css = f.read()
        # CSS handling: with jsx= the styles are composed into the wrapper; with
        # source= they ride as a `css` prop that ReactHost renders into a <style>
        # ahead of the component — so a full component can keep its styles in a
        # separate Python string instead of an inline <style>/`.replace()` hack.
        # Scope rules are the author's own selectors, exactly as an inline tag.
        self._css = ""
        if jsx is not None:
            source = self.compose(jsx, css or "")
        elif css:
            self._css = css
        if source is not None:
            source = self._normalise(source)
        self._source = source or ""
        # Optional third-party libraries to make available to the component as
        # the ``libs`` global. Each name is loaded as ESM from a CDN in the
        # browser on demand (so listing none costs nothing); friendly names
        # (``d3``, ``lodash``, ``framer-motion`` / ``motion``, ``lucide`` /
        # ``lucide-react``, ``date-fns``) map to pinned, React-externalised
        # builds, and any other name is passed through to esm.sh. The component
        # reads them as ``libs`` (e.g. ``const d3 = libs.d3``).
        self._libs = [str(s) for s in (scope or [])]
        # Optional WebAssembly binary. wasm_path= reads bytes from disk;
        # wasm= accepts raw bytes directly. Encoded as base64 and sent in the
        # shape props so the browser can instantiate it without a separate fetch.
        # For large modules (>1 MB) prefer hosting the .wasm file and fetching
        # from a URL inside the JSX instead — base64 adds ~33% overhead and the
        # full string rides in the record store on every reconnect.
        import base64 as _base64
        if wasm is not None and wasm_path is not None:
            raise ValueError("pass either wasm= (bytes) or wasm_path=, not both")
        if wasm_path is not None:
            with open(wasm_path, "rb") as f:
                wasm = f.read()
        if wasm is not None:
            _mb = len(wasm) / (1024 * 1024)
            if _mb > 1:
                print(
                    f"[danvas] warning: wasm module is {_mb:.1f} MB — large binaries "
                    "add latency on load and bloat the canvas store. Consider hosting "
                    "the .wasm file and fetching it from a URL inside the JSX instead.",
                    file=sys.stderr,
                )
            self._wasm_b64 = _base64.b64encode(wasm).decode("ascii")
        else:
            self._wasm_b64 = None
        # Props handed to the component (and merged by ``update``). Carried to the
        # browser as a JSON string prop so they persist in the shape and replay to
        # a reconnecting client.
        self._data = dict(props or {})
        if color is not None:
            self._data.setdefault("_th", _theme.derive(color))
            self._init_color(color)
        # Per-viewer prop overlays for scoped updates (``update(roles=...)`` /
        # ``update(client_id=...)``): role/id -> the props that override the
        # shared ``_data`` for those viewers. Merged shared < role < client both
        # on send and on reconnect replay, so a scoped update *persists* (unlike
        # the one-shot ``push``).
        self._role_data = {}
        self._client_data = {}
        # When False, wheel over this panel is left to the panel's own content
        # (a scroll region, a map, a 3D viewer zooming its camera) instead of
        # zooming the canvas. Unlike Custom (which forwards from inside its
        # iframe), a React panel renders in the canvas DOM, so this rides to the
        # frontend as `wheelLocal` meta and the engine's wheel handler bails when
        # the cursor is over it. See [[iframe-custom-panel-pattern]].
        self._forward_wheel = forward_wheel
        # Inbound ``canvas.send`` routing (on / on_message / dispatch) AND the
        # request/response table (on_request / canvas.request) are shared with
        # Custom via _EventRouter; this seeds them (+ on_change catch-alls).
        self._init_routing(event_key)
        # Auto-height is the default: a React panel fits its rendered content
        # unless the caller pins a numeric height (``h is None`` → auto-fit; a
        # number → fixed). Width stays fixed by default (opt in with w="auto").
        # Unlike Custom (which measures inside its iframe), a native React panel is
        # measured by ReactHost, which reports the content size back to resize the
        # shape; the flags ride along in register_props as ``autoH``/``autoW``.
        self._auto_h = h is None
        self._auto_w = False

    def _compose_props(self, data):
        props = dict(self._props)  # label, w, h
        props["source"] = self._source
        props["data"] = json.dumps(data)
        props["css"] = self._css
        props["autoH"] = self._auto_h
        props["autoW"] = self._auto_w
        props["libs"] = json.dumps(self._libs)
        props["wasm"] = self._wasm_b64 or ""
        return props

    def _data_for(self, role=None, client_id=None):
        """The shared props with the overlays for one viewer merged on top
        (shared < role < client)."""
        data = dict(self._data)
        if role is not None:
            for r in ([role] if isinstance(role, str) else role):
                data.update(self._role_data.get(r, {}))
        if client_id is not None:
            data.update(self._client_data.get(client_id, {}))
        return data

    def register_props(self):
        return self._compose_props(self._data)

    def register_props_for(self, role=None, client_id=None):
        return self._compose_props(self._data_for(role, client_id))

    def _has_viewer_overlays(self):
        return bool(self._role_data or self._client_data) or super()._has_viewer_overlays()

    def _set_auto_h(self):
        """Enable content-fit height live (``comp.h = "auto"``).

        Flips the panel into auto-height and tells the running ReactHost to start
        measuring and reporting its content height. A no-op if already auto. (To
        pin a fixed height again, assign a number: ``comp.h = 240``.)
        """
        if self._auto_h:
            return
        self._auto_h = True
        self._send_update({"autoH": True})

    def _set_fixed_h(self, value):
        """Pin the height to a number, leaving auto-height mode if it was on (so
        the value isn't immediately overridden by the content fit)."""
        if self._auto_h:
            self._auto_h = False
            self._send_update({"autoH": False})
        self.set_layout(h=value)

    def _set_auto_w(self):
        """Enable content-fit width live (``comp.w = "auto"``).

        The width twin of :meth:`_set_auto_h`: flips the panel into auto-width
        and tells the running ReactHost to measure the content's natural width
        and report it back. A no-op if already auto. (To pin a fixed width again,
        assign a number: ``comp.w = 320``.)
        """
        if self._auto_w:
            return
        self._auto_w = True
        self._send_update({"autoW": True})

    def _set_fixed_w(self, value):
        """Pin the width to a number, leaving auto-width mode if it was on (so
        the value isn't immediately overridden by the content fit)."""
        if self._auto_w:
            self._auto_w = False
            self._send_update({"autoW": False})
        self.set_layout(w=value)

    @staticmethod
    def compose(jsx="", css=""):
        """Wrap a JSX expression (plus optional CSS) into a full ``Component``.

        Used internally for the ``jsx=`` / ``css=`` constructor path, but also
        callable directly when you want the assembled source. The CSS lands in
        a ``<style>`` tag scoped only by your own selectors, and the markup is
        wrapped in a ``.react-root`` div that fills the panel.
        """
        return f"""
        function Component({{ canvas, props, value }}) {{
            return (
                <>
                    <style>{{`
                        .react-root {{ width: 100%; height: 100%; }}
                        {css}
                    `}}</style>
                    <div className="react-root">
                        {jsx}
                    </div>
                </>
            );
        }}
        """

    @staticmethod
    def _normalise(source):
        """Normalise any pasted React snippet to the ``function Component`` form.

        Passes through unchanged if the source has no ``import``/``export``
        lines — i.e. it is already in the panel-ready format.  Otherwise:

        * named React imports (useState, useEffect, …) are re-emitted as
          ``const { … } = React;`` (the runtime exposes ``React`` globally but
          not its named exports);
        * ``styled-components`` definitions are converted to a scoped
          ``<style>`` tag (styled-components requires a bundler; the in-browser
          Sucrase pipeline does not have one);
        * ``@keyframes`` / ``@font-face`` blocks are hoisted outside the CSS
          scope wrapper (they are invalid when nested inside a selector);
        * all ``import`` / ``export default`` lines are stripped;
        * the component is renamed and wrapped as
          ``function Component({ canvas, props, value })``.

        This is called automatically by ``__init__`` so any React snippet can
        be passed directly to ``canvas.react(source=...)``.
        """
        if not re.search(r"^\s*(?:import|export)\b", source, re.MULTILINE):
            return source  # already panel-ready, pass through

        # styled-components: collect names and CSS before any stripping.
        styled_re = re.compile(r"const\s+(\w+)\s*=\s*styled\.\w+`([\s\S]*?)`;?")
        styled = {m.group(1): m.group(2) for m in styled_re.finditer(source)}
        css = "\n".join(styled.values())

        # Exported component name; fall back to first non-styled const.
        export_m = re.search(r"export\s+default\s+(\w+)", source)
        if export_m:
            original_name = export_m.group(1)
        else:
            names = [m.group(1) for m in re.finditer(r"const\s+(\w+)\s*=", source)
                     if m.group(1) not in styled]
            original_name = names[0] if names else "SnippetComponent"

        # Rescue named React imports before stripping.
        react_named = []
        for line in source.splitlines():
            m = re.match(r"\s*import\s+React\s*,\s*\{([^}]+)\}\s+from\s+['\"]react['\"]", line)
            if not m:
                m = re.match(r"\s*import\s+\{([^}]+)\}\s+from\s+['\"]react['\"]", line)
            if m:
                react_named = [h.strip() for h in m.group(1).split(",")]
                break

        clean = styled_re.sub("", source)
        clean = re.sub(r"^\s*import\b.*$", "", clean, flags=re.MULTILINE)
        if react_named:
            clean = f"const {{ {', '.join(react_named)} }} = React;\n" + clean
        clean = re.sub(r"^\s*export\s+default\b.*$", "", clean, flags=re.MULTILINE)

        for name in styled:
            clean = re.sub(rf"<{name}(\s|>)", r"<div\1", clean)
            clean = clean.replace(f"</{name}>", "</div>")

        # @keyframes / @font-face cannot be nested inside a selector — hoist.
        atrule_re = re.compile(
            r'@(?:keyframes|font-face)[^{]*\{(?:[^{}]|\{[^{}]*\})*\}', re.DOTALL)
        top_rules = "\n".join(atrule_re.findall(css))
        scoped_css = atrule_re.sub("", css)

        # Guard: styled imports detected but regex extracted nothing — usually
        # means the template-literal delimiter was escaped (e.g. \` in Python).
        if re.search(r"import\s+\S+\s+from\s+['\"]styled-components['\"]", source) and not styled:
            print(
                "[danvas] warning: styled-components import found but no "
                "styled.tag`...` blocks extracted — check for escaped backticks "
                r"(\` → `) in the source string.",
                file=sys.stderr,
            )

        if css.strip():
            inner = f"""<style>{{`
                        {top_rules}
                        .pc-uiverse {{ flex:1;min-height:0;display:flex;flex-direction:column;width:100%; {scoped_css} }}
                        .pc-uiverse > * {{ flex:1;min-height:0; }}
                    `}}</style>
                    <div className="pc-uiverse">
                        <{original_name} {{...props}} />
                    </div>"""
        else:
            inner = f"<{original_name} {{...props}} />"

        result = f"""
        {clean}

        function Component({{ canvas, props, value }}) {{
            return (
                <>
                    {inner}
                </>
            );
        }}
        """

        if "function Component" not in result:
            print(
                "[danvas] warning: no 'function Component' found after normalising "
                "source — the panel will show an error. Check the source defines a "
                "component and has a matching 'export default'.",
                file=sys.stderr,
            )
        return result

    # -- write (Python -> panel) ---------------------------------------------
    def update(self, *, roles=None, client_id=None, **props):
        """Patch the component's ``props`` and re-render, live.

        Merges ``props`` into the current set (so ``update(label="Hi")`` leaves
        the rest untouched) and pushes the merged dict to the panel. Returns
        ``self``.

        **Scope it to specific viewers** with ``roles=`` (a role name or list, as
        in ``serve(passwords=)``) and/or ``client_id=`` (an id from
        ``canvas.viewers``): the props are stored as a per-viewer *overlay* on the
        shared state (precedence shared < role < client) and pushed to just those
        viewers — and, unlike a one-shot :meth:`push`, they **persist and replay**
        when such a viewer reconnects. So each viewer can be shown their own slice
        — a per-team budget, a personalised greeting — with no client-side
        filtering of a global blob::

            panel.update(rows=catalogue)                 # everyone (shared)
            panel.update(roles="Red", budget=1400)       # just the Red team
            panel.update(client_id=v["id"], you=v["name"])

        Omit both ``roles`` and ``client_id`` to update the shared state for
        everyone. ``roles``/``client_id`` are reserved, so a prop can't use those
        names (rename the prop if needed).
        """
        # Shared (broadcast) updates — the high-volume path — keep the full state in
        # ``_data`` (so a reconnecting viewer replays every prop via
        # ``register_props``) but put only the CHANGED keys on the wire as
        # ``data_patch``; the frontend merges them into the panel's current data. A
        # 1000-row table updated one cell at a time no longer re-serializes and
        # re-broadcasts every row each tick. (Conflated ``latest`` panels accumulate
        # patches in _merge_update so none are dropped.)
        if roles is None and client_id is None:
            self._data.update(props)
            self._send_update({"data_patch": dict(props)})
            return self
        # Scoped (per-viewer) updates are infrequent personalisation; they send the
        # full merged view for that viewer, unchanged.
        if roles is not None:
            for r in ([roles] if isinstance(roles, str) else roles):
                self._role_data.setdefault(r, {}).update(props)
                self._send_update_to(
                    {"data": json.dumps(self._data_for(role=r))}, role=r)
        if client_id is not None:
            self._client_data.setdefault(client_id, {}).update(props)
            self._send_update_to(
                {"data": json.dumps(self._data_for(client_id=client_id))},
                client_id=client_id)
        return self

    def update_for(self, *, role=None, client_id=None, **props):
        """Deprecated alias for :meth:`update` with ``roles=`` / ``client_id=``.

        Kept for back-compat — prefer ``update(roles=..., client_id=..., ...)``.
        Note the semantics now **persist** (the scoped props replay on reconnect),
        where the original ``update_for`` was a one-shot push that reverted to the
        shared props on reconnect. With neither ``role`` nor ``client_id`` it's a
        no-op. Returns ``self``.
        """
        if role is None and client_id is None:
            return self
        return self.update(roles=role, client_id=client_id, **props)

    @property
    def color(self):
        """The accent color of this panel (hex string, or None if unset)."""
        return getattr(self, "_frame_color", None)

    @color.setter
    def color(self, value):
        th = _theme.derive(value) if value is not None else {}
        fc = _theme.accent_hex(value) if value is not None else None
        self._frame_color = fc
        # post_style: fast React-state path (same as push/post) so the theme dict
        # updates immediately without relying on store reconciliation.
        self._send_update({"post_style": th})
        # Also update _data so reconnecting clients get the right _th from the store.
        React.update(self, _th=th)
        # None must CLEAR, but set_layout's None means "leave unchanged" —
        # send "" (the wire's clearing value) instead. Same rule as the base
        # setter; without it the tint outlived color=None while Python read
        # back None.
        self.set_layout(frame_color=fc if fc is not None else "")

    def push(self, data):
        """Stream ``data`` to the component without a re-mount.

        Like :meth:`Custom.push`, this bypasses shape props (no churn / reconnect
        replay) and suits high-rate updates. The component sees it as ``value``
        by default; for high-rate or binary streams it can instead subscribe via
        ``canvas.onFrame(cb)`` (in a ``useEffect``) and paint each frame itself —
        that path skips the React re-render the ``value`` prop would trigger.
        """
        self._send_update({"post": data})

    def set_source(self, source):
        """Replace the component's JSX source and recompile it, live."""
        source = self._normalise(source)
        self._source = source
        self._send_update({"source": source})

    def set_css(self, css):
        """Replace the panel's ``css=`` stylesheet, live (source= panels only)."""
        self._css = css or ""
        self._send_update({"css": self._css})

    def watch(self, path=None, css_path=None, interval=0.5):
        """Dev convenience: live-reload the panel's source (and optionally CSS)
        from disk whenever the file changes — edit the ``.jsx``, save, and the
        panel recompiles with no server restart.

        ``path`` defaults to the file the panel was built from (``path=`` on the
        constructor); pass ``css_path`` to also hot-reload a ``css=`` stylesheet.
        A daemon thread polls the files' modification time every ``interval``
        seconds and calls :meth:`set_source` / :meth:`set_css` on change. Returns
        a ``stop()`` callable. Meant for development — poll-based and best paired
        with ``serve(block=True)`` so the process stays alive::

            panel = canvas.react(path="panel.jsx")
            panel.watch()
            canvas.serve()

        Raises if no source file is known (build the panel with ``path=`` or pass
        one here).
        """
        import os
        import threading

        src_path = path or self._path
        if src_path is None and css_path is None:
            raise ValueError(
                "watch() needs a file: build the panel with path= or pass "
                "path=/css_path= here")

        targets = [(p, apply) for p, apply in
                   ((src_path, self.set_source), (css_path, self.set_css)) if p]
        # Snapshot the on-disk state now (synchronously), so a change made right
        # after watch() returns is detected — and the current contents, already
        # loaded, aren't re-pushed on the first poll.
        seen = {}
        for p, _ in targets:
            try:
                seen[p] = os.path.getmtime(p)
            except OSError:
                seen[p] = None
        stop = threading.Event()

        def loop():
            while not stop.is_set():
                for p, apply in targets:
                    try:
                        mtime = os.path.getmtime(p)
                    except OSError:
                        continue
                    if seen.get(p) == mtime:
                        continue
                    seen[p] = mtime
                    try:
                        with open(p, "r", encoding="utf-8") as f:
                            apply(f.read())
                    except OSError:
                        pass
                stop.wait(interval)

        threading.Thread(target=loop, daemon=True).start()
        return stop.set

    def validate(self):
        """Check the source for the common mistakes that otherwise only surface
        as a cryptic browser error, *before* you serve the canvas.

        Returns a list of human-readable problems (empty when it looks OK):

        * an empty source, or one that never declares ``Component``
          (``function Component(...)`` / ``const Component = …``);
        * unbalanced ``()`` / ``[]`` / ``{}`` — the usual fallout of a bad edit.

        Contents of strings and ``//`` / ``/* */`` comments are skipped, so braces
        inside text or a CSS template literal don't trip it. It's a fast
        structural check, **not** a full compile: source that passes here can
        still fail in the browser (a bad hook call, an undefined variable), and
        valid-but-unusual syntax could be flagged — so treat it as a lint, e.g.
        ``assert not panel.validate()`` in a test. Returns ``[]`` for a panel
        built from ``jsx=`` whose wrapper this class generated.
        """
        src = self._source or ""
        if not src.strip():
            return ["empty source"]
        problems = []
        if not re.search(r"\b(function\s+Component\b|Component\s*=)", src):
            problems.append("no `Component` defined — the source must declare "
                            "`function Component(...)` (or `const Component = …`)")
        problems += self._delimiter_problems(src)
        return problems

    @staticmethod
    def _delimiter_problems(src):
        """Balanced-delimiter scan that skips JS strings and comments, so braces
        in text/CSS don't cause false positives. Heuristic (a template literal's
        ``${…}`` is treated as opaque), but catches the common unbalanced edit."""
        close_to_open = {")": "(", "]": "[", "}": "{"}
        stack = []
        i, n = 0, len(src)
        mode = None  # None | "'" | '"' | '`' | '//' | '/*'
        while i < n:
            c = src[i]
            nxt = src[i + 1] if i + 1 < n else ""
            if mode in ("'", '"', "`"):
                if c == "\\":
                    i += 2; continue
                if c == mode:
                    mode = None
                i += 1; continue
            if mode == "//":
                if c == "\n":
                    mode = None
                i += 1; continue
            if mode == "/*":
                if c == "*" and nxt == "/":
                    mode = None; i += 2; continue
                i += 1; continue
            if c == "/" and nxt == "/":
                mode = "//"; i += 2; continue
            if c == "/" and nxt == "*":
                mode = "/*"; i += 2; continue
            if c in ("'", '"', "`"):
                mode = c; i += 1; continue
            if c in "([{":
                stack.append(c); i += 1; continue
            if c in ")]}":
                if not stack or stack[-1] != close_to_open[c]:
                    return [f"unbalanced '{c}'"]
                stack.pop(); i += 1; continue
            i += 1
        if stack:
            return [f"unclosed '{stack[-1]}'"]
        return []

    # -- input routing (panel -> Python) -------------------------------------
    # on() / on_message() / _handle_input() AND request/response (on_request /
    # _handle_request, answering canvas.request) all come from _EventRouter, shared
    # verbatim with Custom — so the two panel kinds route identically.