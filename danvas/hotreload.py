"""The file-watching monitor behind ``Canvas.serve(hot_reload=True)``.

Split out of :mod:`danvas.canvas` because it touches none of the canvas state —
it only watches files and respawns the worker subprocess. The worker process runs
the real server; this side just restarts it on edits.
"""

import ast
import copy
import glob
import json
import os
import secrets
import subprocess
import sys
import time
import urllib.request


def _snapshot(directory, watch_patterns=()):
    """mtimes of the files the monitor watches.

    Always the top-level ``.py`` files in ``directory`` (the script and its
    siblings); plus any file matching a ``serve(watch=...)`` glob, resolved
    relative to ``directory`` — so ``"*.jsx"`` catches sibling JSX and
    ``"panels/**/*.css"`` reaches into subdirectories. A change to any of them
    makes the monitor restart the worker (which re-reads files loaded via
    ``path=``)."""
    out = {}
    for fname in os.listdir(directory):
        if fname.endswith(".py"):
            fpath = os.path.join(directory, fname)
            try:
                out[fpath] = os.path.getmtime(fpath)
            except OSError:
                pass
    for pattern in watch_patterns:
        for fpath in glob.glob(os.path.join(directory, pattern), recursive=True):
            if os.path.isfile(fpath):
                try:
                    out[fpath] = os.path.getmtime(fpath)
                except OSError:
                    pass
    return out


def _react_source_diff(old_text, new_text):
    """Determine whether only React source strings changed between two script versions.

    Returns a ``{component_name: new_source}`` dict when the only differences
    are in top-level string variables that are wired to ``canvas.react(source=)``.
    Returns an empty dict when the two texts are structurally identical (e.g. only
    whitespace or comments changed).  Returns ``None`` when a full restart is needed.
    """
    try:
        old_tree = ast.parse(old_text)
        new_tree = ast.parse(new_text)
    except SyntaxError:
        return None

    # Compare structure with all string constant values zeroed out.
    class _ZeroStr(ast.NodeTransformer):
        def visit_Constant(self, node):
            if isinstance(node.value, str):
                return ast.Constant(value="")
            return node

    old_struct = ast.dump(_ZeroStr().visit(copy.deepcopy(old_tree)))
    new_struct = ast.dump(_ZeroStr().visit(copy.deepcopy(new_tree)))
    if old_struct != new_struct:
        return None  # structural change → full restart

    # Collect top-level bare-name string assignments.
    def _str_assigns(tree):
        out = {}
        for node in tree.body:
            if (isinstance(node, ast.Assign)
                    and len(node.targets) == 1
                    and isinstance(node.targets[0], ast.Name)
                    and isinstance(node.value, ast.Constant)
                    and isinstance(node.value.value, str)):
                out[node.targets[0].id] = node.value.value
        return out

    old_strs = _str_assigns(old_tree)
    new_strs = _str_assigns(new_tree)
    changed_vars = {k for k in new_strs if new_strs[k] != old_strs.get(k)}

    if not changed_vars:
        # Make sure no string constants changed anywhere else in the file
        # (e.g. a color literal inside a function call). The structure check
        # above zeroed all strings, so it couldn't catch those differences.
        def _all_strings(tree):
            return [n.value for n in ast.walk(tree)
                    if isinstance(n, ast.Constant) and isinstance(n.value, str)]
        if _all_strings(old_tree) != _all_strings(new_tree):
            return None  # string changed but not a React source var → full restart
        return {}  # only whitespace/comments changed

    # Build var_name → component_name mapping from canvas.react(source=VAR, name="X") calls.
    def _source_map(tree):
        mapping = {}
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "react"):
                continue
            source_var = comp_name = None
            for kw in node.keywords:
                if kw.arg == "source" and isinstance(kw.value, ast.Name):
                    source_var = kw.value.id
                elif kw.arg == "name" and isinstance(kw.value, ast.Constant):
                    comp_name = kw.value.value
            if source_var and comp_name:
                mapping[source_var] = comp_name
        return mapping

    var_to_comp = _source_map(new_tree)

    updates = {}
    for var in changed_vars:
        comp_name = var_to_comp.get(var)
        if comp_name is None:
            return None  # changed string is not a React source → full restart
        updates[comp_name] = new_strs[var]

    return updates


def _apply_partial_hot_update(port, updates):
    """POST each changed source string to the running worker's internal endpoint.

    Returns True if all updates were delivered, False if any failed (in which
    case the caller should fall through to a full restart).
    """
    for comp_name, source in updates.items():
        body = json.dumps({"name": comp_name, "source": source}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/__hot_source__",
            data=body,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                result = json.loads(resp.read())
            if not result.get("ok"):
                print(f"danvas hot reload: could not update {comp_name!r}: "
                      f"{result.get('error')}")
                return False
            print(f"danvas hot reload: live-updated {comp_name!r} (no restart)")
        except OSError as exc:
            print(f"danvas hot reload: partial update failed ({exc}); restarting...")
            return False
    return True


def _apply_live_patch(port, old_text, new_text):
    """POST old + new script text to the worker's ``/__hot_patch__`` endpoint.

    The worker classifies the diff and, when only top-level function bodies
    changed, swaps those code objects in place — no restart, so the worker's
    heap, threads, and connections are preserved. Returns True on a clean live
    patch (the caller skips the restart), False to fall back to a restart (the
    change wasn't body-only, or the worker couldn't safely apply it).
    """
    body = json.dumps({"old": old_text, "new": new_text}).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/__hot_patch__",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            result = json.loads(resp.read())
    except OSError:
        return False  # worker not reachable yet — fall back to a restart
    if not result.get("ok"):
        return False
    swapped = result.get("swapped") or []
    if swapped:
        print(f"danvas hot reload: live-patched {', '.join(swapped)} "
              "(no restart)")
    return True


def run_monitor(main_file, tunnel=False, port=8000, tunnel_provider="cloudflared",
                watch=None, broker=False):
    """Re-run ``main_file`` as a subprocess, restarting it on edits.

    This is the monitor side of ``serve(hot_reload=True)``: it never binds a
    port itself, just watches the script's directory (top-level ``.py`` files,
    plus any ``watch=`` globs) by polling mtimes, and respawns the worker
    subprocess on any change or addition/removal. The worker is launched with
    ``_danvas_RELOAD_WORKER=1``
    so its own ``serve(hot_reload=True)`` call skips straight to actually
    serving; ``_danvas_RELOAD_RESTART=1`` is added from the second launch
    onward so it doesn't reopen the browser (the frontend reconnects its
    existing websocket automatically).

    Before tearing the running worker down, each edit is pre-flighted in
    ``_danvas_RELOAD_CHECK`` mode (the script runs but serve() exits before
    binding). If that fails -- a syntax slip, a bad import, an exception in the
    module body -- the restart is skipped and the last working version keeps
    serving, so a half-finished edit doesn't take the canvas down.

    When ``tunnel`` is set, the public tunnel is opened *here*, in the monitor,
    rather than in each worker: the monitor outlives every restart, so one tunnel
    to ``port`` stays up across reloads — the public URL never changes and the
    provider (e.g. cloudflared) is started only once, instead of a fresh tunnel
    per edit (which churns quick-tunnel rate limits). Workers bind ``port``
    behind it; during the brief restart gap visitors see a momentary 502 until
    the new worker is up, then the browser reconnects on its own.
    """
    directory = os.path.dirname(os.path.abspath(main_file)) or "."
    watch_patterns = list(watch or [])

    def snapshot():
        return _snapshot(directory, watch_patterns)

    def stop(proc):
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()

    base_env = dict(os.environ)
    base_env["_danvas_RELOAD_WORKER"] = "1"
    # One signing key shared by every worker this monitor spawns, so a viewer's
    # session cookie stays valid across restarts (no re-login on each edit). See
    # server._session_secret.
    base_env.setdefault("_danvas_RELOAD_SECRET", secrets.token_urlsafe(32))

    def spawn(restart):
        env = dict(base_env)
        if restart:
            env["_danvas_RELOAD_RESTART"] = "1"
        from ._procown import spawn_owned
        return spawn_owned([sys.executable, main_file, *sys.argv[1:]],
                           env=env)

    def script_ok():
        """True if the edited script imports/runs cleanly (pre-flight).

        Runs it in check mode -- the body executes but serve() exits before
        binding a port or starting threads, so this never collides with the
        worker that's still serving. On failure the captured stderr is surfaced
        so the error is visible in the console.
        """
        env = dict(base_env)
        env["_danvas_RELOAD_CHECK"] = "1"
        result = subprocess.run(
            [sys.executable, main_file, *sys.argv[1:]],
            env=env, capture_output=True, text=True,
        )
        if result.returncode != 0:
            sys.stderr.write(result.stderr or "")
        return result.returncode == 0

    def wait_for_edit(last):
        """Block until a watched file changes; return the new snapshot."""
        while True:
            time.sleep(0.5)
            snap = snapshot()
            if snap != last:
                return snap

    # Broker mode: spawn danvasd ONCE here in the long-lived monitor (like the
    # tunnel below), so it outlives every worker restart. Workers dial into it
    # as the "host" source (via _danvas_BROKER_PORT) instead of binding the
    # port themselves — so the browser stays connected to danvasd across an
    # edit and retention holds the panels (dim -> refresh) with no disconnect,
    # no 502. A failure to launch is non-fatal: fall back to embedded workers.
    danvasd_proc = None
    if broker:
        from .remote import _find_danvasd, _probe_hub, _port_accepts, \
            _STALE_HINT
        binary = _find_danvasd()
        if binary:
            from ._procown import spawn_owned
            danvasd_proc = spawn_owned(
                [binary, "--port", str(port), "--host", "127.0.0.1"])
            # Readiness = the hub ANSWERS its own HTTP routes, not bare TCP
            # accept — a wedged broker from a hard-killed session still
            # accepts, and dialing it poisons every worker with websocket
            # timeouts (see remote.serve_via_broker, same logic).
            _end = time.time() + 15
            up = False
            while time.time() < _end:
                if danvasd_proc.poll() is not None:
                    # Bind failed: someone holds the port. A LIVE danvas hub
                    # is a previous session's survivor — reuse it (that's the
                    # designed restart path); anything else is fatal here.
                    if _probe_hub(port) == "danvasd":
                        print(f"danvas hot reload: attaching to the danvasd "
                              f"already on port {port}")
                        danvasd_proc = None
                        up = True
                    elif _port_accepts(port):
                        print("danvas hot reload: port "
                              f"{port} is taken by something that never "
                              f"answers.\n{_STALE_HINT}")
                    break
                if _probe_hub(port, timeout=0.5) == "danvasd":
                    # A survivor answers like our own spawn while our doomed
                    # bind is in flight — let the spawn lose the race first
                    # so the exit branch above classifies it (attach/diagnose).
                    time.sleep(0.2)
                    if danvasd_proc.poll() is not None:
                        continue
                    up = True
                    break
                time.sleep(0.1)
            if up:
                base_env["_danvas_BROKER_PORT"] = str(port)
                print(f"danvas hot reload: canvas at http://127.0.0.1:{port} "
                      "(broker danvasd — the UI survives every restart)",
                      flush=True)
            else:
                danvasd_proc = None
                print("danvas hot reload: danvasd wouldn't start; workers "
                      "will try to serve themselves (serving is broker-only "
                      "— fix the port conflict above if they fail too)")

    _watched = "*.py" + (", " + ", ".join(watch_patterns) if watch_patterns else "")
    print(f"danvas hot reload: watching {directory} ({_watched})")
    proc = spawn(restart=False)
    last = snapshot()

    def _read_main():
        try:
            with open(main_file, encoding="utf-8", errors="replace") as f:
                return f.read()
        except OSError:
            return ""

    main_file_abs = os.path.abspath(main_file)
    old_script_text = _read_main()

    # Open the tunnel once, here in the long-lived monitor, so it survives every
    # worker restart (stable URL, no per-edit churn). A failure to start is
    # non-fatal: keep serving locally and say so.
    persistent_tunnel = None
    if tunnel:
        from .tunnel import open_tunnel
        try:
            persistent_tunnel = open_tunnel(port, provider=tunnel_provider)
            print(f"danvas public URL: {persistent_tunnel.url}"
                  "   <- share this; it stays put across hot reloads")
        except Exception as exc:  # noqa: BLE001 - surface any provider failure
            print(f"danvas hot reload: could not start the {tunnel_provider} "
                  f"tunnel ({exc}); serving locally only.")
    try:
        while True:
            # Wait for either a file edit or the worker exiting on its own.
            changed = False
            prev_snap = last
            while proc.poll() is None:
                time.sleep(0.5)
                snap = snapshot()
                if snap != prev_snap:
                    last = snap
                    changed = True
                    break
            if not changed:
                # Worker ended without an edit: a clean exit (e.g. a closed
                # desktop window) stops the monitor; a crash leaves it watching
                # so the next save can bring the canvas back.
                if proc.returncode in (0, None):
                    return
                print("danvas hot reload: the app exited with an error; "
                      "waiting for the next save...")
                last = wait_for_edit(last)
                prev_snap = last  # same as last → only_main_changed=False → full restart

            # Debounce: wait until the watched files stop changing (formatter /
            # editor may write the file several times in quick succession after a
            # save). Re-sample until two consecutive snapshots agree.
            settle_snap = last
            while True:
                time.sleep(0.25)
                s = snapshot()
                if s == settle_snap:
                    break
                settle_snap = s
                last = s

            new_script_text = _read_main()
            print("danvas hot reload: change detected, checking...")

            # Partial React-source hot update: skip the restart entirely when the
            # only change is in top-level string variables used as canvas.react(source=).
            only_main_changed = (
                danvasd_proc is None   # live-patch HTTP-pokes the worker's own
                                       # endpoints; in broker mode danvasd owns
                                       # the port, so just restart (seamless via
                                       # retention — the browser never drops).
                and prev_snap.get(main_file_abs) != last.get(main_file_abs)
                and set(prev_snap.keys()) == set(last.keys())
                and all(prev_snap[f] == last[f]
                        for f in last if f != main_file_abs)
            )
            if only_main_changed:
                updates = _react_source_diff(old_script_text, new_script_text)
                if updates is not None:
                    old_script_text = new_script_text
                    if not updates:
                        # Only whitespace / comments changed — no restart needed.
                        continue
                    if _apply_partial_hot_update(port, updates):
                        continue
                    # HTTP call failed (worker not ready yet?): fall through.
                elif _apply_live_patch(port, old_script_text, new_script_text):
                    # Only top-level function bodies changed — the worker swapped
                    # those code objects live, so its heap, threads, sockets, and
                    # panel state are untouched. No restart.
                    old_script_text = new_script_text
                    continue

            if not script_ok():
                print("danvas hot reload: the edit has an error -- keeping "
                      "the running version. Fix it and save again.")
                continue
            if proc.poll() is None:
                stop(proc)
            print("danvas hot reload: restarting...")
            proc = spawn(restart=True)
            old_script_text = new_script_text
    except KeyboardInterrupt:
        pass
    finally:
        if proc is not None and proc.poll() is None:
            stop(proc)
        if danvasd_proc is not None and danvasd_proc.poll() is None:
            stop(danvasd_proc)
        if persistent_tunnel is not None:
            persistent_tunnel.stop()