//! danvasd: the danvas standing broker (relay core).
//!
//! Speaks wire protocol v1 (../PROTOCOL.md) as a hub: dial-in sources
//! (`/ws?source=1&label=`) contribute panels; browsers (and peer subscribers)
//! receive the composed canvas; interactions route back to the owning source.
//! The design rule is the plan's "parse the envelope, not the world": frames
//! are `serde_json::Value`s — only `type`/`id`/`name`/`owner`/`start`/`end`
//! and the geometry keys are touched; everything else passes through, so new
//! panel types work through an old broker unchanged.
//!
//! Scope (phase 1, relay core): namespacing, caching + replay, fan-out,
//! input/set_props/layout route-back, subscribe/unsubscribe, retention
//! (default on: a dead source's panels freeze dimmed until its label
//! re-dials). Not yet: auth, drawings, offsets, ledger, static frontend —
//! tracked in ../docs/broker-plan.md phase 2+. The definition of done at
//! every step is ../tests/test_conformance.py (DANVAS_HUB_CMD).

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, StatusCode, Uri};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use include_dir::{include_dir, Dir};
use serde_json::{json, Map, Value};

const PROTOCOL_VERSION: u64 = 1;
const FREEZE_OPACITY: f64 = 0.45;

/// The same pre-built frontend the Python package ships, embedded at compile
/// time — a browser points straight at danvasd, no Python anywhere.
static DIST: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/../danvas/frontend/dist");

/// The language-neutral panel templates + contracts, served at /__templates__
/// so an SDK can FETCH them from the hub it dials into instead of shipping a
/// copy -- version-matched to this binary's embedded frontend by construction
/// (PROTOCOL.md, authoring native panels).
static TEMPLATES: &str = include_str!("../../danvas/templates/components.json");

/// One outbound frame: the wire has text (JSON) and binary (media) kinds.
enum Out {
    T(String),
    B(Vec<u8>),
    /// Terminal marker: the writer sends a WebSocket Close and returns. Emitted
    /// on disconnect so the peer's own close handshake completes at once — a
    /// client with buffered-but-unread frames must not have to wait out its
    /// close_timeout because we merely dropped the socket.
    Close,
}

/// Per-connection outbound buffer with *latest-wins conflation under
/// backpressure*. Below the threshold every frame is delivered in order
/// (fifo, the common case). Once a slow viewer's buffer backs up past
/// CONFLATE_THRESHOLD, further `update`/media frames for a panel REPLACE that
/// panel's already-pending frame in place instead of appending — so a slow
/// viewer's memory stays bounded and it always sees the *latest* state,
/// without throttling the source or the other viewers. This is the hub-side
/// `queue="latest"` safety ceiling (matters most for video: one slow phone
/// must not stall the camera for everyone).
const CONFLATE_THRESHOLD: usize = 64;

#[derive(Default)]
struct ConnOut {
    items: std::collections::BTreeMap<u64, Out>,
    latest: HashMap<String, u64>, // conflate key -> seq of its pending item
    seq: u64,
}

impl ConnOut {
    fn push(&mut self, out: Out) {
        // Only conflate when this connection is already behind; otherwise
        // preserve strict order.
        if self.items.len() >= CONFLATE_THRESHOLD {
            if let Some(key) = conflate_key(&out) {
                if let Some(&seq) = self.latest.get(&key) {
                    if let Some(slot) = self.items.get_mut(&seq) {
                        *slot = out; // latest wins, keeps queue position
                        return;
                    }
                }
                let seq = self.seq;
                self.seq += 1;
                self.items.insert(seq, out);
                self.latest.insert(key, seq);
                return;
            }
        }
        let seq = self.seq;
        self.seq += 1;
        self.items.insert(seq, out);
    }

    fn drain(&mut self) -> Vec<Out> {
        self.latest.clear();
        std::mem::take(&mut self.items).into_values().collect()
    }
}

/// The conflation key for a frame, or None for order-critical frames
/// (register/remove/arrow/shape/draw/chat/presence/response/file_*/...).
/// Only ever called on the slow path (buffer already backed up), so the
/// per-frame parse is acceptable.
fn conflate_key(out: &Out) -> Option<String> {
    match out {
        Out::T(text) => {
            let v: Value = serde_json::from_str(text).ok()?;
            if v.get("type").and_then(Value::as_str) == Some("update") {
                let id = v.get("id").and_then(Value::as_str)?;
                Some(format!("u:{id}"))
            } else {
                None
            }
        }
        Out::B(data) => {
            // Media envelopes ([code][idLen][id][payload]) conflate by
            // (code,id); FILE transfers (code 6) never conflate.
            if data.len() < 2 || data[0] == BIN_FILE {
                return None;
            }
            let id = bin_id(data)?;
            Some(format!("b:{}:{id}", data[0]))
        }
        Out::Close => None,
    }
}

/// A connection's send handle: an ordered/conflating buffer plus a waker for
/// its writer task. `send` is non-blocking and never drops frames (it may
/// coalesce them under backpressure).
struct Conn {
    out: std::sync::Mutex<ConnOut>,
    wake: tokio::sync::Notify,
}

impl Conn {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            out: std::sync::Mutex::new(ConnOut::default()),
            wake: tokio::sync::Notify::new(),
        })
    }

    fn send(&self, out: Out) {
        self.out.lock().unwrap().push(out);
        self.wake.notify_one();
    }

    fn drain(&self) -> Vec<Out> {
        self.out.lock().unwrap().drain()
    }
}

type Tx = Arc<Conn>;

#[derive(Default)]
struct Source {
    tag: String,
    live: bool,
    /// The 📍 origin this source is merged at: applied to content coming
    /// down, undone on interactions going back — the source never moves.
    offset: (f64, f64),
    tx: Option<Tx>,
    /// nsid -> register frame (insertion order preserved via Vec of keys).
    reg_order: Vec<String>,
    registers: HashMap<String, Value>,
    updates: HashMap<String, Map<String, Value>>,
    arrows: HashMap<String, Value>,
    /// namespaced record id -> the record's current ("after") state.
    drawings: HashMap<String, Value>,
    /// nsid -> managed-shape frame, kept current (shape_update folds in).
    shapes: HashMap<String, Value>,
    /// Latest shared-assets frame (define/style cumulative snapshot).
    shared: Option<Value>,
    /// Latest graveyard_update (item ids namespaced).
    graveyard: Option<Value>,
}

/// Rewrite every record id (diff keys, records' own `id`, arrow bindings)
/// through `f`. `updated` values are `[before, after]` pairs.
fn remap_draw_diff(diff: &Value, f: &dyn Fn(&str) -> String) -> Value {
    let remap_record = |val: &Value| -> Value {
        let mut v = val.clone();
        if let Some(obj) = v.as_object_mut() {
            if let Some(Value::String(id)) = obj.get("id") {
                let nid = f(id);
                obj.insert("id".into(), Value::String(nid));
            }
            if let Some(Value::Object(props)) = obj.get_mut("props") {
                for key in ["bindStart", "bindEnd"] {
                    if let Some(Value::String(b)) = props.get(key) {
                        let nb = f(b);
                        props.insert(key.into(), Value::String(nb));
                    }
                }
            }
        }
        v
    };
    let mut out = Map::new();
    for bucket in ["added", "updated", "removed"] {
        let mut nb = Map::new();
        if let Some(Value::Object(b)) = diff.get(bucket) {
            for (rid, val) in b {
                let nv = match val {
                    Value::Array(pair) if pair.len() == 2 => Value::Array(
                        vec![remap_record(&pair[0]), remap_record(&pair[1])]),
                    other => remap_record(other),
                };
                nb.insert(f(rid), nv);
            }
        }
        out.insert(bucket.into(), Value::Object(nb));
    }
    Value::Object(out)
}

#[derive(Default)]
struct Hub {
    run_id: String,
    tag_seq: u64,
    /// browser conn id -> sender
    browsers: HashMap<u64, Tx>,
    /// source label -> Source (kept while retained-offline too)
    sources: HashMap<String, Source>,
    tag_to_label: HashMap<String, String>,
    /// composed panel id -> subscriber conn ids (browsers or sources)
    subs: HashMap<String, HashSet<u64>>,
    /// conn id -> sender for ANY connection (for subscription copies)
    conns: HashMap<u64, Tx>,
    /// Hub-native annotation ink (bare record ids): record id -> record.
    drawings: HashMap<String, Value>,
    /// --password gate: None = open. Sessions are opaque server-minted
    /// tokens carried in the pc_session cookie (PROTOCOL.md §transport);
    /// each maps to the login ROLE (None for the single shared password).
    /// DANVAS_ROLE_PASSWORDS=role=pw,role2=pw2 defines role logins.
    password: Option<String>,
    role_passwords: Vec<(String, String)>,
    /// token -> (role, expiry). Bounded by a TTL + prune-on-insert so a
    /// long-lived protected broker's login tokens can't grow without limit
    /// (the Python hub is stateless/HMAC-signed and needs no such store).
    sessions: HashMap<String, (Option<String>, std::time::Instant)>,
    /// Dialed-out sources (merge_add): label -> the retrying dial task, so
    /// merge_remove can stop it for good.
    dial_tasks: HashMap<String, tokio::task::JoinHandle<()>>,
    /// DANVAS_LEDGER=<path.db>: append routed user actions to the SQLite
    /// event ledger (the same schema danvas/_ledger.py writes).
    ledger: Option<rusqlite::Connection>,
    /// reqId -> (asker conn, expiry): the owner's `response` frame routes
    /// back to exactly the viewer that sent the `request`.
    pending_req: HashMap<String, (u64, std::time::Instant)>,
    /// conn id -> viewer meta ({id, name, color, ...}) — everyone connected,
    /// sources included (a process peer is a viewer too).
    viewers: HashMap<u64, Value>,
    chat_history: Vec<Value>,
    chat_seq: u64,
    /// The hub view (camera/chrome), folded from sources' `view` frames and
    /// baked into welcome for late joiners.
    hub_view: Map<String, Value>,
    /// serve(merge_server=): a standing merge server URL the UI shows a
    /// "Merge…" button for (welcome.mergeServer). self_url is the address that
    /// server dials back to reach this canvas.
    merge_server: Option<String>,
    self_url: Option<String>,
    /// UI-affordance gating, baked into every browser's welcome. The owner
    /// (serve()) resolves the default (Inspector/cursors default on only for a
    /// private local bind) and passes the decision as a flag, so a broker-served
    /// canvas gates exactly like the embedded one.
    ui_inspector: bool,
    ui_graveyard: bool,
    cursors: bool,
    /// The 🌐 hosting button: on for a private loopback bind (like the
    /// Inspector). port is filled in main so host_lan/host_tunnel know it.
    ui_hosting: bool,
    host_port: u16,
    lan_url: Option<String>,
    tunnel_url: Option<String>,
    hosting_busy: Option<String>,
    hosting_error: Option<String>,
    lan_shutdown: Option<tokio::sync::watch::Sender<bool>>,
    tunnel_child: Option<std::process::Child>,
    /// In-flight file pulls (HTTP download -> owning source): reqId ->
    /// (meta, bytes, sources-still-undeclined).
    pending_files: HashMap<String, (Option<Value>, Option<Vec<u8>>, usize)>,
    /// In-flight upload pushes: reqId -> (ack, sources-still-undeclined).
    pending_uploads: HashMap<String, (Option<Value>, usize)>,
}

const VIEWER_COLORS: [&str; 6] =
    ["#ef4444", "#f59e0b", "#10b981", "#3b82f6", "#8b5cf6", "#ec4899"];

fn open_ledger(path: &str) -> Option<rusqlite::Connection> {
    let conn = rusqlite::Connection::open(path).ok()?;
    let _ = conn.pragma_update(None, "journal_mode", "WAL");
    let _ = conn.pragma_update(None, "synchronous", "NORMAL");
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
         CREATE TABLE IF NOT EXISTS snapshots (
           seq INTEGER PRIMARY KEY AUTOINCREMENT,
           ts REAL NOT NULL, state TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS events (
           seq INTEGER PRIMARY KEY AUTOINCREMENT,
           ts REAL NOT NULL, type TEXT NOT NULL, comp TEXT, payload TEXT);
         INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', '1');",
    )
    .ok()?;
    Some(conn)
}

fn ledger_record(h: &Hub, kind: &str, comp: Option<&str>, payload: &Value) {
    if let Some(conn) = &h.ledger {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        let _ = conn.execute(
            "INSERT INTO events (ts, type, comp, payload) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![ts, kind, comp, payload.to_string()],
        );
    }
}

/// Normalise a merge_add spec to `(ws_uri, label)`: bare port, host:port, or
/// a full http(s)/ws(s) URL — the same forms the Python hub accepts.
fn normalize_source_uri(spec: &str) -> Option<(String, String)> {
    let text = spec.trim();
    if text.is_empty() {
        return None;
    }
    if let Some((scheme, rest)) = text.split_once("://") {
        let ws_scheme = match scheme {
            "http" | "ws" => "ws",
            "https" | "wss" => "wss",
            _ => return None,
        };
        let rest = rest.trim_end_matches('/');
        let label = rest.split('/').next().unwrap_or(rest).to_string();
        let path = if rest.ends_with("/ws") {
            rest.to_string()
        } else {
            format!("{rest}/ws")
        };
        return Some((format!("{ws_scheme}://{path}"), label));
    }
    let hostport = if let Some(p) = text.strip_prefix(':') {
        format!("localhost:{p}")
    } else if text.contains(':') {
        text.to_string()
    } else {
        format!("localhost:{text}")
    };
    Some((format!("ws://{hostport}/ws"), hostport))
}

impl Hub {
    fn protected(&self) -> bool {
        self.password.is_some() || !self.role_passwords.is_empty()
    }

    /// Ok(role) when the request carries a valid session (role None for the
    /// shared password / an open hub); Err(()) when it must be refused.
    fn session_role(&self, headers: &HeaderMap) -> Result<Option<String>, ()> {
        if !self.protected() {
            return Ok(None);
        }
        let Some(cookies) = headers.get(header::COOKIE).and_then(|v| v.to_str().ok())
        else {
            return Err(());
        };
        let now = std::time::Instant::now();
        for c in cookies.split(';') {
            if let Some(t) = c.trim().strip_prefix("pc_session=") {
                if let Some((role, exp)) = self.sessions.get(t) {
                    if *exp > now {
                        return Ok(role.clone());
                    }
                }
            }
        }
        Err(())
    }

    fn authed(&self, headers: &HeaderMap) -> bool {
        self.session_role(headers).is_ok()
    }
}

const LOGIN_PAGE: &str = r#"<!doctype html><html><head><title>danvas</title></head>
<body style="font-family:system-ui;background:#111;color:#eee;display:flex;
align-items:center;justify-content:center;height:100vh;margin:0">
<form method="post" action="/__auth__" style="text-align:center">
<h2>This canvas is protected</h2>
<input type="password" name="password" autofocus
 style="padding:8px 12px;font-size:14px;border-radius:8px;border:1px solid #444;
 background:#1c1c1c;color:#eee">
<button type="submit" style="padding:8px 16px;font-size:14px;border-radius:8px;
 border:none;background:#2563eb;color:#fff;cursor:pointer">Enter</button>
</form></body></html>"#;

/// Minimal x-www-form-urlencoded "password" extraction (+ and %XX decoded).
fn form_password(body: &str) -> Option<String> {
    for pair in body.split('&') {
        let (k, v) = pair.split_once('=')?;
        if k != "password" {
            continue;
        }
        let mut out = Vec::new();
        let bytes = v.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            match bytes[i] {
                b'+' => out.push(b' '),
                b'%' if i + 2 < bytes.len() => {
                    let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok()?;
                    out.push(u8::from_str_radix(hex, 16).ok()?);
                    i += 2;
                }
                b => out.push(b),
            }
            i += 1;
        }
        return String::from_utf8(out).ok();
    }
    None
}

/// Fold an update payload into the replay cache the way the OWNER's own
/// reconnect replay would express it: geometry onto the cached register's top
/// level, a value `post` into the register's baked `props.data`, the rest
/// onto the accumulated updates. This is what makes a hub browser-refresh
/// equivalent to a direct source reconnect (transient channels like `post`
/// don't survive a fresh mount). The `props.data` peek is the one bounded
/// exception to "parse the envelope, not the world" — the built-in controls'
/// value convention.
fn fold_state(src: &mut Source, nsid: &str, payload: Map<String, Value>) {
    let mut rest = payload;
    if let Some(reg) = src.registers.get_mut(nsid) {
        if let Some(obj) = reg.as_object_mut() {
            for k in ["x", "y", "rotation", "opacity"] {
                if rest.get(k).map(|v| v.is_number()).unwrap_or(false) {
                    obj.insert(k.into(), rest.remove(k).unwrap());
                }
            }
            if let Some(post) = rest.get("post").cloned() {
                let folded = obj
                    .get_mut("props")
                    .and_then(Value::as_object_mut)
                    .and_then(|props| {
                        let data = props.get("data")?.as_str()?;
                        let mut blob: Value = serde_json::from_str(data).ok()?;
                        let b = blob.as_object_mut()?;
                        // The built-in controls' content keys — the one
                        // bounded convention the hub knows about panels.
                        let key = ["value", "text", "src"]
                            .into_iter()
                            .find(|k| b.contains_key(*k))?;
                        b.insert(key.into(), post.clone());
                        props.insert("data".into(), Value::String(blob.to_string()));
                        Some(())
                    })
                    .is_some();
                if folded {
                    rest.remove("post");
                }
            }
        }
    }
    // The streaming-figure channel (PROTOCOL.md, update-payload vocabulary):
    // a full `plot` supersedes any pending `plot_extend`, and a `plot_extend`
    // folds INTO the cached figure (append per trace index, capped at `max`)
    // -- so a late-joining browser replays the complete curve, and a
    // reconnecting one never double-applies the last delta.
    if rest.contains_key("plot") {
        if let Some(u) = src.updates.get_mut(nsid) {
            u.remove("plot_extend");
        }
    } else if let Some(ext) = rest.remove("plot_extend") {
        let folded = src
            .updates
            .get_mut(nsid)
            .and_then(|u| u.get_mut("plot"))
            .map(|fig| apply_plot_extend(fig, &ext))
            .unwrap_or(false);
        if !folded {
            // Nothing to fold into (no full figure seen yet): keep the last
            // delta as before -- a partial replay beats an empty one.
            rest.insert("plot_extend".into(), ext);
        }
    }
    if !rest.is_empty() {
        src.updates.entry(nsid.to_string()).or_default().extend(rest);
    }
}

/// Append a `plot_extend` delta ({indices, x, y, max}) into a cached Plotly
/// figure, mirroring the frontend's Plotly.extendTraces application. Returns
/// false when the figure/delta shapes don't line up (caller keeps the delta).
fn apply_plot_extend(fig: &mut Value, ext: &Value) -> bool {
    let Some(data) = fig.get_mut("data").and_then(Value::as_array_mut) else {
        return false;
    };
    let Some(indices) = ext.get("indices").and_then(Value::as_array) else {
        return false;
    };
    let (Some(xs), Some(ys)) = (ext.get("x").and_then(Value::as_array),
                                ext.get("y").and_then(Value::as_array)) else {
        return false;
    };
    let max = ext.get("max").and_then(Value::as_u64).map(|m| m as usize);
    for (k, ti) in indices.iter().enumerate() {
        let Some(ti) = ti.as_u64().map(|t| t as usize) else { continue };
        let Some(trace) = data.get_mut(ti).and_then(Value::as_object_mut) else {
            continue;
        };
        for (axis, deltas) in [("x", xs), ("y", ys)] {
            let add = deltas.get(k).and_then(Value::as_array).cloned()
                .unwrap_or_default();
            let arr = trace.entry(axis.to_string())
                .or_insert_with(|| Value::Array(vec![]));
            if let Some(a) = arr.as_array_mut() {
                a.extend(add);
                if let Some(m) = max {
                    if a.len() > m {
                        let cut = a.len() - m;
                        a.drain(0..cut);
                    }
                }
            }
        }
    }
    true
}

/// Shift a frame's top-level or payload x/y by (dx, dy) where present.
fn shift_xy(obj: &mut Map<String, Value>, dx: f64, dy: f64) {
    for (key, d) in [("x", dx), ("y", dy)] {
        if let Some(v) = obj.get(key).and_then(Value::as_f64) {
            obj.insert(key.into(), json!(v + d));
        }
    }
}

impl Hub {
    fn fanout_browsers(&self, text: &str) {
        for tx in self.browsers.values() {
            let _ = tx.send(Out::T(text.to_string()));
        }
    }

    /// Fan out to every subscriber EXCEPT one connection — used to keep a
    /// source's own frames from echoing back to it (a source is a subscriber
    /// too, but must not receive its own registers/updates/ink).
    fn fanout_browsers_except(&self, text: &str, except: u64) {
        for (id, tx) in &self.browsers {
            if *id != except {
                let _ = tx.send(Out::T(text.to_string()));
            }
        }
    }

    fn cached_frames(src: &Source) -> Vec<Value> {
        let mut out = Vec::new();
        if let Some(shared) = &src.shared {
            // Before the registers: React panels mount with shared components
            // and the global stylesheet already in place.
            out.push(shared.clone());
        }
        for id in &src.reg_order {
            if let Some(reg) = src.registers.get(id) {
                out.push(reg.clone());
            }
        }
        for (id, payload) in &src.updates {
            out.push(json!({"type": "update", "id": id, "payload": payload}));
        }
        for arrow in src.arrows.values() {
            out.push(arrow.clone());
        }
        for shape in src.shapes.values() {
            out.push(shape.clone());
        }
        if let Some(gy) = &src.graveyard {
            out.push(gy.clone());
        }
        if !src.drawings.is_empty() {
            out.push(json!({"type": "draw", "diff": {
                "added": src.drawings.clone().into_iter()
                    .collect::<Map<String, Value>>(),
                "updated": {}, "removed": {}}}));
        }
        if !src.live {
            out.extend(Self::freeze_frames(src));
        }
        out
    }

    fn freeze_frames(src: &Source) -> Vec<Value> {
        src.reg_order
            .iter()
            .map(|id| {
                json!({"type": "update", "id": id,
                       "payload": {"operable": false, "opacity": FREEZE_OPACITY}})
            })
            .collect()
    }

    /// The merge-panel roster: one entry per source, live or retained-offline.
    fn roster_frame(&self) -> Value {
        let sources: Vec<Value> = self
            .sources
            .iter()
            .map(|(label, s)| {
                json!({"sid": s.tag, "label": label,
                       "uri": format!("dialin:{label}"),
                       "status": if s.live { "live" } else { "offline" },
                       "offset": [s.offset.0, s.offset.1]})
            })
            .collect();
        json!({"type": "merge_sources", "sources": sources})
    }

    fn hosting_state(&self) -> Value {
        json!({
            "type": "hosting",
            "local": format!("http://127.0.0.1:{}", self.host_port),
            "lan": self.lan_url,
            "tunnel": self.tunnel_url,
            "busy": self.hosting_busy,
            "error": self.hosting_error,
        })
    }

    fn broadcast_hosting(&self) {
        let text = self.hosting_state().to_string();
        for tx in self.conns.values() {
            let _ = tx.send(Out::T(text.clone()));
        }
    }

    fn presence_frame(&self) -> Value {
        let viewers: Vec<Value> = self.viewers.values().cloned().collect();
        json!({"type": "presence", "count": viewers.len(), "viewers": viewers})
    }

    fn fanout_all(&self, text: &str) {
        for tx in self.conns.values() {
            let _ = tx.send(Out::T(text.to_string()));
        }
    }

    fn teardown_frames(src: &Source) -> Vec<Value> {
        let mut out: Vec<Value> = src
            .reg_order
            .iter()
            .chain(src.arrows.keys())
            .chain(src.shapes.keys())
            .map(|id| json!({"type": "remove", "id": id}))
            .collect();
        if !src.drawings.is_empty() {
            // Ink lives under its own ids, not shape ids — removed via a diff.
            let removed: Map<String, Value> = src
                .drawings
                .keys()
                .map(|k| (k.clone(), json!({})))
                .collect();
            out.push(json!({"type": "draw", "diff":
                {"added": {}, "updated": {}, "removed": removed}}));
        }
        out
    }
}

fn viewer_role(h: &Hub, conn_id: u64) -> Option<String> {
    h.viewers
        .get(&conn_id)
        .and_then(|v| v.get("role"))
        .and_then(Value::as_str)
        .map(String::from)
}

/// The role allowlist a relayed panel declared ([] = everyone). `frame` lets
/// a register be checked before it lands in the cache.
fn panel_roles(src: &Source, nsid: &str, frame: Option<&Value>) -> Vec<String> {
    frame
        .or_else(|| src.registers.get(nsid))
        .and_then(|r| r.get("roles"))
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).map(String::from).collect())
        .unwrap_or_default()
}

fn role_may_see(role: &Option<String>, roles: &[String]) -> bool {
    roles.is_empty()
        || role
            .as_deref()
            .map(|r| roles.iter().any(|x| x == r))
            .unwrap_or(false)
}

fn now_hex() -> String {
    let ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{ns:x}")
}

static CONN_SEQ: AtomicU64 = AtomicU64::new(1);

/// The router — rebuilt for the live LAN listener the 🌐 button can add.
fn build_app(hub: Arc<Mutex<Hub>>) -> Router {
    Router::new()
        .route("/ws", get(ws_handler))
        .route("/__auth__", post(auth_handler))
        // Liveness + identity, unauthenticated (no viewer data): lets a
        // spawner tell "a live danvas hub holds this port" apart from "a
        // wedged stale broker / some other process" before dialing /ws —
        // TCP accept alone can't (a hung broker still accepts).
        .route("/__health__", get(health_handler))
        .route("/__describe__", get(describe_handler))
        .route("/__templates__", get(|| async {
            ([(header::CONTENT_TYPE, "application/json")], TEMPLATES)
        }))
        .route("/__download__/:token", get(download_handler))
        .route("/__upload__/:token", post(upload_handler))
        .layer(axum::extract::DefaultBodyLimit::max(512 * 1024 * 1024))
        .fallback(get(static_handler))
        .with_state(hub)
}

/// Everything the `danvasd` CLI parses, as a struct — the embedded
/// entrypoint (the danvas SDK's `broker` feature) builds one directly and
/// runs the same hub in-process instead of spawning the binary.
pub struct Config {
    pub host: std::net::IpAddr,
    pub port: u16,
    pub password: Option<String>,
    pub merge_server: Option<String>,
    pub self_url: Option<String>,
    /// UI gating: `None` = the broker's own default (loopback-only for the
    /// hosting button, off otherwise), `Some` = the owner's explicit call.
    pub ui_inspector: Option<bool>,
    pub ui_graveyard: Option<bool>,
    pub ui_hosting: Option<bool>,
    pub cursors: Option<bool>,
    /// `role=password` pairs (what `DANVAS_ROLE_PASSWORDS` carries).
    pub role_passwords: Vec<(String, String)>,
    /// Ledger path (what `DANVAS_LEDGER` carries).
    pub ledger: Option<String>,
    /// Print the human-facing "[danvasd] canvas at …" line on bind.
    pub announce: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            host: std::net::IpAddr::from([127, 0, 0, 1]),
            port: 8080,
            password: None,
            merge_server: None,
            self_url: None,
            ui_inspector: None,
            ui_graveyard: None,
            ui_hosting: None,
            cursors: None,
            role_passwords: Vec::new(),
            ledger: None,
            announce: true,
        }
    }
}

/// Bind and run the hub until the future is dropped or the process ends —
/// the whole broker as one library call. The `danvasd` binary is a thin
/// argv/env parser over this; the danvas SDK's `broker` feature runs it
/// in-process on a background thread.
pub async fn run(cfg: Config) -> std::io::Result<()> {
    let hub = Arc::new(Mutex::new(Hub {
        run_id: now_hex(),
        password: cfg.password,
        role_passwords: cfg.role_passwords,
        merge_server: cfg.merge_server,
        self_url: cfg.self_url,
        ui_inspector: cfg.ui_inspector.unwrap_or(false),
        ui_graveyard: cfg.ui_graveyard.unwrap_or(false),
        cursors: cfg.cursors.unwrap_or(false),
        ledger: cfg.ledger.as_deref().and_then(open_ledger),
        ..Default::default()
    }));
    {
        let mut h = hub.lock().unwrap();
        h.host_port = cfg.port;
        // Like the Inspector: the live hosting button is on only for a private
        // loopback bind (nothing to widen once it's already LAN/public) — unless
        // the owner passed an explicit decision through serve().
        h.ui_hosting = cfg.ui_hosting.unwrap_or_else(|| cfg.host.is_loopback());
    }
    let addr = SocketAddr::from((cfg.host, cfg.port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    // The human-facing line: the page URL, browser-clickable. (The wire
    // endpoint is ws://<addr>/ws; SDKs derive it, people never open it.)
    if cfg.announce {
        let shown = if cfg.host.is_unspecified() {
            SocketAddr::from(([127, 0, 0, 1], cfg.port))
        } else {
            addr
        };
        println!("[danvasd] canvas at http://{shown}");
    }
    axum::serve(listener, build_app(hub.clone())).await
}

async fn auth_handler(State(hub): State<Arc<Mutex<Hub>>>, body: String) -> impl IntoResponse {
    let mut h = hub.lock().unwrap();
    let given = form_password(&body);
    // Which login is this? The shared --password maps to role None; a role
    // password maps to its role. An open hub redirects as a no-op.
    let role: Option<Option<String>> = if !h.protected() {
        Some(None)
    } else {
        match &given {
            Some(g) if h.password.as_ref() == Some(g) => Some(None),
            Some(g) => h
                .role_passwords
                .iter()
                .find(|(_, pw)| pw == g)
                .map(|(r, _)| Some(r.clone())),
            None => None,
        }
    };
    let Some(role) = role else {
        return (StatusCode::UNAUTHORIZED,
                [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
                LOGIN_PAGE).into_response();
    };
    let token = format!("{}{}", now_hex(), CONN_SEQ.fetch_add(1, Ordering::Relaxed));
    // Sessions live 30 days (a cookie surviving reconnects/restarts, matching
    // the Python hub's signed-token lifetime); prune the expired whenever the
    // map grows, so it stays bounded no matter how many logins occur.
    let now = std::time::Instant::now();
    let ttl = std::time::Duration::from_secs(30 * 24 * 3600);
    if h.sessions.len() > 1024 {
        h.sessions.retain(|_, (_, exp)| *exp > now);
    }
    h.sessions.insert(token.clone(), (role, now + ttl));
    (StatusCode::SEE_OTHER,
     [(header::LOCATION, "/".to_string()),
      (header::SET_COOKIE,
       format!("pc_session={token}; Path=/; SameSite=Lax; HttpOnly"))],
     "").into_response()
}

/// Downloads through the hub: the owning SOURCE holds the bytes. Broadcast
/// file_pull (tokens are opaque), the owner answers file_meta + a FILE
/// binary envelope, everyone else declines; first success streams out.
async fn download_handler(
    State(hub): State<Arc<Mutex<Hub>>>,
    headers: HeaderMap,
    axum::extract::Path(token): axum::extract::Path<String>,
) -> impl IntoResponse {
    let req = format!("{}{}", now_hex(), CONN_SEQ.fetch_add(1, Ordering::Relaxed));
    {
        let mut h = hub.lock().unwrap();
        if !h.authed(&headers) {
            return (StatusCode::UNAUTHORIZED, "login required").into_response();
        }
        let targets: Vec<Tx> = h
            .sources
            .values()
            .filter_map(|s| s.tx.clone())
            .collect();
        if targets.is_empty() {
            return (StatusCode::NOT_FOUND, "download expired or not found")
                .into_response();
        }
        h.pending_files.insert(req.clone(), (None, None, targets.len()));
        let pull = json!({"type": "file_pull", "token": token, "reqId": req})
            .to_string();
        for tx in targets {
            let _ = tx.send(Out::T(pull.clone()));
        }
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut h = hub.lock().unwrap();
        let done = match h.pending_files.get(&req) {
            Some((Some(_), Some(_), _)) => true,
            Some((None, _, 0)) => {
                h.pending_files.remove(&req);
                return (StatusCode::NOT_FOUND, "download expired or not found")
                    .into_response();
            }
            Some(_) => false,
            None => false,
        };
        if done {
            let (meta, data, _) = h.pending_files.remove(&req).unwrap();
            let filename = meta
                .and_then(|m| m.get("filename").and_then(Value::as_str)
                    .map(String::from))
                .unwrap_or_else(|| "download".into());
            return (
                [(header::CONTENT_TYPE, "application/octet-stream".to_string()),
                 (header::CONTENT_DISPOSITION,
                  format!("attachment; filename=\"{filename}\""))],
                data.unwrap(),
            )
                .into_response();
        }
        drop(h);
        if std::time::Instant::now() > deadline {
            hub.lock().unwrap().pending_files.remove(&req);
            return (StatusCode::NOT_FOUND, "download expired or not found")
                .into_response();
        }
    }
}

/// Uploads through the hub: push the browser's bytes to whichever source
/// owns the endpoint token (file_push meta + FILE envelope, broadcast; the
/// owner acks, others decline).
async fn upload_handler(
    State(hub): State<Arc<Mutex<Hub>>>,
    headers: HeaderMap,
    axum::extract::Path(token): axum::extract::Path<String>,
    Query(q): Query<HashMap<String, String>>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req = format!("{}{}", now_hex(), CONN_SEQ.fetch_add(1, Ordering::Relaxed));
    let filename = q
        .get("name")
        .map(|n| n.rsplit(['/', '\\']).next().unwrap_or(n).to_string())
        .filter(|n| !n.is_empty())
        .unwrap_or_else(|| "upload.bin".into());
    {
        let mut h = hub.lock().unwrap();
        if !h.authed(&headers) {
            return (StatusCode::UNAUTHORIZED, "login required").into_response();
        }
        let targets: Vec<Tx> = h.sources.values().filter_map(|s| s.tx.clone()).collect();
        if targets.is_empty() {
            return (StatusCode::NOT_FOUND, "unknown upload target").into_response();
        }
        h.pending_uploads.insert(req.clone(), (None, targets.len()));
        let ctype = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream");
        let meta = json!({"type": "file_push", "token": token, "reqId": req,
                          "name": filename, "content_type": ctype})
            .to_string();
        let rid = req.as_bytes();
        let mut frame = Vec::with_capacity(2 + rid.len() + body.len());
        frame.push(BIN_FILE);
        frame.push(rid.len() as u8);
        frame.extend_from_slice(rid);
        frame.extend_from_slice(&body);
        for tx in targets {
            let _ = tx.send(Out::T(meta.clone()));
            let _ = tx.send(Out::B(frame.clone()));
        }
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut h = hub.lock().unwrap();
        match h.pending_uploads.get(&req) {
            Some((Some(_), _)) => {
                let (ack, _) = h.pending_uploads.remove(&req).unwrap();
                let ack = ack.unwrap();
                let out = json!({"ok": true,
                                 "name": ack.get("name").cloned()
                                     .unwrap_or(Value::String(filename)),
                                 "size": ack.get("size").cloned()
                                     .unwrap_or(Value::Null)});
                return ([(header::CONTENT_TYPE, "application/json")],
                        out.to_string()).into_response();
            }
            Some((None, 0)) => {
                h.pending_uploads.remove(&req);
                return (StatusCode::NOT_FOUND, "unknown upload target")
                    .into_response();
            }
            _ => {}
        }
        drop(h);
        if std::time::Instant::now() > deadline {
            hub.lock().unwrap().pending_uploads.remove(&req);
            return (StatusCode::NOT_FOUND, "unknown upload target").into_response();
        }
    }
}

/// Headless inventory of the composed canvas (the replay cache), one entry
/// per merged panel with the cross-process identity and source liveness.
async fn health_handler(State(hub): State<Arc<Mutex<Hub>>>) -> impl IntoResponse {
    let h = hub.lock().unwrap();
    axum::Json(json!({
        "danvasd": env!("CARGO_PKG_VERSION"),
        "run_id": h.run_id,
    }))
}

async fn describe_handler(
    State(hub): State<Arc<Mutex<Hub>>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let h = hub.lock().unwrap();
    if !h.authed(&headers) {
        return (StatusCode::UNAUTHORIZED, "login required").into_response();
    }
    let mut components = Vec::new();
    for (label, src) in &h.sources {
        for id in &src.reg_order {
            if let Some(reg) = src.registers.get(id) {
                components.push(json!({
                    "id": id,
                    "name": reg.get("name").cloned().unwrap_or(Value::Null),
                    "owner": reg.get("owner").cloned()
                        .unwrap_or(Value::String(label.clone())),
                    "component": reg.get("component").cloned().unwrap_or(Value::Null),
                    "x": reg.get("x").cloned().unwrap_or(Value::Null),
                    "y": reg.get("y").cloned().unwrap_or(Value::Null),
                    "source": label,
                    "status": if src.live { "live" } else { "offline" },
                }));
            }
        }
    }
    (
        [(header::CONTENT_TYPE, "application/json")],
        json!({"components": components}).to_string(),
    )
        .into_response()
}

async fn static_handler(
    State(hub): State<Arc<Mutex<Hub>>>,
    headers: HeaderMap,
    uri: Uri,
) -> impl IntoResponse {
    if !hub.lock().unwrap().authed(&headers) {
        return (StatusCode::UNAUTHORIZED,
                [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
                LOGIN_PAGE.as_bytes()).into_response();
    }
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };
    // Unknown paths fall back to the SPA index, matching the Python server.
    let file = DIST.get_file(path).or_else(|| DIST.get_file("index.html"));
    match file {
        Some(f) => {
            let mime = match path.rsplit_once('.').map(|(_, e)| e) {
                Some("html") | None => "text/html; charset=utf-8",
                Some("js") => "text/javascript",
                Some("css") => "text/css",
                Some("svg") => "image/svg+xml",
                Some("png") => "image/png",
                Some("json") => "application/json",
                Some("woff2") => "font/woff2",
                Some("ico") => "image/x-icon",
                _ => "application/octet-stream",
            };
            ([(header::CONTENT_TYPE, mime)], f.contents()).into_response()
        }
        None => (StatusCode::NOT_FOUND, "frontend not built").into_response(),
    }
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(q): Query<HashMap<String, String>>,
    headers: HeaderMap,
    State(hub): State<Arc<Mutex<Hub>>>,
) -> impl IntoResponse {
    let role = match hub.lock().unwrap().session_role(&headers) {
        Ok(r) => r,
        Err(()) => {
            return (StatusCode::UNAUTHORIZED, "login required").into_response()
        }
    };
    // Default tungstenite limits are 64MiB/message but 16MiB/FRAME — and the
    // Python SDK sends each media push as one unfragmented frame, so a big
    // model (a 32MB GLB) would kill the connection on every push and the
    // source would flap: reconnect, replay, re-push, die again. Media scale
    // is the point of the binary envelope; take frames as big as messages.
    const WS_MAX: usize = 256 * 1024 * 1024;
    ws.max_message_size(WS_MAX)
        .max_frame_size(WS_MAX)
        .on_upgrade(move |socket| handle(socket, q, role, hub))
        .into_response()
}

async fn handle(
    socket: WebSocket,
    q: HashMap<String, String>,
    role: Option<String>,
    hub: Arc<Mutex<Hub>>,
) {
    let conn_id = CONN_SEQ.fetch_add(1, Ordering::Relaxed);
    let is_source = q.get("source").map(|v| !v.is_empty()).unwrap_or(false);
    let label = q
        .get("label")
        .cloned()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("source{conn_id}"));
    let display_name = q
        .get("vname")
        .cloned()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            if is_source { label.clone() } else { format!("viewer{conn_id}") }
        });
    let color = VIEWER_COLORS[(conn_id as usize) % VIEWER_COLORS.len()];

    let (mut sink, mut stream) = socket.split();
    let tx = Conn::new();
    let writer = {
        let conn = tx.clone();
        tokio::spawn(async move {
            loop {
                let batch = conn.drain();
                if batch.is_empty() {
                    conn.wake.notified().await;
                    continue;
                }
                for out in batch {
                    let msg = match out {
                        Out::T(t) => Message::Text(t),
                        Out::B(b) => Message::Binary(b),
                        Out::Close => {
                            // Flush what's queued, then close the handshake so
                            // the peer's close() returns immediately.
                            let _ = sink.send(Message::Close(None)).await;
                            let _ = sink.flush().await;
                            return;
                        }
                    };
                    if sink.send(msg).await.is_err() {
                        return;
                    }
                }
            }
        })
    };

    // Welcome first, always — the client's version check reads it.
    let welcome = {
        let h = hub.lock().unwrap();
        json!({
            "type": "welcome",
            "protocol": PROTOCOL_VERSION,
            // Additive capabilities of this hub + its embedded frontend
            // (PROTOCOL.md § relative placement): "rel" = register frames may
            // carry relative placement, resolved and cascaded browser-side —
            // an SDK seeing it switches off its own placement fallbacks.
            "features": ["rel"],
            "you": {"id": format!("v{conn_id}"), "name": display_name,
                     "color": color, "device": "desktop",
                     "role": role.clone()},
            "runId": h.run_id,
            "view": if h.hub_view.is_empty() { Value::Null }
                    else { Value::Object(h.hub_view.clone()) },
            "mergeHost": true,
            "mergeServer": h.merge_server,
            "selfUrl": h.self_url,
            "uiInspector": h.ui_inspector, "uiGraveyard": h.ui_graveyard,
            "uiHosting": h.ui_hosting,
            "cursors": h.cursors,
            "auth": h.protected(),
            "reload": false,
            "hosting": h.hosting_state(),
        })
    };
    let _ = tx.send(Out::T(welcome.to_string()));
    // Everyone is a viewer (sources included): roster in, presence out to all,
    // and the chat so far replays to the newcomer.
    {
        let mut h = hub.lock().unwrap();
        h.conns.insert(conn_id, tx.clone());
        h.viewers.insert(conn_id, json!({
            "id": format!("v{conn_id}"), "name": display_name,
            // Programs on the canvas are roster-visible (that is the
            // feature) but marked so the UI can count HUMANS separately —
            // a solo user must not read "2 viewers".
            "color": color,
            "device": if is_source { "process" } else { "desktop" },
            "role": role.clone(),
        }));
        let p = h.presence_frame().to_string();
        h.fanout_all(&p);
        for entry in &h.chat_history {
            let _ = tx.send(Out::T(entry.to_string()));
        }
    }

    // A source contributes; a browser (or an observing source) receives the
    // composed canvas. A SOURCE also gets the replay — so it can find/edit/
    // observe peers that already exist — but is NOT added to the live browser
    // fan-out (no self-echo, no traffic it doesn't render); subscription copies
    // still reach a source via `conns`. Only OTHER sources are replayed to a
    // source (skipping itself), so an accumulating hub stays O(sources), not
    // O(sources²) per source connect.
    if is_source {
        attach_source(&hub, &label, conn_id, tx.clone());
    }
    {
        let frames: Vec<String> = {
            let mut h = hub.lock().unwrap();
            if !is_source {
                h.browsers.insert(conn_id, tx.clone());
            }
            h.conns.insert(conn_id, tx.clone());
            let mut out: Vec<String> = Vec::new();
            for (slabel, s) in h.sources.iter() {
                // A source replays only OTHER sources' panels (skip itself) —
                // it dials in to find/edit peers, not to see its own panels.
                if is_source && slabel == &label {
                    continue;
                }
                for f in Hub::cached_frames(s) {
                    // A source discovers peers' PANELS (registers/updates/
                    // shapes) so it can find/edit/observe them; it has no use
                    // for peers' freehand ink, and receiving a replayed draw
                    // would land in the source's draw stream ahead of the
                    // edits it's waiting on.
                    if is_source && f.get("type").and_then(Value::as_str) == Some("draw") {
                        continue;
                    }
                    if let Some(cid) = f.get("id").and_then(Value::as_str) {
                        if !role_may_see(&role, &panel_roles(s, cid, Some(&f))) {
                            continue; // role-hidden panel: not replayed
                        }
                    }
                    out.push(f.to_string());
                }
            }
            // Freehand ink is a browser-render concern: a source contributing
            // panels doesn't need (and shouldn't receive) the composed ink
            // frame — replaying it would land in a source's draw stream ahead
            // of the edits it's actually waiting on.
            if !is_source && !h.drawings.is_empty() {
                out.push(json!({"type": "draw", "diff": {
                    "added": h.drawings.clone().into_iter()
                        .collect::<Map<String, Value>>(),
                    "updated": {}, "removed": {}}}).to_string());
            }
            if !h.sources.is_empty() {
                out.push(h.roster_frame().to_string());
            }
            out
        };
        for f in frames {
            let _ = tx.send(Out::T(f));
        }
    }

    // Heartbeat reaping: clients send a heartbeat every ~10s; a connection
    // silent past the deadline is presumed dead (hard-dropped tab, crashed
    // process with no clean close) and reaped — the disconnect path then
    // applies retention. DANVAS_HEARTBEAT_TIMEOUT overrides for tests.
    let idle = std::time::Duration::from_secs_f64(
        std::env::var("DANVAS_HEARTBEAT_TIMEOUT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30.0),
    );
    while let Ok(Some(Ok(msg))) = tokio::time::timeout(idle, stream.next()).await {
        let text = match msg {
            Message::Text(t) => t,
            Message::Binary(b) => {
                binary_frame(&hub, is_source, &label, b);
                continue;
            }
            Message::Close(_) => break,
            _ => continue,
        };
        let Ok(frame) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        if is_source {
            source_frame(&hub, &label, conn_id, frame);
        } else {
            client_frame(&hub, conn_id, frame);
        }
    }

    // -- disconnect ----------------------------------------------------------
    {
        let mut h = hub.lock().unwrap();
        h.browsers.remove(&conn_id);
        h.conns.remove(&conn_id);
        h.viewers.remove(&conn_id);
        for subs in h.subs.values_mut() {
            subs.remove(&conn_id);
        }
        let p = h.presence_frame().to_string();
        h.fanout_all(&p);
        if is_source {
            source_down(&mut h, &label, &tx);
        }
    }
    // Complete the close handshake: ask the writer to flush its queue and send
    // a Close, so the peer's close() returns at once instead of waiting out its
    // close_timeout on a socket we merely dropped. Bounded — a wedged writer
    // (e.g. a peer that stopped reading entirely) is aborted after the grace.
    tx.send(Out::Close);
    let mut writer = writer;
    tokio::select! {
        _ = &mut writer => {}
        _ = tokio::time::sleep(std::time::Duration::from_secs(3)) => writer.abort(),
    }
}

/// A source's connection dropped (server-side dial-in close OR a dialed-out
/// link failing): retention keeps the caches and freezes the panels — unless
/// a newer life already re-took the label (the same_channel check).
fn source_down(h: &mut Hub, label: &str, tx: &Tx) {
    let mut went_offline = false;
    let frames: Vec<String> = if let Some(src) = h.sources.get_mut(label) {
        if src.tx.as_ref().map(|t| Arc::ptr_eq(t, tx)).unwrap_or(false) {
            src.live = false;
            src.tx = None;
            went_offline = true;
            Hub::freeze_frames(src).iter().map(|f| f.to_string()).collect()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    for f in &frames {
        h.fanout_browsers(f);
    }
    if went_offline {
        let roster = h.roster_frame().to_string();
        h.fanout_browsers(&roster);
    }
}

const BIN_INPUT: u8 = 5;

/// Rewrite the id inside a binary envelope ([type][idLen][id][payload]).
fn bin_reframe(data: &[u8], new_id: &str) -> Vec<u8> {
    let idlen = data[1] as usize;
    let nid = new_id.as_bytes();
    let mut out = Vec::with_capacity(2 + nid.len() + data.len() - 2 - idlen);
    out.push(data[0]);
    out.push(nid.len() as u8);
    out.extend_from_slice(nid);
    out.extend_from_slice(&data[2 + idlen..]);
    out
}

fn bin_id(data: &[u8]) -> Option<String> {
    if data.len() < 2 || data.len() < 2 + data[1] as usize {
        return None;
    }
    String::from_utf8(data[2..2 + data[1] as usize].to_vec()).ok()
}

/// One inbound binary envelope. From a source: MEDIA relays to browsers with
/// the id namespaced in-envelope (not cached — streams aren't replayed). From
/// a viewer: a binary INPUT on a merged panel routes to its owner, stripped.
const BIN_FILE: u8 = 6;

fn binary_frame(hub: &Arc<Mutex<Hub>>, from_source: bool, label: &str, data: Vec<u8>) {
    let Some(cid) = bin_id(&data) else { return };
    if from_source && data[0] == BIN_FILE {
        let payload = data[2 + data[1] as usize..].to_vec();
        let mut h = hub.lock().unwrap();
        if let Some(entry) = h.pending_files.get_mut(&cid) {
            entry.1 = Some(payload);
        }
        return;
    }
    let h = hub.lock().unwrap();
    if from_source && data[0] != BIN_INPUT {
        let Some(src) = h.sources.get(label) else { return };
        let out = bin_reframe(&data, &format!("{}:{}", src.tag, cid));
        for tx in h.browsers.values() {
            let _ = tx.send(Out::B(out.clone()));
        }
        return;
    }
    if data[0] == BIN_INPUT {
        let Some((tag, rest)) = cid.split_once(':') else { return };
        let Some(owner) = h.tag_to_label.get(tag) else { return };
        if let Some(src) = h.sources.get(owner) {
            if let Some(tx) = &src.tx {
                let _ = tx.send(Out::B(bin_reframe(&data, rest)));
            }
        }
    }
}

/// Host:port out of a ws uri like `ws://host:port/ws`.
fn host_port_of(ws_uri: &str) -> Option<(String, u16)> {
    let rest = ws_uri.split_once("://")?.1;
    let hostport = rest.split('/').next()?;
    let (host, port) = hostport.rsplit_once(':')?;
    Some((host.to_string(), port.parse().ok()?))
}

/// Minimal HTTP/1.1 exchange over a fresh TCP connection; returns
/// (status, full response text). Enough for the probe and the login — the
/// broker never needs an HTTP client library for localhost/LAN sources.
/// (TLS sources would need one; documented gap.)
async fn http_exchange(host: &str, port: u16, request: String) -> Option<(u16, String)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect((host, port)).await.ok()?;
    stream.write_all(request.as_bytes()).await.ok()?;
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(6),
        stream.read_to_end(&mut buf),
    )
    .await;
    let text = String::from_utf8_lossy(&buf).to_string();
    let status: u16 = text.split_whitespace().nth(1)?.parse().ok()?;
    Some((status, text))
}

async fn http_probe(host: &str, port: u16) -> Option<u16> {
    let req = format!("GET / HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    http_exchange(host, port, req).await.map(|(s, _)| s)
}

/// The /__auth__ password flow; returns the pc_session token on success.
async fn http_login(host: &str, port: u16, password: &str) -> Option<String> {
    let mut body = String::from("password=");
    for b in password.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                body.push(b as char)
            }
            _ => body.push_str(&format!("%{b:02X}")),
        }
    }
    let req = format!(
        "POST /__auth__ HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let (_status, text) = http_exchange(host, port, req).await?;
    for line in text.lines() {
        if line.to_ascii_lowercase().starts_with("set-cookie:") {
            if let Some(rest) = line.split_once("pc_session=") {
                let token: String = rest
                    .1
                    .chars()
                    .take_while(|c| *c != ';' && !c.is_whitespace())
                    .collect();
                if !token.is_empty() {
                    return Some(token);
                }
            }
        }
    }
    None
}

/// Dial OUT to a served canvas (merge_add): connect as a ?proxy=1 client,
/// ingest its stream through the same path a dial-in source's frames take,
/// pump route-backs out, retry forever (retention covers the gaps). Stopped
/// for good by merge_remove aborting the task.
async fn dial_out(
    hub: Arc<Mutex<Hub>>,
    ws_uri: String,
    label: String,
    cookie: Option<String>,
) {
    use tokio_tungstenite::connect_async;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Message as TMsg;
    let sep = if ws_uri.contains('?') { '&' } else { '?' };
    let uri = format!("{ws_uri}{sep}proxy=1");
    loop {
        let mut request = match uri.clone().into_client_request() {
            Ok(r) => r,
            Err(_) => return,
        };
        if let Some(token) = &cookie {
            if let Ok(v) = format!("pc_session={token}").parse() {
                request.headers_mut().insert("Cookie", v);
            }
        }
        if let Ok((stream, _)) = connect_async(request).await {
            let (mut sink, mut read) = stream.split();
            let tx = Conn::new();
            let writer = {
                let conn = tx.clone();
                tokio::spawn(async move {
                    loop {
                        let batch = conn.drain();
                        if batch.is_empty() {
                            conn.wake.notified().await;
                            continue;
                        }
                        for out in batch {
                            let msg = match out {
                                Out::T(t) => TMsg::Text(t),
                                Out::B(b) => TMsg::Binary(b),
                                Out::Close => {
                                    let _ = sink.send(TMsg::Close(None)).await;
                                    let _ = sink.flush().await;
                                    return;
                                }
                            };
                            if sink.send(msg).await.is_err() {
                                return;
                            }
                        }
                    }
                })
            };
            let conn_id = CONN_SEQ.fetch_add(1, Ordering::Relaxed);
            attach_source(&hub, &label, conn_id, tx.clone());
            while let Some(Ok(msg)) = read.next().await {
                match msg {
                    TMsg::Text(text) => {
                        if let Ok(frame) = serde_json::from_str::<Value>(&text) {
                            source_frame(&hub, &label, conn_id, frame);
                        }
                    }
                    TMsg::Binary(b) => binary_frame(&hub, true, &label, b),
                    _ => {}
                }
            }
            writer.abort();
            {
                let mut h = hub.lock().unwrap();
                h.conns.remove(&conn_id);
                source_down(&mut h, &label, &tx);
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

fn attach_source(hub: &Arc<Mutex<Hub>>, label: &str, conn_id: u64, tx: Tx) {
    let mut h = hub.lock().unwrap();
    h.conns.insert(conn_id, tx.clone());
    if !h.sources.contains_key(label) {
        let tag = format!("s{}", h.tag_seq);
        h.tag_seq += 1;
        h.tag_to_label.insert(tag.clone(), label.to_string());
        h.sources.insert(
            label.to_string(),
            Source { tag, ..Default::default() },
        );
    }
    // Same label re-dialing = the source's next life: stale frames out first
    // (ids are minted per run on the source side).
    let teardown: Vec<String> = {
        let src = h.sources.get_mut(label).unwrap();
        let frames = Hub::teardown_frames(src).iter().map(|f| f.to_string()).collect();
        src.reg_order.clear();
        src.registers.clear();
        src.updates.clear();
        src.arrows.clear();
        src.shapes.clear();
        src.shared = None;
        src.graveyard = None;
        src.drawings.clear();
        src.live = true;
        src.tx = Some(tx);
        frames
    };
    for f in &teardown {
        h.fanout_browsers(f);
    }
    let roster = h.roster_frame().to_string();
    h.fanout_browsers(&roster);
}

/// A frame FROM a dial-in source: its canvas content, namespaced + cached +
/// fanned out. Anything else (heartbeat, petitions on others' panels) falls
/// through to the shared client path.
fn source_frame(hub: &Arc<Mutex<Hub>>, label: &str, conn_id: u64, mut frame: Value) {
    let kind = frame.get("type").and_then(Value::as_str).unwrap_or("").to_string();
    if kind == "draw" {
        // The source's own ink: namespace every record, fold into the replay
        // cache (updated pairs keep the "after" state), fan out.
        let mut h = hub.lock().unwrap();
        let Some(src) = h.sources.get(label) else { return };
        let tag = src.tag.clone();
        let ns_diff = remap_draw_diff(
            frame.get("diff").unwrap_or(&Value::Null),
            &|r: &str| format!("{tag}:{r}"),
        );
        let src = h.sources.get_mut(label).unwrap();
        if let Some(Value::Object(a)) = ns_diff.get("added") {
            for (k, v) in a {
                src.drawings.insert(k.clone(), v.clone());
            }
        }
        if let Some(Value::Object(u)) = ns_diff.get("updated") {
            for (k, v) in u {
                let after = match v {
                    Value::Array(p) if p.len() == 2 => p[1].clone(),
                    other => other.clone(),
                };
                src.drawings.insert(k.clone(), after);
            }
        }
        if let Some(Value::Object(r)) = ns_diff.get("removed") {
            for k in r.keys() {
                src.drawings.remove(k);
            }
        }
        let text = json!({"type": "draw", "diff": ns_diff}).to_string();
        h.fanout_browsers_except(&text, conn_id);
        return;
    }
    if kind == "file_meta" {
        let Some(req_id) = frame.get("reqId").and_then(Value::as_str) else { return };
        let mut h = hub.lock().unwrap();
        if let Some(entry) = h.pending_files.get_mut(req_id) {
            if frame.get("ok").and_then(Value::as_bool).unwrap_or(false) {
                entry.0 = Some(frame.clone());
            } else if entry.2 > 0 {
                entry.2 -= 1;
            }
        }
        return;
    }
    if kind == "file_ack" {
        let Some(req_id) = frame.get("reqId").and_then(Value::as_str) else { return };
        let mut h = hub.lock().unwrap();
        if let Some(entry) = h.pending_uploads.get_mut(req_id) {
            if frame.get("ok").and_then(Value::as_bool).unwrap_or(false) {
                entry.0 = Some(frame.clone());
            } else if entry.1 > 0 {
                entry.1 -= 1;
            }
        }
        return;
    }
    if kind == "response" {
        // The owner answered a viewer's request: route to the asker only.
        let Some(req_id) = frame.get("reqId").and_then(Value::as_str) else { return };
        let mut h = hub.lock().unwrap();
        if let Some((asker, expiry)) = h.pending_req.remove(req_id) {
            if expiry > std::time::Instant::now() {
                if let Some(tx) = h.conns.get(&asker) {
                    let _ = tx.send(Out::T(frame.to_string()));
                }
            }
        }
        return;
    }
    if kind == "serve_config" {
        // The owner's resolved serve() gating, delivered on dial-in. When the
        // broker was spawned by something that doesn't know the serve()
        // kwargs (the hot-reload monitor starts it before the script runs),
        // the host source sends the flags it resolved; folded into the hub so
        // every subsequent browser welcome advertises the right affordances.
        let mut h = hub.lock().unwrap();
        if let Some(v) = frame.get("uiInspector").and_then(Value::as_bool) {
            h.ui_inspector = v;
        }
        if let Some(v) = frame.get("uiGraveyard").and_then(Value::as_bool) {
            h.ui_graveyard = v;
        }
        if let Some(v) = frame.get("cursors").and_then(Value::as_bool) {
            h.cursors = v;
        }
        if let Some(v) = frame.get("uiHosting").and_then(Value::as_bool) {
            h.ui_hosting = v;
        }
        // Browsers already connected got a welcome without these flags (a
        // hot-reload browser outlives the worker) — push the change live.
        h.fanout_browsers(&frame.to_string());
        return;
    }
    if kind == "view" {
        // The source (e.g. the transplanted host) sets camera/chrome: fold
        // for late joiners' welcome, relay live.
        let mut h = hub.lock().unwrap();
        if let Some(Value::Object(delta)) = frame.get("view") {
            for (k, v) in delta {
                h.hub_view.insert(k.clone(), v.clone());
            }
        }
        let text = frame.to_string();
        h.fanout_browsers_except(&text, conn_id);
        return;
    }
    if kind == "shared" {
        let mut h = hub.lock().unwrap();
        if let Some(src) = h.sources.get_mut(label) {
            src.shared = Some(frame.clone());
        }
        let text = frame.to_string();
        h.fanout_browsers_except(&text, conn_id);
        return;
    }
    if kind == "get_snapshot" || kind == "get_image" {
        // The host asks a browser for the free-form document / a rendered PNG
        // (canvas.save()/screenshot()); the browser is the only thing that can
        // answer. Relay to the browsers; the reply (snapshot/image, correlated
        // by reqId) routes back to the sources. A targeted screenshot's
        // shapeIds carry the SOURCE's panel ids ("shape:<id>") — namespace
        // the panel id with the source tag like every other owner->browser
        // id, or the browser's store (which holds "shape:<tag>:<id>")
        // matches nothing ("nothing to capture"). An empty list (whole
        // page) passes through untouched.
        let h = hub.lock().unwrap();
        let tag = h
            .sources
            .get(label)
            .map(|s| s.tag.clone())
            .unwrap_or_default();
        let mut f = frame.clone();
        if let Some(obj) = f.as_object_mut() {
            if let Some(Value::Array(ids)) = obj.get("shapeIds") {
                let nids: Vec<Value> = ids
                    .iter()
                    .map(|v| match v {
                        Value::String(id) => {
                            let bare = id.strip_prefix("shape:").unwrap_or(id);
                            Value::String(format!("shape:{tag}:{bare}"))
                        }
                        other => other.clone(),
                    })
                    .collect();
                obj.insert("shapeIds".into(), Value::Array(nids));
            }
        }
        h.fanout_browsers(&f.to_string());
        return;
    }
    if kind == "graveyard_update" {
        let mut h = hub.lock().unwrap();
        let Some(src) = h.sources.get(label) else { return };
        let tag = src.tag.clone();
        let items: Vec<Value> = frame
            .get("items")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .map(|item| {
                        let mut it = item.clone();
                        if let Some(obj) = it.as_object_mut() {
                            if let Some(Value::String(id)) = obj.get("id") {
                                let nid = format!("{tag}:{id}");
                                obj.insert("id".into(), Value::String(nid));
                            }
                        }
                        it
                    })
                    .collect()
            })
            .unwrap_or_default();
        let msg = json!({"type": "graveyard_update", "items": items});
        if let Some(src) = h.sources.get_mut(label) {
            src.graveyard = Some(msg.clone());
        }
        let text = msg.to_string();
        h.fanout_browsers_except(&text, conn_id);
        return;
    }
    if !matches!(kind.as_str(),
                 "register" | "update" | "remove" | "arrow" | "shape"
                 | "shape_update" | "order") {
        client_frame(hub, conn_id, frame);
        return;
    }
    let mut h = hub.lock().unwrap();
    let Some(src) = h.sources.get(label) else { return };
    let tag = src.tag.clone();
    let ns = |id: &str| format!("{tag}:{id}");
    let raw_id = frame.get("id").and_then(Value::as_str).unwrap_or("").to_string();
    let nsid = ns(&raw_id);
    let obj = frame.as_object_mut().unwrap();
    obj.insert("id".into(), Value::String(nsid.clone()));
    match kind.as_str() {
        "register" => {
            // Re-stamp ownership: on the composed canvas the owner is the
            // source, by its label — whatever it says about itself.
            obj.insert("owner".into(), Value::String(label.to_string()));
            let src = h.sources.get_mut(label).unwrap();
            let (ox, oy) = src.offset;
            shift_xy(frame.as_object_mut().unwrap(), ox, oy);
            let src = h.sources.get_mut(label).unwrap();
            if !src.registers.contains_key(&nsid) {
                src.reg_order.push(nsid.clone());
            }
            src.registers.insert(nsid.clone(), frame.clone());
        }
        "update" => {
            let (ox, oy) = h.sources.get(label).map(|s| s.offset).unwrap_or((0.0, 0.0));
            let mut payload = frame
                .get("payload")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            shift_xy(&mut payload, ox, oy);
            frame
                .as_object_mut()
                .unwrap()
                .insert("payload".into(), Value::Object(payload.clone()));
            let src = h.sources.get_mut(label).unwrap();
            fold_state(src, &nsid, payload);
        }
        "remove" => {
            let src = h.sources.get_mut(label).unwrap();
            src.reg_order.retain(|i| i != &nsid);
            src.registers.remove(&nsid);
            src.updates.remove(&nsid);
            src.arrows.remove(&nsid);
            src.shapes.remove(&nsid);
        }
        "shape" => {
            let src = h.sources.get_mut(label).unwrap();
            let (ox, oy) = src.offset;
            shift_xy(frame.as_object_mut().unwrap(), ox, oy);
            let src = h.sources.get_mut(label).unwrap();
            src.shapes.insert(nsid.clone(), frame.clone());
        }
        "shape_update" => {
            let (ox, oy) = h.sources.get(label).map(|s| s.offset).unwrap_or((0.0, 0.0));
            shift_xy(frame.as_object_mut().unwrap(), ox, oy);
            // Fold into the cached shape so a late browser gets the CURRENT
            // shape, not the original plus patches (same rule as panels).
            let src = h.sources.get_mut(label).unwrap();
            if let Some(shape) = src.shapes.get_mut(&nsid) {
                let patch = frame.as_object().unwrap().clone();
                let sobj = shape.as_object_mut().unwrap();
                for (k, v) in patch {
                    if k == "props" {
                        if let (Some(Value::Object(sp)), Value::Object(pp)) =
                            (sobj.get_mut("props"), v)
                        {
                            sp.extend(pp);
                        }
                    } else if k != "type" && k != "id" {
                        sobj.insert(k, v);
                    }
                }
            }
        }
        "arrow" => {
            // Endpoints: the sender's own panels get its namespace; a
            // reference to a panel it can SEE but doesn't own (an already-
            // composed id) passes through — cross-source arrows.
            for key in ["start", "end"] {
                if let Some(Value::String(r)) = frame.get(key) {
                    let composed = compose_endpoint(&h, &tag, r);
                    frame.as_object_mut().unwrap().insert(key.into(), Value::String(composed));
                }
            }
            let src = h.sources.get_mut(label).unwrap();
            src.arrows.insert(nsid.clone(), frame.clone());
        }
        _ => {}
    }
    let text = frame.to_string();
    let roles = h
        .sources
        .get(label)
        .map(|s| panel_roles(s, &nsid, Some(&frame)))
        .unwrap_or_default();
    if roles.is_empty() {
        h.fanout_browsers_except(&text, conn_id);
    } else {
        // Role egress: frames tied to a role-restricted panel reach only
        // viewers whose login role is on the allowlist.
        let ids: Vec<u64> = h.browsers.keys().cloned().collect();
        for bid in ids {
            if bid == conn_id { continue; }   // don't echo to the origin source
            let vrole = viewer_role(&h, bid);
            if role_may_see(&vrole, &roles) {
                if let Some(tx) = h.browsers.get(&bid) {
                    let _ = tx.send(Out::T(text.clone()));
                }
            }
        }
    }
}

fn compose_endpoint(h: &Hub, own_tag: &str, r: &str) -> String {
    if let Some((tag, rest)) = r.split_once(':') {
        if !rest.is_empty() && h.tag_to_label.contains_key(tag) {
            return r.to_string(); // another source's composed id: untouched
        }
    }
    format!("{own_tag}:{r}")
}

/// A frame from a browser (or a source acting as a peer): petitions on
/// composed panels route to the owner; subscriptions live at the hub.
/// Best-effort LAN IPv4 (the address other devices dial): open a UDP socket
/// toward a public address and read which local interface routes out.
fn lan_ip() -> Option<String> {
    let sock = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    sock.connect("8.8.8.8:80").ok()?;
    Some(sock.local_addr().ok()?.ip().to_string())
}

/// Find cloudflared: $CLOUDFLARED, then PATH.
fn find_cloudflared() -> Option<String> {
    if let Ok(p) = std::env::var("CLOUDFLARED") {
        if std::path::Path::new(&p).exists() {
            return Some(p);
        }
    }
    let exe = if cfg!(windows) { "cloudflared.exe" } else { "cloudflared" };
    let which = if cfg!(windows) { "where" } else { "which" };
    let out = std::process::Command::new(which).arg(exe).output().ok()?;
    if out.status.success() {
        let path = String::from_utf8_lossy(&out.stdout);
        let first = path.lines().next()?.trim().to_string();
        if !first.is_empty() {
            return Some(first);
        }
    }
    None
}

/// One live hosting change (LAN listener up/down, tunnel up/down), then
/// broadcast the new state. Errors land in the state, never panic the hub.
async fn hosting_action(hub: Arc<Mutex<Hub>>, action: String) {
    {
        let mut h = hub.lock().unwrap();
        h.hosting_busy = Some(action.clone());
        h.hosting_error = None;
        h.broadcast_hosting();
    }
    let mut err: Option<String> = None;
    match action.as_str() {
        "host_lan" => {
            let already = hub.lock().unwrap().lan_url.is_some();
            if !already {
                match lan_ip() {
                    None => err = Some("no LAN address found".into()),
                    Some(ip) => {
                        let port = hub.lock().unwrap().host_port;
                        let addr: SocketAddr = format!("{ip}:{port}").parse().unwrap();
                        match tokio::net::TcpListener::bind(addr).await {
                            Err(e) => err = Some(format!("LAN bind failed: {e}")),
                            Ok(listener) => {
                                let (tx, mut rx) = tokio::sync::watch::channel(false);
                                let app = build_app(hub.clone());
                                tokio::spawn(async move {
                                    let _ = axum::serve(listener, app)
                                        .with_graceful_shutdown(async move {
                                            let _ = rx.changed().await;
                                        })
                                        .await;
                                });
                                let mut h = hub.lock().unwrap();
                                h.lan_url = Some(format!("http://{ip}:{port}"));
                                h.lan_shutdown = Some(tx);
                            }
                        }
                    }
                }
            }
        }
        "host_lan_off" => {
            let mut h = hub.lock().unwrap();
            if let Some(tx) = h.lan_shutdown.take() {
                let _ = tx.send(true);
            }
            h.lan_url = None;
        }
        "host_tunnel" => {
            let already = hub.lock().unwrap().tunnel_url.is_some();
            if !already {
                match find_cloudflared() {
                    None => err = Some(
                        "cloudflared not found (set $CLOUDFLARED or install it)".into()),
                    Some(bin) => {
                        let port = hub.lock().unwrap().host_port;
                        match spawn_tunnel(&bin, port).await {
                            Err(e) => err = Some(e),
                            Ok((child, url)) => {
                                let mut h = hub.lock().unwrap();
                                h.tunnel_child = Some(child);
                                h.tunnel_url = Some(url);
                            }
                        }
                    }
                }
            }
        }
        "host_tunnel_off" => {
            let mut h = hub.lock().unwrap();
            if let Some(mut c) = h.tunnel_child.take() {
                let _ = c.kill();
            }
            h.tunnel_url = None;
        }
        _ => {}
    }
    let mut h = hub.lock().unwrap();
    h.hosting_busy = None;
    h.hosting_error = err;
    h.broadcast_hosting();
}

/// Spawn `cloudflared tunnel --url http://127.0.0.1:port` and read the
/// trycloudflare URL it prints on stderr.
async fn spawn_tunnel(bin: &str, port: u16) -> Result<(std::process::Child, String), String> {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    let mut child = Command::new(bin)
        .args(["tunnel", "--url", &format!("http://127.0.0.1:{port}"),
               "--no-autoupdate"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("cloudflared failed to start: {e}"))?;
    let stderr = child.stderr.take().ok_or("no cloudflared stderr")?;
    // cloudflared prints the URL within a few seconds; scan its stderr.
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let re_host = "trycloudflare.com";
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            if let Some(i) = line.find("https://") {
                let url: String = line[i..]
                    .split_whitespace().next().unwrap_or("").to_string();
                if url.contains(re_host) {
                    let _ = tx.send(url);
                    return;
                }
            }
        }
    });
    match rx.recv_timeout(std::time::Duration::from_secs(30)) {
        Ok(url) => Ok((child, url)),
        Err(_) => {
            let _ = child.kill();
            Err("cloudflared did not report a URL in time".into())
        }
    }
}

fn client_frame(hub: &Arc<Mutex<Hub>>, conn_id: u64, frame: Value) {
    let kind = frame.get("type").and_then(Value::as_str).unwrap_or("");
    if kind == "snapshot" || kind == "image" {
        // A browser's reply to a host get_snapshot/get_image: route to the
        // sources (the requesting source matches by reqId; others ignore it).
        let h = hub.lock().unwrap();
        let text = frame.to_string();
        for src in h.sources.values() {
            if let Some(tx) = &src.tx {
                let _ = tx.send(Out::T(text.clone()));
            }
        }
        return;
    }
    if kind == "ui" {
        // The 🌐 hosting button: widen this (private) broker's reach live —
        // handled by the broker itself.
        let action = frame.get("action").and_then(Value::as_str).unwrap_or("");
        if matches!(action, "host_lan" | "host_lan_off" | "host_tunnel" | "host_tunnel_off") {
            if hub.lock().unwrap().ui_hosting {
                tokio::spawn(hosting_action(hub.clone(), action.to_string()));
            }
            return;
        }
        // Other native-UI requests (e.g. toggle_inspector) belong to the canvas
        // owner: route to the sources so the host can act (spawn/close the
        // Inspector). A dial-in peer with no canvas ignores it.
        let h = hub.lock().unwrap();
        let text = frame.to_string();
        for src in h.sources.values() {
            if let Some(tx) = &src.tx {
                let _ = tx.send(Out::T(text.clone()));
            }
        }
        return;
    }
    if kind == "draw" {
        // A viewer's ink edit: records under a source's namespace route back
        // to that owner (stripped); bare records are hub-native annotation,
        // relayed to the other browsers.
        let Some(diff) = frame.get("diff") else { return };
        let mut h = hub.lock().unwrap();
        let mut per_dest: HashMap<Option<String>, Map<String, Value>> = HashMap::new();
        for bucket in ["added", "updated", "removed"] {
            if let Some(Value::Object(b)) = diff.get(bucket) {
                for (rid, val) in b {
                    let dest = rid
                        .split_once(':')
                        .filter(|(t, _)| h.tag_to_label.contains_key(*t))
                        .map(|(t, _)| t.to_string());
                    let entry = per_dest.entry(dest).or_insert_with(|| {
                        let mut m = Map::new();
                        for bk in ["added", "updated", "removed"] {
                            m.insert(bk.into(), json!({}));
                        }
                        m
                    });
                    entry
                        .get_mut(bucket)
                        .and_then(Value::as_object_mut)
                        .unwrap()
                        .insert(rid.clone(), val.clone());
                }
            }
        }
        for (dest, sub) in per_dest {
            match dest {
                Some(tag) => {
                    let stripped = remap_draw_diff(&Value::Object(sub), &|r: &str| {
                        r.split_once(':')
                            .map(|(_, rest)| rest.to_string())
                            .unwrap_or_else(|| r.to_string())
                    });
                    if let Some(label) = h.tag_to_label.get(&tag) {
                        if let Some(src) = h.sources.get(label) {
                            if let Some(tx) = &src.tx {
                                let _ = tx.send(Out::T(
                                    json!({"type": "draw", "diff": stripped})
                                        .to_string(),
                                ));
                            }
                        }
                    }
                }
                None => {
                    // Hub-native annotation: store for replay, relay to the
                    // other viewers.
                    if let Some(Value::Object(a)) = sub.get("added") {
                        for (k, v) in a {
                            h.drawings.insert(k.clone(), v.clone());
                        }
                    }
                    if let Some(Value::Object(u)) = sub.get("updated") {
                        for (k, v) in u {
                            let after = match v {
                                Value::Array(p) if p.len() == 2 => p[1].clone(),
                                other => other.clone(),
                            };
                            h.drawings.insert(k.clone(), after);
                        }
                    }
                    if let Some(Value::Object(r)) = sub.get("removed") {
                        for k in r.keys() {
                            h.drawings.remove(k);
                        }
                    }
                    let text =
                        json!({"type": "draw", "diff": Value::Object(sub)}).to_string();
                    for (bid, btx) in &h.browsers {
                        if *bid != conn_id {
                            let _ = btx.send(Out::T(text.clone()));
                        }
                    }
                    // Also deliver hub-native ink to the sources: a serving
                    // canvas observes free-form drawing via canvas.on_draw, and
                    // (unlike the per-render replay) a live diff belongs in a
                    // source's stream. Sources that don't observe ink ignore it.
                    for src in h.sources.values() {
                        if let Some(tx) = &src.tx {
                            let _ = tx.send(Out::T(text.clone()));
                        }
                    }
                }
            }
        }
        return;
    }
    if kind == "chat" {
        let Some(text) = frame.get("text").and_then(Value::as_str) else { return };
        if text.trim().is_empty() {
            return;
        }
        let mut h = hub.lock().unwrap();
        let (name, color) = h
            .viewers
            .get(&conn_id)
            .map(|v| {
                (v.get("name").and_then(Value::as_str).unwrap_or("?").to_string(),
                 v.get("color").and_then(Value::as_str).unwrap_or("#888").to_string())
            })
            .unwrap_or(("?".into(), "#888".into()));
        h.chat_seq += 1;
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        // Identity is server-stamped (the sender's roster entry), never the
        // client's claim — a name can't be spoofed in a chat line.
        let entry = json!({"type": "chat", "msgId": h.chat_seq,
                           "name": name, "color": color,
                           "text": text, "ts": ts});
        h.chat_history.push(entry.clone());
        if h.chat_history.len() > 100 {
            h.chat_history.remove(0);
        }
        let t = entry.to_string();
        h.fanout_all(&t);
        return;
    }
    if kind == "set_name" {
        let Some(name) = frame.get("name").and_then(Value::as_str) else { return };
        let name = name.trim();
        if name.is_empty() {
            return;
        }
        let mut h = hub.lock().unwrap();
        if let Some(v) = h.viewers.get_mut(&conn_id) {
            v.as_object_mut()
                .unwrap()
                .insert("name".into(), Value::String(name.to_string()));
        }
        let p = h.presence_frame().to_string();
        h.fanout_all(&p);
        return;
    }
    if kind == "merge_add" || kind == "merge_auth" {
        // Compose a SERVED canvas by URL, live. merge_add probes first: a
        // password-protected target (HTTP 401) asks the requesting browser
        // for its password (merge_auth_required); merge_auth runs the
        // target's /__auth__ flow and dials with the session cookie (a wrong
        // password reports merge_auth_failed). (Canvas-wide here; the Python
        // hub's per-connection scoping is deliberately unpinned.)
        let Some((ws_uri, label)) = frame
            .get("uri")
            .and_then(Value::as_str)
            .and_then(normalize_source_uri)
        else {
            return;
        };
        let password = frame
            .get("password")
            .and_then(Value::as_str)
            .map(String::from)
            .filter(|_| kind == "merge_auth");
        let (requester, already) = {
            let h = hub.lock().unwrap();
            (h.conns.get(&conn_id).cloned(), h.dial_tasks.contains_key(&label))
        };
        if already {
            return;
        }
        let hub2 = hub.clone();
        tokio::spawn(async move {
            let hp = host_port_of(&ws_uri);
            if let Some(pw) = password {
                let cookie = match &hp {
                    Some((host, port)) => http_login(host, *port, &pw).await,
                    None => None,
                };
                let Some(cookie) = cookie else {
                    if let Some(tx) = requester {
                        let _ = tx.send(Out::T(
                            json!({"type": "merge_auth_failed",
                                   "uri": ws_uri, "label": label})
                            .to_string(),
                        ));
                    }
                    return;
                };
                let task = tokio::spawn(dial_out(
                    hub2.clone(), ws_uri, label.clone(), Some(cookie)));
                hub2.lock().unwrap().dial_tasks.insert(label, task);
                return;
            }
            // merge_add: probe for protection first
            if let Some((host, port)) = &hp {
                if http_probe(host, *port).await == Some(401) {
                    if let Some(tx) = requester {
                        let _ = tx.send(Out::T(
                            json!({"type": "merge_auth_required",
                                   "uri": ws_uri, "label": label})
                            .to_string(),
                        ));
                    }
                    return;
                }
            }
            let task = tokio::spawn(dial_out(
                hub2.clone(), ws_uri, label.clone(), None));
            hub2.lock().unwrap().dial_tasks.insert(label, task);
        });
        return;
    }
    if kind == "merge_remove" {
        let sid = frame.get("sid").and_then(Value::as_str).unwrap_or("").to_string();
        let mut h = hub.lock().unwrap();
        let Some(label) = h.tag_to_label.get(&sid).cloned() else { return };
        if let Some(task) = h.dial_tasks.remove(&label) {
            task.abort(); // no more reconnects
        }
        let frames: Vec<String> = h
            .sources
            .get(&label)
            .map(|src| Hub::teardown_frames(src).iter().map(|f| f.to_string()).collect())
            .unwrap_or_default();
        h.sources.remove(&label);
        h.tag_to_label.remove(&sid);
        for f in &frames {
            h.fanout_browsers(f);
        }
        let roster = h.roster_frame().to_string();
        h.fanout_browsers(&roster);
        return;
    }
    if kind == "merge_offset" {
        // The 📍 origin drag: translate a source's whole block, hub-wide.
        // Cache shifts so replay lands at the new origin; live updates nudge
        // every open browser; the roster reports the offset.
        let sid = frame.get("sid").and_then(Value::as_str).unwrap_or("").to_string();
        let nx = frame.get("x").and_then(Value::as_f64).unwrap_or(0.0);
        let ny = frame.get("y").and_then(Value::as_f64).unwrap_or(0.0);
        let mut h = hub.lock().unwrap();
        let Some(label) = h.tag_to_label.get(&sid).cloned() else { return };
        let updates: Vec<String> = {
            let src = h.sources.get_mut(&label).unwrap();
            let (dx, dy) = (nx - src.offset.0, ny - src.offset.1);
            if dx == 0.0 && dy == 0.0 {
                return;
            }
            src.offset = (nx, ny);
            let mut out = Vec::new();
            for (id, reg) in src.registers.iter_mut() {
                if let Some(obj) = reg.as_object_mut() {
                    shift_xy(obj, dx, dy);
                    if let (Some(x), Some(y)) = (obj.get("x"), obj.get("y")) {
                        out.push(json!({"type": "update", "id": id,
                                        "payload": {"x": x, "y": y}}).to_string());
                    }
                }
            }
            for payload in src.updates.values_mut() {
                shift_xy(payload, dx, dy);
            }
            for shape in src.shapes.values_mut() {
                if let Some(obj) = shape.as_object_mut() {
                    shift_xy(obj, dx, dy);
                }
            }
            out
        };
        for u in &updates {
            h.fanout_browsers(u);
        }
        let roster = h.roster_frame().to_string();
        h.fanout_browsers(&roster);
        return;
    }
    let Some(cid) = frame.get("id").and_then(Value::as_str).map(String::from) else {
        return; // heartbeat / chat / plumbing: nothing to route in phase 1
    };
    match kind {
        "subscribe" => {
            let mut h = hub.lock().unwrap();
            h.subs.entry(cid).or_default().insert(conn_id);
        }
        "unsubscribe" => {
            let mut h = hub.lock().unwrap();
            if let Some(s) = h.subs.get_mut(&cid) {
                s.remove(&conn_id);
            }
        }
        "input" | "set_props" | "layout" | "request" | "graveyard" | "restore" => {
            let Some((tag, rest)) = cid.split_once(':') else { return };
            let mut h = hub.lock().unwrap();
            // Role ingress: a petition on a role-hidden panel is forged (that
            // viewer's browser never rendered it) — swallow before routing.
            let vrole = viewer_role(&h, conn_id);
            if let Some(owner) = h.tag_to_label.get(tag).cloned() {
                if let Some(src) = h.sources.get(&owner) {
                    if !role_may_see(&vrole, &panel_roles(src, &cid, None)) {
                        return;
                    }
                }
            }
            ledger_record(&h, kind, Some(&cid), &frame);
            if kind == "request" {
                if let Some(req_id) = frame.get("reqId").and_then(Value::as_str) {
                    if h.pending_req.len() > 256 {
                        let now = std::time::Instant::now();
                        h.pending_req.retain(|_, (_, exp)| *exp > now);
                    }
                    h.pending_req.insert(
                        req_id.to_string(),
                        (conn_id, std::time::Instant::now()
                            + std::time::Duration::from_secs(30)),
                    );
                }
            }
            let Some(label) = h.tag_to_label.get(tag).cloned() else { return };
            let offset = h.sources.get(&label).map(|s| s.offset).unwrap_or((0.0, 0.0));
            if let Some(src) = h.sources.get(&label) {
                if let Some(tx) = &src.tx {
                    let mut out = frame.clone();
                    let obj = out.as_object_mut().unwrap();
                    obj.insert("id".into(), Value::String(rest.to_string()));
                    // merged-view coords -> the source's own coords
                    if kind == "layout" {
                        shift_xy(obj, -offset.0, -offset.1);
                    } else if kind == "set_props" {
                        if let Some(Value::Object(p)) = obj.get_mut("props") {
                            shift_xy(p, -offset.0, -offset.1);
                        }
                    }
                    let _ = tx.send(Out::T(out.to_string()));
                }
            }
            if kind == "layout" {
                // The owner doesn't echo layout back; the hub folds the
                // (merged-view) geometry into its replay cache and keeps the
                // OTHER browsers in step — same division of labour as the
                // Python hub.
                let mut geom = Map::new();
                for key in ["x", "y", "w", "h", "rotation"] {
                    if let Some(v) = frame.get(key) {
                        if !v.is_null() {
                            geom.insert(key.into(), v.clone());
                        }
                    }
                }
                if !geom.is_empty() {
                    if let Some(src) = h.sources.get_mut(&label) {
                        fold_state(src, &cid, geom.clone());
                    }
                    let text = json!({"type": "update", "id": cid,
                                      "payload": geom}).to_string();
                    for (bid, btx) in &h.browsers {
                        if *bid != conn_id {
                            let _ = btx.send(Out::T(text.clone()));
                        }
                    }
                }
            }
            if kind == "input" {
                // Event subscription fan-out (composed id; originator excluded).
                if let Some(sub_ids) = h.subs.get(&cid) {
                    let copy = json!({"type": "input", "id": cid,
                                      "payload": frame.get("payload").cloned()
                                                      .unwrap_or(Value::Null)});
                    let text = copy.to_string();
                    for sid in sub_ids {
                        if *sid != conn_id {
                            if let Some(tx) = h.conns.get(sid) {
                                let _ = tx.send(Out::T(text.clone()));
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod conflation_tests {
    use super::*;

    fn t(s: &str) -> Out { Out::T(s.to_string()) }
    fn drained_texts(c: &mut ConnOut) -> Vec<String> {
        c.drain().into_iter().filter_map(|o| match o {
            Out::T(s) => Some(s), _ => None }).collect()
    }

    #[test]
    fn below_threshold_keeps_everything_in_order() {
        let mut c = ConnOut::default();
        for i in 0..10 {
            c.push(t(&format!(r#"{{"type":"update","id":"p","payload":{{"v":{i}}}}}"#)));
        }
        // fifo: all 10 kept, in order
        let out = drained_texts(&mut c);
        assert_eq!(out.len(), 10);
        assert!(out[0].contains(r#""v":0"#) && out[9].contains(r#""v":9"#));
    }

    #[test]
    fn above_threshold_conflates_same_panel_latest_wins() {
        let mut c = ConnOut::default();
        // fill past the threshold with OTHER panels so we're in slow mode
        for i in 0..CONFLATE_THRESHOLD {
            c.push(t(&format!(r#"{{"type":"update","id":"x{i}","payload":{{}}}}"#)));
        }
        // now hammer one panel — should collapse to a single latest frame
        for v in 0..100 {
            c.push(t(&format!(r#"{{"type":"update","id":"hot","payload":{{"v":{v}}}}}"#)));
        }
        let out = drained_texts(&mut c);
        let hot: Vec<_> = out.iter().filter(|s| s.contains(r#""id":"hot""#)).collect();
        assert_eq!(hot.len(), 1, "same-panel updates must coalesce to one");
        assert!(hot[0].contains(r#""v":99"#), "latest value wins");
        // and it kept its queue position (after the x* frames), not reordered
        assert_eq!(out.len(), CONFLATE_THRESHOLD + 1);
    }

    #[test]
    fn order_critical_frames_never_conflate() {
        let mut c = ConnOut::default();
        for i in 0..CONFLATE_THRESHOLD {
            c.push(t(&format!(r#"{{"type":"update","id":"x{i}","payload":{{}}}}"#)));
        }
        // register + remove for the same id must both survive (never merged)
        c.push(t(r#"{"type":"register","id":"z","component":"React"}"#));
        c.push(t(r#"{"type":"remove","id":"z"}"#));
        c.push(t(r#"{"type":"register","id":"z","component":"React"}"#));
        let out = drained_texts(&mut c);
        let z = out.iter().filter(|s| s.contains(r#""id":"z""#)).count();
        assert_eq!(z, 3, "register/remove are order-critical, never dropped");
    }

    #[test]
    fn media_conflates_by_code_and_id_but_not_file() {
        let mut c = ConnOut::default();
        for i in 0..CONFLATE_THRESHOLD {
            c.push(t(&format!(r#"{{"type":"update","id":"x{i}","payload":{{}}}}"#)));
        }
        let vid = |n: u8| {
            let id = b"cam";
            let mut f = vec![1u8, id.len() as u8]; // code 1 = VIDEO
            f.extend_from_slice(id);
            f.push(n);
            Out::B(f)
        };
        for n in 0..50 { c.push(vid(n)); }
        // two FILE transfers (code 6) with the same reqId must NOT merge
        let file = |n: u8| Out::B(vec![6u8, 2, b'r', b'q', n]);
        c.push(file(1));
        c.push(file(2));
        let batch = c.drain();
        let vids = batch.iter().filter(|o| matches!(o, Out::B(b) if b[0]==1)).count();
        let files = batch.iter().filter(|o| matches!(o, Out::B(b) if b[0]==6)).count();
        assert_eq!(vids, 1, "video frames coalesce to the latest");
        assert_eq!(files, 2, "FILE transfers never conflate");
    }
}
