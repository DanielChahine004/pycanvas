// The protocol adapter. Inbound wire frame -> store mutation (source:'remote');
// local store change -> outbound frame. This is the faithful port of the old
// bridge.js, rewritten to talk to the engine's `store` + `editor` instead of a
// board library. It invents no protocol — every frame matches danvas's existing
// wire contract (verified against captured frames).
//
// Every message kind is handled here: connection + welcome/register/update/
// remove, the live data/style channels, input/request/binary, shared assets,
// presence, and the arrow/shape/order/container_sync/reflow/draw/snapshot/
// image/cursor/graveyard/chat/view-camera handlers below.
import { generateKeyBetween } from 'fractional-indexing'
import { effect, signal } from 'alien-signals'
import { store } from './engine/store'
import { editor, screenToPage } from './engine/editor'
import { applyCameraFrom, scheduleInitialFit, resetInitialFit, markInitialFitDone, setNavigationMode, zoomCanvasAtClient, setCamera } from './engine/camera'
import { openContextMenuAt } from './engine/contextmenu'
import { toImage } from './engine/export'
import { measuredTextSize } from './react/measure'
import type { ArrowRecord, DrawingRecord, PanelRecord, PanelShapeType, WriteSignal } from './engine/types'
import { BIN_VIDEO, BIN_AUDIO, BIN_CUSTOM, BIN_REACT, BIN_INPUT, BIN_FILE } from './protocol.generated.js'

// --- id helpers --------------------------------------------------------------
export function createShapeId(id: string): string {
  return `shape:${id}`
}
export function componentIdOf(shapeId: string): string {
  return String(shapeId).replace(/^shape:/, '')
}

// --- component -> shape type + default props (see canvas.jsx) -----------------
const COMPONENT_TO_SHAPE: Record<string, PanelShapeType> = {
  Label: 'pcLabel',
  Custom: 'pcHtml',
  React: 'pcReact',
}

function getDefaultProps(type: PanelShapeType): Record<string, any> {
  switch (type) {
    case 'pcLabel':
      return { w: 240, h: 84, label: 'label', value: '' }
    case 'pcHtml':
      return { w: 380, h: 320, label: 'custom', html: '', themed: false, permissions: '' }
    case 'pcReact':
      return { w: 380, h: 320, label: 'react', source: '', data: '{}', css: '', autoH: false, autoW: false, libs: '[]', wasm: '' }
  }
}

// --- fractional z-index -------------------------------------------------------
let lastIndex: string | null = null
function nextIndex(): string {
  lastIndex = generateKeyBetween(lastIndex, null)
  return lastIndex
}

// --- auto-placement flow (port of bridge.js nextPosition) --------------------
// Panels that arrive without x/y flow left-to-right, top-to-bottom, packed by
// each panel's real size (+ a gap) so they never overlap. nextPosition gives a
// sane first slot; the masonry re-pack below (relayoutFlow) settles the set
// once content-fit sizes land.
const FLOW_GAP = 24
const FLOW_X0 = 80
const FLOW_Y0 = 80
const FLOW_MAX_W = 1500
let flowX = FLOW_X0
let flowY = FLOW_Y0
let flowRowH = 0
// Shape ids placed by the flow and not since moved/resized by the user.
// nextPosition gives each a slot immediately; relayoutFlow re-packs the whole set
// once content-fit sizes settle. A user gesture (M3) drops the panel from the set.
const flowItems = new Set<string>()
let relayoutTimer: any = null

// The auto-flow must not bury content the OWNER positioned: track the lowest
// bottom edge of explicitly/rel-placed panels and start the flow below it.
// Without this the flow cursor was blind to explicit panels, so a script
// mixing a hand-placed intro with unplaced plots stacked the plots on top of
// the intro — silently.
let explicitBottom = FLOW_Y0

function noteExplicit(y: number, h: any): void {
  const eh = typeof h === 'number' ? h : 96
  explicitBottom = Math.max(explicitBottom, y + eh + FLOW_GAP)
}

function nextPosition(w: any, h: any): { x: number; y: number } {
  w = typeof w === 'number' ? w : 240
  h = typeof h === 'number' ? h : 96
  if (flowX > FLOW_X0 && flowX + w > FLOW_X0 + FLOW_MAX_W) {
    flowX = FLOW_X0
    flowY += flowRowH + FLOW_GAP
    flowRowH = 0
  }
  if (flowX === FLOW_X0) flowY = Math.max(flowY, explicitBottom)
  const pos = { x: flowX, y: flowY }
  flowX += w + FLOW_GAP
  flowRowH = Math.max(flowRowH, h)
  return pos
}

function resetFlow(): void {
  flowX = FLOW_X0
  flowY = FLOW_Y0
  flowRowH = 0
  explicitBottom = FLOW_Y0
  flowItems.clear()
  armedReflows.clear()
  if (relayoutTimer) {
    clearTimeout(relayoutTimer)
    relayoutTimer = null
  }
  containers.clear()
  panelToContainer.clear()
  containerParent.clear()
  relSpecs.clear()
  relDeps.clear()
  relCascadeQueue.clear()
  if (relCascadeTimer) {
    clearTimeout(relCascadeTimer)
    relCascadeTimer = null
  }
}

// Shortest-column masonry: balance the auto-placed panels across columns by
// running height. Column width = widest panel; column count derives from
// FLOW_MAX_W. Mixed heights pack tight like masonry (port of bridge.js).
function packMasonry(
  items: { id: string; w: number; h: number }[],
  { x0 = FLOW_X0, y0 = FLOW_Y0, gap = FLOW_GAP, maxRowW = FLOW_MAX_W } = {},
): Map<string, { x: number; y: number }> {
  const pos = new Map<string, { x: number; y: number }>()
  if (!items.length) return pos
  const colW = Math.max(...items.map((it) => it.w))
  const cols = Math.max(1, Math.floor((maxRowW + gap) / (colW + gap)))
  const colH = new Array(cols).fill(y0)
  for (const it of items) {
    let j = 0
    for (let k = 1; k < cols; k++) if (colH[k] < colH[j] - 0.5) j = k
    pos.set(it.id, { x: x0 + j * (colW + gap), y: colH[j] })
    colH[j] += it.h + gap
  }
  return pos
}

// Re-pack every still-auto-placed panel by its current (post-fit) size. Runs as
// a remote change (no echo / no re-drop from the flow); new positions are
// reported to Python so they persist across reconnects.
function relayoutFlow(): void {
  relayoutTimer = null
  if (flowItems.size === 0) return
  const items: { id: string; w: number; h: number }[] = []
  for (const shapeId of flowItems) {
    const shape = store.peek(shapeId) as PanelRecord | undefined
    if (!shape) {
      flowItems.delete(shapeId)
      continue
    }
    items.push({ id: shapeId, w: shape.props.w, h: shape.props.h })
  }
  if (!items.length) return
  const pos = packMasonry(items, { y0: Math.max(FLOW_Y0, explicitBottom) })
  const moves: any[] = []
  applyRemote(() => {
    for (const it of items) {
      const shape = store.peek(it.id) as PanelRecord | undefined
      const p = pos.get(it.id)
      if (!shape || !p || (Math.abs(shape.x - p.x) < 0.5 && Math.abs(shape.y - p.y) < 0.5)) continue
      store.patch(it.id, { x: p.x, y: p.y })
      moves.push({ id: componentIdOf(it.id), x: p.x, y: p.y })
    }
  })
  for (const m of moves) sendRaw({ type: 'layout', auto: true, ...m })
}

// Debounced: a burst of content fits lands over a short window after load, so
// re-pack once they've settled rather than on every individual resize.
function scheduleRelayout(): void {
  if (relayoutTimer) clearTimeout(relayoutTimer)
  relayoutTimer = setTimeout(relayoutFlow, 150)
}

// --- relative placement (the register frame's `rel` field) -------------------
// A register may carry {rel: {kind: below|above|right_of|left_of, anchor: <id>,
// gap: px}} — resolved HERE, the one place that always knows live geometry, so
// every SDK's below() chaining is one implementation instead of N (PROTOCOL.md
// § relative placement). Explicit x/y in the frame wins (the SDK resolved it,
// or a folded-back drag); the rel still records the dependency edge, so when
// an anchor's HEIGHT settles differently (auto-fit content, a user resize) the
// chain re-settles in the same frame — no wire round-trip, no flicker. The
// anchor id arrives owner-side (hubs don't parse rel), so it's qualified with
// the registering panel's own namespace tag.
const relSpecs = new Map<string, { anchor: string; kind: string; gap: number }>()
const relDeps = new Map<string, Set<string>>() // anchor shapeId -> dep shapeIds
const relCascadeQueue = new Set<string>()
let relCascadeTimer: any = null

function recordRel(shapeId: string, componentId: string, rel: any) {
  const i = componentId.lastIndexOf(':')
  const tag = i >= 0 ? componentId.slice(0, i + 1) : ''
  const anchor = createShapeId(tag + String(rel.anchor))
  const spec = {
    anchor,
    kind: String(rel.kind || 'below'),
    gap: typeof rel.gap === 'number' ? rel.gap : 16,
  }
  relSpecs.set(shapeId, spec)
  if (!relDeps.has(anchor)) relDeps.set(anchor, new Set())
  relDeps.get(anchor)!.add(shapeId)
  return spec
}

function resolveRel(
  spec: { anchor: string; kind: string; gap: number },
  depW: any,
  depH: any,
): { x: number; y: number } | null {
  const a = store.peek(spec.anchor) as any
  if (!a || typeof a.x !== 'number' || typeof a.y !== 'number') return null
  const aw = typeof a.props?.w === 'number' ? a.props.w : 0
  const ah = typeof a.props?.h === 'number' ? a.props.h : 0
  const w = typeof depW === 'number' ? depW : 0
  const h = typeof depH === 'number' ? depH : 0
  switch (spec.kind) {
    case 'above':
      return { x: a.x, y: a.y - spec.gap - h }
    case 'right_of':
      return { x: a.x + aw + spec.gap, y: a.y }
    case 'left_of':
      return { x: a.x - spec.gap - w, y: a.y }
    default: // below
      return { x: a.x, y: a.y + ah + spec.gap }
  }
}

// Re-settle every panel anchored (transitively) on `anchorSid` from its live
// geometry; new positions are reported back so owners fold them into replay.
function cascadeRel(anchorSid: string, seen: Set<string>): void {
  const deps = relDeps.get(anchorSid)
  if (!deps) return
  for (const dep of Array.from(deps)) {
    if (seen.has(dep)) continue // cycle guard
    seen.add(dep)
    const spec = relSpecs.get(dep)
    const shape = store.peek(dep) as any
    if (!spec || !shape) {
      deps.delete(dep)
      continue
    }
    const p = resolveRel(spec, shape.props?.w, shape.props?.h)
    if (!p) continue
    if (Math.abs(shape.x - p.x) >= 0.5 || Math.abs(shape.y - p.y) >= 0.5) {
      applyRemote(() => store.patch(dep, { x: p.x, y: p.y }))
      sendRaw({ type: 'layout', id: componentIdOf(dep), x: p.x, y: p.y, auto: true })
    }
    cascadeRel(dep, seen)
  }
}

// Debounced per burst (a drag-resize streams h changes every frame).
function scheduleRelCascade(anchorSid: string): void {
  if (!relDeps.has(anchorSid)) return
  relCascadeQueue.add(anchorSid)
  if (relCascadeTimer) clearTimeout(relCascadeTimer)
  relCascadeTimer = setTimeout(() => {
    relCascadeTimer = null
    const seen = new Set<string>()
    for (const sid of relCascadeQueue) cascadeRel(sid, seen)
    relCascadeQueue.clear()
  }, 120)
}

// --- persistent layout containers (canvas.column / row / container) ----------
// When any member's size changes (content-fit), the whole tree repacks so panels
// never overlap after h="auto" settles. Port of bridge.js's container block.
const containers = new Map<string, any>()
const panelToContainer = new Map<string, string>()
const containerParent = new Map<string, string>()
const armedReflows = new Map<string, { spec: any; deadline: number }>()
const REFLOW_ARM_MS = 800
let fillWObserver: ResizeObserver | null = null

function syncContainer(spec: any): void {
  containers.set(spec.key, spec)
  rebuildContainerIndices()
  if (spec.fill_w) {
    setupFillWObserver()
    if (editor.getContainer()) repackContainer(getRootContainerKey(spec.key))
  }
}

function rebuildContainerIndices(): void {
  panelToContainer.clear()
  containerParent.clear()
  for (const [key, spec] of containers) {
    for (const m of spec.members || []) {
      if (m.kind === 'panel') panelToContainer.set(m.id, key)
      else if (m.kind === 'container') containerParent.set(m.key, key)
    }
  }
}

function getRootContainerKey(key: string): string {
  while (containerParent.has(key)) key = containerParent.get(key)!
  return key
}

function refreshFillW(): void {
  const seen = new Set<string>()
  for (const [key, spec] of containers) {
    if (!spec.fill_w) continue
    const root = getRootContainerKey(key)
    if (!seen.has(root)) {
      seen.add(root)
      repackContainer(root)
    }
  }
}

function setupFillWObserver(): void {
  if (fillWObserver || typeof ResizeObserver === 'undefined') return
  const el = editor.getContainer()
  if (!el) return
  fillWObserver = new ResizeObserver(refreshFillW)
  fillWObserver.observe(el)
}

// Recursive repack; returns {w,h} so a parent can advance its cursor past a child.
function repackContainer(key: string, ox?: number, oy?: number): { w: number; h: number } {
  const spec = containers.get(key)
  if (!spec) return { w: 0, h: 0 }
  const el = editor.getContainer()
  const specW = spec.fill_w
    ? Math.floor((el ? el.offsetWidth : 0) / store.camera().z) - 2 * (spec.padding ?? 0)
    : spec.w
  const x0 = ox !== undefined ? ox : spec.x0 ?? 0
  const y0 = oy !== undefined ? oy : spec.y0 ?? 0
  let cx = x0,
    cy = y0
  let crossSize = 0
  const reports: any[] = []

  for (const m of spec.members || []) {
    const mx = spec.mode === 'row' ? cx : x0
    const my = spec.mode === 'column' ? cy : y0
    let mw = 0,
      mh = 0
    if (m.kind === 'panel') {
      const shapeId = createShapeId(m.id)
      const shape = store.peek(shapeId) as PanelRecord | undefined
      if (!shape) continue
      mw = specW != null ? specW : shape.props.w
      mh = spec.h != null ? spec.h : shape.props.h
      const needsMove = Math.abs(shape.x - mx) >= 0.5 || Math.abs(shape.y - my) >= 0.5
      const needsW = spec.mode === 'column' && specW != null && Math.abs(shape.props.w - specW) >= 0.5
      const needsH = spec.mode === 'row' && spec.h != null && Math.abs(shape.props.h - spec.h) >= 0.5
      if (needsMove || needsW || needsH) {
        const patch: any = {}
        if (needsMove) {
          patch.x = mx
          patch.y = my
        }
        if (needsW || needsH) patch.props = { ...(needsW ? { w: specW } : {}), ...(needsH ? { h: spec.h } : {}) }
        applyRemote(() => store.patch(shapeId, patch))
        const report: any = { type: 'layout', id: m.id }
        if (needsMove) {
          report.x = mx
          report.y = my
        }
        if (needsW) report.w = specW
        if (needsH) report.h = spec.h
        reports.push(report)
      }
    } else if (m.kind === 'container') {
      const size = repackContainer(m.key, mx, my)
      mw = size.w
      mh = size.h
    }
    if (spec.mode === 'column') {
      cy += mh + spec.gap
      crossSize = Math.max(crossSize, mw)
    } else {
      cx += mw + spec.gap
      crossSize = Math.max(crossSize, mh)
    }
  }
  for (const r of reports) sendRaw(r)
  const mainSize =
    spec.mode === 'column' ? Math.max(0, cy - y0 - spec.gap) : Math.max(0, cx - x0 - spec.gap)
  return spec.mode === 'column'
    ? { w: specW ?? crossSize, h: spec.h ?? mainSize }
    : { w: specW ?? mainSize, h: spec.h ?? crossSize }
}

function autoRepackForPanel(panelId: string): void {
  const key = panelToContainer.get(panelId)
  if (!key) return
  repackContainer(getRootContainerKey(key))
}

// Explicit one-axis re-pack of a group (canvas.column/row refit()).
function packFlow(spec: any): void {
  if (!spec || !spec.ids || !spec.ids.length) return
  const moves: any[] = []
  applyRemote(() => {
    let cx = spec.x0,
      cy = spec.y0
    for (const cid of spec.ids) {
      const shapeId = createShapeId(cid)
      const shape = store.peek(shapeId) as PanelRecord | undefined
      if (!shape) continue
      const x = spec.kind === 'row' ? cx : spec.x0
      const y = spec.kind === 'row' ? spec.y0 : cy
      if (Math.abs(shape.x - x) >= 0.5 || Math.abs(shape.y - y) >= 0.5) {
        store.patch(shapeId, { x, y })
        moves.push({ id: cid, x, y })
      }
      if (spec.kind === 'row') cx += shape.props.w + spec.gap
      else cy += shape.props.h + spec.gap
    }
  })
  for (const m of moves) sendRaw({ type: 'layout', ...m })
}

function reflowGroup(msg: any): void {
  const spec = { ids: msg.ids || [], kind: msg.kind, x0: msg.x0, y0: msg.y0, gap: msg.gap }
  packFlow(spec)
  if (spec.ids.length) armedReflows.set(msg.key, { spec, deadline: Date.now() + REFLOW_ARM_MS })
}

function settleArmedReflows(componentId: string): void {
  if (!armedReflows.size) return
  const now = Date.now()
  for (const [key, entry] of armedReflows) {
    if (now > entry.deadline) {
      armedReflows.delete(key)
      continue
    }
    if (entry.spec.ids.includes(componentId)) packFlow(entry.spec)
  }
}

// --- lock / chrome meta (port of bridge.js lockMeta) -------------------------
function lockMeta(
  base: PanelRecord['meta'],
  movable?: boolean,
  resizable?: boolean,
  interactive?: boolean,
  selectable?: boolean,
  frame?: boolean,
  frameColor?: string,
  wheelLocal?: boolean,
): PanelRecord['meta'] {
  const meta = { ...(base || {}) }
  if (typeof movable === 'boolean') meta.lockMove = !movable
  if (typeof resizable === 'boolean') meta.lockResize = !resizable
  if (typeof interactive === 'boolean') meta.lockInput = !interactive
  if (typeof selectable === 'boolean') meta.noGrab = !selectable
  if (typeof frame === 'boolean') meta.noFrame = !frame
  if (typeof frameColor === 'string') meta.frameColor = frameColor
  if (typeof wheelLocal === 'boolean') meta.wheelLocal = wheelLocal
  return meta
}

function hasLockFlags(m: any): boolean {
  return (
    typeof m.movable === 'boolean' ||
    typeof m.resizable === 'boolean' ||
    typeof m.interactive === 'boolean' ||
    typeof m.selectable === 'boolean' ||
    typeof m.frame === 'boolean' ||
    typeof m.frameColor === 'string' ||
    typeof m.wheelLocal === 'boolean'
  )
}

// danvas-managed shape ids (panels + connector arrows), excluded from free-form
// drawing sync and cleared on a run change.
const managedIds = new Set<string>()

// Run Python-driven mutations as a remote batch so geometry read-back ignores
// them (no echo). The read-only lift the old build needed only gated *user*
// gestures, which here are handled separately, so a plain remote transact
// suffices: Python updates always apply.
function applyRemote(fn: () => void): void {
  store.transact('remote', fn)
}

// === connection ==============================================================
let ws: WebSocket | null = null
let heartbeatTimer: any = null
let lastRunId: string | null = null

const VIEWER_KEY = 'pc_viewer'
function loadStoredIdentity(): any {
  try {
    return JSON.parse(sessionStorage.getItem(VIEWER_KEY) || 'null')
  } catch {
    return null
  }
}
function persistIdentity(v: any): void {
  try {
    if (v && v.id) sessionStorage.setItem(VIEWER_KEY, JSON.stringify({ id: v.id, name: v.name, color: v.color }))
  } catch {
    /* private mode */
  }
}

function sendRaw(msg: any): void {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg))
}

// --- disconnect treatment ----------------------------------------------------
// When the socket drops, nothing is cleared: the tab keeps rendering the
// last-known state and the reconnect replay heals it. Make that hold VISIBLE —
// stale data must not read as live. After a short grace window (so a
// hot-reload restart or a network blip doesn't flash it), dim the page and pin
// a banner until the connection returns. Inputs already go nowhere while down
// (sendRaw guards on readyState), so this is purely the staleness signal — the
// client-side mirror of the merge hub's retain freeze.
let dcTimer: any = null
let dcOverlay: HTMLElement | null = null
const DC_GRACE_MS = 1500

function showDisconnected(): void {
  if (dcOverlay) return
  const el = document.createElement('div')
  el.id = 'pc-disconnected'
  el.setAttribute(
    'style',
    'position:fixed;inset:0;z-index:99999;pointer-events:none;' +
      'background:rgba(0,0,0,0.28);',
  )
  const banner = document.createElement('div')
  banner.setAttribute(
    'style',
    'position:absolute;top:0;left:0;right:0;padding:6px 14px;' +
      'font:13px/1.4 "Inter Variable",Inter,system-ui,sans-serif;' +
      'text-align:center;color:#fff;background:#b45309;opacity:0.95;',
  )
  banner.textContent = '⚠ Connection lost — showing last known state; retrying…'
  el.appendChild(banner)
  document.body.appendChild(el)
  dcOverlay = el
}

function hideDisconnected(): void {
  if (dcTimer) {
    clearTimeout(dcTimer)
    dcTimer = null
  }
  if (dcOverlay) {
    dcOverlay.remove()
    dcOverlay = null
  }
}

function scheduleDisconnected(): void {
  if (dcTimer || dcOverlay) return
  dcTimer = setTimeout(() => {
    dcTimer = null
    showDisconnected()
  }, DC_GRACE_MS)
}

export function connect(): void {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws'
  let url = `${proto}://${location.host}/ws`
  const me = loadStoredIdentity()
  const q = new URLSearchParams()
  if (me && me.id) {
    q.set('vid', me.id)
    if (me.name) q.set('vname', me.name)
    if (me.color) q.set('vcolor', me.color)
  }
  // Merge view: forward the page's ?sources= so the standing merge server composes
  // this browser's chosen set (a plain canvas has no ?sources= and ignores it).
  const pageSources = new URLSearchParams(location.search).get('sources')
  if (pageSources) q.set('sources', pageSources)
  const qs = q.toString()
  if (qs) url += `?${qs}`
  ws = new WebSocket(url)
  ws.binaryType = 'arraybuffer'

  ws.onopen = () => {
    hideDisconnected()
    if (heartbeatTimer) clearInterval(heartbeatTimer)
    heartbeatTimer = setInterval(() => sendRaw({ type: 'heartbeat' }), 10000)
  }
  ws.onmessage = (ev) => {
    if (ev.data instanceof ArrayBuffer) {
      handleBinary(ev.data)
      return
    }
    let msg
    try {
      msg = JSON.parse(ev.data)
    } catch {
      return
    }
    handle(msg)
  }
  ws.onclose = () => {
    ws = null
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer)
      heartbeatTimer = null
    }
    scheduleDisconnected()
    setTimeout(connect, 1000)
  }
  ws.onerror = () => {
    try {
      ws?.close()
    } catch {
      /* ignore */
    }
  }
}

const frameDecoder = new TextDecoder()
function handleBinary(buf: ArrayBuffer): void {
  if (buf.byteLength < 2) return
  const head = new Uint8Array(buf, 0, 2)
  const type = head[0]
  const idLen = head[1]
  if (buf.byteLength < 2 + idLen) return
  const id = frameDecoder.decode(new Uint8Array(buf, 2, idLen))
  const payload = buf.slice(2 + idLen)
  if (type === BIN_FILE) {
    return // hub <-> owner file transfer; never addressed to a browser
  }
  if (type === BIN_VIDEO || type === BIN_AUDIO || type === BIN_CUSTOM || type === BIN_REACT) {
    const handler = liveHandlers.get(id)
    if (handler) handler(payload)
  }
}

// === inbound dispatch ========================================================
function handle(msg: any): void {
  if (!msg || !msg.type) return
  switch (msg.type) {
    case 'welcome':
      onWelcome(msg)
      break
    case 'register':
      registerComponent(msg)
      break
    case 'update':
      updateComponent(msg.id, msg.payload || {})
      break
    case 'remove':
      removeComponent(msg.id)
      break
    case 'presence':
      setPresence(msg.count || 0)
      setRoster(msg.viewers || [])
      break
    case 'shared':
      applyShared(msg)
      break
    case 'response':
      resolveRequest(msg.reqId, msg.result, msg.error)
      break
    case 'hosting':
      // Live exposure change (LAN listener / tunnel opened) — refresh the 🌐
      // flyout on every viewer.
      setHosting(msg)
      break
    case 'view':
      applyLiveView(msg.view || {})
      break
    case 'container_sync':
      syncContainer(msg)
      break
    case 'reflow':
      reflowGroup(msg)
      break
    case 'order':
      reorderComponent(msg.id, msg.op)
      break
    case 'get_image':
      sendImage(msg.reqId, msg.shapeIds || [])
      break
    case 'get_snapshot':
      // Python wants the user's free-form drawings only (panels/arrows are
      // recreated from code, so they're excluded). The browser is the source of
      // truth for ink; we hand back a serialisable bundle keyed by record id.
      sendRaw({ type: 'snapshot', reqId: msg.reqId, data: userDrawingsSnapshot(msg.panelIds || []) })
      break
    case 'load_snapshot':
      loadDrawingsSnapshot(msg.data)
      scheduleInitialFit(hasExplicitCamera)
      break
    case 'cursor':
      setPeerCursor(msg) // a peer moved — render their cursor
      break
    case 'cursor_gone':
      removePeerCursor(msg.id) // a peer left
      break
    case 'graveyard_update':
      setGraveyardItems(msg.items || [])
      break
    case 'arrow':
      createArrow(msg)
      break
    case 'shape':
      createManagedShape(msg)
      break
    case 'shape_update':
      updateManagedShape(msg)
      break
    case 'draw':
      // A peer (or the server's on-connect replay) sent free-form ink — fold it
      // into our store as a remote change so it doesn't echo back out.
      applyDrawDiff(msg.diff)
      break
    case 'chat':
      ingestChat(msg)
      break
    case 'merge_sources':
      setMergeSources(msg.sources || [])
      break
    case 'merge_auth_required':
      addMergeAuthPrompt(msg.uri, msg.label || msg.uri)
      break
    case 'merge_auth_failed':
      markMergeAuthFailed(msg.uri, msg.label || msg.uri)
      break
    case 'serve_config':
      // The owner delivered its UI gating after we connected (a hot-reload
      // browser outlives the worker, so its welcome predates the flags) —
      // apply live, only the fields present.
      if (typeof msg.uiInspector === 'boolean') setUiInspectorEnabled(msg.uiInspector)
      if (typeof msg.uiGraveyard === 'boolean') setGraveyardEnabled(msg.uiGraveyard)
      if (typeof msg.cursors === 'boolean') setCursorsEnabled(msg.cursors)
      break
    default:
      break
  }
}

function onWelcome(msg: any): void {
  if (msg.reload || (lastRunId !== null && msg.runId && msg.runId !== lastRunId)) {
    clearManaged()
    clearPeerCursors() // stale peers from the previous run shouldn't linger
    resetChat() // a new run restarts msgId from 1 — drop the old log so replay repopulates
  }
  // A hot reload restarts the worker but the browser stays live — the user's
  // camera (and any pan/zoom they did) is still in the store. clearManaged()
  // above re-armed the once-per-load auto-fit; cancel it again so the panels
  // re-registering don't yank the view back to a fit. Toggling serve(view={ui:})
  // goes through this path, so the chrome shows/hides without moving the camera.
  if (msg.reload) markInitialFitDone()
  if (msg.runId) lastRunId = msg.runId
  setIdentity(msg.you || null)
  setUiInspectorEnabled(!!msg.uiInspector)
  setGraveyardEnabled(!!msg.uiGraveyard)
  setHosting({ ...(msg.hosting || {}), enabled: !!msg.uiHosting })
  setAuthEnabled(!!msg.auth)
  setCursorsEnabled(!!msg.cursors)
  setViewConfig(msg.view || null)
  // Merge server advertises mergeHost (this page IS a merge view → show the merge
  // panel); a plain canvas advertises mergeServer + selfUrl (→ show a Merge button
  // that navigates to that server pre-seeded with this canvas). See the merge block.
  setMergeHost(!!msg.mergeHost)
  setMergeLaunch(msg.mergeServer || null, msg.selfUrl || null)
}

// === register / update / remove ==============================================
function registerComponent(msg: any): void {
  const shapeType = COMPONENT_TO_SHAPE[msg.component]
  if (!shapeType) return
  const id: string = msg.id
  if (id === UI_INSPECTOR_ID) setUiInspectorOpen(true)
  const shapeId = createShapeId(id)
  managedIds.add(shapeId)
  if (store.has(shapeId)) return // already on canvas (reconnect)

  const props = msg.props || {}
  let px = msg.x
  let py = msg.y
  // Relative placement: record the dependency edge whether or not the frame
  // also carries x/y (an SDK may have resolved the chain itself; the edge
  // still drives the height-settle cascade). Placement itself: explicit x/y
  // wins, then rel against a known anchor, then the masonry flow.
  const rel = msg.rel && typeof msg.rel === 'object'
    && typeof msg.rel.anchor === 'string'
    ? recordRel(shapeId, id, msg.rel) : null
  let autoPlaced = typeof px !== 'number' || typeof py !== 'number'
  let relPlaced = false
  if (autoPlaced && rel) {
    const p = resolveRel(rel, props.w, props.h)
    if (p) {
      px = p.x
      py = p.y
      relPlaced = true
      autoPlaced = false
    }
  }
  if (autoPlaced) {
    const auto = nextPosition(props.w, props.h)
    if (typeof px !== 'number') px = auto.x
    if (typeof py !== 'number') py = auto.y
  } else {
    noteExplicit(py, props.h)   // flow stays below owner-positioned panels
  }

  const rec: PanelRecord = {
    typeName: 'panel',
    id: shapeId,
    shapeType,
    x: px,
    y: py,
    rotation: typeof msg.rotation === 'number' ? msg.rotation : 0,
    opacity: typeof msg.opacity === 'number' ? msg.opacity : 1,
    isLocked: typeof msg.locked === 'boolean' ? msg.locked : false,
    index: nextIndex(),
    props: { ...getDefaultProps(shapeType), ...props },
    meta: hasLockFlags(msg)
      ? lockMeta({}, msg.movable, msg.resizable, msg.interactive, msg.selectable, msg.frame, msg.frameColor, msg.wheelLocal)
      : {},
  }
  // The native UI inspector + the dispatch-trace panel float above the drawing
  // layer (and other panels) so ink/shapes never cover these debugging tools.
  const lbl = String(props.label || '').toLowerCase()
  if (lbl === 'inspector' || lbl === 'dispatch trace' || lbl.includes('trace')) rec.meta.topmost = true
  applyRemote(() => store.put(rec))

  if (autoPlaced) {
    // Pin the auto-assigned slot back to Python so it survives reconnects.
    sendRaw({ type: 'layout', id, x: px, y: py, auto: true })
    flowItems.add(shapeId)
    scheduleRelayout()
  } else if (relPlaced) {
    // Pin the rel-resolved position back to the owner the same way.
    sendRaw({ type: 'layout', id, x: px, y: py, auto: true })
  }
  // Frame every just-registered panel on first load when no explicit camera was
  // configured. Debounced so a burst of registers fits the whole set at once.
  scheduleInitialFit(hasExplicitCamera)
}

function updateComponent(id: string, payload: any): void {
  // Live telemetry (LivePlot) — bypasses the store, buffered + pushed to node.
  if (payload && payload.plot) {
    liveBuffer.set(id, payload.plot)
    liveHandlers.get(id)?.(payload.plot)
    return
  }
  if (payload && payload.plot_extend) {
    const ext = payload.plot_extend
    const fig: any = liveBuffer.get(id)
    if (!fig || !fig.data) return
    ext.indices.forEach((ti: number, k: number) => {
      const tr = fig.data[ti]
      if (!tr) return
      tr.x = (tr.x || []).concat(ext.x[k])
      tr.y = (tr.y || []).concat(ext.y[k])
      if (ext.max && tr.x.length > ext.max) {
        tr.x = tr.x.slice(-ext.max)
        tr.y = tr.y.slice(-ext.max)
      }
    })
    liveHandlers.get(id)?.({ __extend: ext })
    return
  }
  // Live style (React.color setter) — bypasses the store.
  if (payload && payload.post_style !== undefined) {
    styleBuffer.set(id, payload.post_style)
    styleHandlers.get(id)?.(payload.post_style)
    return
  }
  // Generic push() value — forwarded to the mounted node's `value` AND
  // buffered, so a culled panel re-mounts current (registerLive replays the
  // buffer, exactly as it already does for plots). Without the buffer a
  // control that was scrolled out of view missed this frame and visually
  // snapped back to its registered props on scroll-in — Python's state was
  // right, the display lied. Custom iframes stay unbuffered: their push is
  // an EVENT channel (an in-tab replay could double-fire side effects) and
  // they re-sync through their own ready → owner-re-push contract instead.
  if (payload && payload.post !== undefined) {
    const rec = store.peek(createShapeId(id)) as any
    if (rec?.props?.component !== 'Custom') liveBuffer.set(id, payload.post)
    liveHandlers.get(id)?.(payload.post)
    return
  }

  // Otherwise a shape patch: split top-level fields from props.
  const shapeId = createShapeId(id)
  const shape = store.peek(shapeId)
  if (!shape) return
  const { x, y, rotation, opacity, locked, movable, resizable, interactive, selectable, frame, frameColor, wheelLocal, data_patch, ...props } = payload
  const patch: any = { props: { ...props } }
  // data_patch carries only the changed props (React.update sends a delta, not the
  // whole blob). Merge it into the panel's current data — preferring a same-message
  // full `data` if one is also present (rare) — keeping props.data the full JSON the
  // store/persistence/reconnect path expects.
  if (data_patch && typeof data_patch === 'object') {
    const baseStr = typeof (props as any).data === 'string' ? (props as any).data : (shape as any).props?.data
    let base: any = {}
    try {
      base = baseStr ? JSON.parse(baseStr) : {}
    } catch {
      base = {}
    }
    patch.props.data = JSON.stringify({ ...base, ...data_patch })
  }
  if (typeof x === 'number') patch.x = x
  if (typeof y === 'number') patch.y = y
  if (typeof rotation === 'number') patch.rotation = rotation
  if (typeof opacity === 'number') patch.opacity = opacity
  if (typeof locked === 'boolean') patch.isLocked = locked
  if (hasLockFlags(payload)) {
    patch.meta = lockMeta((shape as any).meta, movable, resizable, interactive, selectable, frame, frameColor, wheelLocal)
  }
  if (typeof x === 'number' && typeof y === 'number') flowItems.delete(shapeId)
  applyRemote(() => store.patch(shapeId, patch))
  // An owner-driven height change re-settles any rel chain hanging off this
  // panel (parity with the owner-side cascade this replaces).
  if (typeof (payload as any).h === 'number') scheduleRelCascade(shapeId)
}

// Change a managed panel's stacking order (Python to_front/to_back/forward/
// backward). Remote so it doesn't echo back as a user edit.
function reorderComponent(id: string, op: string): void {
  const shapeId = createShapeId(id)
  if (!store.has(shapeId)) return
  if (op === 'front' || op === 'back' || op === 'forward' || op === 'backward') {
    store.reorder(shapeId, op, 'remote')
  }
}

// --- user move/resize/rotate read-back -> Python (port of setupGeometrySync) --
// A user gesture patches the record with source:'local'; this watcher reports the
// settled geometry to Python as one debounced `layout` frame and drops the panel
// from the auto-flow (pinning it where the user put it). Remote changes (Python
// updates, relayout, container repack) are ignored, so nothing echoes.
const dirtyShapes = new Set<string>()
let flushTimer: any = null

function flushGeometry(): void {
  flushTimer = null
  for (const shapeId of dirtyShapes) {
    const shape = store.peek(shapeId) as PanelRecord | undefined
    if (!shape) continue
    sendRaw({
      type: 'layout',
      id: componentIdOf(shapeId),
      x: shape.x,
      y: shape.y,
      rotation: shape.rotation, // radians
      w: shape.props.w,
      h: shape.props.h,
      // Carry the content-fit flags so a manual resize that PINS an auto axis
      // (autoH/autoW → false; see SelectionOverlay) sticks in Python and reaches
      // other viewers, instead of the content-fit re-asserting itself.
      ...(typeof shape.props.autoH === 'boolean' ? { autoH: shape.props.autoH } : {}),
      ...(typeof shape.props.autoW === 'boolean' ? { autoW: shape.props.autoW } : {}),
    })
  }
  dirtyShapes.clear()
}

store.subscribe((changes, source) => {
  if (source !== 'local') return
  let any = false
  for (const ch of changes) {
    // A user deleted a danvas-managed shape: tell Python so it keeps the
    // component (callbacks stay live) and shows it in the graveyard for restore.
    if (ch.op === 'remove') {
      if (managedIds.has(ch.id)) sendRaw({ type: 'graveyard', id: componentIdOf(ch.id) })
      continue
    }
    // The inverse: undo of a delete re-adds a managed shape locally — restore it
    // in Python too (un-graveyard). Python's re-register is a no-op here (the
    // record is already present) but reaches the other viewers.
    if (ch.op === 'add') {
      if (managedIds.has(ch.id)) sendRaw({ type: 'restore', id: componentIdOf(ch.id) })
      continue
    }
    if (ch.op !== 'update') continue
    const prev = ch.prev as any
    const next = ch.next as any
    if (!prev || !next) continue
    // Report geometry for panels AND Python-managed shapes (canvas.geo/text/line/
    // …) so a user move/resize of either flows back to Python and survives a
    // reload. User-drawn (non-managed) shapes go via the draw-sync instead.
    const isPanel = next.typeName === 'panel'
    const isManagedShape = next.typeName === 'drawing' && managedIds.has(next.id)
    if (!isPanel && !isManagedShape) continue
    if (
      prev.x !== next.x ||
      prev.y !== next.y ||
      prev.rotation !== next.rotation ||
      prev.props.w !== next.props.w ||
      prev.props.h !== next.props.h
    ) {
      if (isPanel) flowItems.delete(next.id) // a user gesture pins the panel out of the auto-flow
      if (isPanel && prev.props.h !== next.props.h) scheduleRelCascade(next.id)
      dirtyShapes.add(next.id)
      any = true
    }
  }
  // THROTTLE so a panel drag/resize streams to Python (and peers) live, ~16×/s,
  // instead of only landing once the gesture settles (debounce).
  if (any && !flushTimer) flushTimer = setTimeout(flushGeometry, 60)
})

// --- free-form drawing sync + persistence ------------------------------------
// User ink (pen/geo/text/note/lines/arrows the *viewer* draws) is not Python-
// managed: it lives only in the browsers. We relay every local change to Python
// as a `draw` diff; Python folds it into its shadow set, fans it out to the
// other browsers, and replays it to anyone who (re)connects — which is what
// makes drawings both sync to peers and survive a reload. Inbound `draw` frames
// apply as `remote` so they don't echo back. danvas-managed shapes (managedIds)
// are excluded on both directions — those round-trip as shape/arrow frames.
//
// The wire record is our native DrawingRecord plus a `type` alias (the wire
// shape-type string) so Python's DrawingShape / @canvas.on_draw read it the same
// way they read the old build. Python's update() preserves the whole
// record dict and only patches changed fields, so our extra fields survive a
// Python-side mutation and come back intact.
const WIRE_TYPE: Record<string, string> = {
  geo: 'geo',
  text: 'text',
  note: 'note',
  draw: 'draw',
  highlight: 'highlight',
  frame: 'frame',
  line: 'line',
}
function wireType(r: DrawingRecord): string {
  if (r.shapeType === 'line' && r.props?.arrow) return 'arrow'
  return WIRE_TYPE[r.shapeType] || r.shapeType
}
function wireRecord(r: DrawingRecord): any {
  return { ...r, id: r.id, type: wireType(r) }
}
function shapeTypeFromWire(rec: any): DrawingRecord['shapeType'] {
  if (rec.typeName === 'drawing' && rec.shapeType) return rec.shapeType
  const t = rec.type
  if (t === 'arrow') return 'line'
  if (['geo', 'text', 'note', 'draw', 'highlight', 'frame', 'line'].includes(t)) return t
  return 'draw'
}
// Re-hydrate a wire record into a native DrawingRecord. Records that originated
// in another my_danvas browser already carry our fields; ones synthesised by a
// Python update() carry wire-format fields, so we reconstruct from those.
function toDrawingRecord(id: string, rec: any): DrawingRecord {
  if (rec && rec.typeName === 'drawing' && rec.shapeType) return { ...rec, id }
  const isArrow = rec?.type === 'arrow'
  return {
    typeName: 'drawing',
    id,
    shapeType: shapeTypeFromWire(rec || {}),
    x: rec?.x ?? 0,
    y: rec?.y ?? 0,
    rotation: rec?.rotation ?? 0,
    opacity: rec?.opacity ?? 1,
    index: rec?.index ?? nextIndex(),
    props: { ...(rec?.props || {}), ...(isArrow ? { arrow: true } : {}) },
  }
}

// A user drawing = a 'drawing' record that Python doesn't manage.
function isUserDrawing(id: string, rec: any): boolean {
  return !!rec && rec.typeName === 'drawing' && !managedIds.has(id)
}

// Outbound: coalesce a gesture's local changes (one per pointermove for a drag)
// into a single diff on a short trailing timer, then send it. `before` is the
// record state at the start of the window, so the wire `[before, after]` pair
// (and add/remove classification) is computed against the last flush.
const drawDirty = new Map<string, { before: DrawingRecord | undefined }>()
let drawFlushTimer: any = null

store.subscribe((changes, source) => {
  if (source !== 'local') return
  let any = false
  let structural = false // an add/remove (create, delete, undo/redo of either)
  for (const ch of changes) {
    const rec = (ch.next || ch.prev) as any
    if (!isUserDrawing(ch.id, rec)) continue
    if (!drawDirty.has(ch.id)) drawDirty.set(ch.id, { before: ch.prev as DrawingRecord | undefined })
    any = true
    if (ch.op === 'add' || ch.op === 'remove') structural = true
  }
  if (!any) return
  // Structural changes (delete / undo / redo) flush NOW so they can't sit behind
  // the live-draw update throttle. Continuous update drags THROTTLE (~16×/s): a
  // trailing-debounce reset on every pointermove only reached peers once the
  // pointer paused/lifted, so this streams in-progress ink as it's drawn.
  if (structural) {
    if (drawFlushTimer) {
      clearTimeout(drawFlushTimer)
      drawFlushTimer = null
    }
    flushDraw()
  } else if (!drawFlushTimer) {
    drawFlushTimer = setTimeout(flushDraw, 60)
  }
})

function flushDraw(): void {
  drawFlushTimer = null
  const added: any = {}
  const updated: any = {}
  const removed: any = {}
  let any = false
  for (const [id, { before }] of drawDirty) {
    const after = store.peek(id) as DrawingRecord | undefined
    if (!before && after) {
      added[id] = wireRecord(after)
      any = true
    } else if (before && !after) {
      removed[id] = wireRecord(before)
      any = true
    } else if (before && after) {
      if (JSON.stringify(before) !== JSON.stringify(after)) {
        updated[id] = [wireRecord(before), wireRecord(after)]
        any = true
      }
    }
  }
  drawDirty.clear()
  if (any) sendRaw({ type: 'draw', diff: { added, updated, removed } })
}

// Inbound: apply a peer/replay diff as a remote change (no echo). Never let a
// draw frame touch a managed shape id.
function applyDrawDiff(diff: any): void {
  if (!diff) return
  applyRemote(() => {
    for (const [id, rec] of Object.entries(diff.added || {})) {
      if (!managedIds.has(id)) store.put(toDrawingRecord(id, rec))
    }
    for (const [id, pair] of Object.entries(diff.updated || {})) {
      if (managedIds.has(id)) continue
      const next = Array.isArray(pair) ? pair[1] : pair
      store.put(toDrawingRecord(id, next))
    }
    for (const id of Object.keys(diff.removed || {})) {
      if (!managedIds.has(id) && store.has(id)) store.remove(id)
    }
  })
}

// --- snapshot (persistence) --------------------------------------------------
// get_snapshot: hand Python a serialisable bundle of just the user drawings
// (panelIds — the managed shape:<id>s — are excluded). load_snapshot: merge a
// previously-saved bundle back onto the canvas.
function userDrawingsSnapshot(panelIds: string[] = []): any {
  const exclude = new Set(panelIds)
  const drawings: Record<string, any> = {}
  for (const id of store.ids()) {
    if (exclude.has(id) || managedIds.has(id)) continue
    const r = store.peek(id) as DrawingRecord | undefined
    if (r && r.typeName === 'drawing') drawings[id] = wireRecord(r)
  }
  return { pc: 1, drawings }
}

function loadDrawingsSnapshot(data: any): void {
  if (!data) return
  // Our own format is { pc, drawings:{id:rec} }; anything else (e.g. a legacy
  // foreign document) we can't interpret, so ignore it rather than throw.
  const drawings = data.drawings
  if (!drawings || typeof drawings !== 'object') return
  applyRemote(() => {
    for (const [id, rec] of Object.entries(drawings)) {
      if (!managedIds.has(id)) store.put(toDrawingRecord(id, rec))
    }
  })
}

function removeComponent(id: string): void {
  if (id === UI_INSPECTOR_ID) setUiInspectorOpen(false)
  stopCameraCapture(id) // stop any parent-side camera capture for this panel
  stopMicCapture(id) // stop any parent-side mic capture for this panel
  liveHandlers.delete(id)
  liveBuffer.delete(id)
  styleHandlers.delete(id)
  styleBuffer.delete(id)
  const shapeId = createShapeId(id)
  managedIds.delete(shapeId)
  relSpecs.delete(shapeId)
  relDeps.delete(shapeId)
  for (const deps of relDeps.values()) deps.delete(shapeId)
  if (store.has(shapeId)) applyRemote(() => store.remove(shapeId))
}

// --- Python-managed canvas shapes (canvas.geo/text/line/frame/...) -----------
// A code-made canvas.text() ships no w/h (only the browser knows the font
// metrics), so its stored bounding box would stay 0×0 — breaking the selection
// outline, the dimension badge, and hit-testing even though the text renders via
// DrawingLayer's `w || 160` fallback. Measure it the same way the inline editor
// does (page units, matching DRAW_FONT/line-height) and fold the result into the
// record's props so the box hugs the text. auto-width fits both axes; a
// fixed-width box (autoSize:false, explicit w) keeps its w and only grows in h.
const TEXT_FONT: Record<string, number> = { s: 14, m: 20, l: 28, xl: 40 }
function measuredTextProps(props: any): { w: number; h: number } | null {
  if (typeof document === 'undefined') return null
  const fontPx = props.fontSize ?? TEXT_FONT[props.size as string] ?? 20
  const fixedW = props.autoSize === false && props.w ? props.w : undefined
  const m = measuredTextSize(props.text || '', fontPx, fixedW)
  return { w: fixedW ?? Math.max(m.w, fontPx), h: Math.max(m.h, fontPx * 1.3) }
}

// DRAW_FONT prefers 'Inter Variable', which loads async; a measure taken before
// it lands uses the narrower fallback (Segoe UI) and leaves the box short of the
// text. document.fonts.ready alone isn't enough — it resolves immediately if
// nothing has requested Inter yet, so we explicitly force the fetch first, then
// re-measure on the real Inter metrics. Belt-and-suspenders: also wait on ready.
const fontsReady: Promise<unknown> =
  typeof document !== 'undefined' && (document as any).fonts?.load
    ? (document as any).fonts
        .load("16px 'Inter Variable'")
        .catch(() => {})
        .then(() => (document as any).fonts.ready)
        .catch(() => {})
    : Promise.resolve()

function remeasureTextWhenFontsReady(shapeId: string): void {
  fontsReady.then(() => {
    const rec = store.peek(shapeId) as DrawingRecord | undefined
    if (!rec || rec.shapeType !== 'text') return
    const size = measuredTextProps(rec.props)
    if (!size) return
    if (Math.abs((rec.props.w || 0) - size.w) < 0.5 && Math.abs((rec.props.h || 0) - size.h) < 0.5) return
    applyRemote(() => store.patch(shapeId, { props: size }))
  })
}

function createManagedShape(msg: any): void {
  const shapeId = createShapeId(msg.id)
  managedIds.add(shapeId)
  if (store.has(shapeId)) return // reconnect
  const props = { ...(msg.props || {}) }
  if (msg.shapeType === 'text') {
    const size = measuredTextProps(props)
    if (size) Object.assign(props, size)
  }
  const rec: DrawingRecord = {
    typeName: 'drawing',
    id: shapeId,
    shapeType: msg.shapeType,
    x: msg.x ?? 0,
    y: msg.y ?? 0,
    rotation: msg.rotation ?? 0,
    opacity: msg.opacity ?? 1,
    index: nextIndex(),
    props,
  }
  applyRemote(() => store.put(rec))
  if (msg.shapeType === 'text') remeasureTextWhenFontsReady(shapeId)
}

function updateManagedShape(msg: any): void {
  const shapeId = createShapeId(msg.id)
  const existing = store.peek(shapeId) as DrawingRecord | undefined
  if (!existing) return
  const patch: any = {}
  if (msg.x != null) patch.x = msg.x
  if (msg.y != null) patch.y = msg.y
  if (msg.rotation != null) patch.rotation = msg.rotation
  if (msg.opacity != null) patch.opacity = msg.opacity
  if (msg.props && Object.keys(msg.props).length) patch.props = { ...msg.props }
  // A text change re-measures the box (text/size/font may all have moved it),
  // unless this update already pins explicit w/h (e.g. a user resize echo).
  if (existing.shapeType === 'text' && patch.props && patch.props.w == null && patch.props.h == null) {
    const size = measuredTextProps({ ...existing.props, ...patch.props })
    if (size) Object.assign(patch.props, size)
  }
  applyRemote(() => store.patch(shapeId, patch))
  if (existing.shapeType === 'text') remeasureTextWhenFontsReady(shapeId)
}

// A connector arrow bound to two existing shapes (panels or managed shapes). The
// endpoints are stored on the arrow; the renderer reroutes it as they move. Later
// prop changes (color/dash/text/bend) arrive as normal `update` messages.
function createArrow(msg: any): void {
  const arrowId = createShapeId(msg.id)
  managedIds.add(arrowId)
  if (store.has(arrowId)) return
  const rec: ArrowRecord = {
    typeName: 'arrow',
    id: arrowId,
    start: typeof msg.start === 'string' ? createShapeId(msg.start) : undefined,
    end: typeof msg.end === 'string' ? createShapeId(msg.end) : undefined,
    opacity: typeof msg.opacity === 'number' ? msg.opacity : 1,
    index: nextIndex(),
    props: { ...(msg.props || {}) },
  }
  applyRemote(() => store.put(rec))
}

function clearManaged(): void {
  const ids = [...managedIds]
  applyRemote(() => {
    for (const shapeId of ids) if (store.has(shapeId)) store.remove(shapeId)
  })
  managedIds.clear()
  for (const compId of [..._camPanels.keys()]) stopCameraCapture(compId)
  for (const compId of [..._micPanels.keys()]) stopMicCapture(compId)
  liveHandlers.clear()
  liveBuffer.clear()
  styleHandlers.clear()
  styleBuffer.clear()
  resetFlow()
  resetInitialFit()
}

// === content-fit (h="auto" / w="auto") =======================================
// Native ReactHost measures its content and calls this; resize the record to fit
// and report the geometry to Python (same read-back path as a user resize),
// then re-pack whatever layout the panel belongs to (flow / refit group /
// container tree).
export function fitNative(componentId: string, hostEl: HTMLElement, fit: { h?: number; w?: number }): void {
  if (!hostEl || !fit) return
  const shapeId = createShapeId(componentId)
  const shape = store.peek(shapeId)
  if (!shape) return
  const props: any = {}
  const report: any = { type: 'layout', id: componentId }
  if (typeof fit.h === 'number') {
    const overhead = Math.max(0, shape.props.h - hostEl.offsetHeight)
    const h = Math.max(40, Math.ceil(fit.h + overhead))
    if (Math.abs(h - shape.props.h) >= 3) {
      props.h = h
      report.h = h
    }
  }
  if (typeof fit.w === 'number') {
    const overhead = Math.max(0, shape.props.w - hostEl.offsetWidth)
    const w = Math.max(40, Math.ceil(fit.w + overhead))
    if (Math.abs(w - shape.props.w) >= 3) {
      props.w = w
      report.w = w
    }
  }
  if (props.h === undefined && props.w === undefined) return // settled — don't ping-pong
  applyRemote(() => store.patch(shapeId, { props }))
  sendRaw(report)
  // The panel's footprint changed — re-pack whatever layout it belongs to.
  if (props.h !== undefined) scheduleRelCascade(shapeId) // rel chains below it
  if (flowItems.has(shapeId)) scheduleRelayout() // masonry auto-flow
  settleArmedReflows(componentId) // a column.refit() group waiting on it
  autoRepackForPanel(componentId) // any Container tree this panel is in
}

// === live data / style side channels =========================================
const liveHandlers = new Map<string, (data: any) => void>()
const liveBuffer = new Map<string, any>()
const styleHandlers = new Map<string, (th: any) => void>()
const styleBuffer = new Map<string, any>()

export function registerLive(id: string, handler: (data: any) => void): void {
  liveHandlers.set(id, handler)
  if (liveBuffer.has(id)) handler(liveBuffer.get(id))
}
export function unregisterLive(id: string): void {
  liveHandlers.delete(id)
}
export function registerStyle(id: string, handler: (th: any) => void): void {
  styleHandlers.set(id, handler)
  if (styleBuffer.has(id)) handler(styleBuffer.get(id))
}
export function unregisterStyle(id: string): void {
  styleHandlers.delete(id)
}

// === browser -> Python =======================================================
export function sendInput(id: string, payload: any): void {
  sendRaw({ type: 'input', id, payload })
}
export function sendPanelError(id: string, message: any): void {
  sendRaw({ type: 'panel_error', id, message: String(message) })
}
export function sendBinary(compId: string, buffer: ArrayBuffer): void {
  if (!ws) return
  const id = new TextEncoder().encode(compId)
  const frame = new Uint8Array(2 + id.length + buffer.byteLength)
  frame[0] = BIN_INPUT
  frame[1] = id.length
  frame.set(id, 2)
  frame.set(new Uint8Array(buffer), 2 + id.length)
  ws.send(frame.buffer)
}

// === request / response RPC ==================================================
const pendingRequests = new Map<string, { resolve: (v: any) => void; reject: (e: any) => void; timer: any }>()
const REQUEST_NONCE = Math.random().toString(36).slice(2)
let requestSeq = 0

export function requestData(id: string, data: any, timeoutMs = 30000): Promise<any> {
  return new Promise((resolve, reject) => {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      reject(new Error('not connected'))
      return
    }
    const reqId = `${REQUEST_NONCE}:${++requestSeq}`
    const timer = setTimeout(() => {
      pendingRequests.delete(reqId)
      reject(new Error('canvas.request timed out'))
    }, timeoutMs)
    pendingRequests.set(reqId, { resolve, reject, timer })
    sendRaw({ type: 'request', id, reqId, data })
  })
}
function resolveRequest(reqId: string, result: any, error: any): void {
  const pending = pendingRequests.get(reqId)
  if (!pending) return
  clearTimeout(pending.timer)
  pendingRequests.delete(reqId)
  if (error !== undefined && error !== null) pending.reject(new Error(error))
  else pending.resolve(result)
}

// === screenshot (canvas.screenshot / get_image) ==============================
// Render the requested shapes to a PNG and reply by reqId. On any failure reply
// with data:null + an error so Python's waiter wakes instead of timing out.
async function sendImage(reqId: string, shapeIds: string[]): Promise<void> {
  try {
    const { base64, error } = await toImage(shapeIds)
    if (error || !base64) sendRaw({ type: 'image', reqId, data: null, error: error || 'no image' })
    else sendRaw({ type: 'image', reqId, data: base64 })
  } catch (e: any) {
    sendRaw({ type: 'image', reqId, data: null, error: String((e && e.message) || e) })
  }
}

// === shared React assets (canvas.define / canvas.style) ======================
let sharedComponents: Record<string, string> = {}
let sharedVersion = 0
const sharedListeners = new Set<(v: number) => void>()

export function getSharedComponents(): Record<string, string> {
  return sharedComponents
}
export function getSharedVersion(): number {
  return sharedVersion
}
export function subscribeShared(cb: (v: number) => void): () => void {
  sharedListeners.add(cb)
  return () => sharedListeners.delete(cb)
}
function applyShared(msg: any): void {
  sharedComponents = msg.components || {}
  let el = document.getElementById('pc-shared-styles')
  if (!el) {
    el = document.createElement('style')
    el.id = 'pc-shared-styles'
    document.head.appendChild(el)
  }
  el.textContent = msg.styles || ''
  sharedVersion += 1
  for (const cb of sharedListeners) cb(sharedVersion)
}

// === presence ================================================================
let presenceCount = 0
const presenceListeners = new Set<(n: number) => void>()
function setPresence(n: number): void {
  presenceCount = n
  for (const cb of presenceListeners) cb(n)
}
export function subscribePresence(cb: (n: number) => void): () => void {
  presenceListeners.add(cb)
  cb(presenceCount)
  return () => presenceListeners.delete(cb)
}

// === roster ==================================================================
let roster: any[] = []
const rosterListeners = new Set<(vs: any[]) => void>()
function setRoster(vs: any[]): void {
  roster = vs
  // Keep our own identity in step with the server (e.g. after a rename).
  if (myViewer) {
    const mine = vs.find((v) => v.id === myViewer.id)
    if (mine && (mine.name !== myViewer.name || mine.color !== myViewer.color)) setIdentity({ ...myViewer, ...mine })
  }
  for (const cb of rosterListeners) cb(vs)
}
export function subscribeRoster(cb: (vs: any[]) => void): () => void {
  rosterListeners.add(cb)
  cb(roster)
  return () => rosterListeners.delete(cb)
}

// === this viewer's cursor -> Python =========================================
// Off until the server enables it (welcome.cursors). When on, report the pointer
// in page coords (zoom/pan-independent), throttled to one send per frame and
// dead-banded to skip sub-pixel jitter.
let cursorsEnabled = false
let _sentCursors = 0
function setCursorsEnabled(on: boolean): void {
  cursorsEnabled = !!on
}
// Dev-only diagnostics surfaced on window.__danvas (see App).
export const _debug = {
  cursorsEnabled: () => cursorsEnabled,
  sentCursors: () => _sentCursors,
  peerCursors: () => [...peerCursors.values()],
  myViewer: () => myViewer,
  sentUi: () => _sentUi,
}
export function setupCursorReporting(el: HTMLElement): () => void {
  let pending = false
  let lastClient: { x: number; y: number } | null = null
  let lastSent: { x: number; y: number } | null = null
  const flush = () => {
    pending = false
    if (!cursorsEnabled || !lastClient) return
    const p = screenToPage(lastClient)
    if (lastSent && Math.abs(p.x - lastSent.x) < 0.5 && Math.abs(p.y - lastSent.y) < 0.5) return
    lastSent = p
    _sentCursors++
    sendRaw({ type: 'cursor', x: p.x, y: p.y })
  }
  const onMove = (ev: PointerEvent) => {
    if (!cursorsEnabled) return
    lastClient = { x: ev.clientX, y: ev.clientY }
    if (!pending) {
      pending = true
      requestAnimationFrame(flush)
    }
  }
  // Also report on press/release so a plain click (no move) moves the cursor for
  // peers too — not just drags.
  const onDown = (ev: PointerEvent) => {
    if (!cursorsEnabled) return
    lastClient = { x: ev.clientX, y: ev.clientY }
    flush()
  }
  el.addEventListener('pointermove', onMove)
  el.addEventListener('pointerdown', onDown)
  el.addEventListener('pointerup', onDown)
  return () => {
    el.removeEventListener('pointermove', onMove)
    el.removeEventListener('pointerdown', onDown)
    el.removeEventListener('pointerup', onDown)
  }
}

// === peer cursors ============================================================
const peerCursors = new Map<string, any>()
const peerCursorListeners = new Set<(list: any[]) => void>()
function emitPeerCursors(): void {
  const list = [...peerCursors.values()]
  for (const cb of peerCursorListeners) cb(list)
}
function setPeerCursor(c: any): void {
  peerCursors.set(c.id, c)
  emitPeerCursors()
}
function removePeerCursor(id: string): void {
  if (peerCursors.delete(id)) emitPeerCursors()
}
function clearPeerCursors(): void {
  if (peerCursors.size) {
    peerCursors.clear()
    emitPeerCursors()
  }
}
export function subscribePeerCursors(cb: (list: any[]) => void): () => void {
  peerCursorListeners.add(cb)
  cb([...peerCursors.values()])
  return () => peerCursorListeners.delete(cb)
}

// === native UI Inspector toggle =============================================
// Reserved component id Python uses for the ephemeral Inspector panel.
export const UI_INSPECTOR_ID = '__ui_inspector__'
let uiInspector = { enabled: false, open: false }
const uiInspectorListeners = new Set<(s: any) => void>()
function emitUiInspector(): void {
  for (const cb of uiInspectorListeners) cb(uiInspector)
}
function setUiInspectorEnabled(enabled: boolean): void {
  uiInspector = { ...uiInspector, enabled }
  emitUiInspector()
}
function setUiInspectorOpen(open: boolean): void {
  uiInspector = { ...uiInspector, open }
  emitUiInspector()
}
export function subscribeUiInspector(cb: (s: any) => void): () => void {
  uiInspectorListeners.add(cb)
  cb(uiInspector)
  return () => uiInspectorListeners.delete(cb)
}
// === live hosting (the 🌐 button) ===========================================
// Exposure state pushed by the server: current local/LAN/tunnel URLs, which
// action is in flight, and the last error. `enabled` gates the button (the
// server shows it only for a private local bind, like the Inspector).
let hosting: any = { enabled: false, local: null, lan: null, tunnel: null, busy: null, error: null }
const hostingListeners = new Set<(s: any) => void>()
function setHosting(patch: any): void {
  hosting = { ...hosting, ...patch }
  for (const cb of hostingListeners) cb(hosting)
}
export function subscribeHosting(cb: (s: any) => void): () => void {
  hostingListeners.add(cb)
  cb(hosting)
  return () => hostingListeners.delete(cb)
}
export function sendHostAction(
  action: 'host_lan' | 'host_tunnel' | 'host_lan_off' | 'host_tunnel_off',
): void {
  sendRaw({ type: 'ui', action })
}

let _sentUi = 0
export function toggleUiInspector(): void {
  // Send the viewport centre so Python opens the inspector in this viewer's view.
  let center: any = null
  try {
    const c = editor.getViewportPageBounds().center
    center = { x: c.x, y: c.y }
  } catch {
    /* no bounds yet */
  }
  _sentUi++
  sendRaw({ type: 'ui', action: 'toggle_inspector', center })
}

// === built-in graveyard panel ===============================================
let uiGraveyard = { enabled: false, open: false, items: [] as any[] }
const uiGraveyardListeners = new Set<(s: any) => void>()
function emitUiGraveyard(): void {
  for (const cb of uiGraveyardListeners) cb(uiGraveyard)
}
function setGraveyardEnabled(enabled: boolean): void {
  uiGraveyard = { ...uiGraveyard, enabled }
  emitUiGraveyard()
}
function setGraveyardItems(items: any[]): void {
  uiGraveyard = { ...uiGraveyard, items }
  emitUiGraveyard()
  // A graveyarded (deleted) panel must not sit on the canvas. The server's
  // reconnect replay re-registers EVERY component (it doesn't skip graveyarded
  // ones), and a peer's delete reaches us only as this list — so drop any listed
  // component that's currently present. Remote (no echo). It stays in managedIds
  // so a later Restore/re-register brings it back.
  if (items && items.length) {
    applyRemote(() => {
      for (const it of items) {
        const sid = createShapeId(it.id)
        if (store.has(sid)) store.remove(sid)
      }
    })
  }
}
export function subscribeGraveyard(cb: (s: any) => void): () => void {
  uiGraveyardListeners.add(cb)
  cb(uiGraveyard)
  return () => uiGraveyardListeners.delete(cb)
}
export function toggleGraveyard(): void {
  uiGraveyard = { ...uiGraveyard, open: !uiGraveyard.open }
  emitUiGraveyard()
}
export function sendRestore(id: string): void {
  sendRaw({ type: 'restore', id })
}

// === merge (the standing merge server's control plane) ======================
// On a merge view (welcome.mergeHost), the server sends `merge_sources` (the
// roster of this connection's composed sources, each with a stable `sid`, live/
// offline status, and hidden flag) and, when a protected canvas is added,
// `merge_auth_required` / `merge_auth_failed`. The UI (overlays.tsx MergeHostPanel)
// subscribes here and drives it back with merge_add / merge_auth / merge_remove /
// merge_toggle. A plain canvas gets `mergeServer` + `selfUrl` instead, which the
// Merge button (MergeLaunchButton) uses to navigate to the merge view.
let mergeState = {
  isHost: false,
  sources: [] as any[],
  prompts: [] as any[], // { uri, label, error? } for canvases awaiting a password
  server: null as string | null, // a plain canvas's configured merge server URL
  selfUrl: null as string | null, // this canvas's own reachable URL (a source)
  hidden: [] as string[], // sids hidden in this view (mirrors hiddenSourceTags so
  // the merge panel re-renders on toggle — the signal alone drives PanelLayer)
  moveSid: null as string | null, // the source whose origin dot is being positioned
}
const mergeListeners = new Set<(s: any) => void>()
function emitMerge(): void {
  for (const cb of mergeListeners) cb(mergeState)
}
export function subscribeMerge(cb: (s: any) => void): () => void {
  mergeListeners.add(cb)
  cb(mergeState)
  return () => mergeListeners.delete(cb)
}
function setMergeHost(on: boolean): void {
  mergeState = { ...mergeState, isHost: on }
  emitMerge()
}
function setMergeLaunch(server: string | null, selfUrl: string | null): void {
  mergeState = { ...mergeState, server, selfUrl }
  emitMerge()
}
function setMergeSources(list: any[]): void {
  // A source that authenticated now appears here → drop its pending password prompt.
  const uris = new Set(list.map((s) => s.uri))
  // Stop positioning a source that's no longer present.
  const sids = new Set(list.map((s) => s.sid))
  const moveSid = mergeState.moveSid && sids.has(mergeState.moveSid) ? mergeState.moveSid : null
  mergeState = { ...mergeState, sources: list, moveSid, prompts: mergeState.prompts.filter((p) => !uris.has(p.uri)) }
  emitMerge()
  syncMergeUrl(list)
  // Drop hidden flags for sources that are gone, so a re-added source starts shown.
  const cur = hiddenSourceTags()
  const pruned = new Set([...cur].filter((t) => sids.has(t)))
  if (pruned.size !== cur.size) {
    hiddenSourceTags(pruned)
    mergeState = { ...mergeState, hidden: [...pruned] }
    emitMerge()
  }
}

// Per-source visibility in the merged view is PURELY CLIENT-SIDE: hiding a source
// is a render filter only, so its panels stay mounted (a table's sort, a custom
// panel's local React state, etc. survive a hide/show) and keep updating live, and
// the merge server is never told — hide never reaches, or affects, the source
// canvas. The set holds hidden source tags (each source's `sid`); PanelLayer /
// DrawingLayer consult it. A panel id is `s<N>:<compId>`, so its tag is the part
// before the first colon (a plain-canvas id is a bare uuid → never matches).
export const hiddenSourceTags = signal(new Set<string>()) as WriteSignal<Set<string>>
export function setSourceHidden(sid: string, hidden: boolean): void {
  const next = new Set(hiddenSourceTags())
  if (hidden) next.add(sid)
  else next.delete(sid)
  hiddenSourceTags(next) // drives PanelLayer / DrawingLayer (via useValue)
  // Mirror into mergeState with a NEW reference so the merge panel re-renders and
  // its eye icon + the click handler's captured `hidden` stay current.
  mergeState = { ...mergeState, hidden: [...next] }
  emitMerge()
}
export function isSourceTagHidden(tag: string): boolean {
  return hiddenSourceTags().has(tag)
}
export function tagOfComponentId(compId: string): string {
  const i = compId.indexOf(':')
  return i < 0 ? '' : compId.slice(0, i)
}

// Keep the page URL's ?sources= in step with the live composed set, so a REFRESH
// re-seeds the same sources (sources added via the merge panel would otherwise be
// lost — only the initial ?sources= survived a reload). Protected sources still
// re-prompt for their password on reload (passwords are never put in the URL).
function syncMergeUrl(list: any[]): void {
  if (!mergeState.isHost || typeof window === 'undefined' || !window.history?.replaceState) return
  try {
    const url = new URL(window.location.href)
    if (list.length) url.searchParams.set('sources', list.map((s) => s.uri).join(','))
    else url.searchParams.delete('sources')
    window.history.replaceState(null, '', url.toString())
  } catch {
    /* older browser / opaque origin — refresh just won't restore, no worse than before */
  }
}
function addMergeAuthPrompt(uri: string, label: string): void {
  if (mergeState.prompts.some((p) => p.uri === uri)) return
  mergeState = { ...mergeState, prompts: [...mergeState.prompts, { uri, label }] }
  emitMerge()
}
function markMergeAuthFailed(uri: string, label: string): void {
  const has = mergeState.prompts.some((p) => p.uri === uri)
  const prompts = has
    ? mergeState.prompts.map((p) => (p.uri === uri ? { ...p, error: true } : p))
    : [...mergeState.prompts, { uri, label, error: true }]
  mergeState = { ...mergeState, prompts }
  emitMerge()
}
export function mergeAdd(uri: string): void {
  sendRaw({ type: 'merge_add', uri })
}
export function mergeAuth(uri: string, password: string): void {
  sendRaw({ type: 'merge_auth', uri, password })
}
export function mergeRemove(sid: string): void {
  sendRaw({ type: 'merge_remove', sid })
}
// Origin positioning: setMergeMove picks which source's origin dot is draggable
// (null = none); mergeOffset sends its new origin (canvas coords) to the hub, which
// translates that source's whole block for every viewer (the source is untouched).
export function setMergeMove(sid: string | null): void {
  mergeState = { ...mergeState, moveSid: sid }
  emitMerge()
}
export function mergeOffset(sid: string, x: number, y: number): void {
  sendRaw({ type: 'merge_offset', sid, x, y })
}

// === auth (sign-out on a password-protected canvas) =========================
let authEnabled = false
const authListeners = new Set<(e: boolean) => void>()
function setAuthEnabled(enabled: boolean): void {
  authEnabled = enabled
  for (const cb of authListeners) cb(enabled)
}
export function subscribeAuth(cb: (e: boolean) => void): () => void {
  authListeners.add(cb)
  cb(authEnabled)
  return () => authListeners.delete(cb)
}
export function signOut(): void {
  window.location.href = '/__logout__'
}

// === view config subscription (for App's hideUi flag) =======================
const viewConfigListeners = new Set<(v: any) => void>()
export function subscribeViewConfig(cb: (v: any) => void): () => void {
  viewConfigListeners.add(cb)
  cb(viewConfig)
  return () => viewConfigListeners.delete(cb)
}
function emitViewConfig(): void {
  for (const cb of viewConfigListeners) cb(viewConfig)
}

// === viewer identity =========================================================
let myViewer: any = null
const identityListeners = new Set<(v: any) => void>()
function setIdentity(v: any): void {
  myViewer = v
  persistIdentity(v)
  for (const cb of identityListeners) cb(v)
}
export function subscribeIdentity(cb: (v: any) => void): () => void {
  identityListeners.add(cb)
  cb(myViewer)
  return () => identityListeners.delete(cb)
}

// === chat ====================================================================
// The server stamps each line with the sender's identity, broadcasts it to every
// viewer (sender included), and replays recent history on connect. We keep a local
// log so a Chat panel that mounts later can backfill via getChatLog(), and notify
// live subscribers as new lines arrive. Deduped by msgId so the reconnect replay
// can't double-post.
const chatLog: any[] = []
const chatSubs = new Set<(e: any) => void>()
const CHAT_MAX = 200
function ingestChat(entry: any): void {
  if (!entry || entry.msgId == null) return
  if (chatLog.some((e) => e.msgId === entry.msgId)) return
  chatLog.push(entry)
  if (chatLog.length > CHAT_MAX) chatLog.splice(0, chatLog.length - CHAT_MAX)
  for (const cb of chatSubs) cb(entry)
}
function resetChat(): void {
  chatLog.length = 0
}
export function subscribeChat(cb: (e: any) => void): () => void {
  chatSubs.add(cb)
  return () => chatSubs.delete(cb)
}
export function getChatLog(): any[] {
  return chatLog.slice()
}
export function sendChat(text: string): void {
  sendRaw({ type: 'chat', text })
}
export function setMyName(name: string): void {
  sendRaw({ type: 'set_name', name })
  // The server doesn't echo set_name, so reflect the new name locally (keeps the
  // "(you)" label + name field current; future chat lines carry it from the server).
  if (myViewer) setIdentity({ ...myViewer, name })
}

// === view / navigation config (serve(view=...) / set_view) ===================
// The server sends a `view` dict in `welcome` (initial camera, zoom limits,
// lock, read-only/grid) and again on a live set_view. Camera is applied once
// from `welcome` (so a viewer who panned away isn't yanked back on a reconnect
// replay) but every time on a live change.
let viewConfig: any = null
let initialCameraApplied = false

function hasExplicitCamera(): boolean {
  return (
    !!viewConfig &&
    (typeof viewConfig.x === 'number' || typeof viewConfig.y === 'number' || typeof viewConfig.zoom === 'number')
  )
}

function applyViewOptions(v: any): void {
  if (!v) return
  const inst = store.instance()
  const next = { ...inst }
  if (typeof v.read_only === 'boolean') next.readOnly = v.read_only
  if (typeof v.grid === 'boolean') next.gridOn = v.grid
  if (typeof v.locked === 'boolean') next.lockedCamera = v.locked
  if (typeof v.min_zoom === 'number' || typeof v.max_zoom === 'number') {
    next.zoomLimits = {
      min: typeof v.min_zoom === 'number' ? v.min_zoom : inst.zoomLimits.min,
      max: typeof v.max_zoom === 'number' ? v.max_zoom : inst.zoomLimits.max,
    }
  }
  store.instance(next)
  // Navigation mode (scroll_y / scroll_x / free): constrain pan axis + lock zoom.
  if (v.navigation) setNavigationMode(v.navigation.mode, v.navigation.zoom ?? 1)
}

// Initial config from `welcome`.
function setViewConfig(view: any): void {
  viewConfig = view
  emitViewConfig()
  if (!view) return
  applyViewOptions(view)
  if (!initialCameraApplied) {
    applyCameraFrom(view)
    initialCameraApplied = true
  }
}

// A live `view` change from Python (Canvas.set_view).
function applyLiveView(delta: any): void {
  if (!delta) return
  viewConfig = { ...(viewConfig || {}), ...delta }
  emitViewConfig()
  applyViewOptions(delta)
  applyCameraFrom(delta)
}

// Re-exported for ReactHost (canvas.setView).
export { applyCameraFrom }

// === Custom-panel iframe support (parent side) ===============================
// The iframe's window.canvas shim (injected Python-side, custom.py) posts these
// messages up to the parent; we relay them. push() data goes the other way, into
// the iframe, via CustomView's registerLive handler.

// --- auto-height (h="auto" / w="auto" on a Custom panel) ---------------------
function fitFromIframe(sourceWin: any, fit: any): void {
  const iframe = [...document.querySelectorAll('iframe')].find(
    (f) => (f as HTMLIFrameElement).contentWindow === sourceWin,
  ) as HTMLIFrameElement | undefined
  if (!iframe) return
  const shapeId = createShapeId(fit.id)
  const shape = store.peek(shapeId) as PanelRecord | undefined
  if (!shape) return
  const props: any = {}
  const report: any = { type: 'layout', id: fit.id }
  if (typeof fit.h === 'number') {
    const overhead = Math.max(0, shape.props.h - iframe.offsetHeight)
    const h = Math.max(40, Math.ceil(fit.h + overhead))
    if (Math.abs(h - shape.props.h) >= 3) {
      props.h = h
      report.h = h
    }
  }
  if (typeof fit.w === 'number') {
    const overhead = Math.max(0, shape.props.w - iframe.offsetWidth)
    const w = Math.max(40, Math.ceil(fit.w + overhead))
    if (Math.abs(w - shape.props.w) >= 3) {
      props.w = w
      report.w = w
    }
  }
  if (props.h === undefined && props.w === undefined) return // settled — don't ping-pong
  applyRemote(() => store.patch(shapeId, { props }))
  sendRaw(report)
  if (props.h !== undefined) scheduleRelCascade(shapeId)
  if (flowItems.has(shapeId)) scheduleRelayout()
  settleArmedReflows(fit.id)
  autoRepackForPanel(fit.id)
}

// --- ctrl/cmd+wheel inside an iframe: zoom the canvas, not the browser --------
// Coordinates from inside an iframe are in the iframe's OWN css px, but the iframe
// is rendered scaled by the camera zoom `z` in the parent — so a cursor offset of
// `n` iframe-px sits at `n * z` screen px from the iframe's top-left, and a drag of
// `dx` iframe-px equals `dx` page units (the iframe's css px ARE page units, since
// its width is the panel's page-unit width). Convert accordingly.
function zoomFromIframe(sourceWin: any, w: any): void {
  const iframe = [...document.querySelectorAll('iframe')].find(
    (f) => (f as HTMLIFrameElement).contentWindow === sourceWin,
  ) as HTMLIFrameElement | undefined
  if (!iframe) return
  const rect = iframe.getBoundingClientRect()
  const z = store.camera().z
  zoomCanvasAtClient(rect.left + w.x * z, rect.top + w.y * z, w.d)
}

// A right-drag inside a Custom iframe pans the canvas (the parent never sees those
// events). The iframe sends screen-space cursor deltas (immune to the panel moving
// under the cursor mid-pan, which is what made it "double-vision" oscillate), so we
// just convert px→page units (÷z) and shift the camera — exactly like a bare-canvas
// right-drag.
function panFromIframe(p: any): void {
  const cam = store.camera()
  setCamera({ x: cam.x + (p.dx || 0) / cam.z, y: cam.y + (p.dy || 0) / cam.z, z: cam.z })
}

// A right-click (no drag) inside a Custom iframe opens the canvas context menu over
// that panel — passing the iframe as the target so it resolves to its panel record.
function menuFromIframe(sourceWin: any, m: any): void {
  const iframe = [...document.querySelectorAll('iframe')].find(
    (f) => (f as HTMLIFrameElement).contentWindow === sourceWin,
  ) as HTMLIFrameElement | undefined
  if (!iframe) return
  const rect = iframe.getBoundingClientRect()
  const z = store.camera().z
  openContextMenuAt(rect.left + (m.x || 0) * z, rect.top + (m.y || 0) * z, iframe)
}

// --- parent-side camera capture (canvas.requestCamera from a Custom iframe) ---
// getUserMedia is blocked inside a sandboxed iframe; the parent runs it and
// relays each JPEG frame to Python (BIN_INPUT, via sendBinary) and back into the
// iframe (liveHandlers -> canvas.onPush). One shared MediaStream across panels.
let _camStream: MediaStream | null = null
let _camVideo: HTMLVideoElement | null = null
let _camPending: Promise<MediaStream> | null = null
const _camPanels = new Map<string, any>()

async function startCameraCapture(compId: string, opts: any): Promise<void> {
  if (_camPanels.has(compId)) return
  const fps = typeof opts.fps === 'number' && opts.fps > 0 ? opts.fps : 0
  const minInterval = fps > 0 ? 1000 / fps : 0
  const quality = typeof opts.quality === 'number' ? opts.quality : 0.7
  const width = typeof opts.width === 'number' ? opts.width : 320
  const height = typeof opts.height === 'number' ? opts.height : 240
  try {
    if (!_camStream && !_camPending) {
      _camPending = navigator.mediaDevices.getUserMedia({ video: { width, height } })
    }
    if (_camPending) {
      const stream = await _camPending
      if (!_camStream) {
        _camStream = stream
        _camPending = null
        _camVideo = document.createElement('video')
        _camVideo.srcObject = _camStream
        _camVideo.autoplay = true
        _camVideo.muted = true
        _camVideo.playsInline = true
        _camVideo.play().catch(() => {})
      }
    }
    const cap = document.createElement('canvas')
    cap.width = width
    cap.height = height
    const ctx = cap.getContext('2d')!
    const entry: any = { rafId: null, lastCapture: 0, pending: false }
    _camPanels.set(compId, entry)
    const capture = () => {
      if (!_camPanels.has(compId)) return
      entry.rafId = requestAnimationFrame(capture)
      if (!_camVideo || _camVideo.readyState < 2 || entry.pending) return
      const now = performance.now()
      if (minInterval > 0 && now - entry.lastCapture < minInterval) return
      entry.lastCapture = now
      entry.pending = true
      ctx.drawImage(_camVideo, 0, 0, width, height)
      cap.toBlob(
        (blob) => {
          entry.pending = false
          if (!blob || !_camPanels.has(compId)) return
          blob.arrayBuffer().then((buf) => {
            if (!_camPanels.has(compId)) return
            sendBinary(compId, buf) // up to Python (copies internally)
            const handler = liveHandlers.get(compId)
            if (handler) handler(buf) // down into the iframe — transfers buf, call last
          })
        },
        'image/jpeg',
        quality,
      )
    }
    entry.rafId = requestAnimationFrame(capture)
  } catch (err: any) {
    console.warn('[danvas] camera unavailable for panel', compId, '—', err && err.message)
  }
}

function stopCameraCapture(compId: string): void {
  const entry = _camPanels.get(compId)
  if (!entry) return
  if (entry.rafId != null) cancelAnimationFrame(entry.rafId)
  _camPanels.delete(compId)
  if (_camPanels.size === 0 && _camStream) {
    _camStream.getTracks().forEach((t) => t.stop())
    _camStream = null
    _camVideo = null
  }
}

// --- parent-side microphone capture (canvas.requestMicrophone) ---------------
const _micPanels = new Map<string, any>()

async function startMicCapture(compId: string, opts: any): Promise<void> {
  if (_micPanels.has(compId)) return
  const bufferSize = 4096
  try {
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true, video: false })
    const ctx = new AudioContext()
    await ctx.resume()
    sendInput(compId, { event: 'mic_start', sampleRate: ctx.sampleRate, channels: 1 })
    const source = ctx.createMediaStreamSource(stream)
    const processor = ctx.createScriptProcessor(bufferSize, 1, 1)
    processor.onaudioprocess = (e) => {
      if (!_micPanels.has(compId)) return
      const float32 = e.inputBuffer.getChannelData(0)
      const int16 = new Int16Array(float32.length)
      for (let i = 0; i < float32.length; i++) {
        int16[i] = Math.max(-32768, Math.min(32767, float32[i] * 32767))
      }
      sendBinary(compId, int16.buffer)
      const handler = liveHandlers.get(compId)
      if (handler) handler(int16.buffer)
    }
    const silencer = ctx.createGain()
    silencer.gain.value = 0
    source.connect(processor)
    processor.connect(silencer)
    silencer.connect(ctx.destination)
    _micPanels.set(compId, { stream, ctx, source, processor, silencer })
  } catch (err: any) {
    console.warn('[danvas] microphone unavailable for panel', compId, '—', err && err.message)
  }
}

function stopMicCapture(compId: string): void {
  const entry = _micPanels.get(compId)
  if (!entry) return
  try {
    entry.source.disconnect()
    entry.processor.disconnect()
    entry.silencer.disconnect()
    entry.ctx.close()
  } catch {
    /* already torn down */
  }
  entry.stream.getTracks().forEach((t: MediaStreamTrack) => t.stop())
  _micPanels.delete(compId)
}

// Custom-iframe viewport subscribers (canvas.viewport): each subscribed frame is
// pushed the canvas centre + zoom on every camera move — the postMessage twin of
// the React panel's viewport(). A frame that's gone (panel unmounted) is pruned
// the first time posting to it throws.
const _viewportSubs = new Set<Window>()
function _viewportValue() {
  const c = editor.getViewportPageBounds().center
  return { x: Math.round(c.x), y: Math.round(c.y), zoom: editor.getZoomLevel() }
}
function _pushViewport(win: Window, v: any) {
  try {
    win.postMessage({ __danvas_viewport: v }, '*')
  } catch {
    _viewportSubs.delete(win)
  }
}
function addViewportSub(win: Window | null) {
  if (!win) return
  _viewportSubs.add(win)
  _pushViewport(win, _viewportValue()) // prime now, like React's viewport()
}
function removeViewportSub(win: Window | null) {
  if (win) _viewportSubs.delete(win)
}

// Custom-iframe chat subscribers (canvas.chat): an iframe that subscribed to the
// shared room gets each new line / identity change forwarded over postMessage.
// Each channel keeps the live unsubscribe keyed by frame, so 'unsub' tears it down
// and a gone frame is pruned the first time posting throws.
const _chatSubs = new Map<Window, () => void>()
const _idSubs = new Map<Window, () => void>()
function _chatPost(win: Window, msg: any, drop: () => void) {
  try {
    win.postMessage(msg, '*')
  } catch {
    drop()
  }
}
if (typeof window !== 'undefined') {
  // Push the live viewport to every subscribed Custom iframe on each camera move.
  effect(() => {
    store.camera() // track pan/zoom
    if (!_viewportSubs.size) return
    const v = _viewportValue()
    for (const win of Array.from(_viewportSubs)) _pushViewport(win, v)
  })
}

// --- the global relay: messages from every Custom iframe --------------------
if (typeof window !== 'undefined') {
  // Non-iframe Custom usage (top-level page).
  ;(window as any).canvas = { send: (data: any) => sendInput('__custom__', data) }

  window.addEventListener('message', (e: MessageEvent) => {
    const d = e.data as any
    if (!d || typeof d !== 'object') return
    if (d.__danvas_wheel) {
      zoomFromIframe(e.source, d.__danvas_wheel)
    } else if (d.__danvas_pan) {
      panFromIframe(d.__danvas_pan)
    } else if (d.__danvas_menu) {
      menuFromIframe(e.source, d.__danvas_menu)
    } else if (d.__danvas_key) {
      // A canvas tool shortcut pressed while a Custom iframe had focus — replay it
      // on the parent window so the engine's keydown handler (interaction.ts) runs.
      window.dispatchEvent(new KeyboardEvent('keydown', { key: d.__danvas_key.key, bubbles: true }))
    } else if (d.__danvas_fit) {
      fitFromIframe(e.source, d.__danvas_fit)
    } else if (d.__danvas) {
      sendInput(d.__danvas, d.data)
    } else if (d.__danvas_binary && d.data instanceof ArrayBuffer) {
      sendBinary(d.__danvas_binary, d.data)
    } else if (d.__danvas_camera) {
      if (d.action === 'start') startCameraCapture(d.__danvas_camera, d.opts || {})
      else if (d.action === 'stop') stopCameraCapture(d.__danvas_camera)
    } else if (d.__danvas_mic) {
      if (d.action === 'start') startMicCapture(d.__danvas_mic, d.opts || {})
      else if (d.action === 'stop') stopMicCapture(d.__danvas_mic)
    } else if (d.__danvas_request) {
      // canvas.request(data) from a Custom iframe: run the matching @on_request
      // handler (same wire path as a React panel) and post the result back to the
      // requesting frame, matched by reqId.
      const src = e.source as Window | null
      const reqId = d.reqId
      requestData(d.__danvas_request, d.data)
        .then((r) => src && src.postMessage({ __danvas_response: reqId, ok: true, data: r }, '*'))
        .catch((err) => src && src.postMessage({ __danvas_response: reqId, ok: false, error: String((err && err.message) || err) }, '*'))
    } else if (d.__danvas_setview) {
      // canvas.setView({x,y,zoom}) — pan/zoom the canvas to centre a point.
      applyCameraFrom(d.__danvas_setview || {})
    } else if (d.__danvas_viewport) {
      // canvas.viewport(cb) subscribe/unsubscribe (the value is pushed back by the
      // camera effect above).
      if (d.action === 'sub') addViewportSub(e.source as Window | null)
      else if (d.action === 'unsub') removeViewportSub(e.source as Window | null)
    } else if (d.__danvas_chat) {
      // canvas.chat from a Custom iframe — the shared-room twin of the React
      // panel's chat handle.
      const win = e.source as Window | null
      const a = d.__danvas_chat.action
      if (a === 'send') sendChat(String(d.__danvas_chat.text ?? ''))
      else if (a === 'setName') setMyName(String(d.__danvas_chat.name ?? ''))
      else if (a === 'history' && win) _chatPost(win, { __danvas_chat_reply: d.__danvas_chat.reqId, log: getChatLog() }, () => {})
      else if (a === 'sub' && win) {
        if (!_chatSubs.has(win)) {
          _chatSubs.set(win, subscribeChat((entry) => _chatPost(win, { __danvas_chat_msg: entry }, () => {
            const u = _chatSubs.get(win); if (u) { u(); _chatSubs.delete(win) }
          })))
        }
      } else if (a === 'unsub' && win) {
        const u = _chatSubs.get(win); if (u) { u(); _chatSubs.delete(win) }
      } else if (a === 'idsub' && win) {
        if (!_idSubs.has(win)) {
          _idSubs.set(win, subscribeIdentity((v) => _chatPost(win, { __danvas_chat_identity: v }, () => {
            const u = _idSubs.get(win); if (u) { u(); _idSubs.delete(win) }
          })))
        }
      } else if (a === 'idunsub' && win) {
        const u = _idSubs.get(win); if (u) { u(); _idSubs.delete(win) }
      }
    }
  })
}

// re-export so panels can map page<->screen without importing the engine
export { screenToPage }
