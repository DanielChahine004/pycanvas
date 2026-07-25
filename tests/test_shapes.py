"""Unit tests for managed tldraw shapes and ephemeral drawing observation.

Covers:
- Shape construction, props, and register_message format
- BaseShape.update() / move() / remove() prop routing
- Property setters (color, fill, dash, size, text, w, h, label)
- _segments_from_points: flat tuples, tuples with pressure, segment dicts
- _line_points: relative-to-first-point encoding
- DrawingShape: update() and remove() broadcast draw diffs
- Bridge integration: add_shape, remove_shape, replay, draw-tap wiring
- Canvas factories: geo, text, note, draw, highlight, line, frame
- Canvas.drawings snapshot reflects bridge._drawings
- Canvas.on_draw / off_draw registration and dispatch
- Name eviction (re-creating under the same name replaces the old shape)
"""

import json
import threading

import pytest
import danvas
from danvas.bridge import Bridge
from danvas.shapes import (
    BaseShape, Geo, Text, Note, Draw, Highlight, Line, Frame, DrawingShape,
    _segments_from_points, _line_points, _extract_rich_text,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeBridge:
    """Minimal stand-in for Bridge that captures broadcast calls."""

    def __init__(self):
        self.broadcasts = []
        self._drawings = {}
        self._loop = object()  # non-None → broadcast won't early-exit

    def broadcast(self, msg, **_kw):
        self.broadcasts.append(dict(msg))

    def remove_shape(self, shape_id):
        self.broadcast({"type": "remove", "id": shape_id})

    def _notify_mutation(self):
        pass


def _last(bridge):
    return bridge.broadcasts[-1] if bridge.broadcasts else None


# ---------------------------------------------------------------------------
# Construction and register_message
# ---------------------------------------------------------------------------

def test_geo_register_message():
    s = Geo("abc", 10, 20, w=200, h=150, geo="ellipse", color="blue")
    msg = s.register_message()
    assert msg["type"] == "shape"
    assert msg["shapeType"] == "geo"
    assert msg["x"] == 10.0
    assert msg["y"] == 20.0
    assert msg["props"]["geo"] == "ellipse"
    assert msg["props"]["w"] == 200.0
    assert msg["props"]["h"] == 150.0
    assert msg["props"]["color"] == "blue"


def test_text_register_message():
    s = Text("t1", 0, 0, text="Hello", font="sans")
    msg = s.register_message()
    assert msg["shapeType"] == "text"
    assert msg["props"]["text"] == "Hello"
    assert msg["props"]["font"] == "sans"


def test_note_register_message():
    s = Note("n1", 50, 60, text="Sticky", color="yellow")
    msg = s.register_message()
    assert msg["shapeType"] == "note"
    assert msg["props"]["text"] == "Sticky"
    assert msg["props"]["color"] == "yellow"


def test_frame_register_message():
    s = Frame("f1", 0, 0, w=400, h=300, label="My Frame")
    msg = s.register_message()
    assert msg["shapeType"] == "frame"
    assert msg["props"]["w"] == 400.0
    assert msg["props"]["h"] == 300.0
    assert msg["props"]["name"] == "My Frame"  # tldraw uses 'name' for the label


def test_draw_register_message():
    segs = [{"type": "free", "points": [{"x": 0, "y": 0, "z": 0.5}]}]
    s = Draw("d1", 10, 20, segs)
    msg = s.register_message()
    assert msg["shapeType"] == "draw"
    assert msg["props"]["isComplete"] is True
    assert msg["props"]["segments"] == segs


def test_highlight_register_message():
    segs = [{"type": "free", "points": [{"x": 0, "y": 0, "z": 0.5}]}]
    s = Highlight("h1", 0, 0, segs)
    msg = s.register_message()
    assert msg["shapeType"] == "highlight"
    assert msg["props"]["isComplete"] is True


def test_line_register_message():
    pts = _line_points([(0, 0), (100, 50)])
    s = Line("l1", 0, 0, pts, spline="cubic")
    msg = s.register_message()
    assert msg["shapeType"] == "line"
    assert msg["props"]["spline"] == "cubic"
    assert "a1" in msg["props"]["points"]
    assert "a2" in msg["props"]["points"]


def test_rotation_and_opacity_in_register():
    s = Geo("g2", 0, 0, rotation=45.0, opacity=0.5)
    msg = s.register_message()
    assert msg["rotation"] == 45.0
    assert msg["opacity"] == 0.5


# ---------------------------------------------------------------------------
# BaseShape.update() / move() / property setters
# ---------------------------------------------------------------------------

def test_update_top_level_fields():
    s = Geo("g", 0, 0)
    b = _FakeBridge()
    s._bridge = b
    s.update(x=100, y=200)
    assert s.x == 100.0
    assert s.y == 200.0
    msg = _last(b)
    assert msg["type"] == "shape_update"
    assert msg["x"] == 100.0
    assert msg["y"] == 200.0
    assert "props" not in msg  # no prop changes


def test_update_shape_props():
    s = Geo("g", 0, 0)
    b = _FakeBridge()
    s._bridge = b
    s.update(color="red", fill="solid")
    assert s._props["color"] == "red"
    assert s._props["fill"] == "solid"
    msg = _last(b)
    assert msg["props"] == {"color": "red", "fill": "solid"}


def test_update_mixed_top_and_props():
    s = Geo("g", 0, 0)
    b = _FakeBridge()
    s._bridge = b
    s.update(x=50, opacity=0.8, color="blue")
    assert s.x == 50.0
    assert s.opacity == 0.8
    msg = _last(b)
    assert msg["x"] == 50.0
    assert msg["opacity"] == 0.8
    assert msg["props"] == {"color": "blue"}


def test_move_sends_shape_update():
    s = Geo("g", 10, 20)
    b = _FakeBridge()
    s._bridge = b
    s.move(x=99)
    assert s.x == 99.0
    assert s.y == 20.0  # unchanged
    msg = _last(b)
    assert msg == {"type": "shape_update", "id": "g", "x": 99.0}


def test_move_no_op_when_nothing_passed():
    s = Geo("g", 10, 20)
    b = _FakeBridge()
    s._bridge = b
    s.move()  # no-op
    assert b.broadcasts == []


def test_color_setter():
    s = Geo("g", 0, 0)
    b = _FakeBridge()
    s._bridge = b
    s.color = "green"
    assert s.color == "green"
    assert _last(b)["props"] == {"color": "green"}


def test_fill_setter():
    s = Geo("g", 0, 0, fill="none")
    b = _FakeBridge()
    s._bridge = b
    s.fill = "solid"
    assert _last(b)["props"] == {"fill": "solid"}


def test_size_setter():
    s = Geo("g", 0, 0)
    b = _FakeBridge()
    s._bridge = b
    s.size = "xl"
    assert _last(b)["props"] == {"size": "xl"}


def test_geo_text_setter():
    s = Geo("g", 0, 0)
    b = _FakeBridge()
    s._bridge = b
    s.text = "hello"
    assert s.text == "hello"
    assert _last(b)["props"] == {"text": "hello"}


def test_geo_wh_setters():
    s = Geo("g", 0, 0, w=100, h=80)
    b = _FakeBridge()
    s._bridge = b
    s.w = 200
    assert s.w == 200.0
    s.h = 300
    assert s.h == 300.0


def test_note_text_setter():
    s = Note("n", 0, 0, text="old")
    b = _FakeBridge()
    s._bridge = b
    s.text = "new"
    assert s.text == "new"


def test_frame_label_and_size_setters():
    s = Frame("f", 0, 0, w=400, h=300, label="A")
    b = _FakeBridge()
    s._bridge = b
    s.label = "B"
    assert s.label == "B"
    assert _last(b)["props"] == {"name": "B"}
    s.w = 500
    assert s.w == 500.0
    s.h = 400
    assert s.h == 400.0


def test_update_without_bridge_does_not_raise():
    s = Geo("g", 0, 0)
    s.update(color="red")  # _bridge is None — no exception


def test_remove_calls_bridge():
    s = Geo("g", 0, 0, name="box")
    b = _FakeBridge()
    s._bridge = b
    s.remove()
    assert _last(b) == {"type": "remove", "id": "g"}


# ---------------------------------------------------------------------------
# Point helpers
# ---------------------------------------------------------------------------

def test_segments_from_flat_tuples():
    ox, oy, segs = _segments_from_points([(10, 20), (30, 25), (50, 15)])
    assert ox == 10.0
    assert oy == 15.0
    pts = segs[0]["points"]
    assert pts[0] == {"x": 0.0, "y": 5.0, "z": 0.5}
    assert pts[1] == {"x": 20.0, "y": 10.0, "z": 0.5}
    assert pts[2] == {"x": 40.0, "y": 0.0, "z": 0.5}


def test_segments_from_tuples_with_pressure():
    ox, oy, segs = _segments_from_points([(0, 0, 0.8), (10, 10, 0.9)])
    pts = segs[0]["points"]
    assert pts[0]["z"] == pytest.approx(0.8)
    assert pts[1]["z"] == pytest.approx(0.9)


def test_segments_from_existing_segment_dicts():
    raw = [{"type": "free", "points": [{"x": 5, "y": 10, "z": 0.5},
                                        {"x": 15, "y": 20, "z": 0.5}]}]
    ox, oy, segs = _segments_from_points(raw)
    assert ox == 5.0
    assert oy == 10.0
    assert segs[0]["points"][0] == {"x": 0.0, "y": 0.0, "z": 0.5}
    assert segs[0]["points"][1] == {"x": 10.0, "y": 10.0, "z": 0.5}


def test_segments_empty():
    ox, oy, segs = _segments_from_points([])
    assert ox == 0.0 and oy == 0.0
    assert segs[0]["points"] == []


def test_line_points_relative_encoding():
    pts = _line_points([(100, 200), (150, 250), (200, 200)])
    assert pts["a1"] == {"id": "a1", "index": "a1", "x": 0.0, "y": 0.0}
    assert pts["a2"] == {"id": "a2", "index": "a2", "x": 50.0, "y": 50.0}
    assert pts["a3"] == {"id": "a3", "index": "a3", "x": 100.0, "y": 0.0}


def test_line_points_empty():
    assert _line_points([]) == {}


# ---------------------------------------------------------------------------
# DrawingShape — ephemeral shape wrappers
# ---------------------------------------------------------------------------

def _fake_record(sid="shape:abc", shape_type="geo", x=10, y=20, color="black"):
    return {
        "id": sid,
        "type": shape_type,
        "typeName": "shape",
        "x": x,
        "y": y,
        "rotation": 0,
        "opacity": 1,
        "props": {"color": color, "geo": "rectangle"},
    }


def test_drawing_shape_attributes():
    rec = _fake_record()
    s = DrawingShape(rec)
    assert s.id == "shape:abc"
    assert s.type == "geo"
    assert s.x == 10.0
    assert s.y == 20.0
    assert s.color == "black"


def test_drawing_shape_update_top_level():
    b = _FakeBridge()
    b._drawings["shape:abc"] = _fake_record()
    s = DrawingShape(b._drawings["shape:abc"], b)
    s.update(x=99, y=88)
    assert s.x == 99.0
    assert s.y == 88.0
    assert b._drawings["shape:abc"]["x"] == 99.0
    msg = _last(b)
    assert msg["type"] == "draw"
    diff = msg["diff"]
    assert "shape:abc" in diff["updated"]
    pair = diff["updated"]["shape:abc"]
    assert pair[1]["x"] == 99.0


def test_drawing_shape_update_prop():
    b = _FakeBridge()
    b._drawings["shape:abc"] = _fake_record()
    s = DrawingShape(b._drawings["shape:abc"], b)
    s.update(color="red")
    assert s.props["color"] == "red"
    assert b._drawings["shape:abc"]["props"]["color"] == "red"


def test_drawing_shape_remove():
    b = _FakeBridge()
    b._drawings["shape:abc"] = _fake_record()
    s = DrawingShape(b._drawings["shape:abc"], b)
    s.remove()
    assert "shape:abc" not in b._drawings
    msg = _last(b)
    diff = msg["diff"]
    assert "shape:abc" in diff["removed"]


def test_drawing_shape_remove_noop_when_not_in_drawings():
    b = _FakeBridge()
    s = DrawingShape(_fake_record(), b)
    s.remove()  # _drawings is empty — should not raise


def test_drawing_shape_update_without_bridge():
    s = DrawingShape(_fake_record())
    s.update(x=5)  # _bridge is None — should not raise


def test_extract_rich_text():
    rt = {
        "type": "doc",
        "content": [
            {"type": "paragraph", "content": [{"type": "text", "text": "hello"}]},
            {"type": "paragraph", "content": [{"type": "text", "text": "world"}]},
        ],
    }
    assert _extract_rich_text(rt) == "hello\nworld"


def test_drawing_shape_text_from_richtext():
    rec = _fake_record()
    rec["props"]["richText"] = {
        "type": "doc",
        "content": [{"type": "paragraph", "content": [{"type": "text", "text": "hi"}]}],
    }
    s = DrawingShape(rec)
    assert s.text == "hi"


# ---------------------------------------------------------------------------
# Bridge integration
# ---------------------------------------------------------------------------

def test_bridge_add_shape_stores_and_sets_bridge():
    b = Bridge()
    b._loop = object()
    sent = []
    b._emit = lambda targets, msg: sent.append(msg)
    s = Geo("g1", 10, 20, name="box")
    b.add_shape(s)
    assert "g1" in b._shapes
    assert s._bridge is b


def test_bridge_remove_shape_purges():
    b = Bridge()
    b._loop = object()
    sent = []
    b._emit = lambda targets, msg: sent.append(msg)
    s = Geo("g1", 0, 0, name="box")
    b.add_shape(s)
    b.remove_shape("g1")
    assert "g1" not in b._shapes
    assert any(m.get("type") == "remove" and m.get("id") == "g1" for m in sent)


def test_bridge_panel_shape_ids_includes_shapes():
    b = Bridge()
    s = Geo("shapeid", 0, 0, name="box")
    b._shapes["shapeid"] = s
    ids = b._panel_shape_ids()
    assert "shape:shapeid" in ids


def test_bridge_draw_taps_fire_on_apply_draw():
    b = Bridge()
    b._loop = object()
    b._emit = lambda targets, msg: None

    events = []
    b.add_draw_tap(lambda e: events.append(e))

    # _dispatch is a thread pool executor — replace with a synchronous shim
    class _Sync:
        def submit(self, fn, *args):
            fn(*args)
    b._dispatch = _Sync()

    diff = {
        "added": {"shape:x": _fake_record("shape:x")},
        "updated": {},
        "removed": {},
    }
    b._apply_draw(diff)

    assert len(events) == 1
    assert len(events[0]["added"]) == 1
    assert events[0]["added"][0].id == "shape:x"
    assert events[0]["updated"] == []
    assert events[0]["removed"] == []


def test_bridge_draw_tap_updated_and_removed():
    b = Bridge()
    b._loop = object()
    b._emit = lambda targets, msg: None
    b._drawings["shape:y"] = _fake_record("shape:y")

    events = []
    b.add_draw_tap(lambda e: events.append(e))

    class _Sync:
        def submit(self, fn, *args): fn(*args)
    b._dispatch = _Sync()

    old = _fake_record("shape:y")
    new = dict(old); new["x"] = 99
    diff = {
        "added": {},
        "updated": {"shape:y": [old, new]},
        "removed": {"shape:z": _fake_record("shape:z")},
    }
    b._apply_draw(diff)
    e = events[0]
    assert len(e["updated"]) == 1
    assert e["updated"][0].id == "shape:y"
    assert len(e["removed"]) == 1
    assert e["removed"][0] == "shape:z"


def test_bridge_remove_draw_tap():
    b = Bridge()
    b._loop = object()
    b._emit = lambda targets, msg: None
    calls = []
    fn = lambda e: calls.append(e)
    b.add_draw_tap(fn)
    b.remove_draw_tap(fn)

    class _Sync:
        def submit(self, fn, *args): fn(*args)
    b._dispatch = _Sync()

    b._apply_draw({"added": {"shape:a": _fake_record("shape:a")},
                   "updated": {}, "removed": {}})
    assert calls == []  # tap was removed


# ---------------------------------------------------------------------------
# Canvas factory methods
# ---------------------------------------------------------------------------

def test_canvas_geo_factory():
    c = danvas.Canvas()
    s = c.geo(x=10, y=20, w=200, h=150, geo="ellipse", color="blue", name="e")
    assert isinstance(s, Geo)
    assert s.name == "e"
    assert s._props["geo"] == "ellipse"
    assert s._props["color"] == "blue"
    assert s in c.shapes
    assert "e" in c._named


def test_canvas_text_factory():
    c = danvas.Canvas()
    s = c.text(x=0, y=0, text="Hi", font="sans")
    assert isinstance(s, Text)
    assert s._props["text"] == "Hi"


def test_canvas_note_factory():
    c = danvas.Canvas()
    s = c.note(x=0, y=0, text="note", color="yellow")
    assert isinstance(s, Note)
    assert s._props["color"] == "yellow"


def test_canvas_draw_factory_normalises_origin():
    c = danvas.Canvas()
    s = c.draw([(100, 200), (150, 250), (200, 200)], color="red")
    assert isinstance(s, Draw)
    # origin is min(x)=100, min(y)=200 → shape placed at (100, 200)
    assert s.x == 100.0
    assert s.y == 200.0
    # all segment points should be shifted relative to origin
    pts = s._props["segments"][0]["points"]
    assert pts[0]["x"] == 0.0
    assert pts[0]["y"] == 0.0


def test_canvas_highlight_factory():
    c = danvas.Canvas()
    s = c.highlight([(10, 10), (200, 10)], color="yellow")
    assert isinstance(s, Highlight)
    assert s._props["color"] == "yellow"


def test_canvas_line_factory():
    c = danvas.Canvas()
    s = c.line([(0, 0), (100, 50), (200, 0)], color="black")
    assert isinstance(s, Line)
    assert s.x == 0.0
    assert "a1" in s._props["points"]


def test_canvas_frame_factory():
    c = danvas.Canvas()
    s = c.frame(x=50, y=100, w=800, h=400, label="Slide 1")
    assert isinstance(s, Frame)
    assert s.label == "Slide 1"
    assert s._props["w"] == 800.0


def test_canvas_draw_factory_accepts_explicit_xy():
    c = danvas.Canvas()
    s = c.draw([(10, 20), (30, 40)], x=0, y=0, name="d")
    # explicit x/y=0 overrides bounding-box origin
    assert s.x == 0.0
    assert s.y == 0.0


def test_canvas_line_empty_raises():
    c = danvas.Canvas()
    with pytest.raises(ValueError):
        c.line([])


# ---------------------------------------------------------------------------
# Auto-naming and name eviction
# ---------------------------------------------------------------------------

def test_auto_name_assigned_when_none():
    c = danvas.Canvas()
    s1 = c.geo(x=0, y=0)
    s2 = c.geo(x=100, y=0)
    assert s1.name == "geo1"
    assert s2.name == "geo2"


def test_name_eviction_replaces_old_shape():
    c = danvas.Canvas()
    s1 = c.geo(x=0, y=0, name="box")
    s2 = c.geo(x=100, y=0, name="box")
    assert s1 not in c.shapes
    assert s2 in c.shapes
    assert c._named["box"] is s2


# ---------------------------------------------------------------------------
# canvas.shapes and canvas.drawings properties
# ---------------------------------------------------------------------------

def test_canvas_shapes_property():
    c = danvas.Canvas()
    g = c.geo(x=0, y=0, name="g")
    n = c.note(x=100, y=0, name="n")
    assert set(c.shapes) == {g, n}


def test_canvas_drawings_snapshot():
    c = danvas.Canvas()
    rec = _fake_record("shape:q")
    c._bridge._drawings["shape:q"] = rec
    d = c.drawings
    assert "shape:q" in d
    assert isinstance(d["shape:q"], DrawingShape)
    assert d["shape:q"].id == "shape:q"


def test_canvas_drawings_snapshot_is_fresh():
    c = danvas.Canvas()
    c._bridge._drawings["shape:a"] = _fake_record("shape:a")
    _ = c.drawings  # first access
    c._bridge._drawings["shape:b"] = _fake_record("shape:b")
    d2 = c.drawings  # second access
    assert "shape:b" in d2


# ---------------------------------------------------------------------------
# canvas.on_draw / off_draw
# ---------------------------------------------------------------------------

def test_on_draw_registers_tap():
    c = danvas.Canvas()
    fn = lambda e: None
    c.on_draw(fn)
    assert fn in c._bridge._draw_taps


def test_off_draw_removes_tap():
    c = danvas.Canvas()
    fn = lambda e: None
    c.on_draw(fn)
    c.off_draw(fn)
    assert fn not in c._bridge._draw_taps


def test_on_draw_decorator():
    c = danvas.Canvas()
    events = []

    @c.on_draw
    def handler(e):
        events.append(e)

    assert handler in c._bridge._draw_taps


# ---------------------------------------------------------------------------
# canvas.remove_shape
# ---------------------------------------------------------------------------

def test_canvas_remove_shape():
    c = danvas.Canvas()
    s = c.geo(x=0, y=0, name="box")
    c.remove_shape(s)
    assert s not in c.shapes
    assert "box" not in c._named


def test_canvas_remove_shape_by_name():
    c = danvas.Canvas()
    c.geo(x=0, y=0, name="box")
    c.remove_shape("box")
    assert "box" not in c._named


def test_canvas_remove_shape_noop_when_already_gone():
    c = danvas.Canvas()
    s = c.geo(x=0, y=0, name="box")
    c.remove_shape(s)
    c.remove_shape(s)  # second call — must not raise


# ---------------------------------------------------------------------------
# shape anchors fail loudly (user feedback: silent default placement)
# ---------------------------------------------------------------------------
def test_shape_anchor_unknown_name_raises():
    # Anchoring to a name that matches nothing is a typo — landing the shape
    # at the default position silently was the old, wrong behavior.
    c = danvas.Canvas()
    with pytest.raises(ValueError, match="matches no panel or shape"):
        c.text(text="oops", below="no_such_name")


def test_shape_anchor_unplaced_panel_raises():
    # A free panel without x=/y= has no coordinates until auto-layout runs at
    # serve time; shapes are placed immediately, so anchoring to it used to
    # die with a bare TypeError (None + float). Now it says what to do.
    c = danvas.Canvas()
    lbl = c.label("floating", "auto-placed")
    with pytest.raises(ValueError, match="no position yet"):
        c.text(text="under", below=lbl)


def test_shape_anchor_placed_panel_works():
    # The documented path: any placed panel or shape anchors a shape factory.
    c = danvas.Canvas()
    lbl = c.label("pinned", "here", x=100, y=100)
    t = c.text(text="under", below=lbl)
    assert t.x == 100
    assert t.y > 100


# ---------------------------------------------------------------------------
# stacking order: shapes and arrows share the panel z-order API
# ---------------------------------------------------------------------------
def test_shape_to_front_emits_order_and_rotates_replay():
    c = danvas.Canvas()
    a = c.geo(x=0, y=0, name="a")
    b = c.geo(x=50, y=0, name="b")
    sent = []
    c._bridge.broadcast = lambda msg: sent.append(msg)
    a.to_front()
    assert sent[-1] == {"type": "order", "id": a.id, "op": "front"}
    # front/back rotate the replay registry so a reload rebuilds the new
    # stacking (later-replayed records sit on top) — same rule as panels.
    assert list(c._bridge._shapes) == [b.id, a.id]
    b.to_back()
    assert sent[-1] == {"type": "order", "id": b.id, "op": "back"}
    assert list(c._bridge._shapes) == [b.id, a.id]


def test_shape_forward_is_live_only():
    c = danvas.Canvas()
    a = c.geo(x=0, y=0, name="a")
    c.geo(x=50, y=0, name="b")
    sent = []
    c._bridge.broadcast = lambda msg: sent.append(msg)
    before = list(c._bridge._shapes)
    a.forward()
    a.backward()
    assert [m["op"] for m in sent] == ["forward", "backward"]
    assert list(c._bridge._shapes) == before   # nudges don't touch replay


def test_arrow_to_front_emits_order_and_rotates_replay():
    c = danvas.Canvas()
    s1 = c.slider("s1", x=0, y=0)
    s2 = c.slider("s2", x=0, y=200)
    a1 = c.connect(s1, s2, name="a1")
    a2 = c.connect(s2, s1, name="a2")
    sent = []
    c._bridge.broadcast = lambda msg: sent.append(msg)
    a1.to_front()
    assert sent[-1] == {"type": "order", "id": a1.id, "op": "front"}
    assert list(c._bridge._arrows) == [a2.id, a1.id]


def test_panel_reorder_still_rotates_components():
    # The unified reorder path must keep the original panel behavior intact.
    c = danvas.Canvas()
    p1 = c.label("p1", "one", x=0, y=0)
    p2 = c.label("p2", "two", x=0, y=100)
    sent = []
    c._bridge.broadcast = lambda msg: sent.append(msg)
    p1.to_front()
    assert sent[-1] == {"type": "order", "id": p1.id, "op": "front"}
    assert list(c._bridge._components) == [p2.id, p1.id]
    p1.to_back()
    assert list(c._bridge._components) == [p1.id, p2.id]
