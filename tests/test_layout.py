"""Auto-layout containers: canvas.grid / column / row."""

import danvas


def test_grid_flows_left_to_right_then_wraps():
    canvas = danvas.Canvas()
    with canvas.grid(cols=2, slot=(100, 50), gap=10, origin=(0, 0)):
        a = canvas.label("a")
        b = canvas.label("b")
        c = canvas.label("c")   # wraps to the second row
    assert (a.x, a.y) == (0, 0)
    assert (b.x, b.y) == (110, 0)          # next column: 100 + gap
    assert (c.x, c.y) == (0, 60)           # next row: 50 + gap
    # Slot size fills in the panel dimensions.
    assert (a.w, a.h) == (100, 50)


def test_column_stacks_by_natural_height():
    canvas = danvas.Canvas()
    # A label is 84 tall by default, a button 84, a slider 72 — the column keeps
    # each panel's own height and advances the cursor by it (not a uniform slot).
    with canvas.column(x=40, y=40, w=320, gap=10):
        a = canvas.label("a")              # default_h 84
        b = canvas.button("b")             # default_h 84
        c = canvas.slider("c")             # default_h 72
    assert (a.x, a.y, a.w) == (40, 40, 320)
    assert (b.x, b.y) == (40, 40 + 84 + 10)
    assert (c.x, c.y) == (40, 40 + 84 + 10 + 84 + 10)
    assert c.h == 72                       # natural height preserved


def test_column_width_none_keeps_each_panels_own_width():
    canvas = danvas.Canvas()
    with canvas.column(x=0, y=0, gap=10):
        a = canvas.label("a")              # default_w 240
    assert a.w == 240


def test_row_flows_by_natural_width():
    canvas = danvas.Canvas()
    with canvas.row(x=0, y=0, h=50, gap=10):
        a = canvas.label("a")              # default_w 240
        b = canvas.label("b")
    assert [p.y for p in (a, b)] == [0, 0]
    assert [p.x for p in (a, b)] == [0, 240 + 10]
    assert a.h == 50                       # common height applied


def test_explicit_position_overrides_the_grid():
    canvas = danvas.Canvas()
    with canvas.grid(cols=2, slot=(100, 50), gap=10, origin=(0, 0)):
        canvas.label("a")                  # claims slot 0
        b = canvas.label("b", x=500, y=500)  # explicit — bypasses the grid
        c = canvas.label("c")              # takes the *next* slot (1), not 0
    assert (b.x, b.y) == (500, 500)
    assert (c.x, c.y) == (110, 0)


def test_explicit_size_is_kept_position_from_grid():
    canvas = danvas.Canvas()
    with canvas.grid(cols=3, slot=(100, 50), gap=10, origin=(0, 0)):
        a = canvas.label("a", w=333)       # own width, grid height + position
    assert (a.x, a.y, a.w, a.h) == (0, 0, 333, 50)


def test_relative_anchor_wins_over_grid():
    canvas = danvas.Canvas()
    anchor = canvas.label("anchor", x=0, y=0, w=100, h=40)
    with canvas.grid(cols=2, slot=(100, 50), gap=10, origin=(900, 900)):
        b = canvas.label("b", below=anchor)
    assert (b.x, b.y) == (0, 0 + 40 + 16)  # anchored, not grid-placed


def test_layout_stack_unwinds_after_block():
    canvas = danvas.Canvas()
    with canvas.grid(cols=2):
        assert canvas._layout_stack
    assert not canvas._layout_stack
    # Outside the block, panels auto-cascade again (no Python-side position).
    loose = canvas.label("loose")
    assert loose.x is None and loose.y is None


def test_auto_height_panel_keeps_fitting_in_a_grid():
    canvas = danvas.Canvas()
    with canvas.grid(cols=2, slot=(300, 200), gap=10, origin=(0, 0)):
        md = canvas.markdown("# hi", name="notes", h="auto")
    # Grid positions it, but h='auto' is preserved (not forced to slot height).
    assert (md.x, md.y) == (0, 0)
    assert md._auto_h is True


def test_label_defaults_to_auto_height():
    # A standalone label fits its content (no tall empty box)...
    canvas = danvas.Canvas()
    a = canvas.label("a")
    assert a._auto_h is True
    # ...but an explicit height pins it (auto-height off).
    b = canvas.label("b", h=120)
    assert b._auto_h is False
    assert b.h == 120


def test_label_default_auto_height_yields_to_grid_slot():
    # Unlike an explicit h="auto", a label's *default* auto-height defers to a
    # grid slot height so the grid stays uniform.
    canvas = danvas.Canvas()
    with canvas.grid(cols=2, slot=(100, 50), gap=10, origin=(0, 0)):
        a = canvas.label("a")
    assert a.h == 50
    assert a._auto_h is False


def _capture_broadcast(canvas):
    """Record every frame the bridge would broadcast."""
    sent = []
    orig = canvas._bridge.broadcast
    canvas._bridge.broadcast = lambda msg, **kw: sent.append(msg)
    return sent


def test_column_reflow_broadcasts_container_sync():
    canvas = danvas.Canvas()
    with canvas.column(x=40, y=40, w=200, gap=10) as col:
        a = canvas.label("a", h=50)
        b = canvas.label("b", h=50)
        c = canvas.label("c", h=50)
    sent = _capture_broadcast(canvas)
    col.reflow()
    syncs = [m for m in sent if m.get("type") == "container_sync"]
    assert len(syncs) == 1
    m = syncs[0]
    assert m["mode"] == "column"
    assert [mem["id"] for mem in m["members"]] == [a.id, b.id, c.id]
    assert (m["x0"], m["y0"], m["gap"]) == (40, 40, 10)


def test_row_reflow_broadcasts_container_sync():
    canvas = danvas.Canvas()
    with canvas.row(x=0, y=0, h=50, gap=10) as r:
        a = canvas.label("a", w=80)
        b = canvas.label("b", w=80)
    sent = _capture_broadcast(canvas)
    r.reflow()
    syncs = [m for m in sent if m.get("type") == "container_sync"]
    assert syncs[0]["mode"] == "row"
    assert [mem["id"] for mem in syncs[0]["members"]] == [a.id, b.id]


def test_reflow_after_container_remove_excludes_panel():
    # col.remove() is the right way to drop a panel from a container.
    # After removal the container_sync message no longer lists it.
    canvas = danvas.Canvas()
    with canvas.column(x=0, y=0, gap=10) as col:
        a = canvas.label("a")
        b = canvas.label("b")
        c = canvas.label("c")
    col.remove(b)
    sent = _capture_broadcast(canvas)
    col.reflow()
    syncs = [m for m in sent if m.get("type") == "container_sync"]
    assert [mem["id"] for mem in syncs[0]["members"]] == [a.id, c.id]


def test_grid_refit_repacks_locally_by_slot():
    # A grid keeps uniform fixed slots, so it re-packs in Python (no browser
    # round-trip) straight back onto the slot grid.
    canvas = danvas.Canvas()
    with canvas.grid(cols=2, slot=(100, 50), gap=10, origin=(0, 0)) as g:
        a = canvas.label("a")
        b = canvas.label("b")
        c = canvas.label("c")
    a.set_layout(x=999, y=999)                       # knock one out of place
    g.refit()
    assert (a.x, a.y) == (0, 0)
    assert (b.x, b.y) == (110, 0)
    assert (c.x, c.y) == (0, 60)


def test_nested_container_places_children_correctly():
    # A row nested inside a column: the row's children are placed relative to
    # where the column cursor lands, not the column's own origin.
    canvas = danvas.Canvas()
    col = canvas.column(x=0, y=0, gap=10)
    col.add(canvas.label("top", h=40))        # top: y=0..40
    row = col.row(gap=8)                       # row starts at y=50
    row.add(canvas.label("r1", w=100, h=30))
    row.add(canvas.label("r2", w=100, h=30))
    r1 = canvas["r1"]
    r2 = canvas["r2"]
    assert (r1.x, r1.y) == (0, 50)
    assert (r2.x, r2.y) == (108, 50)          # 100 + gap 8


# ---------------------------------------------------------------------------
# color clearing (user report: color=None silently failed — set_layout's
# None-means-unchanged sentinel swallowed the clear while Python read back
# None, so the browser kept the tint forever)
# ---------------------------------------------------------------------------
def test_color_none_actually_clears_the_frame_tint():
    canvas = danvas.Canvas()
    p = canvas.label("tinted", "x", x=0, y=0)
    sent = []
    canvas._bridge.broadcast = lambda msg, **kw: sent.append(msg)
    p.color = "#c2554d"
    assert sent[-1]["payload"]["frameColor"] == "#c2554d"
    p.color = None
    # The clear must reach the wire ("" is the clearing value — a falsy
    # frameColor drops the tint browser-side), and read-back must agree.
    assert sent[-1]["payload"]["frameColor"] == ""
    assert p.color is None


def test_color_clear_survives_replay():
    # A reconnecting browser rebuilds from the stored base layout — the
    # cleared tint must not resurrect there either.
    canvas = danvas.Canvas()
    p = canvas.label("tinted", "x", x=0, y=0)
    p.color = "#c2554d"
    reg = canvas._bridge.register_message(p)
    assert reg.get("frameColor") == "#c2554d"
    p.color = None
    reg = canvas._bridge.register_message(p)
    assert "frameColor" not in reg
