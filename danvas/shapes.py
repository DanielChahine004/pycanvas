"""Managed canvas shapes and ephemeral drawing observation.

Python-created shapes sit alongside panels on the canvas but are simpler: no
iframe, no handlers, just geometry + style on a native canvas shape.  Seven
shape types are supported:

    box  = canvas.geo(x=100, y=100, w=200, h=150, geo='rectangle')
    lbl  = canvas.text(x=300, y=100, text='Hello', color='blue')
    note = canvas.note(x=500, y=100, text='TODO', color='yellow')
    path = canvas.draw([(0,0),(40,20),(80,5)], color='red')
    poly = canvas.line([(0,0),(100,50),(200,0)], spline='cubic')
    art  = canvas.frame(x=50, y=300, w=800, h=500, label='Slide 1')
    mark = canvas.highlight([(10,10),(200,10)], color='yellow')

Each returns a live handle with property setters and an ``update(**kw)`` batch
method.  Shapes can also be used as arrow endpoints with ``canvas.connect()``.

User-drawn (ephemeral) shapes are exposed via ``canvas.drawings`` and
``@canvas.on_draw``; ``DrawingShape`` handles can mutate or delete them.
"""


# ---------------------------------------------------------------------------
# Managed shapes
# ---------------------------------------------------------------------------

class BaseShape:
    """Base for Python-managed canvas shapes."""

    _type: str = ""  # shape type string (e.g. 'geo', 'text')

    def __init__(self, shape_id, x, y, props, name=None,
                 rotation=0.0, opacity=1.0):
        self.id = shape_id
        self.x = float(x)
        self.y = float(y)
        self.rotation = float(rotation)
        self.opacity = float(opacity)
        self._props = dict(props)
        self.name = name
        self._bridge = None

    def register_message(self):
        return {
            "type": "shape",
            "id": self.id,
            "shapeType": self._type,
            "x": self.x,
            "y": self.y,
            "rotation": self.rotation,
            "opacity": self.opacity,
            "props": dict(self._props),
        }

    def _send_update(self, top, props):
        """Store changes locally and broadcast a ``shape_update`` message."""
        for k, v in top.items():
            setattr(self, k, v)
        self._props.update(props)
        if self._bridge is None:
            return
        msg = {"type": "shape_update", "id": self.id}
        if top:
            msg.update(top)
        if props:
            msg["props"] = props
        self._bridge.broadcast(msg)

    def update(self, **kw):
        """Change any shape property live.

        Top-level keys ``x``, ``y``, ``rotation``, ``opacity`` move or tilt
        the shape; all others are merged into the shape's props dict.
        """
        _TOP = {"x", "y", "rotation", "opacity"}
        top = {k: kw.pop(k) for k in list(kw) if k in _TOP}
        self._send_update(top, kw)
        return self

    def move(self, x=None, y=None):
        """Reposition the shape.  Pass only the axes you want to change."""
        top = {k: v for k, v in (("x", x), ("y", y)) if v is not None}
        if top:
            self._send_update(top, {})
        return self

    def remove(self):
        """Remove this shape from the canvas."""
        if self._bridge:
            self._bridge.remove_shape(self.id)

    # -- stacking order (z-index) ---------------------------------------------
    # Shapes, arrows and panels share ONE global z-order in the browser, so
    # these mirror the panel methods exactly. One structural bound: drawings
    # and arrows render in two passes around the panels, so relative to
    # panels a shape is either above ALL of them or below ALL of them.
    def to_front(self):
        """Raise this shape above everything else on the canvas, live.

        Persists across reload: ``front``/``back`` also rotate the replay
        registry, like a panel's :meth:`to_front`.
        """
        self._send_order("front")

    def to_back(self):
        """Lower this shape beneath everything else (panels included), live."""
        self._send_order("back")

    def forward(self):
        """Raise this shape one step up the stack, live (not persisted —
        use :meth:`to_front` for a durable change)."""
        self._send_order("forward")

    def backward(self):
        """Lower this shape one step down the stack, live (not persisted —
        use :meth:`to_back` for a durable change)."""
        self._send_order("backward")

    def _send_order(self, op):
        if self._bridge:
            self._bridge.reorder_component(self.id, op)

    # -- shared style property setters (most shapes have these) ---------------

    @property
    def color(self):
        return self._props.get("color")

    @color.setter
    def color(self, v):
        self._send_update({}, {"color": v})

    @property
    def fill(self):
        return self._props.get("fill")

    @fill.setter
    def fill(self, v):
        self._send_update({}, {"fill": v})

    @property
    def dash(self):
        return self._props.get("dash")

    @dash.setter
    def dash(self, v):
        self._send_update({}, {"dash": v})

    @property
    def size(self):
        """Stroke / text weight: 's', 'm', 'l', or 'xl'."""
        return self._props.get("size")

    @size.setter
    def size(self, v):
        self._send_update({}, {"size": v})


class Geo(BaseShape):
    """A geo shape: rectangle, ellipse, triangle, cloud, star, …

    ``geo`` selects the sub-type (default ``'rectangle'``).  ``w``/``h`` set
    the bounding-box dimensions.  Style kwargs: color, fill, dash, size, font,
    align, label_color.

    Valid ``geo`` values: rectangle, ellipse, triangle, diamond, pentagon,
    hexagon, octagon, star, rhombus, oval, trapezoid, cloud, heart, x-box,
    check-box, arrow-right, arrow-left, arrow-up, arrow-down.
    """

    _type = "geo"

    def __init__(self, shape_id, x, y, w=200, h=150, geo="rectangle",
                 name=None, rotation=0.0, opacity=1.0, **props):
        props = {"geo": geo, "w": float(w), "h": float(h), **props}
        super().__init__(shape_id, x, y, props, name=name,
                         rotation=rotation, opacity=opacity)

    @property
    def text(self):
        return self._props.get("text", "")

    @text.setter
    def text(self, v):
        self._send_update({}, {"text": v})

    @property
    def w(self):
        return self._props.get("w")

    @w.setter
    def w(self, v):
        self._send_update({}, {"w": float(v)})

    @property
    def h(self):
        return self._props.get("h")

    @h.setter
    def h(self, v):
        self._send_update({}, {"h": float(v)})


class Text(BaseShape):
    """Plain floating text without a background.

    Style kwargs: color, size, font (draw/sans/serif/mono).
    """

    _type = "text"

    def __init__(self, shape_id, x, y, text="", name=None,
                 rotation=0.0, opacity=1.0, **props):
        props = {"text": text, **props}
        super().__init__(shape_id, x, y, props, name=name,
                         rotation=rotation, opacity=opacity)

    @property
    def text(self):
        return self._props.get("text", "")

    @text.setter
    def text(self, v):
        self._send_update({}, {"text": v})


class Note(BaseShape):
    """Sticky note with a coloured background.

    Style kwargs: color, size, font, align (start/middle/end).
    """

    _type = "note"

    def __init__(self, shape_id, x, y, text="", name=None,
                 rotation=0.0, opacity=1.0, **props):
        props = {"text": text, **props}
        super().__init__(shape_id, x, y, props, name=name,
                         rotation=rotation, opacity=opacity)

    @property
    def text(self):
        return self._props.get("text", "")

    @text.setter
    def text(self, v):
        self._send_update({}, {"text": v})


def _segments_from_points(raw):
    """Build a normalised segment list from a flat point list or existing segments.

    Accepts ``[(x, y), …]``, ``[(x, y, pressure), …]``, or an existing list of
    ``{type, points}`` segment dicts.  Returns ``(origin_x, origin_y, segments)``
    where all segment points are relative to the bounding-box top-left.
    """
    if not raw:
        return 0.0, 0.0, [{"type": "free", "points": []}]

    if isinstance(raw[0], dict):
        # Already segment dicts — just shift to bounding-box origin.
        segs = raw
        flat = [p for s in segs for p in s.get("points", [])]
        if not flat:
            return 0.0, 0.0, segs
        ox = min(float(p.get("x", 0)) for p in flat)
        oy = min(float(p.get("y", 0)) for p in flat)
        shifted = [
            {
                "type": s.get("type", "free"),
                "points": [
                    {**p, "x": float(p["x"]) - ox, "y": float(p["y"]) - oy}
                    for p in s.get("points", [])
                ],
            }
            for s in segs
        ]
        return ox, oy, shifted

    # Flat list of (x, y) or (x, y, pressure) tuples.
    ox = min(float(p[0]) for p in raw)
    oy = min(float(p[1]) for p in raw)
    points = [
        {
            "x": float(p[0]) - ox,
            "y": float(p[1]) - oy,
            "z": float(p[2]) if len(p) > 2 else 0.5,
        }
        for p in raw
    ]
    return ox, oy, [{"type": "free", "points": points}]


class Draw(BaseShape):
    """A freehand drawn stroke.

    ``points`` is a list of ``(x, y)`` or ``(x, y, pressure)`` tuples, or a
    list of segment dicts ``{type, points}``.  The bounding-box origin
    becomes the shape's ``x``/``y``; all points are stored relative to it.

    Style kwargs: color, fill, dash, size.
    """

    _type = "draw"

    def __init__(self, shape_id, x, y, segments, name=None,
                 rotation=0.0, opacity=1.0, **props):
        props = {
            "segments": segments,
            "isComplete": props.pop("isComplete", True),
            "isClosed":   props.pop("isClosed", False),
            "isPen":      props.pop("isPen", False),
            **props,
        }
        super().__init__(shape_id, x, y, props, name=name,
                         rotation=rotation, opacity=opacity)


class Highlight(BaseShape):
    """Semi-transparent highlighter stroke.  Same point format as :class:`Draw`."""

    _type = "highlight"

    def __init__(self, shape_id, x, y, segments, name=None,
                 rotation=0.0, opacity=1.0, **props):
        props = {
            "segments":   segments,
            "isComplete": props.pop("isComplete", True),
            "isPen":      props.pop("isPen", False),
            **props,
        }
        super().__init__(shape_id, x, y, props, name=name,
                         rotation=rotation, opacity=opacity)


def _line_points(pts):
    """Convert ``[(x, y), …]`` to the line ``points`` dict.

    Points are stored relative to the first point (lines are placed at
    their first control point).  Sequential fractional-style keys (``a1``,
    ``a2``, …) are used for ordering.
    """
    if not pts:
        return {}
    ox = float(pts[0][0])
    oy = float(pts[0][1])
    result = {}
    for i, p in enumerate(pts):
        key = f"a{i + 1}"
        result[key] = {
            "id": key,
            "index": key,
            "x": float(p[0]) - ox,
            "y": float(p[1]) - oy,
        }
    return result


class Line(BaseShape):
    """A polyline (or cubic spline) defined by control points.

    ``points`` is a list of ``(x, y)`` tuples.  The first point becomes the
    shape's ``x``/``y``; all subsequent points are stored relative to it.

    ``spline='cubic'`` makes the line curve smoothly through the control points.
    Style kwargs: color, dash, size.
    """

    _type = "line"

    def __init__(self, shape_id, x, y, points_dict, name=None,
                 rotation=0.0, opacity=1.0, **props):
        props = {"points": points_dict, **props}
        super().__init__(shape_id, x, y, props, name=name,
                         rotation=rotation, opacity=opacity)


class Frame(BaseShape):
    """An artboard-style container frame.

    Use ``label=`` for the visible title (avoids conflict with ``name=``, the
    Python identity / eviction key).  ``w``/``h`` set the frame size.
    """

    _type = "frame"

    def __init__(self, shape_id, x, y, w=400, h=300, label="", name=None,
                 rotation=0.0, opacity=1.0, **props):
        # The frame's display title is stored in a prop called 'name'.
        props = {"w": float(w), "h": float(h), "name": label, **props}
        super().__init__(shape_id, x, y, props, name=name,
                         rotation=rotation, opacity=opacity)

    @property
    def label(self):
        return self._props.get("name", "")

    @label.setter
    def label(self, v):
        self._send_update({}, {"name": v})

    @property
    def w(self):
        return self._props.get("w")

    @w.setter
    def w(self, v):
        self._send_update({}, {"w": float(v)})

    @property
    def h(self):
        return self._props.get("h")

    @h.setter
    def h(self, v):
        self._send_update({}, {"h": float(v)})


# ---------------------------------------------------------------------------
# Ephemeral drawing observation
# ---------------------------------------------------------------------------

class DrawingShape:
    """A user-drawn shape on the canvas (not Python-managed).

    Instances are delivered to ``@canvas.on_draw`` callbacks and are also
    available via ``canvas.drawings[shape_id]``.  Calling :meth:`update` or
    :meth:`remove` mutates the ephemeral shape by broadcasting a draw
    diff to every connected browser and updating the server's shadow store.

    ``type`` is the shape type string (``'geo'``, ``'draw'``, etc.);
    ``props`` is the raw props dict.  ``id`` is the shape id
    (``'shape:…'`` format) — use it as the key in ``canvas.drawings``.
    """

    def __init__(self, record, bridge=None):
        self._record = dict(record)
        self._bridge = bridge
        self.id       = record.get("id", "")
        self.type     = record.get("type", "")
        self.x        = float(record.get("x", 0))
        self.y        = float(record.get("y", 0))
        self.rotation = float(record.get("rotation", 0))
        self.opacity  = float(record.get("opacity", 1))
        self.props    = dict(record.get("props", {}))

    @property
    def color(self):
        return self.props.get("color")

    @property
    def text(self):
        """Best-effort plain text (extracts from ProseMirror richText when present)."""
        rt = self.props.get("richText") or self.props.get("text", "")
        if isinstance(rt, dict):
            return _extract_rich_text(rt)
        return str(rt)

    def update(self, **kw):
        """Mutate this ephemeral shape by broadcasting a draw diff.

        Top-level keys (x, y, rotation, opacity) move or tilt the shape; all
        others are merged into the shape's props dict.
        """
        if not self._bridge:
            return self
        _TOP = {"x", "y", "rotation", "opacity"}
        old = dict(self._record)
        new = dict(old)
        new_props = dict(new.get("props", {}))
        for k, v in kw.items():
            if k in _TOP:
                new[k] = v
                setattr(self, k, v)
            else:
                new_props[k] = v
        new["props"] = new_props
        self.props = new_props
        self._record = new
        self._bridge._drawings[self.id] = new
        self._bridge.broadcast({
            "type": "draw",
            "diff": {"added": {}, "updated": {self.id: [old, new]}, "removed": {}},
        })
        self._bridge._notify_mutation()
        return self

    def remove(self):
        """Remove this ephemeral shape from the canvas."""
        if not self._bridge:
            return
        old = self._bridge._drawings.pop(self.id, None)
        if old is not None:
            self._bridge.broadcast({
                "type": "draw",
                "diff": {"added": {}, "updated": {}, "removed": {self.id: old}},
            })
            self._bridge._notify_mutation()


def _extract_rich_text(rt):
    """Best-effort plain-text extraction from a ProseMirror richText doc."""
    if not isinstance(rt, dict):
        return str(rt)
    texts = []
    for block in rt.get("content", []):
        for node in block.get("content", []):
            if node.get("type") == "text":
                texts.append(node.get("text", ""))
        texts.append("\n")
    return "".join(texts).strip()
