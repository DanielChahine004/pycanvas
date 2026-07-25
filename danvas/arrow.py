"""Arrow: a connector between two panels, managed much like a component.

Split out of :mod:`danvas.canvas` to keep the ``Canvas`` class focused on the
canvas surface itself. An ``Arrow`` is created by :meth:`Canvas.connect` and bound
to the canvas bridge, so its appearance can be changed live.
"""


# Friendly snake_case names mapped onto the arrow shape's prop names. The
# arrow's ``name`` (its identity / eviction key) is handled separately and never
# sent as a shape prop; ``text`` is the caption the canvas actually draws.
_ARROW_PROP_ALIASES = {
    "arrowhead_start": "arrowheadStart",
    "arrowhead_end": "arrowheadEnd",
    "label_color": "labelColor",
}


def _arrow_props(props):
    """Translate snake_case kwargs to the arrow shape's prop names."""
    return {_ARROW_PROP_ALIASES.get(k, k): v for k, v in props.items()}


class Arrow:
    """A connector between two panels, managed much like a component.

    Returned by :meth:`Canvas.connect`. The arrow binds to each panel,
    so it reroutes automatically as the panels move or resize. It is bound to the
    canvas bridge so its appearance can be changed live::

        a = canvas.connect(src, dst, name="flow", text="x1", color="blue")
        a.color = "red"               # or a.update(color="red")
        a.text = "x2"                 # change the visible caption live
        a.update(dash="dashed", text="x3")

    ``name`` is the arrow's **identity**: the ``canvas.<name>`` handle and the
    eviction key, so connecting again under the same ``name`` destroys the
    previous arrow and makes the new one the reference. Omit it and the name is
    derived from the endpoints (``"<start.name>-><end.name>"``), so re-connecting
    the same two panels replaces the old arrow rather than duplicating it.
    ``text`` is the
    **caption** drawn on the arrow; it is purely cosmetic and may change freely
    without affecting identity. When ``text`` is omitted the arrow shows no
    caption (the identity is never drawn).

    Valid values: ``color`` one of black/grey/violet/light-violet/blue/
    light-blue/yellow/orange/green/light-green/light-red/red/white; ``dash`` one
    of draw/solid/dashed/dotted; ``size`` one of s/m/l/xl; ``arrowhead_start`` /
    ``arrowhead_end`` one of none/arrow/triangle/square/dot/pipe/diamond/
    inverted/bar; ``bend`` a number.

    Pass it (or its ``name``) to :meth:`Canvas.disconnect` to remove it.
    """

    def __init__(self, arrow_id, start, end, bridge, props=None,
                 name=None, text=None):
        self.id = arrow_id
        self.start = start
        self.end = end
        self.name = name    # unique identity / canvas.<name> handle / eviction key
        self._bridge = bridge
        self._props = dict(props or {})
        # ``text`` is the visible caption, kept distinct from the identity. When
        # omitted the arrow shows no caption (the identity is never drawn).
        if text is not None:
            self._props["text"] = text

    def register_message(self):
        """Build the ``arrow`` register message (current props included)."""
        return {
            "type": "arrow",
            "id": self.id,
            "start": self.start.id,
            "end": self.end.id,
            "props": dict(self._props),
        }

    def update(self, **props):
        """Change arrow properties live (color, text, dash, size, bend, ...).

        Accepts the friendly names in the class docstring. Stored so a
        reconnecting client replays the new appearance.
        """
        props = _arrow_props(props)
        self._props.update(props)
        if self._bridge is not None:
            self._bridge.broadcast(
                {"type": "update", "id": self.id, "payload": props}
            )
        return self

    # -- stacking order (z-index) ---------------------------------------------
    # Arrows share the browser's ONE global z-order with shapes and panels;
    # these mirror the panel/shape methods. A fresh arrow starts on top;
    # relative to panels an arrow is either above ALL of them or below ALL
    # of them (drawings render in two passes around the panel layer).
    def to_front(self):
        """Raise this arrow above everything else on the canvas, live.

        Persists across reload: ``front``/``back`` also rotate the replay
        registry, like a panel's :meth:`to_front`.
        """
        self._send_order("front")

    def to_back(self):
        """Lower this arrow beneath everything else (panels included), live."""
        self._send_order("back")

    def forward(self):
        """Raise this arrow one step up the stack, live (not persisted —
        use :meth:`to_front` for a durable change)."""
        self._send_order("forward")

    def backward(self):
        """Lower this arrow one step down the stack, live (not persisted —
        use :meth:`to_back` for a durable change)."""
        self._send_order("backward")

    def _send_order(self, op):
        if self._bridge is not None:
            self._bridge.reorder_component(self.id, op)

    # -- convenience accessors for the common props --------------------------
    @property
    def color(self):
        return self._props.get("color")

    @color.setter
    def color(self, value):
        self.update(color=value)

    @property
    def text(self):
        """The caption drawn on the arrow (the ``text`` prop)."""
        return self._props.get("text")

    @text.setter
    def text(self, value):
        self.update(text=value)

    @property
    def dash(self):
        return self._props.get("dash")

    @dash.setter
    def dash(self, value):
        self.update(dash=value)

    @property
    def size(self):
        return self._props.get("size")

    @size.setter
    def size(self, value):
        self.update(size=value)

    @property
    def bend(self):
        return self._props.get("bend")

    @bend.setter
    def bend(self, value):
        self.update(bend=value)
