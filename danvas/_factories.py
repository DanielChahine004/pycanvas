"""Component factory methods for :class:`~danvas.canvas.Canvas`
(``canvas.slider`` / ``button`` / ``react`` / …).

Split out of canvas.py: each is a thin wrapper that builds a component and hands
it to ``Canvas.insert`` via ``_make``. Mixed into Canvas as :class:`_FactoryMixin`,
so the methods run on the real Canvas instance — ``self.insert`` /
``self._auto_name`` / ``self._show_seq`` resolve there.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ._flags import LAYOUT_FLAGS
from .components import (
    AudioFeed,
    Button,
    Chat,
    Custom,
    Download,
    FileBrowser,
    Heatmap,
    Histogram,
    Image,
    Inspector,
    Label,
    LivePlot,
    Markdown,
    Plot,
    React,
    Slider,
    Table,
    TextField,
    Toggle,
    Upload,
    VideoFeed,
    Model3D,
    WebView,
)

if TYPE_CHECKING:
    from typing_extensions import Unpack

    from .canvas import Place

# Keyword names consumed by ``Canvas.insert`` itself. A factory splits these off
# and forwards everything else to the component constructor. ``name`` is
# intentionally absent: it is the component's identity (set in its constructor),
# not a placement option. The lock/chrome flags come from the shared LAYOUT_FLAGS
# table; ``queue`` lives here so all factories accept it uniformly.
_INSERT_KEYS = ("x", "y", "w", "h", "width", "height", "rotation", "queue",
                "below", "above", "right_of", "left_of", "gap",
                "roles", "lock_for", "decorative", *LAYOUT_FLAGS)


class _FactoryMixin:
    """The ``canvas.<component>(...)`` factories. Mixed into :class:`Canvas`."""

    def _make(self, cls, *args, **kw):
        place = {k: kw.pop(k) for k in _INSERT_KEYS if k in kw}
        return self.insert(cls(*args, **kw), **place)

    def slider(self, name="slider", min=0, max=100, default=None, step=1,
               on_release=False, label=None, **place: Unpack[Place]):
        """Insert a :class:`~danvas.Slider`. See :meth:`insert` for ``place``.

        ``step`` sets the granularity and the int-vs-float behaviour (a
        fractional step like ``0.1`` makes it a float slider). ``on_release=True``
        reports only the settled value when the user lets go, instead of every
        value during the drag.
        """
        return self._make(Slider, name=name, min=min, max=max, default=default,
                          step=step, on_release=on_release, label=label, **place)

    def toggle(self, *args, options=None, name="toggle", default=None,
               label=None, **place: Unpack[Place]):
        """Insert a :class:`~danvas.Toggle`. Positionals are flexible — a
        string is the name, a list/tuple the options, in either order:
        ``canvas.toggle(["a", "b"])`` or ``canvas.toggle("mode", ["a", "b"])``.
        See :meth:`insert` for ``place``."""
        return self._make(Toggle, *args, options=options, name=name,
                          default=default, label=label, **place)

    def button(self, name="button", text=None, label=None, **place: Unpack[Place]):
        """Insert a :class:`~danvas.Button`. See :meth:`insert` for ``place``."""
        return self._make(Button, name=name, text=text, label=label, **place)

    def label(self, name="label", value="", label=None, **place: Unpack[Place]):
        """Insert a :class:`~danvas.Label`. See :meth:`insert` for ``place``."""
        return self._make(Label, name=name, value=value, label=label, **place)

    def heatmap(self, *args, array=None, name="heatmap", vmin=None, vmax=None,
                cmap="viridis", extent=None, smooth=False, label=None,
                **place: Unpack[Place]):
        """Insert a :class:`~danvas.Heatmap` — a 2D array as a colormapped
        image with a colorbar and cursor value readout. Positionals are
        flexible: a string is the name, an array-like is the initial data —
        ``canvas.heatmap("dose", slice2d, extent=(0, 350, 0, 350))`` or just
        ``canvas.heatmap("dose")`` + ``hm.update(...)`` later. See
        :meth:`insert` for ``place``."""
        for a in args:
            if isinstance(a, str):
                name = a
            elif a is not None:
                array = a
        hm = self._make(Heatmap, name=name, label=label, **place)
        if array is not None:
            hm.update(array, vmin=vmin, vmax=vmax, cmap=cmap, extent=extent,
                      smooth=smooth)
        return hm

    def video(self, name="video", quality=70, label=None, **place: Unpack[Place]):
        """Insert a :class:`~danvas.VideoFeed`. See :meth:`insert` for ``place``."""
        return self._make(VideoFeed, name=name, quality=quality, label=label, **place)

    def audio(self, name="audio", sample_rate=16000, channels=1, label=None, **place: Unpack[Place]):
        """Insert an :class:`~danvas.AudioFeed`. See :meth:`insert` for ``place``."""
        return self._make(AudioFeed, name=name, sample_rate=sample_rate,
                          channels=channels, label=label, **place)

    def chat(self, name="chat", label=None, **place: Unpack[Place]):
        """Insert a :class:`~danvas.Chat` panel. See :meth:`insert` for ``place``."""
        return self._make(Chat, name=name, label=label, **place)

    def custom(self, html=None, path=None, css=None, js=None, name="custom",
               label=None, keep_mounted=False, forward_wheel=True,
               themed=False, permissions=None, **place: Unpack[Place]):
        """Insert a :class:`~danvas.Custom`. See :meth:`insert` for ``place``.

        ``html``/``css``/``js`` may be given as separate strings (e.g. pasted
        from uiverse.io) — they are composed into one document under the hood.
        Size the panel with ``w``/``h`` in ``place``.

        ``keep_mounted=True`` keeps the iframe alive (hidden, not destroyed)
        when scrolled out of view, so browser-local state — selections, form
        inputs, a camera, an in-progress interaction — survives scroll-out/
        scroll-in. Reach for it when writing a STATEFUL panel; the default
        re-creates the document per scroll-in, which is right for cheap ones.
        ``forward_wheel=False`` lets the panel own its wheel events (its own
        zoom) instead of zooming the canvas; ``themed=True`` forwards the
        live ``--pc-*`` theme variables into the document; ``permissions``
        grants sandboxed-iframe features (e.g. ``"camera"``).
        """
        return self._make(Custom, html=html, path=path, css=css, js=js,
                          name=name, label=label, keep_mounted=keep_mounted,
                          forward_wheel=forward_wheel, themed=themed,
                          permissions=permissions, **place)

    def download(self, name="download", source=None, filename=None, text=None, label=None,
                 **place: Unpack[Place]):
        """Insert a :class:`~danvas.Download` button. See :meth:`insert` for ``place``.

        Clicking it downloads ``source`` — a file path or ``bytes`` — to the
        viewer's machine. For content generated fresh on each click, omit
        ``source`` and register a provider with ``@download.provide``.
        ``filename`` sets the saved name (otherwise a path's basename, or the
        panel name, is used). The host code chooses what each click serves, so
        nothing the viewer sends selects a path.
        """
        return self._make(Download, name=name, source=source, filename=filename,
                          text=text, label=label, **place)

    def upload(self, name="upload", text=None, label=None, dest=None,
               accept=None, multiple=False, max_size=None, **place: Unpack[Place]):
        """Insert an :class:`~danvas.Upload` panel. See :meth:`insert` for ``place``.

        A click-or-drop zone that receives a viewer's file into Python; wire it
        with ``@upload.on_upload``. By default the bytes arrive in memory
        (``file.data``); pass ``dest=`` a directory to stream each upload to disk
        instead (``file.path``), which keeps memory flat for large files.
        ``accept`` filters the picker (e.g. ``".csv"``), ``multiple=True`` allows
        several at once, and ``max_size`` (bytes) rejects oversized uploads — set
        it on any public/tunneled canvas.
        """
        return self._make(Upload, name=name, text=text, label=label, dest=dest,
                          accept=accept, multiple=multiple, max_size=max_size,
                          **place)

    def file_browser(self, name="filebrowser", root=".", label=None, pattern=None,
                     show_hidden=False, **place: Unpack[Place]):
        """Insert a :class:`~danvas.FileBrowser`. See :meth:`insert` for ``place``.

        Navigation is confined to ``root``. ``pattern`` (an fnmatch glob like
        ``"*.csv"``) filters which files are shown. Size it with ``w``/``h`` in
        ``place``.
        """
        return self._make(FileBrowser, name=name, root=root, label=label,
                          pattern=pattern, show_hidden=show_hidden, **place)

    def react(self, source=None, path=None, jsx=None, css=None, css_path=None,
              name="react", label=None, props=None, scope=None,
              wasm=None, wasm_path=None, **place: Unpack[Place]):
        """Insert a :class:`~danvas.React` panel — the workhorse for custom UI.

        ``source`` is JSX defining ``function Component(...)`` (or load it from a
        file with ``path=``); alternatively pass just ``jsx`` markup plus optional
        ``css`` and the Component wrapper is added under the hood. ``css`` also
        works with ``source=`` (it rides as a ``<style>`` the host renders), so a
        full component can keep its styles in a separate string — or load it from a
        file with ``css_path=`` (the ``css`` twin of ``path=``), so a panel keeps
        both halves in sibling files. Use :meth:`React.from_uiverse` to convert a
        uiverse.io styled-components snippet. ``props`` is the initial props dict; ``scope`` is third-party
        library names (e.g. ``["d3"]``) loaded as ESM and exposed as ``libs``.

        ``wasm`` accepts raw ``.wasm`` bytes; ``wasm_path=`` loads them from a
        file. The module is compiled once in the browser and its exports are
        available as ``await canvas.wasm`` inside the JSX component.

        Placement, visibility (``roles`` / ``lock_for``), the lock/chrome flags,
        and ``queue`` all flow through ``**place`` — see :meth:`insert` (and the
        :class:`Place` keys your editor now autocompletes).
        """
        return self._make(React, source=source, path=path, jsx=jsx, css=css,
                          css_path=css_path, name=name, label=label, props=props,
                          scope=scope, wasm=wasm, wasm_path=wasm_path, **place)

    def markdown(self, text="", name="markdown", label=None, **place: Unpack[Place]):
        """Insert a :class:`~danvas.Markdown` panel. See :meth:`insert` for ``place``."""
        return self._make(Markdown, text=text, name=name, label=label, **place)

    def image(self, src, name="image", label=None, fit="contain", **place: Unpack[Place]):
        """Insert an :class:`~danvas.Image` panel. See :meth:`insert` for ``place``.

        ``src`` is a path, URL, image bytes, Matplotlib/PIL figure, or array.
        """
        return self._make(Image, src, name=name, label=label, fit=fit, **place)

    def table(self, data, name="table", label=None, **place: Unpack[Place]):
        """Insert a :class:`~danvas.Table` panel. See :meth:`insert` for ``place``.

        ``data`` is a pandas DataFrame/Series, a list of dicts/rows, or a dict.
        """
        return self._make(Table, data, name=name, label=label, **place)

    def text_field(self, name="text_field", placeholder="", default="", multiline=False,
                   label=None, **place: Unpack[Place]):
        """Insert a :class:`~danvas.TextField`. See :meth:`insert` for ``place``.

        Single-line (default): fires ``on_change`` on Enter or focus-loss.
        Pass ``multiline=True`` for a textarea that fires on focus-loss.
        """
        return self._make(TextField, name=name, placeholder=placeholder,
                          default=default, multiline=multiline, label=label,
                          **place)

    def show(self, value, name=None, label=None, **kw):
        """Auto-render any value as the best-fitting panel and insert it.

        Picks the component the way a notebook decides how to render an output
        (DataFrame → table, figure/array → image, Plotly → plot, rich
        ``_repr_*`` → its HTML, dict/list → JSON, string → label/markdown, else a
        repr label) via :func:`danvas.panel_for`. With no ``name`` a unique one
        is generated; re-using a ``name`` replaces that panel in place. Returns
        the inserted component. See :meth:`insert` for ``place``.

        Any keyword arguments not used for placement are forwarded to the
        chosen component's constructor when it accepts them and silently ignored
        otherwise — so ``editable=True`` enables editing on a table but is a
        no-op if the value renders as an image or label.
        """
        from .dispatch import panel_for
        # Split placement kwargs from component-specific ones.
        place = {k: kw.pop(k) for k in _INSERT_KEYS if k in kw}
        if name is None:
            self._show_seq += 1
            name = f"panel_{self._show_seq}"
        comp = panel_for(value, name=name, label=label, **kw)
        # insert() handles eviction of whatever currently holds this name, so
        # re-showing under the same name replaces in place on its own.
        return self.insert(comp, **place)

    def webview(self, url, name="webview", label=None, **place: Unpack[Place]):
        """Insert a :class:`~danvas.WebView`. See :meth:`insert` for ``place``."""
        return self._make(WebView, url, name=name, label=label, **place)

    def model3d(self, name="model3d", label=None, color=None, view=None,
                **place: Unpack[Place]):
        """Insert a :class:`~danvas.Model3D` — the prebuilt CAD/3D viewer:
        ``viewer.update(glb_bytes_or_path_or_trimesh)`` shows the model with
        orbit, snap measurements, and a section plane. ``view=`` sets the
        starting camera (a preset like ``"iso"``/``"front"``, or
        ``{"eye": ..., "look": ...}``). See :meth:`insert`."""
        return self._make(Model3D, name=name, label=label, color=color,
                          view=view, **place)

    def plot(self, name="plot", label=None, **place: Unpack[Place]):
        """Insert a :class:`~danvas.Plot`. See :meth:`insert` for ``place``."""
        return self._make(Plot, name=name, label=label, **place)

    def live_plot(self, name="liveplot", traces=None, max_points=300,
                  mode="lines", layout=None, smoothing=0.0, label=None,
                  color=None, **place: Unpack[Place]):
        """Insert a :class:`~danvas.LivePlot`. See :meth:`insert` for ``place``.

        ``traces`` only fixes the legend order — pushing an unseen key adds a
        trace on the fly. ``smoothing`` (0–1) is an EMA over each trace;
        ``max_points`` caps the rolling window per trace.
        """
        return self._make(LivePlot, name=name, traces=traces,
                          max_points=max_points, mode=mode, layout=layout,
                          smoothing=smoothing, label=label, color=color,
                          **place)

    def histogram(self, name="histogram", bins=30, mode="heatmap",
                  value_range=None, max_steps=200, label=None, color=None,
                  **place: Unpack[Place]):
        """Insert a :class:`~danvas.Histogram` — a distribution-over-time panel.
        See :meth:`insert` for ``place``.

        Feed it with ``panel.add(values, step)``; needs ``plotly``. ``bins``
        sets the resolution, ``value_range`` pins the bin edges, ``max_steps``
        caps the rolling history, and ``mode`` is ``"heatmap"`` or ``"overlay"``.
        """
        return self._make(Histogram, name=name, bins=bins, mode=mode,
                          value_range=value_range, max_steps=max_steps,
                          label=label, color=color, **place)

    def inspector(self, name="inspector", refresh=1.0, source="components",
                  namespace=None, label=None, **place: Unpack[Place]):
        """Insert an :class:`~danvas.Inspector`. See :meth:`insert` for ``place``.

        ``refresh`` is the auto-refresh period in seconds, so the table stays live
        as panel values and positions change (default ``1.0``); the rebuild only
        runs while serving with a browser connected, so an idle inspector is free.
        Pass ``refresh=None`` to make it manual (the **Refresh** button only).
        """
        return self._make(Inspector, name=name, refresh=refresh, source=source,
                          namespace=namespace, label=label, **place)
