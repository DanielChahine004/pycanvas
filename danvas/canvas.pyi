"""Type hints for canvas.py — consumed by mypy/pyright, not at runtime."""

import concurrent.futures
import threading
from typing import Any, Callable, Literal, TypeVar, overload

from .arrow import Arrow
from .components import (
    AudioFeed,
    BaseComponent,
    Button,
    Chat,
    Custom,
    FileBrowser,
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
    VideoFeed,
    WebView,
)

_C = TypeVar("_C", bound=BaseComponent)

class _FlowLayout:
    def __enter__(self) -> _FlowLayout: ...
    def __exit__(self, *exc: Any) -> bool: ...
    def refit(self) -> _FlowLayout: ...

class _Container:
    def __enter__(self) -> _Container: ...
    def __exit__(self, *exc: Any) -> None: ...
    def add(self, panel: Any) -> Any: ...
    def insert_before(self, ref: Any, panel: Any) -> Any: ...
    def insert_after(self, ref: Any, panel: Any) -> Any: ...
    def row(self, x: float | None = ..., y: float | None = ...,
            h: float | None = ..., gap: float = ...) -> _Container: ...
    def column(self, x: float | None = ..., y: float | None = ...,
               w: float | None = ..., gap: float = ...) -> _Container: ...
    def move(self, x: float, y: float) -> _Container: ...
    def reflow(self) -> _Container: ...

class Canvas:
    def __init__(self) -> None: ...

    # -- lifecycle ------------------------------------------------------------
    def background(
        self, fn: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Callable[..., Any]: ...
    def serve(
        self,
        port: int = ...,
        open_browser: bool = ...,
        host: str = ...,
        block: bool = ...,
        wait: bool = ...,
        tunnel: bool = ...,
        tunnel_provider: str = ...,
        ui_inspector: bool | None = ...,
        cursors: bool | None = ...,
        view: dict[str, Any] | None = ...,
        desktop: bool | None = ...,
        window_title: str = ...,
        window_size: tuple[int, int] = ...,
        password: str | None = ...,
        passwords: dict[str, str] | None = ...,
        login_message: str | None = ...,
        hot_reload: bool = ...,
        debug: bool = ...,
        namespace: dict[str, Any] | None = ...,
        tldraw_license_key: str | None = ...,
        watch: str | list[str] | None = ...,
    ) -> Canvas | None: ...
    def stop(self) -> None: ...
    def wait(self) -> None: ...
    def bake(
        self,
        name: str = ...,
        *,
        icon: str | None = ...,
        onefile: bool = ...,
        windowed: bool = ...,
        distpath: str = ...,
        entry: str | None = ...,
        exclude: list[str] | None = ...,
        include: list[str] | None = ...,
        window_size: tuple[int, int] = ...,
        port: int = ...,
    ) -> Any: ...

    # -- panels ---------------------------------------------------------------
    def insert(
        self,
        component: _C,
        x: float | None = ...,
        y: float | None = ...,
        w: float | str | None = ...,
        h: float | str | None = ...,
        rotation: float | None = ...,
        locked: bool = ...,
        draggable: bool = ...,
        resizable: bool = ...,
        operable: bool = ...,
        grabbable: bool = ...,
        frame: bool = ...,
        decorative: bool = ...,
        name: str | None = ...,
        queue: Literal["fifo", "latest"] | None = ...,
        below: BaseComponent | str | None = ...,
        above: BaseComponent | str | None = ...,
        right_of: BaseComponent | str | None = ...,
        left_of: BaseComponent | str | None = ...,
        gap: float = ...,
        width: float | str | None = ...,
        height: float | str | None = ...,
    ) -> _C: ...
    def remove(self, component: BaseComponent) -> BaseComponent | None: ...
    def clear(self) -> Canvas: ...
    def show(
        self, value: Any, name: str | None = ..., label: str | None = ..., **place: Any
    ) -> BaseComponent: ...

    # -- factory methods ------------------------------------------------------
    def slider(
        self,
        name: str = ...,
        min: float = ...,
        max: float = ...,
        default: float | None = ...,
        step: float = ...,
        on_release: bool = ...,
        color: tuple[int, int, int] | str | None = ...,
        label: str | None = ...,
        **place: Any,
    ) -> Slider: ...
    def toggle(
        self,
        options: Any,
        name: str = ...,
        default: Any = ...,
        color: tuple[int, int, int] | str | None = ...,
        label: str | None = ...,
        **place: Any,
    ) -> Toggle: ...
    def button(
        self,
        name: str = ...,
        text: str | None = ...,
        color: tuple[int, int, int] | str | None = ...,
        label: str | None = ...,
        **place: Any,
    ) -> Button: ...
    def label(
        self, name: str = ..., value: str = ..., color: tuple[int, int, int] | str | None = ...,
        label: str | None = ..., **place: Any
    ) -> Label: ...
    def video(
        self, name: str = ..., quality: int = ..., label: str | None = ..., **place: Any
    ) -> VideoFeed: ...
    def audio(
        self,
        name: str = ...,
        sample_rate: int = ...,
        channels: int = ...,
        label: str | None = ...,
        **place: Any,
    ) -> AudioFeed: ...
    def chat(
        self, name: str = ..., label: str | None = ..., **place: Any
    ) -> Chat: ...
    def custom(
        self,
        html: str | None = ...,
        path: str | None = ...,
        css: str | None = ...,
        js: str | None = ...,
        name: str = ...,
        label: str | None = ...,
        keep_mounted: bool = ...,
        forward_wheel: bool = ...,
        themed: bool = ...,
        permissions: str | list[str] | None = ...,
        **place: Any,
    ) -> Custom: ...
    def file_browser(
        self,
        name: str = ...,
        root: str = ...,
        label: str | None = ...,
        pattern: str | None = ...,
        show_hidden: bool = ...,
        **place: Any,
    ) -> FileBrowser: ...
    def react(
        self,
        source: str | None = ...,
        path: str | None = ...,
        jsx: str | None = ...,
        css: str | None = ...,
        css_path: str | None = ...,
        name: str = ...,
        label: str | None = ...,
        props: dict[str, Any] | None = ...,
        scope: list[str] | None = ...,
        **place: Any,
    ) -> React: ...
    def markdown(
        self, text: str = ..., name: str = ..., color: tuple[int, int, int] | str | None = ...,
        label: str | None = ..., **place: Any
    ) -> Markdown: ...
    def image(
        self,
        src: Any,
        name: str = ...,
        label: str | None = ...,
        fit: str = ...,
        **place: Any,
    ) -> Image: ...
    def table(
        self, data: Any, name: str = ..., label: str | None = ..., **place: Any
    ) -> Table: ...
    def text_field(
        self,
        name: str = ...,
        placeholder: str = ...,
        default: str = ...,
        multiline: bool = ...,
        color: tuple[int, int, int] | str | None = ...,
        label: str | None = ...,
        **place: Any,
    ) -> TextField: ...
    def webview(
        self, url: str, name: str = ..., label: str | None = ..., **place: Any
    ) -> WebView: ...
    def plot(
        self, name: str = ..., label: str | None = ..., **place: Any
    ) -> Plot: ...
    def live_plot(self, name: str = ..., **kw: Any) -> LivePlot: ...
    def histogram(self, name: str = ..., **kw: Any) -> Histogram: ...
    def inspector(
        self,
        name: str = ...,
        refresh: float | None = ...,
        source: str = ...,
        namespace: dict[str, Any] | None = ...,
        color: tuple[int, int, int] | str | None = ...,
        label: str | None = ...,
        **place: Any,
    ) -> Inspector: ...

    # -- arrows ---------------------------------------------------------------
    def connect(
        self,
        start: BaseComponent,
        end: BaseComponent,
        name: str | None = ...,
        text: str | None = ...,
        **props: Any,
    ) -> Arrow: ...
    def disconnect(self, arrow: Arrow | str) -> Arrow | None: ...
    def serve_bytes(
        self,
        data: bytes | bytearray | memoryview | str,
        filename: str = ...,
        ttl: float = ...,
        role: str | None = ...,
    ) -> str: ...
    def receive_files(
        self,
        on_file: Callable[..., Any],
        *,
        dest: str | None = ...,
        max_size: int | None = ...,
        role: str | None = ...,
    ) -> str: ...

    # -- layout containers ----------------------------------------------------
    def grid(
        self,
        cols: int = ...,
        slot: tuple[float, float] = ...,
        gap: float = ...,
        origin: tuple[float, float] = ...,
    ) -> _FlowLayout: ...
    def column(
        self,
        x: float | None = ...,
        y: float | None = ...,
        w: float | None = ...,
        gap: float = ...,
    ) -> _Container: ...
    def row(
        self,
        x: float | None = ...,
        y: float | None = ...,
        h: float | None = ...,
        gap: float = ...,
    ) -> _Container: ...
    def streamlit(
        self,
        gap: float = ...,
        padding: float = ...,
    ) -> _Container: ...

    # -- save / load ----------------------------------------------------------
    @overload
    def save(
        self, path: str, timeout: float = ..., *, blocking: Literal[True] = ...
    ) -> Canvas: ...
    @overload
    def save(
        self, path: str, timeout: float = ..., *, blocking: Literal[False]
    ) -> concurrent.futures.Future[Canvas]: ...
    def load(
        self, source: str | dict[str, Any], formation: bool = ...
    ) -> Canvas: ...
    def wait_for_client(self, timeout: float = ...) -> bool: ...

    # -- view -----------------------------------------------------------------
    def set_view(
        self,
        view: dict[str, Any] | None = ...,
        client_id: str | None = ...,
        **opts: Any,
    ) -> Canvas: ...

    # -- observers ------------------------------------------------------------
    def on_frame(
        self, fn: Callable[[str, dict[str, Any]], Any]
    ) -> Callable[[str, dict[str, Any]], Any]: ...
    def off_frame(self, fn: Callable[[str, dict[str, Any]], Any]) -> None: ...
    def on_dispatch(
        self, fn: Callable[[dict[str, Any]], Any]
    ) -> Callable[[dict[str, Any]], Any]: ...
    def off_dispatch(self, fn: Callable[[dict[str, Any]], Any]) -> None: ...
    def trace_calls(self, enabled: bool = ...) -> "Canvas": ...
    def trace_history(self) -> list[dict[str, Any]]: ...
    def trace(
        self, name: str = ..., label: str = ..., deep: bool = ..., **place: Any
    ) -> Any: ...
    def on_cursor(
        self, fn: Callable[[dict[str, Any]], Any]
    ) -> Callable[[dict[str, Any]], Any]: ...
    def off_cursor(self, fn: Callable[[dict[str, Any]], Any]) -> None: ...

    # -- cell capture ---------------------------------------------------------
    def capture_cells(
        self,
        cols: int = ...,
        slot_w: float = ...,
        slot_h: float = ...,
        gap: float = ...,
        origin: tuple[float, float] = ...,
        include_source: bool = ...,
        auto: bool = ...,
        draggable: bool = ...,
        resizable: bool = ...,
        locked: bool = ...,
        operable: bool = ...,
    ) -> Any: ...
    def stop_capturing_cells(self) -> Canvas: ...

    # -- properties -----------------------------------------------------------
    @property
    def components(self) -> list[BaseComponent]: ...
    @property
    def arrows(self) -> list[Arrow]: ...
    @property
    def viewers(self) -> list[dict[str, Any]]: ...

    # -- attribute / item access ----------------------------------------------
    def __getattr__(self, name: str) -> BaseComponent | Arrow: ...
    def __getitem__(self, name: str) -> BaseComponent | Arrow: ...
    def __contains__(self, name: str) -> bool: ...
