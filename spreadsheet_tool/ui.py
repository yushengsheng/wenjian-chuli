from __future__ import annotations

import tkinter as tk
import tkinter.font as tkfont
from pathlib import Path
from tkinter import ttk

import pandas as pd

from .background_worker import BackgroundWorker
from .dialogs import SourceMappingDialog
from .models import ColumnSetting, FilterRule, SourceSelection, UpdateRule
from .processor_common import DUPLICATE_STRATEGIES
from .ui_compare import UiCompareMixin
from .ui_controls import UiControlsMixin
from .ui_processing import UiProcessingMixin
from .ui_runtime import PendingUiTask, UiRuntimeMixin
from .ui_sources import UiSourcesMixin
from .version import APP_NAME, __version__
from .workflow import build_mapping_session

try:
    from tkinterdnd2 import DND_FILES, TkinterDnD
except ImportError:
    DND_FILES = None
    BaseApp = tk.Tk
else:
    BaseApp = TkinterDnD.Tk


class SpreadsheetApp(
    UiProcessingMixin,
    UiControlsMixin,
    UiCompareMixin,
    UiSourcesMixin,
    UiRuntimeMixin,
    BaseApp,
):
    ROLE_TITLES = {
        "old": "老数据",
        "new": "新数据",
    }

    def __init__(self) -> None:
        super().__init__()
        self.title(f"{APP_NAME} v{__version__}")
        self.geometry("1760x980")
        self.minsize(1440, 820)

        self.sources: dict[str, SourceSelection] = {}
        self.data_cache: dict[str, pd.DataFrame] = {}
        self.filter_rules: list[FilterRule] = []
        self.update_rules: list[UpdateRule] = []
        self.column_settings: dict[str, ColumnSetting] = {}
        self.available_columns: list[str] = []
        self.processed_df: pd.DataFrame | None = None
        self.processed_writeback_df: pd.DataFrame | None = None
        self.last_processed_scope_source_ids: set[str] = set()
        self.drag_enabled = DND_FILES is not None

        self.source_trees: dict[str, ttk.Treeview] = {}
        self.source_summary_labels: dict[str, ttk.Label] = {}
        self.drop_areas: dict[str, tk.Label] = {}
        self.drop_hint_vars: dict[str, tk.StringVar] = {}
        self.active_sheet_source_ids: dict[str, str | None] = {"old": None, "new": None}
        self.compare_before_df: pd.DataFrame | None = None
        self.compare_after_df: pd.DataFrame | None = None
        self.compare_statuses: list[str] = []
        self.compare_changed_columns: list[set[str]] = []
        self.compare_all_before_df: pd.DataFrame | None = None
        self.compare_all_after_df: pd.DataFrame | None = None
        self.compare_all_statuses: list[str] = []
        self.compare_all_changed_columns: list[set[str]] = []
        self.compare_before_total_rows = 0
        self.compare_after_total_rows = 0
        self.compare_column_widths: dict[str, int] = {}
        self.compare_tooltip: tk.Toplevel | None = None
        self.compare_tooltip_label: tk.Label | None = None
        self.compare_scroll_lock = False
        self.compare_x_scroll_lock = False
        self.app_tooltip: tk.Toplevel | None = None
        self.app_tooltip_label: tk.Label | None = None
        self.widget_tooltips: dict[int, object] = {}
        self.tree_preview_frames: dict[int, pd.DataFrame] = {}
        self.tree_preview_after_ids: dict[int, str] = {}
        self.tree_preview_generations: dict[int, int] = {}
        self.resize_after_id: str | None = None
        self.source_sections: dict[str, ttk.LabelFrame] = {}
        self.toolbar_buttons: list[ttk.Button] = []
        self.primary_labels: list[ttk.Label] = []
        self.last_browse_dir: Path | None = None
        self.busy_ttk_widgets: list[tk.Misc] = []
        self.busy_tk_widgets: list[tuple[tk.Misc, str]] = []
        self.background_worker = BackgroundWorker()
        self.pending_ui_task: PendingUiTask | None = None
        self.post_process_action: object | None = None
        self.ui_busy = False
        self.busy_progress: ttk.Progressbar | None = None
        self.close_after_task = False
        self.shutdown_started = False
        self.poll_after_id: str | None = None

        self.duplicate_strategy_var = tk.StringVar(value=DUPLICATE_STRATEGIES["update_and_append"])
        self.compare_changes_only_var = tk.BooleanVar(value=False)
        self.compare_filter_button_text = tk.StringVar(value="仅预览变动")
        self.output_format_var = tk.StringVar(value="Excel (.xlsx)")
        self.output_sheet_var = tk.StringVar(value="处理结果")
        self.include_source_var = tk.BooleanVar(value=True)
        self.freeze_header_var = tk.BooleanVar(value=True)
        self.auto_width_var = tk.BooleanVar(value=True)
        self.style_header_var = tk.BooleanVar(value=True)
        self.status_var = tk.StringVar(value="等待导入老数据和新数据")
        self.operation_pair_var = tk.StringVar(value="当前操作：全部启用数据源。双击老/新数据中的 sheet 可指定配对。")
        self.strategy_full_text_var = tk.StringVar()

        self.strategy_label_to_key = {label: key for key, label in DUPLICATE_STRATEGIES.items()}

        self.font_title = tkfont.Font(family="Microsoft YaHei UI", size=11, weight="bold")
        self.font_body = tkfont.Font(family="Microsoft YaHei UI", size=10)
        self.font_small = tkfont.Font(family="Microsoft YaHei UI", size=9)
        self.font_button = tkfont.Font(family="Microsoft YaHei UI", size=9)
        self.font_tree_heading = tkfont.Font(family="Microsoft YaHei UI", size=9, weight="bold")
        self.font_mono = tkfont.Font(family="Cascadia Mono", size=10)

        self._build_style()
        self._build_layout()
        self.duplicate_strategy_var.trace_add("write", self._on_strategy_changed)
        self.include_source_var.trace_add("write", self._on_include_source_changed)
        self._on_strategy_changed()
        self.bind("<Configure>", self._queue_responsive_refresh)
        self.protocol("WM_DELETE_WINDOW", self.on_close)
        self.after(120, self._refresh_responsive_ui)
        self.poll_after_id = self.after(80, self._poll_background_results)

    def _build_style(self) -> None:
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass
        self.configure(bg="#f3f4f6")
        style.configure(".", background="#f3f4f6", foreground="#1f2937", font=self.font_body)
        style.configure("TFrame", background="#f3f4f6")
        style.configure("TLabel", background="#f3f4f6", font=self.font_body)
        style.configure("Title.TLabel", font=self.font_title, foreground="#111827", background="#f3f4f6")
        style.configure("Summary.TLabel", foreground="#667085", background="#f3f4f6", font=self.font_small)
        style.configure(
            "TLabelframe",
            background="#fbfbfc",
            bordercolor="#d7dbe2",
            lightcolor="#d7dbe2",
            darkcolor="#d7dbe2",
            relief="solid",
            borderwidth=1,
        )
        style.configure("TLabelframe.Label", background="#f3f4f6", foreground="#111827", font=self.font_title)
        style.configure(
            "TButton",
            padding=(10, 6),
            background="#ffffff",
            foreground="#1f2937",
            bordercolor="#d0d5dd",
            focuscolor="#ffffff",
            font=self.font_button,
        )
        style.map(
            "TButton",
            background=[("active", "#eef2f7"), ("pressed", "#e5e7eb")],
            bordercolor=[("active", "#b9c2cf")],
        )
        style.configure("Compact.TButton", padding=(6, 3), font=self.font_button)
        style.configure(
            "Danger.TButton",
            padding=(6, 3),
            font=self.font_button,
            background="#fff1f1",
            foreground="#b42318",
            bordercolor="#f2b8b5",
        )
        style.map(
            "Danger.TButton",
            background=[("active", "#ffe3e3"), ("pressed", "#ffd1d1")],
            bordercolor=[("active", "#e59a95")],
            foreground=[("active", "#912018")],
        )
        style.configure(
            "Treeview",
            rowheight=24,
            font=self.font_small,
            background="#ffffff",
            fieldbackground="#ffffff",
            bordercolor="#d7dbe2",
        )
        style.configure(
            "Treeview.Heading",
            font=self.font_tree_heading,
            background="#eef1f5",
            foreground="#111827",
            bordercolor="#d7dbe2",
        )
        style.configure(
            "Source.Treeview",
            rowheight=22,
            font=self.font_small,
            background="#ffffff",
            fieldbackground="#ffffff",
            bordercolor="#d7dbe2",
        )
        style.configure(
            "Source.Treeview.Heading",
            font=self.font_tree_heading,
            background="#eef1f5",
            foreground="#111827",
            bordercolor="#d7dbe2",
        )
        style.map(
            "Source.Treeview",
            background=[("selected", "#d62828")],
            foreground=[("selected", "#ffffff")],
        )
        style.configure(
            "TNotebook",
            background="#f3f4f6",
            tabmargins=(0, 0, 0, 0),
        )
        style.configure(
            "TNotebook.Tab",
            background="#e8ebf0",
            foreground="#475467",
            padding=(14, 6),
            font=self.font_small,
        )
        style.map(
            "TNotebook.Tab",
            background=[("selected", "#ffffff"), ("active", "#eef2f7")],
            foreground=[("selected", "#111827")],
        )

    def _build_layout(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        toolbar = ttk.Frame(self, padding=(12, 12, 12, 6))
        toolbar.grid(row=0, column=0, sticky="ew")
        toolbar.columnconfigure(11, weight=1)
        toolbar.configure(style="TFrame")

        old_btn = self._make_toolbar_button(toolbar, "导入老数据", lambda: self.import_files_for_role("old"))
        old_btn.grid(row=0, column=0, padx=(0, 8))
        self.register_tooltip(old_btn, "导入旧模板或历史数据，作为本次合并更新的基底。")
        new_btn = self._make_toolbar_button(toolbar, "导入新数据", lambda: self.import_files_for_role("new"))
        new_btn.grid(row=0, column=1, padx=(0, 8))
        self.register_tooltip(new_btn, "导入增量数据或最新数据，用于补全或覆盖老数据。")
        remove_btn = self._make_toolbar_button(toolbar, "移除选中源", self.remove_selected_sources)
        remove_btn.grid(row=0, column=2, padx=(0, 8))
        self.register_tooltip(remove_btn, "移除当前选中的数据源记录。")
        enable_btn = self._make_toolbar_button(toolbar, "启用选中", lambda: self.set_selected_sources_enabled(True))
        enable_btn.grid(row=0, column=3, padx=(0, 8))
        self.register_tooltip(enable_btn, "启用当前选中的数据源，参与处理。")
        disable_btn = self._make_toolbar_button(toolbar, "禁用选中", lambda: self.set_selected_sources_enabled(False))
        disable_btn.grid(row=0, column=4, padx=(0, 8))
        self.register_tooltip(disable_btn, "禁用当前选中的数据源，不参与处理。")
        col_btn = self._make_toolbar_button(toolbar, "字段设置", self.open_column_settings)
        col_btn.grid(row=0, column=5, padx=(0, 8))
        self.register_tooltip(col_btn, "控制导出字段的显示、隐藏和重命名。")
        map_btn = self._make_toolbar_button(toolbar, "字段匹配", self.open_mapping_dialog)
        map_btn.grid(row=0, column=6, padx=(0, 8))
        self.register_tooltip(map_btn, "检查或手动修正新数据字段映射。")
        run_btn = self._make_toolbar_button(toolbar, "应用处理", self.apply_processing)
        run_btn.grid(row=0, column=7, padx=(0, 8))
        self.register_tooltip(run_btn, "按当前主键、字段映射和策略执行数据合并。")
        export_btn = self._make_toolbar_button(toolbar, "导出文件", self.export_processed_file)
        export_btn.grid(row=0, column=8, padx=(0, 8))
        self.register_tooltip(export_btn, "导出处理后的完整老文件（保留全部 sheet，仅替换当前处理目标 sheet）。")
        writeback_btn = self._make_toolbar_button(toolbar, "写入老数据", self.apply_processed_to_old_source)
        writeback_btn.grid(row=0, column=9, padx=(0, 8))
        self.register_tooltip(writeback_btn, "将已处理结果直接写回老数据文件中的目标工作表。")
        ttk.Label(toolbar, textvariable=self.status_var, anchor="e", style="Summary.TLabel").grid(
            row=0,
            column=11,
            sticky="ew",
        )

        paned = ttk.Panedwindow(self, orient=tk.HORIZONTAL)
        paned.grid(row=1, column=0, sticky="nsew", padx=12, pady=(0, 12))
        self.main_paned = paned

        source_frame = ttk.Frame(paned, padding=12)
        preview_frame = ttk.Frame(paned, padding=12)
        control_frame = ttk.Frame(paned, padding=12)

        paned.add(source_frame, weight=2)
        paned.add(preview_frame, weight=14)
        paned.add(control_frame, weight=2)

        self._build_source_panel(source_frame)
        self._build_preview_panel(preview_frame)
        self._build_control_panel(control_frame)
        self.after(80, self._set_initial_pane_positions)

        status_frame = ttk.Frame(self, relief=tk.GROOVE, padding=(8, 4))
        status_frame.grid(row=2, column=0, sticky="ew")
        status_frame.columnconfigure(0, weight=1)
        status_bar = ttk.Label(status_frame, textvariable=self.status_var, anchor="w")
        status_bar.grid(row=0, column=0, sticky="ew")
        status_bar.configure(background="#eef2f7", foreground="#475467")
        busy_progress = ttk.Progressbar(status_frame, mode="indeterminate", length=220)
        busy_progress.grid(row=0, column=1, sticky="e", padx=(12, 0))
        busy_progress.grid_remove()
        self.busy_progress = busy_progress

    def _set_initial_pane_positions(self) -> None:
        self.update_idletasks()
        total_width = self.winfo_width()
        if total_width <= 0:
            return
        left_width = max(250, min(320, int(total_width * 0.18)))
        right_width = max(260, min(320, int(total_width * 0.18)))
        try:
            self.main_paned.sashpos(0, left_width)
            self.main_paned.sashpos(1, total_width - right_width)
        except tk.TclError:
            pass
