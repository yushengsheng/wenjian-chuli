from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import tkinter as tk
import tkinter.font as tkfont
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

import pandas as pd

from .background_worker import BackgroundTaskResult, BackgroundWorker
from .comparison import (
    align_for_comparison,
    build_baseline_dataframe,
    build_baseline_source_dataframe,
    get_ignored_compare_columns,
    preview_value as format_preview_value,
)
from .compare_render import (
    PREVIEW_ROW_NUMBER_COLUMN,
    PREVIEW_ROW_NUMBER_LABEL,
    build_compare_display_columns,
    build_comparison_info,
    compute_compare_column_widths,
    display_compare_column_name,
    filter_comparison_rows,
    fit_compare_text,
    marker_for_cell,
    summarize_comparison_statuses,
)
from .dialogs import ColumnSettingsDialog, FilterRuleDialog, SourceMappingDialog, UpdateRuleDialog
from .export_workflow import (
    apply_writeback_result,
    build_csv_export_summary,
    build_workbook_export_plan,
    build_workbook_export_summary,
    output_format_for_source,
)
from .models import ColumnSetting, ExportSettings, FilterRule, PipelineConfig, SourceSelection, UpdateRule
from .processor import (
    DUPLICATE_STRATEGIES,
    FILTER_OPERATORS,
    INTERNAL_COLUMNS,
    UPDATE_MODES,
    apply_column_settings,
    collect_target_columns,
    collect_available_columns,
    default_display_name,
    default_visible,
    export_dataframe,
    export_dataframe_with_old_workbook,
    infer_source_column_kind,
    load_sources_from_paths,
    paths_refer_to_same_file,
    process_dataframe,
    write_dataframe_back_to_source,
)
from .source_ops import (
    MISSING_OLD,
    MULTIPLE_OLD,
    PAIR_INCOMPLETE,
    expand_input_paths as collect_input_paths,
    get_column_scope_sources as collect_column_scope_sources,
    get_last_processed_scope_sources as collect_last_processed_scope_sources,
    get_mapping_scope_sources as collect_mapping_scope_sources,
    resolve_processing_scope_sources,
    resolve_writeback_target_source as choose_writeback_target_source,
)
from .workflow import (
    MISSING_OLD as PROCESSING_MISSING_OLD,
    NO_DATA as PROCESSING_NO_DATA,
    apply_imported_sources,
    build_mapping_session,
    prepare_processing,
)
from .version import APP_NAME, __version__

try:
    from tkinterdnd2 import DND_FILES, TkinterDnD
except ImportError:
    DND_FILES = None
    BaseApp = tk.Tk
else:
    BaseApp = TkinterDnD.Tk


@dataclass(slots=True)
class PendingUiTask:
    task_name: str
    success_handler: object
    error_handler: object


class SpreadsheetApp(BaseApp):
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

    def _make_toolbar_button(self, parent: ttk.Frame, text: str, command) -> ttk.Button:
        button = ttk.Button(parent, text=text, command=command)
        self.toolbar_buttons.append(button)
        return button

    def register_busy_ttk_widget(self, widget: tk.Misc) -> None:
        self.busy_ttk_widgets.append(widget)

    def register_busy_tk_widget(self, widget: tk.Misc, normal_state: str = "normal") -> None:
        self.busy_tk_widgets.append((widget, normal_state))

    def register_tooltip(self, widget: tk.Misc, text_or_callable) -> None:
        self.widget_tooltips[id(widget)] = text_or_callable
        widget.bind("<Enter>", self._show_widget_tooltip, add="+")
        widget.bind("<Motion>", self._show_widget_tooltip, add="+")
        widget.bind("<Leave>", self._hide_widget_tooltip, add="+")

    def _show_widget_tooltip(self, event: object) -> None:
        widget = getattr(event, "widget", None)
        if widget is None:
            return
        source = self.widget_tooltips.get(id(widget))
        if source is None:
            return
        text = source() if callable(source) else source
        if not text:
            self._hide_widget_tooltip()
            return
        if self.app_tooltip is None:
            tooltip = tk.Toplevel(self)
            tooltip.withdraw()
            tooltip.overrideredirect(True)
            tooltip.attributes("-topmost", True)
            label = tk.Label(
                tooltip,
                text="",
                justify="left",
                anchor="w",
                bg="#fff8d9",
                relief="solid",
                bd=1,
                padx=8,
                pady=6,
                wraplength=420,
                font=self.font_small,
            )
            label.pack()
            self.app_tooltip = tooltip
            self.app_tooltip_label = label
        self.app_tooltip_label.configure(text=text, font=self.font_small)
        self.app_tooltip.geometry(f"+{int(getattr(event, 'x_root', 0)) + 14}+{int(getattr(event, 'y_root', 0)) + 18}")
        self.app_tooltip.deiconify()

    def _hide_widget_tooltip(self, _: object | None = None) -> None:
        if self.app_tooltip is not None:
            self.app_tooltip.withdraw()

    def _on_strategy_changed(self, *_: object) -> None:
        self.strategy_full_text_var.set(self.duplicate_strategy_var.get())
        if self.processed_df is not None:
            self.invalidate_processed_results("主键策略已变化，请重新执行“应用处理”。")

    def _on_include_source_changed(self, *_: object) -> None:
        if self.processed_df is not None:
            self.invalidate_processed_results("输出字段设置已变化，请重新执行“应用处理”。")

    def set_ui_busy(self, busy: bool, status_text: str | None = None, cursor: str = "watch") -> None:
        self.ui_busy = busy
        self.configure(cursor=cursor if busy else "")
        for button in self.toolbar_buttons:
            if busy:
                button.state(["disabled"])
            else:
                button.state(["!disabled"])
        for widget in self.busy_ttk_widgets:
            if busy:
                widget.state(["disabled"])
            else:
                widget.state(["!disabled"])
        for widget, normal_state in self.busy_tk_widgets:
            widget.configure(state="disabled" if busy else normal_state)
        busy_progress = getattr(self, "busy_progress", None)
        if busy_progress is not None:
            if busy:
                busy_progress.grid()
                busy_progress.start(12)
            else:
                busy_progress.stop()
                busy_progress.grid_remove()
        if status_text:
            self.status_var.set(status_text)
        self.update_idletasks()

    @contextmanager
    def busy_state(self, cursor: str = "watch"):
        self.set_ui_busy(True, cursor=cursor)
        try:
            yield
        finally:
            self.set_ui_busy(False)

    def dialog_initialdir(self) -> str:
        return str(self.last_browse_dir) if self.last_browse_dir is not None else str(Path.cwd())

    def remember_browse_path(self, selected_path: str | Path | None) -> None:
        if not selected_path:
            return
        path = Path(selected_path)
        self.last_browse_dir = path if path.is_dir() else path.parent

    def invalidate_processed_results(self, status_text: str | None = None) -> None:
        self.clear_processed_results()
        if status_text:
            self.status_var.set(status_text)

    def action_allowed(self, busy_message: str = "后台任务执行中，请稍候。") -> bool:
        if self.pending_ui_task is None:
            return True
        self.status_var.set(busy_message)
        self.bell()
        return False

    def start_background_task(
        self,
        task_name: str,
        task_func,
        success_handler,
        error_handler,
        busy_message: str,
    ) -> bool:
        if self.pending_ui_task is not None:
            self.status_var.set("后台任务执行中，请稍候。")
            self.bell()
            return False
        self.pending_ui_task = PendingUiTask(
            task_name=task_name,
            success_handler=success_handler,
            error_handler=error_handler,
        )
        self.set_ui_busy(True, busy_message)
        self.background_worker.submit(task_name, task_func)
        return True

    def _poll_background_results(self) -> None:
        if self.shutdown_started:
            return
        for result in self.background_worker.poll_results():
            self._handle_background_result(result)
        if not self.shutdown_started:
            self.poll_after_id = self.after(80, self._poll_background_results)

    def _handle_background_result(self, result: BackgroundTaskResult) -> None:
        pending = self.pending_ui_task
        if pending is None:
            return
        self.pending_ui_task = None
        self.set_ui_busy(False)
        try:
            if result.error is not None:
                pending.error_handler(result.error)
            else:
                pending.success_handler(result.payload)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("操作失败", str(exc), parent=self)
            self.status_var.set("操作失败")
        finally:
            if self.close_after_task and self.pending_ui_task is None:
                self._shutdown_and_destroy()

    def on_close(self) -> None:
        if self.shutdown_started:
            return
        if self.close_after_task and self.pending_ui_task is not None:
            self.status_var.set("后台任务完成后将自动关闭，请稍候。")
            return
        if self.pending_ui_task is not None:
            confirmed = messagebox.askyesno(
                "任务进行中",
                "后台任务仍在执行。为避免中断写回或导出并损坏文件，程序会等待当前任务完成后再关闭。是否继续？",
                parent=self,
            )
            if not confirmed:
                return
            self.close_after_task = True
            self.status_var.set("正在等待后台任务完成后关闭，请稍候。")
            return
        self._shutdown_and_destroy()

    def _shutdown_and_destroy(self) -> None:
        if self.shutdown_started:
            return
        self.shutdown_started = True
        if self.poll_after_id is not None:
            try:
                self.after_cancel(self.poll_after_id)
            except tk.TclError:
                pass
            self.poll_after_id = None
        if self.resize_after_id is not None:
            try:
                self.after_cancel(self.resize_after_id)
            except tk.TclError:
                pass
            self.resize_after_id = None
        self.hide_compare_tooltip()
        self._hide_widget_tooltip()
        self.background_worker.shutdown(wait=True)
        super().destroy()

    def destroy(self) -> None:
        if not self.shutdown_started:
            self._shutdown_and_destroy()
            return
        super().destroy()

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
        old_btn.grid(
            row=0, column=0, padx=(0, 8)
        )
        self.register_tooltip(old_btn, "导入旧模板或历史数据，作为本次合并更新的基底。")
        new_btn = self._make_toolbar_button(toolbar, "导入新数据", lambda: self.import_files_for_role("new"))
        new_btn.grid(
            row=0, column=1, padx=(0, 8)
        )
        self.register_tooltip(new_btn, "导入增量数据或最新数据，用于补全或覆盖老数据。")
        remove_btn = self._make_toolbar_button(toolbar, "移除选中源", self.remove_selected_sources)
        remove_btn.grid(row=0, column=2, padx=(0, 8))
        self.register_tooltip(remove_btn, "移除当前选中的数据源记录。")
        enable_btn = self._make_toolbar_button(toolbar, "启用选中", lambda: self.set_selected_sources_enabled(True))
        enable_btn.grid(
            row=0, column=3, padx=(0, 8)
        )
        self.register_tooltip(enable_btn, "启用当前选中的数据源，参与处理。")
        disable_btn = self._make_toolbar_button(toolbar, "禁用选中", lambda: self.set_selected_sources_enabled(False))
        disable_btn.grid(
            row=0, column=4, padx=(0, 8)
        )
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
            row=0, column=11, sticky="ew"
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

    def _queue_responsive_refresh(self, event: object | None = None) -> None:
        if event is not None and getattr(event, "widget", None) is not self:
            return
        if self.resize_after_id is not None:
            self.after_cancel(self.resize_after_id)
        self.resize_after_id = self.after(70, self._refresh_responsive_ui)

    def _refresh_responsive_ui(self) -> None:
        self.resize_after_id = None
        width = max(self.winfo_width(), 1440)
        height = max(self.winfo_height(), 820)
        scale = min(max(min(width / 1760, height / 980), 0.88), 1.22)

        self.font_title.configure(size=max(10, round(11 * scale)))
        self.font_body.configure(size=max(9, round(10 * scale)))
        self.font_small.configure(size=max(8, round(9 * scale)))
        self.font_button.configure(size=max(8, round(9 * scale)))
        self.font_tree_heading.configure(size=max(8, round(9 * scale)))
        self.font_mono.configure(size=max(9, round(10 * scale)))

        style = ttk.Style()
        style.configure("Source.Treeview", rowheight=max(20, round(22 * scale)))
        style.configure("Treeview", rowheight=max(20, round(24 * scale)))
        style.configure("TNotebook.Tab", padding=(max(10, round(14 * scale)), max(4, round(6 * scale))))
        style.configure("TButton", padding=(max(8, round(10 * scale)), max(5, round(6 * scale))))
        style.configure("Compact.TButton", padding=(max(4, round(6 * scale)), max(2, round(3 * scale))))
        style.configure("Danger.TButton", padding=(max(4, round(6 * scale)), max(2, round(3 * scale))))

        for text_widget in [getattr(self, "compare_before_text", None), getattr(self, "compare_after_text", None)]:
            if text_widget is None:
                continue
            text_widget.configure(font=self.font_mono)
            mono_bold = (self.font_mono.actual("family"), self.font_mono.actual("size"), "bold")
            text_widget.tag_configure("header", font=mono_bold)
            text_widget.tag_configure("plus", font=mono_bold)
            text_widget.tag_configure("minus", font=mono_bold)
        if self.compare_tooltip_label is not None:
            self.compare_tooltip_label.configure(font=self.font_mono)

        self._resize_source_trees()
        self._resize_dynamic_texts()

    def _resize_source_trees(self) -> None:
        for role, tree in self.source_trees.items():
            width = tree.winfo_width()
            if width <= 80:
                continue
            file_width = max(100, int(width * 0.46))
            sheet_width = max(72, int(width * 0.28))
            rows_width = max(42, int(width * 0.12))
            status_width = max(42, width - file_width - sheet_width - rows_width - 32)
            tree.column("#0", width=file_width, minwidth=84)
            tree.column("sheet", width=sheet_width, minwidth=70)
            tree.column("rows", width=rows_width, minwidth=40)
            tree.column("status", width=status_width, minwidth=40)
            tree.configure(height=max(6, min(12, round(self.winfo_height() / 135))))

    def _resize_dynamic_texts(self) -> None:
        source_width = max(self.winfo_width() * 0.18, 240)
        if hasattr(self, "operation_pair_label"):
            self.operation_pair_label.configure(wraplength=max(180, int(source_width - 24)))
        for role, label in self.source_summary_labels.items():
            label.configure(wraplength=max(160, int(source_width - 30)))
        for role, area in self.drop_areas.items():
            area.configure(
                font=self.font_small,
                wraplength=max(160, int(source_width - 40)),
                padx=max(8, round(10 * (self.font_small.cget("size") / 9))),
                pady=max(6, round(8 * (self.font_small.cget("size") / 9))),
            )
        if hasattr(self, "compare_info_label"):
            self.compare_info_label.configure(wraplength=max(360, int(self.winfo_width() * 0.45)))

    def _build_source_panel(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(2, weight=1)
        parent.rowconfigure(3, weight=1)

        title_label = ttk.Label(parent, text="数据源", style="Title.TLabel")
        title_label.grid(row=0, column=0, sticky="w")
        self.primary_labels.append(title_label)
        self.operation_pair_label = ttk.Label(parent, textvariable=self.operation_pair_var, wraplength=290, style="Summary.TLabel")
        self.operation_pair_label.grid(row=1, column=0, sticky="w", pady=(4, 0))
        old_frame = self._create_source_section(parent, "old")
        new_frame = self._create_source_section(parent, "new")
        old_frame.grid(row=2, column=0, sticky="nsew", pady=(8, 8))
        new_frame.grid(row=3, column=0, sticky="nsew")

    def _create_source_section(self, parent: ttk.Frame, role: str) -> ttk.LabelFrame:
        frame = ttk.LabelFrame(parent, text=self.ROLE_TITLES[role], padding=8)
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(2, weight=1)
        self.source_sections[role] = frame

        button_bar = ttk.Frame(frame)
        button_bar.grid(row=0, column=0, sticky="ew")
        import_btn = self._make_toolbar_button(button_bar, f"导入{self.ROLE_TITLES[role]}", lambda r=role: self.import_files_for_role(r))
        import_btn.pack(side=tk.LEFT)
        remove_role_btn = ttk.Button(
            button_bar,
            text="移除选中文件",
            style="Danger.TButton",
            command=lambda r=role: self.remove_selected_files(r),
            width=11,
        )
        remove_role_btn.pack(side=tk.LEFT, padx=(8, 0))
        self.register_busy_ttk_widget(remove_role_btn)
        self.register_tooltip(import_btn, f"导入{self.ROLE_TITLES[role]}中的表格文件。")
        self.register_tooltip(remove_role_btn, f"删除当前选中的{self.ROLE_TITLES[role]}文件及其全部 sheet。")

        drop_hint = (
            f"把文件拖到这里导入{self.ROLE_TITLES[role]}"
            if self.drag_enabled
            else f"点击上方按钮导入{self.ROLE_TITLES[role]}"
        )
        hint_var = tk.StringVar(value=drop_hint)
        self.drop_hint_vars[role] = hint_var

        drop_area = tk.Label(
            frame,
            textvariable=hint_var,
            justify="left",
            anchor="w",
            padx=10,
            pady=8,
            relief="ridge",
            bd=1,
            bg="#f7faff",
            fg="#344054",
        )
        drop_area.grid(row=1, column=0, sticky="ew", pady=(8, 8))
        self.drop_areas[role] = drop_area

        container = ttk.Frame(frame)
        container.grid(row=2, column=0, sticky="nsew")
        container.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=1)

        columns = ("sheet", "rows", "status")
        tree = ttk.Treeview(container, columns=columns, show="tree headings", height=7, style="Source.Treeview")
        tree.heading("#0", text="文件")
        tree.heading("sheet", text="工作表")
        tree.heading("rows", text="行数")
        tree.heading("status", text="状态")
        tree.column("#0", width=165, anchor="w", minwidth=100)
        tree.column("sheet", width=85, anchor="w")
        tree.column("rows", width=55, anchor="center")
        tree.column("status", width=55, anchor="center")
        tree.grid(row=0, column=0, sticky="nsew")
        tree.bind("<<TreeviewSelect>>", lambda event, active_role=role: self.on_source_selected(active_role, event))
        tree.bind("<Double-1>", lambda event, active_role=role: self.on_source_double_click(active_role, event))
        tree.tag_configure("file_parent", background="#dff6df", foreground="#14532d")
        tree.tag_configure("active_pair", background="#ffd6d6", foreground="#8b0000")

        scrollbar = ttk.Scrollbar(container, orient=tk.VERTICAL, command=tree.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        tree.configure(yscrollcommand=scrollbar.set)
        self.source_trees[role] = tree

        summary_label = ttk.Label(frame, text="未导入数据", style="Summary.TLabel")
        summary_label.grid(row=3, column=0, sticky="w", pady=(8, 0))
        self.source_summary_labels[role] = summary_label

        if self.drag_enabled:
            self._register_drop_target(drop_area, role)
            self._register_drop_target(tree, role)

        self.register_tooltip(drop_area, f"把文件直接拖到这里，导入到{self.ROLE_TITLES[role]}分组。")
        self.bind_treeview_tooltip(tree)

        return frame

    def _register_drop_target(self, widget: tk.Misc, role: str) -> None:
        if DND_FILES is None:
            return
        widget.drop_target_register(DND_FILES)
        widget.dnd_bind("<<DropEnter>>", lambda event, active_role=role: self.on_drop_enter(active_role, event))
        widget.dnd_bind("<<DropLeave>>", lambda event, active_role=role: self.on_drop_leave(active_role, event))
        widget.dnd_bind("<<Drop>>", lambda event, active_role=role: self.on_drop(active_role, event))

    def on_drop_enter(self, role: str, _: object | None = None) -> str:
        self.drop_areas[role].configure(bg="#D9ECFF")
        self.drop_hint_vars[role].set(f"释放鼠标即可导入{self.ROLE_TITLES[role]}")
        self.status_var.set(f"检测到拖拽到{self.ROLE_TITLES[role]}")
        return "copy"

    def on_drop_leave(self, role: str, _: object | None = None) -> None:
        self.drop_areas[role].configure(bg="#F3F7FB")
        if self.drag_enabled:
            self.drop_hint_vars[role].set(f"把文件拖到这里导入{self.ROLE_TITLES[role]}")

    def on_drop(self, role: str, event: object) -> str:
        if not self.action_allowed():
            return "break"
        self.on_drop_leave(role)
        raw_paths = self.parse_drop_paths(getattr(event, "data", ""))
        self.import_paths(raw_paths, role, source_name=f"拖拽导入{self.ROLE_TITLES[role]}")
        return "copy"

    def bind_treeview_tooltip(self, tree: ttk.Treeview) -> None:
        tree.bind("<Motion>", lambda event, active_tree=tree: self.on_treeview_hover(active_tree, event), add="+")
        tree.bind("<Leave>", self._hide_widget_tooltip, add="+")

    def on_treeview_hover(self, tree: ttk.Treeview, event: object) -> None:
        row_id = tree.identify_row(int(getattr(event, "y", 0)))
        col_id = tree.identify_column(int(getattr(event, "x", 0)))
        if not row_id or not col_id:
            self._hide_widget_tooltip()
            return

        full_text = self.get_full_tree_cell_text(tree, row_id, col_id)
        if not full_text:
            self._hide_widget_tooltip()
            return

        fake_event = type("TooltipEvent", (), {})()
        fake_event.widget = tree
        fake_event.x_root = int(getattr(event, "x_root", 0))
        fake_event.y_root = int(getattr(event, "y_root", 0))
        self.widget_tooltips[id(tree)] = full_text
        self._show_widget_tooltip(fake_event)

    def on_control_notebook_hover(self, event: object) -> None:
        notebook = getattr(self, "control_notebook", None)
        if notebook is None:
            return
        try:
            tab_id = notebook.tk.call(notebook._w, "identify", "tab", int(getattr(event, "x", 0)), int(getattr(event, "y", 0)))
        except tk.TclError:
            self._hide_widget_tooltip()
            return
        if tab_id == "":
            self._hide_widget_tooltip()
            return
        descriptions = {
            0: "设置主键字段和新旧数据的合并更新策略。",
            1: "按条件过滤处理结果，只保留符合条件的数据。",
            2: "批量替换、补空或改写指定字段的值。",
            3: "设置导出格式、工作表名称和表头冻结等选项。",
        }
        text = descriptions.get(int(tab_id), "")
        if not text:
            self._hide_widget_tooltip()
            return
        fake_event = type("TooltipEvent", (), {})()
        fake_event.widget = notebook
        fake_event.x_root = int(getattr(event, "x_root", 0))
        fake_event.y_root = int(getattr(event, "y_root", 0))
        self.widget_tooltips[id(notebook)] = text
        self._show_widget_tooltip(fake_event)

    def bind_listbox_tooltip(self, listbox: tk.Listbox, resolver=None) -> None:
        listbox.bind("<Motion>", lambda event, active_listbox=listbox, active_resolver=resolver: self.on_listbox_hover(active_listbox, event, active_resolver), add="+")
        listbox.bind("<Leave>", self._hide_widget_tooltip, add="+")

    def on_listbox_hover(self, listbox: tk.Listbox, event: object, resolver=None) -> None:
        index = listbox.nearest(int(getattr(event, "y", 0)))
        if index < 0 or index >= listbox.size():
            self._hide_widget_tooltip()
            return
        try:
            text = resolver(index) if resolver is not None else str(listbox.get(index))
        except Exception:
            text = str(listbox.get(index))
        if not text:
            self._hide_widget_tooltip()
            return
        fake_event = type("TooltipEvent", (), {})()
        fake_event.widget = listbox
        fake_event.x_root = int(getattr(event, "x_root", 0))
        fake_event.y_root = int(getattr(event, "y_root", 0))
        self.widget_tooltips[id(listbox)] = text
        self._show_widget_tooltip(fake_event)

    def on_key_listbox_single_click(self, event: object) -> str:
        self.key_listbox.focus_set()
        return "break"

    def on_key_listbox_double_click(self, event: object) -> str:
        index = self.key_listbox.nearest(int(getattr(event, "y", 0)))
        if index < 0 or index >= self.key_listbox.size():
            return "break"
        if self.key_listbox.selection_includes(index):
            self.key_listbox.selection_clear(index)
        else:
            self.key_listbox.selection_set(index)
        if self.processed_df is not None:
            self.invalidate_processed_results("主键字段已变化，请重新执行“应用处理”。")
        return "break"

    def get_source_file_parent_id(self, role: str, path: Path) -> str:
        return f"file::{role}::{str(path.resolve()).lower()}"

    def get_sources_grouped_by_file(self, role: str) -> list[tuple[str, list[SourceSelection]]]:
        groups: dict[str, list[SourceSelection]] = {}
        for source in self.sources.values():
            if source.dataset_role != role:
                continue
            marker = self.get_source_file_parent_id(role, source.path)
            groups.setdefault(marker, []).append(source)
        return list(groups.items())

    def expand_source_parent(self, source: SourceSelection) -> None:
        tree = self.source_trees[source.dataset_role]
        parent_id = self.get_source_file_parent_id(source.dataset_role, source.path)
        if tree.exists(parent_id):
            tree.item(parent_id, open=True)

    def resolve_selection_to_source_ids(self, tree: ttk.Treeview, selected_ids: tuple[str, ...]) -> list[str]:
        resolved: list[str] = []
        seen: set[str] = set()
        for item_id in selected_ids:
            if item_id in self.sources:
                if item_id not in seen:
                    resolved.append(item_id)
                    seen.add(item_id)
                continue
            if tree.exists(item_id):
                for child_id in tree.get_children(item_id):
                    if child_id in self.sources and child_id not in seen:
                        resolved.append(child_id)
                        seen.add(child_id)
        return resolved

    def get_full_tree_cell_text(self, tree: ttk.Treeview, row_id: str, column_id: str) -> str:
        if column_id == "#0":
            return str(tree.item(row_id, "text"))
        column_index = int(column_id.replace("#", "")) - 1
        preview_df = self.tree_preview_frames.get(id(tree))
        if preview_df is not None:
            preview_columns = tree["columns"]
            has_row_number_column = bool(preview_columns) and preview_columns[0] == PREVIEW_ROW_NUMBER_COLUMN
            item_index = tree.index(row_id)
            if has_row_number_column and column_index == 0:
                return str(item_index + 1)
            if has_row_number_column:
                column_index -= 1
            if 0 <= item_index < len(preview_df):
                if column_index < 0 or column_index >= len(preview_df.columns):
                    return ""
                value = preview_df.iloc[item_index, column_index]
                return "" if pd.isna(value) else str(value)
        if row_id in self.sources:
            source = self.sources[row_id]
            if column_index == 0:
                return source.sheet_name
            if column_index == 1:
                return str(source.row_count)
            if column_index == 2:
                return "启用" if source.enabled else "停用"
        values = tree.item(row_id, "values")
        if 0 <= column_index < len(values):
            return str(values[column_index])
        return ""

    def parse_drop_paths(self, data: str) -> list[str]:
        if not data:
            return []
        try:
            values = list(self.tk.splitlist(data))
        except tk.TclError:
            values = [data]

        paths: list[str] = []
        for value in values:
            text = value.strip().strip('"')
            if text.startswith("{") and text.endswith("}"):
                text = text[1:-1]
            if text:
                paths.append(text)
        return paths

    def _build_preview_panel(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)

        header = ttk.Frame(parent)
        header.grid(row=0, column=0, sticky="w")
        ttk.Label(header, text="预览", style="Title.TLabel").grid(row=0, column=0, sticky="w")
        self.compare_filter_button = ttk.Button(
            header,
            textvariable=self.compare_filter_button_text,
            style="Compact.TButton",
            command=self.toggle_compare_changes_only,
        )
        self.compare_filter_button.grid(row=0, column=1, padx=(8, 0))
        self.register_tooltip(
            self.compare_filter_button,
            lambda: (
                "当前仅显示新增、变更、移除行；点击后恢复全部预览。"
                if self.compare_changes_only_var.get()
                else "点击后只显示新增、变更、移除行。"
            ),
        )

        notebook = ttk.Notebook(parent)
        notebook.grid(row=1, column=0, sticky="nsew", pady=(8, 0))

        raw_tab = ttk.Frame(notebook, padding=8)
        compare_tab = ttk.Frame(notebook, padding=8)
        notebook.add(raw_tab, text="原始预览")
        notebook.add(compare_tab, text="修改对比")

        self.raw_tree, self.raw_info_label = self._create_preview_table(raw_tab)
        self._build_compare_preview(compare_tab)

    def _create_preview_table(self, parent: ttk.Frame) -> tuple[ttk.Treeview, ttk.Label]:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)

        info_label = ttk.Label(parent, text="暂无预览", style="Summary.TLabel")
        info_label.grid(row=0, column=0, sticky="w", pady=(0, 6))

        container = ttk.Frame(parent)
        container.grid(row=1, column=0, sticky="nsew")
        container.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=1)

        tree = ttk.Treeview(container, show="headings")
        tree.grid(row=0, column=0, sticky="nsew")

        y_scroll = ttk.Scrollbar(container, orient=tk.VERTICAL, command=tree.yview)
        y_scroll.grid(row=0, column=1, sticky="ns")

        x_scroll = ttk.Scrollbar(container, orient=tk.HORIZONTAL, command=tree.xview)
        x_scroll.grid(row=1, column=0, sticky="ew")

        tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        self.bind_treeview_tooltip(tree)
        return tree, info_label

    def _build_compare_preview(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)

        self.compare_info_label = ttk.Label(
            parent,
            text="仅标记具体改动单元格：左侧窄标记列显示绿色 + / 红色 -，变更值本身也会带浅绿色 / 浅红色背景。",
            style="Summary.TLabel",
        )
        self.compare_info_label.grid(row=0, column=0, sticky="w", pady=(0, 6))

        compare_paned = ttk.Panedwindow(parent, orient=tk.HORIZONTAL)
        compare_paned.grid(row=1, column=0, sticky="nsew")

        before_frame = ttk.LabelFrame(compare_paned, text="修改前", padding=8)
        after_frame = ttk.LabelFrame(compare_paned, text="修改后", padding=8)
        compare_paned.add(before_frame, weight=1)
        compare_paned.add(after_frame, weight=1)

        (
            self.compare_before_text,
            self.compare_before_info_label,
            self.compare_before_y_scroll,
            self.compare_before_x_scroll,
        ) = self._create_compare_text_panel(before_frame, side="before")
        (
            self.compare_after_text,
            self.compare_after_info_label,
            self.compare_after_y_scroll,
            self.compare_after_x_scroll,
        ) = self._create_compare_text_panel(after_frame, side="after")
        self.compare_before_text.configure(
            yscrollcommand=lambda first, last: self.sync_compare_yview("before", first, last)
        )
        self.compare_after_text.configure(
            yscrollcommand=lambda first, last: self.sync_compare_yview("after", first, last)
        )
        self.compare_before_text.configure(
            xscrollcommand=lambda first, last: self.sync_compare_xview("before", first, last)
        )
        self.compare_after_text.configure(
            xscrollcommand=lambda first, last: self.sync_compare_xview("after", first, last)
        )

    def toggle_compare_changes_only(self) -> None:
        self.compare_changes_only_var.set(not self.compare_changes_only_var.get())
        self.refresh_compare_filter_button()
        self.refresh_comparison_preview()

    def refresh_compare_filter_button(self) -> None:
        if self.compare_changes_only_var.get():
            self.compare_filter_button_text.set("显示全部")
        else:
            self.compare_filter_button_text.set("仅预览变动")

    def _create_compare_text_panel(
        self,
        parent: ttk.Frame,
        side: str,
    ) -> tuple[tk.Text, ttk.Label, ttk.Scrollbar, ttk.Scrollbar]:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)

        info_label = ttk.Label(parent, text="暂无对比", style="Summary.TLabel")
        info_label.grid(row=0, column=0, sticky="w", pady=(0, 6))

        container = ttk.Frame(parent)
        container.grid(row=1, column=0, sticky="nsew")
        container.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=1)

        text = tk.Text(container, wrap="none", font=self.font_mono, bg="#ffffff", relief="flat")
        text.grid(row=0, column=0, sticky="nsew")
        y_scroll = ttk.Scrollbar(container, orient=tk.VERTICAL, command=text.yview)
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll = ttk.Scrollbar(container, orient=tk.HORIZONTAL, command=lambda *args, active_side=side: self.on_compare_xscroll(active_side, *args))
        x_scroll.grid(row=1, column=0, sticky="ew")
        text.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)

        text.tag_configure("header", font=(self.font_mono.actual("family"), self.font_mono.actual("size"), "bold"))
        text.tag_configure("plus", foreground="#1f9d55", font=(self.font_mono.actual("family"), self.font_mono.actual("size"), "bold"))
        text.tag_configure("minus", foreground="#d62828", font=(self.font_mono.actual("family"), self.font_mono.actual("size"), "bold"))
        text.tag_configure("plus_value", background="#d7f5d0", foreground="#14532d")
        text.tag_configure("minus_value", background="#ffd8d8", foreground="#7f1d1d")
        text.tag_configure("separator", foreground="#777777")
        text.bind("<Enter>", lambda event, active_side=side: self.on_compare_hover(active_side, event))
        text.bind("<Motion>", lambda event, active_side=side: self.on_compare_hover(active_side, event))
        text.bind("<Leave>", self.hide_compare_tooltip)
        text.configure(state="disabled")
        return text, info_label, y_scroll, x_scroll

    def _build_control_panel(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)

        ttk.Label(parent, text="处理设置", style="Title.TLabel").grid(row=0, column=0, sticky="w")

        notebook = ttk.Notebook(parent)
        notebook.grid(row=1, column=0, sticky="nsew", pady=(8, 8))
        self.control_notebook = notebook

        dedupe_tab = ttk.Frame(notebook, padding=10)
        filter_tab = ttk.Frame(notebook, padding=10)
        update_tab = ttk.Frame(notebook, padding=10)
        export_tab = ttk.Frame(notebook, padding=10)

        notebook.add(dedupe_tab, text="主键合并")
        notebook.add(filter_tab, text="筛选规则")
        notebook.add(update_tab, text="更新规则")
        notebook.add(export_tab, text="导出设置")
        notebook.bind("<Motion>", self.on_control_notebook_hover, add="+")
        notebook.bind("<Leave>", self._hide_widget_tooltip, add="+")

        self._build_dedupe_tab(dedupe_tab)
        self._build_filter_tab(filter_tab)
        self._build_update_tab(update_tab)
        self._build_export_tab(export_tab)

        summary_frame = ttk.LabelFrame(parent, text="处理摘要", padding=8)
        summary_frame.grid(row=2, column=0, sticky="nsew")
        summary_frame.columnconfigure(0, weight=1)
        summary_frame.rowconfigure(0, weight=1)

        self.summary_text = tk.Text(summary_frame, height=7, wrap="word")
        self.summary_text.grid(row=0, column=0, sticky="nsew")
        self.summary_text.configure(state="disabled")

    def _build_dedupe_tab(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(3, weight=1)

        ttk.Label(parent, text="选择匹配主键字段（可多选）").grid(row=0, column=0, sticky="w")
        strategy_box = ttk.Combobox(
            parent,
            textvariable=self.duplicate_strategy_var,
            values=list(DUPLICATE_STRATEGIES.values()),
            state="readonly",
        )
        strategy_box.grid(row=1, column=0, sticky="ew", pady=(6, 10))
        self.register_busy_ttk_widget(strategy_box)
        self.register_tooltip(strategy_box, lambda: f"当前策略：{self.duplicate_strategy_var.get()}")

        self.strategy_full_label = ttk.Label(
            parent,
            textvariable=self.strategy_full_text_var,
            wraplength=340,
            style="Summary.TLabel",
        )
        self.strategy_full_label.grid(row=2, column=0, sticky="w", pady=(0, 8))

        list_container = ttk.Frame(parent)
        list_container.grid(row=3, column=0, sticky="nsew")
        list_container.columnconfigure(0, weight=1)
        list_container.rowconfigure(0, weight=1)

        self.key_listbox = tk.Listbox(list_container, selectmode=tk.MULTIPLE, exportselection=False)
        self.key_listbox.grid(row=0, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(list_container, orient=tk.VERTICAL, command=self.key_listbox.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.key_listbox.configure(yscrollcommand=scrollbar.set)
        self.register_busy_tk_widget(self.key_listbox)
        self.register_tooltip(self.key_listbox, "选择用于匹配老数据和新数据的主键字段。可多选。")
        self.bind_listbox_tooltip(self.key_listbox, lambda index: self.column_display_label(self.available_columns[index]) if index < len(self.available_columns) else "")
        self.key_listbox.bind("<Button-1>", self.on_key_listbox_single_click, add="+")
        self.key_listbox.bind("<Double-Button-1>", self.on_key_listbox_double_click, add="+")

        ttk.Label(
            parent,
            text="推荐默认策略是“更新并新增”：主键已存在时按字段比较，只用新表里的非空值更新有变化的字段；主键不存在时自动新增。若只想更新已存在记录、不新增，则改成“仅更新不新增”；若只想补空值，则改成“仅用新数据补全老数据空值”。",
            wraplength=360,
            style="Summary.TLabel",
        ).grid(row=4, column=0, sticky="w", pady=(10, 0))

    def _build_filter_tab(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(0, weight=1)

        columns = ("column", "operator", "value")
        self.filter_tree = ttk.Treeview(parent, columns=columns, show="headings", height=10)
        self.filter_tree.heading("column", text="字段")
        self.filter_tree.heading("operator", text="条件")
        self.filter_tree.heading("value", text="值")
        self.filter_tree.column("column", width=140, anchor="w")
        self.filter_tree.column("operator", width=100, anchor="center")
        self.filter_tree.column("value", width=120, anchor="w")
        self.filter_tree.grid(row=0, column=0, sticky="nsew")
        self.filter_tree.bind("<Double-1>", lambda _: self.edit_selected_filter_rule())
        self.filter_tree.bind("<Return>", lambda _: self.edit_selected_filter_rule())
        self.filter_tree.bind("<Delete>", lambda _: self.remove_selected_filter_rule())

        button_bar = ttk.Frame(parent)
        button_bar.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        add_filter_btn = ttk.Button(button_bar, text="新增规则", style="Compact.TButton", width=7, command=self.add_filter_rule)
        add_filter_btn.pack(side=tk.LEFT, padx=(0, 6))
        edit_filter_btn = ttk.Button(
            button_bar,
            text="编辑选中",
            style="Compact.TButton",
            width=7,
            command=self.edit_selected_filter_rule,
        )
        edit_filter_btn.pack(side=tk.LEFT, padx=(0, 6))
        remove_filter_btn = ttk.Button(
            button_bar,
            text="删除选中",
            style="Compact.TButton",
            width=7,
            command=self.remove_selected_filter_rule,
        )
        remove_filter_btn.pack(side=tk.LEFT, padx=(0, 6))
        clear_filter_btn = ttk.Button(button_bar, text="清空规则", style="Compact.TButton", width=7, command=self.clear_filter_rules)
        clear_filter_btn.pack(side=tk.LEFT)
        for widget in (add_filter_btn, edit_filter_btn, remove_filter_btn, clear_filter_btn):
            self.register_busy_ttk_widget(widget)

    def _build_update_tab(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(0, weight=1)

        columns = ("column", "mode", "find", "replace")
        self.update_tree = ttk.Treeview(parent, columns=columns, show="headings", height=10)
        self.update_tree.heading("column", text="字段")
        self.update_tree.heading("mode", text="操作")
        self.update_tree.heading("find", text="查找值")
        self.update_tree.heading("replace", text="新值")
        self.update_tree.column("column", width=120, anchor="w")
        self.update_tree.column("mode", width=120, anchor="center")
        self.update_tree.column("find", width=110, anchor="w")
        self.update_tree.column("replace", width=120, anchor="w")
        self.update_tree.grid(row=0, column=0, sticky="nsew")
        self.update_tree.bind("<Double-1>", lambda _: self.edit_selected_update_rule())
        self.update_tree.bind("<Return>", lambda _: self.edit_selected_update_rule())
        self.update_tree.bind("<Delete>", lambda _: self.remove_selected_update_rule())

        button_bar = ttk.Frame(parent)
        button_bar.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        add_update_btn = ttk.Button(button_bar, text="新增规则", style="Compact.TButton", width=7, command=self.add_update_rule)
        add_update_btn.pack(side=tk.LEFT, padx=(0, 6))
        edit_update_btn = ttk.Button(
            button_bar,
            text="编辑选中",
            style="Compact.TButton",
            width=7,
            command=self.edit_selected_update_rule,
        )
        edit_update_btn.pack(side=tk.LEFT, padx=(0, 6))
        remove_update_btn = ttk.Button(
            button_bar,
            text="删除选中",
            style="Compact.TButton",
            width=7,
            command=self.remove_selected_update_rule,
        )
        remove_update_btn.pack(side=tk.LEFT, padx=(0, 6))
        clear_update_btn = ttk.Button(button_bar, text="清空规则", style="Compact.TButton", width=7, command=self.clear_update_rules)
        clear_update_btn.pack(side=tk.LEFT)
        for widget in (add_update_btn, edit_update_btn, remove_update_btn, clear_update_btn):
            self.register_busy_ttk_widget(widget)

    def _build_export_tab(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(1, weight=1)

        ttk.Label(parent, text="输出格式").grid(row=0, column=0, sticky="w")
        output_format_box = ttk.Combobox(
            parent,
            textvariable=self.output_format_var,
            values=["Excel (.xlsx)", "CSV (.csv)"],
            state="readonly",
        )
        output_format_box.grid(row=0, column=1, sticky="ew", pady=(0, 8))
        self.register_busy_ttk_widget(output_format_box)

        ttk.Label(parent, text="工作表名称").grid(row=1, column=0, sticky="w")
        output_sheet_entry = ttk.Entry(parent, textvariable=self.output_sheet_var)
        output_sheet_entry.grid(row=1, column=1, sticky="ew", pady=(0, 8))
        self.register_busy_ttk_widget(output_sheet_entry)

        include_source_check = ttk.Checkbutton(parent, text="输出来源字段", variable=self.include_source_var)
        include_source_check.grid(
            row=2, column=0, columnspan=2, sticky="w"
        )
        self.register_busy_ttk_widget(include_source_check)
        freeze_header_check = ttk.Checkbutton(parent, text="冻结首行", variable=self.freeze_header_var)
        freeze_header_check.grid(
            row=3, column=0, columnspan=2, sticky="w"
        )
        self.register_busy_ttk_widget(freeze_header_check)
        auto_width_check = ttk.Checkbutton(parent, text="自动列宽", variable=self.auto_width_var)
        auto_width_check.grid(
            row=4, column=0, columnspan=2, sticky="w"
        )
        self.register_busy_ttk_widget(auto_width_check)
        style_header_check = ttk.Checkbutton(parent, text="导出表头样式", variable=self.style_header_var)
        style_header_check.grid(
            row=5, column=0, columnspan=2, sticky="w"
        )
        self.register_busy_ttk_widget(style_header_check)

        ttk.Label(
            parent,
            text="字段显示/隐藏和导出名称修改，请使用顶部“字段设置”。",
            wraplength=320,
            style="Summary.TLabel",
        ).grid(row=6, column=0, columnspan=2, sticky="w", pady=(10, 0))

    def import_files_for_role(self, role: str) -> None:
        if not self.action_allowed():
            return
        file_types = [
            ("表格文件", "*.xlsx *.xlsm *.csv *.tsv"),
            ("Excel 工作簿", "*.xlsx *.xlsm"),
            ("CSV 文件", "*.csv"),
            ("TSV 文件", "*.tsv"),
            ("所有文件", "*.*"),
        ]
        paths = filedialog.askopenfilenames(
            title=f"选择{self.ROLE_TITLES[role]}",
            filetypes=file_types,
            initialdir=self.dialog_initialdir(),
        )
        if paths:
            self.remember_browse_path(paths[0])
            self.import_paths(paths, role, source_name=f"手动导入{self.ROLE_TITLES[role]}")

    def import_paths(self, raw_paths: list[str] | tuple[str, ...], dataset_role: str, source_name: str) -> None:
        if not self.action_allowed():
            return
        paths = self.expand_input_paths(raw_paths)
        if not paths:
            if raw_paths:
                messagebox.showwarning("没有可导入文件", "未找到支持的表格文件。")
            return
        self.remember_browse_path(paths[0])

        def on_success(payload: object) -> None:
            new_sources, new_cache = payload
            import_result = apply_imported_sources(
                self.sources,
                self.data_cache,
                new_sources,
                new_cache,
                dataset_role,
                source_name,
                len(paths),
            )
            self.sources = import_result.sources
            self.data_cache = import_result.cache
            self.invalidate_processed_results()
            self.refresh_source_trees()
            self.refresh_available_columns()
            self.write_summary(import_result.summary_lines)
            self.status_var.set(import_result.status_text)

            if import_result.first_source is not None:
                first_source = import_result.first_source
                self.clear_tree_selection_except(dataset_role)
                self.expand_source_parent(first_source)
                self.source_trees[dataset_role].selection_set(first_source.source_id)
                self.preview_source(first_source.source_id)

            if dataset_role == "new" and collect_target_columns(self.sources):
                mapping_completed = self.open_mapping_dialog_for_sources(import_result.imported_sources, auto_open=True)
                if not mapping_completed:
                    self.write_summary(
                        import_result.summary_lines + ["部分新数据尚未完成字段匹配，可稍后点击“字段匹配”继续。"]
                    )
                    self.status_var.set("导入完成：部分新数据尚未完成字段匹配")

        def on_error(exc: Exception) -> None:
            messagebox.showerror("导入失败", str(exc))
            self.status_var.set("导入失败")

        self.start_background_task(
            task_name="import_sources",
            task_func=lambda: load_sources_from_paths(paths),
            success_handler=on_success,
            error_handler=on_error,
            busy_message=f"{source_name}中，请稍候...",
        )

    def expand_input_paths(self, raw_paths: list[str] | tuple[str, ...]) -> list[Path]:
        return collect_input_paths(raw_paths)

    def remove_selected_sources(self) -> None:
        if not self.action_allowed():
            return
        selected_ids = self.get_selected_source_ids()
        if not selected_ids:
            return

        for source_id in selected_ids:
            self.sources.pop(source_id, None)
            self.data_cache.pop(source_id, None)
            for role, active_id in self.active_sheet_source_ids.items():
                if active_id == source_id:
                    self.active_sheet_source_ids[role] = None

        self.refresh_source_trees()
        self.refresh_available_columns()
        self.update_operation_pair_label()
        self.populate_dataframe_preview(self.raw_tree, None, self.raw_info_label, "暂无预览")
        self.clear_processed_results()
        self.status_var.set("已移除选中数据源")

    def remove_selected_files(self, role: str) -> None:
        if not self.action_allowed():
            return
        tree = self.source_trees[role]
        selected = tree.selection()
        if not selected:
            return
        confirmed = messagebox.askyesno(
            "确认移除",
            f"确定要移除选中的{self.ROLE_TITLES[role]}文件吗？",
            parent=self,
        )
        if not confirmed:
            return
        target_ids = self.resolve_selection_to_source_ids(tree, selected)
        if not target_ids:
            return
        for source_id in target_ids:
            self.sources.pop(source_id, None)
            self.data_cache.pop(source_id, None)
            if self.active_sheet_source_ids.get(role) == source_id:
                self.active_sheet_source_ids[role] = None
        self.refresh_source_trees()
        self.refresh_available_columns()
        self.update_operation_pair_label()
        self.populate_dataframe_preview(self.raw_tree, None, self.raw_info_label, "暂无预览")
        self.clear_processed_results()
        self.status_var.set(f"已移除选中的{self.ROLE_TITLES[role]}文件")

    def set_selected_sources_enabled(self, enabled: bool) -> None:
        if not self.action_allowed():
            return
        selected_ids = self.get_selected_source_ids()
        if not selected_ids:
            return

        for source_id in selected_ids:
            if source_id in self.sources:
                self.sources[source_id].enabled = enabled

        self.refresh_source_trees()
        self.refresh_available_columns()
        self.invalidate_processed_results()
        self.status_var.set("已更新数据源状态")

    def on_source_double_click(self, role: str, event: object) -> str:
        if not self.action_allowed():
            return "break"
        tree = self.source_trees[role]
        row_id = tree.identify_row(getattr(event, "y", 0))
        if not row_id:
            return "break"
        if row_id not in self.sources:
            if tree.exists(row_id):
                tree.item(row_id, open=not self.is_tree_item_open(tree, row_id))
            return "break"

        current_active = self.active_sheet_source_ids.get(role)
        if current_active == row_id:
            self.active_sheet_source_ids[role] = None
            self.status_var.set(f"已取消{self.ROLE_TITLES[role]}操作 sheet 选择")
        else:
            self.active_sheet_source_ids[role] = row_id
            self.sources[row_id].enabled = True
            self.status_var.set(f"已选择{self.ROLE_TITLES[role]}操作 sheet：{self.sources[row_id].sheet_name}")

        self.refresh_source_trees()
        self.refresh_available_columns()
        self.update_operation_pair_label()
        self.clear_processed_results()
        parent_id = self.get_source_file_parent_id(role, self.sources[row_id].path)
        if tree.exists(parent_id):
            tree.item(parent_id, open=True)
        self.preview_source(row_id)
        return "break"

    def clear_processed_results(self) -> None:
        self.processed_df = None
        self.processed_writeback_df = None
        self.last_processed_scope_source_ids.clear()
        self.compare_before_df = None
        self.compare_after_df = None
        self.compare_statuses = []
        self.compare_changed_columns = []
        self.compare_all_before_df = None
        self.compare_all_after_df = None
        self.compare_all_statuses = []
        self.compare_all_changed_columns = []
        self.compare_before_total_rows = 0
        self.compare_after_total_rows = 0
        self.compare_column_widths = {}
        self.hide_compare_tooltip()
        self.clear_compare_text(self.compare_before_text, self.compare_before_info_label, "暂无修改前预览")
        self.clear_compare_text(self.compare_after_text, self.compare_after_info_label, "暂无修改后预览")

    def clear_compare_text(self, text_widget: tk.Text, info_label: ttk.Label, info_text: str) -> None:
        text_widget.configure(state="normal")
        text_widget.delete("1.0", tk.END)
        text_widget.configure(state="disabled")
        info_label.configure(text=info_text)

    def sync_compare_yview(self, side: str, first: str, last: str) -> None:
        if side == "before":
            self.compare_before_y_scroll.set(first, last)
            target_text = self.compare_after_text
            target_scroll = self.compare_after_y_scroll
        else:
            self.compare_after_y_scroll.set(first, last)
            target_text = self.compare_before_text
            target_scroll = self.compare_before_y_scroll

        if self.compare_scroll_lock:
            return

        try:
            self.compare_scroll_lock = True
            target_text.yview_moveto(first)
            target_scroll.set(first, last)
        finally:
            self.compare_scroll_lock = False

    def sync_compare_xview(self, side: str, first: str, last: str) -> None:
        if side == "before":
            self.compare_before_x_scroll.set(first, last)
            target_text = self.compare_after_text
            target_scroll = self.compare_after_x_scroll
        else:
            self.compare_after_x_scroll.set(first, last)
            target_text = self.compare_before_text
            target_scroll = self.compare_before_x_scroll

        if self.compare_x_scroll_lock:
            return

        try:
            self.compare_x_scroll_lock = True
            target_text.xview_moveto(first)
            target_scroll.set(first, last)
        finally:
            self.compare_x_scroll_lock = False

    def on_compare_xscroll(self, side: str, *args: str) -> None:
        if self.compare_x_scroll_lock:
            return

        if side == "before":
            source_text = self.compare_before_text
            target_text = self.compare_after_text
        else:
            source_text = self.compare_after_text
            target_text = self.compare_before_text

        try:
            self.compare_x_scroll_lock = True
            source_text.xview(*args)
            target_text.xview(*args)
        finally:
            self.compare_x_scroll_lock = False

    def show_compare_tooltip(self, x_root: int, y_root: int, text: str) -> None:
        if not text:
            return
        if self.compare_tooltip is None:
            tooltip = tk.Toplevel(self)
            tooltip.withdraw()
            tooltip.overrideredirect(True)
            tooltip.attributes("-topmost", True)
            label = tk.Label(
                tooltip,
                text="",
                justify="left",
                anchor="w",
                bg="#fff8d9",
                relief="solid",
                bd=1,
                padx=8,
                pady=6,
                wraplength=520,
                font=("Consolas", 10),
            )
            label.pack()
            self.compare_tooltip = tooltip
            self.compare_tooltip_label = label
        self.compare_tooltip_label.configure(text=text)
        self.compare_tooltip.geometry(f"+{x_root + 14}+{y_root + 18}")
        self.compare_tooltip.deiconify()

    def hide_compare_tooltip(self, _: object | None = None) -> None:
        if self.compare_tooltip is not None:
            self.compare_tooltip.withdraw()

    def on_compare_hover(self, side: str, event: object) -> None:
        if self.compare_before_df is None or self.compare_after_df is None or not self.compare_column_widths:
            self.hide_compare_tooltip()
            return
        text_widget = self.compare_before_text if side == "before" else self.compare_after_text
        dataframe = self.compare_before_df if side == "before" else self.compare_after_df
        x = int(getattr(event, "x", 0))
        y = int(getattr(event, "y", 0))
        line, column_name = self.locate_compare_cell(
            text_widget,
            x,
            y,
            build_compare_display_columns(list(dataframe.columns)),
        )
        if line is None or column_name is None or column_name == PREVIEW_ROW_NUMBER_COLUMN:
            self.hide_compare_tooltip()
            return
        row_index = line - 3
        if row_index < 0 or row_index >= len(dataframe):
            self.hide_compare_tooltip()
            return
        full_text = self.preview_value(dataframe.iloc[row_index].get(column_name, ""))
        if len(full_text) <= self.compare_column_widths.get(column_name, 0):
            self.hide_compare_tooltip()
            return
        self.show_compare_tooltip(int(getattr(event, "x_root", 0)), int(getattr(event, "y_root", 0)), full_text)

    def locate_compare_cell(
        self,
        text_widget: tk.Text,
        x: int,
        y: int,
        columns: list[str],
    ) -> tuple[int | None, str | None]:
        try:
            index = text_widget.index(f"@{x},{y}")
        except tk.TclError:
            return None, None
        line_text = text_widget.get(f"{index} linestart", f"{index} lineend")
        if not line_text or set(line_text) <= {"-", "+", "|", " "}:
            return None, None
        line_no_str, col_no_str = index.split(".")
        line_no = int(line_no_str)
        col_no = int(col_no_str)
        cursor = 0
        for column in columns:
            cell_width = self.compare_column_widths.get(column, 10) + 2
            if cursor <= col_no < cursor + cell_width:
                return line_no, column
            cursor += cell_width
            if column != columns[-1]:
                cursor += 3
        return None, None

    def get_selected_source_ids(self) -> list[str]:
        selected_ids: list[str] = []
        seen: set[str] = set()
        for tree in self.source_trees.values():
            for source_id in self.resolve_selection_to_source_ids(tree, tree.selection()):
                if source_id not in seen:
                    selected_ids.append(source_id)
                    seen.add(source_id)
        return selected_ids

    def clear_tree_selection_except(self, active_role: str) -> None:
        for role, tree in self.source_trees.items():
            if role != active_role:
                tree.selection_remove(tree.selection())

    def is_tree_item_open(self, tree: ttk.Treeview, item_id: str) -> bool:
        raw_state = tree.item(item_id, "open")
        if isinstance(raw_state, str):
            return raw_state.lower() in {"1", "true", "yes"}
        return bool(raw_state)

    def get_open_parent_ids(self) -> dict[str, set[str]]:
        open_ids: dict[str, set[str]] = {}
        for role, tree in self.source_trees.items():
            role_open_ids: set[str] = set()
            for item_id in tree.get_children(""):
                if self.is_tree_item_open(tree, item_id):
                    role_open_ids.add(item_id)
            open_ids[role] = role_open_ids
        return open_ids

    def refresh_source_trees(self) -> None:
        open_parent_ids = self.get_open_parent_ids()
        for role, tree in self.source_trees.items():
            tree.delete(*tree.get_children())
            role_open_ids = open_parent_ids.get(role, set())
            grouped_sources = self.get_sources_grouped_by_file(role)
            role_sources = [source for _, items in grouped_sources for source in items]
            enabled_count = 0
            enabled_rows = 0
            confirmed_count = 0
            for parent_id, file_sources in grouped_sources:
                first_source = file_sources[0]
                tree.insert(
                    "",
                    "end",
                    iid=parent_id,
                    text=first_source.path.name,
                    values=("", "", ""),
                    tags=("file_parent",),
                )
                parent_open = parent_id in role_open_ids
                for source in file_sources:
                    status = "启用" if source.enabled else "停用"
                    if source.enabled:
                        enabled_count += 1
                        enabled_rows += source.row_count
                    if role == "new" and source.mapping_confirmed:
                        confirmed_count += 1
                    tags = ("active_pair",) if self.active_sheet_source_ids.get(role) == source.source_id else ()
                    tree.insert(
                        parent_id,
                        "end",
                        iid=source.source_id,
                        text="",
                        values=(source.sheet_name, source.row_count, status),
                        tags=tags,
                    )
                    if self.active_sheet_source_ids.get(role) == source.source_id:
                        parent_open = True
                tree.item(parent_id, open=parent_open)
            file_count = len(grouped_sources)
            summary_text = f"{self.ROLE_TITLES[role]}: {file_count} 个文件 | {len(role_sources)} 个sheet | 启用 {enabled_count} 个 | 行数 {enabled_rows}"
            if role == "new":
                summary_text += f" | 已确认映射 {confirmed_count} 个"
            self.source_summary_labels[role].configure(text=summary_text)

    def update_operation_pair_label(self) -> None:
        old_id = self.active_sheet_source_ids.get("old")
        new_id = self.active_sheet_source_ids.get("new")
        if old_id and new_id and old_id in self.sources and new_id in self.sources:
            old_source = self.sources[old_id]
            new_source = self.sources[new_id]
            self.operation_pair_var.set(
                f"当前操作配对：老数据 {old_source.path.name}/{old_source.sheet_name} <- 新数据 {new_source.path.name}/{new_source.sheet_name}"
            )
            return
        if old_id and old_id in self.sources:
            old_source = self.sources[old_id]
            self.operation_pair_var.set(
                f"已选老数据 sheet：{old_source.path.name}/{old_source.sheet_name}。请再双击选择一个新数据 sheet。"
            )
            return
        if new_id and new_id in self.sources:
            new_source = self.sources[new_id]
            self.operation_pair_var.set(
                f"已选新数据 sheet：{new_source.path.name}/{new_source.sheet_name}。请再双击选择一个老数据 sheet。"
            )
            return
        self.operation_pair_var.set("当前操作：全部启用数据源。双击老/新数据中的 sheet 可指定配对。")

    def refresh_available_columns(self) -> None:
        previous_selection = set(self.selected_duplicate_keys())
        self.available_columns = collect_available_columns(self.get_column_scope_sources())

        self.key_listbox.delete(0, tk.END)
        for index, column in enumerate(self.available_columns):
            self.key_listbox.insert(tk.END, self.column_display_label(column))
            if column in previous_selection:
                self.key_listbox.selection_set(index)
            self.column_settings.setdefault(
                column,
                ColumnSetting(
                    visible=default_visible(column, self.include_source_var.get()),
                    rename_to=default_display_name(column),
                ),
            )

    def selected_duplicate_keys(self) -> list[str]:
        return [self.available_columns[index] for index in self.key_listbox.curselection()]

    def get_column_scope_sources(self) -> dict[str, SourceSelection]:
        return collect_column_scope_sources(self.sources, self.active_sheet_source_ids)

    def get_processing_scope_sources(self) -> dict[str, SourceSelection] | None:
        decision = resolve_processing_scope_sources(self.sources, self.active_sheet_source_ids)
        if decision.reason == PAIR_INCOMPLETE:
            messagebox.showinfo("配对未完成", "请分别双击选择一个老数据 sheet 和一个新数据 sheet，或取消配对后处理全部启用数据源。")
            return None
        return decision.sources

    def get_mapping_scope_sources(
        self,
        sources_to_map: list[SourceSelection],
    ) -> dict[str, SourceSelection]:
        return collect_mapping_scope_sources(self.sources, self.active_sheet_source_ids, sources_to_map)

    def on_source_selected(self, role: str, _: object | None = None) -> None:
        tree = self.source_trees[role]
        selected = tree.selection()
        if not selected:
            return
        selected_id = selected[0]
        if selected_id in self.sources:
            self.clear_tree_selection_except(role)
            self.preview_source(selected_id)

    def preview_source(self, source_id: str) -> None:
        if source_id not in self.sources or source_id not in self.data_cache:
            return

        source = self.sources[source_id]
        dataframe = self.data_cache[source_id]
        info = (
            f"{self.ROLE_TITLES[source.dataset_role]} / {source.path.name} / {source.sheet_name}"
            f" | 行数: {len(dataframe)} | 列数: {len(dataframe.columns)} | 已加载全部"
        )
        self.populate_dataframe_preview(self.raw_tree, dataframe, self.raw_info_label, info)

    def apply_processing(self) -> None:
        if not self.action_allowed():
            return
        scoped_sources = self.get_processing_scope_sources()
        if scoped_sources is None:
            self.post_process_action = None
            return
        def on_prepare_success(payload: object) -> None:
            preparation = payload
            raw_dataframe = preparation.raw_dataframe
            if preparation.reason == PROCESSING_NO_DATA:
                self.post_process_action = None
                messagebox.showwarning("没有可处理的数据", "请先导入并启用老数据或新数据。")
                self.status_var.set("处理失败：没有启用的数据源")
                return
            if preparation.reason == PROCESSING_MISSING_OLD:
                self.post_process_action = None
                messagebox.showwarning("缺少老数据", "请至少导入一份老数据。")
                return

            unmapped_new_sources = preparation.unmapped_new_sources
            if unmapped_new_sources:
                confirmed = self.open_mapping_dialog_for_sources(unmapped_new_sources, auto_open=False)
                if not confirmed:
                    self.post_process_action = None
                    self.status_var.set("处理取消：未完成字段匹配确认")
                    return

            config = self.build_pipeline_config()
            reuse_prepared_raw_dataframe = preparation.raw_dataframe_ready and not unmapped_new_sources

            def on_process_success(result_payload: object) -> None:
                raw_dataframe_result, result = result_payload
                self.processed_df = result.dataframe
                self.processed_writeback_df = result.writeback_dataframe
                self.last_processed_scope_source_ids = set(scoped_sources.keys())
                self.prepare_comparison_preview(raw_dataframe_result, config, self.processed_df)
                self.write_summary(result.summary_lines + summarize_comparison_statuses(self.compare_all_statuses))
                self.status_var.set(f"处理完成：当前结果 {len(self.processed_df)} 行")
                if self.post_process_action is not None:
                    callback = self.post_process_action
                    self.post_process_action = None
                    self.after(0, callback)

            def on_process_error(exc: Exception) -> None:
                self.post_process_action = None
                messagebox.showerror("处理失败", str(exc))
                self.status_var.set("处理失败")

            def process_task():
                if reuse_prepared_raw_dataframe:
                    refreshed = raw_dataframe
                else:
                    refreshed = prepare_processing(scoped_sources, self.data_cache).raw_dataframe
                return refreshed, process_dataframe(refreshed, config)

            self.start_background_task(
                task_name="run_processing",
                task_func=process_task,
                success_handler=on_process_success,
                error_handler=on_process_error,
                busy_message="正在处理数据，请稍候...",
            )

        def on_prepare_error(exc: Exception) -> None:
            self.post_process_action = None
            messagebox.showerror("处理失败", str(exc))
            self.status_var.set("处理失败")

        self.start_background_task(
            task_name="prepare_processing",
            task_func=lambda: prepare_processing(scoped_sources, self.data_cache),
            success_handler=on_prepare_success,
            error_handler=on_prepare_error,
            busy_message="正在检查可处理数据，请稍候...",
        )

    def apply_processed_to_old_source(self) -> None:
        if not self.action_allowed():
            return
        if self.processed_df is None or self.processed_writeback_df is None:
            messagebox.showwarning("没有结果", "请先执行“应用处理”并核对预览。")
            return

        scoped_sources = self.get_last_processed_scope_sources()
        if not scoped_sources:
            messagebox.showwarning("没有可写回范围", "请先执行“应用处理”后再写入老数据。")
            return

        target_source = self.resolve_writeback_target_source(scoped_sources)
        if target_source is None:
            return

        confirmed = messagebox.askyesno(
            "确认写回",
            f"将直接覆盖老数据文件中的目标工作表：\n"
            f"{target_source.path.name} / {target_source.sheet_name}\n\n"
            "该操作不可撤销，是否继续？",
            parent=self,
        )
        if not confirmed:
            return

        output_format = output_format_for_source(target_source)
        settings = self.build_export_settings(output_format)
        writeback_df = self.processed_writeback_df.copy()
        def on_success(_: object) -> None:
            writeback = apply_writeback_result(self.data_cache, target_source, writeback_df)
            self.data_cache = writeback.updated_cache
            self.refresh_source_trees()
            self.refresh_available_columns()
            self.clear_tree_selection_except(target_source.dataset_role)
            self.source_trees[target_source.dataset_role].selection_set(target_source.source_id)
            self.preview_source(target_source.source_id)
            self.status_var.set(writeback.status_text)
            self.write_summary(writeback.summary_lines)

        def on_error(exc: Exception) -> None:
            if isinstance(exc, PermissionError):
                messagebox.showerror(
                    "写回失败",
                    f"无法写入文件：{target_source.path}\n\n"
                    "可能原因：\n"
                    "1. 文件正在被 Excel/WPS 等程序占用\n"
                    "2. 文件被设置为只读，或当前账号没有写入权限\n\n"
                    "请先关闭占用该文件的程序后重试。",
                    parent=self,
                )
                self.status_var.set("写回失败：文件被占用或无写入权限")
                return
            messagebox.showerror("写回失败", str(exc))
            self.status_var.set("写回失败")

        self.start_background_task(
            task_name="writeback_old_source",
            task_func=lambda: write_dataframe_back_to_source(writeback_df, target_source, settings),
            success_handler=on_success,
            error_handler=on_error,
            busy_message="正在写回老数据，请稍候...",
        )

    def get_last_processed_scope_sources(self) -> dict[str, SourceSelection]:
        return collect_last_processed_scope_sources(self.sources, self.last_processed_scope_source_ids)

    def resolve_writeback_target_source(self, scoped_sources: dict[str, SourceSelection]) -> SourceSelection | None:
        decision = choose_writeback_target_source(
            scoped_sources,
            self.active_sheet_source_ids.get("old"),
            self.sources,
        )
        if decision.reason == MISSING_OLD:
            messagebox.showwarning("缺少老数据", "当前处理范围中没有可写回的老数据。")
            return None
        if decision.reason == MULTIPLE_OLD:
            messagebox.showinfo(
                "请选择目标老数据",
                "检测到多个老数据工作表，请先双击选中一个老数据 sheet 后再执行“直接写回”。",
                parent=self,
            )
            return None
        return decision.source

    def build_pipeline_config(self) -> PipelineConfig:
        return PipelineConfig(
            duplicate_keys=self.selected_duplicate_keys(),
            duplicate_strategy=self.strategy_label_to_key[self.duplicate_strategy_var.get()],
            filter_rules=list(self.filter_rules),
            update_rules=list(self.update_rules),
            column_settings=dict(self.column_settings),
            include_source_columns=self.include_source_var.get(),
        )

    def open_mapping_dialog(self) -> None:
        if not self.action_allowed():
            return
        candidate_sources = self.get_processing_scope_sources()
        if candidate_sources is None:
            return
        target_columns = collect_target_columns(candidate_sources)
        if not target_columns:
            messagebox.showinfo("缺少老数据模板", "请先导入老数据，再为新数据确认字段匹配。")
            return

        selected_new_sources = self.get_selected_new_sources()
        if not selected_new_sources:
            selected_new_sources = [source for source in candidate_sources.values() if source.dataset_role == "new"]
        if not selected_new_sources:
            messagebox.showinfo("没有新数据", "请先导入新数据。")
            return

        self.open_mapping_dialog_for_sources(selected_new_sources, auto_open=False)

    def get_selected_new_sources(self) -> list[SourceSelection]:
        selected_ids = set(self.get_selected_source_ids())
        return [
            source
            for source in self.sources.values()
            if source.source_id in selected_ids and source.dataset_role == "new"
        ]

    def open_mapping_dialog_for_sources(
        self,
        sources_to_map: list[SourceSelection],
        auto_open: bool,
    ) -> bool:
        context_sources = self.get_mapping_scope_sources(sources_to_map)
        session = build_mapping_session(context_sources, self.data_cache, sources_to_map)
        if not session.target_columns:
            return True

        mapping_changed = False
        for candidate in session.candidates:
            if auto_open and candidate.can_auto_apply:
                candidate.source.source_column_mapping = {
                    str(source_column): candidate.direct_mapping.get(str(source_column), "")
                    for source_column in candidate.dataframe.columns
                }
                candidate.source.mapping_confirmed = True
                mapping_changed = True
                continue

            dialog = SourceMappingDialog(
                self,
                source=candidate.source,
                dataframe=candidate.dataframe,
                target_columns=session.target_columns,
                suggested_mapping=candidate.suggested_mapping,
            )
            self.wait_window(dialog)
            if dialog.result is None:
                if mapping_changed and self.processed_df is not None:
                    self.invalidate_processed_results("字段匹配已变化，请重新执行“应用处理”。")
                self.status_var.set("字段匹配未完成，可稍后继续。")
                return False
            candidate.source.source_column_mapping = dialog.result
            candidate.source.mapping_confirmed = True
            mapping_changed = True

        if mapping_changed and self.processed_df is not None:
            self.invalidate_processed_results("字段匹配已变化，请重新执行“应用处理”。")
        if mapping_changed:
            self.status_var.set("字段匹配已更新")
        return True

    def export_processed_file(self) -> None:
        if not self.action_allowed():
            return
        if self.processed_df is None and self.sources:
            self.post_process_action = self.export_processed_file
            self.apply_processing()
            return

        if self.processed_df is None:
            messagebox.showwarning("没有结果", "请先导入数据并执行处理。")
            return

        output_format = "csv" if self.output_format_var.get() == "CSV (.csv)" else "xlsx"
        if output_format == "csv":
            save_path = filedialog.asksaveasfilename(
                title="保存处理结果",
                defaultextension=".csv",
                filetypes=[("CSV 文件", "*.csv"), ("所有文件", "*.*")],
                initialfile="处理结果.csv",
                initialdir=self.dialog_initialdir(),
            )
            if not save_path:
                self.post_process_action = None
                return
            self.remember_browse_path(save_path)
            settings = self.build_export_settings(output_format)

            def on_csv_success(_: object) -> None:
                self.status_var.set(f"导出完成: {save_path}")
                self.write_summary(build_csv_export_summary(save_path))

            def on_csv_error(exc: Exception) -> None:
                messagebox.showerror("导出失败", str(exc))
                self.status_var.set("导出失败")

            self.start_background_task(
                task_name="export_csv",
                task_func=lambda: export_dataframe(self.processed_df, save_path, settings),
                success_handler=on_csv_success,
                error_handler=on_csv_error,
                busy_message="正在导出 CSV，请稍候...",
            )
            return

        scoped_sources = self.get_last_processed_scope_sources()
        if not scoped_sources:
            messagebox.showwarning("没有可导出范围", "请先执行“应用处理”后再导出。")
            return

        target_source = self.resolve_writeback_target_source(scoped_sources)
        if target_source is None:
            return
        if self.processed_writeback_df is None:
            messagebox.showwarning("没有结果", "请先执行“应用处理”并核对预览。")
            return

        source_suffix = target_source.path.suffix.lower()
        if source_suffix not in {".xlsx", ".xlsm"}:
            messagebox.showwarning("导出失败", "当前老数据不是 Excel 文件，无法导出整本工作簿。")
            return

        export_plan = build_workbook_export_plan(target_source)
        save_path = filedialog.asksaveasfilename(
            title="导出完整老文件（保留全部sheet）",
            defaultextension=export_plan.extension,
            filetypes=[("Excel 文件", "*.xlsx *.xlsm"), ("所有文件", "*.*")],
            initialfile=export_plan.initial_filename,
            initialdir=self.dialog_initialdir(),
        )
        if not save_path:
            self.post_process_action = None
            return
        if paths_refer_to_same_file(save_path, target_source.path):
            self.post_process_action = None
            messagebox.showwarning(
                "导出失败",
                "为保护原始老文件，导出完整老文件时请选择新的保存位置，不能直接覆盖原文件。",
                parent=self,
            )
            self.status_var.set("导出取消：完整老文件不能覆盖原文件")
            return
        self.remember_browse_path(save_path)

        settings = self.build_export_settings("xlsx")
        workbook_df = self.processed_writeback_df.copy()
        def on_workbook_success(_: object) -> None:
            self.status_var.set(f"导出完成: {save_path}")
            self.write_summary(build_workbook_export_summary(save_path, target_source))

        def on_workbook_error(exc: Exception) -> None:
            if isinstance(exc, PermissionError):
                messagebox.showerror(
                    "导出失败",
                    "导出目标文件无法写入，请关闭占用该文件的程序（Excel/WPS）后重试。",
                    parent=self,
                )
                self.status_var.set("导出失败：目标文件被占用")
                return
            messagebox.showerror("导出失败", str(exc))
            self.status_var.set("导出失败")

        self.start_background_task(
            task_name="export_workbook",
            task_func=lambda: export_dataframe_with_old_workbook(workbook_df, target_source, save_path, settings),
            success_handler=on_workbook_success,
            error_handler=on_workbook_error,
            busy_message="正在导出完整老文件，请稍候...",
        )

    def build_export_settings(self, output_format: str) -> ExportSettings:
        return ExportSettings(
            output_format=output_format,
            sheet_name=self.output_sheet_var.get().strip() or "处理结果",
            freeze_header=self.freeze_header_var.get(),
            auto_width=self.auto_width_var.get(),
            style_header=self.style_header_var.get(),
        )

    def prepare_comparison_preview(
        self,
        raw_dataframe: pd.DataFrame,
        config: PipelineConfig,
        processed_df: pd.DataFrame,
    ) -> None:
        baseline_source_df = build_baseline_source_dataframe(raw_dataframe, config)
        baseline_df = apply_column_settings(baseline_source_df, config)
        after_source_df = self.processed_writeback_df.copy() if self.processed_writeback_df is not None else processed_df.copy()
        ignored_columns = get_ignored_compare_columns([str(column) for column in raw_dataframe.columns], config)
        comparison = align_for_comparison(
            baseline_df,
            processed_df,
            config,
            self.column_settings,
            ignored_columns=ignored_columns,
            before_key_df=baseline_source_df,
            after_key_df=after_source_df,
            key_columns=[key for key in config.duplicate_keys if key in baseline_source_df.columns and key in after_source_df.columns],
        )
        self.compare_all_before_df = comparison.before_df
        self.compare_all_after_df = comparison.after_df
        self.compare_all_statuses = comparison.statuses
        self.compare_all_changed_columns = comparison.changed_columns
        self.compare_before_total_rows = len(baseline_df)
        self.compare_after_total_rows = len(processed_df)
        self.refresh_comparison_preview()

    def refresh_comparison_preview(self) -> None:
        self.refresh_compare_filter_button()
        if self.compare_all_before_df is None or self.compare_all_after_df is None:
            return

        (
            self.compare_before_df,
            self.compare_after_df,
            self.compare_statuses,
            self.compare_changed_columns,
        ) = filter_comparison_rows(
            self.compare_all_before_df,
            self.compare_all_after_df,
            self.compare_all_statuses,
            self.compare_all_changed_columns,
            changes_only=self.compare_changes_only_var.get(),
        )
        self.populate_comparison_preview(
            self.compare_before_df,
            self.compare_after_df,
            self.compare_statuses,
            self.compare_changed_columns,
            before_total_rows=self.compare_before_total_rows,
            after_total_rows=self.compare_after_total_rows,
            changes_only=self.compare_changes_only_var.get(),
        )

    def populate_comparison_preview(
        self,
        before_df: pd.DataFrame,
        after_df: pd.DataFrame,
        statuses: list[str],
        changed_columns: list[set[str]],
        before_total_rows: int,
        after_total_rows: int,
        changes_only: bool,
    ) -> None:
        displayed_rows = max(len(before_df), len(after_df))
        before_info, after_info = build_comparison_info(
            before_total_rows,
            after_total_rows,
            displayed_rows,
            changes_only=changes_only,
        )
        column_widths = self.compute_compare_column_widths(before_df, after_df)
        self.compare_column_widths = column_widths
        self.hide_compare_tooltip()
        self.populate_comparison_text(self.compare_before_text, before_df, statuses, changed_columns, side="before", column_widths=column_widths)
        self.populate_comparison_text(self.compare_after_text, after_df, statuses, changed_columns, side="after", column_widths=column_widths)
        self.compare_before_info_label.configure(text=before_info)
        self.compare_after_info_label.configure(text=after_info)

    def compute_compare_column_widths(self, before_df: pd.DataFrame, after_df: pd.DataFrame) -> dict[str, int]:
        return compute_compare_column_widths(before_df, after_df)

    def populate_comparison_text(
        self,
        text_widget: tk.Text,
        dataframe: pd.DataFrame,
        statuses: list[str],
        changed_columns: list[set[str]],
        side: str,
        column_widths: dict[str, int],
    ) -> None:
        text_widget.configure(state="normal")
        text_widget.delete("1.0", tk.END)
        if dataframe is None or dataframe.empty:
            text_widget.configure(state="disabled")
            return

        columns = [str(column) for column in dataframe.columns]
        display_columns = build_compare_display_columns(columns)
        self.insert_compare_line(text_widget, display_columns, column_widths, header=True)
        self.insert_compare_separator(text_widget, display_columns, column_widths)

        for row_index, row in enumerate(dataframe.itertuples(index=False, name=None)):
            status = statuses[row_index] if row_index < len(statuses) else "same"
            changed = changed_columns[row_index] if row_index < len(changed_columns) else set()
            for display_index, column in enumerate(display_columns):
                if column == PREVIEW_ROW_NUMBER_COLUMN:
                    self.insert_compare_cell(text_widget, str(row_index + 1), column_widths[column], "", None)
                else:
                    value = row[display_index - 1]
                    display = self.preview_value(value)
                    marker, marker_tag = marker_for_cell(status, side, column, changed)
                    self.insert_compare_cell(text_widget, display, column_widths[column], marker, marker_tag)
                if column != display_columns[-1]:
                    text_widget.insert(tk.END, " | ", ("separator",))
            text_widget.insert(tk.END, "\n")
        text_widget.configure(state="disabled")

    def insert_compare_line(
        self,
        text_widget: tk.Text,
        columns: list[str],
        column_widths: dict[str, int],
        header: bool = False,
    ) -> None:
        for index, column in enumerate(columns):
            display = self.fit_compare_text(display_compare_column_name(column), column_widths[column])
            tag = "header" if header else ()
            text_widget.insert(tk.END, f"  {display}", tag)
            if index != len(columns) - 1:
                text_widget.insert(tk.END, " | ", ("separator",))
        text_widget.insert(tk.END, "\n")

    def insert_compare_separator(
        self,
        text_widget: tk.Text,
        columns: list[str],
        column_widths: dict[str, int],
    ) -> None:
        for index, column in enumerate(columns):
            text_widget.insert(tk.END, "-" * (column_widths[column] + 2), ("separator",))
            if index != len(columns) - 1:
                text_widget.insert(tk.END, "-+-", ("separator",))
        text_widget.insert(tk.END, "\n")

    def insert_compare_cell(
        self,
        text_widget: tk.Text,
        value: str,
        width: int,
        marker: str,
        marker_tag: str | None,
    ) -> None:
        fitted = self.fit_compare_text(value, width)
        if marker and marker_tag:
            text_widget.insert(tk.END, marker, (marker_tag,))
            text_widget.insert(tk.END, " ")
            value_tag = "plus_value" if marker_tag == "plus" else "minus_value"
            text_widget.insert(tk.END, fitted, (value_tag,))
        else:
            text_widget.insert(tk.END, "  ")
            text_widget.insert(tk.END, fitted)

    def fit_compare_text(self, text: str, width: int) -> str:
        return fit_compare_text(text, width)

    def get_selected_rule_index(self, tree: ttk.Treeview) -> int | None:
        selected = tree.selection()
        if not selected:
            return None
        try:
            return int(selected[0])
        except (TypeError, ValueError):
            return None

    def add_filter_rule(self) -> None:
        if not self.action_allowed():
            return
        if not self.available_columns:
            messagebox.showinfo("没有字段", "请先导入文件。")
            return

        dialog = FilterRuleDialog(self, self.available_columns, self.column_display_label)
        self.wait_window(dialog)
        if dialog.result is None:
            return

        self.filter_rules.append(dialog.result)
        self.refresh_filter_tree()
        self.invalidate_processed_results()
        self.status_var.set("已新增筛选规则")

    def edit_selected_filter_rule(self) -> None:
        if not self.action_allowed():
            return
        if not self.available_columns:
            messagebox.showinfo("没有字段", "请先导入文件。")
            return
        index = self.get_selected_rule_index(self.filter_tree)
        if index is None or index >= len(self.filter_rules):
            return

        dialog = FilterRuleDialog(
            self,
            self.available_columns,
            self.column_display_label,
            initial_rule=self.filter_rules[index],
        )
        self.wait_window(dialog)
        if dialog.result is None:
            return

        self.filter_rules[index] = dialog.result
        self.refresh_filter_tree()
        self.filter_tree.selection_set(str(index))
        self.invalidate_processed_results("筛选规则已变化，请重新执行“应用处理”。")
        self.status_var.set("已更新筛选规则")

    def remove_selected_filter_rule(self) -> None:
        if not self.action_allowed():
            return
        selected = self.filter_tree.selection()
        if not selected:
            return
        indexes = sorted((int(item_id) for item_id in selected), reverse=True)
        for index in indexes:
            self.filter_rules.pop(index)
        self.refresh_filter_tree()
        self.invalidate_processed_results("筛选规则已变化，请重新执行“应用处理”。")

    def clear_filter_rules(self) -> None:
        if not self.action_allowed():
            return
        self.filter_rules.clear()
        self.refresh_filter_tree()
        self.invalidate_processed_results("筛选规则已清空，请重新执行“应用处理”。")

    def refresh_filter_tree(self) -> None:
        self.filter_tree.delete(*self.filter_tree.get_children())
        for index, rule in enumerate(self.filter_rules):
            self.filter_tree.insert(
                "",
                "end",
                iid=str(index),
                values=(self.column_display_label(rule.column), FILTER_OPERATORS[rule.operator], rule.value),
            )

    def add_update_rule(self) -> None:
        if not self.action_allowed():
            return
        if not self.available_columns:
            messagebox.showinfo("没有字段", "请先导入文件。")
            return

        dialog = UpdateRuleDialog(self, self.available_columns, self.column_display_label)
        self.wait_window(dialog)
        if dialog.result is None:
            return

        self.update_rules.append(dialog.result)
        self.refresh_update_tree()
        self.invalidate_processed_results()
        self.status_var.set("已新增更新规则")

    def edit_selected_update_rule(self) -> None:
        if not self.action_allowed():
            return
        if not self.available_columns:
            messagebox.showinfo("没有字段", "请先导入文件。")
            return
        index = self.get_selected_rule_index(self.update_tree)
        if index is None or index >= len(self.update_rules):
            return

        dialog = UpdateRuleDialog(
            self,
            self.available_columns,
            self.column_display_label,
            initial_rule=self.update_rules[index],
        )
        self.wait_window(dialog)
        if dialog.result is None:
            return

        self.update_rules[index] = dialog.result
        self.refresh_update_tree()
        self.update_tree.selection_set(str(index))
        self.invalidate_processed_results("更新规则已变化，请重新执行“应用处理”。")
        self.status_var.set("已更新更新规则")

    def remove_selected_update_rule(self) -> None:
        if not self.action_allowed():
            return
        selected = self.update_tree.selection()
        if not selected:
            return
        indexes = sorted((int(item_id) for item_id in selected), reverse=True)
        for index in indexes:
            self.update_rules.pop(index)
        self.refresh_update_tree()
        self.invalidate_processed_results("更新规则已变化，请重新执行“应用处理”。")

    def clear_update_rules(self) -> None:
        if not self.action_allowed():
            return
        self.update_rules.clear()
        self.refresh_update_tree()
        self.invalidate_processed_results("更新规则已清空，请重新执行“应用处理”。")

    def refresh_update_tree(self) -> None:
        self.update_tree.delete(*self.update_tree.get_children())
        for index, rule in enumerate(self.update_rules):
            self.update_tree.insert(
                "",
                "end",
                iid=str(index),
                values=(
                    self.column_display_label(rule.column),
                    UPDATE_MODES[rule.mode],
                    rule.find_value,
                    rule.replace_value,
                ),
            )

    def open_column_settings(self) -> None:
        if not self.action_allowed():
            return
        if not self.available_columns:
            messagebox.showinfo("没有字段", "请先导入文件。")
            return

        dialog = ColumnSettingsDialog(self, self.available_columns, self.column_settings, self.column_display_label)
        self.wait_window(dialog)
        if dialog.result is None:
            return

        self.column_settings = dialog.result
        self.invalidate_processed_results()
        self.status_var.set("字段设置已更新")

    def column_display_label(self, column_name: str) -> str:
        if column_name in INTERNAL_COLUMNS:
            return f"{default_display_name(column_name)} ({column_name})"
        return column_name

    def populate_dataframe_preview(
        self,
        tree: ttk.Treeview,
        dataframe: pd.DataFrame | None,
        info_label: ttk.Label,
        info_text: str,
    ) -> None:
        tree.delete(*tree.get_children())
        self.tree_preview_frames.pop(id(tree), None)
        if dataframe is None:
            tree["columns"] = ()
            info_label.configure(text=info_text)
            return

        self.tree_preview_frames[id(tree)] = dataframe
        data_columns = [str(column) for column in dataframe.columns]
        columns = [PREVIEW_ROW_NUMBER_COLUMN, *data_columns]
        tree["columns"] = columns

        for column in columns:
            if column == PREVIEW_ROW_NUMBER_COLUMN:
                tree.heading(column, text=PREVIEW_ROW_NUMBER_LABEL)
                tree.column(column, width=64, anchor="e", stretch=False)
            else:
                tree.heading(column, text=column)
                tree.column(column, width=130, anchor="w", stretch=True)

        for row_index, row in enumerate(dataframe.itertuples(index=False, name=None), start=1):
            tree.insert("", "end", values=[str(row_index), *[self.preview_value(value) for value in row]])

        info_label.configure(text=info_text)

    def preview_value(self, value: object) -> str:
        return format_preview_value(value)

    def write_summary(self, lines: list[str]) -> None:
        self.summary_text.configure(state="normal")
        self.summary_text.delete("1.0", tk.END)
        self.summary_text.insert(tk.END, "\n".join(lines))
        self.summary_text.configure(state="disabled")
