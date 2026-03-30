from __future__ import annotations

import tkinter as tk
from tkinter import ttk

import pandas as pd

from .comparison import (
    align_for_comparison,
    build_baseline_dataframe,
    build_baseline_source_dataframe,
    full_preview_value,
    get_ignored_compare_columns,
    preview_value as format_preview_value,
)
from .compare_render import (
    PREVIEW_ROW_NUMBER_COLUMN,
    PREVIEW_ROW_NUMBER_LABEL,
    build_compare_display_columns,
    build_compare_text_content,
    build_comparison_info,
    compute_compare_column_widths,
    display_compare_column_name,
    filter_comparison_rows,
    fit_compare_text,
    marker_for_cell,
)

PREVIEW_RENDER_BATCH_SIZE = 200


class UiCompareMixin:
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
        x_scroll = ttk.Scrollbar(
            container,
            orient=tk.HORIZONTAL,
            command=lambda *args, active_side=side: self.on_compare_xscroll(active_side, *args),
        )
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
        value = dataframe.iloc[row_index].get(column_name, "")
        full_text = full_preview_value(value)
        displayed_text = self.preview_value(value)
        if not full_text:
            self.hide_compare_tooltip()
            return
        if full_text == displayed_text and len(displayed_text) <= self.compare_column_widths.get(column_name, 0):
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

    def prepare_comparison_preview(
        self,
        raw_dataframe: pd.DataFrame,
        config: object,
        processed_df: pd.DataFrame,
    ) -> None:
        baseline_source_df = build_baseline_source_dataframe(raw_dataframe, config)
        baseline_df = build_baseline_dataframe(raw_dataframe, list(processed_df.columns), config)
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
        content, tag_ranges = build_compare_text_content(
            dataframe,
            statuses,
            changed_columns,
            side=side,
            column_widths=column_widths,
        )
        if not content:
            text_widget.configure(state="disabled")
            return
        text_widget.insert("1.0", content)
        for tag_name, start, end in tag_ranges:
            text_widget.tag_add(tag_name, f"1.0 + {start} chars", f"1.0 + {end} chars")
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

    def populate_dataframe_preview(
        self,
        tree: ttk.Treeview,
        dataframe: pd.DataFrame | None,
        info_label: ttk.Label,
        info_text: str,
    ) -> None:
        UiCompareMixin.cancel_preview_render(self, tree)
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

        info_label.configure(text=info_text)
        if not callable(getattr(self, "after", None)):
            UiCompareMixin.insert_preview_rows(self, tree, dataframe, start=0, stop=len(dataframe))
            return

        tree_id = id(tree)
        generations = getattr(self, "tree_preview_generations", None)
        if not isinstance(generations, dict):
            generations = {}
            setattr(self, "tree_preview_generations", generations)
        generation = generations.get(tree_id, 0) + 1
        generations[tree_id] = generation
        UiCompareMixin._populate_dataframe_preview_batch(
            self,
            tree,
            dataframe,
            info_label,
            info_text,
            start=0,
            generation=generation,
        )

    def cancel_preview_render(self, tree: ttk.Treeview) -> None:
        after_ids = getattr(self, "tree_preview_after_ids", None)
        if not isinstance(after_ids, dict):
            return
        after_id = after_ids.pop(id(tree), None)
        if after_id is None or not callable(getattr(self, "after_cancel", None)):
            return
        try:
            self.after_cancel(after_id)
        except tk.TclError:
            pass

    def insert_preview_rows(
        self,
        tree: ttk.Treeview,
        dataframe: pd.DataFrame,
        start: int,
        stop: int,
    ) -> None:
        for row_index, row in enumerate(dataframe.iloc[start:stop].itertuples(index=False, name=None), start=start + 1):
            tree.insert("", "end", values=[str(row_index), *[self.preview_value(value) for value in row]])

    def _populate_dataframe_preview_batch(
        self,
        tree: ttk.Treeview,
        dataframe: pd.DataFrame,
        info_label: ttk.Label,
        info_text: str,
        start: int,
        generation: int,
    ) -> None:
        tree_id = id(tree)
        generations = getattr(self, "tree_preview_generations", None)
        if not isinstance(generations, dict) or generations.get(tree_id) != generation:
            return
        if not self._tree_widget_available(tree):
            after_ids = getattr(self, "tree_preview_after_ids", None)
            if isinstance(after_ids, dict):
                after_ids.pop(tree_id, None)
            return

        stop = min(start + PREVIEW_RENDER_BATCH_SIZE, len(dataframe))
        self.insert_preview_rows(tree, dataframe, start, stop)
        if stop >= len(dataframe):
            after_ids = getattr(self, "tree_preview_after_ids", None)
            if isinstance(after_ids, dict):
                after_ids.pop(tree_id, None)
            info_label.configure(text=info_text)
            return

        info_label.configure(text=f"{info_text} | 预览渲染中 {stop}/{len(dataframe)} 行")
        after_ids = getattr(self, "tree_preview_after_ids", None)
        if not isinstance(after_ids, dict):
            after_ids = {}
            setattr(self, "tree_preview_after_ids", after_ids)
        after_ids[tree_id] = self.after(
            1,
            lambda active_tree=tree, active_dataframe=dataframe, active_label=info_label, active_text=info_text, active_stop=stop, active_generation=generation: self._populate_dataframe_preview_batch(
                active_tree,
                active_dataframe,
                active_label,
                active_text,
                start=active_stop,
                generation=active_generation,
            ),
        )

    def _tree_widget_available(self, tree: ttk.Treeview) -> bool:
        exists_method = getattr(tree, "winfo_exists", None)
        if not callable(exists_method):
            return True
        try:
            return bool(exists_method())
        except tk.TclError:
            return False

    def preview_value(self, value: object) -> str:
        return format_preview_value(value)

    def write_summary(self, lines: list[str]) -> None:
        self.summary_text.configure(state="normal")
        self.summary_text.delete("1.0", tk.END)
        self.summary_text.insert(tk.END, "\n".join(lines))
        self.summary_text.configure(state="disabled")
