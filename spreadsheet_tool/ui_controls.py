from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from .dialogs import ColumnSettingsDialog, FilterRuleDialog, UpdateRuleDialog
from .models import ColumnSetting
from .processor_common import FILTER_OPERATORS, INTERNAL_COLUMNS, UPDATE_MODES, default_display_name


class UiControlsMixin:
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
            values=list(self.strategy_label_to_key),
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
        self.bind_listbox_tooltip(
            self.key_listbox,
            lambda index: self.column_display_label(self.available_columns[index]) if index < len(self.available_columns) else "",
        )
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
        edit_filter_btn = ttk.Button(button_bar, text="编辑选中", style="Compact.TButton", width=7, command=self.edit_selected_filter_rule)
        edit_filter_btn.pack(side=tk.LEFT, padx=(0, 6))
        remove_filter_btn = ttk.Button(button_bar, text="删除选中", style="Compact.TButton", width=7, command=self.remove_selected_filter_rule)
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
        edit_update_btn = ttk.Button(button_bar, text="编辑选中", style="Compact.TButton", width=7, command=self.edit_selected_update_rule)
        edit_update_btn.pack(side=tk.LEFT, padx=(0, 6))
        remove_update_btn = ttk.Button(button_bar, text="删除选中", style="Compact.TButton", width=7, command=self.remove_selected_update_rule)
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

        ttk.Label(parent, text="工作表名称（仅新建导出）").grid(row=1, column=0, sticky="w")
        output_sheet_entry = ttk.Entry(parent, textvariable=self.output_sheet_var)
        output_sheet_entry.grid(row=1, column=1, sticky="ew", pady=(0, 8))
        self.register_busy_ttk_widget(output_sheet_entry)

        include_source_check = ttk.Checkbutton(parent, text="输出来源字段", variable=self.include_source_var)
        include_source_check.grid(row=2, column=0, columnspan=2, sticky="w")
        self.register_busy_ttk_widget(include_source_check)
        freeze_header_check = ttk.Checkbutton(parent, text="新建工作表时冻结首行", variable=self.freeze_header_var)
        freeze_header_check.grid(row=3, column=0, columnspan=2, sticky="w")
        self.register_busy_ttk_widget(freeze_header_check)
        auto_width_check = ttk.Checkbutton(parent, text="新建工作表时自动列宽", variable=self.auto_width_var)
        auto_width_check.grid(row=4, column=0, columnspan=2, sticky="w")
        self.register_busy_ttk_widget(auto_width_check)
        style_header_check = ttk.Checkbutton(parent, text="新建工作表时设置表头样式", variable=self.style_header_var)
        style_header_check.grid(row=5, column=0, columnspan=2, sticky="w")
        self.register_busy_ttk_widget(style_header_check)

        ttk.Label(
            parent,
            text=(
                "直接写回老数据或导出完整老文件时，会保留目标工作表原有结构；"
                "上面的工作表名称、冻结首行、自动列宽、表头样式仅在新建工作表时生效。"
                " 字段显示/隐藏和导出名称修改，请使用顶部“字段设置”。"
            ),
            wraplength=320,
            style="Summary.TLabel",
        ).grid(row=6, column=0, columnspan=2, sticky="w", pady=(10, 0))

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
