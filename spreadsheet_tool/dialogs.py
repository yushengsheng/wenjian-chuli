from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk
from typing import TYPE_CHECKING

import pandas as pd

from .models import ColumnSetting, FilterRule, SourceSelection, UpdateRule
from .processor import (
    FILTER_OPERATORS,
    INTERNAL_COLUMNS,
    UPDATE_MODES,
    default_display_name,
    default_visible,
    infer_source_column_kind,
)

if TYPE_CHECKING:
    from .ui import SpreadsheetApp


class FilterRuleDialog(tk.Toplevel):
    MISSING_COLUMN_SUFFIX = "（原字段已不存在）"

    def __init__(
        self,
        parent: SpreadsheetApp,
        columns: list[str],
        label_builder,
        initial_rule: FilterRule | None = None,
    ) -> None:
        super().__init__(parent)
        self.title("编辑筛选规则" if initial_rule is not None else "新增筛选规则")
        self.transient(parent)
        self.grab_set()
        self.resizable(False, False)
        self.result: FilterRule | None = None
        self.column_map = {label_builder(column): column for column in columns}
        self.missing_column_label: str | None = None
        self.operator_map = {label: key for key, label in FILTER_OPERATORS.items()}
        self.reverse_operator_map = {key: label for label, key in self.operator_map.items()}

        default_column = initial_rule.column if initial_rule is not None else columns[0]
        default_operator = initial_rule.operator if initial_rule is not None else "equals"
        default_value = initial_rule.value if initial_rule is not None else ""
        default_column_label = label_builder(default_column)
        column_options = list(self.column_map)
        if default_column_label not in self.column_map:
            self.missing_column_label = f"{default_column_label}{self.MISSING_COLUMN_SUFFIX}"
            column_options.insert(0, self.missing_column_label)

        self.column_var = tk.StringVar(value=self.missing_column_label or default_column_label)
        self.operator_var = tk.StringVar(value=self.reverse_operator_map.get(default_operator, FILTER_OPERATORS["equals"]))
        self.value_var = tk.StringVar(value=default_value)

        body = ttk.Frame(self, padding=12)
        body.grid(row=0, column=0, sticky="nsew")
        body.columnconfigure(1, weight=1)

        ttk.Label(body, text="字段").grid(row=0, column=0, sticky="w", pady=(0, 8))
        ttk.Combobox(body, textvariable=self.column_var, values=column_options, state="readonly").grid(
            row=0, column=1, sticky="ew", pady=(0, 8)
        )

        ttk.Label(body, text="条件").grid(row=1, column=0, sticky="w", pady=(0, 8))
        operator_box = ttk.Combobox(body, textvariable=self.operator_var, values=list(self.operator_map), state="readonly")
        operator_box.grid(row=1, column=1, sticky="ew", pady=(0, 8))
        operator_box.bind("<<ComboboxSelected>>", self._refresh_value_state)

        ttk.Label(body, text="值").grid(row=2, column=0, sticky="w")
        self.value_entry = ttk.Entry(body, textvariable=self.value_var)
        self.value_entry.grid(row=2, column=1, sticky="ew")

        button_bar = ttk.Frame(body)
        button_bar.grid(row=3, column=0, columnspan=2, sticky="e", pady=(12, 0))
        ttk.Button(button_bar, text="取消", command=self.destroy).pack(side=tk.RIGHT)
        ttk.Button(button_bar, text="确定", command=self.on_confirm).pack(side=tk.RIGHT, padx=(0, 8))

        self.bind("<Return>", lambda _: self.on_confirm())
        self.bind("<Escape>", lambda _: self.destroy())
        self._refresh_value_state()
        self.value_entry.focus_set()

    def _refresh_value_state(self, _: object | None = None) -> None:
        operator_key = self.operator_map[self.operator_var.get()]
        state = "disabled" if operator_key in {"is_empty", "not_empty"} else "normal"
        self.value_entry.configure(state=state)
        if state == "disabled":
            self.value_var.set("")

    def on_confirm(self) -> None:
        selected_column_label = self.column_var.get()
        if selected_column_label not in self.column_map:
            messagebox.showwarning("字段已失效", "当前规则引用的原字段已不存在，请重新选择字段。", parent=self)
            return
        operator_key = self.operator_map[self.operator_var.get()]
        if operator_key not in {"is_empty", "not_empty"} and not self.value_var.get():
            messagebox.showwarning("缺少值", "当前筛选条件需要填写比较值。", parent=self)
            return

        self.result = FilterRule(
            column=self.column_map[selected_column_label],
            operator=operator_key,
            value=self.value_var.get(),
        )
        self.destroy()


class UpdateRuleDialog(tk.Toplevel):
    MISSING_COLUMN_SUFFIX = "（原字段已不存在）"

    def __init__(
        self,
        parent: SpreadsheetApp,
        columns: list[str],
        label_builder,
        initial_rule: UpdateRule | None = None,
    ) -> None:
        super().__init__(parent)
        self.title("编辑更新规则" if initial_rule is not None else "新增更新规则")
        self.transient(parent)
        self.grab_set()
        self.resizable(False, False)
        self.result: UpdateRule | None = None
        self.column_map = {label_builder(column): column for column in columns}
        self.missing_column_label: str | None = None
        self.mode_map = {label: key for key, label in UPDATE_MODES.items()}
        self.reverse_mode_map = {key: label for label, key in self.mode_map.items()}

        default_column = initial_rule.column if initial_rule is not None else columns[0]
        default_mode = initial_rule.mode if initial_rule is not None else "set_value"
        default_find = initial_rule.find_value if initial_rule is not None else ""
        default_replace = initial_rule.replace_value if initial_rule is not None else ""
        default_column_label = label_builder(default_column)
        column_options = list(self.column_map)
        if default_column_label not in self.column_map:
            self.missing_column_label = f"{default_column_label}{self.MISSING_COLUMN_SUFFIX}"
            column_options.insert(0, self.missing_column_label)

        self.column_var = tk.StringVar(value=self.missing_column_label or default_column_label)
        self.mode_var = tk.StringVar(value=self.reverse_mode_map.get(default_mode, UPDATE_MODES["set_value"]))
        self.find_var = tk.StringVar(value=default_find)
        self.replace_var = tk.StringVar(value=default_replace)

        body = ttk.Frame(self, padding=12)
        body.grid(row=0, column=0, sticky="nsew")
        body.columnconfigure(1, weight=1)

        ttk.Label(body, text="字段").grid(row=0, column=0, sticky="w", pady=(0, 8))
        ttk.Combobox(body, textvariable=self.column_var, values=column_options, state="readonly").grid(
            row=0, column=1, sticky="ew", pady=(0, 8)
        )

        ttk.Label(body, text="操作").grid(row=1, column=0, sticky="w", pady=(0, 8))
        mode_box = ttk.Combobox(body, textvariable=self.mode_var, values=list(self.mode_map), state="readonly")
        mode_box.grid(row=1, column=1, sticky="ew", pady=(0, 8))
        mode_box.bind("<<ComboboxSelected>>", self._refresh_field_state)

        ttk.Label(body, text="查找值").grid(row=2, column=0, sticky="w", pady=(0, 8))
        self.find_entry = ttk.Entry(body, textvariable=self.find_var)
        self.find_entry.grid(row=2, column=1, sticky="ew", pady=(0, 8))

        ttk.Label(body, text="新值").grid(row=3, column=0, sticky="w")
        ttk.Entry(body, textvariable=self.replace_var).grid(row=3, column=1, sticky="ew")

        button_bar = ttk.Frame(body)
        button_bar.grid(row=4, column=0, columnspan=2, sticky="e", pady=(12, 0))
        ttk.Button(button_bar, text="取消", command=self.destroy).pack(side=tk.RIGHT)
        ttk.Button(button_bar, text="确定", command=self.on_confirm).pack(side=tk.RIGHT, padx=(0, 8))

        self.bind("<Return>", lambda _: self.on_confirm())
        self.bind("<Escape>", lambda _: self.destroy())
        self._refresh_field_state()
        self.find_entry.focus_set()

    def _refresh_field_state(self, _: object | None = None) -> None:
        mode_key = self.mode_map[self.mode_var.get()]
        find_required = mode_key in {"replace_text", "replace_exact"}
        self.find_entry.configure(state="normal" if find_required else "disabled")
        if not find_required:
            self.find_var.set("")

    def on_confirm(self) -> None:
        selected_column_label = self.column_var.get()
        if selected_column_label not in self.column_map:
            messagebox.showwarning("字段已失效", "当前规则引用的原字段已不存在，请重新选择字段。", parent=self)
            return
        mode_key = self.mode_map[self.mode_var.get()]
        if mode_key in {"replace_text", "replace_exact"} and not self.find_var.get():
            messagebox.showwarning("缺少查找值", "当前更新规则需要填写查找值。", parent=self)
            return

        self.result = UpdateRule(
            mode=mode_key,
            column=self.column_map[selected_column_label],
            find_value=self.find_var.get(),
            replace_value=self.replace_var.get(),
        )
        self.destroy()


class SourceMappingDialog(tk.Toplevel):
    IGNORE_LABEL = "忽略该列"

    def __init__(
        self,
        parent: SpreadsheetApp,
        source: SourceSelection,
        dataframe: pd.DataFrame,
        target_columns: list[str],
        suggested_mapping: dict[str, str],
    ) -> None:
        super().__init__(parent)
        self.title(f"字段匹配确认 - {source.path.name} / {source.sheet_name}")
        self.transient(parent)
        self.grab_set()
        self.geometry("980x620")
        self.result: dict[str, str] | None = None
        self.parent_app = parent
        self.source = source
        self.dataframe = dataframe
        self.target_columns = target_columns
        self.mapping_vars: dict[str, tk.StringVar] = {}
        self.options = [self.IGNORE_LABEL, *target_columns]

        container = ttk.Frame(self, padding=12)
        container.grid(row=0, column=0, sticky="nsew")
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        intro = (
            "请确认新数据各列对应到老数据模板的哪个字段。"
            " 程序已给出自动建议；如果某列不需要导入，保留“忽略该列”即可。"
        )
        ttk.Label(container, text=intro, wraplength=920, style="Summary.TLabel").grid(row=0, column=0, sticky="w")

        canvas = tk.Canvas(container, highlightthickness=0)
        canvas.grid(row=1, column=0, sticky="nsew", pady=(10, 0))
        scrollbar = ttk.Scrollbar(container, orient=tk.VERTICAL, command=canvas.yview)
        scrollbar.grid(row=1, column=1, sticky="ns", pady=(10, 0))
        canvas.configure(yscrollcommand=scrollbar.set)

        inner = ttk.Frame(canvas)
        inner.bind("<Configure>", lambda _: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=inner, anchor="nw")
        inner.columnconfigure(1, weight=1)
        inner.columnconfigure(3, weight=1)

        ttk.Label(inner, text="源列", style="Title.TLabel").grid(row=0, column=0, sticky="w", padx=(0, 12))
        ttk.Label(inner, text="示例值", style="Title.TLabel").grid(row=0, column=1, sticky="w", padx=(0, 12))
        ttk.Label(inner, text="识别类型", style="Title.TLabel").grid(row=0, column=2, sticky="w", padx=(0, 12))
        ttk.Label(inner, text="映射到", style="Title.TLabel").grid(row=0, column=3, sticky="w")

        existing_mapping = source.source_column_mapping or {}
        for row_index, source_column in enumerate(dataframe.columns, start=1):
            default_value = existing_mapping.get(source_column)
            if default_value is None:
                default_value = suggested_mapping.get(source_column, self.IGNORE_LABEL)
            if default_value == "":
                default_value = self.IGNORE_LABEL

            var = tk.StringVar(value=default_value)
            self.mapping_vars[str(source_column)] = var

            source_text = str(source_column)
            sample_text = self.sample_values_for_column(source_column)
            source_label = ttk.Label(inner, text=source_text)
            source_label.grid(row=row_index, column=0, sticky="w", padx=(0, 12), pady=4)
            self.parent_app.register_tooltip(source_label, source_text)

            sample_label = ttk.Label(inner, text=sample_text, wraplength=320)
            sample_label.grid(row=row_index, column=1, sticky="w", padx=(0, 12), pady=4)
            self.parent_app.register_tooltip(sample_label, self.full_sample_values_for_column(source_column))

            kind_text = infer_source_column_kind(dataframe[source_column])
            kind_label = ttk.Label(inner, text=kind_text)
            kind_label.grid(row=row_index, column=2, sticky="w", padx=(0, 12), pady=4)
            self.parent_app.register_tooltip(kind_label, f"识别类型：{kind_text}")

            combo = ttk.Combobox(inner, textvariable=var, values=self.options, state="readonly")
            combo.grid(row=row_index, column=3, sticky="ew", pady=4)
            self.parent_app.register_tooltip(combo, lambda v=var: f"当前映射到：{v.get()}")

        button_bar = ttk.Frame(container)
        button_bar.grid(row=2, column=0, sticky="e", pady=(12, 0))
        ttk.Button(button_bar, text="全部忽略", command=self.set_all_ignore).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(button_bar, text="取消", command=self.destroy).pack(side=tk.RIGHT)
        ttk.Button(button_bar, text="保存匹配", command=self.on_confirm).pack(side=tk.RIGHT, padx=(0, 8))

        self.bind("<Return>", lambda _: self.on_confirm())
        self.bind("<Escape>", lambda _: self.destroy())

    def sample_values_for_column(self, source_column: object) -> str:
        series = self.dataframe[source_column].dropna().tolist()
        values: list[str] = []
        for value in series[:3]:
            text = str(value).strip()
            if not text:
                continue
            values.append(text if len(text) <= 36 else f"{text[:33]}...")
        return " | ".join(values) if values else "(空列)"

    def full_sample_values_for_column(self, source_column: object) -> str:
        series = self.dataframe[source_column].dropna().tolist()
        values: list[str] = []
        for value in series[:5]:
            text = str(value).strip()
            if not text:
                continue
            values.append(text)
        return "\n".join(values) if values else "(空列)"

    def set_all_ignore(self) -> None:
        for var in self.mapping_vars.values():
            var.set(self.IGNORE_LABEL)

    def on_confirm(self) -> None:
        chosen_targets: dict[str, str] = {}
        result: dict[str, str] = {}
        for source_column, var in self.mapping_vars.items():
            target_column = var.get().strip()
            if not target_column or target_column == self.IGNORE_LABEL:
                result[source_column] = ""
                continue
            if target_column in chosen_targets:
                messagebox.showwarning(
                    "重复映射",
                    f"字段“{target_column}”被重复分配给了“{chosen_targets[target_column]}”和“{source_column}”。",
                    parent=self,
                )
                return
            chosen_targets[target_column] = source_column
            result[source_column] = target_column
        self.result = result
        self.destroy()


class ColumnSettingsDialog(tk.Toplevel):
    def __init__(
        self,
        parent: SpreadsheetApp,
        columns: list[str],
        existing_settings: dict[str, ColumnSetting],
        label_builder,
    ) -> None:
        super().__init__(parent)
        self.title("字段设置")
        self.transient(parent)
        self.grab_set()
        self.geometry("780x560")
        self.result: dict[str, ColumnSetting] | None = None
        self.columns = columns
        self.visible_vars: dict[str, tk.BooleanVar] = {}
        self.rename_vars: dict[str, tk.StringVar] = {}

        container = ttk.Frame(self, padding=12)
        container.grid(row=0, column=0, sticky="nsew")
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        top_bar = ttk.Frame(container)
        top_bar.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Button(top_bar, text="全部显示", command=self.show_all).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(top_bar, text="隐藏来源字段", command=self.hide_internal).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(top_bar, text="恢复默认", command=self.restore_defaults).pack(side=tk.LEFT)

        canvas = tk.Canvas(container, highlightthickness=0)
        canvas.grid(row=1, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(container, orient=tk.VERTICAL, command=canvas.yview)
        scrollbar.grid(row=1, column=1, sticky="ns")
        canvas.configure(yscrollcommand=scrollbar.set)

        inner = ttk.Frame(canvas)
        inner.bind("<Configure>", lambda _: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=inner, anchor="nw")
        inner.columnconfigure(2, weight=1)

        ttk.Label(inner, text="显示", style="Title.TLabel").grid(row=0, column=0, sticky="w", padx=(0, 12))
        ttk.Label(inner, text="原字段", style="Title.TLabel").grid(row=0, column=1, sticky="w", padx=(0, 12))
        ttk.Label(inner, text="导出名称", style="Title.TLabel").grid(row=0, column=2, sticky="w")

        for row_index, column in enumerate(columns, start=1):
            setting = existing_settings.get(
                column,
                ColumnSetting(visible=default_visible(column), rename_to=default_display_name(column)),
            )
            visible_var = tk.BooleanVar(value=setting.visible)
            rename_var = tk.StringVar(value=setting.rename_to or default_display_name(column))
            self.visible_vars[column] = visible_var
            self.rename_vars[column] = rename_var

            ttk.Checkbutton(inner, variable=visible_var).grid(row=row_index, column=0, sticky="w", padx=(0, 12), pady=4)
            ttk.Label(inner, text=label_builder(column)).grid(row=row_index, column=1, sticky="w", padx=(0, 12), pady=4)
            ttk.Entry(inner, textvariable=rename_var).grid(row=row_index, column=2, sticky="ew", pady=4)

        button_bar = ttk.Frame(container)
        button_bar.grid(row=2, column=0, sticky="e", pady=(10, 0))
        ttk.Button(button_bar, text="取消", command=self.destroy).pack(side=tk.RIGHT)
        ttk.Button(button_bar, text="保存", command=self.on_confirm).pack(side=tk.RIGHT, padx=(0, 8))

        self.bind("<Return>", lambda _: self.on_confirm())
        self.bind("<Escape>", lambda _: self.destroy())

    def show_all(self) -> None:
        for variable in self.visible_vars.values():
            variable.set(True)

    def hide_internal(self) -> None:
        for column, variable in self.visible_vars.items():
            if column in INTERNAL_COLUMNS:
                variable.set(False)

    def restore_defaults(self) -> None:
        for column in self.columns:
            self.visible_vars[column].set(default_visible(column))
            self.rename_vars[column].set(default_display_name(column))

    def on_confirm(self) -> None:
        self.result = {
            column: ColumnSetting(
                visible=self.visible_vars[column].get(),
                rename_to=self.rename_vars[column].get().strip(),
            )
            for column in self.columns
        }
        self.destroy()
