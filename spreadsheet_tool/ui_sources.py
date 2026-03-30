from __future__ import annotations

from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from .models import ColumnSetting, SourceSelection
from .processor_common import default_display_name, default_visible
from .processor_ingest import load_sources_from_paths
from .processor_mapping import collect_available_columns, collect_target_columns
from .source_ops import (
    PAIR_INCOMPLETE,
    expand_input_paths as collect_input_paths,
    get_column_scope_sources as collect_column_scope_sources,
    get_mapping_scope_sources as collect_mapping_scope_sources,
    resolve_processing_scope_sources,
)
from .workflow import apply_imported_sources

try:
    from tkinterdnd2 import DND_FILES
except ImportError:
    DND_FILES = None


class UiSourcesMixin:
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
            text="移除选中项",
            style="Danger.TButton",
            command=lambda r=role: self.remove_selected_files(r),
            width=11,
        )
        remove_role_btn.pack(side=tk.LEFT, padx=(8, 0))
        self.register_busy_ttk_widget(remove_role_btn)
        self.register_tooltip(import_btn, f"导入{self.ROLE_TITLES[role]}中的表格文件。")
        self.register_tooltip(
            remove_role_btn,
            f"删除当前选中的{self.ROLE_TITLES[role]}项：选中文件会删除整份文件，选中单个 sheet 只删除该 sheet。",
        )

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
            f"确定要移除选中的{self.ROLE_TITLES[role]}项吗？",
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
