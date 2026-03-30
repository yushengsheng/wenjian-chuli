from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import tkinter as tk
from tkinter import messagebox, ttk

import pandas as pd

from .background_worker import BackgroundTaskResult
from .compare_render import PREVIEW_ROW_NUMBER_COLUMN


@dataclass(slots=True)
class PendingUiTask:
    task_name: str
    success_handler: object
    error_handler: object


class UiRuntimeMixin:
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
        if result.task_name != pending.task_name:
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
        for after_id in list(self.tree_preview_after_ids.values()):
            try:
                self.after_cancel(after_id)
            except tk.TclError:
                pass
        self.tree_preview_after_ids.clear()
        self.hide_compare_tooltip()
        self._hide_widget_tooltip()
        self.background_worker.shutdown(wait=True)
        super().destroy()

    def destroy(self) -> None:
        if not self.shutdown_started:
            self._shutdown_and_destroy()
            return
        super().destroy()

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
        for tree in self.source_trees.values():
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
        for label in self.source_summary_labels.values():
            label.configure(wraplength=max(160, int(source_width - 30)))
        for area in self.drop_areas.values():
            area.configure(
                font=self.font_small,
                wraplength=max(160, int(source_width - 40)),
                padx=max(8, round(10 * (self.font_small.cget("size") / 9))),
                pady=max(6, round(8 * (self.font_small.cget("size") / 9))),
            )
        if hasattr(self, "compare_info_label"):
            self.compare_info_label.configure(wraplength=max(360, int(self.winfo_width() * 0.45)))

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
            tab_id = notebook.tk.call(
                notebook._w,
                "identify",
                "tab",
                int(getattr(event, "x", 0)),
                int(getattr(event, "y", 0)),
            )
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
            3: "设置导出格式；若是直接写回老文件或导出完整老文件，会保留目标工作表原有结构。",
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
        listbox.bind(
            "<Motion>",
            lambda event, active_listbox=listbox, active_resolver=resolver: self.on_listbox_hover(
                active_listbox,
                event,
                active_resolver,
            ),
            add="+",
        )
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
