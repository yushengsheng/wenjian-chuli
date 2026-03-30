from __future__ import annotations

import tkinter as tk
from tkinter import filedialog, messagebox

from .compare_render import summarize_comparison_statuses
from .export_workflow import (
    apply_writeback_result,
    build_csv_export_summary,
    build_workbook_export_plan,
    build_workbook_export_summary,
    output_format_for_source,
)
from .models import ExportSettings, PipelineConfig, SourceSelection
from .processor_io import export_dataframe, export_dataframe_with_old_workbook, paths_refer_to_same_file, write_dataframe_back_to_source
from .processor_mapping import collect_target_columns
from .processor_pipeline import process_dataframe
from .source_ops import (
    MISSING_OLD,
    MULTIPLE_OLD,
    get_last_processed_scope_sources as collect_last_processed_scope_sources,
    resolve_writeback_target_source as choose_writeback_target_source,
)
from .workflow import (
    MISSING_OLD as PROCESSING_MISSING_OLD,
    NO_DATA as PROCESSING_NO_DATA,
    prepare_processing,
)


class UiProcessingMixin:
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
        from . import ui as ui_module

        context_sources = self.get_mapping_scope_sources(sources_to_map)
        session = ui_module.build_mapping_session(context_sources, self.data_cache, sources_to_map)
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

            dialog = ui_module.SourceMappingDialog(
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
