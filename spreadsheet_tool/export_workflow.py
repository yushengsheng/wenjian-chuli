from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import pandas as pd

from .models import SourceSelection


@dataclass(slots=True)
class WritebackApplication:
    updated_cache: dict[str, pd.DataFrame]
    target_source: SourceSelection
    status_text: str
    summary_lines: list[str]


@dataclass(slots=True)
class WorkbookExportPlan:
    extension: str
    initial_filename: str


def output_format_for_source(source: SourceSelection) -> str:
    return "csv" if source.path.suffix.lower() in {".csv", ".tsv"} else "xlsx"


def apply_writeback_result(
    data_cache: Mapping[str, pd.DataFrame],
    target_source: SourceSelection,
    processed_df: pd.DataFrame,
) -> WritebackApplication:
    updated_cache = dict(data_cache)
    updated_cache[target_source.source_id] = processed_df.copy()
    target_source.row_count = len(processed_df)
    target_source.columns = [str(column) for column in processed_df.columns]
    return WritebackApplication(
        updated_cache=updated_cache,
        target_source=target_source,
        status_text=f"写回完成: {target_source.path.name} / {target_source.sheet_name}",
        summary_lines=[
            f"已直接写回老数据文件: {target_source.path}",
            f"目标工作表: {target_source.sheet_name}",
            f"写回行数: {len(processed_df)}",
        ],
    )


def build_workbook_export_plan(source: SourceSelection) -> WorkbookExportPlan:
    extension = source.path.suffix.lower()
    return WorkbookExportPlan(
        extension=extension,
        initial_filename=f"{source.path.stem}_处理后{extension}",
    )


def build_csv_export_summary(save_path: str | Path) -> list[str]:
    return [f"文件已导出到: {save_path}"]


def build_workbook_export_summary(save_path: str | Path, target_source: SourceSelection) -> list[str]:
    return [
        f"已导出完整老文件: {save_path}",
        f"基于老文件模板: {target_source.path}",
        f"替换工作表: {target_source.sheet_name}",
    ]
