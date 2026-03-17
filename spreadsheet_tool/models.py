from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class SourceSelection:
    source_id: str
    path: Path
    sheet_name: str
    dataset_role: str = "new"
    enabled: bool = True
    row_count: int = 0
    columns: list[str] = field(default_factory=list)
    source_column_mapping: dict[str, str] = field(default_factory=dict)
    mapping_confirmed: bool = False


@dataclass(slots=True)
class FilterRule:
    column: str
    operator: str
    value: str = ""


@dataclass(slots=True)
class UpdateRule:
    mode: str
    column: str
    find_value: str = ""
    replace_value: str = ""


@dataclass(slots=True)
class ColumnSetting:
    visible: bool = True
    rename_to: str = ""


@dataclass(slots=True)
class ExportSettings:
    output_format: str = "xlsx"
    sheet_name: str = "处理结果"
    freeze_header: bool = True
    auto_width: bool = True
    style_header: bool = True


@dataclass(slots=True)
class PipelineConfig:
    duplicate_keys: list[str] = field(default_factory=list)
    duplicate_strategy: str = "update_and_append"
    filter_rules: list[FilterRule] = field(default_factory=list)
    update_rules: list[UpdateRule] = field(default_factory=list)
    column_settings: dict[str, ColumnSetting] = field(default_factory=dict)
    include_source_columns: bool = True


@dataclass(slots=True)
class ProcessResult:
    dataframe: object
    writeback_dataframe: object | None = None
    summary_lines: list[str] = field(default_factory=list)
