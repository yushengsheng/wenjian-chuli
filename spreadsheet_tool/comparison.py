from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .models import ColumnSetting, PipelineConfig
from .processor import (
    INTERNAL_COLUMNS,
    INTERNAL_SOURCE_ROLE,
    apply_column_settings,
    apply_duplicate_strategy,
    canonical_internal_column_name,
    default_display_name,
    make_unique_rename_map,
)


@dataclass(slots=True)
class ComparisonPreview:
    before_df: pd.DataFrame
    after_df: pd.DataFrame
    statuses: list[str]
    changed_columns: list[set[str]]


def preview_value(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    text = str(value)
    return text if len(text) <= 160 else f"{text[:157]}..."


def build_baseline_dataframe(
    raw_dataframe: pd.DataFrame,
    processed_columns: list[str] | None,
    config: PipelineConfig,
) -> pd.DataFrame:
    old_only = raw_dataframe[raw_dataframe[INTERNAL_SOURCE_ROLE] == "old"].copy()
    if old_only.empty:
        return pd.DataFrame(columns=processed_columns or [])
    if config.duplicate_keys:
        old_only = apply_duplicate_strategy(old_only, config.duplicate_keys, "keep_last")
    return apply_column_settings(old_only, config)


def align_for_comparison(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    config: PipelineConfig,
    column_settings: dict[str, ColumnSetting],
    ignored_columns: set[str] | None = None,
) -> ComparisonPreview:
    columns = list(after_df.columns)
    if not columns:
        return ComparisonPreview(before_df.copy(), after_df.copy(), [], [])

    key_columns = get_compare_key_columns(config, before_df, after_df, column_settings)
    if not key_columns:
        return align_by_index(before_df, after_df, columns, ignored_columns=ignored_columns)

    before_groups = dataframe_to_key_groups(before_df, key_columns)
    after_groups = dataframe_to_key_groups(after_df, key_columns)
    ordered_keys = list(before_groups.keys()) + [key for key in after_groups if key not in before_groups]

    before_rows: list[dict[str, object]] = []
    after_rows: list[dict[str, object]] = []
    statuses: list[str] = []
    changed_columns: list[set[str]] = []

    for key in ordered_keys:
        before_group = before_groups.get(key, [])
        after_group = after_groups.get(key, [])
        row_count = max(len(before_group), len(after_group))
        for index in range(row_count):
            before_row = before_group[index] if index < len(before_group) else empty_row(columns)
            after_row = after_group[index] if index < len(after_group) else empty_row(columns)
            status, changed = compare_row_values(before_row, after_row, columns, ignored_columns=ignored_columns)
            before_rows.append(before_row)
            after_rows.append(after_row)
            statuses.append(status)
            changed_columns.append(changed)

    return ComparisonPreview(
        before_df=pd.DataFrame(before_rows, columns=columns),
        after_df=pd.DataFrame(after_rows, columns=columns),
        statuses=statuses,
        changed_columns=changed_columns,
    )


def get_compare_key_columns(
    config: PipelineConfig,
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    column_settings: dict[str, ColumnSetting],
) -> list[str]:
    output_key_columns: list[str] = []
    for original_key in config.duplicate_keys:
        output_name = output_name_for_column(original_key, column_settings)
        if output_name in before_df.columns and output_name in after_df.columns:
            output_key_columns.append(output_name)
    return output_key_columns


def output_name_for_column(column_name: str, column_settings: dict[str, ColumnSetting]) -> str:
    setting = column_settings.get(column_name)
    rename_target = setting.rename_to.strip() if setting and setting.rename_to else ""
    return rename_target or default_display_name(column_name)


def dataframe_to_key_groups(
    dataframe: pd.DataFrame,
    key_columns: list[str],
) -> dict[tuple[str, ...], list[dict[str, object]]]:
    mapping: dict[tuple[str, ...], list[dict[str, object]]] = {}
    for _, row in dataframe.iterrows():
        key = tuple(preview_value(row.get(column)) for column in key_columns)
        mapping.setdefault(key, []).append({column: row.get(column, "") for column in dataframe.columns})
    return mapping


def align_by_index(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    columns: list[str],
    ignored_columns: set[str] | None = None,
) -> ComparisonPreview:
    max_len = max(len(before_df), len(after_df))
    before_rows: list[dict[str, object]] = []
    after_rows: list[dict[str, object]] = []
    statuses: list[str] = []
    changed_columns: list[set[str]] = []

    for index in range(max_len):
        before_row = before_df.iloc[index].to_dict() if index < len(before_df) else empty_row(columns)
        after_row = after_df.iloc[index].to_dict() if index < len(after_df) else empty_row(columns)
        status, changed = compare_row_values(before_row, after_row, columns, ignored_columns=ignored_columns)
        before_rows.append(before_row)
        after_rows.append(after_row)
        statuses.append(status)
        changed_columns.append(changed)

    return ComparisonPreview(
        before_df=pd.DataFrame(before_rows, columns=columns),
        after_df=pd.DataFrame(after_rows, columns=columns),
        statuses=statuses,
        changed_columns=changed_columns,
    )


def compare_row_values(
    before_row: dict[str, object],
    after_row: dict[str, object],
    columns: list[str],
    ignored_columns: set[str] | None = None,
) -> tuple[str, set[str]]:
    ignored_columns = ignored_columns or set()
    changed_columns: set[str] = set()
    before_non_empty = False
    after_non_empty = False
    for column in columns:
        if column in ignored_columns:
            continue
        before_text = preview_value(before_row.get(column))
        after_text = preview_value(after_row.get(column))
        if before_text:
            before_non_empty = True
        if after_text:
            after_non_empty = True
        if before_text != after_text:
            changed_columns.add(column)

    if not before_non_empty and after_non_empty:
        return "added", changed_columns
    if before_non_empty and not after_non_empty:
        return "removed", changed_columns
    if changed_columns:
        return "changed", changed_columns
    return "same", changed_columns


def get_ignored_compare_columns(source_columns: list[str], config: PipelineConfig) -> set[str]:
    rename_map: dict[str, str] = {}
    for column in source_columns:
        canonical_internal = canonical_internal_column_name(column)
        setting = config.column_settings.get(column, ColumnSetting())
        visible = setting.visible
        if canonical_internal is not None and not config.include_source_columns:
            visible = False
        if not visible:
            continue
        rename_map[column] = setting.rename_to.strip() or default_display_name(column)

    unique_map = make_unique_rename_map(rename_map)
    return {
        unique_map[column]
        for column in source_columns
        if canonical_internal_column_name(column) is not None and column in unique_map
    }


def empty_row(columns: list[str]) -> dict[str, str]:
    return {column: "" for column in columns}
