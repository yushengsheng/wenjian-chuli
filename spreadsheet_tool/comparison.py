from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
import re

import pandas as pd

from .models import ColumnSetting, PipelineConfig
from .processor import (
    INTERNAL_COLUMNS,
    INTERNAL_SOURCE_ROLE,
    apply_column_settings,
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
    if not text.strip():
        return ""
    text = re.sub(r"[\r\n]+", " ", text)
    text = text.strip()
    return text if len(text) <= 160 else f"{text[:157]}..."


def build_baseline_dataframe(
    raw_dataframe: pd.DataFrame,
    processed_columns: list[str] | None,
    config: PipelineConfig,
) -> pd.DataFrame:
    old_only = build_baseline_source_dataframe(raw_dataframe, config)
    if old_only.empty:
        return pd.DataFrame(columns=processed_columns or [])
    return apply_column_settings(old_only, config)


def build_baseline_source_dataframe(
    raw_dataframe: pd.DataFrame,
    config: PipelineConfig,
) -> pd.DataFrame:
    old_only = raw_dataframe[raw_dataframe[INTERNAL_SOURCE_ROLE] == "old"].copy()
    if old_only.empty:
        return pd.DataFrame()
    return old_only.loc[:, [column for column in old_only.columns if column not in INTERNAL_COLUMNS]].reset_index(drop=True)


def align_for_comparison(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    config: PipelineConfig,
    column_settings: dict[str, ColumnSetting],
    ignored_columns: set[str] | None = None,
    before_key_df: pd.DataFrame | None = None,
    after_key_df: pd.DataFrame | None = None,
    key_columns: list[str] | None = None,
) -> ComparisonPreview:
    columns = list(after_df.columns) or list(before_df.columns)
    if not columns:
        return ComparisonPreview(before_df.copy(), after_df.copy(), [], [])

    key_before = before_key_df.reset_index(drop=True) if before_key_df is not None else before_df.reset_index(drop=True)
    key_after = after_key_df.reset_index(drop=True) if after_key_df is not None else after_df.reset_index(drop=True)
    compare_key_columns = key_columns or get_compare_key_columns(
        config,
        key_before,
        key_after,
        column_settings,
        use_output_names=before_key_df is None and after_key_df is None,
    )
    if not compare_key_columns:
        return align_by_index(before_df, after_df, columns, ignored_columns=ignored_columns)

    row_pairs = build_comparison_row_pairs(
        before_df,
        after_df,
        key_before,
        key_after,
        compare_key_columns,
        ignored_columns=ignored_columns,
    )

    before_rows: list[dict[str, object]] = []
    after_rows: list[dict[str, object]] = []
    statuses: list[str] = []
    changed_columns: list[set[str]] = []

    for before_index, after_index in row_pairs:
        before_row = before_df.iloc[before_index].to_dict() if before_index is not None and before_index < len(before_df) else empty_row(columns)
        after_row = after_df.iloc[after_index].to_dict() if after_index is not None and after_index < len(after_df) else empty_row(columns)
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
    use_output_names: bool = True,
) -> list[str]:
    output_key_columns: list[str] = []
    for original_key in config.duplicate_keys:
        candidate_name = output_name_for_column(original_key, column_settings) if use_output_names else original_key
        if candidate_name in before_df.columns and candidate_name in after_df.columns:
            output_key_columns.append(candidate_name)
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


def build_comparison_row_pairs(
    before_display_df: pd.DataFrame,
    after_display_df: pd.DataFrame,
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    key_columns: list[str],
    ignored_columns: set[str] | None = None,
) -> list[tuple[int | None, int | None]]:
    ignored_columns = ignored_columns or set()
    unmatched_before = set(range(len(before_df)))
    matched_after_to_before: dict[int, int] = {}
    key_indexes = build_comparison_key_indexes(before_df, key_columns)

    for after_index, (_, after_row) in enumerate(after_df.iterrows()):
        candidate_before = choose_best_matching_comparison_row(
            before_df,
            after_row,
            key_columns,
            unmatched_before,
            key_indexes,
        )
        if candidate_before is None:
            continue
        unmatched_before.remove(candidate_before)
        matched_after_to_before[after_index] = candidate_before

    unmatched_before_list = [index for index in range(len(before_df)) if index in unmatched_before]
    unmatched_after_list = [index for index in range(len(after_df)) if index not in matched_after_to_before]
    fallback_pairs = align_unmatched_rows_by_content(
        before_display_df,
        after_display_df,
        unmatched_before_list,
        unmatched_after_list,
        ignored_columns=ignored_columns,
    )

    before_to_after = {before_index: after_index for after_index, before_index in matched_after_to_before.items()}
    for before_index, after_index in fallback_pairs:
        if before_index is not None:
            before_to_after[before_index] = after_index

    pairs: list[tuple[int | None, int | None]] = []
    consumed_before: set[int] = set()
    for after_index in range(len(after_df)):
        matched_before = matched_after_to_before.get(after_index)
        if matched_before is None:
            matched_before = next(
                (before_index for before_index, candidate_after in fallback_pairs if candidate_after == after_index),
                None,
            )
        if matched_before is not None:
            consumed_before.add(matched_before)
        pairs.append((matched_before, after_index))

    for before_index in range(len(before_df)):
        if before_index not in consumed_before:
            pairs.append((before_index, None))

    return pairs


def choose_best_matching_comparison_row(
    before_df: pd.DataFrame,
    after_row: pd.Series,
    key_columns: list[str],
    unmatched_before: set[int],
    key_indexes: dict[str, dict[str, list[int]]],
) -> int | None:
    candidate_indexes: set[int] = set()
    for key in key_columns:
        after_value = preview_value(after_row.get(key))
        if not after_value:
            continue
        candidate_indexes.update(key_indexes.get(key, {}).get(after_value, []))
    if not candidate_indexes:
        return None

    best_index: int | None = None
    best_score = -1
    for before_index in candidate_indexes:
        if before_index not in unmatched_before:
            continue
        before_row = before_df.iloc[before_index]
        score = count_matching_compare_keys(before_row, after_row, key_columns)
        if score <= 0:
            continue
        if score > best_score or best_index is None or before_index < best_index:
            best_index = before_index
            best_score = score
    return best_index


def count_matching_compare_keys(before_row: pd.Series, after_row: pd.Series, key_columns: list[str]) -> int:
    score = 0
    for key in key_columns:
        before_value = preview_value(before_row.get(key))
        after_value = preview_value(after_row.get(key))
        if before_value and after_value and before_value == after_value:
            score += 1
    return score


def build_comparison_key_indexes(
    dataframe: pd.DataFrame,
    key_columns: list[str],
) -> dict[str, dict[str, list[int]]]:
    indexes: dict[str, dict[str, list[int]]] = {key: {} for key in key_columns}
    for index, (_, row) in enumerate(dataframe.iterrows()):
        for key in key_columns:
            value = preview_value(row.get(key))
            if not value:
                continue
            indexes[key].setdefault(value, []).append(index)
    return indexes


def align_unmatched_rows_by_content(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    before_indexes: list[int],
    after_indexes: list[int],
    ignored_columns: set[str] | None = None,
) -> list[tuple[int | None, int | None]]:
    ignored_columns = ignored_columns or set()
    before_by_signature: dict[tuple[str, ...], deque[int]] = defaultdict(deque)
    matched_before: set[int] = set()
    pairs: list[tuple[int | None, int | None]] = []

    for before_index in before_indexes:
        signature = row_signature(before_df.iloc[before_index].to_dict(), ignored_columns)
        before_by_signature[signature].append(before_index)

    for after_index in after_indexes:
        signature = row_signature(after_df.iloc[after_index].to_dict(), ignored_columns)
        if before_by_signature[signature]:
            matched_index = before_by_signature[signature].popleft()
            matched_before.add(matched_index)
            pairs.append((matched_index, after_index))
        else:
            pairs.append((None, after_index))

    for before_index in before_indexes:
        if before_index not in matched_before:
            pairs.append((before_index, None))
    return pairs


def row_signature(row: dict[str, object], ignored_columns: set[str]) -> tuple[str, ...]:
    return tuple(preview_value(value) for column, value in row.items() if column not in ignored_columns)


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
