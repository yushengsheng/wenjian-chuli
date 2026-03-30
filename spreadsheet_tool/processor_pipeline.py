from __future__ import annotations

from typing import Iterable

import pandas as pd

from .models import ColumnSetting, FilterRule, PipelineConfig, ProcessResult, SourceSelection, UpdateRule
from .processor_common import (
    DUPLICATE_STRATEGIES,
    EMPTY_TEXT_VALUES,
    INTERNAL_APPEND_ORDER,
    INTERNAL_COLUMNS,
    INTERNAL_SOURCE_FILE,
    INTERNAL_SOURCE_ROLE,
    INTERNAL_SOURCE_SHEET,
    UNMAPPED_TARGET,
    default_display_name,
    is_empty_series,
    is_empty_value,
    is_unmapped_target,
    normalize_compare_value,
    normalize_duplicate_strategy,
    normalize_key_value,
    values_differ,
)
from .processor_mapping import align_dataframe_to_target, collect_target_columns


def combine_enabled_sources(
    sources: dict[str, SourceSelection],
    cache: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    append_order = 1
    target_columns = collect_target_columns(sources)

    enabled_sources = [source for source in sources.values() if source.enabled]
    ordered_sources = [source for source in enabled_sources if source.dataset_role == "old"]
    ordered_sources.extend(source for source in enabled_sources if source.dataset_role != "old")

    for source in ordered_sources:
        frame = cache[source.source_id].copy()
        if target_columns:
            if source.dataset_role == "new":
                frame = align_dataframe_to_target(
                    frame,
                    target_columns,
                    manual_source_mapping=source.source_column_mapping,
                )
            else:
                frame = frame.reindex(columns=target_columns)
        frame[INTERNAL_SOURCE_FILE] = source.path.name
        frame[INTERNAL_SOURCE_SHEET] = source.sheet_name
        frame[INTERNAL_SOURCE_ROLE] = source.dataset_role
        frame[INTERNAL_APPEND_ORDER] = range(append_order, append_order + len(frame))
        append_order += len(frame)
        frames.append(frame)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True, sort=False)


def process_dataframe(dataframe: pd.DataFrame, config: PipelineConfig) -> ProcessResult:
    working = dataframe.copy()
    summary_lines = [f"初始记录数: {len(working)}"]

    if working.empty:
        writeback_output = build_writeback_dataframe(working)
        output = apply_column_settings(working, config)
        summary_lines.append("没有可处理的数据。")
        return ProcessResult(dataframe=output, writeback_dataframe=writeback_output, summary_lines=summary_lines)

    if config.duplicate_keys and config.duplicate_strategy != "none":
        before = len(working)
        working = apply_duplicate_strategy(working, config.duplicate_keys, config.duplicate_strategy)
        summary_lines.append(f"主键合并后: {len(working)} 行，减少 {before - len(working)} 行")
    else:
        summary_lines.append("主键合并: 未启用")

    working = cleanup_unmapped_targets(working)

    if config.update_rules:
        working = apply_update_rules(working, config.update_rules)
        summary_lines.append(f"更新规则数: {len(config.update_rules)}")
    else:
        summary_lines.append("更新规则数: 0")

    if config.filter_rules:
        before = len(working)
        working = apply_filter_rules(working, config.filter_rules)
        summary_lines.append(f"筛选后: {len(working)} 行，过滤 {before - len(working)} 行")
    else:
        summary_lines.append("筛选规则数: 0")

    writeback_output = build_writeback_dataframe(working)
    working = apply_column_settings(working, config)
    summary_lines.append(f"输出列数: {len(working.columns)}")
    return ProcessResult(dataframe=working, writeback_dataframe=writeback_output, summary_lines=summary_lines)


def apply_duplicate_strategy(
    dataframe: pd.DataFrame,
    keys: list[str],
    strategy: str,
) -> pd.DataFrame:
    strategy = normalize_duplicate_strategy(strategy)
    valid_keys = [key for key in keys if key in dataframe.columns]
    if not valid_keys:
        return dataframe.reset_index(drop=True)

    has_key_mask = pd.Series(False, index=dataframe.index)
    for key in valid_keys:
        has_key_mask = has_key_mask | ~is_empty_series(dataframe[key])

    keyed_rows = dataframe[has_key_mask].copy()
    blank_rows = filter_blank_key_rows_by_strategy(dataframe[~has_key_mask].copy(), strategy)
    if keyed_rows.empty:
        return sort_duplicate_strategy_result(blank_rows)

    keyed_rows = keyed_rows.sort_values(INTERNAL_APPEND_ORDER, kind="mergesort")
    helper_columns = attach_normalized_key_columns(keyed_rows, valid_keys)

    if strategy == "keep_first":
        deduped = keyed_rows.drop_duplicates(subset=helper_columns, keep="first")
    elif strategy == "keep_last":
        deduped = keyed_rows.drop_duplicates(subset=helper_columns, keep="last")
    elif strategy in {"update_and_append", "update_only", "fill_old_empty"}:
        deduped = merge_rows_by_selected_key_match(keyed_rows, valid_keys, strategy)
    else:
        deduped = keyed_rows

    deduped = deduped.drop(columns=helper_columns, errors="ignore")
    merged = pd.concat([deduped, blank_rows], ignore_index=True, sort=False)
    return sort_duplicate_strategy_result(merged)


def filter_blank_key_rows_by_strategy(dataframe: pd.DataFrame, strategy: str) -> pd.DataFrame:
    strategy = normalize_duplicate_strategy(strategy)
    if dataframe.empty or strategy not in {"update_and_append", "update_only", "fill_old_empty"}:
        return dataframe
    if INTERNAL_SOURCE_ROLE not in dataframe.columns:
        return dataframe
    return dataframe[dataframe[INTERNAL_SOURCE_ROLE] == "old"].copy()


def sort_duplicate_strategy_result(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty or INTERNAL_APPEND_ORDER not in dataframe.columns:
        return dataframe.reset_index(drop=True)
    return dataframe.sort_values(INTERNAL_APPEND_ORDER, kind="mergesort").reset_index(drop=True)


def merge_rows_by_selected_key_match(
    dataframe: pd.DataFrame,
    key_columns: list[str],
    strategy: str,
) -> pd.DataFrame:
    strategy = normalize_duplicate_strategy(strategy)
    output_columns = list(dataframe.columns)
    old_rows_df = dataframe[dataframe[INTERNAL_SOURCE_ROLE] == "old"].sort_values(INTERNAL_APPEND_ORDER, kind="mergesort")
    new_rows_df = dataframe[dataframe[INTERNAL_SOURCE_ROLE] == "new"].sort_values(INTERNAL_APPEND_ORDER, kind="mergesort")

    old_rows = _dataframe_rows_to_dicts(old_rows_df, output_columns)
    old_row_keys = [build_normalized_row_keys(row, key_columns) for row in old_rows]
    key_indexes = build_old_row_key_indexes(old_row_keys, key_columns)
    unmatched_new_rows: list[dict[str, object]] = []

    for new_row in _dataframe_rows_to_dicts(new_rows_df, output_columns):
        new_row_keys = build_normalized_row_keys(new_row, key_columns)
        match_position = choose_best_old_row_match(old_row_keys, new_row_keys, key_columns, key_indexes)
        if match_position is None:
            if strategy != "update_only":
                unmatched_new_rows.append(new_row)
            continue

        old_row = old_rows[match_position]
        old_append_order = old_row.get(INTERNAL_APPEND_ORDER)
        merged_row = overlay_new_row(old_row, new_row, key_columns, strategy)
        merged_row[INTERNAL_APPEND_ORDER] = old_append_order
        merged_row_keys = build_normalized_row_keys(merged_row, key_columns)
        old_rows[match_position] = merged_row
        refresh_old_row_key_indexes(key_indexes, key_columns, old_row_keys[match_position], merged_row_keys, match_position)
        old_row_keys[match_position] = merged_row_keys

    rows = [*old_rows, *unmatched_new_rows]
    if not rows:
        return pd.DataFrame(columns=output_columns)
    result = pd.DataFrame(rows, columns=output_columns)
    return result.sort_values(INTERNAL_APPEND_ORDER, kind="mergesort").reset_index(drop=True)


def _dataframe_rows_to_dicts(dataframe: pd.DataFrame, output_columns: list[str]) -> list[dict[str, object]]:
    return [dict(zip(output_columns, values)) for values in dataframe.loc[:, output_columns].itertuples(index=False, name=None)]


def build_normalized_row_keys(row: dict[str, object], key_columns: list[str]) -> dict[str, str]:
    return {key: normalize_key_value(row.get(key)) for key in key_columns}


def build_old_row_key_indexes(
    old_row_keys: list[dict[str, str]],
    key_columns: list[str],
) -> dict[str, dict[str, list[int]]]:
    indexes: dict[str, dict[str, list[int]]] = {key: {} for key in key_columns}
    for position, row_keys in enumerate(old_row_keys):
        for key in key_columns:
            normalized = row_keys.get(key, "")
            if not normalized:
                continue
            indexes[key].setdefault(normalized, []).append(position)
    return indexes


def choose_best_old_row_match(
    old_row_keys: list[dict[str, str]],
    new_row_keys: dict[str, str],
    key_columns: list[str],
    key_indexes: dict[str, dict[str, list[int]]],
) -> int | None:
    candidate_positions: set[int] = set()
    for key in key_columns:
        normalized = new_row_keys.get(key, "")
        if not normalized:
            continue
        candidate_positions.update(key_indexes.get(key, {}).get(normalized, []))

    best_position: int | None = None
    best_score = -1
    for position in candidate_positions:
        score = count_matching_row_keys(old_row_keys[position], new_row_keys, key_columns)
        if score <= 0:
            continue
        if score > best_score or best_position is None or position < best_position:
            best_position = position
            best_score = score
    return best_position


def count_matching_row_keys(
    left_row: dict[str, object],
    right_row: dict[str, object],
    key_columns: list[str],
) -> int:
    score = 0
    for key in key_columns:
        left_value = _key_value_from_row(left_row, key)
        right_value = _key_value_from_row(right_row, key)
        if left_value and right_value and left_value == right_value:
            score += 1
    return score


def _key_value_from_row(row: dict[str, object], key: str) -> str:
    value = row.get(key, "")
    return value if isinstance(value, str) else normalize_key_value(value)


def compare_order_value(left: object, right: object) -> int:
    if right is None:
        return -1
    if left is None:
        return 1
    return -1 if left < right else (1 if left > right else 0)


def refresh_old_row_key_indexes(
    key_indexes: dict[str, dict[str, list[int]]],
    key_columns: list[str],
    old_row_keys: dict[str, str],
    new_row_keys: dict[str, str],
    position: int,
) -> None:
    for key in key_columns:
        old_value = old_row_keys.get(key, "")
        new_value = new_row_keys.get(key, "")
        if old_value == new_value:
            continue
        if old_value:
            remove_old_row_key_index(key_indexes, key, old_value, position)
        if new_value:
            key_indexes.setdefault(key, {}).setdefault(new_value, []).append(position)


def remove_old_row_key_index(
    key_indexes: dict[str, dict[str, list[int]]],
    key: str,
    value: str,
    position: int,
) -> None:
    positions = key_indexes.get(key, {}).get(value)
    if not positions:
        return
    key_indexes[key][value] = [existing for existing in positions if existing != position]
    if not key_indexes[key][value]:
        key_indexes[key].pop(value, None)


def attach_normalized_key_columns(dataframe: pd.DataFrame, keys: list[str]) -> list[str]:
    helper_columns: list[str] = []
    for index, key in enumerate(keys):
        helper_column = f"__group_key_{index}"
        dataframe[helper_column] = dataframe[key].map(normalize_key_value)
        helper_columns.append(helper_column)
    return helper_columns


def overlay_new_row(
    old_row: dict[str, object],
    new_row: dict[str, object],
    key_columns: list[str],
    strategy: str,
) -> dict[str, object]:
    strategy = normalize_duplicate_strategy(strategy)
    merged_row = dict(old_row)
    for column, new_value in new_row.items():
        if column == INTERNAL_APPEND_ORDER:
            merged_row[column] = max_value(merged_row.get(column), new_value)
            continue
        if column in {INTERNAL_SOURCE_FILE, INTERNAL_SOURCE_SHEET, INTERNAL_SOURCE_ROLE}:
            merged_row[column] = join_distinct_values([merged_row.get(column), new_value])
            continue
        if is_unmapped_target(new_value):
            continue
        if column in key_columns:
            if not is_empty_value(new_value):
                merged_row[column] = normalize_key_value(new_value)
            continue

        if strategy in {"update_and_append", "update_only"}:
            if not is_empty_value(new_value) and values_differ(merged_row.get(column), new_value):
                merged_row[column] = new_value
        elif strategy == "fill_old_empty":
            if is_empty_value(merged_row.get(column)) and not is_empty_value(new_value):
                merged_row[column] = new_value

    return merged_row


def max_value(left: object, right: object) -> object:
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right)


def best_key_display_value(series: pd.Series) -> str | object:
    for value in reversed(series.tolist()):
        normalized = normalize_key_value(value)
        if normalized != "":
            return normalized
    return last_non_empty(series)


def join_distinct_non_empty(series: pd.Series) -> str:
    return join_distinct_values(series.tolist())


def join_distinct_values(values: Iterable[object]) -> str:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        if is_empty_value(value):
            continue
        text = str(value).strip()
        if text in seen:
            continue
        seen.add(text)
        output.append(text)
    return " | ".join(output)


def last_non_empty(series: pd.Series) -> object:
    for value in reversed(series.tolist()):
        if not is_empty_value(value):
            return value
    return series.iloc[-1] if not series.empty else None


def apply_filter_rules(dataframe: pd.DataFrame, rules: list[FilterRule]) -> pd.DataFrame:
    filtered = dataframe.copy()
    for rule in rules:
        if rule.column not in filtered.columns:
            continue
        mask = build_filter_mask(filtered[rule.column], rule.operator, rule.value)
        filtered = filtered[mask].copy()
    return filtered.reset_index(drop=True)


def build_filter_mask(series: pd.Series, operator: str, value: str) -> pd.Series:
    text_series = series.fillna("").astype(str)

    if operator == "equals":
        return text_series == value
    if operator == "not_equals":
        return text_series != value
    if operator == "contains":
        return text_series.str.contains(value, na=False, regex=False)
    if operator == "not_contains":
        return ~text_series.str.contains(value, na=False, regex=False)
    if operator in {"greater_than", "greater_equal", "less_than", "less_equal"}:
        numeric_series = pd.to_numeric(series, errors="coerce")
        numeric_value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.isna(numeric_value):
            raise ValueError(f"筛选值不是有效数字: {value}")
        if operator == "greater_than":
            return numeric_series > numeric_value
        if operator == "greater_equal":
            return numeric_series >= numeric_value
        if operator == "less_than":
            return numeric_series < numeric_value
        return numeric_series <= numeric_value
    if operator == "is_empty":
        return is_empty_series(series)
    if operator == "not_empty":
        return ~is_empty_series(series)

    raise ValueError(f"未知筛选条件: {operator}")


def apply_update_rules(dataframe: pd.DataFrame, rules: list[UpdateRule]) -> pd.DataFrame:
    updated = dataframe.copy()
    for rule in rules:
        if rule.column not in updated.columns:
            continue

        if rule.mode == "set_value":
            updated.loc[:, rule.column] = rule.replace_value
            continue

        if rule.mode == "fill_empty":
            empty_mask = is_empty_series(updated[rule.column])
            updated.loc[empty_mask, rule.column] = rule.replace_value
            continue

        if rule.mode == "replace_text":
            if not rule.find_value:
                continue
            updated.loc[:, rule.column] = updated[rule.column].map(
                lambda value: replace_text_value(value, rule.find_value, rule.replace_value)
            )
            continue

        if rule.mode == "replace_exact":
            text_series = updated[rule.column].fillna("").astype(str)
            updated.loc[text_series == rule.find_value, rule.column] = rule.replace_value

    return updated


def replace_text_value(value: object, source: str, target: str) -> object:
    if is_empty_value(value):
        return value
    return str(value).replace(source, target)


def apply_column_settings(dataframe: pd.DataFrame, config: PipelineConfig) -> pd.DataFrame:
    selected_columns: list[str] = []
    rename_map: dict[str, str] = {}

    for column in dataframe.columns:
        setting = config.column_settings.get(column, ColumnSetting())
        visible = setting.visible
        if column in INTERNAL_COLUMNS and not config.include_source_columns:
            visible = False
        if not visible:
            continue
        selected_columns.append(column)
        rename_map[column] = setting.rename_to.strip() or default_display_name(column)

    output = dataframe.loc[:, selected_columns].copy()
    output = output.rename(columns=make_unique_rename_map(rename_map))
    return output.reset_index(drop=True)


def make_unique_rename_map(rename_map: dict[str, str]) -> dict[str, str]:
    seen: dict[str, int] = {}
    unique_map: dict[str, str] = {}
    for original, target in rename_map.items():
        count = seen.get(target, 0)
        if count == 0:
            unique_map[original] = target
        else:
            unique_map[original] = f"{target}_{count + 1}"
        seen[target] = count + 1
    return unique_map


def cleanup_unmapped_targets(dataframe: pd.DataFrame) -> pd.DataFrame:
    cleaned = dataframe.copy()
    for column in cleaned.columns:
        mask = cleaned[column].map(is_unmapped_target)
        if mask.any():
            cleaned.loc[mask, column] = pd.NA
    return cleaned


def build_writeback_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    writeback_columns = [column for column in dataframe.columns if column not in INTERNAL_COLUMNS]
    return dataframe.loc[:, writeback_columns].copy().reset_index(drop=True)


def prepare_dataframe_for_excel(dataframe: pd.DataFrame) -> pd.DataFrame:
    excel_ready = dataframe.copy().astype(object)
    return excel_ready.where(~excel_ready.isna(), None)


def cell_display_length(value: object) -> int:
    if is_empty_value(value):
        return 0
    return len(str(value))


__all__ = [
    "DUPLICATE_STRATEGIES",
    "apply_column_settings",
    "apply_duplicate_strategy",
    "apply_filter_rules",
    "apply_update_rules",
    "attach_normalized_key_columns",
    "best_key_display_value",
    "build_filter_mask",
    "build_old_row_key_indexes",
    "build_writeback_dataframe",
    "cell_display_length",
    "cleanup_unmapped_targets",
    "combine_enabled_sources",
    "compare_order_value",
    "count_matching_row_keys",
    "join_distinct_non_empty",
    "join_distinct_values",
    "last_non_empty",
    "make_unique_rename_map",
    "max_value",
    "merge_rows_by_selected_key_match",
    "normalize_key_value",
    "overlay_new_row",
    "prepare_dataframe_for_excel",
    "process_dataframe",
    "refresh_old_row_key_indexes",
    "remove_old_row_key_index",
    "replace_text_value",
    "sort_duplicate_strategy_result",
]
