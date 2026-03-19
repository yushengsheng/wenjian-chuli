from __future__ import annotations

from collections import Counter

import pandas as pd

from .comparison import preview_value

COLUMN_WIDTH_SAMPLE_LIMIT = 200
PREVIEW_ROW_NUMBER_COLUMN = "__preview_row_number__"
PREVIEW_ROW_NUMBER_LABEL = "行号"


def build_comparison_info(
    before_total_rows: int,
    after_total_rows: int,
    displayed_rows: int,
    changes_only: bool = False,
) -> tuple[str, str]:
    mode_text = "仅变动" if changes_only else "全部"
    return (
        f"修改前 {before_total_rows} 行 | 当前显示 {displayed_rows} 行（{mode_text}）",
        f"修改后 {after_total_rows} 行 | 当前显示 {displayed_rows} 行（{mode_text}）",
    )


def compute_compare_column_widths(before_df: pd.DataFrame, after_df: pd.DataFrame) -> dict[str, int]:
    columns = [str(column) for column in after_df.columns]
    widths: dict[str, int] = {
        PREVIEW_ROW_NUMBER_COLUMN: max(len(PREVIEW_ROW_NUMBER_LABEL), len(str(max(len(before_df), len(after_df), 1))))
    }
    for column in columns:
        max_len = len(column)
        for dataframe in (before_df, after_df):
            if column not in dataframe.columns:
                continue
            for value in dataframe[column].head(COLUMN_WIDTH_SAMPLE_LIMIT).tolist():
                max_len = max(max_len, len(preview_value(value)))
        widths[column] = min(max(max_len + 2, 10), 28)
    return widths


def build_compare_display_columns(columns: list[str]) -> list[str]:
    return [PREVIEW_ROW_NUMBER_COLUMN, *columns]


def display_compare_column_name(column: str) -> str:
    if column == PREVIEW_ROW_NUMBER_COLUMN:
        return PREVIEW_ROW_NUMBER_LABEL
    return column


def filter_comparison_rows(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    statuses: list[str],
    changed_columns: list[set[str]],
    changes_only: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str], list[set[str]]]:
    if not changes_only:
        return before_df, after_df, statuses, changed_columns

    visible_indexes = [index for index, status in enumerate(statuses) if status != "same"]
    filtered_before = before_df.iloc[visible_indexes].reset_index(drop=True)
    filtered_after = after_df.iloc[visible_indexes].reset_index(drop=True)
    filtered_statuses = [statuses[index] for index in visible_indexes]
    filtered_changed_columns = [changed_columns[index] for index in visible_indexes]
    return filtered_before, filtered_after, filtered_statuses, filtered_changed_columns


def marker_for_cell(status: str, side: str, column: str, changed_columns: set[str]) -> tuple[str, str | None]:
    if column not in changed_columns:
        return "", None
    if status == "added" and side == "after":
        return "+", "plus"
    if status == "removed" and side == "before":
        return "-", "minus"
    if status == "changed":
        return ("-", "minus") if side == "before" else ("+", "plus")
    return "", None


def fit_compare_text(text: str, width: int) -> str:
    if len(text) > width:
        return text[: max(width - 1, 1)] + "…"
    return text.ljust(width)


def summarize_comparison_statuses(statuses: list[str]) -> list[str]:
    if not statuses:
        return []
    counts = Counter(statuses)
    return [
        f"比较摘要: 新增 {counts.get('added', 0)} 行",
        f"比较摘要: 变更 {counts.get('changed', 0)} 行",
        f"比较摘要: 未变化 {counts.get('same', 0)} 行",
        f"比较摘要: 移除 {counts.get('removed', 0)} 行",
    ]
