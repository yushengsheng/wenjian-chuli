from __future__ import annotations

from collections import Counter

import pandas as pd

from .comparison import preview_value

PREVIEW_LIMIT = 200


def build_comparison_info(before_df: pd.DataFrame, after_df: pd.DataFrame) -> tuple[str, str]:
    return (
        f"修改前 {len(before_df)} 行 | 预览前 {PREVIEW_LIMIT} 行",
        f"修改后 {len(after_df)} 行 | 预览前 {PREVIEW_LIMIT} 行",
    )


def compute_compare_column_widths(before_df: pd.DataFrame, after_df: pd.DataFrame) -> dict[str, int]:
    columns = [str(column) for column in after_df.columns]
    widths: dict[str, int] = {}
    for column in columns:
        max_len = len(column)
        for dataframe in (before_df, after_df):
            if column not in dataframe.columns:
                continue
            for value in dataframe[column].head(PREVIEW_LIMIT).tolist():
                max_len = max(max_len, len(preview_value(value)))
        widths[column] = min(max(max_len + 2, 10), 28)
    return widths


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
