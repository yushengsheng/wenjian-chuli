from __future__ import annotations

import unittest

import pandas as pd

from spreadsheet_tool.compare_render import (
    build_comparison_info,
    compute_compare_column_widths,
    fit_compare_text,
    marker_for_cell,
    summarize_comparison_statuses,
)


class CompareRenderTests(unittest.TestCase):
    def test_build_comparison_info_uses_row_counts(self) -> None:
        before = pd.DataFrame([{"email": "a@example.com"}])
        after = pd.DataFrame([{"email": "a@example.com"}, {"email": "b@example.com"}])

        before_info, after_info = build_comparison_info(before, after)

        self.assertEqual(before_info, "修改前 1 行 | 预览前 200 行")
        self.assertEqual(after_info, "修改后 2 行 | 预览前 200 行")

    def test_compute_compare_column_widths_uses_preview_lengths(self) -> None:
        before = pd.DataFrame([{"email": "a@example.com"}])
        after = pd.DataFrame([{"email": "very.long.email@example.com"}])

        widths = compute_compare_column_widths(before, after)

        self.assertEqual(widths["email"], 28)

    def test_marker_for_cell_matches_side_and_status(self) -> None:
        self.assertEqual(marker_for_cell("same", "before", "email", {"email"}), ("", None))
        self.assertEqual(marker_for_cell("added", "after", "email", {"email"}), ("+", "plus"))
        self.assertEqual(marker_for_cell("removed", "before", "email", {"email"}), ("-", "minus"))
        self.assertEqual(marker_for_cell("changed", "before", "email", {"email"}), ("-", "minus"))
        self.assertEqual(marker_for_cell("changed", "after", "email", {"email"}), ("+", "plus"))

    def test_fit_compare_text_truncates_and_pads(self) -> None:
        self.assertEqual(fit_compare_text("abcdef", 4), "abc…")
        self.assertEqual(fit_compare_text("abc", 5), "abc  ")

    def test_summarize_comparison_statuses_counts_each_status(self) -> None:
        summary = summarize_comparison_statuses(["same", "changed", "added", "changed"])

        self.assertEqual(
            summary,
            [
                "比较摘要: 新增 1 行",
                "比较摘要: 变更 2 行",
                "比较摘要: 未变化 1 行",
                "比较摘要: 移除 0 行",
            ],
        )


if __name__ == "__main__":
    unittest.main()
