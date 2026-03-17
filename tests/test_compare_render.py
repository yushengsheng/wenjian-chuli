from __future__ import annotations

import unittest

import pandas as pd

from spreadsheet_tool.compare_render import (
    build_comparison_info,
    compute_compare_column_widths,
    filter_comparison_rows,
    fit_compare_text,
    marker_for_cell,
    summarize_comparison_statuses,
)


class CompareRenderTests(unittest.TestCase):
    def test_build_comparison_info_uses_actual_and_displayed_row_counts(self) -> None:
        before_info, after_info = build_comparison_info(1, 2, 2)

        self.assertEqual(before_info, "修改前 1 行 | 当前显示 2 行（全部）")
        self.assertEqual(after_info, "修改后 2 行 | 当前显示 2 行（全部）")

    def test_build_comparison_info_marks_changes_only_mode(self) -> None:
        before_info, after_info = build_comparison_info(1905, 1971, 66, changes_only=True)

        self.assertEqual(before_info, "修改前 1905 行 | 当前显示 66 行（仅变动）")
        self.assertEqual(after_info, "修改后 1971 行 | 当前显示 66 行（仅变动）")

    def test_compute_compare_column_widths_uses_preview_lengths(self) -> None:
        before = pd.DataFrame([{"email": "a@example.com"}])
        after = pd.DataFrame([{"email": "very.long.email@example.com"}])

        widths = compute_compare_column_widths(before, after)

        self.assertEqual(widths["email"], 28)

    def test_filter_comparison_rows_keeps_only_non_same_rows(self) -> None:
        before = pd.DataFrame(
            [
                {"email": "same@example.com", "password": "same"},
                {"email": "changed@example.com", "password": "old"},
                {"email": "", "password": ""},
            ]
        )
        after = pd.DataFrame(
            [
                {"email": "same@example.com", "password": "same"},
                {"email": "changed@example.com", "password": "new"},
                {"email": "added@example.com", "password": "fresh"},
            ]
        )

        filtered_before, filtered_after, statuses, changed_columns = filter_comparison_rows(
            before,
            after,
            ["same", "changed", "added"],
            [set(), {"password"}, {"email", "password"}],
            changes_only=True,
        )

        self.assertEqual(filtered_before["email"].tolist(), ["changed@example.com", ""])
        self.assertEqual(filtered_after["email"].tolist(), ["changed@example.com", "added@example.com"])
        self.assertEqual(statuses, ["changed", "added"])
        self.assertEqual(changed_columns, [{"password"}, {"email", "password"}])

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
