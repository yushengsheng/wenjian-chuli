from __future__ import annotations

import unittest
from pathlib import Path

import pandas as pd

from spreadsheet_tool.export_workflow import (
    apply_writeback_result,
    build_csv_export_summary,
    build_workbook_export_plan,
    build_workbook_export_summary,
    output_format_for_source,
)
from spreadsheet_tool.models import SourceSelection


class ExportWorkflowTests(unittest.TestCase):
    def test_output_format_for_source_follows_suffix(self) -> None:
        self.assertEqual(output_format_for_source(SourceSelection("a", Path("a.csv"), "Sheet1")), "csv")
        self.assertEqual(output_format_for_source(SourceSelection("b", Path("b.tsv"), "Sheet1")), "csv")
        self.assertEqual(output_format_for_source(SourceSelection("c", Path("c.xlsx"), "Sheet1")), "xlsx")

    def test_apply_writeback_result_updates_cache_and_source_metadata(self) -> None:
        target = SourceSelection("old", Path("old.xlsx"), "Sheet1", dataset_role="old")
        processed = pd.DataFrame([{"email": "a@example.com"}, {"email": "b@example.com"}])

        result = apply_writeback_result({}, target, processed)

        self.assertEqual(result.updated_cache["old"].to_dict(orient="records"), processed.to_dict(orient="records"))
        self.assertEqual(target.row_count, 2)
        self.assertEqual(target.columns, ["email"])
        self.assertEqual(result.status_text, "写回完成: old.xlsx / Sheet1")
        self.assertEqual(result.summary_lines[-1], "写回行数: 2")

    def test_build_workbook_export_plan_and_summaries(self) -> None:
        target = SourceSelection("old", Path("old.xlsm"), "Sheet1", dataset_role="old")

        plan = build_workbook_export_plan(target)

        self.assertEqual(plan.extension, ".xlsm")
        self.assertEqual(plan.initial_filename, "old_处理后.xlsm")
        self.assertEqual(build_csv_export_summary("out.csv"), ["文件已导出到: out.csv"])
        self.assertEqual(
            build_workbook_export_summary("out.xlsm", target),
            [
                "已导出完整老文件: out.xlsm",
                "基于老文件模板: old.xlsm",
                "替换工作表: Sheet1",
            ],
        )


if __name__ == "__main__":
    unittest.main()
