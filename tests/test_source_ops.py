from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from spreadsheet_tool.models import SourceSelection
from spreadsheet_tool.source_ops import (
    MISSING_OLD,
    MULTIPLE_OLD,
    PAIR_INCOMPLETE,
    expand_input_paths,
    resolve_processing_scope_sources,
    resolve_writeback_target_source,
)


class SourceOpsTests(unittest.TestCase):
    def test_expand_input_paths_filters_supported_files_and_deduplicates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            xlsx_path = base / "a.xlsx"
            csv_path = base / "b.csv"
            ignored_path = base / "ignore.txt"
            xlsx_path.write_text("x", encoding="utf-8")
            csv_path.write_text("x", encoding="utf-8")
            ignored_path.write_text("x", encoding="utf-8")

            expanded = expand_input_paths([base, f'"{csv_path}"', xlsx_path])

            self.assertEqual(expanded, [xlsx_path, csv_path])

    def test_resolve_processing_scope_sources_requires_complete_pair(self) -> None:
        old_source = SourceSelection("old", Path("old.xlsx"), "Sheet1", dataset_role="old")
        new_source = SourceSelection("new", Path("new.xlsx"), "Sheet1", dataset_role="new")
        sources = {"old": old_source, "new": new_source}

        incomplete = resolve_processing_scope_sources(sources, {"old": "old", "new": None})
        complete = resolve_processing_scope_sources(sources, {"old": "old", "new": "new"})
        missing_active = resolve_processing_scope_sources(sources, {"old": "gone", "new": "new"})

        self.assertIsNone(incomplete.sources)
        self.assertEqual(incomplete.reason, PAIR_INCOMPLETE)
        self.assertEqual(list(complete.sources), ["old", "new"])
        self.assertEqual(list(missing_active.sources), ["old", "new"])

    def test_resolve_writeback_target_source_reports_missing_and_multiple_old_sources(self) -> None:
        old_a = SourceSelection("old_a", Path("old_a.xlsx"), "Sheet1", dataset_role="old")
        old_b = SourceSelection("old_b", Path("old_b.xlsx"), "Sheet1", dataset_role="old")
        new_source = SourceSelection("new", Path("new.xlsx"), "Sheet1", dataset_role="new")

        missing = resolve_writeback_target_source({"new": new_source}, None, {"new": new_source})
        preferred = resolve_writeback_target_source(
            {"old_a": old_a, "old_b": old_b, "new": new_source},
            "old_b",
            {"old_a": old_a, "old_b": old_b, "new": new_source},
        )
        ambiguous = resolve_writeback_target_source(
            {"old_a": old_a, "old_b": old_b},
            None,
            {"old_a": old_a, "old_b": old_b},
        )

        self.assertIsNone(missing.source)
        self.assertEqual(missing.reason, MISSING_OLD)
        self.assertIs(preferred.source, old_b)
        self.assertEqual(preferred.reason, "ok")
        self.assertIsNone(ambiguous.source)
        self.assertEqual(ambiguous.reason, MULTIPLE_OLD)


if __name__ == "__main__":
    unittest.main()
