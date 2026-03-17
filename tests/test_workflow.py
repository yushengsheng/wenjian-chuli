from __future__ import annotations

import unittest
from unittest import mock
from pathlib import Path

import pandas as pd

from spreadsheet_tool.models import SourceSelection
from spreadsheet_tool.processor import build_direct_source_to_target_mapping
from spreadsheet_tool.workflow import (
    MISSING_OLD,
    NO_DATA,
    apply_imported_sources,
    build_mapping_session,
    prepare_processing,
)


class WorkflowTests(unittest.TestCase):
    def test_direct_mapping_prioritizes_exact_same_header_names(self) -> None:
        mapping = build_direct_source_to_target_mapping(
            ["编号", "邮箱", "密码", "邮箱密钥1", "邮箱密钥2", "币安谷歌"],
            ["备注", "邮箱", "密码", "邮箱密钥1", "邮箱密钥2", "币安谷歌", "api", "secret"],
        )

        self.assertEqual(
            mapping,
            {
                "邮箱": "邮箱",
                "密码": "密码",
                "邮箱密钥1": "邮箱密钥1",
                "邮箱密钥2": "邮箱密钥2",
                "币安谷歌": "币安谷歌",
            },
        )

    def test_apply_imported_sources_assigns_role_and_merges_cache(self) -> None:
        existing_source = SourceSelection("old", Path("old.xlsx"), "Sheet1", dataset_role="old")
        imported_source = SourceSelection("new", Path("new.xlsx"), "Sheet1")
        result = apply_imported_sources(
            {"old": existing_source},
            {"old": pd.DataFrame([{"email": "a@example.com"}])},
            [imported_source],
            {"new": pd.DataFrame([{"email": "b@example.com"}])},
            "new",
            "手动导入新数据",
            1,
        )

        self.assertEqual(imported_source.dataset_role, "new")
        self.assertEqual(set(result.sources), {"old", "new"})
        self.assertEqual(set(result.cache), {"old", "new"})
        self.assertIs(result.first_source, imported_source)
        self.assertEqual(result.status_text, "手动导入新数据完成：1 个数据源")
        self.assertEqual(len(result.summary_lines), 2)

    def test_prepare_processing_reports_no_data_and_missing_old(self) -> None:
        new_source = SourceSelection("new", Path("new.xlsx"), "Sheet1", dataset_role="new", enabled=True)
        no_data = prepare_processing({"new": new_source}, {"new": pd.DataFrame()})
        missing_old = prepare_processing(
            {"new": new_source},
            {"new": pd.DataFrame([{"email": "a@example.com"}])},
        )

        self.assertEqual(no_data.reason, NO_DATA)
        self.assertTrue(no_data.raw_dataframe.empty)
        self.assertEqual(missing_old.reason, MISSING_OLD)

    def test_prepare_processing_collects_unmapped_new_sources(self) -> None:
        old_source = SourceSelection(
            "old",
            Path("old.xlsx"),
            "Sheet1",
            dataset_role="old",
            enabled=True,
            columns=["email"],
        )
        mapped_new = SourceSelection(
            "new_a",
            Path("new_a.xlsx"),
            "Sheet1",
            dataset_role="new",
            enabled=True,
            columns=["email"],
            mapping_confirmed=True,
        )
        unmapped_new = SourceSelection(
            "new_b",
            Path("new_b.xlsx"),
            "Sheet1",
            dataset_role="new",
            enabled=True,
            columns=["email"],
        )
        preparation = prepare_processing(
            {"old": old_source, "new_a": mapped_new, "new_b": unmapped_new},
            {
                "old": pd.DataFrame([{"email": "a@example.com"}]),
                "new_a": pd.DataFrame([{"email": "b@example.com"}]),
                "new_b": pd.DataFrame([{"email": "c@example.com"}]),
            },
        )

        self.assertEqual(preparation.reason, "ok")
        self.assertEqual([source.source_id for source in preparation.unmapped_new_sources], ["new_b"])
        self.assertFalse(preparation.raw_dataframe_ready)

    def test_prepare_processing_skips_combine_when_missing_old(self) -> None:
        new_source = SourceSelection("new", Path("new.xlsx"), "Sheet1", dataset_role="new", enabled=True)
        cache = {"new": pd.DataFrame([{"email": "a@example.com"}])}

        with mock.patch("spreadsheet_tool.workflow.combine_enabled_sources") as combine_mock:
            preparation = prepare_processing({"new": new_source}, cache)

        self.assertEqual(preparation.reason, MISSING_OLD)
        self.assertFalse(preparation.raw_dataframe_ready)
        combine_mock.assert_not_called()

    def test_prepare_processing_skips_combine_until_mapping_confirmed(self) -> None:
        old_source = SourceSelection(
            "old",
            Path("old.xlsx"),
            "Sheet1",
            dataset_role="old",
            enabled=True,
            columns=["email"],
        )
        new_source = SourceSelection(
            "new",
            Path("new.xlsx"),
            "Sheet1",
            dataset_role="new",
            enabled=True,
            columns=["email"],
            mapping_confirmed=False,
        )
        cache = {
            "old": pd.DataFrame([{"email": "a@example.com"}]),
            "new": pd.DataFrame([{"email": "b@example.com"}]),
        }

        with mock.patch("spreadsheet_tool.workflow.combine_enabled_sources") as combine_mock:
            preparation = prepare_processing({"old": old_source, "new": new_source}, cache)

        self.assertEqual(preparation.reason, "ok")
        self.assertEqual([source.source_id for source in preparation.unmapped_new_sources], ["new"])
        self.assertFalse(preparation.raw_dataframe_ready)
        combine_mock.assert_not_called()

    def test_build_mapping_session_detects_auto_confirm_and_suggestions(self) -> None:
        old_source = SourceSelection(
            "old",
            Path("old.xlsx"),
            "Sheet1",
            dataset_role="old",
            enabled=True,
            columns=["email", "password"],
        )
        direct_new = SourceSelection(
            "new_direct",
            Path("new_direct.xlsx"),
            "Sheet1",
            dataset_role="new",
            enabled=True,
            columns=["email", "password"],
        )
        loose_new = SourceSelection(
            "new_loose",
            Path("new_loose.xlsx"),
            "Sheet1",
            dataset_role="new",
            enabled=True,
            columns=["列1", "列2"],
        )
        cache = {
            "old": pd.DataFrame([{"email": "a@example.com", "password": "old"}]),
            "new_direct": pd.DataFrame([{"email": "b@example.com", "password": "new"}]),
            "new_loose": pd.DataFrame([{"列1": "c@example.com", "列2": "pw123456"}]),
        }

        session = build_mapping_session(
            {"old": old_source, "new_direct": direct_new, "new_loose": loose_new},
            cache,
            [direct_new, loose_new],
        )

        self.assertEqual(session.target_columns, ["email", "password"])
        self.assertEqual(len(session.candidates), 2)
        self.assertTrue(session.candidates[0].auto_confirmed)
        self.assertTrue(session.candidates[0].can_auto_apply)
        self.assertEqual(session.candidates[0].direct_mapping, {"email": "email", "password": "password"})
        self.assertFalse(session.candidates[1].auto_confirmed)
        self.assertFalse(session.candidates[1].can_auto_apply)
        self.assertEqual(session.candidates[1].suggested_mapping, {"列1": "email", "列2": "password"})

    def test_build_mapping_session_uses_exact_headers_even_when_template_has_no_sample_rows(self) -> None:
        old_source = SourceSelection(
            "old",
            Path("old.xlsx"),
            "Sheet1",
            dataset_role="old",
            enabled=True,
            columns=["备注", "邮箱", "密码", "邮箱密钥1", "邮箱密钥2", "币安谷歌", "api", "secret"],
        )
        new_source = SourceSelection(
            "new",
            Path("new.xlsx"),
            "Sheet1",
            dataset_role="new",
            enabled=True,
            columns=["编号", "邮箱", "密码", "邮箱密钥1", "邮箱密钥2", "币安谷歌", "日期"],
        )
        cache = {
            "old": pd.DataFrame(columns=old_source.columns),
            "new": pd.DataFrame(
                [
                    {
                        "编号": "1",
                        "邮箱": "a@example.com",
                        "密码": "pass-1",
                        "邮箱密钥1": "lowersecretabc",
                        "邮箱密钥2": "temp@mail.com",
                        "币安谷歌": "ABCDEFGHJKLMNPQR",
                        "日期": "2026-03-16",
                    }
                ]
            ),
        }

        session = build_mapping_session({"old": old_source, "new": new_source}, cache, [new_source])

        self.assertEqual(len(session.candidates), 1)
        self.assertTrue(session.candidates[0].can_auto_apply)
        self.assertEqual(
            session.candidates[0].suggested_mapping,
            {
                "邮箱": "邮箱",
                "密码": "密码",
                "邮箱密钥1": "邮箱密钥1",
                "邮箱密钥2": "邮箱密钥2",
                "币安谷歌": "币安谷歌",
            },
        )


if __name__ == "__main__":
    unittest.main()
