from __future__ import annotations

import tkinter as tk
import unittest
from unittest import mock

from spreadsheet_tool.dialogs import FilterRuleDialog, UpdateRuleDialog
from spreadsheet_tool.models import FilterRule, UpdateRule


class DialogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = tk.Tk()
        self.root.withdraw()

    def tearDown(self) -> None:
        self.root.destroy()

    def test_filter_rule_dialog_warns_when_original_column_is_missing(self) -> None:
        dialog = FilterRuleDialog(
            self.root,
            ["email"],
            lambda column: column,
            initial_rule=FilterRule(column="missing", operator="equals", value="x"),
        )
        try:
            with mock.patch("spreadsheet_tool.dialogs.messagebox.showwarning") as warning_mock:
                dialog.on_confirm()

            self.assertIsNone(dialog.result)
            warning_mock.assert_called_once()
            self.assertIn("字段已失效", warning_mock.call_args.args[0])
        finally:
            dialog.destroy()

    def test_update_rule_dialog_warns_when_original_column_is_missing(self) -> None:
        dialog = UpdateRuleDialog(
            self.root,
            ["email"],
            lambda column: column,
            initial_rule=UpdateRule(mode="set_value", column="missing", replace_value="x"),
        )
        try:
            with mock.patch("spreadsheet_tool.dialogs.messagebox.showwarning") as warning_mock:
                dialog.on_confirm()

            self.assertIsNone(dialog.result)
            warning_mock.assert_called_once()
            self.assertIn("字段已失效", warning_mock.call_args.args[0])
        finally:
            dialog.destroy()


if __name__ == "__main__":
    unittest.main()
