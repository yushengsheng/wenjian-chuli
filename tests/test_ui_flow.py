from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pandas as pd

from spreadsheet_tool.models import SourceSelection
from spreadsheet_tool.ui import SpreadsheetApp
from spreadsheet_tool.workflow import MappingCandidate, MappingSession


class FakeTreeview:
    def __init__(self) -> None:
        self.columns: tuple[str, ...] = ()
        self.rows: list[tuple[str, tuple[object, ...]]] = []
        self.headings: dict[str, str] = {}
        self.column_configs: dict[str, dict[str, object]] = {}

    def get_children(self) -> tuple[str, ...]:
        return tuple(row_id for row_id, _ in self.rows)

    def delete(self, *item_ids: str) -> None:
        if not item_ids:
            self.rows.clear()
            return
        remove_ids = set(item_ids)
        self.rows = [(row_id, values) for row_id, values in self.rows if row_id not in remove_ids]

    def heading(self, column: str, text: str) -> None:
        self.headings[column] = text

    def column(self, column: str, **kwargs: object) -> None:
        self.column_configs[column] = kwargs

    def insert(self, _parent: str, _index: str, values: list[object]) -> str:
        row_id = f"row{len(self.rows)}"
        self.rows.append((row_id, tuple(values)))
        return row_id

    def index(self, row_id: str) -> int:
        for index, (current_row_id, _) in enumerate(self.rows):
            if current_row_id == row_id:
                return index
        raise ValueError(row_id)

    def item(self, row_id: str, option: str) -> tuple[object, ...] | str:
        if option == "text":
            return ""
        if option == "values":
            for current_row_id, values in self.rows:
                if current_row_id == row_id:
                    return values
        raise ValueError(option)

    def __setitem__(self, key: str, value: object) -> None:
        if key == "columns":
            self.columns = tuple(value)
            return
        raise KeyError(key)

    def __getitem__(self, key: str) -> tuple[str, ...]:
        if key == "columns":
            return self.columns
        raise KeyError(key)


class FakeLabel:
    def __init__(self) -> None:
        self.text = ""

    def configure(self, *, text: str) -> None:
        self.text = text


class FakeProgressbar:
    def __init__(self) -> None:
        self.started_with: list[int] = []
        self.stop_count = 0
        self.visible = False

    def start(self, interval: int) -> None:
        self.started_with.append(interval)

    def stop(self) -> None:
        self.stop_count += 1

    def grid(self) -> None:
        self.visible = True

    def grid_remove(self) -> None:
        self.visible = False


class UiFlowTests(unittest.TestCase):
    def test_set_ui_busy_toggles_busy_progressbar(self) -> None:
        fake_app = SimpleNamespace(
            ui_busy=False,
            toolbar_buttons=[],
            busy_ttk_widgets=[],
            busy_tk_widgets=[],
            busy_progress=FakeProgressbar(),
            status_var=mock.Mock(),
            configure=mock.Mock(),
            update_idletasks=mock.Mock(),
        )

        SpreadsheetApp.set_ui_busy(fake_app, True, "处理中")
        self.assertTrue(fake_app.ui_busy)
        self.assertTrue(fake_app.busy_progress.visible)
        self.assertEqual(fake_app.busy_progress.started_with, [12])
        fake_app.status_var.set.assert_called_with("处理中")

        SpreadsheetApp.set_ui_busy(fake_app, False)
        self.assertFalse(fake_app.ui_busy)
        self.assertFalse(fake_app.busy_progress.visible)
        self.assertEqual(fake_app.busy_progress.stop_count, 1)

    def test_populate_dataframe_preview_adds_preview_row_number_column(self) -> None:
        fake_app = SimpleNamespace(
            tree_preview_frames={},
            preview_value=lambda value: SpreadsheetApp.preview_value(SimpleNamespace(), value),
        )
        tree = FakeTreeview()
        label = FakeLabel()
        dataframe = pd.DataFrame([{"邮箱": "a@example.com", "备注": "人脸"}, {"邮箱": "b@example.com", "备注": "风控"}])

        SpreadsheetApp.populate_dataframe_preview(fake_app, tree, dataframe, label, "info")

        self.assertEqual(tree.columns[0], "__preview_row_number__")
        self.assertEqual(tree.headings["__preview_row_number__"], "行号")
        self.assertEqual(tree.rows[0][1][0], "1")
        self.assertEqual(tree.rows[1][1][0], "2")
        self.assertEqual(label.text, "info")

    def test_get_full_tree_cell_text_handles_preview_row_number_column(self) -> None:
        fake_app = SimpleNamespace(
            tree_preview_frames={},
            sources={},
            preview_value=lambda value: SpreadsheetApp.preview_value(SimpleNamespace(), value),
        )
        tree = FakeTreeview()
        label = FakeLabel()
        dataframe = pd.DataFrame([{"邮箱": "a@example.com", "备注": "人脸"}])

        SpreadsheetApp.populate_dataframe_preview(fake_app, tree, dataframe, label, "info")

        row_id = tree.rows[0][0]
        self.assertEqual(SpreadsheetApp.get_full_tree_cell_text(fake_app, tree, row_id, "#1"), "1")
        self.assertEqual(SpreadsheetApp.get_full_tree_cell_text(fake_app, tree, row_id, "#2"), "a@example.com")
        self.assertEqual(SpreadsheetApp.get_full_tree_cell_text(fake_app, tree, row_id, "#3"), "人脸")

    def test_open_mapping_dialog_for_sources_returns_false_when_auto_open_is_cancelled(self) -> None:
        new_source = SourceSelection("new", Path("new.xlsx"), "Sheet1", dataset_role="new", columns=["邮箱"])
        dataframe = pd.DataFrame([{"邮箱": "a@example.com"}])
        candidate = MappingCandidate(
            source=new_source,
            dataframe=dataframe,
            suggested_mapping={"邮箱": "邮箱"},
            direct_mapping={},
            auto_confirmed=False,
            can_auto_apply=False,
        )
        fake_app = SimpleNamespace(
            get_mapping_scope_sources=mock.Mock(return_value={"new": new_source}),
            data_cache={"new": dataframe},
            wait_window=mock.Mock(),
            processed_df=None,
            status_var=mock.Mock(),
            invalidate_processed_results=mock.Mock(),
        )

        with mock.patch(
            "spreadsheet_tool.ui.build_mapping_session",
            return_value=MappingSession(target_columns=["邮箱"], candidates=[candidate]),
        ), mock.patch("spreadsheet_tool.ui.SourceMappingDialog") as dialog_cls:
            dialog_cls.return_value.result = None

            result = SpreadsheetApp.open_mapping_dialog_for_sources(fake_app, [new_source], auto_open=True)

        self.assertFalse(result)
        self.assertFalse(new_source.mapping_confirmed)
        fake_app.status_var.set.assert_called_once_with("字段匹配未完成，可稍后继续。")
        fake_app.invalidate_processed_results.assert_not_called()

    def test_import_paths_marks_new_sources_as_pending_when_auto_mapping_is_cancelled(self) -> None:
        old_source = SourceSelection(
            "old",
            Path("old.xlsx"),
            "Sheet1",
            dataset_role="old",
            columns=["邮箱"],
            row_count=1,
        )
        imported_source = SourceSelection(
            "new",
            Path("new.xlsx"),
            "Sheet1",
            columns=["邮箱"],
            row_count=1,
        )
        old_cache = {"old": pd.DataFrame([{"邮箱": "old@example.com"}])}
        new_cache = {"new": pd.DataFrame([{"邮箱": "new@example.com"}])}
        tree = mock.Mock()

        def start_background_task(*, success_handler, **_: object) -> bool:
            success_handler(([imported_source], new_cache))
            return True

        fake_app = SimpleNamespace(
            action_allowed=mock.Mock(return_value=True),
            expand_input_paths=mock.Mock(return_value=[Path("new.xlsx")]),
            remember_browse_path=mock.Mock(),
            start_background_task=start_background_task,
            sources={"old": old_source},
            data_cache=old_cache,
            invalidate_processed_results=mock.Mock(),
            refresh_source_trees=mock.Mock(),
            refresh_available_columns=mock.Mock(),
            write_summary=mock.Mock(),
            status_var=mock.Mock(),
            clear_tree_selection_except=mock.Mock(),
            expand_source_parent=mock.Mock(),
            source_trees={"new": tree},
            preview_source=mock.Mock(),
            open_mapping_dialog_for_sources=mock.Mock(return_value=False),
        )

        SpreadsheetApp.import_paths(fake_app, ["new.xlsx"], "new", "手动导入新数据")

        fake_app.open_mapping_dialog_for_sources.assert_called_once_with([imported_source], auto_open=True)
        self.assertEqual(
            fake_app.status_var.set.call_args.args[0],
            "导入完成：部分新数据尚未完成字段匹配",
        )
        fake_app.write_summary.assert_called_with(
            mock.ANY,
        )
        self.assertIn(
            "部分新数据尚未完成字段匹配，可稍后点击“字段匹配”继续。",
            fake_app.write_summary.call_args.args[0],
        )


if __name__ == "__main__":
    unittest.main()
