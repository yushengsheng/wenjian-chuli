from __future__ import annotations

from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
from typing import Iterable
from uuid import uuid4
from xml.etree import ElementTree as ET
from zipfile import BadZipFile, ZipFile

import pandas as pd

from .models import SourceSelection
from .processor_common import (
    CSV_ENCODINGS,
    EMPTY_TEXT_VALUES,
    SPREADSHEET_MAIN_NS,
    STYLES_XML_PATH,
    SUPPORTED_FILE_TYPES,
    canonical_internal_column_name,
    is_empty_value,
)


def load_sources_from_paths(
    paths: Iterable[str | Path],
) -> tuple[list[SourceSelection], dict[str, pd.DataFrame]]:
    sources: list[SourceSelection] = []
    cache: dict[str, pd.DataFrame] = {}

    for raw_path in paths:
        path = Path(raw_path)
        suffix = path.suffix.lower()
        if suffix not in SUPPORTED_FILE_TYPES:
            raise ValueError(f"暂不支持该文件类型: {path.name}")

        if suffix in {".csv", ".tsv"}:
            dataframe = read_delimited_file(path, "\t" if suffix == ".tsv" else ",")
            source_id = uuid4().hex
            sources.append(
                SourceSelection(
                    source_id=source_id,
                    path=path,
                    sheet_name="数据",
                    row_count=len(dataframe),
                    columns=list(dataframe.columns),
                )
            )
            cache[source_id] = dataframe
            continue

        with open_excel_file(path) as workbook:
            for sheet_name in workbook.sheet_names:
                raw = workbook.parse(
                    sheet_name=sheet_name,
                    dtype=object,
                    header=None,
                )
                dataframe = materialize_dataframe(raw)
                source_id = uuid4().hex
                sources.append(
                    SourceSelection(
                        source_id=source_id,
                        path=path,
                        sheet_name=sheet_name,
                        row_count=len(dataframe),
                        columns=list(dataframe.columns),
                    )
                )
                cache[source_id] = dataframe

    return sources, cache


@contextmanager
def open_excel_file(path: Path):
    original_error: Exception | None = None
    try:
        with pd.ExcelFile(path, engine="openpyxl") as workbook:
            yield workbook
            return
    except Exception as exc:
        if not should_retry_excel_with_style_repair(path, exc):
            raise
        original_error = exc

    repaired_workbook = repair_excel_styles_for_openpyxl(path)
    if repaired_workbook is None or original_error is None:
        raise original_error or ValueError(f"无法导入 Excel 文件: {path.name}")

    try:
        with pd.ExcelFile(repaired_workbook, engine="openpyxl") as workbook:
            yield workbook
    except Exception as exc:
        raise ValueError(
            f"无法导入 Excel 文件 {path.name}：已尝试兼容损坏的单元格填充样式，但仍然失败。原始错误: {original_error}"
        ) from exc


def should_retry_excel_with_style_repair(path: Path, exc: Exception) -> bool:
    return path.suffix.lower() in {".xlsx", ".xlsm"} and "openpyxl.styles.fills.Fill" in str(exc)


def repair_excel_styles_for_openpyxl(path: Path) -> BytesIO | None:
    try:
        with ZipFile(path) as source_zip:
            try:
                styles_xml = source_zip.read(STYLES_XML_PATH)
            except KeyError:
                return None

            repaired_styles_xml = repair_stylesheet_empty_fill_nodes(styles_xml)
            if repaired_styles_xml is None:
                return None

            rebuilt = BytesIO()
            with ZipFile(rebuilt, mode="w") as target_zip:
                for item in source_zip.infolist():
                    payload = repaired_styles_xml if item.filename == STYLES_XML_PATH else source_zip.read(item.filename)
                    target_zip.writestr(item, payload)
            rebuilt.seek(0)
            return rebuilt
    except (BadZipFile, OSError, ET.ParseError):
        return None


def repair_stylesheet_empty_fill_nodes(styles_xml: bytes) -> bytes | None:
    root = ET.fromstring(styles_xml)
    fills = root.find(f"{{{SPREADSHEET_MAIN_NS}}}fills")
    if fills is None:
        return None

    repaired = False
    for fill in list(fills):
        if xml_local_name(fill.tag) != "fill":
            continue
        if len(list(fill)) != 0:
            continue
        ET.SubElement(fill, f"{{{SPREADSHEET_MAIN_NS}}}patternFill")
        repaired = True

    if not repaired:
        return None

    ET.register_namespace("", SPREADSHEET_MAIN_NS)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def xml_local_name(tag: str) -> str:
    if "}" not in tag:
        return tag
    return tag.rsplit("}", 1)[-1]


def read_delimited_file(path: Path, separator: str) -> pd.DataFrame:
    last_error: Exception | None = None
    for encoding in CSV_ENCODINGS:
        try:
            raw = pd.read_csv(path, sep=separator, dtype=object, encoding=encoding, header=None)
            return materialize_dataframe(raw)
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error is not None:
        raise ValueError(f"无法识别文件编码: {path.name}") from last_error
    raw = pd.read_csv(path, sep=separator, dtype=object, header=None)
    return materialize_dataframe(raw)


def read_excel_sheet(path: Path, sheet_name: str) -> pd.DataFrame:
    with open_excel_file(path) as workbook:
        raw = workbook.parse(
            sheet_name=sheet_name,
            dtype=object,
            header=None,
        )
    return materialize_dataframe(raw)


def materialize_dataframe(raw: pd.DataFrame) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame()

    prepared = raw.dropna(axis=0, how="all").reset_index(drop=True)
    if prepared.empty:
        return pd.DataFrame()

    header_values = prepared.iloc[0].tolist()
    body = prepared.iloc[1:].copy()
    selected_indexes: list[int] = []
    columns: list[str] = []
    for index, value in enumerate(header_values):
        if is_empty_value(value):
            continue
        selected_indexes.append(index)
        columns.append(normalize_column_name(value, index))

    dataframe = body.iloc[:, selected_indexes].copy() if selected_indexes else pd.DataFrame()
    dataframe.columns = columns
    return normalize_dataframe(dataframe, preserve_empty_columns=True)


def normalize_dataframe(dataframe: pd.DataFrame, preserve_empty_columns: bool = False) -> pd.DataFrame:
    if dataframe is None:
        return pd.DataFrame()

    normalized = dataframe.copy()
    normalized.columns = make_unique_column_names(
        [normalize_column_name(column_name, index) for index, column_name in enumerate(normalized.columns)]
    )
    normalized = normalized.dropna(axis=0, how="all")
    if not preserve_empty_columns:
        normalized = normalized.dropna(axis=1, how="all")
    normalized = normalized.reset_index(drop=True)
    return normalized


def normalize_column_name(column_name: object, index: int) -> str:
    if column_name is None:
        return f"列{index + 1}"
    text = str(column_name).strip()
    internal_column = canonical_internal_column_name(text)
    if internal_column is not None:
        return internal_column
    if text.lower() in EMPTY_TEXT_VALUES:
        return f"列{index + 1}"
    return text or f"列{index + 1}"


def make_unique_column_names(columns: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    unique_columns: list[str] = []
    for column in columns:
        count = seen.get(column, 0)
        if count == 0:
            unique_columns.append(column)
        else:
            unique_columns.append(f"{column}_{count + 1}")
        seen[column] = count + 1
    return unique_columns


def first_row_looks_like_header(raw: pd.DataFrame) -> bool:
    return not raw.empty


__all__ = [
    "first_row_looks_like_header",
    "load_sources_from_paths",
    "make_unique_column_names",
    "materialize_dataframe",
    "normalize_column_name",
    "normalize_dataframe",
    "open_excel_file",
    "read_delimited_file",
    "read_excel_sheet",
    "repair_excel_styles_for_openpyxl",
    "repair_stylesheet_empty_fill_nodes",
    "should_retry_excel_with_style_repair",
    "xml_local_name",
]
