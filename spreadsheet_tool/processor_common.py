from __future__ import annotations

import re

import pandas as pd

INTERNAL_SOURCE_FILE = "__source_file"
INTERNAL_SOURCE_SHEET = "__source_sheet"
INTERNAL_SOURCE_ROLE = "__source_role"
INTERNAL_APPEND_ORDER = "__append_order"
INTERNAL_COLUMNS = [
    INTERNAL_SOURCE_FILE,
    INTERNAL_SOURCE_SHEET,
    INTERNAL_SOURCE_ROLE,
    INTERNAL_APPEND_ORDER,
]

DISPLAY_NAME_OVERRIDES = {
    INTERNAL_SOURCE_FILE: "来源文件",
    INTERNAL_SOURCE_SHEET: "来源工作表",
    INTERNAL_SOURCE_ROLE: "数据分组",
    INTERNAL_APPEND_ORDER: "导入顺序",
}

SUPPORTED_FILE_TYPES = {
    ".xlsx",
    ".xlsm",
    ".csv",
    ".tsv",
}

CSV_ENCODINGS = ("utf-8-sig", "utf-8", "gb18030", "gbk")
EMPTY_TEXT_VALUES = {"", "nan", "none", "null", "nat", "<na>"}
SPREADSHEET_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
STYLES_XML_PATH = "xl/styles.xml"
_INTEGERISH_TEXT = re.compile(r"^-?\d+\.0+$")
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
HEX_ADDRESS_PATTERN = re.compile(r"^0x[a-fA-F0-9]{40}$")
UPPER_SECRET_PATTERN = re.compile(r"^[A-Z2-7]{16,32}$")
LOWER_SECRET_PATTERN = re.compile(r"^[a-z0-9]{16,32}$")
LONG_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9]{40,120}$")
SHORT_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9]{6,24}$")
CODE_PATTERN = re.compile(r"^[A-Za-z]{1,4}\d{1,6}$")
PHONE_PATTERN = re.compile(r"^\+?\d[\d\-\s()]{6,18}\d$")

HEADER_ALIASES = {
    "特殊备注": {"特殊备注", "备注", "remark", "note", "notes"},
    "邮箱": {"邮箱", "email", "mail", "e-mail", "outlook"},
    "手机号": {"手机号", "手机", "电话", "联系电话", "phone", "mobile", "tel", "telephone"},
    "邮箱密码": {"邮箱密码", "emailpassword", "mailpassword", "password", "邮箱pass", "mailpass"},
    "邮箱2fa": {
        "邮箱2fa",
        "邮箱2fa/Oauth",
        "邮箱2fa/oauth",
        "email2fa",
        "mail2fa",
        "邮箱otp",
        "emailotp",
        "2fa",
        "oauth",
        "oath",
    },
    "币安2fa": {"币安2fa", "binance2fa", "币安谷歌", "币安otp", "binanceotp"},
    "币安充值地址": {
        "币安充值地址",
        "充值地址",
        "地址",
        "depositaddress",
        "walletaddress",
        "binanceaddress",
    },
    "apikey": {"apikey", "api_key", "api key", "key"},
    "apisecret": {"apisecret", "api_secret", "api secret", "secret"},
}

WEAK_KINDS = {"text", "unknown", "empty", "code", "number"}

FILTER_OPERATORS = {
    "equals": "等于",
    "not_equals": "不等于",
    "contains": "包含",
    "not_contains": "不包含",
    "greater_than": "大于",
    "greater_equal": "大于等于",
    "less_than": "小于",
    "less_equal": "小于等于",
    "is_empty": "为空",
    "not_empty": "不为空",
}

UPDATE_MODES = {
    "set_value": "整列赋值",
    "fill_empty": "空值补全",
    "replace_text": "文本替换",
    "replace_exact": "精确替换",
}

DUPLICATE_STRATEGIES = {
    "update_and_append": "更新并新增",
    "update_only": "仅更新不新增",
    "fill_old_empty": "仅用新数据补全老数据空值",
    "keep_first": "保留首条",
    "keep_last": "保留末条",
    "none": "不处理重复",
}

LEGACY_DUPLICATE_STRATEGY_ALIASES = {
    "new_overwrite_old": "update_and_append",
}

UNMAPPED_TARGET = object()


def normalize_header_text(text: str) -> str:
    return re.sub(r"[\s_\-:/\\]+", "", text.strip().lower())


_NORMALIZED_HEADER_ALIAS_LOOKUP: dict[str, str] = {}
for _target, _aliases in HEADER_ALIASES.items():
    for _alias in _aliases | {_target}:
        _NORMALIZED_HEADER_ALIAS_LOOKUP.setdefault(normalize_header_text(_alias), _target)


def default_visible(column_name: str, include_source_columns: bool = True) -> bool:
    if canonical_internal_column_name(column_name) is not None:
        return include_source_columns
    return True


def default_display_name(column_name: str) -> str:
    return DISPLAY_NAME_OVERRIDES.get(column_name, column_name)


def canonical_internal_column_name(column_name: object) -> str | None:
    text = str(column_name).strip()
    if not text:
        return None
    for internal_column in INTERNAL_COLUMNS:
        if text == internal_column or text == default_display_name(internal_column):
            return internal_column
    return None


def header_alias_key(value: object) -> str:
    if value is None:
        return ""
    return _NORMALIZED_HEADER_ALIAS_LOOKUP.get(normalize_header_text(str(value)), "")


def normalize_duplicate_strategy(strategy: str) -> str:
    return LEGACY_DUPLICATE_STRATEGY_ALIASES.get(strategy, strategy)


def is_unmapped_target(value: object) -> bool:
    return value is UNMAPPED_TARGET


def is_empty_value(value: object) -> bool:
    if is_unmapped_target(value):
        return True
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    text = str(value).strip()
    return text.lower() in EMPTY_TEXT_VALUES


def is_empty_series(series: pd.Series) -> pd.Series:
    unmapped_mask = series.map(is_unmapped_target) if not series.empty else pd.Series(False, index=series.index)
    return unmapped_mask | series.isna() | series.fillna("").astype(str).str.strip().str.lower().isin(EMPTY_TEXT_VALUES)


def normalize_key_value(value: object) -> str:
    if is_empty_value(value):
        return ""

    if isinstance(value, bool):
        return str(value)

    if isinstance(value, int):
        return str(value)

    if isinstance(value, float):
        if pd.isna(value):
            return ""
        if value.is_integer():
            return str(int(value))
        return format(value, "g")

    text = str(value).strip()
    lowered = text.lower()
    if lowered in EMPTY_TEXT_VALUES:
        return ""
    if _INTEGERISH_TEXT.fullmatch(text):
        return text.split(".", 1)[0]
    return text


def normalize_compare_value(value: object) -> str:
    if is_unmapped_target(value) or is_empty_value(value):
        return ""
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if pd.isna(value):
            return ""
        if value.is_integer():
            return str(int(value))
        return format(value, "g")
    return str(value).strip()


def values_differ(left: object, right: object) -> bool:
    return normalize_compare_value(left) != normalize_compare_value(right)


__all__ = [
    "CODE_PATTERN",
    "CSV_ENCODINGS",
    "DISPLAY_NAME_OVERRIDES",
    "DUPLICATE_STRATEGIES",
    "EMAIL_PATTERN",
    "EMPTY_TEXT_VALUES",
    "FILTER_OPERATORS",
    "HEADER_ALIASES",
    "HEX_ADDRESS_PATTERN",
    "INTERNAL_APPEND_ORDER",
    "INTERNAL_COLUMNS",
    "INTERNAL_SOURCE_FILE",
    "INTERNAL_SOURCE_ROLE",
    "INTERNAL_SOURCE_SHEET",
    "LEGACY_DUPLICATE_STRATEGY_ALIASES",
    "LONG_TOKEN_PATTERN",
    "LOWER_SECRET_PATTERN",
    "PHONE_PATTERN",
    "SHORT_TOKEN_PATTERN",
    "SPREADSHEET_MAIN_NS",
    "STYLES_XML_PATH",
    "SUPPORTED_FILE_TYPES",
    "UNMAPPED_TARGET",
    "UPDATE_MODES",
    "UPPER_SECRET_PATTERN",
    "WEAK_KINDS",
    "canonical_internal_column_name",
    "default_display_name",
    "default_visible",
    "header_alias_key",
    "is_empty_series",
    "is_empty_value",
    "is_unmapped_target",
    "normalize_compare_value",
    "normalize_duplicate_strategy",
    "normalize_header_text",
    "normalize_key_value",
    "values_differ",
]
