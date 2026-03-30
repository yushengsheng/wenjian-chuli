from __future__ import annotations

from typing import Iterable

import pandas as pd

from .models import SourceSelection
from .processor_common import (
    CODE_PATTERN,
    EMAIL_PATTERN,
    HEX_ADDRESS_PATTERN,
    LONG_TOKEN_PATTERN,
    LOWER_SECRET_PATTERN,
    PHONE_PATTERN,
    SHORT_TOKEN_PATTERN,
    UPPER_SECRET_PATTERN,
    UNMAPPED_TARGET,
    WEAK_KINDS,
    canonical_internal_column_name,
    header_alias_key,
    is_empty_value,
    normalize_header_text,
)


def collect_available_columns(
    sources: dict[str, SourceSelection],
    include_internal: bool = True,
) -> list[str]:
    old_columns = collect_target_columns(sources)
    if old_columns:
        columns = list(old_columns)
    else:
        columns = []
        seen: set[str] = set()
        for source in sources.values():
            if not source.enabled:
                continue
            for column in source.columns:
                if canonical_internal_column_name(column) is not None:
                    continue
                if column in seen:
                    continue
                seen.add(column)
                columns.append(column)

    if include_internal:
        from .processor_common import INTERNAL_COLUMNS

        for column in INTERNAL_COLUMNS:
            if column not in columns:
                columns.append(column)

    return columns


def collect_target_columns(sources: dict[str, SourceSelection]) -> list[str]:
    columns: list[str] = []
    seen: set[str] = set()
    for source in sources.values():
        if not source.enabled or source.dataset_role != "old":
            continue
        for column in source.columns:
            if canonical_internal_column_name(column) is not None:
                continue
            if column in seen:
                continue
            seen.add(column)
            columns.append(column)
    return columns


def build_target_profiles(
    sources: dict[str, SourceSelection],
    cache: dict[str, pd.DataFrame],
    target_columns: list[str],
) -> dict[str, dict[str, object]]:
    profiles: dict[str, dict[str, object]] = {}
    for index, column in enumerate(target_columns):
        samples: list[object] = []
        for source in sources.values():
            if not source.enabled or source.dataset_role != "old":
                continue
            frame = cache[source.source_id]
            if column in frame.columns:
                series = frame[column].dropna().tolist()
                samples.extend(series[:20])
        profiles[column] = {
            "kind": infer_target_kind(column, samples),
            "index": index,
        }
    return profiles


def suggest_source_to_target_mapping(
    dataframe: pd.DataFrame,
    target_columns: list[str],
    target_profiles: dict[str, dict[str, object]],
) -> dict[str, str]:
    _ = target_profiles
    return build_direct_source_to_target_mapping(dataframe.columns, target_columns)


def build_direct_source_to_target_mapping(
    source_columns: Iterable[object],
    target_columns: list[str],
) -> dict[str, str]:
    direct_mapping = build_direct_mapping(source_columns, target_columns)
    return {source_column: target_column for target_column, source_column in direct_mapping.items()}


def is_direct_header_match_complete(
    source_columns: Iterable[object],
    target_columns: list[str],
) -> tuple[bool, dict[str, str]]:
    source_column_list = [str(column) for column in source_columns]
    direct_mapping = build_direct_source_to_target_mapping(source_column_list, target_columns)
    return len(direct_mapping) == len(source_column_list), direct_mapping


def suggest_target_to_source_mapping(
    dataframe: pd.DataFrame,
    target_columns: list[str],
    target_profiles: dict[str, dict[str, object]],
    excluded_sources: set[str] | None = None,
    excluded_targets: set[str] | None = None,
) -> dict[str, str]:
    excluded_sources = excluded_sources or set()
    excluded_targets = excluded_targets or set()

    mapping = build_direct_mapping(
        [column for column in dataframe.columns if column not in excluded_sources],
        [column for column in target_columns if column not in excluded_targets],
    )
    used_sources = set(mapping.values()) | set(excluded_sources)
    source_profiles = {
        column: {
            "kind": infer_source_column_kind(dataframe[column]),
            "index": index,
        }
        for index, column in enumerate(dataframe.columns)
        if column not in used_sources
    }

    for target_column in target_columns:
        if target_column in excluded_targets or target_column in mapping:
            continue
        target_kind = target_profiles.get(target_column, {}).get("kind", "unknown")
        candidate = choose_best_source_column(target_column, target_kind, source_profiles)
        if candidate is None:
            continue
        mapping[target_column] = candidate
        source_profiles.pop(candidate, None)
    return mapping


def align_dataframe_to_target(
    dataframe: pd.DataFrame,
    target_columns: list[str],
    target_profiles: dict[str, dict[str, object]] | None = None,
    manual_source_mapping: dict[str, str] | None = None,
) -> pd.DataFrame:
    _ = target_profiles
    if dataframe.empty:
        return pd.DataFrame(columns=target_columns)

    aligned = pd.DataFrame(index=dataframe.index)
    assigned_targets: set[str] = set()

    if manual_source_mapping:
        for source_column, target_column in manual_source_mapping.items():
            if source_column not in dataframe.columns:
                continue
            if not target_column or target_column not in target_columns or target_column in assigned_targets:
                continue
            aligned[target_column] = dataframe[source_column]
            assigned_targets.add(target_column)
    else:
        direct_mapping = build_direct_mapping(dataframe.columns, target_columns)
        for target_column, source_column in direct_mapping.items():
            aligned[target_column] = dataframe[source_column]
            assigned_targets.add(target_column)

    for target_column in target_columns:
        if target_column not in aligned.columns:
            aligned[target_column] = UNMAPPED_TARGET

    return aligned.reindex(columns=target_columns)


def build_direct_mapping(source_columns: Iterable[object], target_columns: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    used_sources: set[str] = set()
    target_by_name = {
        target_name: column
        for column in target_columns
        if (target_name := str(column).strip())
    }

    for source_column in source_columns:
        source_text = str(source_column)
        source_name = source_text.strip()
        if not source_name:
            continue
        target_column = target_by_name.get(source_name)
        if target_column is None or target_column in mapping or source_text in used_sources:
            continue
        mapping[target_column] = source_text
        used_sources.add(source_text)
    return mapping


def choose_best_source_column(
    target_column: str,
    target_kind: str,
    source_profiles: dict[str, dict[str, object]],
) -> str | None:
    if target_kind in WEAK_KINDS:
        return None

    candidates: list[tuple[int, int, str]] = []
    for source_column, profile in source_profiles.items():
        source_kind = profile["kind"]
        if not kinds_are_compatible(target_kind, source_kind):
            continue
        penalty = abs(int(profile["index"]))
        bonus = 0 if source_kind == target_kind else 1
        candidates.append((bonus, penalty, source_column))

    if not candidates:
        return None

    candidates.sort()
    return candidates[0][2]


def kinds_are_compatible(target_kind: str, source_kind: str) -> bool:
    if target_kind == source_kind:
        return True
    compatible_groups = [
        {"long_token"},
        {"wallet_address"},
        {"email"},
        {"phone"},
        {"otp_lower"},
        {"otp_or_oauth", "otp_lower", "long_token"},
        {"otp_upper"},
        {"short_token", "password_like"},
    ]
    for group in compatible_groups:
        if target_kind in group and source_kind in group:
            return True
    return False


def infer_target_kind(column_name: str, samples: list[object]) -> str:
    alias = header_alias_key(column_name)
    normalized_name = normalize_header_text(str(column_name))
    if alias == "邮箱":
        return "email"
    if alias == "手机号":
        return "phone"
    if alias == "邮箱密码":
        return "password_like"
    if alias == "邮箱2fa":
        if "oauth" in normalized_name or "oath" in normalized_name:
            return "otp_or_oauth"
        return "otp_lower"
    if alias == "币安2fa":
        return "otp_upper"
    if alias == "币安充值地址":
        return "wallet_address"
    if alias == "特殊备注":
        return "text"
    if alias in {"apikey", "apisecret"}:
        return "long_token"
    return infer_values_kind(samples)


def infer_source_column_kind(series: pd.Series) -> str:
    values = series.dropna().tolist()[:30]
    return infer_values_kind(values)


def infer_values_kind(values: list[object]) -> str:
    counts: dict[str, int] = {}
    for value in values:
        kind = infer_value_kind(value)
        counts[kind] = counts.get(kind, 0) + 1
    if not counts:
        return "empty"
    return max(counts.items(), key=lambda item: item[1])[0]


def infer_value_kind(value: object) -> str:
    if is_empty_value(value):
        return "empty"
    text = str(value).strip()
    if EMAIL_PATTERN.fullmatch(text):
        return "email"
    if looks_like_phone(text):
        return "phone"
    if HEX_ADDRESS_PATTERN.fullmatch(text):
        return "wallet_address"
    if UPPER_SECRET_PATTERN.fullmatch(text):
        return "otp_upper"
    if LOWER_SECRET_PATTERN.fullmatch(text):
        return "otp_lower"
    if LONG_TOKEN_PATTERN.fullmatch(text):
        return "long_token"
    if SHORT_TOKEN_PATTERN.fullmatch(text):
        return "password_like"
    if CODE_PATTERN.fullmatch(text):
        return "code"
    if looks_like_number(text):
        return "number"
    return "text"


def looks_like_number(text: str) -> bool:
    try:
        float(text)
        return True
    except ValueError:
        return False


def looks_like_phone(text: str) -> bool:
    digits = "".join(character for character in text if character.isdigit())
    if len(digits) < 7 or len(digits) > 15:
        return False
    if EMAIL_PATTERN.fullmatch(text) or HEX_ADDRESS_PATTERN.fullmatch(text):
        return False
    if PHONE_PATTERN.fullmatch(text):
        return True
    return text.isdigit() and len(digits) >= 8


__all__ = [
    "align_dataframe_to_target",
    "build_direct_mapping",
    "build_direct_source_to_target_mapping",
    "build_target_profiles",
    "choose_best_source_column",
    "collect_available_columns",
    "collect_target_columns",
    "infer_source_column_kind",
    "infer_target_kind",
    "infer_value_kind",
    "infer_values_kind",
    "is_direct_header_match_complete",
    "kinds_are_compatible",
    "looks_like_number",
    "looks_like_phone",
    "suggest_source_to_target_mapping",
    "suggest_target_to_source_mapping",
]
