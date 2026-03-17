from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

import pandas as pd

from .models import SourceSelection
from .processor import (
    collect_target_columns,
    combine_enabled_sources,
    is_direct_header_match_complete,
)

OK = "ok"
NO_DATA = "no_data"
MISSING_OLD = "missing_old"


@dataclass(slots=True)
class ImportApplication:
    sources: dict[str, SourceSelection]
    cache: dict[str, pd.DataFrame]
    imported_sources: list[SourceSelection]
    first_source: SourceSelection | None
    summary_lines: list[str]
    status_text: str


@dataclass(slots=True)
class ProcessingPreparation:
    raw_dataframe: pd.DataFrame
    unmapped_new_sources: list[SourceSelection]
    reason: str = OK
    raw_dataframe_ready: bool = False


@dataclass(slots=True)
class MappingCandidate:
    source: SourceSelection
    dataframe: pd.DataFrame
    suggested_mapping: dict[str, str]
    direct_mapping: dict[str, str]
    auto_confirmed: bool
    can_auto_apply: bool


@dataclass(slots=True)
class MappingSession:
    target_columns: list[str]
    candidates: list[MappingCandidate]


def apply_imported_sources(
    existing_sources: Mapping[str, SourceSelection],
    existing_cache: Mapping[str, pd.DataFrame],
    imported_sources: list[SourceSelection],
    imported_cache: Mapping[str, pd.DataFrame],
    dataset_role: str,
    source_name: str,
    file_count: int,
) -> ImportApplication:
    sources = dict(existing_sources)
    cache = dict(existing_cache)

    for source in imported_sources:
        source.dataset_role = dataset_role
        sources[source.source_id] = source
        cache[source.source_id] = imported_cache[source.source_id]

    summary_lines = [
        f"{source_name}: 导入 {file_count} 个文件，生成 {len(imported_sources)} 个数据源。",
        "如果你的目标是用新数据更新老数据：把旧表导入到“老数据”，把增量表导入到“新数据”，选择账号/手机号等主键字段后点击“应用处理”。",
    ]
    return ImportApplication(
        sources=sources,
        cache=cache,
        imported_sources=imported_sources,
        first_source=imported_sources[0] if imported_sources else None,
        summary_lines=summary_lines,
        status_text=f"{source_name}完成：{len(imported_sources)} 个数据源",
    )


def prepare_processing(
    scoped_sources: Mapping[str, SourceSelection],
    data_cache: Mapping[str, pd.DataFrame],
) -> ProcessingPreparation:
    enabled_sources = [source for source in scoped_sources.values() if source.enabled]
    if not enabled_sources:
        return ProcessingPreparation(raw_dataframe=pd.DataFrame(), unmapped_new_sources=[], reason=NO_DATA)

    has_enabled_old = any(source.dataset_role == "old" for source in enabled_sources)
    if not has_enabled_old:
        has_any_enabled_data = any(
            not data_cache.get(source.source_id, pd.DataFrame()).empty
            for source in enabled_sources
        )
        reason = MISSING_OLD if has_any_enabled_data else NO_DATA
        return ProcessingPreparation(raw_dataframe=pd.DataFrame(), unmapped_new_sources=[], reason=reason)

    unmapped_new_sources = [
        source
        for source in enabled_sources
        if source.dataset_role == "new" and not source.mapping_confirmed
    ]
    if unmapped_new_sources:
        return ProcessingPreparation(
            raw_dataframe=pd.DataFrame(),
            unmapped_new_sources=unmapped_new_sources,
            reason=OK,
        )

    raw_dataframe = combine_enabled_sources(dict(scoped_sources), dict(data_cache))
    if raw_dataframe.empty:
        return ProcessingPreparation(
            raw_dataframe=raw_dataframe,
            unmapped_new_sources=[],
            reason=NO_DATA,
            raw_dataframe_ready=True,
        )
    return ProcessingPreparation(
        raw_dataframe=raw_dataframe,
        unmapped_new_sources=unmapped_new_sources,
        reason=OK,
        raw_dataframe_ready=True,
    )


def build_mapping_session(
    context_sources: Mapping[str, SourceSelection],
    data_cache: Mapping[str, pd.DataFrame],
    sources_to_map: list[SourceSelection],
) -> MappingSession:
    target_columns = collect_target_columns(dict(context_sources))
    if not target_columns:
        return MappingSession(target_columns=[], candidates=[])

    candidates: list[MappingCandidate] = []
    for source in sources_to_map:
        if source.dataset_role != "new":
            continue
        dataframe = data_cache.get(source.source_id)
        if dataframe is None:
            continue

        auto_confirmed, direct_mapping = is_direct_header_match_complete(dataframe.columns, target_columns)
        suggested_mapping = dict(direct_mapping)
        candidates.append(
            MappingCandidate(
                source=source,
                dataframe=dataframe,
                suggested_mapping=suggested_mapping,
                direct_mapping=direct_mapping,
                auto_confirmed=auto_confirmed,
                can_auto_apply=bool(direct_mapping) and auto_confirmed,
            )
        )
    return MappingSession(target_columns=target_columns, candidates=candidates)
