from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

from .models import SourceSelection
from .processor import SUPPORTED_FILE_TYPES

PAIR_INCOMPLETE = "pair_incomplete"
MISSING_OLD = "missing_old"
MULTIPLE_OLD = "multiple_old"
OK = "ok"


@dataclass(slots=True)
class SourceScopeDecision:
    sources: dict[str, SourceSelection] | None
    reason: str = OK


@dataclass(slots=True)
class WritebackTargetDecision:
    source: SourceSelection | None
    reason: str = OK


def expand_input_paths(raw_paths: Iterable[str | Path]) -> list[Path]:
    output: list[Path] = []
    seen: set[str] = set()

    for raw_path in raw_paths:
        path = Path(str(raw_path).strip().strip('"'))
        if not path.exists():
            continue

        if path.is_dir():
            for child in sorted(path.iterdir()):
                if child.is_file() and child.suffix.lower() in SUPPORTED_FILE_TYPES:
                    append_unique_path(output, seen, child)
            continue

        if path.is_file() and path.suffix.lower() in SUPPORTED_FILE_TYPES:
            append_unique_path(output, seen, path)

    return output


def append_unique_path(output: list[Path], seen: set[str], path: Path) -> None:
    marker = str(path.resolve()).lower()
    if marker in seen:
        return
    seen.add(marker)
    output.append(path)


def get_column_scope_sources(
    sources: Mapping[str, SourceSelection],
    active_sheet_source_ids: Mapping[str, str | None],
) -> dict[str, SourceSelection]:
    active_old_id = active_sheet_source_ids.get("old")
    active_new_id = active_sheet_source_ids.get("new")
    if active_old_id and active_old_id in sources:
        scoped: dict[str, SourceSelection] = {active_old_id: sources[active_old_id]}
        if active_new_id and active_new_id in sources:
            scoped[active_new_id] = sources[active_new_id]
        return scoped
    return dict(sources)


def resolve_processing_scope_sources(
    sources: Mapping[str, SourceSelection],
    active_sheet_source_ids: Mapping[str, str | None],
) -> SourceScopeDecision:
    active_old_id = active_sheet_source_ids.get("old")
    active_new_id = active_sheet_source_ids.get("new")
    if not active_old_id and not active_new_id:
        return SourceScopeDecision(dict(sources))
    if not active_old_id or not active_new_id:
        return SourceScopeDecision(None, PAIR_INCOMPLETE)
    if active_old_id not in sources or active_new_id not in sources:
        return SourceScopeDecision(dict(sources))
    return SourceScopeDecision(
        {
            active_old_id: sources[active_old_id],
            active_new_id: sources[active_new_id],
        }
    )


def get_mapping_scope_sources(
    all_sources: Mapping[str, SourceSelection],
    active_sheet_source_ids: Mapping[str, str | None],
    sources_to_map: list[SourceSelection],
) -> dict[str, SourceSelection]:
    scoped: dict[str, SourceSelection] = {}
    active_old_id = active_sheet_source_ids.get("old")
    if active_old_id and active_old_id in all_sources:
        scoped[active_old_id] = all_sources[active_old_id]
    else:
        for source in all_sources.values():
            if source.dataset_role == "old" and source.enabled:
                scoped[source.source_id] = source
    for source in sources_to_map:
        if source.source_id in all_sources:
            scoped[source.source_id] = all_sources[source.source_id]
    return scoped


def get_last_processed_scope_sources(
    sources: Mapping[str, SourceSelection],
    last_processed_scope_source_ids: set[str],
) -> dict[str, SourceSelection]:
    if not last_processed_scope_source_ids:
        return {}
    return {
        source_id: sources[source_id]
        for source_id in last_processed_scope_source_ids
        if source_id in sources
    }


def resolve_writeback_target_source(
    scoped_sources: Mapping[str, SourceSelection],
    active_old_id: str | None,
    all_sources: Mapping[str, SourceSelection],
) -> WritebackTargetDecision:
    old_sources = [
        source
        for source in scoped_sources.values()
        if source.dataset_role == "old" and source.enabled
    ]
    if not old_sources:
        return WritebackTargetDecision(None, MISSING_OLD)

    if active_old_id and active_old_id in all_sources:
        active_source = all_sources[active_old_id]
        if any(source.source_id == active_source.source_id for source in old_sources):
            return WritebackTargetDecision(active_source)

    if len(old_sources) == 1:
        return WritebackTargetDecision(old_sources[0])

    return WritebackTargetDecision(None, MULTIPLE_OLD)
