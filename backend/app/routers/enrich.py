"""补全工作台路由：补全后明细数据集的查询、执行、二次校验与导出。

与通用资源路由（/api/data_enrich，管补全规则 CRUD）分开：
这里面向「补全后明细」这一跨模块数据产物，路径前缀 /api/enrich。
"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.services.data_enrich import DataEnrichService
from app.services.result_verify import ResultVerifyService
from app.store import store

router = APIRouter(prefix="/api/enrich", tags=["补全工作台"])

DETAIL_COLUMNS = [
    ("detail_id", "明细ID"),
    ("batch_ref", "批次编号"),
    ("record_id", "采集记录ID"),
    ("field_name", "补全字段"),
    ("current_value", "当前补全值"),
    ("source", "补全来源"),
    ("source_version", "来源版本"),
    ("match_key", "匹配键"),
    ("match_value", "匹配值"),
    ("rule_name", "补全规则"),
    ("conflict_policy", "冲突策略"),
    ("enriched_at", "补全发生时间"),
    ("recheck_status", "二次校验状态"),
    ("recheck_reason", "二次校验说明"),
    ("recheck_source_version", "校验来源版本"),
    ("recheck_at", "二次校验时间"),
]


class BatchPayload(BaseModel):
    batch_ref: str


class ConflictConfigPayload(BaseModel):
    conflict_policy: str | None = None
    authority_source: str | None = None


def _enrich_service() -> DataEnrichService:
    from app.services import service_for
    service = service_for("data_enrich")
    assert isinstance(service, DataEnrichService)
    return service


@router.get("/details")
def list_details(
    batch_ref: str | None = Query(default=None),
    field_name: str | None = Query(default=None),
    source_version: str | None = Query(default=None),
    source: str | None = Query(default=None),
    recheck_status: str | None = Query(default=None),
    keyword: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> dict[str, Any]:
    """补全后明细分页查询：支持按批次、字段、来源版本、来源、校验状态筛选。"""
    service = _enrich_service()
    items, total = service.list_enrich_details(
        batch_ref=batch_ref, field_name=field_name,
        source_version=source_version, source=source,
        recheck_status=recheck_status, keyword=keyword, page=page, size=size)
    return {"items": items, "total": total, "page": page, "size": size}


@router.get("/facets")
def facets() -> dict[str, Any]:
    """工作台筛选维度与冲突处理配置。"""
    return _enrich_service().workbench_facets()


@router.get("/stats")
def stats() -> dict[str, Any]:
    """补全工作台统计卡片数据。"""
    return _enrich_service().workbench_stats()


@router.post("/run")
def run_enrich(payload: BatchPayload) -> dict[str, Any]:
    """按批次生成补全后明细数据集（补全值与来源证据同时写入）。"""
    if not payload.batch_ref.strip():
        raise HTTPException(status_code=400, detail="batch_ref 不能为空")
    records = store.collect_records.list_by_batch(payload.batch_ref.strip())
    if not records:
        raise HTTPException(
            status_code=404,
            detail=f"批次 {payload.batch_ref} 无采集记录，无法执行补全")
    return _enrich_service().enrich_batch(payload.batch_ref.strip())


@router.post("/recheck")
def run_recheck(payload: BatchPayload) -> dict[str, Any]:
    """结果校验模块加载补全后明细，按补全来源版本重新校验受影响字段。"""
    service = ResultVerifyService()
    return service.recheck_enriched_details(payload.batch_ref.strip() or None)


@router.get("/recheck/stats")
def recheck_stats() -> dict[str, Any]:
    """二次校验结果视图（供概览看板展示）。"""
    return ResultVerifyService().get_enrich_recheck_stats()


@router.get("/config")
def get_config() -> dict[str, Any]:
    return store.enrich_details.config_snapshot()


@router.post("/config")
def update_config(payload: ConflictConfigPayload) -> dict[str, Any]:
    """配置冲突处理方式：latest=较新值，authority=权威来源指定值。"""
    if payload.conflict_policy and payload.conflict_policy not in ("latest", "authority"):
        raise HTTPException(status_code=400, detail="conflict_policy 仅支持 latest/authority")
    return store.enrich_details.update_config(
        payload.conflict_policy, payload.authority_source)


@router.get("/ledgers")
def ledgers(batch_ref: str | None = Query(default=None)) -> dict[str, Any]:
    """处理台账：每次补全/二次校验处理动作留痕。"""
    rows = store.enrich_details.list_ledgers(batch_ref)
    return {"items": rows, "total": len(rows)}


@router.get("/results")
def results() -> dict[str, Any]:
    """结果清单：同一(批次,处理类型)重复提交只保留一条结果。"""
    rows = store.enrich_details.list_results()
    return {"items": rows, "total": len(rows)}


@router.get("/todos")
def todos(include_done: bool = Query(default=False)) -> dict[str, Any]:
    """待办：另一业务入口（结果校验）联动更新。"""
    rows = store.enrich_details.list_todos(include_done=include_done)
    return {"items": rows, "total": len(rows)}


@router.get("/cross-check")
def cross_check() -> dict[str, Any]:
    """处理台账与结果清单联动核对结果。"""
    return store.enrich_details.cross_check()


@router.get("/export")
def export_details(
    batch_ref: str | None = Query(default=None),
    field_name: str | None = Query(default=None),
    source_version: str | None = Query(default=None),
    source: str | None = Query(default=None),
    recheck_status: str | None = Query(default=None),
) -> StreamingResponse:
    """导出完整补全明细（含来源证据、候选来源、校验结论）为 CSV。"""
    service = _enrich_service()
    items, _ = service.list_enrich_details(
        batch_ref=batch_ref, field_name=field_name,
        source_version=source_version, source=source,
        recheck_status=recheck_status, page=1, size=1_000_000)

    output = io.StringIO()
    output.write("﻿")
    writer = csv.writer(output, lineterminator="\n")
    headers = [label for _, label in DETAIL_COLUMNS] + ["候选来源"]
    writer.writerow(headers)
    for item in items:
        row = [item.get(key, "") for key, _ in DETAIL_COLUMNS]
        candidate_text = " | ".join(
            f"{c.get('source')}@{c.get('source_version')}={c.get('value')}"
            f"(更新于{c.get('source_updated_at')})"
            for c in item.get("candidates", [])
        )
        row.append(candidate_text)
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in row
        ])

    filename = "enrich_details.csv"
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
