"""数据补全领域服务 — 生成「补全后明细数据集」，补全值与来源证据同时落库。

数据工程核心：数据聚合计算与指标产出 — 跨模块数据产物

关键口径（处理项寅）：
- 读取采集记录 + 当前补全二次校验相关的**已生效**补全规则，解析每个空字段的补全来源；
- 补全值与来源证据（来源、来源版本、匹配键/匹配值、候选来源、发生时间）同时写入
  ``store.enrich_details`` 明细，**不再直接改写采集记录原字段**，避免与页面采集值混在一起；
- 同一字段只发布一个当前值（按 record_id + field_name 幂等 upsert），匹配键为空则跳过；
- 两个外部来源对同一空字段给出不同值时，按冲突配置处理：
  ``latest`` 取来源更新时间较新值，``authority`` 取权威来源指定值，两种方式都只发布一个当前值；
- 重复提交按既有版本口径覆盖更新，处理台账与结果清单联动，待办同步给另一业务入口。
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 两个外部数据源（模拟企业内常见的主数据中心与缓存库）
EXTERNAL_SOURCES = ("企业主数据中心(MDM)", "外部缓存库")
SOURCE_UPDATE_TIME = {
    "企业主数据中心(MDM)": "2026-11-17T08:00:00+00:00",
    "外部缓存库": "2026-11-15T08:00:00+00:00",
}


class DataEnrichService(BaseModuleService):
    MODULE = "data_enrich"
    ENTITY = "补全规则"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["规则名称", "目标字段", "补全来源", "匹配键", "补全方式", "缺失处理", "数据时效"]
    REQUIRED_FIELDS = ["规则名称", "目标字段", "补全来源"]
    STATUS_ORDER = ["草稿", "已生效", "已停用"]
    ACTION_RULES = {"启用规则": "草稿", "停用规则": "已生效"}
    NEGATIVE_ACTIONS = ["停用规则"]

    # ---- 补全后明细数据集生成 ----

    def enrich_batch(self, batch_ref: str) -> dict[str, Any]:
        """对指定批次执行补全，生成带来源证据的补全后明细数据集。

        流程：
        1. 读取批次采集记录与全部已生效补全规则；
        2. 逐条规则解析空字段，校验匹配键非空；
        3. 从外部数据源解析候选来源（含来源版本与来源更新时间）；
        4. 按冲突策略选出唯一当前补全值，补全值与来源证据同时写入明细；
        5. 写处理台账/结果清单/待办，并发布 ``rpa.enrich_completed`` 通道事件。

        Returns:
            批次处理摘要（enriched/skipped/missing_key/detail_ids 等）
        """
        started_at = datetime.now(timezone.utc).isoformat()
        records = store.collect_records.list_by_batch(batch_ref)
        active_rules = self._get_active_rules()
        config = store.enrich_details.config_snapshot()

        enriched = 0
        skipped = 0
        missing_key = 0
        detail_ids: list[int] = []
        touched_versions: set[str] = set()
        touched_fields: set[str] = set()

        for record in records:
            fields = record.fields
            for rule in active_rules:
                field_name = rule["field"]
                # 仅补全页面采集为空的字段；已有采集值保持原值，不与补全值混用
                if field_name in fields and str(fields.get(field_name, "")).strip():
                    skipped += 1
                    continue

                match_key = rule["match_key"]
                match_value = str(fields.get(match_key, "")).strip()
                if not match_key.strip() or not match_value:
                    # 匹配键不能为空：无法建立来源证据，跳过并计数
                    missing_key += 1
                    continue

                candidates = self._resolve_candidates(rule, match_value, record.entry_id)
                if not candidates:
                    missing_key += 1
                    continue

                chosen = self._choose_candidate(candidates, config)
                source_version = self._source_version(chosen["source"], chosen["value"])
                detail = store.enrich_details.upsert_detail({
                    "batch_ref": batch_ref,
                    "record_id": record.entry_id,
                    "field_name": field_name,
                    "current_value": chosen["value"],
                    "source": chosen["source"],
                    "source_version": source_version,
                    "match_key": match_key,
                    "match_value": match_value,
                    "rule_id": rule["id"],
                    "rule_name": rule["name"],
                    "conflict_policy": config["conflict_policy"],
                    "enriched_at": started_at,
                    "candidates": candidates,
                    "original_empty": True,
                })
                detail_ids.append(detail.detail_id)
                enriched += 1
                touched_fields.add(field_name)
                touched_versions.add(source_version)

        result = store.enrich_details.publish_result({
            "batch_ref": batch_ref,
            "process_type": "enrich",
            "enriched": enriched,
            "skipped": skipped,
            "missing_key": missing_key,
            "detail_count": len(detail_ids),
            "rules_applied": len(active_rules),
            "conflict_policy": config["conflict_policy"],
            "source_versions": sorted(touched_versions),
            "processed_at": started_at,
        })
        store.enrich_details.add_ledger({
            "batch_ref": batch_ref,
            "process_type": "enrich",
            "action": "生成补全后明细数据集",
            "enriched": enriched,
            "skipped": skipped,
            "missing_key": missing_key,
            "processed_at": started_at,
        })
        if enriched:
            # 另一个业务入口（结果校验）跟着更新待办
            store.enrich_details.upsert_todo({
                "batch_ref": batch_ref,
                "todo_type": "recheck",
                "title": f"批次 {batch_ref} 补全后明细待二次校验",
                "status": "待处理",
                "created_at": started_at,
            })
            store.channel.publish(
                topic="rpa.enrich_completed",
                source=self.MODULE,
                payload={
                    "batch_ref": batch_ref,
                    "enriched": enriched,
                    "missing_key": missing_key,
                    "detail_ids": detail_ids,
                    "fields": sorted(touched_fields),
                    "timestamp": started_at,
                },
            )

        return {
            "batch_ref": batch_ref,
            "enriched": enriched,
            "skipped": skipped,
            "missing_key": missing_key,
            "detail_ids": detail_ids,
            "result": result,
        }

    def enrich_records(self, batch_ref: str) -> dict[str, Any]:
        """兼容旧入口：执行补全并返回原口径摘要（内部已改为写明细、不改原字段）。"""
        outcome = self.enrich_batch(batch_ref)
        return {
            "batch_ref": batch_ref,
            "enriched": outcome["enriched"],
            "skipped": outcome["skipped"],
        }

    # ---- 明细查询 / 导出 / 统计（补全工作台） ----

    def list_enrich_details(
        self,
        *,
        batch_ref: str | None = None,
        field_name: str | None = None,
        source_version: str | None = None,
        source: str | None = None,
        recheck_status: str | None = None,
        keyword: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        return store.enrich_details.list_details(
            batch_ref=batch_ref, field_name=field_name,
            source_version=source_version, source=source,
            recheck_status=recheck_status, keyword=keyword, page=page, size=size)

    def workbench_facets(self) -> dict[str, Any]:
        """工作台筛选项：批次、字段、来源版本及冲突配置。"""
        return {
            "batches": store.enrich_details.distinct_batches(),
            "fields": store.enrich_details.distinct_fields(),
            "source_versions": store.enrich_details.distinct_versions(),
            "config": store.enrich_details.config_snapshot(),
        }

    def update_conflict_config(self, conflict_policy: str | None = None,
                               authority_source: str | None = None) -> dict[str, Any]:
        return store.enrich_details.update_config(conflict_policy, authority_source)

    def workbench_stats(self) -> dict[str, Any]:
        return store.enrich_details.snapshot()

    # ---- 外部数据源解析（模拟） ----

    def _get_active_rules(self) -> list[dict[str, Any]]:
        rows = store.rows(self.MODULE)
        return [
            {"id": r.get("id"), "name": r.get("规则名称"),
             "field": r.get("目标字段"), "source": r.get("补全来源"),
             "match_key": r.get("匹配键") or "", "method": r.get("补全方式")}
            for r in rows if r.get("status") == "已生效"
        ]

    def _resolve_candidates(
        self,
        rule: dict[str, Any],
        match_value: str,
        record_id: int,
    ) -> list[dict[str, Any]]:
        """从两个外部数据源解析候选补全值，返回带来源版本/更新时间的候选来源列表。

        候选值按 (记录, 字段, 来源) 确定性生成，保证测试可复现；
        两个来源约 3/4 概率给出不同值，用于演示冲突处理。
        """
        candidates: list[dict[str, Any]] = []
        for source in EXTERNAL_SOURCES:
            value = self._generate_fill_value(rule["field"], source, match_value, record_id)
            candidates.append({
                "source": source,
                "source_version": self._source_version(source, value),
                "value": value,
                "match_key": rule["match_key"],
                "source_updated_at": SOURCE_UPDATE_TIME[source],
            })
        return candidates

    @staticmethod
    def _generate_fill_value(field: str, source: str,
                             match_value: str, record_id: int) -> str:
        """根据字段类型与匹配键确定性生成模拟外部补全值（不同来源给出不同值）。"""
        digest = int(hashlib.md5(
            f"{record_id}|{field}|{source}|{match_value}".encode()).hexdigest(), 16)
        source_tag = "MDM" if "MDM" in source else "缓存"
        if field == "行业分类":
            options = ["软件和信息技术服务业", "计算机通信和其他电子设备制造业",
                       "批发业", "专用设备制造业"]
            return options[digest % len(options)]
        if field == "注册地址":
            districts = ["西湖区文三路", "浦东新区临港大道", "南山区科技园", "经开区启航路"]
            return f"杭州市{districts[digest % len(districts)]}{(digest % 99) + 1}号（{source_tag}）"
        if field == "规格型号":
            return f"{source_tag}-{(digest % 9000) + 1000}/{field[:2]}"
        if field == "生产厂家":
            makers = ["杭州云栖制造有限公司", "上海临港装备股份有限公司",
                      "深圳前海精密制造有限公司"]
            return f"{makers[digest % len(makers)]}（{source_tag}口径）"
        if field == "联系电话":
            return f"0571-8{digest % 10}{(digest // 10) % 10}{(digest // 100) % 10:02d}"
        return f"{source_tag}-{field}-{(digest % 900) + 100}"

    @staticmethod
    def _source_version(source: str, value: str) -> str:
        """来源版本：来源标识 + 值内容指纹，保证同一来源同一值版本稳定（沿用既有版本口径）。"""
        fingerprint = hashlib.md5(f"{source}|{value}".encode()).hexdigest()[:8]
        prefix = "mdm" if "MDM" in source else "cache"
        return f"{prefix}-v{fingerprint}"

    def _choose_candidate(
        self,
        candidates: list[dict[str, Any]],
        config: dict[str, Any],
    ) -> dict[str, Any]:
        """按冲突策略选出唯一当前补全值。

        - latest：两个来源值不同（冲突）时取来源更新时间较新者；
        - authority：取配置的权威来源指定值，权威来源缺失时回退较新值。
        两种方式都只发布一个当前值。
        """
        if len(candidates) == 1:
            return candidates[0]
        values = {c["value"] for c in candidates}
        if len(values) == 1:
            return candidates[0]  # 来源值一致，无冲突

        if config.get("conflict_policy") == "authority":
            authority = config.get("authority_source", "")
            for candidate in candidates:
                if candidate["source"] == authority:
                    return candidate
        # latest 或权威来源缺失回退：按来源更新时间取较新值
        return max(candidates, key=lambda c: c["source_updated_at"])

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        return {
            **base,
            "active_rules": sum(1 for r in rows if r.get("status") == "已生效"),
            "stopped_rules": sum(1 for r in rows if r.get("status") == "已停用"),
            "enrich_detail": store.enrich_details.snapshot(),
        }
