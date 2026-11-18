"""结果校验领域服务 — 校验采集结果并触发回调。

数据工程核心：数据校验与去重策略执行 — 跨模块数据产物

数据流:
    channel("rpa.data_collected") ← data_extract
    → result_verify 消费校验 + DirtyDataVault
    → channel("rpa.validation_complete") → callback_hook
"""
from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


def _safe_float(value: str, default: float = 1.0) -> float:
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


class ResultVerifyService(BaseModuleService):
    MODULE = "result_verify"
    ENTITY = "校验规则"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["规则名称", "校验字段", "校验条件", "期望模式", "异常动作", "抽样比例", "关联流程"]
    REQUIRED_FIELDS = ["规则名称", "校验字段", "校验条件"]
    STATUS_ORDER = ["草稿", "已启用", "已关闭"]
    ACTION_RULES = {"启用规则": "草稿", "关闭规则": "已启用"}
    NEGATIVE_ACTIONS = []

    # ---- 跨模块业务方法 ----

    def verify_collected_data(self) -> dict[str, Any]:
        """消费 data_extract 发布的采集结果，执行校验。

        从 channel("rpa.data_collected") 获取采集结果批次，
        按已启用的校验规则进行逐条校验。

        不通过的记录移入 DirtyDataVault。

        Returns:
            {"verified": int, "passed": int, "failed": int, "batches": [...]}
        """
        msgs = store.channel.consume(
            consumer=self.MODULE,
            topic="rpa.data_collected",
        )
        if not msgs:
            return {"verified": 0, "passed": 0, "failed": 0, "batches": []}

        batches = []
        total_passed = 0
        total_failed = 0

        for msg in msgs:
            batch_data = msg.payload
            batch_ref = batch_data.get("batch_ref", "")

            # 获取已启用的校验规则
            verify_rules = self._get_active_rules()
            if not verify_rules:
                store.channel.confirm(msg.msg_id, self.MODULE)
                continue

            # 对批次中的记录进行校验
            records = store.collect_records.list_by_batch(batch_ref)
            passed, failed = 0, 0

            for record_entry in records:
                quality = self._validate_record(
                    record_entry.fields, verify_rules)
                if quality["passed"]:
                    store.collect_records.mark_validated(
                        record_entry.entry_id, quality["score"])
                    passed += 1
                else:
                    failed += 1
                    # 移入脏数据区
                    store.dirty_vault.quarantine(
                        source=self.MODULE,
                        batch_ref=batch_ref,
                        raw_data=dict(record_entry.fields),
                        reason=quality["reason"],
                    )

            total_passed += passed
            total_failed += failed

            # 发布校验完成事件
            store.channel.publish(
                topic="rpa.validation_complete",
                source=self.MODULE,
                payload={
                    "batch_ref": batch_ref,
                    "site": batch_data.get("site"),
                    "passed": passed,
                    "failed": failed,
                    "rules_applied": len(verify_rules),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )
            store.channel.confirm(msg.msg_id, self.MODULE)

            batches.append({
                "batch_ref": batch_ref,
                "passed": passed,
                "failed": failed,
                "total": len(records),
            })

        return {
            "verified": total_passed + total_failed,
            "passed": total_passed,
            "failed": total_failed,
            "batches": batches,
        }

    def _get_active_rules(self) -> list[dict[str, Any]]:
        """获取已启用的校验规则。"""
        rows = store.rows(self.MODULE)
        return [
            {
                "id": r.get("id"),
                "name": r.get("规则名称"),
                "field": r.get("校验字段"),
                "condition": r.get("校验条件"),
                "pattern": r.get("期望模式", "non_empty"),
                "action_on_fail": r.get("异常动作", "quarantine"),
                "sample_rate": _safe_float(r.get("抽样比例", "1.0")),
                "bot_ref": r.get("关联流程", ""),
            }
            for r in rows if r.get("status") == "已启用"
        ]

    def _validate_record(
        self,
        fields: dict[str, Any],
        rules: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """按规则校验单条采集记录。

        Returns:
            {"passed": bool, "score": float, "reason": str}
        """
        if not fields:
            return {"passed": False, "score": 0.0, "reason": "空记录"}

        applied = 0
        failed = 0
        reasons = []

        for rule in rules:
            if random.random() > rule["sample_rate"]:
                continue  # 抽样跳过

            field_name = rule["field"]
            condition = rule["condition"]

            # 模拟校验逻辑
            if field_name in fields:
                value = fields[field_name]
                applied += 1

                if condition == "non_empty" and not str(value).strip():
                    failed += 1
                    reasons.append(f"字段[{field_name}]为空")
                elif condition == "numeric" and not str(value).replace(".", "").isdigit():
                    failed += 1
                    reasons.append(f"字段[{field_name}]不是数值")
                elif condition == "length_gt_0" and len(str(value)) == 0:
                    failed += 1
                    reasons.append(f"字段[{field_name}]长度为零")
                # 模拟额外场景
                elif random.random() < 0.08:
                    failed += 1
                    reasons.append(f"字段[{field_name}]格式异常")
            else:
                # 字段缺失
                failed += 1
                reasons.append(f"缺少字段[{field_name}]")

        if applied == 0:
            # 无匹配字段视为通过
            return {"passed": True, "score": 1.0, "reason": "无校验规则匹配"}

        passed = failed == 0
        score = 1.0 - (failed / max(applied, 1)) if applied > 0 else 0.0
        return {
            "passed": passed,
            "score": round(score, 2),
            "reason": "; ".join(reasons) if reasons else "通过",
        }

    def run_single_validate(
        self,
        record: dict[str, Any],
        rule_ids: list[int] | None = None,
    ) -> dict[str, Any]:
        """对单条记录执行指定规则校验。

        Args:
            record: 待校验记录
            rule_ids: 要使用的规则 ID 列表（None = 所有已启用）

        Returns:
            校验结果
        """
        all_rules = self._get_active_rules()
        if rule_ids:
            rules = [r for r in all_rules if r["id"] in rule_ids]
        else:
            rules = all_rules

        return self._validate_record(record, rules)

    # ---- 补全后明细二次校验（处理项寅口径，按补全来源版本） ----

    def recheck_enriched_details(self, batch_ref: str | None = None) -> dict[str, Any]:
        """加载处理项寅关联的补全后明细数据集，按补全来源版本重新校验受影响字段。

        - 数据来源：``store.enrich_details`` 的当前补全值（不重复采集、不改写原记录）；
        - 按 ``source_version`` 分组，沿用结果校验模块既有规则口径逐条校验受影响字段；
        - 校验结果（通过/不通过、原因、来源版本、发生时间）回写到同一条补全明细；
        - 同一批次重复提交只保留一条 recheck 结果（结果清单按 批次+处理类型 去重）；
        - 处理台账与结果清单联动核对，完成后关闭待办并发布 ``rpa.enrich_rechecked`` 事件。

        Returns:
            按来源版本汇总的二次校验结果
        """
        checked_at = datetime.now(timezone.utc).isoformat()
        details = store.enrich_details.list_current_raw(batch_ref)
        if not details:
            return {
                "batch_ref": batch_ref, "checked": 0, "passed": 0,
                "failed": 0, "by_source_version": {}, "batches": [],
            }

        verify_rules = self._get_active_rules()
        batches: list[str] = []
        total_checked = total_pass = total_fail = 0
        by_version: dict[str, dict[str, Any]] = {}

        for detail in details:
            target_batch = detail.batch_ref
            if target_batch not in batches:
                batches.append(target_batch)

            rule = self._match_verify_rule(detail.field_name, verify_rules)
            status, reason = self._recheck_one(detail, rule)

            store.enrich_details.apply_recheck(
                detail.detail_id, status, reason,
                detail.source_version, checked_at)

            bucket = by_version.setdefault(detail.source_version, {
                "source": detail.source,
                "source_version": detail.source_version,
                "checked": 0, "passed": 0, "failed": 0,
                "fields": set(),
            })
            bucket["checked"] += 1
            bucket["fields"].add(detail.field_name)
            total_checked += 1
            if status == "校验通过":
                bucket["passed"] += 1
                total_pass += 1
            else:
                bucket["failed"] += 1
                total_fail += 1

        # set 转为可序列化列表
        for bucket in by_version.values():
            bucket["fields"] = sorted(bucket["fields"])

        affected_batches = batches if batch_ref is None else [batch_ref]
        for target_batch in affected_batches:
            # 重复提交只保留一条结果
            store.enrich_details.publish_result({
                "batch_ref": target_batch,
                "process_type": "recheck",
                "checked": sum(
                    1 for d in details if d.batch_ref == target_batch),
                "passed": sum(
                    1 for d in details
                    if d.batch_ref == target_batch and d.recheck_status == "校验通过"),
                "failed": sum(
                    1 for d in details
                    if d.batch_ref == target_batch and d.recheck_status == "校验不通过"),
                "source_versions": sorted({
                    d.source_version for d in details if d.batch_ref == target_batch}),
                "processed_at": checked_at,
            })
            store.enrich_details.add_ledger({
                "batch_ref": target_batch,
                "process_type": "recheck",
                "action": "按补全来源版本二次校验受影响字段",
                "processed_at": checked_at,
            })
            # 处理台账与结果清单联动核对
            cross_check = store.enrich_details.cross_check()
            # 关闭另一业务入口的待办
            store.enrich_details.resolve_todo(target_batch, "recheck")
            store.channel.publish(
                topic="rpa.enrich_rechecked",
                source=self.MODULE,
                payload={
                    "batch_ref": target_batch,
                    "checked": sum(
                        1 for d in details if d.batch_ref == target_batch),
                    "passed": sum(
                        1 for d in details
                        if d.batch_ref == target_batch and d.recheck_status == "校验通过"),
                    "failed": sum(
                        1 for d in details
                        if d.batch_ref == target_batch and d.recheck_status == "校验不通过"),
                    "cross_check_consistent": cross_check["consistent"],
                    "timestamp": checked_at,
                },
            )

        return {
            "batch_ref": batch_ref,
            "checked": total_checked,
            "passed": total_pass,
            "failed": total_fail,
            "by_source_version": by_version,
            "batches": batches,
            "cross_check": store.enrich_details.cross_check(),
        }

    def _match_verify_rule(
        self,
        field_name: str,
        verify_rules: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """按校验字段匹配补全字段对应的已启用校验规则；无精确匹配时取非空兜底规则。"""
        for rule in verify_rules:
            if rule.get("field") == field_name:
                return rule
        for rule in verify_rules:
            if rule.get("pattern") == "non_empty":
                return rule
        return verify_rules[0] if verify_rules else None

    @staticmethod
    def _recheck_one(
        detail: Any,
        rule: dict[str, Any] | None,
    ) -> tuple[str, str]:
        """按单条校验规则口径校验一个补全值（确定性，不使用随机抽样）。

        Returns:
            (校验状态, 原因说明)
        """
        value = str(detail.current_value or "").strip()
        if not value:
            return "校验不通过", f"字段[{detail.field_name}]补全值为空"
        if rule is None:
            # 无可用校验规则：非空即通过（沿用既有“无匹配规则视为通过”口径）
            return "校验通过", "无校验规则匹配，非空通过"

        condition = rule.get("condition")
        if condition == "numeric" and not value.replace(".", "").isdigit():
            return "校验不通过", f"字段[{detail.field_name}]不是数值"
        if condition == "length_gt_0" and len(value) == 0:
            return "校验不通过", f"字段[{detail.field_name}]长度为零"
        return "校验通过", f"按来源版本{detail.source_version}校验通过"

    def get_enrich_recheck_stats(self) -> dict[str, Any]:
        """补全后明细二次校验统计，供概览看板结果视图使用。"""
        snapshot = store.enrich_details.snapshot()
        return {
            "enrich_detail_total": snapshot["total"],
            "recheck_pending": snapshot["recheck_pending"],
            "recheck_pass": snapshot["recheck_pass"],
            "recheck_fail": snapshot["recheck_fail"],
            "source_versions": snapshot["source_versions"],
            "cross_check": store.enrich_details.cross_check(),
            "todos_open": snapshot["todo_open"],
        }

    def get_validation_stats(self) -> dict[str, Any]:
        """获取校验统计概览。"""
        validate_events = store.channel.snapshot(topic="rpa.validation_complete")
        total_passed = sum(
            e.get("payload", {}).get("passed", 0) for e in validate_events)
        total_failed = sum(
            e.get("payload", {}).get("failed", 0) for e in validate_events)
        return {
            "total_validation_events": len(validate_events),
            "total_passed": total_passed,
            "total_failed": total_failed,
            "dirty_vault": store.dirty_vault.snapshot(),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        stats = self.get_validation_stats()
        return {**base, **stats, "enrich_recheck": self.get_enrich_recheck_stats()}