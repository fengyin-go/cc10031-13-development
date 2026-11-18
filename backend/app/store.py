"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

跨模块数据流能力：
- DataChannel: 服务间数据发布/消费通道，支持 topic 订阅、快照读、消费确认
- DirtyDataVault: 脏数据隔离区，存储校验不通过/解析失败的脏数据记录
- FailureRecovery: 失败恢复管理器，记录失败步骤并支持重试
- CollectRecord: 采集记录存储，记录每次采集产出的结构化数据
"""
from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.seed import SEED_ROWS

# ---------------------------------------------------------------------------
# 跨模块数据通道 (DataChannel) — 服务间发布/消费
# ---------------------------------------------------------------------------

@dataclass
class ChannelMessage:
    """数据通道中的单条消息，带 topic、来源、负载和确认状态。"""
    msg_id: int
    topic: str
    source_module: str
    payload: dict[str, Any]
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    consumed_by: list[str] = field(default_factory=list)
    confirmed: bool = False

    def mark_consumed(self, consumer: str) -> None:
        if consumer not in self.consumed_by:
            self.consumed_by.append(consumer)


class DataChannel:
    """跨模块数据通道：支持 topic 发布/订阅，服务间数据流转的桥梁。

    用法::

        store.channel.publish("rpa.bot_created", "bot_definition", bot_data)
        msgs = store.channel.consume("step_editor", "rpa.bot_created")
        store.channel.confirm(msg_id, "step_editor")
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._messages: list[ChannelMessage] = []

    @property
    def total_messages(self) -> int:
        with self._lock:
            return len(self._messages)

    def publish(self, topic: str, source: str, payload: dict[str, Any]) -> ChannelMessage:
        with self._lock:
            self._seq += 1
            msg = ChannelMessage(
                msg_id=self._seq, topic=topic,
                source_module=source, payload=payload,
            )
            self._messages.append(msg)
            return msg

    def consume(self, consumer: str, topic: str | None = None,
                since_msg_id: int | None = None) -> list[ChannelMessage]:
        """获取指定 consumer 尚未消费的消息。"""
        with self._lock:
            results: list[ChannelMessage] = []
            for msg in self._messages:
                if topic and msg.topic != topic:
                    continue
                if since_msg_id is not None and msg.msg_id <= since_msg_id:
                    continue
                if consumer not in msg.consumed_by:
                    results.append(msg)
                    msg.mark_consumed(consumer)
            return results

    def confirm(self, msg_id: int, consumer: str) -> bool:
        with self._lock:
            for msg in self._messages:
                if msg.msg_id == msg_id:
                    msg.confirmed = True
                    return True
            return False

    def snapshot(self, topic: str | None = None) -> list[dict[str, Any]]:
        """返回消息快照，供跨模块分析/看板消费。"""
        with self._lock:
            msgs = self._messages if topic is None else [
                m for m in self._messages if m.topic == topic
            ]
            return [{
                "msg_id": m.msg_id, "topic": m.topic,
                "source": m.source_module, "payload": m.payload,
                "created_at": m.created_at, "consumed_by": m.consumed_by,
                "confirmed": m.confirmed,
            } for m in msgs]

    def latest(self, topic: str) -> dict[str, Any] | None:
        with self._lock:
            for msg in reversed(self._messages):
                if msg.topic == topic:
                    return {
                        "msg_id": msg.msg_id, "topic": msg.topic,
                        "source": msg.source_module, "payload": msg.payload,
                        "created_at": msg.created_at, "confirmed": msg.confirmed,
                    }
            return None

    def uncomfirmed_count(self) -> int:
        with self._lock:
            return sum(1 for m in self._messages if not m.confirmed)


# ---------------------------------------------------------------------------
# 脏数据隔离区 (DirtyDataVault)
# ---------------------------------------------------------------------------

@dataclass
class DirtyRecord:
    """脏数据记录：校验失败或解析异常的数据行。"""
    record_id: int
    source_module: str
    batch_ref: str
    raw_data: dict[str, Any]
    reason: str  # 如 "字段缺失", "格式非法", "解析超时"
    detected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    quarantined: bool = True
    recovered_to: str | None = None


class DirtyDataVault:
    """脏数据隔离区：被校验规则或解析器标记为问题数据后移入此处。

    用法::

        vault = store.dirty_vault
        vault.quarantine("data_extract", "BATC-001", row, "缺少必填字段")
        dirty = vault.list_by_source("data_extract")
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._records: list[DirtyRecord] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._records)

    def quarantine(self, source: str, batch_ref: str,
                   raw_data: dict[str, Any], reason: str) -> DirtyRecord:
        with self._lock:
            self._seq += 1
            rec = DirtyRecord(
                record_id=self._seq, source_module=source,
                batch_ref=batch_ref, raw_data=raw_data, reason=reason,
            )
            self._records.append(rec)
            return rec

    def list_by_source(self, source: str) -> list[DirtyRecord]:
        with self._lock:
            return [r for r in self._records if r.source_module == source]

    def list_by_batch(self, batch_ref: str) -> list[DirtyRecord]:
        with self._lock:
            return [r for r in self._records if r.batch_ref == batch_ref]

    def mark_recovered(self, record_id: int, recovered_to: str) -> bool:
        with self._lock:
            for r in self._records:
                if r.record_id == record_id:
                    r.quarantined = False
                    r.recovered_to = recovered_to
                    return True
            return False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            quarantined = sum(1 for r in self._records if r.quarantined)
            by_reason: dict[str, int] = {}
            for r in self._records:
                by_reason[r.reason] = by_reason.get(r.reason, 0) + 1
            return {
                "total": len(self._records),
                "quarantined": quarantined,
                "recovered": len(self._records) - quarantined,
                "by_reason": by_reason,
            }


# ---------------------------------------------------------------------------
# 采集记录 (CollectRecord) — 每次采集产出的结构化数据
# ---------------------------------------------------------------------------

@dataclass
class CollectEntry:
    entry_id: int
    batch_ref: str
    source_module: str
    fields: dict[str, Any]
    collected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    validated: bool = False
    quality_score: float = 1.0


class CollectRecordStore:
    """采集记录存储：每次采集流程产出的结构化数据行。

    用法::

        rec = store.collect_records.add("BATC-001", "data_extract", {"字段1": "值1", ...})
        store.collect_records.mark_validated(rec.entry_id)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._entries: list[CollectEntry] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def add(self, batch_ref: str, source: str,
            fields: dict[str, Any]) -> CollectEntry:
        with self._lock:
            self._seq += 1
            entry = CollectEntry(
                entry_id=self._seq, batch_ref=batch_ref,
                source_module=source, fields=dict(fields),
            )
            self._entries.append(entry)
            return entry

    def list_by_batch(self, batch_ref: str) -> list[CollectEntry]:
        with self._lock:
            return [e for e in self._entries if e.batch_ref == batch_ref]

    def list_by_source(self, source: str) -> list[CollectEntry]:
        with self._lock:
            return [e for e in self._entries if e.source_module == source]

    def mark_validated(self, entry_id: int, score: float = 1.0) -> bool:
        with self._lock:
            for e in self._entries:
                if e.entry_id == entry_id:
                    e.validated = True
                    e.quality_score = score
                    return True
            return False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            validated = sum(1 for e in self._entries if e.validated)
            by_source: dict[str, int] = {}
            for e in self._entries:
                by_source[e.source_module] = by_source.get(e.source_module, 0) + 1
            return {
                "total": len(self._entries),
                "validated": validated,
                "unvalidated": len(self._entries) - validated,
                "by_source": by_source,
            }


# ---------------------------------------------------------------------------
# 补全后明细数据集 (EnrichDetailStore)
# 补全值与来源证据同时落库，不再直接改写采集记录原字段
# ---------------------------------------------------------------------------

@dataclass
class EnrichCandidate:
    """外部数据源给出的单个候选补全值（来源证据）。"""
    source: str
    source_version: str
    value: str
    match_key: str
    source_updated_at: str


@dataclass
class EnrichDetail:
    """补全后明细：同一采集记录的同一字段只发布一个当前值（is_current=True）。

    补全值与来源证据（来源、来源版本、匹配键、候选来源、发生时间）同行存储，
    二次校验结果按补全来源版本回写到同一条明细。
    """
    detail_id: int
    batch_ref: str
    record_id: int
    field_name: str
    current_value: str
    source: str
    source_version: str
    match_key: str
    match_value: str
    rule_id: int
    rule_name: str
    conflict_policy: str
    enriched_at: str
    candidates: list[dict[str, Any]] = field(default_factory=list)
    is_current: bool = True
    original_empty: bool = True
    # 二次校验（按处理项寅口径、按补全来源版本）
    recheck_status: str = "待校验"
    recheck_reason: str = ""
    recheck_source_version: str | None = None
    recheck_at: str | None = None


class EnrichDetailStore:
    """补全后明细数据集存储 + 处理台账/结果清单/待办联动。

    用法::

        store.enrich_details.upsert_detail(detail_dict)
        rows, total = store.enrich_details.list_details({"batch_ref": "B1"})
    """

    def __init__(self) -> None:
        # 可重入锁：update_config 等方法持锁后会内部复用 config_snapshot() 等带锁方法
        self._lock = threading.RLock()
        self._seq = 0
        self._details: list[EnrichDetail] = []
        # 处理台账：每次处理动作一条
        self._ledgers: list[dict[str, Any]] = []
        # 结果清单：同一(批次, 处理类型)重复提交只保留一条结果
        self._results: list[dict[str, Any]] = []
        # 待办：另一个业务入口消费
        self._todos: list[dict[str, Any]] = []
        # 冲突处理配置：latest=按来源更新时间取较新值，authority=按权威来源取指定值
        self.conflict_policy = "latest"
        self.authority_source = "企业主数据中心(MDM)"

    # ---- 配置 ----

    def update_config(self, conflict_policy: str | None = None,
                      authority_source: str | None = None) -> dict[str, Any]:
        with self._lock:
            if conflict_policy in ("latest", "authority"):
                self.conflict_policy = conflict_policy
            if authority_source:
                self.authority_source = authority_source
            return self.config_snapshot()

    def config_snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "conflict_policy": self.conflict_policy,
                "authority_source": self.authority_source,
            }

    # ---- 补全后明细 ----

    def upsert_detail(self, payload: dict[str, Any]) -> EnrichDetail:
        """按 (record_id, field_name) 幂等写入：同一字段只保留一个当前值。

        重复补全时覆盖旧明细（沿用既有版本口径，来源版本一并更新），
        不新增第二行，保证一个字段只有一个当前值。
        """
        with self._lock:
            for detail in self._details:
                if (detail.record_id == payload["record_id"]
                        and detail.field_name == payload["field_name"]):
                    payload_detail = self._build_detail(detail.detail_id, payload)
                    self._details[self._details.index(detail)] = payload_detail
                    return payload_detail
            self._seq += 1
            detail = self._build_detail(self._seq, payload)
            self._details.append(detail)
            return detail

    def _build_detail(self, detail_id: int, payload: dict[str, Any]) -> EnrichDetail:
        return EnrichDetail(
            detail_id=detail_id,
            batch_ref=payload["batch_ref"],
            record_id=payload["record_id"],
            field_name=payload["field_name"],
            current_value=payload["current_value"],
            source=payload["source"],
            source_version=payload["source_version"],
            match_key=payload["match_key"],
            match_value=payload["match_value"],
            rule_id=payload["rule_id"],
            rule_name=payload["rule_name"],
            conflict_policy=payload["conflict_policy"],
            enriched_at=payload["enriched_at"],
            candidates=list(payload.get("candidates", [])),
            is_current=True,
            original_empty=bool(payload.get("original_empty", True)),
            recheck_status=payload.get("recheck_status", "待校验"),
            recheck_reason=payload.get("recheck_reason", ""),
            recheck_source_version=payload.get("recheck_source_version"),
            recheck_at=payload.get("recheck_at"),
        )

    def list_details(
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
        with self._lock:
            rows = [self._to_dict(d) for d in self._details if d.is_current]
        if batch_ref:
            rows = [r for r in rows if r["batch_ref"] == batch_ref]
        if field_name:
            rows = [r for r in rows if field_name.casefold() in r["field_name"].casefold()]
        if source_version:
            rows = [r for r in rows if r["source_version"] == source_version]
        if source:
            rows = [r for r in rows if source.casefold() in r["source"].casefold()]
        if recheck_status:
            rows = [r for r in rows if r["recheck_status"] == recheck_status]
        if keyword:
            kw = keyword.casefold()
            rows = [
                r for r in rows
                if kw in r["current_value"].casefold()
                or kw in r["match_value"].casefold()
                or kw in r["field_name"].casefold()
            ]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def list_current_raw(self, batch_ref: str | None = None) -> list[EnrichDetail]:
        with self._lock:
            rows = [d for d in self._details if d.is_current]
        if batch_ref:
            rows = [d for d in rows if d.batch_ref == batch_ref]
        return rows

    def current_value(self, record_id: int, field_name: str) -> dict[str, Any] | None:
        with self._lock:
            for d in self._details:
                if d.record_id == record_id and d.field_name == field_name and d.is_current:
                    return self._to_dict(d)
        return None

    def apply_recheck(self, detail_id: int, status: str,
                      reason: str, source_version: str, checked_at: str) -> bool:
        with self._lock:
            for d in self._details:
                if d.detail_id == detail_id:
                    d.recheck_status = status
                    d.recheck_reason = reason
                    d.recheck_source_version = source_version
                    d.recheck_at = checked_at
                    return True
        return False

    def distinct_versions(self) -> list[str]:
        with self._lock:
            return sorted({d.source_version for d in self._details if d.is_current})

    def distinct_batches(self) -> list[str]:
        with self._lock:
            return sorted({d.batch_ref for d in self._details if d.is_current})

    def distinct_fields(self) -> list[str]:
        with self._lock:
            return sorted({d.field_name for d in self._details if d.is_current})

    # ---- 处理台账 / 结果清单 / 待办 ----

    def add_ledger(self, entry: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            entry = dict(entry)
            entry.setdefault("ledger_id", len(self._ledgers) + 1)
            self._ledgers.append(entry)
            return entry

    def list_ledgers(self, batch_ref: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            rows = list(self._ledgers)
        if batch_ref:
            rows = [r for r in rows if r.get("batch_ref") == batch_ref]
        return rows

    def publish_result(self, result: dict[str, Any]) -> dict[str, Any]:
        """发布处理结果：同一(批次, 处理类型)重复提交只保留一条结果。"""
        with self._lock:
            key = (result.get("batch_ref"), result.get("process_type"))
            for existing in self._results:
                if (existing.get("batch_ref"), existing.get("process_type")) == key:
                    existing.clear()
                    existing.update(result)
                    return existing
            self._results.append(dict(result))
            return dict(result)

    def list_results(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._results)

    def upsert_todo(self, todo: dict[str, Any]) -> dict[str, Any]:
        """待办按 (batch_ref, todo_type) 去重，另一个业务入口跟着更新。"""
        with self._lock:
            key = (todo.get("batch_ref"), todo.get("todo_type"))
            for existing in self._todos:
                if (existing.get("batch_ref"), existing.get("todo_type")) == key:
                    existing.update(todo)
                    return existing
            todo = dict(todo)
            todo.setdefault("todo_id", len(self._todos) + 1)
            self._todos.append(todo)
            return todo

    def resolve_todo(self, batch_ref: str, todo_type: str) -> bool:
        with self._lock:
            for todo in self._todos:
                if todo.get("batch_ref") == batch_ref and todo.get("todo_type") == todo_type:
                    todo["status"] = "已完成"
                    todo["resolved_at"] = datetime.now(timezone.utc).isoformat()
                    return True
        return False

    def list_todos(self, include_done: bool = False) -> list[dict[str, Any]]:
        with self._lock:
            rows = list(self._todos)
        if not include_done:
            rows = [r for r in rows if r.get("status") != "已完成"]
        return rows

    def cross_check(self) -> dict[str, Any]:
        """处理台账与结果清单联动核对：每个已发布结果必须有对应台账记录。"""
        with self._lock:
            ledgers = list(self._ledgers)
            results = list(self._results)
        ledger_keys = {(l.get("batch_ref"), l.get("process_type")) for l in ledgers}
        orphan = [
            r for r in results
            if (r.get("batch_ref"), r.get("process_type")) not in ledger_keys
        ]
        return {
            "ledger_count": len(ledgers),
            "result_count": len(results),
            "consistent": not orphan,
            "orphan_results": orphan,
        }

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            details = [d for d in self._details if d.is_current]
            total_candidates = sum(len(d.candidates) for d in details)
            recheck_pass = sum(1 for d in details if d.recheck_status == "校验通过")
            recheck_fail = sum(1 for d in details if d.recheck_status == "校验不通过")
            recheck_pending = sum(1 for d in details if d.recheck_status == "待校验")
            return {
                "total": len(details),
                "candidate_total": total_candidates,
                "batches": len({d.batch_ref for d in details}),
                "fields": len({d.field_name for d in details}),
                "source_versions": sorted({d.source_version for d in details}),
                "recheck_pending": recheck_pending,
                "recheck_pass": recheck_pass,
                "recheck_fail": recheck_fail,
                "ledger_total": len(self._ledgers),
                "result_total": len(self._results),
                "todo_open": sum(1 for t in self._todos if t.get("status") != "已完成"),
                "config": {
                    "conflict_policy": self.conflict_policy,
                    "authority_source": self.authority_source,
                },
            }

    @staticmethod
    def _to_dict(d: EnrichDetail) -> dict[str, Any]:
        return {
            "detail_id": d.detail_id,
            "batch_ref": d.batch_ref,
            "record_id": d.record_id,
            "field_name": d.field_name,
            "current_value": d.current_value,
            "source": d.source,
            "source_version": d.source_version,
            "match_key": d.match_key,
            "match_value": d.match_value,
            "rule_id": d.rule_id,
            "rule_name": d.rule_name,
            "conflict_policy": d.conflict_policy,
            "enriched_at": d.enriched_at,
            "candidates": list(d.candidates),
            "is_current": d.is_current,
            "original_empty": d.original_empty,
            "recheck_status": d.recheck_status,
            "recheck_reason": d.recheck_reason,
            "recheck_source_version": d.recheck_source_version,
            "recheck_at": d.recheck_at,
        }


# ---------------------------------------------------------------------------
# 失败恢复管理 (FailureRecovery)
# ---------------------------------------------------------------------------

@dataclass
class RecoveryEntry:
    recovery_id: int
    module: str
    batch_ref: str
    step: str
    error_detail: str
    attempt: int = 0
    max_attempts: int = 3
    resolved: bool = False
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class FailureRecovery:
    """失败恢复管理器：记录失败步骤并支持自动重试。

    用法::

        rec = store.recovery.log("step_editor", "BATC-001", "元素定位",
                                  "选择器超时未匹配")
        store.recovery.retry(rec.recovery_id)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._entries: list[RecoveryEntry] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def log(self, module: str, batch_ref: str, step: str,
            error_detail: str, max_attempts: int = 3) -> RecoveryEntry:
        with self._lock:
            self._seq += 1
            entry = RecoveryEntry(
                recovery_id=self._seq, module=module,
                batch_ref=batch_ref, step=step,
                error_detail=error_detail, max_attempts=max_attempts,
            )
            self._entries.append(entry)
            return entry

    def retry(self, recovery_id: int) -> tuple[bool, str]:
        with self._lock:
            for e in self._entries:
                if e.recovery_id == recovery_id:
                    if e.resolved:
                        return False, "已恢复，无需重试"
                    e.attempt += 1
                    if e.attempt > e.max_attempts:
                        return False, f"已达最大重试次数 {e.max_attempts}"
                    return True, f"重试 {e.attempt}/{e.max_attempts}"
            return False, "恢复记录不存在"

    def resolve(self, recovery_id: int) -> bool:
        with self._lock:
            for e in self._entries:
                if e.recovery_id == recovery_id:
                    e.resolved = True
                    return True
            return False

    def pending_count(self) -> int:
        with self._lock:
            return sum(1 for e in self._entries if not e.resolved)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            pending = sum(1 for e in self._entries if not e.resolved)
            return {
                "total": len(self._entries),
                "resolved": len(self._entries) - pending,
                "pending": pending,
            }


# ---------------------------------------------------------------------------
# 增强版 Store — 整合通道、脏数据、采集记录、恢复
# ---------------------------------------------------------------------------

class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 跨模块基础设施
        self.channel = DataChannel()
        self.dirty_vault = DirtyDataVault()
        self.collect_records = CollectRecordStore()
        self.recovery = FailureRecovery()
        self.enrich_details = EnrichDetailStore()
        _bootstrap_demo_collect_records(self)

    # ---- 基础数据操作 ----

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


def _bootstrap_demo_collect_records(target_store: "Store") -> None:
    """写入演示采集批次：部分目标字段为空，供补全工作台直接演示来源证据。

    与 data_enrich 种子规则的「目标字段/匹配键」口径保持一致；
    每条记录保留页面采集原值，补全值只写入 EnrichDetailStore，不改写原字段。
    """
    if target_store.collect_records.count:
        return
    base_time = "2026-11-18T02:00:00+00:00"
    demo_rows = [
        # 批次1：工商信息补全（企业名称/统一社会信用代码 有值可作匹配键，行业分类/注册地址为空）
        {"企业名称": "杭州云栖科技有限公司", "统一社会信用代码": "91330106MA27H1AA01",
         "行业分类": "", "注册地址": "", "采集页面": "企业信用公示页"},
        {"企业名称": "上海临港制造股份有限公司", "统一社会信用代码": "91310115MA1K3BB02",
         "行业分类": "", "注册地址": "上海市浦东新区临港大道100号", "采集页面": "企业信用公示页"},
        {"企业名称": "深圳前海贸易有限公司", "统一社会信用代码": "91440300MA5ECX03",
         "行业分类": "批发业", "注册地址": "", "采集页面": "企业信用公示页"},
        # 批次2：商品信息补全（商品编码/品牌 有值，规格型号/生产厂家为空）
        {"商品名称": "工业级温湿度传感器", "商品编码": "SKU-TH-2026-001", "品牌": "云栖",
         "规格型号": "", "生产厂家": "", "采集页面": "商品详情页"},
        {"商品名称": "智能网关设备", "商品编码": "SKU-GW-2026-002", "品牌": "临港",
         "规格型号": "GW-X9", "生产厂家": "", "采集页面": "商品详情页"},
        {"商品名称": "高精度数控模组", "商品编码": "", "品牌": "临工",
         "规格型号": "", "生产厂家": "上海临港制造股份有限公司", "采集页面": "商品详情页"},
    ]
    batches = ["BATC-DEMO-20261118-01", "BATC-DEMO-20261118-02"]
    for index, row in enumerate(demo_rows):
        batch_ref = batches[0] if index < 3 else batches[1]
        entry = target_store.collect_records.add(
            batch_ref=batch_ref, source="data_extract", fields=dict(row))
        entry.collected_at = base_time

store = Store()
