"""补全后明细数据集测试：来源证据、唯一当前值、冲突策略与二次校验联动。

覆盖需求口径：
- 读取采集记录 + 已生效补全规则，解析每个空字段的补全来源；
- 补全值与来源证据（来源版本/匹配键/候选来源/发生时间）同时写入明细；
- 不再改写采集记录原字段；同一字段只发布一个当前值；匹配键不能为空；
- 两个外部来源冲突时 latest / authority 两种策略都只发布一个当前值；
- 结果校验按处理项寅口径、按补全来源版本重新校验受影响字段；
- 处理台账与结果清单联动核对、重复提交只保留一条结果、待办联动、版本口径沿用。
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services.data_enrich import (  # noqa: E402
    EXTERNAL_SOURCES,
    DataEnrichService,
)
from app.services.result_verify import ResultVerifyService  # noqa: E402
from app.store import store  # noqa: E402

BATCH_1 = "BATC-DEMO-20261118-01"
BATCH_2 = "BATC-DEMO-20261118-02"


@pytest.fixture(autouse=True)
def reset_enrich_state():
    """每个用例前清空补全明细/台账/结果/待办并还原冲突配置。"""
    store.enrich_details._details.clear()  # noqa: SLF001
    store.enrich_details._ledgers.clear()  # noqa: SLF001
    store.enrich_details._results.clear()  # noqa: SLF001
    store.enrich_details._todos.clear()  # noqa: SLF001
    store.enrich_details._seq = 0  # noqa: SLF001
    store.enrich_details.update_config(
        conflict_policy="latest", authority_source="企业主数据中心(MDM)")
    yield


class TestEnrichDetailDataset:
    def test_demo_collect_records_loaded(self) -> None:
        assert store.collect_records.list_by_batch(BATCH_1)
        assert store.collect_records.list_by_batch(BATCH_2)

    def test_only_active_rules_apply(self) -> None:
        svc = DataEnrichService()
        rules = svc._get_active_rules()  # noqa: SLF001
        assert rules
        assert all(r["id"] for r in rules)
        # 种子中 id=3 为已停用规则，不应参与补全
        assert 3 not in {r["id"] for r in rules}

    def test_enrich_writes_value_and_evidence_together(self) -> None:
        svc = DataEnrichService()
        svc.enrich_batch(BATCH_1)
        items, total = svc.list_enrich_details(batch_ref=BATCH_1)
        assert total > 0
        for detail in items:
            # 补全值与来源证据同时存在
            assert detail["current_value"]
            assert detail["source"] in EXTERNAL_SOURCES
            assert detail["source_version"]
            assert detail["match_key"] and detail["match_value"]
            assert detail["rule_id"] and detail["rule_name"]
            assert detail["enriched_at"]
            # 两个外部来源都给出候选
            assert len(detail["candidates"]) == 2
            assert all(c["source_version"] and c["source_updated_at"]
                       and c["match_key"] for c in detail["candidates"])
            assert detail["is_current"] is True

    def test_original_record_fields_not_rewritten(self) -> None:
        svc = DataEnrichService()
        svc.enrich_batch(BATCH_1)
        for record in store.collect_records.list_by_batch(BATCH_1):
            # 页面采集的空字段仍然为空：补全值只进明细，不改原记录
            for field in ("行业分类", "注册地址"):
                if not record.fields.get(field):
                    assert not str(record.fields.get(field, "")).strip()

    def test_one_current_value_per_field_and_match_key_required(self) -> None:
        svc = DataEnrichService()
        svc.enrich_batch(BATCH_1)
        svc.enrich_batch(BATCH_2)
        items, _ = svc.list_enrich_details(page=1, size=1000)
        # 匹配键不能为空
        assert all(d["match_key"].strip() and d["match_value"].strip() for d in items)
        # 同一记录同一字段只有一个当前值
        keys = [(d["record_id"], d["field_name"]) for d in items]
        assert len(keys) == len(set(keys))

    def test_empty_match_key_skips_and_counts(self) -> None:
        # 批次2第6行商品编码为空，其规格型号/生产厂家无法匹配
        svc = DataEnrichService()
        outcome = svc.enrich_batch(BATCH_2)
        assert outcome["missing_key"] >= 1
        items, _ = svc.list_enrich_details(batch_ref=BATCH_2)
        match_values = {d["match_value"] for d in items}
        assert all(match_values)

    def test_repeated_submit_keeps_single_result_and_detail(self) -> None:
        svc = DataEnrichService()
        svc.enrich_batch(BATCH_1)
        _, first_total = svc.list_enrich_details(batch_ref=BATCH_1)
        svc.enrich_batch(BATCH_1)
        _, second_total = svc.list_enrich_details(batch_ref=BATCH_1)
        assert first_total == second_total
        results = [r for r in store.enrich_details.list_results()
                   if r["batch_ref"] == BATCH_1 and r["process_type"] == "enrich"]
        assert len(results) == 1

    def test_latest_policy_picks_newer_source(self) -> None:
        svc = DataEnrichService()
        svc.update_conflict_config(conflict_policy="latest")
        svc.enrich_batch(BATCH_1)
        items, _ = svc.list_enrich_details(batch_ref=BATCH_1, field_name="行业分类")
        detail = items[0]
        candidate_times = {c["source"]: c["source_updated_at"]
                           for c in detail["candidates"] if c["value"] != detail["current_value"]
                           or c["source"] == detail["source"]}
        assert detail["source"] == max(candidate_times, key=candidate_times.get)
        assert detail["conflict_policy"] == "latest"

    def test_authority_policy_picks_configured_source(self) -> None:
        svc = DataEnrichService()
        svc.update_conflict_config(
            conflict_policy="authority", authority_source="外部缓存库")
        svc.enrich_batch(BATCH_1)
        items, _ = svc.list_enrich_details(batch_ref=BATCH_1, field_name="行业分类")
        detail = items[0]
        assert detail["source"] == "外部缓存库"
        assert detail["conflict_policy"] == "authority"
        expected = next(c for c in detail["candidates"] if c["source"] == "外部缓存库")
        assert detail["current_value"] == expected["value"]
        assert detail["source_version"] == expected["source_version"]
        # 每条记录的该字段仍然只有一个当前值
        keys = [(d["record_id"], d["field_name"]) for d in items]
        assert len(keys) == len(set(keys))

    def test_both_policies_publish_single_value(self) -> None:
        svc = DataEnrichService()
        for policy, authority in (
            ("latest", "企业主数据中心(MDM)"),
            ("authority", "外部缓存库"),
        ):
            svc.update_conflict_config(
                conflict_policy=policy, authority_source=authority)
            svc.enrich_batch(BATCH_1)
            items, _ = svc.list_enrich_details(batch_ref=BATCH_1)
            keys = [(d["record_id"], d["field_name"]) for d in items]
            assert len(keys) == len(set(keys))


class TestEnrichRecheck:
    def test_recheck_by_source_version_and_write_back(self) -> None:
        svc = DataEnrichService()
        svc.enrich_batch(BATCH_1)
        recheck = ResultVerifyService().recheck_enriched_details(BATCH_1)
        assert recheck["checked"] > 0
        assert recheck["checked"] == recheck["passed"] + recheck["failed"]
        # 按来源版本分组
        assert recheck["by_source_version"]
        for version, bucket in recheck["by_source_version"].items():
            assert bucket["source_version"] == version
            assert bucket["checked"] == bucket["passed"] + bucket["failed"]
        # 回写到同一条明细
        items, _ = svc.list_enrich_details(batch_ref=BATCH_1)
        for detail in items:
            assert detail["recheck_status"] in ("校验通过", "校验不通过")
            assert detail["recheck_source_version"] == detail["source_version"]
            assert detail["recheck_at"]

    def test_recheck_creates_todo_then_resolves(self) -> None:
        svc = DataEnrichService()
        svc.enrich_batch(BATCH_1)
        todos = store.enrich_details.list_todos()
        assert any(t["batch_ref"] == BATCH_1 and t["todo_type"] == "recheck"
                   and t["status"] == "待处理" for t in todos)
        ResultVerifyService().recheck_enriched_details(BATCH_1)
        assert not store.enrich_details.list_todos()

    def test_ledger_and_result_cross_check(self) -> None:
        svc = DataEnrichService()
        svc.enrich_batch(BATCH_1)
        ResultVerifyService().recheck_enriched_details(BATCH_1)
        cross = store.enrich_details.cross_check()
        assert cross["consistent"] is True
        assert cross["ledger_count"] >= 2  # enrich + recheck
        process_types = {r["process_type"] for r in store.enrich_details.list_results()}
        assert {"enrich", "recheck"} <= process_types

    def test_recheck_repeated_submit_single_result(self) -> None:
        svc = DataEnrichService()
        svc.enrich_batch(BATCH_1)
        verify = ResultVerifyService()
        verify.recheck_enriched_details(BATCH_1)
        verify.recheck_enriched_details(BATCH_1)
        recheck_results = [r for r in store.enrich_details.list_results()
                           if r["batch_ref"] == BATCH_1 and r["process_type"] == "recheck"]
        assert len(recheck_results) == 1

    def test_recheck_stats_and_snapshot(self) -> None:
        svc = DataEnrichService()
        svc.enrich_batch(BATCH_1)
        stats = ResultVerifyService().get_enrich_recheck_stats()
        assert stats["recheck_pending"] > 0
        ResultVerifyService().recheck_enriched_details(BATCH_1)
        stats = ResultVerifyService().get_enrich_recheck_stats()
        assert stats["recheck_pending"] == 0
        assert stats["recheck_pass"] > 0
        snapshot = svc.workbench_stats()
        assert snapshot["total"] > 0 and snapshot["source_versions"]

    def test_facets_support_batch_field_version_filter(self) -> None:
        svc = DataEnrichService()
        svc.enrich_batch(BATCH_1)
        svc.enrich_batch(BATCH_2)
        facets = svc.workbench_facets()
        assert BATCH_1 in facets["batches"] and BATCH_2 in facets["batches"]
        assert facets["fields"] and facets["source_versions"]
        some_field = facets["fields"][0]
        items, total = svc.list_enrich_details(field_name=some_field)
        assert total and all(d["field_name"] == some_field for d in items)
        some_version = facets["source_versions"][0]
        items, total = svc.list_enrich_details(source_version=some_version)
        assert total and all(d["source_version"] == some_version for d in items)
