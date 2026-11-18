<template>
  <section class="page enrich-workbench" data-module="enrich_workbench">
    <header class="page-head">
      <div>
        <h2>补全工作台</h2>
        <p class="page-desc">
          补全值与来源证据同时写入补全后明细数据集，不再改写采集记录原字段；
          同一字段只发布一个当前值，匹配键不能为空，可按来源版本追溯二次校验结论。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="run-enrich" type="button" :disabled="!selectedBatch || busy" @click="handleRun">
          {{ busy ? '处理中…' : '执行批次补全' }}
        </button>
        <button class="btn" data-testid="run-recheck" type="button" :disabled="!selectedBatch || busy" @click="handleRecheck">
          按来源版本二次校验
        </button>
        <button class="btn" data-testid="export-enrich" type="button" @click="handleExport">导出完整补全明细</button>
      </div>
    </header>

    <!-- 统计卡片：结果视图口径 -->
    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">补全明细总数</span>
        <strong class="stat-value">{{ stats.total ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">候选来源条数</span>
        <strong class="stat-value">{{ stats.candidate_total ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">待二次校验</span>
        <strong class="stat-value">{{ stats.recheck_pending ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">校验通过 / 不通过</span>
        <strong class="stat-value">{{ stats.recheck_pass ?? 0 }} / {{ stats.recheck_fail ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">开放待办</span>
        <strong class="stat-value">{{ stats.todo_open ?? 0 }}</strong>
      </article>
    </div>

    <!-- 筛选栏：批次、字段、来源版本 -->
    <form class="filter-bar enrich-filters" @submit.prevent="reload">
      <label class="filter-item">
        <span>批次</span>
        <select v-model="filters.batch_ref" data-testid="filter-batch">
          <option value="">全部批次</option>
          <option v-for="b in facets.batches" :key="b" :value="b">{{ b }}</option>
        </select>
      </label>
      <label class="filter-item">
        <span>字段</span>
        <select v-model="filters.field_name" data-testid="filter-field">
          <option value="">全部字段</option>
          <option v-for="f in facets.fields" :key="f" :value="f">{{ f }}</option>
        </select>
      </label>
      <label class="filter-item">
        <span>来源版本</span>
        <select v-model="filters.source_version" data-testid="filter-version">
          <option value="">全部版本</option>
          <option v-for="v in facets.source_versions" :key="v" :value="v">{{ v }}</option>
        </select>
      </label>
      <label class="filter-item">
        <span>校验状态</span>
        <select v-model="filters.recheck_status" data-testid="filter-recheck">
          <option value="">全部状态</option>
          <option value="待校验">待校验</option>
          <option value="校验通过">校验通过</option>
          <option value="校验不通过">校验不通过</option>
        </select>
      </label>
      <label class="filter-item">
        <span>关键字</span>
        <input v-model="filters.keyword" data-testid="filter-keyword" placeholder="当前值/匹配值/字段" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <!-- 冲突处理配置 -->
    <div class="config-bar">
      <span class="config-title">两个外部来源给出不同值时的发布口径：</span>
      <label class="config-item">
        <input v-model="conflictForm.conflict_policy" type="radio" value="latest" data-testid="policy-latest"
               @change="saveConfig" />
        按来源更新时间取较新值
      </label>
      <label class="config-item">
        <input v-model="conflictForm.conflict_policy" type="radio" value="authority" data-testid="policy-authority"
               @change="saveConfig" />
        按权威来源取指定值
      </label>
      <label class="config-item">
        <span>权威来源</span>
        <select v-model="conflictForm.authority_source" data-testid="authority-source" @change="saveConfig">
          <option value="企业主数据中心(MDM)">企业主数据中心(MDM)</option>
          <option value="外部缓存库">外部缓存库</option>
        </select>
      </label>
      <span class="config-hint">两种方式都只发布一个当前补全值</span>
    </div>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <!-- 补全后明细数据集 -->
    <table class="data-table enrich-table">
      <thead>
        <tr>
          <th>批次</th>
          <th>补全字段</th>
          <th>当前补全值</th>
          <th>补全来源 / 来源版本</th>
          <th>匹配键 / 匹配值</th>
          <th>候选来源</th>
          <th>补全发生时间</th>
          <th>二次校验</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in details" :key="row.detail_id" :data-testid="'detail-' + row.detail_id"
            :data-status="row.recheck_status">
          <td>{{ row.batch_ref }}</td>
          <td>{{ row.field_name }}</td>
          <td>
            <strong>{{ row.current_value }}</strong>
            <span class="badge badge-current">当前值</span>
          </td>
          <td>
            <div>{{ row.source }}</div>
            <div class="mono">{{ row.source_version }}</div>
            <div class="muted">策略：{{ row.conflict_policy === 'latest' ? '较新值' : '权威来源' }}</div>
          </td>
          <td>
            <div>{{ row.match_key }}</div>
            <div class="mono">{{ row.match_value }}</div>
          </td>
          <td>
            <ul class="candidate-list">
              <li v-for="c in row.candidates" :key="c.source + '-' + c.source_version"
                  :class="{ chosen: c.source === row.source }">
                <span class="candidate-source">{{ c.source }}</span>
                <span class="candidate-value">{{ c.value }}</span>
                <span class="muted">更新于 {{ formatTime(c.source_updated_at) }}</span>
              </li>
            </ul>
          </td>
          <td class="nowrap">{{ formatTime(row.enriched_at) }}</td>
          <td>
            <span :class="['recheck-badge', recheckClass(row.recheck_status)]">{{ row.recheck_status }}</span>
            <div class="muted recheck-reason">{{ row.recheck_reason || '—' }}</div>
            <div v-if="row.recheck_at" class="muted nowrap">{{ formatTime(row.recheck_at) }}</div>
          </td>
        </tr>
        <tr v-if="!details.length">
          <td colspan="8" class="empty-state">
            暂无补全明细，请选择批次后点击「执行批次补全」
          </td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条补全明细（每条均带来源证据与发生时间）</span>
      <div class="pager">
        <button class="btn" type="button" :disabled="page <= 1 || busy" @click="changePage(page - 1)">上一页</button>
        <span>第 {{ page }} 页</span>
        <button class="btn" type="button" :disabled="page * size >= total || busy" @click="changePage(page + 1)">下一页</button>
      </div>
    </footer>

    <!-- 处理台账 / 结果清单 / 待办 / 联动核对 -->
    <div class="governance-grid">
      <article class="governance-card">
        <h3>处理台账</h3>
        <ul class="gov-list">
          <li v-for="item in ledgers" :key="String(item.ledger_id)">
            <span class="gov-tag">{{ item.process_type === 'enrich' ? '补全' : '二次校验' }}</span>
            {{ item.batch_ref }} · {{ item.action }} · {{ formatTime(item.processed_at) }}
          </li>
          <li v-if="!ledgers.length" class="muted">暂无处理记录</li>
        </ul>
      </article>
      <article class="governance-card">
        <h3>结果清单（重复提交只保留一条）</h3>
        <ul class="gov-list">
          <li v-for="(item, idx) in results" :key="idx">
            <span class="gov-tag">{{ item.process_type === 'enrich' ? '补全' : '二次校验' }}</span>
            {{ item.batch_ref }}
            <template v-if="item.process_type === 'enrich'">
              · 补全 {{ item.enriched }} / 跳过 {{ item.skipped }} / 匹配键缺失 {{ item.missing_key }}
            </template>
            <template v-else>
              · 校验 {{ item.checked }}（通过 {{ item.passed }} / 不通过 {{ item.failed }}）
            </template>
          </li>
          <li v-if="!results.length" class="muted">暂无结果</li>
        </ul>
      </article>
      <article class="governance-card">
        <h3>联动待办（另一业务入口）</h3>
        <ul class="gov-list">
          <li v-for="item in todos" :key="String(item.todo_id)">
            <span class="gov-tag todo">{{ item.status }}</span>
            {{ item.title }}
          </li>
          <li v-if="!todos.length" class="muted">无开放待办</li>
        </ul>
      </article>
      <article class="governance-card">
        <h3>台账与结果清单联动核对</h3>
        <p :class="crossCheck.consistent ? 'check-ok' : 'check-bad'">
          {{ crossCheck.consistent ? '核对一致' : '存在孤立结果' }}
        </p>
        <p class="muted">台账 {{ crossCheck.ledger_count ?? 0 }} 条 · 结果 {{ crossCheck.result_count ?? 0 }} 条</p>
      </article>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'

import {
  downloadEnrichDetails,
  loadEnrichCrossCheck,
  loadEnrichDetails,
  loadEnrichFacets,
  loadEnrichLedgers,
  loadEnrichResults,
  loadEnrichStats,
  loadEnrichTodos,
  runEnrich,
  runEnrichRecheck,
  updateEnrichConfig,
  type EnrichDetail,
  type EnrichFacets,
} from '@/api/backend-service'

const PAGE_SIZE = 10
const details = ref<EnrichDetail[]>([])
const total = ref(0)
const page = ref(1)
const size = ref(PAGE_SIZE)
const busy = ref(false)
const errorMessage = ref('')
const filters = reactive<Record<string, string>>({
  batch_ref: '',
  field_name: '',
  source_version: '',
  recheck_status: '',
  keyword: '',
})
const facets = ref<EnrichFacets>({
  batches: [],
  fields: [],
  source_versions: [],
  config: { conflict_policy: 'latest', authority_source: '企业主数据中心(MDM)' },
})
const stats = ref<Record<string, any>>({})
const ledgers = ref<Record<string, unknown>[]>([])
const results = ref<Record<string, unknown>[]>([])
const todos = ref<Record<string, unknown>[]>([])
const crossCheck = ref<Record<string, any>>({})
const conflictForm = reactive({
  conflict_policy: 'latest',
  authority_source: '企业主数据中心(MDM)',
})

const selectedBatch = computed(() => filters.batch_ref)

function formatTime(value?: unknown): string {
  if (typeof value !== 'string' || !value) return '—'
  return value.replace('T', ' ').replace(/\+\d{2}:\d{2}$/, '').slice(0, 19)
}

function recheckClass(status: string): string {
  if (status === '校验通过') return 'pass'
  if (status === '校验不通过') return 'fail'
  return 'pending'
}

function activeFilters(): Record<string, string> {
  return Object.fromEntries(Object.entries(filters).filter(([, v]) => v && v.trim()))
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await loadEnrichDetails(activeFilters(), page.value, size.value)
    details.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '补全明细读取失败'
  }
}

async function reloadFacetsAndStats() {
  const [facetPayload, statsPayload] = await Promise.all([
    loadEnrichFacets(),
    loadEnrichStats(),
  ])
  facets.value = facetPayload
  stats.value = statsPayload
  conflictForm.conflict_policy = facetPayload.config.conflict_policy
  conflictForm.authority_source = facetPayload.config.authority_source
}

async function reloadGovernance() {
  const [ledgerPayload, resultPayload, todoPayload, crossPayload] = await Promise.all([
    loadEnrichLedgers(),
    loadEnrichResults(),
    loadEnrichTodos(),
    loadEnrichCrossCheck(),
  ])
  ledgers.value = ledgerPayload.items
  results.value = resultPayload.items
  todos.value = todoPayload.items
  crossCheck.value = crossPayload
}

async function refreshAll() {
  await Promise.all([reloadFacetsAndStats(), reloadGovernance()])
  await reload()
}

function resetFilters() {
  Object.keys(filters).forEach((key) => {
    filters[key] = ''
  })
  page.value = 1
  void reload()
}

function changePage(next: number) {
  page.value = next
  void reload()
}

async function handleRun() {
  if (!selectedBatch.value) {
    errorMessage.value = '请先选择批次再执行补全'
    return
  }
  busy.value = true
  errorMessage.value = ''
  try {
    await runEnrich(selectedBatch.value)
    await refreshAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批次补全失败'
  } finally {
    busy.value = false
  }
}

async function handleRecheck() {
  if (!selectedBatch.value) {
    errorMessage.value = '请先选择批次再执行二次校验'
    return
  }
  busy.value = true
  errorMessage.value = ''
  try {
    await runEnrichRecheck(selectedBatch.value)
    await refreshAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '二次校验失败'
  } finally {
    busy.value = false
  }
}

async function saveConfig() {
  errorMessage.value = ''
  try {
    const payload = await updateEnrichConfig({ ...conflictForm })
    const config = payload as { conflict_policy: string; authority_source: string }
    conflictForm.conflict_policy = config.conflict_policy
    conflictForm.authority_source = config.authority_source
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '冲突配置保存失败'
  }
}

async function handleExport() {
  errorMessage.value = ''
  try {
    await downloadEnrichDetails(activeFilters())
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

onMounted(refreshAll)
</script>

<style scoped>
.enrich-filters select,
.enrich-filters input {
  min-width: 150px;
}

.config-bar {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 16px;
  padding: 12px 16px;
  margin-bottom: 16px;
  border: 1px solid var(--border-color, #d9e0ec);
  border-radius: 8px;
  background: #f7f9fc;
}

.config-title {
  font-weight: 600;
}

.config-item {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  white-space: nowrap;
}

.config-hint,
.muted {
  color: #7a8699;
  font-size: 12px;
}

.mono {
  font-family: 'SFMono-Regular', Consolas, monospace;
  font-size: 12px;
  color: #4a5568;
}

.enrich-table td {
  vertical-align: top;
  font-size: 13px;
}

.badge {
  display: inline-block;
  margin-left: 6px;
  padding: 1px 7px;
  border-radius: 10px;
  font-size: 11px;
}

.badge-current {
  background: #e6f4ff;
  color: #1677ff;
}

.candidate-list {
  margin: 0;
  padding: 0;
  list-style: none;
}

.candidate-list li {
  padding: 3px 6px;
  border-radius: 6px;
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.candidate-list li.chosen {
  background: #f0f9eb;
  outline: 1px solid #b7e1a1;
}

.candidate-source {
  font-weight: 600;
  font-size: 12px;
}

.candidate-value {
  font-size: 12px;
}

.recheck-badge {
  display: inline-block;
  padding: 2px 10px;
  border-radius: 10px;
  font-size: 12px;
}

.recheck-badge.pass {
  background: #f0f9eb;
  color: #389e0d;
}

.recheck-badge.fail {
  background: #fff1f0;
  color: #cf1322;
}

.recheck-badge.pending {
  background: #fffbe6;
  color: #d48806;
}

.recheck-reason {
  max-width: 180px;
  margin-top: 4px;
}

.nowrap {
  white-space: nowrap;
}

.pager {
  display: inline-flex;
  align-items: center;
  gap: 10px;
}

.governance-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
  gap: 16px;
  margin-top: 24px;
}

.governance-card {
  border: 1px solid var(--border-color, #d9e0ec);
  border-radius: 8px;
  padding: 14px 16px;
  background: #fff;
}

.governance-card h3 {
  margin: 0 0 10px;
  font-size: 15px;
}

.gov-list {
  margin: 0;
  padding: 0;
  list-style: none;
  display: flex;
  flex-direction: column;
  gap: 8px;
  font-size: 13px;
}

.gov-tag {
  display: inline-block;
  margin-right: 6px;
  padding: 1px 8px;
  border-radius: 10px;
  background: #eef2ff;
  color: #3949ab;
  font-size: 11px;
}

.gov-tag.todo {
  background: #fff7e6;
  color: #d46b08;
}

.check-ok {
  color: #389e0d;
  font-weight: 600;
}

.check-bad {
  color: #cf1322;
  font-weight: 600;
}
</style>
