import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

/** 跨模块数据流：获取全平台领域快照 */
export async function loadNodeStats(module?: string): Promise<Record<string, unknown>> {
  const query = module ? `?module=${module}` : ''
  const response = await request(`/api/nodestats${query}`)
  if (!response.ok) throw new Error('节点统计读取失败')
  return await response.json()
}

/** 跨模块数据流：获取指定 topic 的通道消息 */
export async function loadChannelSnapshot(topic: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/channel/${topic}`)
  if (!response.ok) throw new Error('通道快照读取失败')
  return await response.json()
}

/** 跨模块数据流：查询脏数据隔离区 */
export async function loadDirtyVault(source?: string): Promise<Record<string, unknown>> {
  const query = source ? `?source=${source}` : ''
  const response = await request(`/api/dirty-vault${query}`)
  if (!response.ok) throw new Error('脏数据查询失败')
  return await response.json()
}

/** 跨模块数据流：查询采集记录 */
export async function loadCollectRecords(batchRef?: string): Promise<Record<string, unknown>> {
  const query = batchRef ? `?batch_ref=${batchRef}` : ''
  const response = await request(`/api/collect-records${query}`)
  if (!response.ok) throw new Error('采集记录查询失败')
  return await response.json()
}

/** 跨模块数据流：执行RPA全生命周期管道 */
export async function triggerPipeline(lifecycle: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/pipeline/${lifecycle}`, {
    method: 'POST',
  })
  if (!response.ok) throw new Error('管道执行失败')
  return await response.json()
}

/** 跨模块数据流：获取失败恢复状态 */
export async function loadRecoveryStats(): Promise<Record<string, unknown>> {
  const response = await request('/api/recovery')
  if (!response.ok) throw new Error('恢复状态查询失败')
  return await response.json()
}

// ---- 数据补全工作台：补全后明细数据集（补全值 + 来源证据） ----

export type EnrichDetail = {
  detail_id: number
  batch_ref: string
  record_id: number
  field_name: string
  current_value: string
  source: string
  source_version: string
  match_key: string
  match_value: string
  rule_id: number
  rule_name: string
  conflict_policy: string
  enriched_at: string
  candidates: {
    source: string
    source_version: string
    value: string
    match_key: string
    source_updated_at: string
  }[]
  is_current: boolean
  original_empty: boolean
  recheck_status: string
  recheck_reason: string
  recheck_source_version: string | null
  recheck_at: string | null
}

export type EnrichFacets = {
  batches: string[]
  fields: string[]
  source_versions: string[]
  config: { conflict_policy: string; authority_source: string }
}

async function enrichGet(path: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/enrich${path}`)
  if (!response.ok) throw new Error('补全工作台数据读取失败')
  return await response.json()
}

async function enrichPost(path: string, body: Record<string, unknown>): Promise<Record<string, unknown>> {
  const response = await request(`/api/enrich${path}`, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}))
    throw new Error(detail.detail ?? '补全工作台操作失败')
  }
  return await response.json()
}

/** 按批次/字段/来源版本/校验状态分页查询补全后明细 */
export async function loadEnrichDetails(
  filters: Record<string, string>,
  page = 1,
  size = 20,
): Promise<{ items: EnrichDetail[]; total: number; page: number; size: number }> {
  const query = new URLSearchParams({ ...filters, page: String(page), size: String(size) }).toString()
  const response = await request(`/api/enrich/details?${query}`)
  if (!response.ok) throw new Error('补全明细读取失败')
  return await response.json()
}

export function loadEnrichFacets(): Promise<EnrichFacets> {
  return enrichGet('/facets') as Promise<EnrichFacets>
}

export function loadEnrichStats(): Promise<Record<string, unknown>> {
  return enrichGet('/stats')
}

/** 按批次生成补全后明细数据集（补全值与来源证据同时写入） */
export function runEnrich(batchRef: string): Promise<Record<string, unknown>> {
  return enrichPost('/run', { batch_ref: batchRef })
}

/** 结果校验按补全来源版本重新校验受影响字段 */
export function runEnrichRecheck(batchRef: string): Promise<Record<string, unknown>> {
  return enrichPost('/recheck', { batch_ref: batchRef })
}

export function loadEnrichLedgers(): Promise<{ items: Record<string, unknown>[]; total: number }> {
  return enrichGet('/ledgers') as Promise<{ items: Record<string, unknown>[]; total: number }>
}

export function loadEnrichResults(): Promise<{ items: Record<string, unknown>[]; total: number }> {
  return enrichGet('/results') as Promise<{ items: Record<string, unknown>[]; total: number }>
}

export function loadEnrichTodos(): Promise<{ items: Record<string, unknown>[]; total: number }> {
  return enrichGet('/todos') as Promise<{ items: Record<string, unknown>[]; total: number }>
}

export function loadEnrichCrossCheck(): Promise<Record<string, unknown>> {
  return enrichGet('/cross-check')
}

export function loadEnrichRecheckStats(): Promise<Record<string, unknown>> {
  return enrichGet('/recheck/stats')
}

/** 配置冲突处理：latest=较新值，authority=权威来源指定值 */
export function updateEnrichConfig(payload: {
  conflict_policy?: string
  authority_source?: string
}): Promise<Record<string, unknown>> {
  return enrichPost('/config', payload)
}

/** 导出完整补全明细（含来源证据与候选来源）CSV */
export async function downloadEnrichDetails(filters: Record<string, string> = {}): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/enrich/export?${query}`)
  if (!response.ok) throw new Error('补全明细导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = 'enrich_details.csv'
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}
