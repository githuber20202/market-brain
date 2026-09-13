import { projectReceipt, parseReceiptJSON, validateMetadata, assertSameMetadata } from './sync.mjs';

const fail = code => { throw new Error(code); };
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const KINDS = ['PREOPEN', 'POSTOPEN', 'REVIEW'];
const DELIVERIES = ['REPORTED_MATCH', 'REPORTED_BLOCKED', 'REPORTED_FAILED', 'REPORTED_PARTIAL', 'NOT_RECORDED'];
const STATES = ['MANUAL_EXPORT', 'OK', 'WAITING_FOR_RECEIPT', 'SOURCE_BLOCKED'];
const date = value => typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value)
  && Number.isFinite(Date.parse(value)) && new Date(value).toISOString().slice(0, 10) === value;
const instant = value => typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$/.test(value)
  && Number.isFinite(Date.parse(value));
const ticker = value => typeof value === 'string' && /^[A-Z]{1,5}(?:[.-][A-Z]{1,2})?$/.test(value);
const keys = (value, expected) => {
  if (!object(value) || Object.keys(value).length !== expected.length
    || Object.keys(value).some(key => !expected.includes(key))) fail('PUBLIC_FIELDS_REJECTED');
};
const utc = value => value == null ? null : instant(value) ? new Date(value).toISOString() : fail('PUBLIC_TIME_INVALID');

// This is a data-only projection, not a new research or ranking algorithm.
// No free text, links, quotes, account information, broker IDs or source IDs
// are included. Every public value is a bounded identifier, enum or timestamp.
export function publicSummary(bytes, before, after, receiptName, exportedAt) {
  assertSameMetadata(before, after);
  validateMetadata(before);
  const match = /^MARKET_(RESEARCH|POSTOPEN|DELIVERY_REVIEW)_RECEIPT_(\d{4}-\d{2}-\d{2})\.json$/.exec(receiptName);
  if (!match || !date(match[2]) || !instant(exportedAt) || bytes.byteLength !== before.size_bytes) fail('PUBLIC_SOURCE_INVALID');
  const kind = { RESEARCH: 'PREOPEN', POSTOPEN: 'POSTOPEN', DELIVERY_REVIEW: 'REVIEW' }[match[1]];
  let completedAt, publishedAt = null, candidates = [], delivery = 'NOT_RECORDED';
  if (kind !== 'REVIEW') {
    const projected = projectReceipt(bytes, before, exportedAt);
    if (projected.run_id !== `${match[2]}-${kind}`) fail('PUBLIC_SOURCE_DATE_CONFLICT');
    completedAt = projected.completed_at;
    publishedAt = projected.published_at;
    candidates = projected.candidates.filter(row => row.decision !== 'NOT_RECORDED')
      .map(row => ({ ticker: row.ticker, decision: row.decision, rank: row.candidate_rank }));
    const d = projected.delivery;
    if (d.write_performed === true && ['PASS', 'MATCH'].includes(d.reported_readback_status)) delivery = 'REPORTED_MATCH';
    else if (['BLOCKED_RESOURCE_UNAVAILABLE_WATCHLIST_UNCHANGED', 'BLOCKED', 'RESOURCE_UNAVAILABLE'].includes(d.reported_status)) delivery = 'REPORTED_BLOCKED';
    else if (['DELIVERY_FAILED', 'FAILED'].includes(d.reported_status)) delivery = 'REPORTED_FAILED';
  } else {
    const raw = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
    const d = parseReceiptJSON(raw);
    if (d.schema !== 'market-delivery-review-receipt.v1' || d.mode !== 'RESEARCH_ONLY'
      || d.resource_revision !== '2026-09-10.RECOVERY.4' || d.session_date_et !== match[2]) fail('PUBLIC_REVIEW_INVALID');
    const guard = d.prohibited_actions_confirmation;
    if (!guard || ['broker_list_write_performed_by_review', 'order_action_performed',
      'resource_change_performed', 'weight_change_performed', 'schedule_change_performed',
      'ready_or_execution_card_emitted'].some(key => guard[key] !== false)) fail('PUBLIC_REVIEW_GUARDRAIL_MISSING');
    completedAt = d.reviewed_at;
    delivery = ({ PARTIAL_DELIVERY: 'REPORTED_PARTIAL', DELIVERY_FAILED: 'REPORTED_FAILED',
      DELIVERED_AND_READBACK_VERIFIED: 'REPORTED_MATCH' })[d.overall_delivery_status] ?? 'NOT_RECORDED';
  }
  const record = { receipt_id: `${match[2]}-${kind}`, session_date_et: match[2], kind,
    completed_at: utc(completedAt), published_at: utc(publishedAt),
    source_version: before.version_id, source_modified_at: utc(before.modified_at),
    delivery_status: delivery, candidates };
  validateSummary(record, exportedAt);
  return record;
}

export function validateSummary(record, now) {
  keys(record, ['receipt_id', 'session_date_et', 'kind', 'completed_at', 'published_at',
    'source_version', 'source_modified_at', 'delivery_status', 'candidates']);
  if (!date(record.session_date_et) || !KINDS.includes(record.kind)
    || record.receipt_id !== `${record.session_date_et}-${record.kind}`
    || !DELIVERIES.includes(record.delivery_status)
    || !(record.source_version === null || typeof record.source_version === 'string' && /^\d{1,12}$/.test(record.source_version))
    || !Array.isArray(record.candidates) || record.candidates.length > 30) fail('PUBLIC_RECORD_INVALID');
  for (const field of ['completed_at', 'source_modified_at', 'published_at']) {
    if (field === 'published_at' && record[field] === null) continue;
    if (!instant(record[field]) || Date.parse(record[field]) > Date.parse(now) + 5000) fail('PUBLIC_TIME_INVALID');
  }
  const seen = new Set();
  for (const row of record.candidates) {
    keys(row, ['ticker', 'decision', 'rank']);
    if (!ticker(row.ticker) || seen.has(row.ticker) || !['WATCH', 'EXCLUDED', 'BLOCKED'].includes(row.decision)
      || !(row.rank === null || Number.isInteger(row.rank) && row.rank >= 1 && row.rank <= 10)) fail('PUBLIC_CANDIDATE_INVALID');
    seen.add(row.ticker);
  }
  if (record.kind === 'REVIEW' && record.candidates.length) fail('PUBLIC_REVIEW_INVALID');
  return record;
}

export function validatePublicFeed(feed, now = new Date().toISOString()) {
  keys(feed, ['schema_version', 'mode', 'ready_allowed', 'orders_allowed', 'exported_at', 'producer', 'records']);
  keys(feed.producer, ['status', 'checked_session_date_et', 'pending_kinds']);
  if (feed.schema_version !== 'market-public-feed.v1' || feed.mode !== 'RESEARCH_ONLY'
    || feed.ready_allowed !== false || feed.orders_allowed !== false || !instant(now)
    || !instant(feed.exported_at) || Date.parse(feed.exported_at) > Date.parse(now) + 5000
    || !STATES.includes(feed.producer.status) || !date(feed.producer.checked_session_date_et)
    || !Array.isArray(feed.producer.pending_kinds) || feed.producer.pending_kinds.length > 3
    || feed.producer.pending_kinds.some(kind => !KINDS.includes(kind))
    || new Set(feed.producer.pending_kinds).size !== feed.producer.pending_kinds.length
    || !Array.isArray(feed.records) || feed.records.length > 30) fail('PUBLIC_FEED_INVALID');
  const seen = new Set();
  for (const record of feed.records) {
    validateSummary(record, feed.exported_at);
    if (seen.has(record.receipt_id)) fail('PUBLIC_RECEIPT_DUPLICATE');
    seen.add(record.receipt_id);
  }
  return feed;
}

export function mergePublicFeed(previous, summaries, { now, sessionDate, status, pendingKinds = [] }) {
  if (previous) validatePublicFeed(previous, now);
  const records = new Map((previous?.records ?? []).map(record => [record.receipt_id, record]));
  for (const record of summaries) {
    validateSummary(record, now);
    const prior = records.get(record.receipt_id);
    if (prior && JSON.stringify(prior) !== JSON.stringify(record)) {
      if (record.source_version === null || prior.source_version === null
        || BigInt(record.source_version) <= BigInt(prior.source_version)) fail('PUBLIC_VERSION_CONFLICT');
    }
    records.set(record.receipt_id, record);
  }
  return validatePublicFeed({ schema_version: 'market-public-feed.v1', mode: 'RESEARCH_ONLY',
    ready_allowed: false, orders_allowed: false, exported_at: now,
    producer: { status, checked_session_date_et: sessionDate, pending_kinds: pendingKinds },
    records: [...records.values()].sort((a, b) => b.completed_at.localeCompare(a.completed_at)).slice(0, 30) }, now);
}
