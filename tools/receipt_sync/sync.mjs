import { createHash } from 'node:crypto';

// Transport and faithful receipt projection only. No market requests or decisions.
export class SyncError extends Error {
  constructor(code) { super(code); this.code = code; }
}
const requireValue = (value, code) => { if (!value) throw new SyncError(code); };
const object = v => v !== null && typeof v === 'object' && !Array.isArray(v);
const timestamp = v => typeof v === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(v) && Number.isFinite(Date.parse(v));
const dayET = value => new Intl.DateTimeFormat('en-CA', {timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(value));
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const text = (v, max = 3000) => { requireValue(typeof v === 'string' && v.length <= max, 'INVALID_TEXT'); return v; };
const nullableText = v => v == null ? null : text(v);
const time = v => { requireValue(v == null || timestamp(v), 'INVALID_TIMESTAMP'); return v ?? null; };
const number = v => { requireValue(v == null || (typeof v === 'number' && Number.isFinite(v)), 'INVALID_NUMBER'); return v ?? null; };
const flag = v => { requireValue(v == null || typeof v === 'boolean', 'INVALID_BOOLEAN'); return v ?? null; };
const array = (v, max = 200) => { requireValue(Array.isArray(v) && v.length <= max, 'INVALID_ARRAY'); return v; };

// Reject duplicate JSON keys instead of allowing JSON.parse to hide a conflict.
export function parseReceiptJSON(raw) {
  requireValue(typeof raw === 'string' && Buffer.byteLength(raw) <= 512000, 'RECEIPT_TOO_LARGE');
  let i = 0;
  const ws = () => { while (i < raw.length && /[ \t\r\n]/.test(raw[i])) i++; };
  function string() {
    const start = i++;
    while (i < raw.length) {
      if (raw[i] === '\\') { i += 2; continue; }
      if (raw[i++] === '"') return JSON.parse(raw.slice(start, i));
    }
    throw new SyncError('INCOMPLETE_JSON');
  }
  function value(depth = 0) {
    requireValue(depth <= 40, 'JSON_DEPTH_EXCEEDED'); ws();
    if (raw[i] === '"') return string();
    if (raw[i] === '{') {
      i++; ws(); const result = Object.create(null); const seen = new Set();
      if (raw[i] === '}') { i++; return result; }
      while (i < raw.length) {
        ws(); requireValue(raw[i] === '"', 'INVALID_JSON'); const key = string();
        requireValue(!seen.has(key), 'DUPLICATE_JSON_KEY'); seen.add(key);
        ws(); requireValue(raw[i++] === ':', 'INVALID_JSON'); result[key] = value(depth + 1);
        ws(); const separator = raw[i++]; if (separator === '}') return result;
        requireValue(separator === ',', 'INVALID_JSON');
      }
    } else if (raw[i] === '[') {
      i++; ws(); const result = [];
      if (raw[i] === ']') { i++; return result; }
      while (i < raw.length) {
        requireValue(result.length < 2000, 'JSON_ARRAY_TOO_LARGE'); result.push(value(depth + 1));
        ws(); const separator = raw[i++]; if (separator === ']') return result;
        requireValue(separator === ',', 'INVALID_JSON');
      }
    } else {
      const match = /^(?:true|false|null|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)/.exec(raw.slice(i));
      requireValue(match, 'INVALID_JSON'); i += match[0].length; return JSON.parse(match[0]);
    }
    throw new SyncError('INCOMPLETE_JSON');
  }
  try { const result = value(); ws(); requireValue(i === raw.length, 'INVALID_JSON'); return result; }
  catch (e) { if (e instanceof SyncError) throw e; throw new SyncError('INVALID_JSON'); }
}

export function validateMetadata(m) {
  requireValue(object(m), 'SOURCE_METADATA_MISSING');
  for (const k of ['resource_id','file_id']) requireValue(typeof m[k] === 'string' && /^[A-Za-z0-9_-]{1,180}$/.test(m[k]), 'SOURCE_IDENTITY_MISSING');
  requireValue(m.version_id === null || (typeof m.version_id === 'string' && /^\d+$/.test(m.version_id)), 'SOURCE_VERSION_MISSING');
  requireValue(Number.isSafeInteger(m.size_bytes) && m.size_bytes > 0 && m.size_bytes <= 512000, 'SOURCE_SIZE_INVALID');
  requireValue(timestamp(m.modified_at), 'SOURCE_TIME_MISSING');
  return {resource_id:m.resource_id,file_id:m.file_id,version_id:m.version_id,size_bytes:m.size_bytes,modified_at:m.modified_at};
}
export function assertSameMetadata(a, b) {
  requireValue(JSON.stringify(validateMetadata(a)) === JSON.stringify(validateMetadata(b)), 'SOURCE_VERSION_CHANGED');
}
function alias(o, names) {
  const values = names.filter(k => o[k] != null).map(k => o[k]);
  requireValue(values.every(v => JSON.stringify(v) === JSON.stringify(values[0])), 'FIELD_ALIAS_CONFLICT');
  return values[0] ?? null;
}
function observation(o, path) {
  if (!o) return null;
  requireValue(object(o), 'OBSERVATION_INVALID');
  return {
    evidence_ref:path, source:nullableText(alias(o,['provider','source'])),
    source_timestamp:time(alias(o,['source_timestamp','trade_ts'])),
    collected_at:time(o.collection_timestamp),
    reported_age_seconds:number(alias(o,['age_at_collection_seconds','age_at_collection_s','age_seconds','age_s'])),
    reported_data_status:nullableText(o.status), price:number(o.price), change_pct:number(o.change_pct),
    volume:number(o.volume), halted:flag(o.halted), is_close:flag(o.is_close)
  };
}
function symbol(v) { requireValue(typeof v === 'string' && /^[A-Z0-9][A-Z0-9.\/-]{0,19}$/.test(v), 'SYMBOL_INVALID'); return v; }
function uniqueSymbols(rows) {
  const seen = new Set();
  for (const row of rows) { requireValue(object(row), 'ROW_INVALID'); const s = symbol(row.symbol); requireValue(!seen.has(s), 'DUPLICATE_TICKER'); seen.add(s); }
}
function sourceDecision(v) { requireValue(['WATCH','EXCLUDED','BLOCKED'].includes(v), 'UNSUPPORTED_RECORDED_DECISION'); return v; }

export function projectReceipt(bytes, metadata, syncedAt) {
  const source = validateMetadata(metadata);
  requireValue(bytes instanceof Uint8Array && bytes.byteLength === source.size_bytes, 'SOURCE_BYTES_INCOMPLETE');
  requireValue(timestamp(syncedAt), 'SYNC_TIME_INVALID');
  let raw;
  try { raw = new TextDecoder('utf-8', {fatal:true}).decode(bytes); } catch { throw new SyncError('INVALID_UTF8'); }
  const d = parseReceiptJSON(raw);
  requireValue(object(d) && d.schema === 'market-research-receipt.v1', 'UNSUPPORTED_RECEIPT_SCHEMA');
  // A supported historical adapter profile, never a claim of current policy verification.
  requireValue(d.resource_revision === '2026-09-10.RECOVERY.4', 'UNSUPPORTED_RECEIPT_REVISION');
  requireValue(d.mode === 'RESEARCH_ONLY', 'RESEARCH_ONLY_REQUIRED');
  requireValue(timestamp(d.started_at) && timestamp(d.completed_at), 'COMPLETED_RECEIPT_REQUIRED');
  const publishedAt = time(d.published_at);
  const scheduledFor = time(d.scheduled_for);
  requireValue(Date.parse(d.completed_at) >= Date.parse(d.started_at), 'RUN_TIME_CONFLICT');
  for (const t of [d.started_at,d.completed_at,publishedAt,source.modified_at].filter(Boolean))
    requireValue(Date.parse(t) <= Date.parse(syncedAt) + 5000, 'FUTURE_TIMESTAMP');
  requireValue(!publishedAt || Date.parse(publishedAt) >= Date.parse(d.started_at), 'RUN_TIME_CONFLICT');
  text(d.run_id,100);
  const match = /^(\d{4}-\d{2}-\d{2})-(PREOPEN|POSTOPEN)$/.exec(d.run_id);
  requireValue(match && match[1] === dayET(d.started_at), 'RUN_ID_DATE_CONFLICT');
  requireValue(d.session_date_et == null || d.session_date_et === match[1], 'SESSION_DATE_CONFLICT');
  requireValue(object(d.resource_verification), 'RESOURCE_EVIDENCE_MISSING');
  const pre = match[2] === 'PREOPEN';
  requireValue(pre ? Array.isArray(d.finalists_reviewed) && !d.candidates : Array.isArray(d.candidates) && !d.finalists_reviewed, 'UNSUPPORTED_RECEIPT_PROFILE');
  if (pre) {
    requireValue(d.guardrails?.ready_forbidden === true && d.guardrails?.order_actions_forbidden === true && d.delivery?.order_action_performed === false, 'RESEARCH_GUARDRAIL_EVIDENCE_MISSING');
    requireValue(d.readiness?.ready == null && d.readiness?.entry_stop_targets_quantity_emitted === false, 'EXECUTION_OUTPUT_FORBIDDEN');
  } else requireValue(d.ready_allowed === false && d.orders_allowed === false && d.order_action_performed === false, 'RESEARCH_GUARDRAIL_EVIDENCE_MISSING');
  requireValue(d.ready_allowed !== true && d.orders_allowed !== true && d.order_action_performed !== true, 'EXECUTION_OUTPUT_FORBIDDEN');
  const audit = array(d.price_research?.audit_rows);
  const finalists = array(pre ? d.finalists_reviewed : d.candidates,20);
  const excluded = array(d.exclusions ?? [],200);
  const publication = array(d.publication_freshness?.observations ?? [],20);
  for (const list of [audit,finalists,excluded,publication]) uniqueSymbols(list);
  const candidates = new Map();
  const base = ticker => ({ticker,decision:'NOT_RECORDED',decision_evidence_ref:null,reason:null,candidate_rank:null,
    reported_eligibility:null,reported_identity:null,audit_reason:null,observations:[],score_components:null,score_status:'NOT_PROJECTED',
    news_classification:null,news_headline:null,news_url:null,news_published_at:null});
  audit.forEach((r,i) => {
    const c=base(r.symbol); c.reported_eligibility=flag(alias(r,['candidate_eligible_at_broad_scan','ranking_eligible']));
    c.reported_identity=nullableText(r.identity_state); c.audit_reason=nullableText(r.exclusion_reason);
    c.observations.push(observation(r.price_observation ?? r,`#/price_research/audit_rows/${i}${r.price_observation?'/price_observation':''}`));
    candidates.set(r.symbol,c);
  });
  const ranks = new Set();
  finalists.forEach((r,i) => {
    const c = candidates.get(r.symbol) ?? base(r.symbol);
    c.decision=sourceDecision(alias(r,['decision','status']));
    c.decision_evidence_ref=`#/${pre?'finalists_reviewed':'candidates'}/${i}`;
    c.reason=nullableText(alias(r,['reason','rationale']));
    c.candidate_rank=number(r.rank);
    if(c.candidate_rank !== null) {
      requireValue(Number.isInteger(c.candidate_rank) && c.candidate_rank >= 1 && c.candidate_rank <= 10 && !ranks.has(c.candidate_rank), 'RANK_CONFLICT'); ranks.add(c.candidate_rank);
    }
    if(r.final_refresh) c.observations.push(observation(r.final_refresh,c.decision_evidence_ref+'/final_refresh'));
    if(r.news) {
      c.news_classification=nullableText(r.news.classification);c.news_headline=nullableText(r.news.headline);
      c.news_published_at=nullableText(alias(r.news,['published_at','published_date']));
      if(r.news.url != null) { const url=new URL(text(r.news.url));requireValue(url.protocol==='https:'&&!url.username&&!url.password,'NEWS_URL_INVALID');c.news_url=url.href; }
    }
    candidates.set(r.symbol,c);
  });
  excluded.forEach((r,i) => {
    const c=candidates.get(r.symbol)??base(r.symbol);
    requireValue(c.decision==='NOT_RECORDED', 'DECISION_CONFLICT');
    c.decision='EXCLUDED';c.reason=nullableText(r.reason);c.decision_evidence_ref=`#/exclusions/${i}`;candidates.set(r.symbol,c);
  });
  publication.forEach((r,i) => {
    const c=candidates.get(r.symbol)??base(r.symbol);
    c.observations.push(observation(r,`#/publication_freshness/observations/${i}`));candidates.set(r.symbol,c);
  });
  requireValue(candidates.size<=200,'RECEIPT_TICKER_LIMIT');
  const issues=[];
  if(!publishedAt) issues.push('PUBLISHED_AT_MISSING');
  if(!scheduledFor) issues.push('SCHEDULED_FOR_MISSING');
  issues.push('NO_ORIGINAL_EVENT_STREAM','SCORES_NOT_PROJECTED');
  const instrument = r => ({ticker:symbol(r.symbol),contract_id:nullableText(r.contract_id_ex)});
  const delivery = pre ? {
    reported_status:null,write_performed:flag(d.delivery.full_replace?.performed),
    intended_contract_ids:array(d.delivery.full_replace?.instruments??[],10).map(v=>text(v,40)),
    reported_readback_status:nullableText(d.delivery.readback?.status),
    readback:array(d.delivery.readback?.instruments??[],10).map(instrument),
    evidence_ref:'#/delivery'
  } : {
    reported_status:nullableText(d.delivery_status),write_performed:null,
    reported_submission_status:nullableText(d.watchlist_intended?.status),
    intended_contract_ids:array(d.watchlist_intended?.research_shortlist??[],10).map(v=>text(v.contract_id_ex,40)),
    reported_readback_status:nullableText(d.watchlist_readback?.status),
    readback:array(d.watchlist_readback?.instruments??[],10).map(instrument),evidence_ref:'#/watchlist_readback'
  };
  // No VERIFIED is synthesized from broker arrays. These are historical reported facts.
  return {
    schema_version:'market-synced-receipt.v1',display_state:'COMPLETED_RECEIPT',live_verified:false,
    ready_allowed:false,orders_allowed:false,run_id:d.run_id,session_date_et:match[1],
    receipt_profile:pre?'preopen-recovery4':'postopen-recovery4',recorded_resource_revision:d.resource_revision,
    current_policy_verification:'NOT_PERFORMED',scheduled_for:scheduledFor,started_at:d.started_at,
    completed_at:d.completed_at,published_at:publishedAt,synced_at:syncedAt,
    source:{...source,sha256:hash(bytes),hash_basis:'COMPUTED_FROM_RETRIEVED_BYTES_NOT_AN_INDEPENDENT_SIGNATURE'},
    resource_verification_reported:nullableText(alias(d.resource_verification,['overall_status','status'])),
    audit_rows_recorded:audit.length,candidates:[...candidates.values()],delivery,issues,
    original_events_available:false
  };
}

// The sink must enforce these checks atomically, with a unique resource/version key.
// No last-seen cursor may be committed before this receipt's durable acknowledgement.
export function compareImport(existing, incoming) {
  for (const row of existing) {
    if (row.run_id === incoming.run_id && row.source.resource_id !== incoming.source.resource_id) throw new SyncError('RUN_SOURCE_CONFLICT');
    if (row.source.resource_id !== incoming.source.resource_id) continue;
    requireValue(row.run_id === incoming.run_id, 'RESOURCE_RUN_CONFLICT');
    if (row.source.version_id === incoming.source.version_id) {
      requireValue(row.source.file_id === incoming.source.file_id && row.source.sha256 === incoming.source.sha256, 'VERSION_CONTENT_CONFLICT');
      return 'DUPLICATE';
    }
    requireValue(row.source.version_id !== null && incoming.source.version_id !== null, 'VERSIONLESS_RESOURCE_CHANGED');
    requireValue(BigInt(incoming.source.version_id) > BigInt(row.source.version_id), 'VERSION_ROLLBACK');
  }
  return 'APPEND';
}

const deadline = (fn, timeoutMs) => new Promise((resolve,reject) => {
  const id=setTimeout(()=>reject(new SyncError('ADAPTER_TIMEOUT')),timeoutMs);
  Promise.resolve().then(fn).then(resolve,reject).finally(()=>clearTimeout(id));
});
export async function syncOnce({source=null,sink=null,now=()=>new Date().toISOString(),maxPages=10,timeoutMs=15000}={}) {
  if (!source || !sink) return {status:'BLOCKED',blocker:'SOURCE_OR_SINK_NOT_CONFIGURED',items:[]};
  // Capability contracts are configuration supplied by a trusted adapter, not receipt fields.
  if(source.access!=='READ_ONLY' || sink.atomicCommit!==true)
    return {status:'BLOCKED',blocker:'ADAPTER_CAPABILITIES_UNVERIFIED',items:[]};
  const items=[];const seen=new Map();const cursors=new Set();let cursor=null;
  try {
    requireValue(Number.isInteger(maxPages)&&maxPages>=1&&maxPages<=20&&Number.isFinite(timeoutMs)&&timeoutMs>0&&timeoutMs<=30000,'SYNC_BUDGET_INVALID');
    for(let pageIndex=0;pageIndex<maxPages;pageIndex++) {
      const page=await deadline(()=>source.listReceipts({cursor,limit:50}),timeoutMs);
      requireValue(object(page)&&Array.isArray(page.items)&&page.items.length<=50&&('next_cursor' in page),'SOURCE_PAGE_INCOMPLETE');
      requireValue(page.next_cursor===null || typeof page.next_cursor==='string'&&page.next_cursor.length>0,'SOURCE_PAGE_INCOMPLETE');
      for(const item of page.items) {
        const metadata=validateMetadata(item);
        if(seen.has(metadata.resource_id)) { assertSameMetadata(seen.get(metadata.resource_id),metadata);continue; }
        seen.set(metadata.resource_id,metadata);
        try {
          const read=await deadline(()=>source.readVersion({...metadata}),timeoutMs);
          requireValue(read?.complete===true,'SOURCE_READ_INCOMPLETE');assertSameMetadata(metadata,read.metadata);
          const after=await deadline(()=>source.getMetadata(metadata.resource_id),timeoutMs);assertSameMetadata(metadata,after);
          const record=projectReceipt(read.bytes,metadata,now());
          const ack=await deadline(()=>sink.commit(record),timeoutMs);
          requireValue(['STORED','DUPLICATE'].includes(ack?.status)&&ack.resource_id===metadata.resource_id&&ack.version_id===metadata.version_id&&ack.sha256===record.source.sha256,'SINK_ACK_MISMATCH');
          items.push({resource_id:metadata.resource_id,run_id:record.run_id,status:ack.status});
        } catch(e) {
          items.push({resource_id:metadata.resource_id,status:'BLOCKED',blocker:e instanceof SyncError?e.code:'ADAPTER_FAILURE'});
        }
      }
      if(page.next_cursor===null) return {status:items.some(i=>i.status==='BLOCKED')?'PARTIAL':'COMPLETE',items};
      requireValue(!cursors.has(page.next_cursor),'SOURCE_CURSOR_LOOP');cursors.add(page.next_cursor);cursor=page.next_cursor;
    }
    return {status:'PARTIAL',blocker:'SOURCE_PAGE_BUDGET_EXCEEDED',items};
  } catch(e) { return {status:'BLOCKED',blocker:e instanceof SyncError?e.code:'SOURCE_UNAVAILABLE',items}; }
}
