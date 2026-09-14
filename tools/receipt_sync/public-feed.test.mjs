import test from 'node:test';
import assert from 'node:assert/strict';
import { publicSummary } from './public-feed.mjs';

const now = '2026-09-14T15:00:00.000Z';
const receiptName = 'MARKET_POSTOPEN_RECEIPT_2026-09-14.json';
const roles = [
  ['ACTIVE_SOURCE_OF_TRUTH', '2026-09-10.RECOVERY.4'],
  ['ACTIVE_RULES', '2026-09-10.RECOVERY.4'],
  ['ACTIVE_RUNTIME', '2026-09-10.RECOVERY.4'],
  ['ACTIVE_UNIVERSE', 'UNIVERSE_2026-09-02.1'],
  ['ACTIVE_STATIC_RELEASE_MANIFEST', '2026-09-10.RECOVERY.4']
];

function fixture() {
  return {
    schema_version: 'market-research-receipt.v1',
    run_type: 'POSTOPEN_REFRESH',
    mode: 'RESEARCH_ONLY',
    session_date_et: '2026-09-14',
    run_id: 'POSTOPEN_REFRESH-2026-09-14T14:18:24Z',
    started_at_utc: '2026-09-14T14:18:24Z',
    publication_refresh_completed_at_utc: '2026-09-14T14:27:21.201Z',
    resource_verification: {
      status: 'RESOURCE_UNAVAILABLE',
      content_and_identity_status: 'PASS',
      reads_complete_to_has_more_false: true,
      watchlist_write_allowed: false,
      resources: roles.map(([document_role, resource_revision], index) => ({
        document_role,
        resource_revision,
        status: 'PASS_CONTENT_IDENTITY',
        file_id: `file_${index}`,
        library_file_id: `libfile_${index}`
      }))
    },
    research_candidates: [
      { symbol: 'CRWD', status: 'WATCH', rank: 1, event: 'not projected' }
    ],
    output_limits: {
      ready: null, entry: null, stop: null, targets: null,
      quantity: null, orders: null
    },
    delivery: {
      status: 'WATCHLIST_WRITE_BLOCKED_RESOURCE_UNAVAILABLE',
      today_candidates_before: { symbols: ['CRWD'] },
      today_candidates_after_readback: { symbols: ['CRWD'], status: 'UNCHANGED_CONFIRMED' },
      favorites_modified: false,
      market_universe_modified: false,
      order_actions_performed: false
    }
  };
}

function project(receipt) {
  const bytes = new TextEncoder().encode(JSON.stringify(receipt));
  const metadata = {
    resource_id: 'libfile_source',
    file_id: 'file_source',
    version_id: null,
    size_bytes: bytes.byteLength,
    modified_at: '2026-09-14T14:29:06.378386Z'
  };
  return publicSummary(bytes, metadata, metadata, receiptName, now);
}

test('projects only bounded public fields from the alternate post-open profile', () => {
  const summary = project(fixture());
  assert.deepEqual(summary, {
    receipt_id: '2026-09-14-POSTOPEN',
    session_date_et: '2026-09-14',
    kind: 'POSTOPEN',
    completed_at: '2026-09-14T14:27:21.201Z',
    published_at: null,
    source_version: null,
    source_modified_at: '2026-09-14T14:29:06.378Z',
    delivery_status: 'REPORTED_BLOCKED',
    candidates: [{ ticker: 'CRWD', decision: 'WATCH', rank: 1 }]
  });
  assert.equal(JSON.stringify(summary).includes('not projected'), false);
  assert.equal(JSON.stringify(summary).includes('libfile_'), false);
  assert.equal(JSON.stringify(summary).includes('file_'), false);
});

test('rejects a receipt that weakens execution guardrails', () => {
  const receipt = fixture();
  receipt.output_limits.orders = false;
  assert.throws(() => project(receipt), /PUBLIC_POSTOPEN_ALTERNATE_GUARDRAIL_MISSING/);
});

test('rejects a receipt whose protected watchlist changed', () => {
  const receipt = fixture();
  receipt.delivery.today_candidates_after_readback.symbols = ['PANW'];
  assert.throws(() => project(receipt), /PUBLIC_POSTOPEN_ALTERNATE_GUARDRAIL_MISSING/);
});

test('rejects a receipt with mismatched active policy revision', () => {
  const receipt = fixture();
  receipt.resource_verification.resources[0].resource_revision = '2026-09-09.OLD';
  assert.throws(() => project(receipt), /PUBLIC_POSTOPEN_ALTERNATE_RESOURCE_INVALID/);
});
