import { readFileSync, writeFileSync } from 'node:fs';
import { resolve, basename } from 'node:path';
import { publicSummary, mergePublicFeed, validatePublicFeed } from '../receipt_sync/public-feed.mjs';

// Explicit local inputs only. No network, broker connector, credentials or shell.
// Keep this private manifest and the original files out of the public repository.
const manifest = JSON.parse(readFileSync(process.argv[2], 'utf8'));
const now = new Date().toISOString();
if (!Array.isArray(manifest.files) || manifest.files.length > 10) throw new Error('SOURCE_BATCH_INVALID');
const summaries = manifest.files.map(item => {
  const path = resolve(item.path);
  if (basename(path) !== item.receipt_name) throw new Error('SOURCE_NAME_MISMATCH');
  return publicSummary(readFileSync(path), item.before, item.after, item.receipt_name, now);
});
const previous = manifest.previous_feed ? validatePublicFeed(JSON.parse(readFileSync(manifest.previous_feed, 'utf8')), now) : null;
const output = mergePublicFeed(previous, summaries, { now, sessionDate: manifest.session_date_et,
  status: manifest.status, pendingKinds: manifest.pending_kinds ?? [] });
const serialized = JSON.stringify(output, null, 2) + '\n';
writeFileSync(process.argv[3], serialized, { mode: 0o600 });
console.log(JSON.stringify({ status: 'VALIDATED', receipts: summaries.length,
  public_bytes: Buffer.byteLength(serialized) }));
