/**
 * SQLite async worker — off-event-loop SQLite ops for the Spine organ.
 *
 * Phase-2 Strand 3 (binding-rule #40 architectural-class closure for
 * health-check + lifecycle-callback hot paths). EA-ratified at 2026-05-09
 * 1334 R / 1428 R per CEO orchestrator routing 1336 R.
 *
 * Architecture:
 * - Worker thread with its OWN better-sqlite3 read-write connection
 * - WAL mode preserved (busy_timeout = 5000); main thread also has its own
 *   connection; concurrent reads + serialized writes per WAL semantics
 * - Worker prepares statements at startup (sync-init-first; main thread runs
 *   schema DDL via init.js BEFORE adapter constructor spawns worker)
 * - Message protocol: parentPort.on('message', { id, op, params }) →
 *   parentPort.postMessage({ id, ok, result | error })
 * - Worker signals readiness via parentPort.postMessage({ type: 'ready' })
 *   BEFORE accepting op messages
 *
 * Ops covered (9 total):
 * Phase-1 deferred Strand-3 health-check (4):
 *   - getTotalMailboxDepth
 *   - expireMessages (DELETE; transactional via better-sqlite3 inline)
 *   - getMailboxesUnderPressure
 *   - getOrganStateCounts
 *
 * Phase-2 lifecycle callback chain (5 unique methods):
 *   - getEntity
 *   - transition (UPDATE + INSERT in db.transaction)
 *   - updateMailboxStatus (UPDATE; dispatches by status)
 *   - getMailboxDepth
 *   - persistEvent (INSERT; URN generated locally)
 *
 * Per EA Point 4 + Verdict 1 (interpretation b): adapter exposes parallel
 * `*Async` methods routed via this worker; existing sync methods preserved
 * untouched on main thread.
 */

import { parentPort, workerData } from 'node:worker_threads';
import Database from 'better-sqlite3';
import { generateUrn } from '../../lib/urn.js';

if (!parentPort) {
  throw new Error('sqlite-async-worker.js must be loaded as a Worker thread');
}

const { databasePath } = workerData || {};
if (!databasePath) {
  parentPort.postMessage({ type: 'error', error: 'workerData.databasePath required' });
  process.exit(1);
}

// --- Open worker's own better-sqlite3 connection ---

const db = new Database(databasePath, { fileMustExist: true });
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 5000');

// --- Prepare statements for the 9 ops ---

// Phase-1 health-check (4)
const stmtTotalMailboxDepth = db.prepare(
  'SELECT COUNT(*) as count FROM (SELECT 1 FROM mailbox_messages WHERE delivered = 0 LIMIT 1000000)'
);

const stmtExpireMessages = db.prepare(`
  DELETE FROM mailbox_messages
  WHERE ttl_seconds IS NOT NULL
    AND delivered = 0
    AND (julianday('now') - julianday(created_at)) * 86400 > ttl_seconds
`);

const stmtMailboxesUnderPressure = db.prepare(`
  SELECT target_organ, COUNT(*) as depth FROM mailbox_messages
  WHERE delivered = 0
  GROUP BY target_organ
  HAVING depth > ?
  LIMIT 100
`);

const stmtOrganStateCounts = db.prepare(`
  SELECT current_state, COUNT(*) as count FROM state_entities
  WHERE entity_type = 'organ'
  GROUP BY current_state
  LIMIT 100
`);

// Phase-2 lifecycle (5)
const stmtGetEntity = db.prepare(
  'SELECT * FROM state_entities WHERE entity_urn = ?'
);

const stmtUpdateEntityState = db.prepare(`
  UPDATE state_entities
  SET current_state = ?, updated_at = datetime('now')
  WHERE entity_urn = ?
`);

const stmtInsertTransition = db.prepare(`
  INSERT INTO state_transitions
    (transition_id, entity_urn, from_state, to_state, reason, actor)
  VALUES (?, ?, ?, ?, ?, ?)
`);

// Atomic transition: UPDATE state_entities + INSERT into state_transitions
const txTransition = db.transaction((urn, fromState, toState, transitionId, reason, actor) => {
  stmtUpdateEntityState.run(toState, urn);
  stmtInsertTransition.run(transitionId, urn, fromState, toState, reason, actor);
});

const stmtSetMailboxActive = db.prepare(`
  UPDATE mailboxes
  SET status = 'active',
      last_connected_at = datetime('now')
  WHERE organ_name = ?
`);

const stmtSetMailboxDisconnected = db.prepare(`
  UPDATE mailboxes
  SET status = 'disconnected'
  WHERE organ_name = ?
`);

const stmtMailboxDepth = db.prepare(
  'SELECT COUNT(*) as count FROM mailbox_messages WHERE target_organ = ? AND delivered = 0'
);

const stmtInsertEvent = db.prepare(`
  INSERT INTO events (urn, message_type, source_organ, target_organ, routing, envelope, created_at)
  VALUES (?, ?, ?, ?, ?, ?, ?)
`);

// --- Op handlers (each takes params array, returns result or throws) ---

const handlers = {
  // Phase-1 health-check
  getTotalMailboxDepth: () => {
    return stmtTotalMailboxDepth.get().count;
  },

  expireMessages: () => {
    const result = stmtExpireMessages.run();
    return result.changes;
  },

  getMailboxesUnderPressure: ([threshold]) => {
    return stmtMailboxesUnderPressure.all(threshold);
  },

  getOrganStateCounts: () => {
    const rows = stmtOrganStateCounts.all();
    return rows.reduce((acc, r) => { acc[r.current_state] = r.count; return acc; }, {});
  },

  // Phase-2 lifecycle
  getEntity: ([entityUrn]) => {
    const row = stmtGetEntity.get(entityUrn);
    if (!row) return null;
    return {
      entity_urn: row.entity_urn,
      entity_type: row.entity_type,
      current_state: row.current_state,
      metadata: row.metadata ? JSON.parse(row.metadata) : null,
      created_at: row.created_at,
      updated_at: row.updated_at,
    };
  },

  transition: ([entityUrn, fromState, toState, transitionId, reason, actor]) => {
    txTransition(entityUrn, fromState, toState, transitionId, reason, actor);
    // Return updated entity (mirrors sync transition() semantics)
    const row = stmtGetEntity.get(entityUrn);
    if (!row) return null;
    return {
      entity_urn: row.entity_urn,
      entity_type: row.entity_type,
      current_state: row.current_state,
      metadata: row.metadata ? JSON.parse(row.metadata) : null,
      created_at: row.created_at,
      updated_at: row.updated_at,
    };
  },

  updateMailboxStatus: ([organName, status]) => {
    if (status === 'active') {
      stmtSetMailboxActive.run(organName);
    } else if (status === 'disconnected') {
      stmtSetMailboxDisconnected.run(organName);
    }
    return { success: true };
  },

  getMailboxDepth: ([organName]) => {
    return stmtMailboxDepth.get(organName).count;
  },

  persistEvent: ([envelope, routing]) => {
    const urn = generateUrn('event');
    const now = new Date().toISOString();
    stmtInsertEvent.run(
      urn,
      envelope.type,
      envelope.source_organ,
      envelope.target_organ,
      routing,
      JSON.stringify(envelope),
      now,
    );
    return { urn, created_at: now };
  },
};

// --- Message protocol ---

parentPort.on('message', (msg) => {
  const { id, op, params } = msg || {};
  if (typeof id !== 'number' || !op) {
    parentPort.postMessage({ id, ok: false, error: 'malformed message: requires { id, op, params }' });
    return;
  }
  const handler = handlers[op];
  if (!handler) {
    parentPort.postMessage({ id, ok: false, error: `unknown op: ${op}` });
    return;
  }
  try {
    const result = handler(params || []);
    parentPort.postMessage({ id, ok: true, result });
  } catch (error) {
    parentPort.postMessage({
      id,
      ok: false,
      error: error.message,
      stack: error.stack,
    });
  }
});

// --- Cleanup on worker exit ---

process.on('exit', () => {
  try { db.close(); } catch { /* ignore */ }
});

// --- Signal ready (after statements prepared, before accepting ops) ---

parentPort.postMessage({ type: 'ready' });
