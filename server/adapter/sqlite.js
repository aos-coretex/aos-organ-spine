/**
 * SQLiteStorageAdapter — concrete StorageAdapter backed by better-sqlite3.
 *
 * This is the ONLY module that calls db.prepare(). All other Spine modules
 * access storage exclusively through the adapter methods.
 *
 * Relay 5.
 */

import { StorageAdapter } from './interface.js';
import { generateUrn } from '../../lib/urn.js';
import { Worker } from 'node:worker_threads';
import { fileURLToPath } from 'node:url';
import { dirname, resolve as pathResolve } from 'node:path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const WORKER_PATH = pathResolve(__dirname, 'sqlite-async-worker.js');

function _workerLog(event, data = {}) {
  const entry = { timestamp: new Date().toISOString(), event, ...data };
  process.stdout.write(JSON.stringify(entry) + '\n');
}

/**
 * Phase-2 Strand 3 error types for the worker-thread async API.
 * Per EA Verdict 2 ratification (2026-05-09 1428 R).
 */
export class WorkerQueueFullError extends Error {
  constructor(message) {
    super(message);
    this.name = 'WorkerQueueFullError';
    this.code = 'WORKER_QUEUE_FULL';
  }
}

export class WorkerCrashedError extends Error {
  constructor(message) {
    super(message);
    this.name = 'WorkerCrashedError';
    this.code = 'WORKER_CRASHED';
  }
}

export class SQLiteStorageAdapter extends StorageAdapter {

  /** @param {import('better-sqlite3').Database} db */
  constructor(db) {
    super();
    this._db = db;
    this._prepareStatements();

    // Phase-2 Strand 3: spawn async worker for off-event-loop ops
    // (binding-rule #40 architectural-class closure for health-check +
    // lifecycle-callback hot paths). Per EA Point 1: NOT lazy spawn.
    // Per EA Verdict 2 path (i): sync init (CREATE TABLE) runs BEFORE
    // adapter constructor; worker spawns AFTER schema is ready.
    this._worker = null;
    this._workerReady = null;
    this._pendingRequests = new Map();
    this._nextRequestId = 1;
    this._queueDepth = 0;
    this._maxPendingRequests = 1000;
    this._lastCrashAt = null;
    this._terminating = false;
    this._databasePath = db.name;

    // Skip worker for in-memory DBs (test mode); each in-memory db is
    // connection-private so the worker cannot share schema with main thread.
    // Async methods fall back to sync execution when this._worker is null.
    if (this._databasePath && this._databasePath !== ':memory:') {
      this._spawnWorker();
    }
  }

  _prepareStatements() {
    const db = this._db;

    // --- Events (Relay 5) ---

    this._insertEvent = db.prepare(`
      INSERT INTO events (urn, message_type, source_organ, target_organ, routing, envelope, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `);

    this._getEvent = db.prepare(
      'SELECT * FROM events WHERE urn = ?'
    );

    // queryEvents is built dynamically — no prepared statement here

    // --- Event Schemas (Relay 5) ---

    this._insertEventSchema = db.prepare(`
      INSERT INTO event_schemas (urn, event_type, version, fields, description)
      VALUES (?, ?, ?, ?, ?)
    `);

    this._getEventSchemaLatest = db.prepare(
      'SELECT * FROM event_schemas WHERE event_type = ? ORDER BY version DESC LIMIT 1'
    );

    this._listEventSchemas = db.prepare(
      'SELECT * FROM event_schemas ORDER BY event_type, version DESC'
    );

    // --- State (Relay 1) ---

    this._insertMachine = db.prepare(`
      INSERT INTO state_machine_defs
        (entity_type, states, transitions, initial_state, terminal_states)
      VALUES (?, ?, ?, ?, ?)
    `);

    this._insertMachineIdempotent = db.prepare(`
      INSERT OR IGNORE INTO state_machine_defs
        (entity_type, states, transitions, initial_state, terminal_states)
      VALUES (?, ?, ?, ?, ?)
    `);

    this._getMachine = db.prepare(
      'SELECT * FROM state_machine_defs WHERE entity_type = ?'
    );

    this._getAllMachines = db.prepare(
      'SELECT * FROM state_machine_defs ORDER BY entity_type'
    );

    this._insertEntity = db.prepare(`
      INSERT INTO state_entities
        (entity_urn, entity_type, current_state, metadata)
      VALUES (?, ?, ?, ?)
    `);

    this._getEntityRow = db.prepare(
      'SELECT * FROM state_entities WHERE entity_urn = ?'
    );

    this._updateEntityState = db.prepare(`
      UPDATE state_entities
      SET current_state = ?, updated_at = datetime('now')
      WHERE entity_urn = ?
    `);

    this._insertTransition = db.prepare(`
      INSERT INTO state_transitions
        (transition_id, entity_urn, from_state, to_state, reason, actor)
      VALUES (?, ?, ?, ?, ?, ?)
    `);

    this._getTransitionsStmt = db.prepare(
      'SELECT * FROM state_transitions WHERE entity_urn = ? ORDER BY timestamp ASC'
    );

    // Atomic transition: update state + insert transition record
    this._doTransition = db.transaction((entityUrn, fromState, toState, transitionId, reason, actor) => {
      this._updateEntityState.run(toState, entityUrn);
      this._insertTransition.run(transitionId, entityUrn, fromState, toState, reason, actor);
    });

    // --- Mailboxes (Relay 2) ---

    this._getMailboxStmt = db.prepare(
      'SELECT * FROM mailboxes WHERE organ_name = ?'
    );

    this._insertMailbox = db.prepare(
      'INSERT OR IGNORE INTO mailboxes (organ_name) VALUES (?)'
    );

    this._pendingCount = db.prepare(
      'SELECT COUNT(*) as count FROM mailbox_messages WHERE target_organ = ? AND delivered = 0'
    );

    this._oldestPending = db.prepare(
      'SELECT MIN(created_at) as oldest FROM mailbox_messages WHERE target_organ = ? AND delivered = 0'
    );

    this._drainMessages = db.prepare(
      `SELECT message_id, envelope FROM mailbox_messages
       WHERE target_organ = ? AND delivered = 0
       ORDER BY created_at ASC
       LIMIT ?`
    );

    this._updateLastDrainStmt = db.prepare(
      "UPDATE mailboxes SET last_drain_at = datetime('now') WHERE organ_name = ?"
    );

    this._ackMessage = db.prepare(
      "UPDATE mailbox_messages SET delivered = 1, delivered_at = datetime('now') WHERE message_id = ? AND delivered = 0"
    );

    this._ackTransaction = db.transaction((ids) => {
      let acknowledged = 0;
      for (const id of ids) {
        const result = this._ackMessage.run(id);
        acknowledged += result.changes;
      }
      return acknowledged;
    });

    this._insertMailboxMessage = db.prepare(`
      INSERT INTO mailbox_messages (message_id, target_organ, source_organ, envelope, delivered, delivered_at, ttl_seconds)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `);

    this._setActive = db.prepare(
      "UPDATE mailboxes SET status = 'active', last_connected_at = datetime('now') WHERE organ_name = ?"
    );

    this._setDisconnected = db.prepare(
      "UPDATE mailboxes SET status = 'disconnected' WHERE organ_name = ?"
    );

    this._pendingMessages = db.prepare(
      `SELECT message_id, envelope FROM mailbox_messages
       WHERE target_organ = ? AND delivered = 0
       ORDER BY created_at ASC`
    );

    this._markDelivered = db.prepare(
      "UPDATE mailbox_messages SET delivered = 1, delivered_at = datetime('now') WHERE message_id = ?"
    );

    // --- Organ Manifest (Relay 4) ---

    this._getAllManifest = db.prepare(
      'SELECT * FROM organ_manifest ORDER BY organ_id'
    );

    this._getOneManifest = db.prepare(
      'SELECT * FROM organ_manifest WHERE organ_id = ?'
    );

    this._insertManifest = db.prepare(
      'INSERT OR IGNORE INTO organ_manifest (organ_id, required) VALUES (?, ?)'
    );

    // --- Subscription Registry (Relay 4) ---

    this._insertSubscription = db.prepare(`
      INSERT OR REPLACE INTO subscription_registry
        (organ_id, subscription_type, topic_filter, filter_mode, registered_at)
      VALUES (?, 'broadcast', ?, 'AND', datetime('now'))
    `);

    this._deleteSubscription = db.prepare(
      "DELETE FROM subscription_registry WHERE organ_id = ? AND subscription_type = 'broadcast' AND topic_filter = ?"
    );

    this._getOrganSubscriptions = db.prepare(
      "SELECT * FROM subscription_registry WHERE organ_id = ? AND subscription_type = 'broadcast'"
    );

    this._getAllBroadcastSubscriptions = db.prepare(
      "SELECT * FROM subscription_registry WHERE subscription_type = 'broadcast'"
    );

    this._getAllSubscriptionsForDisplay = db.prepare(
      'SELECT organ_id, topic_filter, registered_at FROM subscription_registry ORDER BY organ_id'
    );

    this._getOrganSubscriptionsForDisplay = db.prepare(
      'SELECT topic_filter, registered_at FROM subscription_registry WHERE organ_id = ? ORDER BY registered_at'
    );

    // --- Schema Changelog (Relay 3) ---

    this._insertChangelog = db.prepare(`
      INSERT OR IGNORE INTO schema_changelog
        (message_type, version, change_description, fields_added, fields_removed, fields_modified)
      VALUES (?, ?, ?, ?, ?, ?)
    `);

    this._getChangelogStmt = db.prepare(
      'SELECT * FROM schema_changelog ORDER BY id'
    );

    this._changelogCount = db.prepare(
      'SELECT COUNT(*) as count FROM schema_changelog WHERE version = ?'
    );

    // --- Relay 6: Health monitoring ---

    this._expireMessages = db.prepare(`
      DELETE FROM mailbox_messages
      WHERE ttl_seconds IS NOT NULL
        AND delivered = 0
        AND (julianday('now') - julianday(created_at)) * 86400 > ttl_seconds
    `);

    // Phase-1 wedge-fix (2026-05-09 spine-wedge-recurrence-prevention):
    // Strand 2 — LIMIT clauses + COUNT short-circuiting (defense-in-depth).
    // The COUNT(*) is wrapped in a subquery LIMIT 1_000_000 so worst-case
    // scan cost is bounded; if undelivered mailbox depth exceeds 1M, the
    // count saturates at 1M which carries enough signal for health/alerting
    // (>1M backlog is itself a critical-condition signal regardless of
    // exact count). Per E-ORG spec 2026-05-09 spine-wedge-fix-relay-prompt-body.md.
    this._totalMailboxDepth = db.prepare(
      'SELECT COUNT(*) as count FROM (SELECT 1 FROM mailbox_messages WHERE delivered = 0 LIMIT 1000000)'
    );

    // Phase-1 wedge-fix Strand 2: bounded result-set safety net.
    // N organs is small (~30) so LIMIT 100 is non-active under normal
    // conditions but caps worst-case under degraded GROUP BY plans.
    this._mailboxesUnderPressure = db.prepare(`
      SELECT target_organ, COUNT(*) as depth FROM mailbox_messages
      WHERE delivered = 0
      GROUP BY target_organ
      HAVING depth > ?
      LIMIT 100
    `);

    // Phase-1 wedge-fix Strand 2: bounded result-set safety net.
    // N organ states is small (~5-10) so LIMIT 100 is non-active under normal
    // conditions but caps worst-case under degraded GROUP BY plans.
    this._organStateCounts = db.prepare(`
      SELECT current_state, COUNT(*) as count FROM state_entities
      WHERE entity_type = 'organ'
      GROUP BY current_state
      LIMIT 100
    `);

    // --- Diagnostics ---

    this._healthPing = db.prepare('SELECT 1');

    this._countEntities = db.prepare(
      'SELECT COUNT(*) as count FROM state_entities'
    );

    this._entitiesByType = db.prepare(
      'SELECT entity_type, COUNT(*) as count FROM state_entities GROUP BY entity_type'
    );

    this._entitiesByState = db.prepare(
      'SELECT current_state, COUNT(*) as count FROM state_entities GROUP BY current_state'
    );

    this._countTransitions = db.prepare(
      'SELECT COUNT(*) as count FROM state_transitions'
    );

    this._lastTransition = db.prepare(
      'SELECT * FROM state_transitions ORDER BY timestamp DESC LIMIT 1'
    );

    this._listTablesStmt = db.prepare(
      "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    );
  }

  // ================================================================
  // Events (Relay 5)
  // ================================================================

  persistEvent(envelope, routing) {
    const urn = generateUrn('event');
    const now = new Date().toISOString();
    this._insertEvent.run(
      urn,
      envelope.type,
      envelope.source_organ,
      envelope.target_organ,
      routing,
      JSON.stringify(envelope),
      now,
    );
    return { urn, created_at: now };
  }

  queryEvents(filters = {}) {
    const conditions = [];
    const params = [];

    if (filters.type) {
      conditions.push('message_type = ?');
      params.push(filters.type);
    }
    if (filters.source_organ) {
      conditions.push('source_organ = ?');
      params.push(filters.source_organ);
    }
    if (filters.target_organ) {
      conditions.push('target_organ = ?');
      params.push(filters.target_organ);
    }
    if (filters.routing) {
      conditions.push('routing = ?');
      params.push(filters.routing);
    }
    if (filters.since) {
      conditions.push('created_at >= ?');
      params.push(filters.since);
    }
    if (filters.until) {
      conditions.push('created_at <= ?');
      params.push(filters.until);
    }

    let limit = parseInt(filters.limit, 10) || 100;
    if (limit < 1) limit = 1;
    if (limit > 1000) limit = 1000;

    const where = conditions.length > 0
      ? 'WHERE ' + conditions.join(' AND ')
      : '';

    const sql = `SELECT * FROM events ${where} ORDER BY created_at DESC LIMIT ?`;
    params.push(limit);

    const rows = this._db.prepare(sql).all(...params);
    const events = rows.map(r => this._parseEventRow(r));
    return { events, count: events.length };
  }

  getEvent(urn) {
    const row = this._getEvent.get(urn);
    return row ? this._parseEventRow(row) : null;
  }

  _parseEventRow(row) {
    return {
      urn: row.urn,
      message_type: row.message_type,
      source_organ: row.source_organ,
      target_organ: row.target_organ,
      routing: row.routing,
      envelope: JSON.parse(row.envelope),
      created_at: row.created_at,
    };
  }

  // ================================================================
  // State (Relay 1)
  // ================================================================

  getStateMachineDef(entityType) {
    const row = this._getMachine.get(entityType);
    return row ? this._parseMachineRow(row) : null;
  }

  registerStateMachineDef(entityType, def, { idempotent = false } = {}) {
    const stmt = idempotent ? this._insertMachineIdempotent : this._insertMachine;
    stmt.run(
      entityType,
      JSON.stringify(def.states),
      JSON.stringify(def.transitions),
      def.initial_state,
      JSON.stringify(def.terminal_states),
    );
    return this.getStateMachineDef(entityType);
  }

  listStateMachineDefs() {
    return this._getAllMachines.all().map(r => this._parseMachineRow(r));
  }

  createEntity(entityUrn, entityType, initialState, meta = null) {
    this._insertEntity.run(
      entityUrn,
      entityType,
      initialState,
      meta ? JSON.stringify(meta) : null,
    );
    return this.getEntity(entityUrn);
  }

  getEntity(entityUrn) {
    const row = this._getEntityRow.get(entityUrn);
    if (!row) return null;
    return {
      entity_urn: row.entity_urn,
      entity_type: row.entity_type,
      current_state: row.current_state,
      metadata: row.metadata ? JSON.parse(row.metadata) : null,
      created_at: row.created_at,
      updated_at: row.updated_at,
    };
  }

  getTransitions(entityUrn) {
    return this._getTransitionsStmt.all(entityUrn);
  }

  transition(entityUrn, fromState, toState, transitionId, reason, actor) {
    this._doTransition(entityUrn, fromState, toState, transitionId, reason, actor);
    return this.getEntity(entityUrn);
  }

  _parseMachineRow(row) {
    return {
      entity_type: row.entity_type,
      states: JSON.parse(row.states),
      transitions: JSON.parse(row.transitions),
      initial_state: row.initial_state,
      terminal_states: JSON.parse(row.terminal_states),
      created_at: row.created_at,
    };
  }

  // ================================================================
  // Mailboxes (Relay 2)
  // ================================================================

  registerMailbox(organName) {
    this._insertMailbox.run(organName);
    return this.getMailbox(organName);
  }

  getMailbox(organName) {
    return this._getMailboxStmt.get(organName) || null;
  }

  getMailboxDepth(organName) {
    return this._pendingCount.get(organName).count;
  }

  getOldestPending(organName) {
    return this._oldestPending.get(organName).oldest || null;
  }

  persistMailboxMessage(messageId, targetOrgan, sourceOrgan, envelope, delivered = false, ttlSeconds = null) {
    this._insertMailboxMessage.run(
      messageId,
      targetOrgan,
      sourceOrgan,
      JSON.stringify(envelope),
      delivered ? 1 : 0,
      delivered ? new Date().toISOString() : null,
      ttlSeconds,
    );
  }

  drainMailbox(organName, limit) {
    return this._drainMessages.all(organName, limit);
  }

  updateLastDrain(organName) {
    this._updateLastDrainStmt.run(organName);
  }

  ackMessages(messageIds) {
    return this._ackTransaction(messageIds);
  }

  setMailboxActive(organName) {
    this._setActive.run(organName);
  }

  setMailboxDisconnected(organName) {
    this._setDisconnected.run(organName);
  }

  getPendingMessages(organName) {
    return this._pendingMessages.all(organName);
  }

  markMessageDelivered(messageId) {
    this._markDelivered.run(messageId);
  }

  // ================================================================
  // Event Schemas (Relay 5)
  // ================================================================

  registerEventSchema(eventType, version, fields, description = null) {
    const urn = `urn:llm-ops:event-schema:${eventType}:v${version}`;
    this._insertEventSchema.run(
      urn,
      eventType,
      version,
      JSON.stringify(fields),
      description,
    );
    return {
      urn,
      event_type: eventType,
      version,
      fields,
      description,
      registered_at: new Date().toISOString(),
    };
  }

  getEventSchema(eventType) {
    const row = this._getEventSchemaLatest.get(eventType);
    if (!row) return null;
    return {
      urn: row.urn,
      event_type: row.event_type,
      version: row.version,
      fields: JSON.parse(row.fields),
      description: row.description,
      registered_at: row.registered_at,
    };
  }

  listEventSchemas() {
    return this._listEventSchemas.all().map(r => ({
      urn: r.urn,
      event_type: r.event_type,
      version: r.version,
      fields: JSON.parse(r.fields),
      description: r.description,
      registered_at: r.registered_at,
    }));
  }

  // ================================================================
  // Organ Manifest (Relay 4)
  // ================================================================

  getManifest() {
    return this._getAllManifest.all().map(r => ({
      organ_id: r.organ_id,
      required: r.required === 1,
      expected_subscriptions: r.expected_subscriptions
        ? JSON.parse(r.expected_subscriptions)
        : null,
      registered_at: r.registered_at,
    }));
  }

  getManifestEntry(organId) {
    const row = this._getOneManifest.get(organId);
    if (!row) return null;
    return {
      organ_id: row.organ_id,
      required: row.required === 1,
      expected_subscriptions: row.expected_subscriptions
        ? JSON.parse(row.expected_subscriptions)
        : null,
      registered_at: row.registered_at,
    };
  }

  addManifestEntry(organId, required = false) {
    const result = this._insertManifest.run(organId, required ? 1 : 0);
    if (result.changes === 0) return null; // already exists
    this._insertMailbox.run(organId);
    return this.getManifestEntry(organId);
  }

  // ================================================================
  // Subscription Registry (Relay 4)
  // ================================================================

  getSubscriptions(organId) {
    return this._getOrganSubscriptions.all(organId);
  }

  getAllSubscriptions() {
    return this._getAllBroadcastSubscriptions.all();
  }

  addSubscription(organId, filter) {
    this._insertSubscription.run(organId, filter);
  }

  removeSubscription(organId, filter) {
    this._deleteSubscription.run(organId, filter);
  }

  /** Get all subscriptions formatted for display (with organ_id). */
  getSubscriptionsForDisplay() {
    return this._getAllSubscriptionsForDisplay.all();
  }

  /** Get subscriptions for one organ formatted for display. */
  getOrganSubscriptionsForDisplay(organId) {
    return this._getOrganSubscriptionsForDisplay.all(organId);
  }

  // ================================================================
  // Schema Changelog (Relay 3)
  // ================================================================

  addChangelogEntry(messageType, version, description, added = null, removed = null, modified = null) {
    this._insertChangelog.run(
      messageType,
      version,
      description,
      added ? JSON.stringify(added) : null,
      removed ? JSON.stringify(removed) : null,
      modified ? JSON.stringify(modified) : null,
    );
  }

  getChangelog() {
    return this._getChangelogStmt.all();
  }

  /** Check if changelog has entries for a given version. */
  hasChangelogVersion(version) {
    return this._changelogCount.get(version).count > 0;
  }

  // ================================================================
  // Relay 6: Health Monitoring
  // ================================================================

  updateMailboxStatus(organName, status) {
    if (status === 'active') {
      this.setMailboxActive(organName);
    } else if (status === 'disconnected') {
      this.setMailboxDisconnected(organName);
    }
  }

  expireMessages() {
    const result = this._expireMessages.run();
    return result.changes;
  }

  getTotalMailboxDepth() {
    return this._totalMailboxDepth.get().count;
  }

  getMailboxesUnderPressure(threshold) {
    return this._mailboxesUnderPressure.all(threshold);
  }

  getOrganStateCounts() {
    return this._organStateCounts.all()
      .reduce((acc, r) => { acc[r.current_state] = r.count; return acc; }, {});
  }

  // ================================================================
  // Diagnostics
  // ================================================================

  healthCheck() {
    try {
      this._healthPing.get();
      return true;
    } catch {
      return false;
    }
  }

  getStats() {
    const totalEntities = this._countEntities.get().count;

    const byType = this._entitiesByType.all()
      .reduce((acc, r) => { acc[r.entity_type] = r.count; return acc; }, {});

    const byState = this._entitiesByState.all()
      .reduce((acc, r) => { acc[r.current_state] = r.count; return acc; }, {});

    const totalTransitions = this._countTransitions.get().count;
    const lastTransition = this._lastTransition.get() || null;

    return {
      state_entities: { total: totalEntities, by_type: byType, by_state: byState },
      state_transitions: { total: totalTransitions, last_transition: lastTransition },
    };
  }

  getTables() {
    return this._listTablesStmt.all().map(r => r.name);
  }

  // ================================================================
  // Phase-2 Strand 3: Async worker plumbing (binding-rule #40)
  // ================================================================

  /**
   * Spawn the SQLite async worker thread.
   * Called once at constructor; respawned at most once on crash with 30s
   * safety window (replacement crash within 30s → process.exit(1) for
   * LaunchAgent-mediated full Spine restart).
   */
  _spawnWorker() {
    this._worker = new Worker(WORKER_PATH, {
      workerData: { databasePath: this._databasePath },
    });

    this._workerReady = new Promise((resolve, reject) => {
      const onReadyMsg = (msg) => {
        if (msg && msg.type === 'ready') {
          this._worker.off('message', onReadyMsg);
          resolve();
        }
      };
      this._worker.on('message', onReadyMsg);
      // If worker errors before ready, the error handler below rejects;
      // also wire up a one-shot reject for that path.
      this._worker.once('error', (err) => {
        // Only reject if we never resolved (i.e., still waiting for ready)
        // Otherwise this is handled by the steady-state error handler.
        try { reject(err); } catch { /* already resolved */ }
      });
    });

    this._worker.on('message', (msg) => this._handleWorkerMessage(msg));
    this._worker.on('exit', (code) => this._handleWorkerExit(code));
    this._worker.on('error', (err) => this._handleWorkerError(err));
  }

  /** Steady-state worker message dispatcher. */
  _handleWorkerMessage(msg) {
    if (!msg || msg.type === 'ready') return; // ready handled by _workerReady
    const { id, ok, result, error, stack } = msg;
    if (typeof id !== 'number') return; // malformed; ignore
    const pending = this._pendingRequests.get(id);
    if (!pending) return; // unknown id (already settled or cleared)
    this._pendingRequests.delete(id);
    this._queueDepth--;
    if (ok) {
      pending.resolve(result);
    } else {
      const err = new Error(error || 'worker error');
      if (stack) err.workerStack = stack;
      pending.reject(err);
    }
  }

  _handleWorkerExit(code) {
    _workerLog('spine_sqlite_worker_exit', { code });
    if (this._terminating) return; // expected exit during close()
    this._handleWorkerCrash();
  }

  _handleWorkerError(err) {
    _workerLog('spine_sqlite_worker_error', { error: err.message });
    // 'error' event is followed by 'exit' which triggers crash recovery;
    // here we just log + reject pending (defensive — exit handler also rejects).
  }

  _handleWorkerCrash() {
    // Reject all pending requests
    for (const [, pending] of this._pendingRequests) {
      pending.reject(new WorkerCrashedError('worker crashed; pending request rejected'));
    }
    this._pendingRequests.clear();
    this._queueDepth = 0;

    // Crash recovery: single restart attempt with 30s safety window
    const now = Date.now();
    if (this._lastCrashAt && (now - this._lastCrashAt) < 30000) {
      _workerLog('spine_sqlite_worker_crashed_repeatedly', {
        first_crash_ms: this._lastCrashAt,
        second_crash_ms: now,
        elapsed_ms: now - this._lastCrashAt,
        action: 'process_exit_for_launchagent_restart',
      });
      process.exit(1); // LaunchAgent restarts Spine
      return;
    }

    this._lastCrashAt = now;
    _workerLog('spine_sqlite_worker_respawning');
    this._worker = null;
    this._workerReady = null;
    this._spawnWorker();
  }

  /**
   * Send an op message to the worker; return Promise resolving to result
   * (or rejecting with error). Bounded queue (1000 entries by default)
   * with WorkerQueueFullError back-pressure. Falls back to sync execution
   * in test mode (when this._worker is null because db is :memory:).
   *
   * @param {string} op - one of: getTotalMailboxDepth, expireMessages,
   *   getMailboxesUnderPressure, getOrganStateCounts, getEntity, transition,
   *   updateMailboxStatus, getMailboxDepth, persistEvent
   * @param {Array} params - positional params to the op handler
   */
  async _callWorker(op, params) {
    if (this._queueDepth >= this._maxPendingRequests) {
      throw new WorkerQueueFullError(
        `worker queue full (${this._maxPendingRequests}); op=${op}`
      );
    }

    // Lazy await readiness (worker spawned in constructor; readiness signal
    // comes async; first async caller waits for it)
    if (this._workerReady) {
      await this._workerReady;
    }

    return new Promise((resolve, reject) => {
      const id = this._nextRequestId++;
      this._pendingRequests.set(id, { resolve, reject });
      this._queueDepth++;
      try {
        this._worker.postMessage({ id, op, params });
      } catch (err) {
        this._pendingRequests.delete(id);
        this._queueDepth--;
        reject(err);
      }
    });
  }

  // --- 9 async parallel methods (preserve sync API; route through worker
  //     when available, fall back to sync in test mode) ---

  /** @returns {Promise<number>} */
  async getTotalMailboxDepthAsync() {
    if (!this._worker) return this.getTotalMailboxDepth();
    return this._callWorker('getTotalMailboxDepth', []);
  }

  /** @returns {Promise<number>} */
  async expireMessagesAsync() {
    if (!this._worker) return this.expireMessages();
    return this._callWorker('expireMessages', []);
  }

  /** @returns {Promise<Array<{target_organ: string, depth: number}>>} */
  async getMailboxesUnderPressureAsync(threshold) {
    if (!this._worker) return this.getMailboxesUnderPressure(threshold);
    return this._callWorker('getMailboxesUnderPressure', [threshold]);
  }

  /** @returns {Promise<Object<string, number>>} */
  async getOrganStateCountsAsync() {
    if (!this._worker) return this.getOrganStateCounts();
    return this._callWorker('getOrganStateCounts', []);
  }

  /** @returns {Promise<Object|null>} */
  async getEntityAsync(entityUrn) {
    if (!this._worker) return this.getEntity(entityUrn);
    return this._callWorker('getEntity', [entityUrn]);
  }

  /** @returns {Promise<Object|null>} */
  async transitionAsync(entityUrn, fromState, toState, transitionId, reason, actor) {
    if (!this._worker) return this.transition(entityUrn, fromState, toState, transitionId, reason, actor);
    return this._callWorker('transition', [entityUrn, fromState, toState, transitionId, reason, actor]);
  }

  /** @returns {Promise<{success: true}>} */
  async updateMailboxStatusAsync(organName, status) {
    if (!this._worker) {
      this.updateMailboxStatus(organName, status);
      return { success: true };
    }
    return this._callWorker('updateMailboxStatus', [organName, status]);
  }

  /** @returns {Promise<number>} */
  async getMailboxDepthAsync(organName) {
    if (!this._worker) return this.getMailboxDepth(organName);
    return this._callWorker('getMailboxDepth', [organName]);
  }

  /** @returns {Promise<{urn: string, created_at: string}>} */
  async persistEventAsync(envelope, routing) {
    if (!this._worker) return this.persistEvent(envelope, routing);
    return this._callWorker('persistEvent', [envelope, routing]);
  }

  close() {
    this._terminating = true;
    if (this._worker) {
      try { this._worker.terminate(); } catch { /* ignore */ }
      this._worker = null;
    }
    // Reject any pending requests
    for (const [, pending] of this._pendingRequests) {
      pending.reject(new WorkerCrashedError('adapter closed'));
    }
    this._pendingRequests.clear();
    this._queueDepth = 0;
    this._db.close();
  }
}
