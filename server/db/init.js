/**
 * SQLite initialization for the Spine organ.
 *
 * Opens spine.db with WAL mode, creates all tables (Relays 1–5),
 * seeds initial data, creates the StorageAdapter, and exports it.
 *
 * The raw db instance never leaves this module — all runtime access
 * goes through the adapter.
 */

import Database from 'better-sqlite3';
import { mkdirSync, existsSync } from 'node:fs';
import { dirname } from 'node:path';
import { config } from '../config.js';
import { getStateMachineDefinitions } from '../state/definitions.js';
import { schemas } from '../schemas/index.js';
import { SQLiteStorageAdapter } from '../adapter/sqlite.js';

function log(event, data = {}) {
  const entry = { timestamp: new Date().toISOString(), event, ...data };
  process.stdout.write(JSON.stringify(entry) + '\n');
}

/**
 * Initialize the database and return a StorageAdapter.
 * Accepts an optional path override (used by tests passing ':memory:').
 */
export function initDatabase(dbPath) {
  const resolvedPath = dbPath || config.dbPath;

  // Ensure the data directory exists (skip for in-memory)
  if (resolvedPath !== ':memory:') {
    const dir = dirname(resolvedPath);
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }
  }

  const db = new Database(resolvedPath);

  // Pragmas
  db.pragma('journal_mode = WAL');
  db.pragma('busy_timeout = 5000');

  // --- DDL: create all tables ---

  db.exec(`
    -- Relay 1: spine-state tables

    CREATE TABLE IF NOT EXISTS state_machine_defs (
      entity_type TEXT PRIMARY KEY,
      states TEXT NOT NULL CHECK(json_valid(states)),
      transitions TEXT NOT NULL CHECK(json_valid(transitions)),
      initial_state TEXT NOT NULL,
      terminal_states TEXT NOT NULL CHECK(json_valid(terminal_states)),
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS state_entities (
      entity_urn TEXT PRIMARY KEY,
      entity_type TEXT NOT NULL,
      current_state TEXT NOT NULL,
      metadata TEXT CHECK(metadata IS NULL OR json_valid(metadata)),
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (entity_type) REFERENCES state_machine_defs(entity_type)
    );

    CREATE TABLE IF NOT EXISTS state_transitions (
      transition_id TEXT PRIMARY KEY,
      entity_urn TEXT NOT NULL,
      from_state TEXT NOT NULL,
      to_state TEXT NOT NULL,
      reason TEXT NOT NULL,
      actor TEXT NOT NULL,
      timestamp TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (entity_urn) REFERENCES state_entities(entity_urn)
    );

    CREATE INDEX IF NOT EXISTS idx_state_entities_type
      ON state_entities(entity_type);

    CREATE INDEX IF NOT EXISTS idx_state_transitions_entity
      ON state_transitions(entity_urn);

    CREATE INDEX IF NOT EXISTS idx_state_transitions_timestamp
      ON state_transitions(timestamp);

    -- Relay 2: organ mailboxes and persistent message queue

    CREATE TABLE IF NOT EXISTS mailboxes (
      organ_name TEXT PRIMARY KEY,
      registered_at TEXT NOT NULL DEFAULT (datetime('now')),
      last_drain_at TEXT,
      last_connected_at TEXT,
      status TEXT NOT NULL DEFAULT 'registered'
        CHECK(status IN ('registered', 'active', 'disconnected'))
    );

    CREATE TABLE IF NOT EXISTS mailbox_messages (
      message_id TEXT PRIMARY KEY,
      target_organ TEXT NOT NULL,
      source_organ TEXT NOT NULL,
      envelope TEXT NOT NULL CHECK(json_valid(envelope)),
      delivered INTEGER NOT NULL DEFAULT 0,
      delivered_at TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      ttl_seconds INTEGER,
      FOREIGN KEY (target_organ) REFERENCES mailboxes(organ_name)
    );

    CREATE INDEX IF NOT EXISTS idx_mailbox_messages_target_delivered
      ON mailbox_messages(target_organ, delivered);

    CREATE INDEX IF NOT EXISTS idx_mailbox_messages_created
      ON mailbox_messages(created_at);

    -- Phase-1 wedge-fix (2026-05-09 spine-wedge-recurrence-prevention):
    -- The four health-check queries that fire on the 60s setInterval tick
    -- previously planned as SCAN TABLE, allowing the empirical 6-day wedge
    -- on _totalMailboxDepth (sqlite.js:239-241). These three indexes turn
    -- the wedge call sites into SEARCH USING INDEX. Per E-ORG spec
    -- 2026-05-09 spine-wedge-fix-relay-prompt-body.md.

    CREATE INDEX IF NOT EXISTS idx_mailbox_messages_delivered
      ON mailbox_messages(delivered);

    CREATE INDEX IF NOT EXISTS idx_mailbox_messages_delivered_ttl
      ON mailbox_messages(delivered, ttl_seconds)
      WHERE ttl_seconds IS NOT NULL AND delivered = 0;

    CREATE INDEX IF NOT EXISTS idx_state_entities_type_state
      ON state_entities(entity_type, current_state);

    -- Relay 3: schema changelog for message type schema evolution

    CREATE TABLE IF NOT EXISTS schema_changelog (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      message_type TEXT NOT NULL,
      version INTEGER NOT NULL,
      change_description TEXT NOT NULL,
      fields_added TEXT CHECK(fields_added IS NULL OR json_valid(fields_added)),
      fields_removed TEXT CHECK(fields_removed IS NULL OR json_valid(fields_removed)),
      fields_modified TEXT CHECK(fields_modified IS NULL OR json_valid(fields_modified)),
      timestamp TEXT NOT NULL DEFAULT (datetime('now'))
    );

    -- Relay 4: organ manifest — the DIO's anatomy, which organs Spine expects

    CREATE TABLE IF NOT EXISTS organ_manifest (
      organ_id TEXT PRIMARY KEY,
      required INTEGER NOT NULL DEFAULT 1,
      expected_subscriptions TEXT CHECK(expected_subscriptions IS NULL OR json_valid(expected_subscriptions)),
      registered_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    -- Relay 4: persistent organ subscriptions

    CREATE TABLE IF NOT EXISTS subscription_registry (
      organ_id TEXT NOT NULL,
      subscription_type TEXT NOT NULL CHECK(subscription_type IN ('broadcast', 'directed')),
      topic_filter TEXT,
      filter_mode TEXT CHECK(filter_mode IN ('AND', 'OR')),
      registered_at TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (organ_id, subscription_type, topic_filter),
      FOREIGN KEY (organ_id) REFERENCES organ_manifest(organ_id)
    );

    CREATE INDEX IF NOT EXISTS idx_subscription_registry_organ
      ON subscription_registry(organ_id);

    -- Relay 5: events — permanent audit trail for every message through Spine

    CREATE TABLE IF NOT EXISTS events (
      urn TEXT PRIMARY KEY,
      message_type TEXT NOT NULL CHECK(message_type IN ('OTM', 'APM', 'PEM', 'ATM', 'HOM')),
      source_organ TEXT NOT NULL,
      target_organ TEXT NOT NULL,
      routing TEXT NOT NULL CHECK(routing IN ('directed', 'broadcast')),
      envelope TEXT NOT NULL CHECK(json_valid(envelope)),
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE INDEX IF NOT EXISTS idx_events_message_type ON events(message_type);
    CREATE INDEX IF NOT EXISTS idx_events_source_organ ON events(source_organ);
    CREATE INDEX IF NOT EXISTS idx_events_target_organ ON events(target_organ);
    CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at);

    -- Relay 5: event schemas — runtime-registered domain schemas for OTM subtypes

    CREATE TABLE IF NOT EXISTS event_schemas (
      urn TEXT PRIMARY KEY,
      event_type TEXT NOT NULL,
      version INTEGER NOT NULL,
      fields TEXT NOT NULL CHECK(json_valid(fields)),
      description TEXT,
      registered_at TEXT NOT NULL DEFAULT (datetime('now')),
      UNIQUE(event_type, version)
    );

    CREATE INDEX IF NOT EXISTS idx_event_schemas_event_type ON event_schemas(event_type);
  `);

  // --- Create the adapter (all runtime access goes through this) ---

  const adapter = new SQLiteStorageAdapter(db);

  // --- Seed state machine definitions (idempotent) ---

  const defs = getStateMachineDefinitions();
  for (const def of defs) {
    adapter.registerStateMachineDef(def.entity_type, def, { idempotent: true });
  }

  // --- Seed schema changelog v1 entries (idempotent) ---

  if (!adapter.hasChangelogVersion(1)) {
    for (const [type, schema] of Object.entries(schemas)) {
      const fieldNames = Object.keys(schema.fields);
      adapter.addChangelogEntry(type, 1, 'Initial schema definition', fieldNames);
    }
  }

  // --- Seed organ manifest (idempotent) ---

  const DIO_OS_ORGANS = [
    { organ_id: 'Spine',       required: 1 },
    { organ_id: 'Vectr',       required: 1 },
    { organ_id: 'Graph',       required: 1 },
    { organ_id: 'Phi',         required: 1 },
    { organ_id: 'Radiant',     required: 1 },
    { organ_id: 'Minder',      required: 1 },
    { organ_id: 'Hippocampus', required: 1 },
    { organ_id: 'Soul',        required: 1 },
    { organ_id: 'Lobe',        required: 1 },
    { organ_id: 'Syntra',      required: 1 },
    { organ_id: 'Vigil',       required: 1 },
    { organ_id: 'Glia',        required: 1 },
    { organ_id: 'SafeVault',   required: 1 },
    { organ_id: 'GitSync',     required: 1 },
    { organ_id: 'Promote',     required: 1 },
    { organ_id: 'Sourcegraph', required: 1 },
    { organ_id: 'Engram',      required: 1 },
    { organ_id: 'Arbiter',     required: 1 },
    { organ_id: 'Nomos',       required: 1 },
    { organ_id: 'Cerberus',    required: 1 },
    { organ_id: 'Senate',      required: 1 },
    { organ_id: 'Cortex',      required: 1 },
    { organ_id: 'Thalamus',    required: 1 },
    { organ_id: 'ModelBroker', required: 1 },
    { organ_id: 'Receptor',    required: 1 },
    { organ_id: 'MCP-Router',  required: 1 },
    { organ_id: 'MCP-Gateway', required: 1 },
    { organ_id: 'Axon',        required: 1 },
    // Relay 6: Human principal — consumer on Spine, mailbox persists HOMs
    { organ_id: 'human-principal', required: 0 },
  ];

  for (const organ of DIO_OS_ORGANS) {
    adapter.addManifestEntry(organ.organ_id, organ.required === 1);
  }

  const tables = adapter.getTables();
  const manifestCount = adapter.getManifest().length;

  log('db_initialized', {
    path: resolvedPath,
    tables,
    state_machines: defs.map(d => d.entity_type),
    manifest_organs: manifestCount,
  });

  return adapter;
}
