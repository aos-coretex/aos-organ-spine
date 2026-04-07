/**
 * Relay 5 — StorageAdapter unit tests.
 *
 * Tests the SQLiteStorageAdapter directly. Verifies that all operations
 * from Relays 1–5 work correctly through the adapter layer.
 *
 * Uses Node.js built-in test runner and in-memory SQLite for isolation.
 * Run: node --test test/adapter.test.js
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { initDatabase } from '../server/db/init.js';

describe('SQLiteStorageAdapter', () => {
  let adapter;

  before(() => {
    adapter = initDatabase(':memory:');
  });

  after(() => {
    adapter.close();
  });

  // ── Events (Relay 5) ─────────────────────────────────────────────

  describe('Event persistence', () => {
    it('1. persistEvent stores event, retrievable by URN', () => {
      const envelope = {
        type: 'OTM',
        source_organ: 'Vigil',
        target_organ: 'Glia',
        message_id: 'urn:llm-ops:otm:2026-04-07T10:00:00.000Z-test',
        correlation_id: null,
        reply_to: 'Vigil',
        timestamp: '2026-04-07T10:00:00.000Z',
        payload: { event_type: 'cv_failure', data: { test_id: 'test-1' } },
      };

      const result = adapter.persistEvent(envelope, 'directed');
      assert.ok(result.urn, 'should return event URN');
      assert.ok(result.urn.startsWith('urn:llm-ops:event:'), 'URN should have event namespace');
      assert.ok(result.created_at, 'should return created_at');

      const retrieved = adapter.getEvent(result.urn);
      assert.ok(retrieved, 'should be retrievable by URN');
      assert.equal(retrieved.message_type, 'OTM');
      assert.equal(retrieved.source_organ, 'Vigil');
      assert.equal(retrieved.target_organ, 'Glia');
      assert.equal(retrieved.routing, 'directed');
      assert.deepEqual(retrieved.envelope.payload.event_type, 'cv_failure');
    });

    it('2. queryEvents with type filter returns correct subset', () => {
      // Persist an APM event
      adapter.persistEvent({
        type: 'APM',
        source_organ: 'Thalamus',
        target_organ: 'Nomos',
        message_id: 'urn:llm-ops:apm:test-filter',
        correlation_id: null,
        reply_to: 'Thalamus',
        timestamp: new Date().toISOString(),
        payload: {
          action: 'deploy',
          targets: ['urn:test:1'],
          risk_tier: 'low',
          evidence_refs: ['urn:e:1'],
          rollback_plan: 'revert',
          reason: 'test',
        },
      }, 'directed');

      const result = adapter.queryEvents({ type: 'APM' });
      assert.ok(result.events.length >= 1, 'should find APM events');
      assert.ok(result.events.every(e => e.message_type === 'APM'), 'all should be APM type');
    });

    it('3. queryEvents with time range returns correct subset', () => {
      const now = new Date();
      const past = new Date(now.getTime() - 60000).toISOString();
      const future = new Date(now.getTime() + 60000).toISOString();

      const result = adapter.queryEvents({ since: past, until: future });
      assert.ok(result.events.length >= 1, 'should find events in time range');

      // Verify no events outside range
      const noResults = adapter.queryEvents({
        since: '2020-01-01T00:00:00Z',
        until: '2020-01-02T00:00:00Z',
      });
      assert.equal(noResults.events.length, 0, 'should find no events in far past');
    });

    it('4. queryEvents with multiple filters uses AND semantics', () => {
      const result = adapter.queryEvents({
        type: 'OTM',
        source_organ: 'Vigil',
        routing: 'directed',
      });
      assert.ok(result.events.length >= 1, 'should find events matching all filters');
      for (const event of result.events) {
        assert.equal(event.message_type, 'OTM');
        assert.equal(event.source_organ, 'Vigil');
        assert.equal(event.routing, 'directed');
      }
    });
  });

  // ── State operations (Relay 1) ────────────────────────────────────

  describe('State operations through adapter', () => {
    it('5. state machine CRUD operations work through adapter', () => {
      // getStateMachineDef — seeded definitions exist
      const jobDef = adapter.getStateMachineDef('job');
      assert.ok(jobDef, 'job machine should exist');
      assert.equal(jobDef.initial_state, 'CREATED');

      // listStateMachineDefs
      const allDefs = adapter.listStateMachineDefs();
      assert.ok(allDefs.length >= 2, 'should have at least job and organ');

      // createEntity
      const entity = adapter.createEntity('urn:adapter:test:1', 'job', 'CREATED', { name: 'test' });
      assert.ok(entity, 'entity should be created');
      assert.equal(entity.current_state, 'CREATED');
      assert.deepEqual(entity.metadata, { name: 'test' });

      // getEntity
      const fetched = adapter.getEntity('urn:adapter:test:1');
      assert.equal(fetched.entity_urn, 'urn:adapter:test:1');

      // transition (atomic)
      const updated = adapter.transition(
        'urn:adapter:test:1', 'CREATED', 'PLANNING',
        'urn:llm-ops:transition:test-1', 'plan', 'thalamus'
      );
      assert.equal(updated.current_state, 'PLANNING');

      // getTransitions
      const history = adapter.getTransitions('urn:adapter:test:1');
      assert.equal(history.length, 1);
      assert.equal(history[0].from_state, 'CREATED');
      assert.equal(history[0].to_state, 'PLANNING');
    });
  });

  // ── Mailbox operations (Relay 2) ──────────────────────────────────

  describe('Mailbox operations through adapter', () => {
    it('6. mailbox CRUD operations work through adapter', () => {
      // getMailbox — seeded mailboxes exist from manifest
      const mailbox = adapter.getMailbox('Vigil');
      assert.ok(mailbox, 'Vigil mailbox should exist from manifest seed');
      assert.equal(mailbox.organ_name, 'Vigil');

      // getMailboxDepth — initially 0
      const depth = adapter.getMailboxDepth('Vigil');
      assert.equal(depth, 0);

      // persistMailboxMessage
      adapter.persistMailboxMessage(
        'urn:llm-ops:otm:test-msg-1', 'Vigil', 'Lobe',
        { type: 'OTM', source_organ: 'Lobe', target_organ: 'Vigil', payload: { event_type: 'test' } },
        false,
      );

      // getMailboxDepth after persist
      assert.equal(adapter.getMailboxDepth('Vigil'), 1);

      // drainMailbox
      const drained = adapter.drainMailbox('Vigil', 10);
      assert.equal(drained.length, 1);
      assert.equal(drained[0].message_id, 'urn:llm-ops:otm:test-msg-1');

      // ackMessages
      const acked = adapter.ackMessages(['urn:llm-ops:otm:test-msg-1']);
      assert.equal(acked, 1);

      // Depth back to 0 after ack
      assert.equal(adapter.getMailboxDepth('Vigil'), 0);

      // setMailboxActive / setMailboxDisconnected
      adapter.setMailboxActive('Vigil');
      const active = adapter.getMailbox('Vigil');
      assert.equal(active.status, 'active');

      adapter.setMailboxDisconnected('Vigil');
      const disconnected = adapter.getMailbox('Vigil');
      assert.equal(disconnected.status, 'disconnected');
    });
  });

  // ── Event Schemas (Relay 5) ────────────────────────────────────────

  describe('Event schema operations', () => {
    it('7. registerEventSchema stores schema, retrievable by event_type', () => {
      const schema = adapter.registerEventSchema(
        'cv_failure', 1,
        { test_id: { type: 'string', required: true }, severity: { type: 'string', required: false } },
        'Schema for CV failure events',
      );

      assert.ok(schema.urn, 'should return URN');
      assert.equal(schema.urn, 'urn:llm-ops:event-schema:cv_failure:v1');
      assert.equal(schema.event_type, 'cv_failure');
      assert.equal(schema.version, 1);
      assert.ok(schema.fields.test_id);
    });

    it('8. getEventSchema returns latest version', () => {
      // Register v2
      adapter.registerEventSchema(
        'cv_failure', 2,
        {
          test_id: { type: 'string', required: true },
          severity: { type: 'string', required: true },
          component: { type: 'string', required: false },
        },
        'Schema v2 — severity now required',
      );

      const latest = adapter.getEventSchema('cv_failure');
      assert.equal(latest.version, 2, 'should return latest version (2)');
      assert.equal(latest.fields.severity.required, true, 'v2 field should be present');
    });

    it('listEventSchemas returns all schemas', () => {
      const all = adapter.listEventSchemas();
      assert.ok(all.length >= 2, 'should have at least 2 event schemas');
      const cvFailures = all.filter(s => s.event_type === 'cv_failure');
      assert.equal(cvFailures.length, 2, 'should have v1 and v2 for cv_failure');
    });
  });

  // ── Schema changelog (Relay 3) ────────────────────────────────────

  describe('Changelog operations through adapter', () => {
    it('9. changelog has seeded entries, addChangelogEntry works', () => {
      const entries = adapter.getChangelog();
      assert.ok(entries.length >= 5, 'should have seeded changelog entries for 5 message types');

      adapter.addChangelogEntry('OTM', 2, 'Added optional priority field', ['priority']);
      const updated = adapter.getChangelog();
      assert.equal(updated.length, entries.length + 1, 'should have one more entry');
      const v2 = updated.find(e => e.message_type === 'OTM' && e.version === 2);
      assert.ok(v2, 'should have OTM v2 entry');
    });
  });

  // ── Manifest & Subscriptions (Relay 4) ─────────────────────────────

  describe('Manifest and subscription operations', () => {
    it('getManifest returns seeded organs', () => {
      const manifest = adapter.getManifest();
      assert.equal(manifest.length, 29, 'should have 29 seeded organs (28 DIO + human-principal)');
      const spineEntry = manifest.find(o => o.organ_id === 'Spine');
      assert.ok(spineEntry, 'should include Spine');
      assert.equal(spineEntry.required, true);
    });

    it('addManifestEntry adds new organ + mailbox', () => {
      const entry = adapter.addManifestEntry('AdapterTestOrgan', false);
      assert.ok(entry, 'should return new entry');
      assert.equal(entry.organ_id, 'AdapterTestOrgan');
      assert.equal(entry.required, false);

      // Mailbox auto-created
      const mailbox = adapter.getMailbox('AdapterTestOrgan');
      assert.ok(mailbox, 'mailbox should be auto-created');

      // Idempotent
      const dup = adapter.addManifestEntry('AdapterTestOrgan', false);
      assert.equal(dup, null, 'should return null for duplicate');
    });

    it('subscription CRUD via adapter', () => {
      const filterStr = JSON.stringify({ event_type: 'test_sub' });
      adapter.addSubscription('Vigil', filterStr);

      const subs = adapter.getSubscriptions('Vigil');
      assert.ok(subs.length > 0, 'should have subscriptions');

      adapter.removeSubscription('Vigil', filterStr);
      const afterRemove = adapter.getSubscriptions('Vigil');
      const hasTestSub = afterRemove.some(s => s.topic_filter === filterStr);
      assert.ok(!hasTestSub, 'should be removed');
    });
  });

  // ── Diagnostics ────────────────────────────────────────────────────

  describe('Diagnostic operations', () => {
    it('healthCheck returns true', () => {
      assert.equal(adapter.healthCheck(), true);
    });

    it('getStats returns correct structure', () => {
      const stats = adapter.getStats();
      assert.ok('state_entities' in stats);
      assert.ok('state_transitions' in stats);
      assert.equal(typeof stats.state_entities.total, 'number');
    });

    it('getTables returns all expected tables', () => {
      const tables = adapter.getTables();
      assert.ok(tables.includes('state_machine_defs'));
      assert.ok(tables.includes('state_entities'));
      assert.ok(tables.includes('mailboxes'));
      assert.ok(tables.includes('events'), 'should include events table (Relay 5)');
      assert.ok(tables.includes('event_schemas'), 'should include event_schemas table (Relay 5)');
    });
  });
});
