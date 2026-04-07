/**
 * Relay 5 — Event audit trail integration tests.
 *
 * Tests that messages flowing through the Spine are recorded in the events
 * table and queryable via the /events endpoints.
 *
 * Uses Node.js built-in test runner, real HTTP server on random port,
 * in-memory SQLite for isolation.
 *
 * Run: node --test test/events.test.js
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { initDatabase } from '../server/db/init.js';
import { createManifest } from '../server/manifest/manifest.js';
import { createMessagesRouter } from '../server/routes/messages.js';
import { createMailboxRouter } from '../server/routes/mailbox.js';
import { createEventsRouter } from '../server/routes/events.js';
import { createSchemasRouter } from '../server/routes/schemas.js';
import { createWebSocketHandler } from '../server/ws/handler.js';

describe('Event audit trail', () => {
  let adapter, app, server, baseUrl, wsHandler;

  before(async () => {
    adapter = initDatabase(':memory:');
    const manifest = createManifest(adapter);
    wsHandler = createWebSocketHandler(adapter, manifest);

    app = express();
    app.use(express.json());
    app.use('/', createMessagesRouter(adapter, wsHandler, manifest));
    app.use('/mailbox', createMailboxRouter(adapter));
    app.use('/', createEventsRouter(adapter));
    app.use('/', createSchemasRouter(adapter));

    await new Promise((resolve) => {
      server = app.listen(0, '127.0.0.1', () => {
        const addr = server.address();
        baseUrl = `http://127.0.0.1:${addr.port}`;
        resolve();
      });
    });

    wsHandler.attach(server);
  });

  after(async () => {
    wsHandler.cleanup();
    await new Promise((resolve) => server.close(resolve));
    adapter.close();
  });

  // Helper: POST /messages
  async function postMessage(body) {
    const res = await fetch(`${baseUrl}/messages`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    return { status: res.status, body: await res.json() };
  }

  // ── Event persistence ─────────────────────────────────────────────

  it('10. directed message records event with routing "directed"', async () => {
    const { status, body } = await postMessage({
      type: 'OTM',
      source_organ: 'Vigil',
      target_organ: 'Glia',
      payload: { event_type: 'health_check', data: { status: 'ok' } },
    });

    assert.equal(status, 202);

    // Query events to find the recorded message
    const res = await fetch(`${baseUrl}/events?source_organ=Vigil&type=OTM`);
    const eventBody = await res.json();
    assert.ok(eventBody.events.length >= 1, 'should have at least one event');

    const event = eventBody.events.find(
      e => e.envelope.message_id === body.message_id
    );
    assert.ok(event, 'should find the specific event by message_id');
    assert.equal(event.routing, 'directed');
    assert.equal(event.message_type, 'OTM');
    assert.equal(event.source_organ, 'Vigil');
    assert.equal(event.target_organ, 'Glia');
  });

  it('11. broadcast message records event with routing "broadcast"', async () => {
    const { status, body } = await postMessage({
      type: 'OTM',
      source_organ: 'Vigil',
      target_organ: '*',
      payload: { event_type: 'health_check_all' },
    });

    assert.equal(status, 202);
    assert.equal(body.routing, 'broadcast');

    // Query events to find the broadcast
    const res = await fetch(`${baseUrl}/events?routing=broadcast`);
    const eventBody = await res.json();

    const event = eventBody.events.find(
      e => e.envelope.message_id === body.message_id
    );
    assert.ok(event, 'should find the broadcast event');
    assert.equal(event.routing, 'broadcast');
    assert.equal(event.target_organ, '*');
  });

  // ── Event query endpoints ─────────────────────────────────────────

  it('12. GET /events returns all events', async () => {
    const res = await fetch(`${baseUrl}/events`);
    assert.equal(res.status, 200);
    const body = await res.json();
    assert.ok(Array.isArray(body.events));
    assert.ok(body.events.length >= 2, 'should have at least 2 events from tests 10-11');
    assert.equal(body.count, body.events.length);
  });

  it('13. GET /events?type=OTM returns filtered results', async () => {
    // Send an APM message to create non-OTM event
    await postMessage({
      type: 'APM',
      source_organ: 'Thalamus',
      target_organ: 'Nomos',
      payload: {
        action: 'test_filter',
        targets: ['urn:test:1'],
        risk_tier: 'low',
        evidence_refs: ['urn:e:1'],
        rollback_plan: 'revert',
        reason: 'testing event filter',
      },
    });

    const res = await fetch(`${baseUrl}/events?type=OTM`);
    const body = await res.json();
    assert.ok(body.events.length >= 1);
    assert.ok(body.events.every(e => e.message_type === 'OTM'), 'all events should be OTM');

    const resApm = await fetch(`${baseUrl}/events?type=APM`);
    const bodyApm = await resApm.json();
    assert.ok(bodyApm.events.length >= 1, 'should have at least one APM event');
    assert.ok(bodyApm.events.every(e => e.message_type === 'APM'), 'all events should be APM');
  });

  it('14. GET /events?since=... returns time-filtered results', async () => {
    const now = new Date();
    const past = new Date(now.getTime() - 60000).toISOString();
    const future = new Date(now.getTime() + 60000).toISOString();

    const res = await fetch(`${baseUrl}/events?since=${encodeURIComponent(past)}&until=${encodeURIComponent(future)}`);
    assert.equal(res.status, 200);
    const body = await res.json();
    assert.ok(body.events.length >= 1, 'should find events in time range');

    // No results in the far past
    const resPast = await fetch(`${baseUrl}/events?since=2020-01-01T00:00:00Z&until=2020-01-02T00:00:00Z`);
    const bodyPast = await resPast.json();
    assert.equal(bodyPast.events.length, 0, 'should find no events in far past');
  });

  it('15. GET /events/:urn returns single event', async () => {
    // Get an event URN from the query
    const listRes = await fetch(`${baseUrl}/events?limit=1`);
    const listBody = await listRes.json();
    assert.ok(listBody.events.length >= 1);

    const urn = listBody.events[0].urn;
    const encodedUrn = encodeURIComponent(urn);
    const res = await fetch(`${baseUrl}/events/${encodedUrn}`);
    assert.equal(res.status, 200);

    const body = await res.json();
    assert.equal(body.urn, urn);
    assert.ok(body.envelope, 'should include full envelope');
    assert.ok(body.created_at);
  });

  it('16. GET /events/:urn with non-existent URN returns 404', async () => {
    const encodedUrn = encodeURIComponent('urn:llm-ops:event:2099-01-01T00:00:00.000Z-zzzz');
    const res = await fetch(`${baseUrl}/events/${encodedUrn}`);
    assert.equal(res.status, 404);

    const body = await res.json();
    assert.equal(body.error, 'EVENT_NOT_FOUND');
  });

  // ── Domain event schema endpoints (Relay 5) ───────────────────────

  describe('Domain event schema endpoints', () => {
    it('POST /schemas/events registers a domain event schema', async () => {
      const res = await fetch(`${baseUrl}/schemas/events`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          event_type: 'backup_done',
          version: 1,
          fields: {
            bucket: { type: 'string', required: true },
            duration_s: { type: 'number', required: false },
          },
          description: 'Schema for backup completion events',
        }),
      });

      assert.equal(res.status, 201);
      const body = await res.json();
      assert.equal(body.event_type, 'backup_done');
      assert.equal(body.version, 1);
      assert.equal(body.urn, 'urn:llm-ops:event-schema:backup_done:v1');
    });

    it('GET /schemas/events lists all domain event schemas', async () => {
      const res = await fetch(`${baseUrl}/schemas/events`);
      assert.equal(res.status, 200);
      const body = await res.json();
      assert.ok(Array.isArray(body.schemas));
      assert.ok(body.schemas.length >= 1);
      assert.ok(body.schemas.some(s => s.event_type === 'backup_done'));
    });

    it('GET /schemas/events/:event_type returns latest version', async () => {
      const res = await fetch(`${baseUrl}/schemas/events/backup_done`);
      assert.equal(res.status, 200);
      const body = await res.json();
      assert.equal(body.event_type, 'backup_done');
      assert.equal(body.version, 1);
      assert.ok(body.fields.bucket);
    });

    it('GET /schemas/events/:event_type returns 404 for unknown type', async () => {
      const res = await fetch(`${baseUrl}/schemas/events/nonexistent_type`);
      assert.equal(res.status, 404);
      const body = await res.json();
      assert.equal(body.error, 'EVENT_SCHEMA_NOT_FOUND');
    });

    it('POST /schemas/events with duplicate version returns 409', async () => {
      const res = await fetch(`${baseUrl}/schemas/events`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          event_type: 'backup_done',
          version: 1,
          fields: { bucket: { type: 'string', required: true } },
        }),
      });

      assert.equal(res.status, 409);
      const body = await res.json();
      assert.equal(body.error, 'SCHEMA_VERSION_EXISTS');
    });
  });
});
