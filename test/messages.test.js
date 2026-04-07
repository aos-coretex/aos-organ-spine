/**
 * Relay 2 — POST /messages endpoint tests.
 *
 * Uses Node.js built-in test runner, real HTTP server on random port,
 * in-memory SQLite for isolation.
 *
 * Updated for Relay 3: validation middleware now enforces typed envelopes.
 * All messages must include a valid `type` and conformant payload.
 * Schema validation tests are in test/schemas.test.js.
 *
 * Updated for Relay 4: manifest and routing engine integration.
 *
 * Run: node --test test/messages.test.js
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { initDatabase } from '../server/db/init.js';
import { createManifest } from '../server/manifest/manifest.js';
import { createMessagesRouter } from '../server/routes/messages.js';
import { createMailboxRouter } from '../server/routes/mailbox.js';
import { createWebSocketHandler } from '../server/ws/handler.js';

describe('POST /messages', () => {
  let db, app, server, baseUrl, wsHandler;

  before(async () => {
    db = initDatabase(':memory:');
    const manifest = createManifest(db);
    wsHandler = createWebSocketHandler(db, manifest);

    app = express();
    app.use(express.json());
    app.use('/', createMessagesRouter(db, wsHandler, manifest));
    app.use('/mailbox', createMailboxRouter(db));

    await new Promise((resolve) => {
      server = app.listen(0, '127.0.0.1', () => {
        const addr = server.address();
        baseUrl = `http://127.0.0.1:${addr.port}`;
        resolve();
      });
    });

    wsHandler.attach(server);

    // Glia mailbox is auto-created by manifest seed (Relay 4)
  });

  after(async () => {
    wsHandler.cleanup();
    await new Promise((resolve) => server.close(resolve));
    db.close();
  });

  it('1. valid directed message returns 202 with message_id assigned', async () => {
    const res = await fetch(`${baseUrl}/messages`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        type: 'OTM',
        source_organ: 'Vigil',
        target_organ: 'Glia',
        payload: { event_type: 'check_health' },
      }),
    });

    assert.equal(res.status, 202);
    const body = await res.json();
    assert.equal(body.status, 'accepted');
    assert.equal(body.routing, 'directed');
    assert.equal(body.target_organ, 'Glia');
    assert.ok(body.message_id, 'should have message_id');
    assert.ok(body.timestamp, 'should have timestamp');
  });

  it('2. missing source_organ returns 400 SCHEMA_VALIDATION_FAILED', async () => {
    const res = await fetch(`${baseUrl}/messages`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        type: 'OTM',
        target_organ: 'Glia',
        payload: { event_type: 'test' },
      }),
    });

    assert.equal(res.status, 400);
    const body = await res.json();
    assert.equal(body.error, 'SCHEMA_VALIDATION_FAILED');
    const violation = body.violations.find((v) => v.field === 'source_organ');
    assert.ok(violation, 'should have source_organ violation');
    assert.equal(violation.rule, 'required');
  });

  it('3. directed message to organ not in manifest returns 400 ROUTING_FAILED', async () => {
    const res = await fetch(`${baseUrl}/messages`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        type: 'OTM',
        source_organ: 'Vigil',
        target_organ: 'NonExistentOrgan',
        payload: { event_type: 'test' },
      }),
    });

    assert.equal(res.status, 400);
    const body = await res.json();
    assert.equal(body.error, 'ROUTING_FAILED');
    assert.ok(body.message.includes('NonExistentOrgan'));
  });

  it('4. correlation_id is preserved in the envelope', async () => {
    const correlationId = 'urn:llm-ops:otm:2026-04-07T10:00:00.000Z-test';
    const res = await fetch(`${baseUrl}/messages`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        type: 'OTM',
        source_organ: 'Lobe',
        target_organ: 'Glia',
        correlation_id: correlationId,
        payload: { event_type: 'query_response', response_to: 'query' },
      }),
    });

    assert.equal(res.status, 202);
    const body = await res.json();

    // Verify the message was persisted with correlation_id by draining
    const rows = db.drainMailbox('Glia', 100);
    const match = rows.find(r => {
      const env = JSON.parse(r.envelope);
      return env.message_id === body.message_id;
    });
    assert.ok(match, 'message should be persisted');
    const envelope = JSON.parse(match.envelope);
    assert.equal(envelope.correlation_id, correlationId);
  });

  it('5. broadcast target ("*") returns 202 with broadcast routing', async () => {
    const res = await fetch(`${baseUrl}/messages`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        type: 'OTM',
        source_organ: 'Vigil',
        target_organ: '*',
        payload: { event_type: 'health_check_all' },
      }),
    });

    assert.equal(res.status, 202);
    const body = await res.json();
    assert.equal(body.routing, 'broadcast');
    assert.ok(Array.isArray(body.delivered_to), 'broadcast should include delivered_to array');
  });

  it('6. Spine assigns message_id with type-specific URN namespace', async () => {
    const res = await fetch(`${baseUrl}/messages`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        type: 'APM',
        source_organ: 'Thalamus',
        target_organ: 'Glia',
        payload: {
          action: 'test_action',
          targets: ['urn:test:1'],
          risk_tier: 'low',
          evidence_refs: ['urn:evidence:1'],
          rollback_plan: 'revert',
          reason: 'testing',
        },
      }),
    });

    assert.equal(res.status, 202);
    const body = await res.json();

    // message_id should be a URN with type-specific namespace
    assert.ok(body.message_id.startsWith('urn:llm-ops:apm:'),
      `Expected URN with apm namespace, got: ${body.message_id}`);
    // 4-char random suffix after timestamp
    assert.match(body.message_id, /urn:llm-ops:apm:\d{4}-\d{2}-\d{2}T.*-[a-z0-9]{4}$/);

    // timestamp should be ISO8601
    const parsed = new Date(body.timestamp);
    assert.ok(!isNaN(parsed.getTime()), 'timestamp should be valid ISO8601');
  });
});
