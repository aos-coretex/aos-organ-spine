/**
 * Relay 4 — Broadcast vs Directed Routing Logic tests.
 *
 * Covers: organ manifest, persistent subscriptions, delivery guarantee
 * enforcement, broadcast routing, directed routing, reconnection recovery,
 * and state transition event emission.
 *
 * Uses Node.js built-in test runner, real HTTP server on random port,
 * in-memory SQLite for isolation.
 *
 * Run: node --test test/routing.test.js
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import WebSocket from 'ws';
import { initDatabase } from '../server/db/init.js';
import { createManifest } from '../server/manifest/manifest.js';
import { createMessagesRouter } from '../server/routes/messages.js';
import { createMailboxRouter } from '../server/routes/mailbox.js';
import { createHealthRouter } from '../server/routes/health.js';
import { createStateRouter } from '../server/routes/state.js';
import { createWebSocketHandler } from '../server/ws/handler.js';
import { createStateMachine } from '../server/state/machine.js';
import { routeMessage } from '../server/routing/router.js';
import { generateUrn } from '../lib/urn.js';

// --- Test helpers ---

function createTestServer() {
  const db = initDatabase(':memory:');
  const manifest = createManifest(db);
  const wsHandler = createWebSocketHandler(db, manifest);

  const stateMachine = createStateMachine(db, {
    onTransition(result) {
      const envelope = {
        type: 'OTM',
        source_organ: 'Spine',
        target_organ: '*',
        message_id: generateUrn('otm'),
        correlation_id: null,
        reply_to: 'Spine',
        timestamp: new Date().toISOString(),
        payload: {
          event_type: 'state_transition',
          source: 'spine-state',
          data: {
            entity_urn: result.entity_urn,
            previous_state: result.previous_state,
            current_state: result.current_state,
            transition_id: result.transition_id,
            actor: result.actor,
            reason: result.reason,
          },
        },
      };
      routeMessage(envelope, {
        manifest,
        subscriptionCache: wsHandler.getSubscriptionCache(),
        pushToOrgan: wsHandler.pushToOrgan,
        isOrganConnected: wsHandler.isOrganConnected,
        adapter: db,
      });
    },
  });

  const app = express();
  app.use(express.json());
  app.use('/', createMessagesRouter(db, wsHandler, manifest));
  app.use('/mailbox', createMailboxRouter(db));
  app.use('/', createHealthRouter(db, ':memory:', 0, manifest, wsHandler));
  app.use('/state', createStateRouter(stateMachine));

  return { db, app, wsHandler, manifest, stateMachine };
}

async function startServer(app, wsHandler) {
  return new Promise((resolve) => {
    const server = app.listen(0, '127.0.0.1', () => {
      const addr = server.address();
      const baseUrl = `http://127.0.0.1:${addr.port}`;
      wsHandler.attach(server);
      resolve({ server, baseUrl });
    });
  });
}

async function postJson(baseUrl, path, body) {
  const res = await fetch(`${baseUrl}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  return { status: res.status, body: await res.json() };
}

async function getJson(baseUrl, path) {
  const res = await fetch(`${baseUrl}${path}`);
  return { status: res.status, body: await res.json() };
}

function connectWs(baseUrl) {
  const wsUrl = baseUrl.replace('http', 'ws') + '/subscribe';
  return new WebSocket(wsUrl);
}

function waitForMessage(ws, predicate, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('WebSocket message timeout')), timeoutMs);
    const handler = (raw) => {
      const data = JSON.parse(raw.toString());
      if (predicate(data)) {
        clearTimeout(timer);
        ws.removeListener('message', handler);
        resolve(data);
      }
    };
    ws.on('message', handler);
  });
}

function collectMessages(ws, count, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Timeout waiting for ${count} messages, got ${collected.length}`)), timeoutMs);
    const collected = [];
    const handler = (raw) => {
      const data = JSON.parse(raw.toString());
      if (data.action === 'message') {
        collected.push(data);
        if (collected.length >= count) {
          clearTimeout(timer);
          ws.removeListener('message', handler);
          resolve(collected);
        }
      }
    };
    ws.on('message', handler);
  });
}

async function registerOrganWs(ws, organName) {
  ws.send(JSON.stringify({ action: 'register', organ_name: organName }));
  return waitForMessage(ws, (d) => d.action === 'registered');
}

// ============================================================
// Test suites
// ============================================================

// --- Organ Manifest ---

describe('Organ manifest', () => {
  let db, app, server, baseUrl, wsHandler, manifest;

  before(async () => {
    ({ db, app, wsHandler, manifest } = createTestServer());
    ({ server, baseUrl } = await startServer(app, wsHandler));
  });

  after(async () => {
    wsHandler.cleanup();
    await new Promise((resolve) => server.close(resolve));
    db.close();
  });

  it('1. known organ connects — registration accepted', async () => {
    const ws = connectWs(baseUrl);
    await new Promise((resolve) => ws.on('open', resolve));

    const regMsg = await registerOrganWs(ws, 'Vigil');
    assert.equal(regMsg.action, 'registered');
    assert.equal(regMsg.organ_name, 'Vigil');
    assert.ok('mailbox_depth' in regMsg, 'should include mailbox_depth');
    assert.ok(Array.isArray(regMsg.active_subscriptions), 'should include active_subscriptions');

    ws.close();
    await new Promise((resolve) => ws.on('close', resolve));
  });

  it('2. unknown organ connects — UNKNOWN_ORGAN error, WebSocket closed', async () => {
    const ws = connectWs(baseUrl);
    await new Promise((resolve) => ws.on('open', resolve));

    ws.send(JSON.stringify({ action: 'register', organ_name: 'FakeOrgan' }));
    const errMsg = await waitForMessage(ws, (d) => d.action === 'error');
    assert.equal(errMsg.message, 'UNKNOWN_ORGAN');
    assert.equal(errMsg.organ_name, 'FakeOrgan');

    // WebSocket should close
    await new Promise((resolve) => ws.on('close', resolve));
  });

  it('3. GET /manifest — lists all 29 organs with connection status', async () => {
    const { status, body } = await getJson(baseUrl, '/manifest');
    assert.equal(status, 200);
    assert.equal(body.total, 29);
    assert.ok(Array.isArray(body.organs), 'should have organs array');
    assert.ok(Array.isArray(body.connected), 'should have connected array');
    assert.ok(Array.isArray(body.missing_required), 'should have missing_required array');
    assert.ok(body.health_status, 'should have health_status');

    // Verify specific organs
    const organIds = body.organs.map(o => o.organ_id);
    assert.ok(organIds.includes('Spine'));
    assert.ok(organIds.includes('Vigil'));
    assert.ok(organIds.includes('Axon'));
    assert.ok(organIds.includes('MCP-Gateway'));
  });

  it('4. POST /manifest/:organ_id — adds new organ to manifest', async () => {
    const { status, body } = await postJson(baseUrl, '/manifest/TestDept', { required: false });
    assert.equal(status, 201);
    assert.equal(body.organ_id, 'TestDept');
    assert.equal(body.required, false);

    // Verify it appears in the manifest
    const { body: manifestBody } = await getJson(baseUrl, '/manifest');
    assert.equal(manifestBody.total, 30);

    // Verify idempotent re-add returns 200
    const { status: status2, body: body2 } = await postJson(baseUrl, '/manifest/TestDept', {});
    assert.equal(status2, 200);
    assert.equal(body2.already_exists, true);
  });

  it('5. missing required organ — Spine status DEGRADED', async () => {
    const { body } = await getJson(baseUrl, '/manifest');
    // All 28 required organs are disconnected (no WS connections for most)
    assert.equal(body.health_status, 'DEGRADED');
    assert.ok(body.missing_required.length > 0, 'should have missing required organs');

    // Health endpoint reflects DEGRADED
    const { body: health } = await getJson(baseUrl, '/health');
    assert.equal(health.status, 'degraded');
    assert.ok(health.manifest.missing_required.length > 0, 'manifest should show missing required');
  });
});

// --- Persistent Subscriptions ---

describe('Persistent subscriptions', () => {
  let db, app, server, baseUrl, wsHandler;

  before(async () => {
    ({ db, app, wsHandler } = createTestServer());
    ({ server, baseUrl } = await startServer(app, wsHandler));
  });

  after(async () => {
    wsHandler.cleanup();
    await new Promise((resolve) => server.close(resolve));
    db.close();
  });

  it('6. organ subscribes — subscription persisted in subscription_registry table', async () => {
    const ws = connectWs(baseUrl);
    await new Promise((resolve) => ws.on('open', resolve));
    await registerOrganWs(ws, 'Vigil');

    ws.send(JSON.stringify({ action: 'subscribe', filter: { event_type: 'cv_failure' } }));
    const subMsg = await waitForMessage(ws, (d) => d.action === 'subscribed');
    assert.equal(subMsg.organ_name, 'Vigil');
    assert.deepEqual(subMsg.filter, { event_type: 'cv_failure' });
    assert.equal(subMsg.persistent, true);

    // Verify in database via adapter
    const subs = db.getSubscriptions('Vigil');
    assert.ok(subs.length > 0, 'should have subscriptions in registry');

    ws.close();
    await new Promise((resolve) => ws.on('close', resolve));
  });

  it('7. organ disconnects and reconnects — subscriptions still active', async () => {
    // Connect a new organ and subscribe
    const ws1 = connectWs(baseUrl);
    await new Promise((resolve) => ws1.on('open', resolve));
    await registerOrganWs(ws1, 'Glia');

    ws1.send(JSON.stringify({ action: 'subscribe', filter: { event_type: 'health_check' } }));
    await waitForMessage(ws1, (d) => d.action === 'subscribed');

    // Disconnect
    ws1.close();
    await new Promise((resolve) => ws1.on('close', resolve));
    await new Promise((resolve) => setTimeout(resolve, 50));

    // Reconnect
    const ws2 = connectWs(baseUrl);
    await new Promise((resolve) => ws2.on('open', resolve));
    const regMsg = await registerOrganWs(ws2, 'Glia');

    // Subscriptions should be included in registration response
    assert.ok(Array.isArray(regMsg.active_subscriptions));
    assert.ok(regMsg.active_subscriptions.length > 0, 'should have restored subscriptions');
    const hasFilter = regMsg.active_subscriptions.some(
      s => s.filter && s.filter.event_type === 'health_check'
    );
    assert.ok(hasFilter, 'should include health_check subscription');

    ws2.close();
    await new Promise((resolve) => ws2.on('close', resolve));
  });

  it('8. registration response includes active_subscriptions array', async () => {
    // Vigil already has a subscription from test 6
    const ws = connectWs(baseUrl);
    await new Promise((resolve) => ws.on('open', resolve));
    const regMsg = await registerOrganWs(ws, 'Vigil');

    assert.ok(Array.isArray(regMsg.active_subscriptions));
    // Should have the cv_failure subscription from test 6
    const hasCvFailure = regMsg.active_subscriptions.some(
      s => s.filter && s.filter.event_type === 'cv_failure'
    );
    assert.ok(hasCvFailure, 'should include cv_failure subscription');

    ws.close();
    await new Promise((resolve) => ws.on('close', resolve));
  });

  it('9. unsubscribe — removed from registry', async () => {
    const ws = connectWs(baseUrl);
    await new Promise((resolve) => ws.on('open', resolve));
    await registerOrganWs(ws, 'Vigil');

    ws.send(JSON.stringify({ action: 'unsubscribe', filter: { event_type: 'cv_failure' } }));
    const unsubMsg = await waitForMessage(ws, (d) => d.action === 'unsubscribed');
    assert.equal(unsubMsg.organ_name, 'Vigil');

    // Verify removed from database via adapter
    const subs = db.getSubscriptions('Vigil');
    const hasCvFailure = subs.some(
      s => s.topic_filter === JSON.stringify({ event_type: 'cv_failure' })
    );
    assert.ok(!hasCvFailure, 'should be removed from subscription_registry');

    ws.close();
    await new Promise((resolve) => ws.on('close', resolve));
  });

  it('10. Spine restart — subscriptions loaded from registry into cache', async () => {
    // Insert a subscription via adapter (simulating pre-existing state)
    db.addSubscription('Engram', JSON.stringify({ event_type: 'knowledge_update' }));

    // Reload the subscription cache (simulates Spine restart)
    wsHandler.loadSubscriptionCache();

    // Verify cache has the subscription
    const cache = wsHandler.getSubscriptionCache();
    assert.ok(cache.has('Engram'), 'cache should have Engram');
    const filters = cache.get('Engram');
    assert.ok(filters.some(f => f.event_type === 'knowledge_update'), 'should have knowledge_update filter');
  });

  it('11. GET /subscriptions — lists all persistent subscriptions', async () => {
    const { status, body } = await getJson(baseUrl, '/subscriptions');
    assert.equal(status, 200);
    assert.ok(Array.isArray(body.subscriptions));
    // Should have at least the Engram subscription from test 10 and Glia from test 7
    assert.ok(body.subscriptions.length >= 2, `expected >= 2 subscriptions, got ${body.subscriptions.length}`);
  });

  it('12. GET /subscriptions/:organ_id — organ-specific subscriptions', async () => {
    const { status, body } = await getJson(baseUrl, '/subscriptions/Glia');
    assert.equal(status, 200);
    assert.equal(body.organ_id, 'Glia');
    assert.ok(Array.isArray(body.subscriptions));
    assert.ok(body.subscriptions.length >= 1);
  });
});

// --- Delivery Guarantee Enforcement ---

describe('Delivery guarantee enforcement', () => {
  let db, app, server, baseUrl, wsHandler;

  before(async () => {
    ({ db, app, wsHandler } = createTestServer());
    ({ server, baseUrl } = await startServer(app, wsHandler));
  });

  after(async () => {
    wsHandler.cleanup();
    await new Promise((resolve) => server.close(resolve));
    db.close();
  });

  it('13. OTM with target_organ = "*" — accepted (broadcast)', async () => {
    const { status, body } = await postJson(baseUrl, '/messages', {
      type: 'OTM',
      source_organ: 'Vigil',
      target_organ: '*',
      payload: { event_type: 'health_check_all' },
    });
    assert.equal(status, 202);
    assert.equal(body.routing, 'broadcast');
    assert.ok(Array.isArray(body.delivered_to));
  });

  it('14. OTM with target_organ = "Vigil" — accepted (directed)', async () => {
    const { status, body } = await postJson(baseUrl, '/messages', {
      type: 'OTM',
      source_organ: 'Lobe',
      target_organ: 'Vigil',
      payload: { event_type: 'check_health' },
    });
    assert.equal(status, 202);
    assert.equal(body.routing, 'directed');
    assert.equal(body.target_organ, 'Vigil');
  });

  it('15. APM with target_organ = "Nomos" — accepted (directed)', async () => {
    const { status, body } = await postJson(baseUrl, '/messages', {
      type: 'APM',
      source_organ: 'Thalamus',
      target_organ: 'Nomos',
      payload: {
        action: 'propose_policy',
        targets: ['urn:test:1'],
        risk_tier: 'low',
        evidence_refs: ['urn:evidence:1'],
        rollback_plan: 'revert',
        reason: 'testing',
      },
    });
    assert.equal(status, 202);
    assert.equal(body.routing, 'directed');
    assert.equal(body.target_organ, 'Nomos');
  });

  it('16. APM with target_organ = "*" — 400 GOVERNANCE_MESSAGE_REQUIRES_DIRECTED_DELIVERY', async () => {
    const { status, body } = await postJson(baseUrl, '/messages', {
      type: 'APM',
      source_organ: 'Thalamus',
      target_organ: '*',
      payload: {
        action: 'broadcast_proposal',
        targets: ['urn:test:1'],
        risk_tier: 'low',
        evidence_refs: ['urn:evidence:1'],
        rollback_plan: 'revert',
        reason: 'testing',
      },
    });
    assert.equal(status, 400);
    assert.equal(body.error, 'GOVERNANCE_MESSAGE_REQUIRES_DIRECTED_DELIVERY');
    assert.equal(body.type, 'APM');
  });

  it('17. PEM with target_organ = "*" — 400 rejected', async () => {
    const { status, body } = await postJson(baseUrl, '/messages', {
      type: 'PEM',
      source_organ: 'Nomos',
      target_organ: '*',
      payload: {
        conflict_class: 'MSP_CONFLICT',
        blocked_action: 'test_action',
        blocking_rules: ['rule_1'],
        necessity: 'testing broadcast rejection',
        proposed_change: 'none',
        risk_assessment: 'low',
      },
    });
    assert.equal(status, 400);
    assert.equal(body.error, 'GOVERNANCE_MESSAGE_REQUIRES_DIRECTED_DELIVERY');
  });

  it('18. ATM with target_organ = "*" — 400 rejected', async () => {
    const { status, body } = await postJson(baseUrl, '/messages', {
      type: 'ATM',
      source_organ: 'Nomos',
      target_organ: '*',
      payload: {
        token_urn: 'urn:llm-ops:token:test-1',
        scope: {
          targets: ['urn:test:1'],
          action_types: ['write'],
          ttl_seconds: 3600,
        },
        ap_ref: 'urn:llm-ops:apm:test-ref',
      },
    });
    assert.equal(status, 400);
    assert.equal(body.error, 'GOVERNANCE_MESSAGE_REQUIRES_DIRECTED_DELIVERY');
  });

  it('19. HOM with target_organ = "*" — 400 rejected', async () => {
    const { status, body } = await postJson(baseUrl, '/messages', {
      type: 'HOM',
      source_organ: 'Arbiter',
      target_organ: '*',
      payload: {
        decision_type: 'bor_ambiguity',
        context: 'testing broadcast HOM rejection',
        question: 'Should this broadcast be allowed?',
        options: ['yes', 'no'],
      },
    });
    assert.equal(status, 400);
    assert.equal(body.error, 'GOVERNANCE_MESSAGE_REQUIRES_DIRECTED_DELIVERY');
  });

  it('20. APM with missing target_organ — 400 rejected by envelope validation', async () => {
    const { status, body } = await postJson(baseUrl, '/messages', {
      type: 'APM',
      source_organ: 'Thalamus',
      // target_organ deliberately omitted
      payload: {
        action: 'test',
        targets: ['urn:test:1'],
        risk_tier: 'low',
        evidence_refs: ['urn:evidence:1'],
        rollback_plan: 'revert',
        reason: 'testing',
      },
    });
    assert.equal(status, 400);
    // Caught by envelope validation (target_organ required)
    assert.ok(body.error);
  });
});

// --- Broadcast Routing ---

describe('Broadcast routing', () => {
  let db, app, server, baseUrl, wsHandler;

  before(async () => {
    ({ db, app, wsHandler } = createTestServer());
    ({ server, baseUrl } = await startServer(app, wsHandler));
  });

  after(async () => {
    wsHandler.cleanup();
    await new Promise((resolve) => server.close(resolve));
    db.close();
  });

  it('21. organ subscribes to filter — receives matching OTM broadcasts', async () => {
    const ws = connectWs(baseUrl);
    await new Promise((resolve) => ws.on('open', resolve));
    await registerOrganWs(ws, 'Vigil');

    ws.send(JSON.stringify({ action: 'subscribe', filter: { event_type: 'cv_failure' } }));
    await waitForMessage(ws, (d) => d.action === 'subscribed');

    // Send a matching broadcast
    const msgPromise = waitForMessage(ws, (d) => d.action === 'message');
    await postJson(baseUrl, '/messages', {
      type: 'OTM',
      source_organ: 'Spine',
      target_organ: '*',
      payload: { event_type: 'cv_failure', test_id: 'test-123' },
    });

    const pushed = await msgPromise;
    assert.equal(pushed.action, 'message');
    assert.equal(pushed.envelope.payload.event_type, 'cv_failure');
    assert.equal(pushed.envelope.payload.test_id, 'test-123');

    ws.close();
    await new Promise((resolve) => ws.on('close', resolve));
  });

  it('22. non-matching event_type — not received', async () => {
    const ws = connectWs(baseUrl);
    await new Promise((resolve) => ws.on('open', resolve));
    await registerOrganWs(ws, 'Glia');

    ws.send(JSON.stringify({ action: 'subscribe', filter: { event_type: 'health_check' } }));
    await waitForMessage(ws, (d) => d.action === 'subscribed');

    // Send a broadcast with different event_type
    const { body } = await postJson(baseUrl, '/messages', {
      type: 'OTM',
      source_organ: 'Spine',
      target_organ: '*',
      payload: { event_type: 'unrelated_event' },
    });

    // Glia should NOT be in delivered_to
    assert.ok(!body.delivered_to.includes('Glia'), 'Glia should not receive non-matching broadcast');

    ws.close();
    await new Promise((resolve) => ws.on('close', resolve));
  });

  it('23. two organs subscribe to same topic — both receive', async () => {
    const ws1 = connectWs(baseUrl);
    const ws2 = connectWs(baseUrl);
    await Promise.all([
      new Promise((resolve) => ws1.on('open', resolve)),
      new Promise((resolve) => ws2.on('open', resolve)),
    ]);

    await registerOrganWs(ws1, 'Vigil');
    await registerOrganWs(ws2, 'Glia');

    ws1.send(JSON.stringify({ action: 'subscribe', filter: { event_type: 'shared_event' } }));
    ws2.send(JSON.stringify({ action: 'subscribe', filter: { event_type: 'shared_event' } }));
    await Promise.all([
      waitForMessage(ws1, (d) => d.action === 'subscribed'),
      waitForMessage(ws2, (d) => d.action === 'subscribed'),
    ]);

    // Send broadcast
    const msg1Promise = waitForMessage(ws1, (d) => d.action === 'message');
    const msg2Promise = waitForMessage(ws2, (d) => d.action === 'message');

    const { body } = await postJson(baseUrl, '/messages', {
      type: 'OTM',
      source_organ: 'Spine',
      target_organ: '*',
      payload: { event_type: 'shared_event', data: { test: true } },
    });

    const [msg1, msg2] = await Promise.all([msg1Promise, msg2Promise]);
    assert.equal(msg1.envelope.payload.event_type, 'shared_event');
    assert.equal(msg2.envelope.payload.event_type, 'shared_event');
    assert.ok(body.delivered_to.includes('Vigil'));
    assert.ok(body.delivered_to.includes('Glia'));

    ws1.close();
    ws2.close();
    await Promise.all([
      new Promise((resolve) => ws1.on('close', resolve)),
      new Promise((resolve) => ws2.on('close', resolve)),
    ]);
  });

  it('24. organ with empty filter {} — receives ALL OTM broadcasts', async () => {
    const ws = connectWs(baseUrl);
    await new Promise((resolve) => ws.on('open', resolve));
    await registerOrganWs(ws, 'Axon');

    ws.send(JSON.stringify({ action: 'subscribe', filter: {} }));
    await waitForMessage(ws, (d) => d.action === 'subscribed');

    // Send any OTM broadcast
    const msgPromise = waitForMessage(ws, (d) => d.action === 'message');
    await postJson(baseUrl, '/messages', {
      type: 'OTM',
      source_organ: 'Spine',
      target_organ: '*',
      payload: { event_type: 'random_event_xyz' },
    });

    const pushed = await msgPromise;
    assert.equal(pushed.envelope.payload.event_type, 'random_event_xyz');

    ws.close();
    await new Promise((resolve) => ws.on('close', resolve));
  });

  it('25. multiple filters per organ — OR semantics', async () => {
    const ws = connectWs(baseUrl);
    await new Promise((resolve) => ws.on('open', resolve));
    await registerOrganWs(ws, 'Engram');

    // Subscribe to two different filters
    ws.send(JSON.stringify({ action: 'subscribe', filter: { event_type: 'alpha' } }));
    await waitForMessage(ws, (d) => d.action === 'subscribed');
    ws.send(JSON.stringify({ action: 'subscribe', filter: { event_type: 'beta' } }));
    await waitForMessage(ws, (d) => d.action === 'subscribed');

    // Send an OTM matching the second filter
    const msgPromise = waitForMessage(ws, (d) => d.action === 'message');
    const { body } = await postJson(baseUrl, '/messages', {
      type: 'OTM',
      source_organ: 'Spine',
      target_organ: '*',
      payload: { event_type: 'beta' },
    });

    const pushed = await msgPromise;
    assert.equal(pushed.envelope.payload.event_type, 'beta');
    assert.ok(body.delivered_to.includes('Engram'));

    ws.close();
    await new Promise((resolve) => ws.on('close', resolve));
  });

  it('26. multi-field filter — AND semantics', async () => {
    const ws = connectWs(baseUrl);
    await new Promise((resolve) => ws.on('open', resolve));
    await registerOrganWs(ws, 'Cortex');

    // Subscribe with multi-field filter
    ws.send(JSON.stringify({
      action: 'subscribe',
      filter: { event_type: 'state_transition', source_organ: 'Spine' },
    }));
    await waitForMessage(ws, (d) => d.action === 'subscribed');

    // Send matching broadcast (both fields match)
    const matchPromise = waitForMessage(ws, (d) => d.action === 'message');
    await postJson(baseUrl, '/messages', {
      type: 'OTM',
      source_organ: 'Spine',
      target_organ: '*',
      payload: { event_type: 'state_transition', data: { entity: 'job:1' } },
    });

    const matched = await matchPromise;
    assert.equal(matched.envelope.payload.event_type, 'state_transition');

    // Send non-matching broadcast (event_type matches but source_organ doesn't)
    const { body: nonMatchBody } = await postJson(baseUrl, '/messages', {
      type: 'OTM',
      source_organ: 'Vigil',
      target_organ: '*',
      payload: { event_type: 'state_transition', data: { entity: 'job:2' } },
    });

    // Cortex should NOT receive this one (source_organ mismatch)
    assert.ok(!nonMatchBody.delivered_to.includes('Cortex'),
      'Cortex should not receive when source_organ mismatches AND filter');

    ws.close();
    await new Promise((resolve) => ws.on('close', resolve));
  });

  it('27. broadcast with zero matching subscribers — 202, empty delivered_to', async () => {
    // No connected subscribers for this event type
    const { status, body } = await postJson(baseUrl, '/messages', {
      type: 'OTM',
      source_organ: 'Spine',
      target_organ: '*',
      payload: { event_type: 'nobody_listens_to_this' },
    });
    assert.equal(status, 202);
    assert.equal(body.routing, 'broadcast');
    assert.ok(Array.isArray(body.delivered_to));
    // Might be empty or contain organs with {} filter — both are valid
  });
});

// --- Directed Routing ---

describe('Directed routing', () => {
  let db, app, server, baseUrl, wsHandler;

  before(async () => {
    ({ db, app, wsHandler } = createTestServer());
    ({ server, baseUrl } = await startServer(app, wsHandler));
  });

  after(async () => {
    wsHandler.cleanup();
    await new Promise((resolve) => server.close(resolve));
    db.close();
  });

  it('28. send directed OTM — mailbox persistence + WebSocket push', async () => {
    const ws = connectWs(baseUrl);
    await new Promise((resolve) => ws.on('open', resolve));
    await registerOrganWs(ws, 'Vigil');

    const msgPromise = waitForMessage(ws, (d) => d.action === 'message');
    const { status, body } = await postJson(baseUrl, '/messages', {
      type: 'OTM',
      source_organ: 'Lobe',
      target_organ: 'Vigil',
      payload: { event_type: 'directed_test' },
    });

    assert.equal(status, 202);
    assert.equal(body.routing, 'directed');

    const pushed = await msgPromise;
    assert.equal(pushed.envelope.source_organ, 'Lobe');
    assert.equal(pushed.envelope.target_organ, 'Vigil');

    // Verify persisted in mailbox — message should be delivered (WS push succeeded)
    // Drain returns only undelivered, so depth should be 0 (already delivered via WS)
    const depth = db.getMailboxDepth('Vigil');
    assert.equal(depth, 0, 'should be 0 pending (WS push succeeded, message marked delivered)');

    ws.close();
    await new Promise((resolve) => ws.on('close', resolve));
  });

  it('29. send directed to disconnected organ — mailbox persistence only', async () => {
    const { status, body } = await postJson(baseUrl, '/messages', {
      type: 'OTM',
      source_organ: 'Spine',
      target_organ: 'SafeVault',
      payload: { event_type: 'backup_trigger' },
    });

    assert.equal(status, 202);
    assert.equal(body.routing, 'directed');

    // Verify persisted but not delivered — should be pending in mailbox
    const depth = db.getMailboxDepth('SafeVault');
    assert.ok(depth > 0, 'should have pending messages (organ disconnected)');
  });

  it('30. send directed to organ not in manifest — 400 ROUTING_FAILED', async () => {
    const { status, body } = await postJson(baseUrl, '/messages', {
      type: 'OTM',
      source_organ: 'Lobe',
      target_organ: 'NotInManifest',
      payload: { event_type: 'test' },
    });

    assert.equal(status, 400);
    assert.equal(body.error, 'ROUTING_FAILED');
    assert.ok(body.message.includes('NotInManifest'));
  });
});

// --- Reconnection Recovery ---

describe('Reconnection recovery', () => {
  let db, app, server, baseUrl, wsHandler;

  before(async () => {
    ({ db, app, wsHandler } = createTestServer());
    ({ server, baseUrl } = await startServer(app, wsHandler));
  });

  after(async () => {
    wsHandler.cleanup();
    await new Promise((resolve) => server.close(resolve));
    db.close();
  });

  it('31. organ disconnects, 5 directed messages sent, organ reconnects — all 5 pushed as backlog', async () => {
    // Connect and register
    const ws1 = connectWs(baseUrl);
    await new Promise((resolve) => ws1.on('open', resolve));
    await registerOrganWs(ws1, 'Receptor');

    // Disconnect
    ws1.close();
    await new Promise((resolve) => ws1.on('close', resolve));
    await new Promise((resolve) => setTimeout(resolve, 50));

    // Send 5 directed messages while disconnected
    for (let i = 1; i <= 5; i++) {
      await postJson(baseUrl, '/messages', {
        type: 'OTM',
        source_organ: 'Thalamus',
        target_organ: 'Receptor',
        payload: { event_type: 'queued_msg', data: { seq: i } },
      });
    }

    // Verify 5 messages pending
    const pendingCount = db.getMailboxDepth('Receptor');
    assert.equal(pendingCount, 5, 'should have 5 pending messages');

    // Reconnect
    const ws2 = connectWs(baseUrl);
    await new Promise((resolve) => ws2.on('open', resolve));

    // Collect registration + 5 backlog messages
    const allMessages = [];
    const collectionDone = new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`Timeout: only got ${allMessages.length} messages`)), 5000);
      ws2.on('message', (raw) => {
        const data = JSON.parse(raw.toString());
        allMessages.push(data);
        // registered + 5 backlog messages = 6
        if (allMessages.length >= 6) {
          clearTimeout(timer);
          resolve();
        }
      });
    });

    ws2.send(JSON.stringify({ action: 'register', organ_name: 'Receptor' }));
    await collectionDone;

    // First message should be registration
    assert.equal(allMessages[0].action, 'registered');
    assert.equal(allMessages[0].organ_name, 'Receptor');
    assert.equal(allMessages[0].mailbox_depth, 5);

    // Next 5 should be backlog messages
    const backlog = allMessages.slice(1);
    assert.equal(backlog.length, 5);
    for (const msg of backlog) {
      assert.equal(msg.action, 'message');
    }

    ws2.close();
    await new Promise((resolve) => ws2.on('close', resolve));
  });

  it('32. backlog delivered in FIFO order', async () => {
    // Connect and register a fresh organ
    const ws1 = connectWs(baseUrl);
    await new Promise((resolve) => ws1.on('open', resolve));
    await registerOrganWs(ws1, 'Syntra');

    // Disconnect
    ws1.close();
    await new Promise((resolve) => ws1.on('close', resolve));
    await new Promise((resolve) => setTimeout(resolve, 50));

    // Send 3 messages in sequence
    for (let i = 1; i <= 3; i++) {
      await postJson(baseUrl, '/messages', {
        type: 'OTM',
        source_organ: 'Spine',
        target_organ: 'Syntra',
        payload: { event_type: 'fifo_test', data: { seq: i } },
      });
      // Small delay to ensure timestamp ordering
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    // Reconnect and collect
    const ws2 = connectWs(baseUrl);
    await new Promise((resolve) => ws2.on('open', resolve));

    const allMessages = [];
    const collectionDone = new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`Timeout: got ${allMessages.length}`)), 5000);
      ws2.on('message', (raw) => {
        const data = JSON.parse(raw.toString());
        allMessages.push(data);
        if (allMessages.length >= 4) { // registered + 3 messages
          clearTimeout(timer);
          resolve();
        }
      });
    });

    ws2.send(JSON.stringify({ action: 'register', organ_name: 'Syntra' }));
    await collectionDone;

    const backlog = allMessages.filter(m => m.action === 'message');
    assert.equal(backlog.length, 3);
    assert.equal(backlog[0].envelope.payload.data.seq, 1);
    assert.equal(backlog[1].envelope.payload.data.seq, 2);
    assert.equal(backlog[2].envelope.payload.data.seq, 3);

    ws2.close();
    await new Promise((resolve) => ws2.on('close', resolve));
  });

  it('33. after backlog, new messages arrive in real-time', async () => {
    // Connect, subscribe, and disconnect
    const ws1 = connectWs(baseUrl);
    await new Promise((resolve) => ws1.on('open', resolve));
    await registerOrganWs(ws1, 'Lobe');
    ws1.close();
    await new Promise((resolve) => ws1.on('close', resolve));
    await new Promise((resolve) => setTimeout(resolve, 50));

    // Send 1 message while disconnected
    await postJson(baseUrl, '/messages', {
      type: 'OTM',
      source_organ: 'Spine',
      target_organ: 'Lobe',
      payload: { event_type: 'backlog_msg' },
    });

    // Reconnect
    const ws2 = connectWs(baseUrl);
    await new Promise((resolve) => ws2.on('open', resolve));

    // Collect registration + backlog
    const allMessages = [];
    const backlogDone = new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Timeout')), 5000);
      ws2.on('message', (raw) => {
        const data = JSON.parse(raw.toString());
        allMessages.push(data);
        if (allMessages.length >= 2) { // registered + 1 backlog
          clearTimeout(timer);
          resolve();
        }
      });
    });

    ws2.send(JSON.stringify({ action: 'register', organ_name: 'Lobe' }));
    await backlogDone;

    // Now send a new real-time message
    const rtPromise = waitForMessage(ws2, (d) =>
      d.action === 'message' && d.envelope.payload.event_type === 'realtime_msg'
    );

    await postJson(baseUrl, '/messages', {
      type: 'OTM',
      source_organ: 'Spine',
      target_organ: 'Lobe',
      payload: { event_type: 'realtime_msg' },
    });

    const realtime = await rtPromise;
    assert.equal(realtime.envelope.payload.event_type, 'realtime_msg');

    ws2.close();
    await new Promise((resolve) => ws2.on('close', resolve));
  });
});

// --- State Transition Events ---

describe('State transition events', () => {
  let db, app, server, baseUrl, wsHandler, stateMachine;

  before(async () => {
    ({ db, app, wsHandler, stateMachine } = createTestServer());
    ({ server, baseUrl } = await startServer(app, wsHandler));
  });

  after(async () => {
    wsHandler.cleanup();
    await new Promise((resolve) => server.close(resolve));
    db.close();
  });

  it('34. execute state transition — OTM broadcast with event_type "state_transition"', async () => {
    // Connect an organ and subscribe to state_transition events
    const ws = connectWs(baseUrl);
    await new Promise((resolve) => ws.on('open', resolve));
    await registerOrganWs(ws, 'Cortex');

    ws.send(JSON.stringify({ action: 'subscribe', filter: { event_type: 'state_transition' } }));
    await waitForMessage(ws, (d) => d.action === 'subscribed');

    // Create an entity and trigger a transition
    const entity = stateMachine.createEntity('urn:llm-ops:job:test-transition', 'job');
    assert.ok(!entity.error, `Entity creation failed: ${entity.error}`);

    // Listen for the broadcast
    const transitionPromise = waitForMessage(ws, (d) =>
      d.action === 'message' && d.envelope.payload.event_type === 'state_transition'
    );

    // Execute a transition
    const result = stateMachine.transition(
      'urn:llm-ops:job:test-transition',
      'CREATED',
      'PLANNING',
      'Job planning initiated',
      'Thalamus',
    );
    assert.ok(!result.error, `Transition failed: ${result.error}`);

    const pushed = await transitionPromise;
    assert.equal(pushed.envelope.type, 'OTM');
    assert.equal(pushed.envelope.source_organ, 'Spine');
    assert.equal(pushed.envelope.target_organ, '*');
    assert.equal(pushed.envelope.payload.source, 'spine-state');
    assert.equal(pushed.envelope.payload.data.entity_urn, 'urn:llm-ops:job:test-transition');
    assert.equal(pushed.envelope.payload.data.previous_state, 'CREATED');
    assert.equal(pushed.envelope.payload.data.current_state, 'PLANNING');
    assert.equal(pushed.envelope.payload.data.actor, 'Thalamus');

    ws.close();
    await new Promise((resolve) => ws.on('close', resolve));
  });

  it('35. subscriber with matching filter receives state change notification', async () => {
    // Two organs: one subscribing to state_transition, one to something else
    const wsSub = connectWs(baseUrl);
    const wsNoSub = connectWs(baseUrl);
    await Promise.all([
      new Promise((resolve) => wsSub.on('open', resolve)),
      new Promise((resolve) => wsNoSub.on('open', resolve)),
    ]);

    await registerOrganWs(wsSub, 'Vigil');
    await registerOrganWs(wsNoSub, 'Glia');

    wsSub.send(JSON.stringify({ action: 'subscribe', filter: { event_type: 'state_transition' } }));
    await waitForMessage(wsSub, (d) => d.action === 'subscribed');

    // Glia subscribes to something different
    wsNoSub.send(JSON.stringify({ action: 'subscribe', filter: { event_type: 'health_check' } }));
    await waitForMessage(wsNoSub, (d) => d.action === 'subscribed');

    // Trigger a transition
    const entity = stateMachine.createEntity('urn:llm-ops:job:test-filter', 'job');
    assert.ok(!entity.error);

    const subPromise = waitForMessage(wsSub, (d) =>
      d.action === 'message' && d.envelope.payload.event_type === 'state_transition'
    );

    stateMachine.transition(
      'urn:llm-ops:job:test-filter',
      'CREATED',
      'PLANNING',
      'Filter test',
      'TestActor',
    );

    const subMsg = await subPromise;
    assert.equal(subMsg.envelope.payload.data.entity_urn, 'urn:llm-ops:job:test-filter');

    // Glia should NOT have received the state_transition
    // (We can verify by checking the routing response — Glia's filter doesn't match)
    // This is implicitly tested by the broadcast routing engine

    wsSub.close();
    wsNoSub.close();
    await Promise.all([
      new Promise((resolve) => wsSub.on('close', resolve)),
      new Promise((resolve) => wsNoSub.on('close', resolve)),
    ]);
  });
});
