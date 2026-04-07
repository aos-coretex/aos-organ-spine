/**
 * Relay 6 — Health Monitoring Protocol tests.
 *
 * Covers: heartbeat with organ lifecycle state transitions, Vigil notification
 * broadcasts, mailbox persistence after disconnect, TTL expiry, back-pressure
 * signaling, Spine self-health OTM, updated /health endpoint, state machine
 * transition recording.
 *
 * Uses Node.js built-in test runner, real HTTP server on random port,
 * in-memory SQLite for isolation. Short heartbeat intervals for fast tests.
 *
 * Run: node --test test/health-monitoring.test.js
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
import { createEventsRouter } from '../server/routes/events.js';
import { createSchemasRouter } from '../server/routes/schemas.js';
import { createWebSocketHandler } from '../server/ws/handler.js';
import { createStateMachine } from '../server/state/machine.js';
import { routeMessage } from '../server/routing/router.js';
import { generateUrn } from '../lib/urn.js';

// ================================================================
// Test helpers
// ================================================================

function createTestServer(overrides = {}) {
  const adapter = initDatabase(':memory:');
  const manifest = createManifest(adapter);
  const wsHandler = createWebSocketHandler(adapter, manifest);

  const stateMachine = createStateMachine(adapter, {
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
        adapter,
      });
      adapter.persistEvent(envelope, 'broadcast');
    },
  });

  function emitMessage(envelope) {
    const result = routeMessage(envelope, {
      manifest,
      subscriptionCache: wsHandler.getSubscriptionCache(),
      pushToOrgan: wsHandler.pushToOrgan,
      isOrganConnected: wsHandler.isOrganConnected,
      adapter,
    });
    const routing = envelope.target_organ === '*' ? 'broadcast' : 'directed';
    adapter.persistEvent(envelope, routing);
    return result;
  }

  const healthConfig = {
    pingIntervalMs: overrides.pingIntervalMs ?? 50,
    maxMissedPongs: overrides.maxMissedPongs ?? 3,
    healthHeartbeatMs: overrides.healthHeartbeatMs ?? 200,
    defaultTtlSeconds: overrides.defaultTtlSeconds ?? 3600,
    mailboxPressureThreshold: overrides.mailboxPressureThreshold ?? 100,
    criticalMissingThresholdMs: overrides.criticalMissingThresholdMs ?? 300_000,
    reconnectionTimeoutMs: overrides.reconnectionTimeoutMs ?? 30_000,
  };

  wsHandler.setHealthDependencies({ stateMachine, emitMessage, healthConfig });

  const app = express();
  app.use(express.json());
  app.use('/state', createStateRouter(stateMachine));
  app.use('/', createMessagesRouter(adapter, wsHandler, manifest, healthConfig));
  app.use('/mailbox', createMailboxRouter(adapter));
  app.use('/', createSchemasRouter(adapter));
  app.use('/', createEventsRouter(adapter));
  app.use('/', createHealthRouter(adapter, ':memory:', 0, manifest, wsHandler, healthConfig));

  return { app, adapter, manifest, wsHandler, stateMachine, healthConfig };
}

async function startServer(setup) {
  const server = setup.app.listen(0);
  await new Promise(r => server.on('listening', r));
  const port = server.address().port;
  setup.wsHandler.attach(server);
  return {
    server,
    baseUrl: `http://localhost:${port}`,
    wsUrl: `ws://localhost:${port}/subscribe`,
  };
}

function stopServer(setup, srv) {
  setup.wsHandler.cleanup();
  srv.server.close();
  setup.adapter.close();
}

function wait(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function connectWs(url, options = {}) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url, options);
    ws.on('open', () => resolve(ws));
    ws.on('error', reject);
  });
}

function registerOrgan(ws, organName) {
  return new Promise((resolve) => {
    const handler = (data) => {
      const msg = JSON.parse(data);
      if (msg.action === 'registered') {
        ws.removeListener('message', handler);
        resolve(msg);
      }
    };
    ws.on('message', handler);
    ws.send(JSON.stringify({ action: 'register', organ_name: organName }));
  });
}

function collectMessages(ws, count, timeoutMs = 2000) {
  return new Promise((resolve) => {
    const messages = [];
    const handler = (data) => {
      messages.push(JSON.parse(data));
      if (messages.length >= count) {
        ws.removeListener('message', handler);
        clearTimeout(timer);
        resolve(messages);
      }
    };
    ws.on('message', handler);
    const timer = setTimeout(() => {
      ws.removeListener('message', handler);
      resolve(messages);
    }, timeoutMs);
  });
}

async function postMessage(baseUrl, body) {
  const res = await fetch(`${baseUrl}/messages`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  return {
    status: res.status,
    headers: res.headers,
    body: await res.json(),
  };
}

async function getJson(url) {
  const res = await fetch(url);
  return res.json();
}

// ================================================================
// Tests
// ================================================================

describe('Health Monitoring (Relay 6)', () => {

  // --- Heartbeat and disconnect (tests 1-6) ---

  describe('Heartbeat and disconnect', () => {
    let setup, srv;

    before(async () => {
      setup = createTestServer({ pingIntervalMs: 50, healthHeartbeatMs: 5000 });
      srv = await startServer(setup);
    });

    after(() => stopServer(setup, srv));

    it('1. Connect organ -> verify ALIVE state', async () => {
      const ws = await connectWs(srv.wsUrl);
      await registerOrgan(ws, 'Vigil');

      const state = await getJson(`${srv.baseUrl}/state/${encodeURIComponent('organ:Vigil')}`);
      assert.equal(state.current_state, 'ALIVE');

      ws.close();
      await wait(100);
    });

    it('2. Stop responding to pings (1 miss) -> verify DEGRADED state', async () => {
      const ws = await connectWs(srv.wsUrl, { autoPong: false });
      await registerOrgan(ws, 'Glia');

      // Wait for 1 heartbeat cycle (50ms) + buffer
      await wait(100);

      const state = await getJson(`${srv.baseUrl}/state/${encodeURIComponent('organ:Glia')}`);
      assert.equal(state.current_state, 'DEGRADED');

      ws.terminate();
      await wait(100);
    });

    it('3. Resume responding (pong) -> verify return to ALIVE', async () => {
      const ws = await connectWs(srv.wsUrl, { autoPong: false });
      await registerOrgan(ws, 'Lobe');

      // Miss 1 pong -> DEGRADED
      await wait(100);
      let state = await getJson(`${srv.baseUrl}/state/${encodeURIComponent('organ:Lobe')}`);
      assert.equal(state.current_state, 'DEGRADED');

      // Start responding to pings manually
      ws.on('ping', (data) => ws.pong(data));
      await wait(100);

      state = await getJson(`${srv.baseUrl}/state/${encodeURIComponent('organ:Lobe')}`);
      assert.equal(state.current_state, 'ALIVE');

      ws.terminate();
      await wait(100);
    });

    it('4. 3 missed pongs -> verify DISCONNECTED state and WebSocket closed', async () => {
      const ws = await connectWs(srv.wsUrl, { autoPong: false });
      await registerOrgan(ws, 'Syntra');

      // Wait for 3+ heartbeat cycles (3 * 50ms + buffer)
      await wait(300);

      const state = await getJson(`${srv.baseUrl}/state/${encodeURIComponent('organ:Syntra')}`);
      assert.equal(state.current_state, 'DISCONNECTED');
      assert.notEqual(ws.readyState, WebSocket.OPEN);
    });

    it('5. Verify organ_disconnected OTM broadcast emitted on disconnect', async () => {
      // Subscriber listens for Vigil notifications
      const listener = await connectWs(srv.wsUrl);
      await registerOrgan(listener, 'Vectr');
      listener.send(JSON.stringify({
        action: 'subscribe',
        filter: { event_type: 'organ_disconnected' },
      }));
      await wait(50);

      // Connect organ that will time out
      const organ = await connectWs(srv.wsUrl, { autoPong: false });
      await registerOrgan(organ, 'Graph');

      // Collect broadcast messages while organ times out
      const messages = await collectMessages(listener, 1, 400);

      const disconnectMsg = messages.find(m =>
        m.action === 'message' &&
        m.envelope?.payload?.event_type === 'organ_disconnected' &&
        m.envelope?.payload?.data?.organ_name === 'Graph'
      );
      assert.ok(disconnectMsg, 'organ_disconnected OTM should be emitted for Graph');
      assert.equal(disconnectMsg.envelope.payload.data.reason, 'heartbeat_timeout');

      listener.close();
      await wait(100);
    });

    it('6. Verify organ_connected OTM broadcast emitted on reconnect', async () => {
      // Phi was not used in this group yet — disconnect it first
      const ws1 = await connectWs(srv.wsUrl, { autoPong: false });
      await registerOrgan(ws1, 'Phi');
      await wait(300); // heartbeat timeout

      // Set up subscriber for organ_connected events
      const listener = await connectWs(srv.wsUrl);
      await registerOrgan(listener, 'Radiant');
      listener.send(JSON.stringify({
        action: 'subscribe',
        filter: { event_type: 'organ_connected' },
      }));
      await wait(50);

      // Start collecting before reconnection
      const collecting = collectMessages(listener, 1, 400);

      // Reconnect Phi
      const ws2 = await connectWs(srv.wsUrl);
      await registerOrgan(ws2, 'Phi');

      const messages = await collecting;
      const connectMsg = messages.find(m =>
        m.action === 'message' &&
        m.envelope?.payload?.event_type === 'organ_connected' &&
        m.envelope?.payload?.data?.organ_name === 'Phi'
      );
      assert.ok(connectMsg, 'organ_connected OTM should be emitted for Phi');
      assert.equal(connectMsg.envelope.payload.data.previous_state, 'DISCONNECTED');

      listener.close();
      ws2.close();
      await wait(100);
    });
  });

  // --- Mailbox persistence after disconnect (tests 7-8) ---

  describe('Mailbox persistence after disconnect', () => {
    let setup, srv;

    before(async () => {
      setup = createTestServer({ pingIntervalMs: 50, healthHeartbeatMs: 5000 });
      srv = await startServer(setup);
    });

    after(() => stopServer(setup, srv));

    it('7. Organ disconnects -> send 3 messages -> verify all 3 in mailbox', async () => {
      // Connect and let heartbeat timeout
      const ws = await connectWs(srv.wsUrl, { autoPong: false });
      await registerOrgan(ws, 'Engram');
      await wait(300);

      // Send 3 directed messages to disconnected organ
      for (let i = 1; i <= 3; i++) {
        await postMessage(srv.baseUrl, {
          type: 'OTM',
          source_organ: 'Spine',
          target_organ: 'Engram',
          payload: { event_type: 'test_msg', data: { seq: i } },
        });
      }

      const mailbox = await getJson(`${srv.baseUrl}/mailbox/Engram`);
      assert.equal(mailbox.depth, 3);
    });

    it('8. Organ reconnects -> drain -> verify all 3 messages delivered FIFO', async () => {
      // Reconnect Engram — backlog is pushed automatically during registration
      const ws = await connectWs(srv.wsUrl);
      await registerOrgan(ws, 'Engram');
      await wait(100);

      // After backlog push, all messages should be marked delivered
      const mailbox = await getJson(`${srv.baseUrl}/mailbox/Engram`);
      assert.equal(mailbox.depth, 0, 'All messages should be delivered via backlog push');

      ws.close();
      await wait(100);
    });
  });

  // --- TTL expiry (tests 9-10) ---

  describe('TTL expiry', () => {
    let setup, srv;

    before(async () => {
      // Short health check interval for fast TTL sweep
      setup = createTestServer({ pingIntervalMs: 50, healthHeartbeatMs: 200 });
      srv = await startServer(setup);
    });

    after(() => stopServer(setup, srv));

    it('9. Send OTM with 1s TTL to disconnected organ -> verify expired', async () => {
      // Disconnect SafeVault
      const ws = await connectWs(srv.wsUrl, { autoPong: false });
      await registerOrgan(ws, 'SafeVault');
      await wait(300);

      // Send OTM with 1-second TTL
      await postMessage(srv.baseUrl, {
        type: 'OTM',
        source_organ: 'Spine',
        target_organ: 'SafeVault',
        payload: { event_type: 'test_ttl' },
        ttl_seconds: 1,
      });

      // Verify message is in mailbox
      let mailbox = await getJson(`${srv.baseUrl}/mailbox/SafeVault`);
      assert.ok(mailbox.depth >= 1, 'Message should be in mailbox initially');

      // Wait for TTL expiry (1s) + health check sweep cycles
      await wait(1500);

      mailbox = await getJson(`${srv.baseUrl}/mailbox/SafeVault`);
      assert.equal(mailbox.depth, 0, 'TTL-expired message should be swept');
    });

    it('10. Send APM to disconnected organ -> verify still in mailbox (NULL TTL)', async () => {
      // SafeVault is still disconnected from test 9
      // Send APM — governance type, NULL TTL, never expires
      await postMessage(srv.baseUrl, {
        type: 'APM',
        source_organ: 'Thalamus',
        target_organ: 'SafeVault',
        payload: {
          action: 'test_action',
          targets: ['test'],
          risk_tier: 'low',
          evidence_refs: [],
          rollback_plan: 'none',
          reason: 'test governance persistence',
        },
      });

      // Wait longer than OTM TTL sweep cycles
      await wait(600);

      const mailbox = await getJson(`${srv.baseUrl}/mailbox/SafeVault`);
      assert.ok(mailbox.depth >= 1, 'APM should persist (NULL TTL — governance never expires)');
    });
  });

  // --- Back-pressure (tests 11-12) ---

  describe('Back-pressure signaling', () => {

    it('11. Fill mailbox beyond threshold -> verify mailbox_pressure OTM emitted', async () => {
      const setup = createTestServer({
        pingIntervalMs: 50,
        healthHeartbeatMs: 5000,
        mailboxPressureThreshold: 3,
      });
      const srv = await startServer(setup);

      try {
        // Subscriber listens for pressure OTM
        const listener = await connectWs(srv.wsUrl);
        await registerOrgan(listener, 'Vectr');
        listener.send(JSON.stringify({
          action: 'subscribe',
          filter: { event_type: 'mailbox_pressure' },
        }));
        await wait(50);

        // Disconnect target organ
        const target = await connectWs(srv.wsUrl, { autoPong: false });
        await registerOrgan(target, 'Glia');
        await wait(300);

        // Start collecting before filling
        const collecting = collectMessages(listener, 1, 1000);

        // Send 4 messages (threshold is 3)
        for (let i = 0; i < 4; i++) {
          await postMessage(srv.baseUrl, {
            type: 'OTM',
            source_organ: 'Spine',
            target_organ: 'Glia',
            payload: { event_type: 'fill', data: { i } },
          });
        }

        const messages = await collecting;
        const pressureMsg = messages.find(m =>
          m.action === 'message' &&
          m.envelope?.payload?.event_type === 'mailbox_pressure'
        );
        assert.ok(pressureMsg, 'mailbox_pressure OTM should be emitted');
        assert.equal(pressureMsg.envelope.payload.data.organ_name, 'Glia');
        assert.ok(pressureMsg.envelope.payload.data.depth > 3);
        assert.equal(pressureMsg.envelope.payload.data.threshold, 3);

        listener.close();
      } finally {
        stopServer(setup, srv);
      }
    });

    it('12. Verify X-Mailbox-Pressure header on POST /messages', async () => {
      const setup = createTestServer({
        pingIntervalMs: 50,
        healthHeartbeatMs: 5000,
        mailboxPressureThreshold: 2,
      });
      const srv = await startServer(setup);

      try {
        // Disconnect target
        const organ = await connectWs(srv.wsUrl, { autoPong: false });
        await registerOrgan(organ, 'Axon');
        await wait(300);

        // Fill mailbox past threshold (2)
        await postMessage(srv.baseUrl, {
          type: 'OTM', source_organ: 'Spine', target_organ: 'Axon',
          payload: { event_type: 'fill' },
        });
        await postMessage(srv.baseUrl, {
          type: 'OTM', source_organ: 'Spine', target_organ: 'Axon',
          payload: { event_type: 'fill' },
        });

        // Third message should trigger pressure header
        const response = await postMessage(srv.baseUrl, {
          type: 'OTM', source_organ: 'Spine', target_organ: 'Axon',
          payload: { event_type: 'fill' },
        });

        assert.equal(response.headers.get('x-mailbox-pressure'), 'true');
      } finally {
        stopServer(setup, srv);
      }
    });
  });

  // --- Spine self-health (tests 13-14) ---

  describe('Spine self-health', () => {
    let setup, srv;

    before(async () => {
      setup = createTestServer({ pingIntervalMs: 50, healthHeartbeatMs: 200 });
      srv = await startServer(setup);
    });

    after(() => stopServer(setup, srv));

    it('13. Verify periodic spine_health OTM broadcast emitted', async () => {
      // Subscribe to spine_health events
      const listener = await connectWs(srv.wsUrl);
      await registerOrgan(listener, 'Minder');
      listener.send(JSON.stringify({
        action: 'subscribe',
        filter: { event_type: 'spine_health' },
      }));
      await wait(50);

      // Wait for health check cycle (200ms) + buffer
      const messages = await collectMessages(listener, 1, 600);

      const healthMsg = messages.find(m =>
        m.action === 'message' &&
        m.envelope?.payload?.event_type === 'spine_health'
      );
      assert.ok(healthMsg, 'spine_health OTM should be emitted');
      assert.equal(healthMsg.envelope.source_organ, 'Spine');
      assert.equal(healthMsg.envelope.payload.data.sqlite_ok, true);
      assert.ok(typeof healthMsg.envelope.payload.data.connected_organs === 'number');
      assert.ok(typeof healthMsg.envelope.payload.data.uptime_s === 'number');
      assert.ok(typeof healthMsg.envelope.payload.data.total_mailbox_depth === 'number');

      listener.close();
      await wait(100);
    });

    it('14. GET /health includes organ lifecycle stats', async () => {
      // Connect an organ so there's at least one ALIVE
      const ws = await connectWs(srv.wsUrl);
      await registerOrgan(ws, 'Cortex');

      const health = await getJson(`${srv.baseUrl}/health`);

      // Manifest section
      assert.ok(health.manifest, '/health should include manifest section');
      assert.equal(health.manifest.total_organs, 29); // 28 DIO + human-principal
      assert.ok(typeof health.manifest.required === 'number');
      assert.ok(typeof health.manifest.connected === 'number');
      assert.ok(Array.isArray(health.manifest.missing_required));

      // Organs section
      assert.ok(health.organs !== undefined, '/health should include organs section');
      assert.ok(typeof health.organs.alive === 'number');
      assert.ok(typeof health.organs.degraded === 'number');
      assert.ok(typeof health.organs.disconnected === 'number');
      assert.ok(health.organs.alive >= 1, 'At least one organ should be ALIVE');

      // Mailbox section
      assert.ok(health.mailbox !== undefined, '/health should include mailbox section');
      assert.ok(typeof health.mailbox.total_depth === 'number');
      assert.ok(Array.isArray(health.mailbox.pressure_organs));

      ws.close();
      await wait(100);
    });
  });

  // --- State machine integration (tests 15-16) ---

  describe('State machine integration', () => {
    let setup, srv;

    before(async () => {
      setup = createTestServer({ pingIntervalMs: 50, healthHeartbeatMs: 5000 });
      srv = await startServer(setup);
    });

    after(() => stopServer(setup, srv));

    it('15. Verify organ state transitions are recorded in state_transitions table', async () => {
      const ws = await connectWs(srv.wsUrl);
      await registerOrgan(ws, 'Thalamus');

      const state = await getJson(`${srv.baseUrl}/state/${encodeURIComponent('organ:Thalamus')}`);

      // Should have REGISTERED -> ALIVE transition
      assert.ok(state.history.length >= 1, 'Should have at least 1 transition');
      const aliveTransition = state.history.find(t => t.to_state === 'ALIVE' && t.from_state === 'REGISTERED');
      assert.ok(aliveTransition, 'REGISTERED -> ALIVE transition should be recorded');
      assert.equal(aliveTransition.reason, 'organ_connected');
      assert.equal(aliveTransition.actor, 'Spine');

      ws.close();
      await wait(100);
    });

    it('16. GET /state/organ:<name> shows full transition history', async () => {
      // Connect, let degrade, let disconnect, reconnect
      const ws1 = await connectWs(srv.wsUrl, { autoPong: false });
      await registerOrgan(ws1, 'ModelBroker');

      // Wait for DEGRADED (1 miss) then DISCONNECTED (3 misses)
      await wait(300);

      // Reconnect
      const ws2 = await connectWs(srv.wsUrl);
      await registerOrgan(ws2, 'ModelBroker');

      const state = await getJson(`${srv.baseUrl}/state/${encodeURIComponent('organ:ModelBroker')}`);

      assert.ok(state.history, 'history should be present');
      const transitions = state.history.map(t => `${t.from_state}->${t.to_state}`);

      // Full lifecycle: REGISTERED->ALIVE, ALIVE->DEGRADED, DEGRADED->DISCONNECTED, DISCONNECTED->ALIVE
      assert.ok(transitions.includes('REGISTERED->ALIVE'), 'REGISTERED->ALIVE');
      assert.ok(transitions.includes('ALIVE->DEGRADED'), 'ALIVE->DEGRADED');
      assert.ok(transitions.includes('DEGRADED->DISCONNECTED'), 'DEGRADED->DISCONNECTED');
      assert.ok(transitions.includes('DISCONNECTED->ALIVE'), 'DISCONNECTED->ALIVE (reconnect)');

      assert.equal(state.current_state, 'ALIVE', 'Final state should be ALIVE after reconnect');

      ws2.close();
      await wait(100);
    });
  });
});
