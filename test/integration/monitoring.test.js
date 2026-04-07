/**
 * Integration test: Health monitoring (tests 36-47).
 *
 * Exercises organ health monitoring protocol, heartbeat lifecycle,
 * TTL expiry, back-pressure signaling, and manifest-aware supervision.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import WebSocket from 'ws';
import {
  createTestServer, startServer, stopServer,
  connectWs, registerOrgan, subscribeFilter,
  collectMessages, waitForMessage, postMessage, getJson, wait,
} from './helpers.js';

describe('Integration: Health monitoring', () => {

  describe('Organ lifecycle', () => {
    let setup, srv;

    before(async () => {
      setup = createTestServer({ pingIntervalMs: 50, healthHeartbeatMs: 5000 });
      srv = await startServer(setup);
    });

    after(() => stopServer(setup, srv));

    it('36. Connect organ → verify state ALIVE', async () => {
      const ws = await connectWs(srv.wsUrl);
      await registerOrgan(ws, 'Vigil');

      const state = await getJson(`${srv.baseUrl}/state/${encodeURIComponent('organ:Vigil')}`);
      assert.equal(state.current_state, 'ALIVE');

      ws.close();
      await wait(100);
    });

    it('37. Organ stops responding → 1 missed pong → DEGRADED', async () => {
      const ws = await connectWs(srv.wsUrl, { autoPong: false });
      await registerOrgan(ws, 'Glia');

      // Wait for 1 heartbeat cycle (50ms) + buffer
      await wait(100);

      const state = await getJson(`${srv.baseUrl}/state/${encodeURIComponent('organ:Glia')}`);
      assert.equal(state.current_state, 'DEGRADED');

      ws.terminate();
      await wait(100);
    });

    it('38. Organ resumes → pong → back to ALIVE', async () => {
      const ws = await connectWs(srv.wsUrl, { autoPong: false });
      await registerOrgan(ws, 'Lobe');

      // Miss 1 pong → DEGRADED
      await wait(100);
      let state = await getJson(`${srv.baseUrl}/state/${encodeURIComponent('organ:Lobe')}`);
      assert.equal(state.current_state, 'DEGRADED');

      // Start responding manually
      ws.on('ping', (data) => ws.pong(data));
      await wait(100);

      state = await getJson(`${srv.baseUrl}/state/${encodeURIComponent('organ:Lobe')}`);
      assert.equal(state.current_state, 'ALIVE');

      ws.terminate();
      await wait(100);
    });

    it('39. 3 missed pongs → DISCONNECTED', async () => {
      const ws = await connectWs(srv.wsUrl, { autoPong: false });
      await registerOrgan(ws, 'Syntra');

      // Wait for 3+ heartbeat cycles (3 * 50ms + buffer)
      await wait(300);

      const state = await getJson(`${srv.baseUrl}/state/${encodeURIComponent('organ:Syntra')}`);
      assert.equal(state.current_state, 'DISCONNECTED');
      assert.notEqual(ws.readyState, WebSocket.OPEN);
    });

    it('40. Verify organ_disconnected OTM broadcast', async () => {
      const listener = await connectWs(srv.wsUrl);
      await registerOrgan(listener, 'Vectr');
      await subscribeFilter(listener, { event_type: 'organ_disconnected' });
      await wait(50);

      // Connect organ that will time out
      const organ = await connectWs(srv.wsUrl, { autoPong: false });
      await registerOrgan(organ, 'Graph');

      const msg = await waitForMessage(listener, m =>
        m.action === 'message' &&
        m.envelope?.payload?.event_type === 'organ_disconnected' &&
        m.envelope?.payload?.data?.organ_name === 'Graph',
        500,
      );

      assert.ok(msg, 'organ_disconnected OTM should be broadcast');
      assert.equal(msg.envelope.source_organ, 'Spine');
      assert.equal(msg.envelope.payload.data.reason, 'heartbeat_timeout');

      listener.close();
      await wait(100);
    });

    it('41. Organ reconnects → ALIVE → organ_connected OTM broadcast emitted', async () => {
      // Disconnect Phi
      const ws1 = await connectWs(srv.wsUrl, { autoPong: false });
      await registerOrgan(ws1, 'Phi');
      await wait(300);

      // Set up listener
      const listener = await connectWs(srv.wsUrl);
      await registerOrgan(listener, 'Radiant');
      await subscribeFilter(listener, { event_type: 'organ_connected' });
      await wait(50);

      const collecting = waitForMessage(listener, m =>
        m.action === 'message' &&
        m.envelope?.payload?.event_type === 'organ_connected' &&
        m.envelope?.payload?.data?.organ_name === 'Phi',
        500,
      );

      // Reconnect
      const ws2 = await connectWs(srv.wsUrl);
      await registerOrgan(ws2, 'Phi');

      const msg = await collecting;
      assert.ok(msg, 'organ_connected OTM should be broadcast on reconnect');

      const state = await getJson(`${srv.baseUrl}/state/${encodeURIComponent('organ:Phi')}`);
      assert.equal(state.current_state, 'ALIVE');

      listener.close();
      ws2.close();
      await wait(100);
    });

    it('42. Transition history shows full lifecycle', async () => {
      // Connect, degrade, disconnect, reconnect
      const ws1 = await connectWs(srv.wsUrl, { autoPong: false });
      await registerOrgan(ws1, 'ModelBroker');
      await wait(300); // DEGRADED then DISCONNECTED

      const ws2 = await connectWs(srv.wsUrl);
      await registerOrgan(ws2, 'ModelBroker');

      const state = await getJson(`${srv.baseUrl}/state/${encodeURIComponent('organ:ModelBroker')}`);
      assert.ok(state.history.length >= 4, 'Should have full lifecycle history');

      const transitions = state.history.map(t => `${t.from_state}->${t.to_state}`);
      assert.ok(transitions.includes('REGISTERED->ALIVE'));
      assert.ok(transitions.includes('ALIVE->DEGRADED'));
      assert.ok(transitions.includes('DEGRADED->DISCONNECTED'));
      assert.ok(transitions.includes('DISCONNECTED->ALIVE'));

      ws2.close();
      await wait(100);
    });
  });

  describe('TTL expiry', () => {
    let setup, srv;

    before(async () => {
      setup = createTestServer({ pingIntervalMs: 50, healthHeartbeatMs: 200 });
      srv = await startServer(setup);
    });

    after(() => stopServer(setup, srv));

    it('43. OTM with 1s TTL → expires after disconnect → message gone from mailbox', async () => {
      // Disconnect organ
      const ws = await connectWs(srv.wsUrl, { autoPong: false });
      await registerOrgan(ws, 'SafeVault');
      await wait(300);

      // Send OTM with 1-second TTL
      await postMessage(srv.baseUrl, {
        type: 'OTM',
        source_organ: 'Spine',
        target_organ: 'SafeVault',
        payload: { event_type: 'ttl_test' },
        ttl_seconds: 1,
      });

      let mailbox = await getJson(`${srv.baseUrl}/mailbox/SafeVault`);
      assert.ok(mailbox.depth >= 1, 'Message should be in mailbox initially');

      // Wait for TTL expiry (1s) + sweep cycles
      await wait(1500);

      mailbox = await getJson(`${srv.baseUrl}/mailbox/SafeVault`);
      assert.equal(mailbox.depth, 0, 'TTL-expired message should be swept');
    });

    it('44. APM has NULL TTL → persists indefinitely', async () => {
      // SafeVault is still disconnected from test 43
      await postMessage(srv.baseUrl, {
        type: 'APM',
        source_organ: 'Thalamus',
        target_organ: 'SafeVault',
        payload: {
          action: 'test', targets: ['t1'], risk_tier: 'low',
          evidence_refs: [], rollback_plan: 'none', reason: 'persistence test',
        },
      });

      // Wait longer than OTM TTL would have expired
      await wait(600);

      const mailbox = await getJson(`${srv.baseUrl}/mailbox/SafeVault`);
      assert.ok(mailbox.depth >= 1, 'APM should persist (NULL TTL — governance never expires)');
    });
  });

  describe('Back-pressure', () => {
    it('45. Fill mailbox beyond threshold → mailbox_pressure OTM broadcast emitted', async () => {
      const setup = createTestServer({
        pingIntervalMs: 50, healthHeartbeatMs: 5000,
        mailboxPressureThreshold: 3,
      });
      const srv = await startServer(setup);

      try {
        const listener = await connectWs(srv.wsUrl);
        await registerOrgan(listener, 'Vectr');
        await subscribeFilter(listener, { event_type: 'mailbox_pressure' });
        await wait(50);

        // Disconnect target
        const target = await connectWs(srv.wsUrl, { autoPong: false });
        await registerOrgan(target, 'Glia');
        await wait(300);

        const collecting = waitForMessage(listener, m =>
          m.action === 'message' && m.envelope?.payload?.event_type === 'mailbox_pressure',
          1000,
        );

        // Fill mailbox past threshold
        for (let i = 0; i < 4; i++) {
          await postMessage(srv.baseUrl, {
            type: 'OTM', source_organ: 'Spine', target_organ: 'Glia',
            payload: { event_type: 'fill', data: { i } },
          });
        }

        const msg = await collecting;
        assert.ok(msg, 'mailbox_pressure OTM should be emitted');
        assert.equal(msg.envelope.payload.data.organ_name, 'Glia');
        assert.ok(msg.envelope.payload.data.depth > 3);
        assert.equal(msg.envelope.payload.data.threshold, 3);

        listener.close();
      } finally {
        stopServer(setup, srv);
      }
    });
  });

  describe('Manifest-aware health', () => {
    let setup, srv;

    before(async () => {
      setup = createTestServer({ pingIntervalMs: 50, healthHeartbeatMs: 5000 });
      srv = await startServer(setup);
    });

    after(() => stopServer(setup, srv));

    it('46. GET /health includes manifest section with missing_required organs', async () => {
      const ws = await connectWs(srv.wsUrl);
      await registerOrgan(ws, 'Cortex');

      const health = await getJson(`${srv.baseUrl}/health`);

      assert.ok(health.manifest, '/health should include manifest section');
      assert.ok(typeof health.manifest.total_organs === 'number');
      assert.ok(typeof health.manifest.required === 'number');
      assert.ok(typeof health.manifest.connected === 'number');
      assert.ok(Array.isArray(health.manifest.missing_required));
      assert.ok(health.manifest.missing_required.length > 0, 'Most required organs should be missing');

      ws.close();
      await wait(50);
    });

    it('47. Required organ missing → Spine status = "degraded"', async () => {
      // No organs connected — all required organs are missing
      const health = await getJson(`${srv.baseUrl}/health`);
      assert.equal(health.status, 'degraded',
        'Status should be degraded when required organs are missing');
    });
  });
});
