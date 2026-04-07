/**
 * Integration test: Mailbox persistence + reconnection recovery (tests 16-23a).
 *
 * Exercises mailbox persistence across organ downtime, backlog auto-delivery
 * on reconnect, drain/ack lifecycle, and manifest validation.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import WebSocket from 'ws';
import {
  createTestServer, startServer, stopServer,
  connectWs, registerOrgan, subscribeFilter, ackWs,
  collectMessages, waitForMessage, postMessage, getJson, postJson, wait,
} from './helpers.js';

describe('Integration: Mailbox persistence + reconnection recovery', () => {

  describe('Disconnect → queue → reconnect cycle', () => {
    let setup, srv;

    before(async () => {
      setup = createTestServer();
      srv = await startServer(setup);
    });

    after(() => stopServer(setup, srv));

    it('16. Register Organ A, connect, then disconnect → mailbox status = disconnected', async () => {
      const ws = await connectWs(srv.wsUrl);
      await registerOrgan(ws, 'Vigil');
      ws.close();
      await wait(100);

      const mailbox = await getJson(`${srv.baseUrl}/mailbox/Vigil`);
      assert.equal(mailbox.status, 'disconnected');
    });

    it('17. Send 5 directed OTMs to disconnected Organ A → all 5 in mailbox (depth 5)', async () => {
      for (let i = 1; i <= 5; i++) {
        await postMessage(srv.baseUrl, {
          type: 'OTM',
          source_organ: 'Spine',
          target_organ: 'Vigil',
          payload: { event_type: 'queued_msg', data: { seq: i } },
        });
      }

      const mailbox = await getJson(`${srv.baseUrl}/mailbox/Vigil`);
      assert.equal(mailbox.depth, 5, 'All 5 messages should be queued');
    });

    it('18. Organ A reconnects → backlog auto-pushed → all 5 messages in FIFO order', async () => {
      const ws = await connectWs(srv.wsUrl);

      // Collect messages that arrive during/after registration (backlog push)
      const collecting = collectMessages(ws, 6, 3000); // 1 registered + 5 messages

      ws.send(JSON.stringify({ action: 'register', organ_name: 'Vigil' }));

      const all = await collecting;

      // First should be registration confirmation
      const registered = all.find(m => m.action === 'registered');
      assert.ok(registered, 'Should receive registration confirmation');

      // Then 5 backlog messages
      const messages = all.filter(m => m.action === 'message');
      assert.equal(messages.length, 5, 'All 5 backlog messages should be pushed');

      // Verify FIFO order
      for (let i = 0; i < messages.length; i++) {
        assert.equal(messages[i].envelope.payload.data.seq, i + 1,
          `Message ${i} should be seq ${i + 1} (FIFO order)`);
      }

      ws.close();
      await wait(50);
    });

    it('19. Registration response shows mailbox_depth and active_subscriptions', async () => {
      // Disconnect first
      const ws1 = await connectWs(srv.wsUrl);
      await registerOrgan(ws1, 'Glia');
      await subscribeFilter(ws1, { event_type: 'test_sub' });
      ws1.close();
      await wait(100);

      // Send 2 messages while disconnected
      await postMessage(srv.baseUrl, {
        type: 'OTM', source_organ: 'Spine', target_organ: 'Glia',
        payload: { event_type: 'queued' },
      });
      await postMessage(srv.baseUrl, {
        type: 'OTM', source_organ: 'Spine', target_organ: 'Glia',
        payload: { event_type: 'queued' },
      });

      // Reconnect — check registration response
      const ws2 = await connectWs(srv.wsUrl);
      const reg = await registerOrgan(ws2, 'Glia');

      assert.ok(typeof reg.mailbox_depth === 'number', 'Should have mailbox_depth');
      assert.ok(reg.mailbox_depth >= 2, 'mailbox_depth should reflect queued messages');
      assert.ok(Array.isArray(reg.active_subscriptions), 'Should have active_subscriptions');
      assert.ok(reg.active_subscriptions.length > 0, 'Should have persisted subscription');

      ws2.close();
      await wait(50);
    });

    it('20. Organ A acks messages → GET /mailbox/A → depth 0', async () => {
      // Setup: disconnect Engram, queue messages
      const ws1 = await connectWs(srv.wsUrl);
      await registerOrgan(ws1, 'Engram');
      ws1.close();
      await wait(100);

      const messageIds = [];
      for (let i = 0; i < 3; i++) {
        const res = await postMessage(srv.baseUrl, {
          type: 'OTM', source_organ: 'Spine', target_organ: 'Engram',
          payload: { event_type: 'ack_test', data: { i } },
        });
        messageIds.push(res.body.message_id);
      }

      // Reconnect — backlog push marks messages delivered
      const ws2 = await connectWs(srv.wsUrl);
      const collecting = collectMessages(ws2, 4, 2000); // registered + 3 messages
      ws2.send(JSON.stringify({ action: 'register', organ_name: 'Engram' }));
      await collecting;

      // After backlog push, messages are marked delivered → depth should be 0
      const mailbox = await getJson(`${srv.baseUrl}/mailbox/Engram`);
      assert.equal(mailbox.depth, 0, 'Depth should be 0 after backlog delivery');

      // Ack via WebSocket (idempotent — already delivered)
      const ackResult = await ackWs(ws2, messageIds);
      assert.equal(typeof ackResult.acknowledged, 'number');

      ws2.close();
      await wait(50);
    });

    it('21. Manual drain after backlog push → empty (already delivered)', async () => {
      // Setup: disconnect, queue, reconnect (backlog pushed)
      const ws1 = await connectWs(srv.wsUrl);
      await registerOrgan(ws1, 'Arbiter');
      ws1.close();
      await wait(100);

      await postMessage(srv.baseUrl, {
        type: 'OTM', source_organ: 'Spine', target_organ: 'Arbiter',
        payload: { event_type: 'drain_test' },
      });

      const ws2 = await connectWs(srv.wsUrl);
      await registerOrgan(ws2, 'Arbiter');
      await wait(200); // Let backlog push complete

      // Drain should return 0 messages (all delivered by backlog push)
      const drain = await postJson(`${srv.baseUrl}/mailbox/Arbiter/drain`, { limit: 10 });
      assert.equal(drain.body.count, 0, 'Drain should return 0 after backlog push');
      assert.deepEqual(drain.body.messages, []);

      ws2.close();
      await wait(50);
    });

    it('22. Send message while Organ A is connected → real-time push (immediate delivery)', async () => {
      const ws = await connectWs(srv.wsUrl);
      await registerOrgan(ws, 'Cortex');

      const collector = waitForMessage(ws, m =>
        m.action === 'message' && m.envelope?.payload?.event_type === 'realtime_test'
      );

      await postMessage(srv.baseUrl, {
        type: 'OTM',
        source_organ: 'Spine',
        target_organ: 'Cortex',
        payload: { event_type: 'realtime_test', data: { live: true } },
      });

      const msg = await collector;
      assert.ok(msg, 'Connected organ should receive message immediately via WebSocket');
      assert.equal(msg.envelope.payload.event_type, 'realtime_test');

      // Mailbox depth should be 0 (message delivered immediately, persisted as delivered=1)
      const mailbox = await getJson(`${srv.baseUrl}/mailbox/Cortex`);
      assert.equal(mailbox.depth, 0, 'Depth should be 0 — message was delivered in real-time');

      ws.close();
      await wait(50);
    });
  });

  describe('Manifest validation', () => {
    let setup, srv;

    before(async () => {
      setup = createTestServer();
      srv = await startServer(setup);
    });

    after(() => stopServer(setup, srv));

    it('23. Unknown organ connects → rejected → UNKNOWN_ORGAN error, WebSocket closed', async () => {
      const ws = await connectWs(srv.wsUrl);

      const response = await registerOrgan(ws, 'TotallyFakeOrgan');

      assert.equal(response.action, 'error');
      assert.equal(response.message, 'UNKNOWN_ORGAN');

      // WebSocket should be closed by server
      await wait(200);
      assert.notEqual(ws.readyState, WebSocket.OPEN, 'WebSocket should be closed');
    });

    it('23a. GET /manifest → shows connected and missing_required', async () => {
      // Connect one organ
      const ws = await connectWs(srv.wsUrl);
      await registerOrgan(ws, 'Vectr');

      const manifest = await getJson(`${srv.baseUrl}/manifest`);

      assert.ok(manifest.organs, 'Should have organs array');
      assert.equal(manifest.total, 29, 'Manifest should have 29 organs (28 required + human-principal)');
      assert.ok(Array.isArray(manifest.connected), 'Should have connected array');
      assert.ok(manifest.connected.includes('Vectr'), 'Vectr should be connected');
      assert.ok(Array.isArray(manifest.missing_required), 'Should have missing_required array');
      assert.ok(manifest.missing_required.length > 0, 'Some required organs should be missing');
      assert.ok(!manifest.missing_required.includes('Vectr'), 'Vectr should not be missing');

      ws.close();
      await wait(50);
    });
  });
});
