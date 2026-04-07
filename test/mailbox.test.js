/**
 * Relay 2 — Mailbox management and WebSocket tests.
 *
 * Uses Node.js built-in test runner, real HTTP server on random port,
 * in-memory SQLite for isolation.
 *
 * Updated for Relay 3: all messages must include valid typed payloads.
 * Updated for Relay 4: manifest integration, auto-created mailboxes.
 *
 * Run: node --test test/mailbox.test.js
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import WebSocket from 'ws';
import { initDatabase } from '../server/db/init.js';
import { createManifest } from '../server/manifest/manifest.js';
import { createMessagesRouter } from '../server/routes/messages.js';
import { createMailboxRouter } from '../server/routes/mailbox.js';
import { createWebSocketHandler } from '../server/ws/handler.js';

/**
 * Helper: create a full test server with messages, mailbox, and WebSocket
 */
function createTestServer() {
  const db = initDatabase(':memory:');
  const manifest = createManifest(db);
  const wsHandler = createWebSocketHandler(db, manifest);

  const app = express();
  app.use(express.json());
  app.use('/', createMessagesRouter(db, wsHandler, manifest));
  app.use('/mailbox', createMailboxRouter(db));

  return { db, app, wsHandler, manifest };
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

// --- Mailbox HTTP tests ---

describe('Mailbox endpoints', () => {
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

  it('7. POST /mailbox/Vigil — Vigil already exists from manifest seed (200)', async () => {
    // Relay 4: mailboxes for all 28 manifest organs are auto-created during DB init
    const { status, body } = await postJson(baseUrl, '/mailbox/Vigil', {});
    assert.equal(status, 200);
    assert.equal(body.mailbox, 'Vigil');
    assert.equal(body.already_registered, true);
  });

  it('8. POST /mailbox/Vigil again is idempotent (200)', async () => {
    const { status, body } = await postJson(baseUrl, '/mailbox/Vigil', {});
    assert.equal(status, 200);
    assert.equal(body.mailbox, 'Vigil');
    assert.equal(body.already_registered, true);
  });

  it('9. GET /mailbox/Vigil shows depth 0', async () => {
    const { status, body } = await getJson(baseUrl, '/mailbox/Vigil');
    assert.equal(status, 200);
    assert.equal(body.depth, 0);
    assert.equal(body.oldest_message_at, null);
    assert.equal(body.status, 'registered');
  });

  it('10. directed message increases mailbox depth to 1', async () => {
    // Send a message to Vigil (no WS connected, so it stays pending)
    await postJson(baseUrl, '/messages', {
      type: 'OTM',
      source_organ: 'Lobe',
      target_organ: 'Vigil',
      payload: { event_type: 'test_depth', data: { value: 'test' } },
    });

    const { body } = await getJson(baseUrl, '/mailbox/Vigil');
    assert.equal(body.depth, 1);
  });

  it('11. drain returns message but depth remains 1 (not acked)', async () => {
    const { status, body: drainBody } = await postJson(baseUrl, '/mailbox/Vigil/drain', {});
    assert.equal(status, 200);
    assert.equal(drainBody.count, 1);
    assert.equal(drainBody.messages.length, 1);
    assert.equal(drainBody.messages[0].source_organ, 'Lobe');
    assert.equal(drainBody.messages[0].target_organ, 'Vigil');

    // Depth still 1 because not acked
    const { body: statusBody } = await getJson(baseUrl, '/mailbox/Vigil');
    assert.equal(statusBody.depth, 1);
  });

  it('12. ack reduces depth to 0', async () => {
    // Get the message_id from drain
    const { body: drainBody } = await postJson(baseUrl, '/mailbox/Vigil/drain', {});
    const messageId = drainBody.messages[0].message_id;

    const { status, body: ackBody } = await postJson(baseUrl, '/mailbox/Vigil/ack', {
      message_ids: [messageId],
    });
    assert.equal(status, 200);
    assert.equal(ackBody.acknowledged, 1);

    // Depth is now 0
    const { body: statusBody } = await getJson(baseUrl, '/mailbox/Vigil');
    assert.equal(statusBody.depth, 0);
  });

  it('13. FIFO ordering: send 3 messages, drain returns oldest first', async () => {
    // Send 3 messages with distinguishable payloads
    for (let i = 1; i <= 3; i++) {
      await postJson(baseUrl, '/messages', {
        type: 'OTM',
        source_organ: 'Lobe',
        target_organ: 'Vigil',
        payload: { event_type: 'fifo_test', data: { seq: i } },
      });
    }

    const { body } = await postJson(baseUrl, '/mailbox/Vigil/drain', { limit: 10 });
    assert.equal(body.count, 3);
    assert.equal(body.messages[0].payload.data.seq, 1);
    assert.equal(body.messages[1].payload.data.seq, 2);
    assert.equal(body.messages[2].payload.data.seq, 3);
  });

  it('14. drain with limit respects the limit', async () => {
    // We have 3 unacked messages from test 13
    const { body } = await postJson(baseUrl, '/mailbox/Vigil/drain', { limit: 2 });
    assert.equal(body.count, 2);
    assert.equal(body.messages.length, 2);
  });
});

// --- WebSocket tests ---

describe('WebSocket messaging', () => {
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

  it('15. WebSocket: register organ, send message, pushed in real-time', async () => {
    // Axon mailbox exists from manifest seed (Relay 4)

    // Connect via WebSocket
    const ws = connectWs(baseUrl);
    await new Promise((resolve) => ws.on('open', resolve));

    // Register organ on WebSocket
    ws.send(JSON.stringify({ action: 'register', organ_name: 'Axon' }));
    const regMsg = await waitForMessage(ws, (d) => d.action === 'registered');
    assert.equal(regMsg.organ_name, 'Axon');

    // Send a directed message to Axon via HTTP
    const messagePromise = waitForMessage(ws, (d) => d.action === 'message');
    await postJson(baseUrl, '/messages', {
      type: 'OTM',
      source_organ: 'Thalamus',
      target_organ: 'Axon',
      payload: { event_type: 'ws_test', data: { instruction: 'execute' } },
    });

    // Should receive the message via WebSocket push
    const pushed = await messagePromise;
    assert.equal(pushed.action, 'message');
    assert.equal(pushed.envelope.source_organ, 'Thalamus');
    assert.equal(pushed.envelope.target_organ, 'Axon');
    assert.equal(pushed.envelope.payload.data.instruction, 'execute');
    assert.ok(pushed.envelope.message_id, 'pushed envelope should have message_id');

    ws.close();
    await new Promise((resolve) => ws.on('close', resolve));
  });

  it('16. WebSocket: organ disconnects, message persists, organ drains on reconnect', async () => {
    // Receptor mailbox exists from manifest seed (Relay 4)

    // Connect and register via WebSocket
    const ws1 = connectWs(baseUrl);
    await new Promise((resolve) => ws1.on('open', resolve));
    ws1.send(JSON.stringify({ action: 'register', organ_name: 'Receptor' }));
    await waitForMessage(ws1, (d) => d.action === 'registered');

    // Disconnect
    ws1.close();
    await new Promise((resolve) => ws1.on('close', resolve));

    // Small delay for disconnect handler to run
    await new Promise((resolve) => setTimeout(resolve, 50));

    // Send message while disconnected
    const { body: sendBody } = await postJson(baseUrl, '/messages', {
      type: 'OTM',
      source_organ: 'Thalamus',
      target_organ: 'Receptor',
      payload: { event_type: 'disconnect_test', data: { queued: true } },
    });

    // Verify message is persisted (not delivered via WS)
    const { body: statusBody } = await getJson(baseUrl, '/mailbox/Receptor');
    assert.equal(statusBody.depth, 1, 'message should be pending in mailbox');

    // Reconnect and drain
    const { body: drainBody } = await postJson(baseUrl, '/mailbox/Receptor/drain', {});
    assert.equal(drainBody.count, 1);
    assert.equal(drainBody.messages[0].payload.data.queued, true);
    assert.equal(drainBody.messages[0].message_id, sendBody.message_id);
  });
});
