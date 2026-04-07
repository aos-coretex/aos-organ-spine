/**
 * Shared test infrastructure for Spine ESB integration tests.
 *
 * Provides a full-stack Spine server (Express + WebSocket + SQLite in-memory)
 * with configurable health monitoring intervals for fast testing.
 *
 * Each test file creates its own server instance for isolation.
 */

import express from 'express';
import WebSocket from 'ws';
import { initDatabase } from '../../server/db/init.js';
import { createManifest } from '../../server/manifest/manifest.js';
import { createMessagesRouter } from '../../server/routes/messages.js';
import { createMailboxRouter } from '../../server/routes/mailbox.js';
import { createHealthRouter } from '../../server/routes/health.js';
import { createStateRouter } from '../../server/routes/state.js';
import { createEventsRouter } from '../../server/routes/events.js';
import { createSchemasRouter } from '../../server/routes/schemas.js';
import { createWebSocketHandler } from '../../server/ws/handler.js';
import { createStateMachine } from '../../server/state/machine.js';
import { routeMessage } from '../../server/routing/router.js';
import { generateUrn } from '../../lib/urn.js';

/**
 * Create a fully wired Spine ESB server with in-memory SQLite.
 *
 * @param {object} overrides - Health monitoring config overrides
 * @returns {{ app, adapter, manifest, wsHandler, stateMachine, healthConfig }}
 */
export function createTestServer(overrides = {}) {
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
    healthHeartbeatMs: overrides.healthHeartbeatMs ?? 5000,
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

/**
 * Start the test server on a random available port.
 * @returns {{ server, baseUrl, wsUrl }}
 */
export async function startServer(setup) {
  const server = setup.app.listen(0);
  await new Promise(r => server.on('listening', r));
  const port = server.address().port;
  setup.wsHandler.attach(server);
  return {
    server,
    baseUrl: `http://127.0.0.1:${port}`,
    wsUrl: `ws://127.0.0.1:${port}/subscribe`,
  };
}

/**
 * Gracefully stop the test server.
 */
export function stopServer(setup, srv) {
  setup.wsHandler.cleanup();
  srv.server.close();
  setup.adapter.close();
}

/** Async wait helper. */
export function wait(ms) {
  return new Promise(r => setTimeout(r, ms));
}

/**
 * Open a WebSocket connection.
 * @param {string} url - WebSocket URL
 * @param {object} options - ws options (e.g., { autoPong: false })
 */
export function connectWs(url, options = {}) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url, options);
    ws.on('open', () => resolve(ws));
    ws.on('error', reject);
  });
}

/**
 * Register an organ on an open WebSocket connection.
 * Returns the registration response.
 */
export function registerOrgan(ws, organName) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      ws.removeListener('message', handler);
      reject(new Error(`registerOrgan timeout for ${organName}`));
    }, 5000);
    const handler = (data) => {
      const msg = JSON.parse(data.toString());
      if (msg.action === 'registered' || msg.action === 'error') {
        ws.removeListener('message', handler);
        clearTimeout(timer);
        resolve(msg);
      }
    };
    ws.on('message', handler);
    ws.send(JSON.stringify({ action: 'register', organ_name: organName }));
  });
}

/**
 * Subscribe to broadcast events via WebSocket.
 * Returns the subscription confirmation.
 */
export function subscribeFilter(ws, filter) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      ws.removeListener('message', handler);
      reject(new Error('subscribeFilter timeout'));
    }, 5000);
    const handler = (data) => {
      const msg = JSON.parse(data.toString());
      if (msg.action === 'subscribed') {
        ws.removeListener('message', handler);
        clearTimeout(timer);
        resolve(msg);
      }
    };
    ws.on('message', handler);
    ws.send(JSON.stringify({ action: 'subscribe', filter }));
  });
}

/**
 * Unsubscribe from broadcast events via WebSocket.
 */
export function unsubscribeFilter(ws, filter) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      ws.removeListener('message', handler);
      reject(new Error('unsubscribeFilter timeout'));
    }, 5000);
    const handler = (data) => {
      const msg = JSON.parse(data.toString());
      if (msg.action === 'unsubscribed') {
        ws.removeListener('message', handler);
        clearTimeout(timer);
        resolve(msg);
      }
    };
    ws.on('message', handler);
    ws.send(JSON.stringify({ action: 'unsubscribe', filter }));
  });
}

/**
 * Ack messages via WebSocket.
 */
export function ackWs(ws, messageIds) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      ws.removeListener('message', handler);
      reject(new Error('ackWs timeout'));
    }, 5000);
    const handler = (data) => {
      const msg = JSON.parse(data.toString());
      if (msg.action === 'acked') {
        ws.removeListener('message', handler);
        clearTimeout(timer);
        resolve(msg);
      }
    };
    ws.on('message', handler);
    ws.send(JSON.stringify({ action: 'ack', message_ids: messageIds }));
  });
}

/**
 * Collect N WebSocket messages (or timeout).
 * Returns array of parsed message objects.
 */
export function collectMessages(ws, count, timeoutMs = 2000) {
  return new Promise((resolve) => {
    const messages = [];
    const handler = (data) => {
      messages.push(JSON.parse(data.toString()));
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

/**
 * Wait for a single WebSocket message matching a predicate.
 * Returns the matching message or null on timeout.
 */
export function waitForMessage(ws, predicate, timeoutMs = 2000) {
  return new Promise((resolve) => {
    const handler = (data) => {
      const msg = JSON.parse(data.toString());
      if (predicate(msg)) {
        ws.removeListener('message', handler);
        clearTimeout(timer);
        resolve(msg);
      }
    };
    ws.on('message', handler);
    const timer = setTimeout(() => {
      ws.removeListener('message', handler);
      resolve(null);
    }, timeoutMs);
  });
}

/**
 * POST /messages — send a message through the ESB.
 * Returns { status, headers, body }.
 */
export async function postMessage(baseUrl, body) {
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

/**
 * GET JSON from a URL.
 */
export async function getJson(url) {
  const res = await fetch(url);
  return res.json();
}

/**
 * POST JSON and return { status, body }.
 */
export async function postJson(url, body) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  return { status: res.status, body: await res.json() };
}
