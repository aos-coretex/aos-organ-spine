/**
 * ESB Spine client library for organ developers.
 *
 * Provides HTTP methods (send, drain, ack, register, health) and
 * WebSocket connection (connect) for real-time message delivery.
 *
 * Usage:
 *   import { createSpineClient } from './spine-client.js';
 *   const client = createSpineClient({ serverUrl: 'http://127.0.0.1:4000', organName: 'Vigil' });
 *   await client.register();
 *   await client.send({ type: 'OTM', source_organ: 'Vigil', target_organ: 'Glia', payload: {} });
 */

import WebSocket from 'ws';

export function createSpineClient(opts = {}) {
  const serverUrl = opts.serverUrl || 'http://127.0.0.1:4000';
  const organName = opts.organName;
  let ws = null;

  async function httpRequest(method, path, body) {
    const url = `${serverUrl}${path}`;
    const options = {
      method,
      headers: { 'Content-Type': 'application/json' },
    };
    if (body !== undefined) {
      options.body = JSON.stringify(body);
    }
    const res = await fetch(url, options);
    const data = await res.json();
    if (!res.ok) {
      const err = new Error(data.error || `HTTP ${res.status}`);
      err.status = res.status;
      err.body = data;
      throw err;
    }
    return data;
  }

  async function send(envelope) {
    return httpRequest('POST', '/messages', envelope);
  }

  async function drain(limit) {
    const body = limit ? { limit } : {};
    return httpRequest('POST', `/mailbox/${encodeURIComponent(organName)}/drain`, body);
  }

  async function ack(messageIds) {
    return httpRequest('POST', `/mailbox/${encodeURIComponent(organName)}/ack`, {
      message_ids: messageIds,
    });
  }

  async function register() {
    return httpRequest('POST', `/mailbox/${encodeURIComponent(organName)}`, {});
  }

  function connect(onMessage) {
    const wsUrl = serverUrl.replace(/^http/, 'ws') + '/subscribe';
    ws = new WebSocket(wsUrl);

    ws.on('open', () => {
      ws.send(JSON.stringify({ action: 'register', organ_name: organName }));
    });

    ws.on('message', (raw) => {
      try {
        const data = JSON.parse(raw.toString());
        if (data.action === 'message' && onMessage) {
          onMessage(data.envelope);
        }
      } catch { /* ignore parse errors */ }
    });

    ws.on('error', () => { /* swallow — close event follows */ });

    return function disconnect() {
      if (ws) {
        ws.close();
        ws = null;
      }
    };
  }

  async function health() {
    return httpRequest('GET', '/health');
  }

  function close() {
    if (ws) {
      ws.close();
      ws = null;
    }
  }

  return { send, drain, ack, register, connect, health, close };
}
