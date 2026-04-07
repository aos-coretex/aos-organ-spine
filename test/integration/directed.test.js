/**
 * Integration test: Directed message round-trip (tests 1-7).
 *
 * Exercises the complete directed messaging flow between two organs
 * using real HTTP + WebSocket connections to a full Spine server.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import {
  createTestServer, startServer, stopServer,
  connectWs, registerOrgan, subscribeFilter,
  collectMessages, waitForMessage, postMessage, getJson, wait,
} from './helpers.js';

describe('Integration: Directed message round-trip', () => {
  let setup, srv;

  before(async () => {
    setup = createTestServer();
    srv = await startServer(setup);
  });

  after(() => stopServer(setup, srv));

  it('1. Organ A sends OTM to Organ B via POST /messages → 202 Accepted, message_id assigned', async () => {
    // Connect Organ B to receive via WebSocket
    const wsB = await connectWs(srv.wsUrl);
    await registerOrgan(wsB, 'Glia');

    const res = await postMessage(srv.baseUrl, {
      type: 'OTM',
      source_organ: 'Vigil',
      target_organ: 'Glia',
      payload: { event_type: 'check_health', data: { test: true } },
    });

    assert.equal(res.status, 202);
    assert.equal(res.body.status, 'accepted');
    assert.equal(res.body.routing, 'directed');
    assert.equal(res.body.target_organ, 'Glia');
    assert.ok(res.body.message_id, 'message_id should be assigned');
    assert.ok(res.body.message_id.startsWith('urn:llm-ops:otm:'), 'message_id should use OTM URN namespace');
    assert.ok(res.body.timestamp, 'timestamp should be assigned');

    wsB.close();
    await wait(50);
  });

  it('2. Organ B receives message via WebSocket push → envelope matches sent message', async () => {
    const wsB = await connectWs(srv.wsUrl);
    await registerOrgan(wsB, 'Nomos');

    // Start collecting before sending
    const collecting = waitForMessage(wsB, m =>
      m.action === 'message' && m.envelope?.target_organ === 'Nomos'
    );

    const res = await postMessage(srv.baseUrl, {
      type: 'OTM',
      source_organ: 'Thalamus',
      target_organ: 'Nomos',
      payload: { event_type: 'action_request', data: { id: 42 } },
    });

    const msg = await collecting;
    assert.ok(msg, 'Organ B should receive the message via WebSocket');
    assert.equal(msg.envelope.type, 'OTM');
    assert.equal(msg.envelope.source_organ, 'Thalamus');
    assert.equal(msg.envelope.target_organ, 'Nomos');
    assert.equal(msg.envelope.message_id, res.body.message_id);
    assert.equal(msg.envelope.payload.event_type, 'action_request');
    assert.deepEqual(msg.envelope.payload.data, { id: 42 });

    wsB.close();
    await wait(50);
  });

  it('3. Organ B sends response OTM with correlation_id → response has correlation_id = original message_id', async () => {
    const wsA = await connectWs(srv.wsUrl);
    await registerOrgan(wsA, 'Cortex');

    const wsB = await connectWs(srv.wsUrl);
    await registerOrgan(wsB, 'Arbiter');

    // A sends to B
    const origRes = await postMessage(srv.baseUrl, {
      type: 'OTM',
      source_organ: 'Cortex',
      target_organ: 'Arbiter',
      payload: { event_type: 'query', data: { q: 'status' } },
    });
    await wait(50);

    // B sends response with correlation_id = original message_id
    const replyRes = await postMessage(srv.baseUrl, {
      type: 'OTM',
      source_organ: 'Arbiter',
      target_organ: 'Cortex',
      correlation_id: origRes.body.message_id,
      payload: { event_type: 'query_response', data: { status: 'ok' } },
    });

    assert.equal(replyRes.status, 202);
    assert.ok(replyRes.body.message_id.startsWith('urn:llm-ops:otm:'));

    wsA.close();
    wsB.close();
    await wait(50);
  });

  it('4. Organ A receives response via WebSocket → correlation_id matches, reply_to correct', async () => {
    const wsA = await connectWs(srv.wsUrl);
    await registerOrgan(wsA, 'Lobe');

    const wsB = await connectWs(srv.wsUrl);
    await registerOrgan(wsB, 'Syntra');

    // A sends to B
    const origRes = await postMessage(srv.baseUrl, {
      type: 'OTM',
      source_organ: 'Lobe',
      target_organ: 'Syntra',
      payload: { event_type: 'request', data: { x: 1 } },
    });
    await wait(50);

    // Start collecting A's messages before B responds
    const collecting = waitForMessage(wsA, m =>
      m.action === 'message' && m.envelope?.correlation_id === origRes.body.message_id
    );

    // B responds with correlation_id
    await postMessage(srv.baseUrl, {
      type: 'OTM',
      source_organ: 'Syntra',
      target_organ: 'Lobe',
      correlation_id: origRes.body.message_id,
      reply_to: 'Syntra',
      payload: { event_type: 'response', data: { result: 'done' } },
    });

    const msg = await collecting;
    assert.ok(msg, 'Organ A should receive the response');
    assert.equal(msg.envelope.correlation_id, origRes.body.message_id);
    assert.equal(msg.envelope.reply_to, 'Syntra');
    assert.equal(msg.envelope.source_organ, 'Syntra');

    wsA.close();
    wsB.close();
    await wait(50);
  });

  it('5. Full request-response round-trip → both messages in events audit table', async () => {
    const wsA = await connectWs(srv.wsUrl);
    await registerOrgan(wsA, 'Vectr');

    const wsB = await connectWs(srv.wsUrl);
    await registerOrgan(wsB, 'Graph');

    // Send request
    const req = await postMessage(srv.baseUrl, {
      type: 'OTM',
      source_organ: 'Vectr',
      target_organ: 'Graph',
      payload: { event_type: 'ping' },
    });
    await wait(50);

    // Send response
    const resp = await postMessage(srv.baseUrl, {
      type: 'OTM',
      source_organ: 'Graph',
      target_organ: 'Vectr',
      correlation_id: req.body.message_id,
      payload: { event_type: 'pong' },
    });

    // Both messages should be in the events audit table
    const events = await getJson(`${srv.baseUrl}/events?source_organ=Vectr`);
    const requestEvent = events.events.find(e => e.envelope.message_id === req.body.message_id);
    assert.ok(requestEvent, 'Request message should be in events table');
    assert.equal(requestEvent.routing, 'directed');

    const events2 = await getJson(`${srv.baseUrl}/events?source_organ=Graph`);
    const responseEvent = events2.events.find(e => e.envelope.message_id === resp.body.message_id);
    assert.ok(responseEvent, 'Response message should be in events table');
    assert.equal(responseEvent.routing, 'directed');

    wsA.close();
    wsB.close();
    await wait(50);
  });

  it('6. Send APM (Thalamus → Nomos pattern) → schema validated, directed to Nomos mailbox', async () => {
    const wsNomos = await connectWs(srv.wsUrl);
    await registerOrgan(wsNomos, 'Senate');

    const collecting = waitForMessage(wsNomos, m =>
      m.action === 'message' && m.envelope?.type === 'APM'
    );

    const res = await postMessage(srv.baseUrl, {
      type: 'APM',
      source_organ: 'Thalamus',
      target_organ: 'Senate',
      payload: {
        action: 'deploy_service',
        targets: ['urn:llm-ops:service:spine'],
        risk_tier: 'medium',
        evidence_refs: ['urn:llm-ops:analysis:001'],
        rollback_plan: 'revert to previous version',
        reason: 'performance improvement',
      },
    });

    assert.equal(res.status, 202);
    assert.ok(res.body.message_id.startsWith('urn:llm-ops:apm:'), 'APM should use apm URN namespace');
    assert.equal(res.body.routing, 'directed');

    const msg = await collecting;
    assert.ok(msg, 'Nomos should receive the APM via WebSocket');
    assert.equal(msg.envelope.type, 'APM');
    assert.equal(msg.envelope.payload.risk_tier, 'medium');

    wsNomos.close();
    await wait(50);
  });

  it('7. Send ATM response (Nomos → Thalamus pattern) → correlation_id links APM and ATM', async () => {
    const wsThalamus = await connectWs(srv.wsUrl);
    await registerOrgan(wsThalamus, 'Thalamus');

    // First send APM from Thalamus to Nomos
    const apmRes = await postMessage(srv.baseUrl, {
      type: 'APM',
      source_organ: 'Thalamus',
      target_organ: 'Cerberus',
      payload: {
        action: 'modify_config',
        targets: ['urn:llm-ops:config:main'],
        risk_tier: 'low',
        evidence_refs: ['urn:llm-ops:review:002'],
        rollback_plan: 'restore backup config',
        reason: 'add new parameter',
      },
    });
    await wait(50);

    // Start collecting before ATM is sent
    const collecting = waitForMessage(wsThalamus, m =>
      m.action === 'message' && m.envelope?.type === 'ATM'
    );

    // Nomos responds with ATM, correlation_id = APM's message_id
    const atmRes = await postMessage(srv.baseUrl, {
      type: 'ATM',
      source_organ: 'Cerberus',
      target_organ: 'Thalamus',
      correlation_id: apmRes.body.message_id,
      payload: {
        token_urn: 'urn:llm-ops:token:auth-001',
        scope: {
          targets: ['urn:llm-ops:config:main'],
          action_types: ['modify_config'],
          ttl_seconds: 3600,
          conditions: [],
        },
        ap_ref: apmRes.body.message_id,
      },
    });

    assert.equal(atmRes.status, 202);
    assert.ok(atmRes.body.message_id.startsWith('urn:llm-ops:atm:'), 'ATM should use atm URN namespace');

    const msg = await collecting;
    assert.ok(msg, 'Thalamus should receive the ATM');
    assert.equal(msg.envelope.correlation_id, apmRes.body.message_id, 'correlation_id should link to APM');
    assert.equal(msg.envelope.payload.ap_ref, apmRes.body.message_id, 'ap_ref should reference APM');

    wsThalamus.close();
    await wait(50);
  });
});
