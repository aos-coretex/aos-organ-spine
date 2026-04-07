/**
 * Integration test: State machine (tests 46-57).
 *
 * Exercises the spine-state job lifecycle end-to-end via HTTP API,
 * including transition validation, terminal states, and broadcast emissions.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import {
  createTestServer, startServer, stopServer,
  connectWs, registerOrgan, subscribeFilter,
  waitForMessage, getJson, postJson, wait,
} from './helpers.js';

describe('Integration: State machine', () => {
  let setup, srv;

  before(async () => {
    setup = createTestServer();
    srv = await startServer(setup);
  });

  after(() => stopServer(setup, srv));

  it('46. Create job entity → state CREATED', async () => {
    const res = await postJson(`${srv.baseUrl}/state/entities`, {
      entity_urn: 'urn:llm-ops:job:test-001',
      entity_type: 'job',
      metadata: { description: 'integration test job' },
    });

    assert.equal(res.status, 201);
    assert.equal(res.body.entity_urn, 'urn:llm-ops:job:test-001');
    assert.equal(res.body.entity_type, 'job');
    assert.equal(res.body.current_state, 'CREATED');
    assert.deepEqual(res.body.metadata, { description: 'integration test job' });
  });

  it('47. Transition CREATED → PLANNING', async () => {
    const urn = encodeURIComponent('urn:llm-ops:job:test-001');
    const res = await postJson(`${srv.baseUrl}/state/${urn}/transition`, {
      from_state: 'CREATED',
      to_state: 'PLANNING',
      reason: 'job decomposition started',
      actor: 'Thalamus',
    });

    assert.equal(res.status, 200);
    assert.equal(res.body.previous_state, 'CREATED');
    assert.equal(res.body.current_state, 'PLANNING');
    assert.ok(res.body.transition_id);
    assert.equal(res.body.actor, 'Thalamus');
  });

  it('48. Transition PLANNING → AWAITING_AUTH (write lane)', async () => {
    const urn = encodeURIComponent('urn:llm-ops:job:test-001');
    const res = await postJson(`${srv.baseUrl}/state/${urn}/transition`, {
      from_state: 'PLANNING',
      to_state: 'AWAITING_AUTH',
      reason: 'requires authorization for write operation',
      actor: 'Thalamus',
    });

    assert.equal(res.status, 200);
    assert.equal(res.body.current_state, 'AWAITING_AUTH');
  });

  it('49. Transition AWAITING_AUTH → DISPATCHED (authorized)', async () => {
    const urn = encodeURIComponent('urn:llm-ops:job:test-001');
    const res = await postJson(`${srv.baseUrl}/state/${urn}/transition`, {
      from_state: 'AWAITING_AUTH',
      to_state: 'DISPATCHED',
      reason: 'authorized by Nomos',
      actor: 'Nomos',
    });

    assert.equal(res.status, 200);
    assert.equal(res.body.current_state, 'DISPATCHED');
  });

  it('50. Transition DISPATCHED → EXECUTING', async () => {
    const urn = encodeURIComponent('urn:llm-ops:job:test-001');
    const res = await postJson(`${srv.baseUrl}/state/${urn}/transition`, {
      from_state: 'DISPATCHED',
      to_state: 'EXECUTING',
      reason: 'execution started',
      actor: 'Cortex',
    });

    assert.equal(res.status, 200);
    assert.equal(res.body.current_state, 'EXECUTING');
  });

  it('51. Transition EXECUTING → SUCCEEDED (terminal)', async () => {
    const urn = encodeURIComponent('urn:llm-ops:job:test-001');
    const res = await postJson(`${srv.baseUrl}/state/${urn}/transition`, {
      from_state: 'EXECUTING',
      to_state: 'SUCCEEDED',
      reason: 'execution completed successfully',
      actor: 'Cortex',
    });

    assert.equal(res.status, 200);
    assert.equal(res.body.current_state, 'SUCCEEDED');
  });

  it('52. Attempt transition from SUCCEEDED → CREATED → 409 TERMINAL_STATE', async () => {
    const urn = encodeURIComponent('urn:llm-ops:job:test-001');
    const res = await postJson(`${srv.baseUrl}/state/${urn}/transition`, {
      from_state: 'SUCCEEDED',
      to_state: 'CREATED',
      reason: 'attempt to restart',
      actor: 'Thalamus',
    });

    assert.equal(res.status, 409);
    assert.equal(res.body.error, 'TERMINAL_STATE');
    assert.equal(res.body.current_state, 'SUCCEEDED');
    assert.deepEqual(res.body.allowed_transitions, []);
  });

  it('53. R0 flow: PLANNING → DISPATCHED (skip auth)', async () => {
    // Create a new job for the R0 flow
    await postJson(`${srv.baseUrl}/state/entities`, {
      entity_urn: 'urn:llm-ops:job:r0-001',
      entity_type: 'job',
    });

    const urn = encodeURIComponent('urn:llm-ops:job:r0-001');

    // CREATED → PLANNING
    await postJson(`${srv.baseUrl}/state/${urn}/transition`, {
      from_state: 'CREATED', to_state: 'PLANNING',
      reason: 'planning', actor: 'Thalamus',
    });

    // R0: PLANNING → DISPATCHED (skip AWAITING_AUTH)
    const res = await postJson(`${srv.baseUrl}/state/${urn}/transition`, {
      from_state: 'PLANNING',
      to_state: 'DISPATCHED',
      reason: 'R0 — no auth required for read-only operation',
      actor: 'Thalamus',
    });

    assert.equal(res.status, 200);
    assert.equal(res.body.current_state, 'DISPATCHED');
  });

  it('54. Invalid transition: CREATED → EXECUTING → 409 STATE_TRANSITION_INVALID', async () => {
    await postJson(`${srv.baseUrl}/state/entities`, {
      entity_urn: 'urn:llm-ops:job:invalid-001',
      entity_type: 'job',
    });

    const urn = encodeURIComponent('urn:llm-ops:job:invalid-001');
    const res = await postJson(`${srv.baseUrl}/state/${urn}/transition`, {
      from_state: 'CREATED',
      to_state: 'EXECUTING',
      reason: 'trying to skip states',
      actor: 'Thalamus',
    });

    assert.equal(res.status, 409);
    assert.equal(res.body.error, 'STATE_TRANSITION_INVALID');
    assert.ok(Array.isArray(res.body.allowed_transitions));
    assert.ok(res.body.allowed_transitions.includes('PLANNING'),
      'Allowed transitions from CREATED should include PLANNING');
    assert.ok(!res.body.allowed_transitions.includes('EXECUTING'),
      'EXECUTING should not be directly reachable from CREATED');
  });

  it('55. Stale state: from_state mismatch → 409 STALE_STATE', async () => {
    await postJson(`${srv.baseUrl}/state/entities`, {
      entity_urn: 'urn:llm-ops:job:stale-001',
      entity_type: 'job',
    });

    const urn = encodeURIComponent('urn:llm-ops:job:stale-001');

    // Move to PLANNING first
    await postJson(`${srv.baseUrl}/state/${urn}/transition`, {
      from_state: 'CREATED', to_state: 'PLANNING',
      reason: 'planning', actor: 'Thalamus',
    });

    // Try transition from CREATED (stale — it's already PLANNING)
    const res = await postJson(`${srv.baseUrl}/state/${urn}/transition`, {
      from_state: 'CREATED',
      to_state: 'PLANNING',
      reason: 'stale attempt',
      actor: 'Thalamus',
    });

    assert.equal(res.status, 409);
    assert.equal(res.body.error, 'STALE_STATE');
    assert.equal(res.body.current_state, 'PLANNING', 'Should report actual current state');
  });

  it('56. Full transition history via GET /state/:urn', async () => {
    const urn = encodeURIComponent('urn:llm-ops:job:test-001');
    const state = await getJson(`${srv.baseUrl}/state/${urn}`);

    assert.equal(state.entity_urn, 'urn:llm-ops:job:test-001');
    assert.equal(state.current_state, 'SUCCEEDED');
    assert.ok(Array.isArray(state.history));

    // Should have all 5 transitions: CREATED→PLANNING→AWAITING_AUTH→DISPATCHED→EXECUTING→SUCCEEDED
    assert.ok(state.history.length >= 5, 'Should have full transition history');

    const transitionPairs = state.history.map(t => `${t.from_state}->${t.to_state}`);
    assert.ok(transitionPairs.includes('CREATED->PLANNING'));
    assert.ok(transitionPairs.includes('PLANNING->AWAITING_AUTH'));
    assert.ok(transitionPairs.includes('AWAITING_AUTH->DISPATCHED'));
    assert.ok(transitionPairs.includes('DISPATCHED->EXECUTING'));
    assert.ok(transitionPairs.includes('EXECUTING->SUCCEEDED'));

    // Each transition should have required fields
    for (const t of state.history) {
      assert.ok(t.transition_id, 'Each transition should have transition_id');
      assert.ok(t.reason, 'Each transition should have reason');
      assert.ok(t.actor, 'Each transition should have actor');
      assert.ok(t.timestamp, 'Each transition should have timestamp');
    }
  });

  it('57. State transition OTM → verify subscriber received broadcast', async () => {
    // Connect a subscriber listening for state_transition events
    const ws = await connectWs(srv.wsUrl);
    await registerOrgan(ws, 'Vigil');
    await subscribeFilter(ws, { event_type: 'state_transition' });
    await wait(50);

    // Create a job and transition it
    await postJson(`${srv.baseUrl}/state/entities`, {
      entity_urn: 'urn:llm-ops:job:broadcast-001',
      entity_type: 'job',
    });

    const collecting = waitForMessage(ws, m =>
      m.action === 'message' &&
      m.envelope?.payload?.event_type === 'state_transition' &&
      m.envelope?.payload?.data?.entity_urn === 'urn:llm-ops:job:broadcast-001',
      2000,
    );

    const urn = encodeURIComponent('urn:llm-ops:job:broadcast-001');
    await postJson(`${srv.baseUrl}/state/${urn}/transition`, {
      from_state: 'CREATED', to_state: 'PLANNING',
      reason: 'broadcast test', actor: 'Thalamus',
    });

    const msg = await collecting;
    assert.ok(msg, 'Subscriber should receive state_transition OTM broadcast');
    assert.equal(msg.envelope.type, 'OTM');
    assert.equal(msg.envelope.source_organ, 'Spine');
    assert.equal(msg.envelope.target_organ, '*');
    assert.equal(msg.envelope.payload.data.entity_urn, 'urn:llm-ops:job:broadcast-001');
    assert.equal(msg.envelope.payload.data.previous_state, 'CREATED');
    assert.equal(msg.envelope.payload.data.current_state, 'PLANNING');

    ws.close();
    await wait(50);
  });
});
