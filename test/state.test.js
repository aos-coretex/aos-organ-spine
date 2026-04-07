/**
 * spine-state subsystem tests.
 *
 * Uses Node.js built-in test runner and in-memory SQLite for isolation.
 * Run: node --test test/state.test.js
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { initDatabase } from '../server/db/init.js';
import { createStateMachine } from '../server/state/machine.js';
import express from 'express';
import { createStateRouter } from '../server/routes/state.js';
import { createHealthRouter } from '../server/routes/health.js';

// -- Unit tests: StateMachine module directly --

describe('StateMachine engine', () => {
  let db;
  let sm;

  before(() => {
    db = initDatabase(':memory:');
    sm = createStateMachine(db);  // db is the adapter now
  });

  after(() => {
    db.close();
  });

  it('1. should have registered job and organ state machines on init', () => {
    const jobDef = sm.getMachineDefinition('job');
    assert.ok(jobDef, 'job machine should exist');
    assert.equal(jobDef.entity_type, 'job');
    assert.equal(jobDef.states.length, 8);
    assert.equal(jobDef.initial_state, 'CREATED');
    assert.deepEqual(jobDef.terminal_states, ['SUCCEEDED', 'DENIED', 'FAILED']);

    const organDef = sm.getMachineDefinition('organ');
    assert.ok(organDef, 'organ machine should exist');
    assert.equal(organDef.entity_type, 'organ');
    assert.equal(organDef.states.length, 4);
    assert.equal(organDef.initial_state, 'REGISTERED');
    assert.deepEqual(organDef.terminal_states, []);
  });

  it('2. should create an entity at initial state', () => {
    const result = sm.createEntity('urn:test:job:001', 'job', { name: 'test-job' });
    assert.equal(result.entity_urn, 'urn:test:job:001');
    assert.equal(result.entity_type, 'job');
    assert.equal(result.current_state, 'CREATED');
    assert.deepEqual(result.metadata, { name: 'test-job' });
    assert.ok(result.created_at);
  });

  it('3. should reject duplicate entity creation', () => {
    const result = sm.createEntity('urn:test:job:001', 'job');
    assert.equal(result.error, 'ENTITY_EXISTS');
    assert.equal(result.entity_urn, 'urn:test:job:001');
  });

  it('4. should execute a valid transition (CREATED -> PLANNING)', () => {
    const result = sm.transition(
      'urn:test:job:001', 'CREATED', 'PLANNING', 'starting plan', 'thalamus'
    );
    assert.equal(result.previous_state, 'CREATED');
    assert.equal(result.current_state, 'PLANNING');
    assert.ok(result.transition_id.startsWith('urn:llm-ops:transition:'));
    assert.ok(result.timestamp);
  });

  it('5. should reject an invalid transition (CREATED -> EXECUTING)', () => {
    sm.createEntity('urn:test:job:002', 'job');
    const result = sm.transition(
      'urn:test:job:002', 'CREATED', 'EXECUTING', 'skip ahead', 'rogue'
    );
    assert.equal(result.error, 'STATE_TRANSITION_INVALID');
    assert.equal(result.current_state, 'CREATED');
    assert.deepEqual(result.allowed_transitions, ['PLANNING']);
  });

  it('6. should reject stale state (from_state mismatch)', () => {
    // urn:test:job:001 is now in PLANNING, not CREATED
    const result = sm.transition(
      'urn:test:job:001', 'CREATED', 'PLANNING', 'stale attempt', 'thalamus'
    );
    assert.equal(result.error, 'STALE_STATE');
    assert.equal(result.current_state, 'PLANNING');
  });

  it('7. should reject transitions from terminal state', () => {
    // Walk job:003 to SUCCEEDED
    sm.createEntity('urn:test:job:003', 'job');
    sm.transition('urn:test:job:003', 'CREATED', 'PLANNING', 'plan', 'thalamus');
    sm.transition('urn:test:job:003', 'PLANNING', 'DISPATCHED', 'dispatch', 'thalamus');
    sm.transition('urn:test:job:003', 'DISPATCHED', 'EXECUTING', 'exec', 'cerberus');
    sm.transition('urn:test:job:003', 'EXECUTING', 'SUCCEEDED', 'done', 'cerberus');

    const result = sm.transition(
      'urn:test:job:003', 'SUCCEEDED', 'CREATED', 'retry', 'rogue'
    );
    assert.equal(result.error, 'TERMINAL_STATE');
    assert.equal(result.current_state, 'SUCCEEDED');
    assert.deepEqual(result.allowed_transitions, []);
  });

  it('8. should return entity with full transition history', () => {
    const entity = sm.getEntity('urn:test:job:003');
    assert.equal(entity.entity_urn, 'urn:test:job:003');
    assert.equal(entity.current_state, 'SUCCEEDED');
    assert.equal(entity.history.length, 4);
    assert.equal(entity.history[0].from_state, 'CREATED');
    assert.equal(entity.history[0].to_state, 'PLANNING');
    assert.equal(entity.history[3].from_state, 'EXECUTING');
    assert.equal(entity.history[3].to_state, 'SUCCEEDED');
  });

  it('9. should walk the full job lifecycle (R0 lane)', () => {
    sm.createEntity('urn:test:job:lifecycle', 'job');

    const steps = [
      ['CREATED', 'PLANNING', 'plan phase'],
      ['PLANNING', 'DISPATCHED', 'R0 lane — no auth needed'],
      ['DISPATCHED', 'EXECUTING', 'dispatched to executor'],
      ['EXECUTING', 'SUCCEEDED', 'execution complete'],
    ];

    for (const [from, to, reason] of steps) {
      const result = sm.transition(
        'urn:test:job:lifecycle', from, to, reason, 'thalamus'
      );
      assert.ok(!result.error, `transition ${from}->${to} should succeed, got: ${result.error}`);
      assert.equal(result.current_state, to);
    }

    const final = sm.getEntity('urn:test:job:lifecycle');
    assert.equal(final.current_state, 'SUCCEEDED');
    assert.equal(final.history.length, 4);
  });
});

// -- HTTP integration tests --

describe('HTTP endpoints', () => {
  let db;
  let app;
  let baseUrl;
  let server;

  before(async () => {
    db = initDatabase(':memory:');
    const sm = createStateMachine(db);

    app = express();
    app.use(express.json());
    app.use('/state', createStateRouter(sm));
    app.use('/', createHealthRouter(db, ':memory:', 0));

    await new Promise((resolve) => {
      server = app.listen(0, '127.0.0.1', () => {
        const addr = server.address();
        baseUrl = `http://127.0.0.1:${addr.port}`;
        resolve();
      });
    });
  });

  after(async () => {
    await new Promise((resolve) => server.close(resolve));
    db.close();
  });

  it('10. health endpoint returns ok with correct structure', async () => {
    const res = await fetch(`${baseUrl}/health`);
    assert.equal(res.status, 200);
    const body = await res.json();
    assert.equal(body.status, 'ok');
    assert.equal(typeof body.uptime_s, 'number');
    assert.equal(body.sqlite_connected, true);
  });

  it('should list state machine definitions via GET /state/machines', async () => {
    const res = await fetch(`${baseUrl}/state/machines`);
    assert.equal(res.status, 200);
    const body = await res.json();
    assert.equal(body.machines.length, 2);
    const types = body.machines.map(m => m.entity_type).sort();
    assert.deepEqual(types, ['job', 'organ']);
  });

  it('should create entity via POST /state/entities', async () => {
    const res = await fetch(`${baseUrl}/state/entities`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ entity_urn: 'urn:test:http:job:1', entity_type: 'job' }),
    });
    assert.equal(res.status, 201);
    const body = await res.json();
    assert.equal(body.current_state, 'CREATED');
  });

  it('should get entity via GET /state/:entity_urn', async () => {
    const encoded = encodeURIComponent('urn:test:http:job:1');
    const res = await fetch(`${baseUrl}/state/${encoded}`);
    assert.equal(res.status, 200);
    const body = await res.json();
    assert.equal(body.entity_urn, 'urn:test:http:job:1');
    assert.equal(body.current_state, 'CREATED');
  });

  it('should transition via POST /state/:entity_urn/transition', async () => {
    const encoded = encodeURIComponent('urn:test:http:job:1');
    const res = await fetch(`${baseUrl}/state/${encoded}/transition`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        from_state: 'CREATED',
        to_state: 'PLANNING',
        reason: 'http test',
        actor: 'test-agent',
      }),
    });
    assert.equal(res.status, 200);
    const body = await res.json();
    assert.equal(body.previous_state, 'CREATED');
    assert.equal(body.current_state, 'PLANNING');
  });

  it('should return 409 for invalid transition via HTTP', async () => {
    const encoded = encodeURIComponent('urn:test:http:job:1');
    const res = await fetch(`${baseUrl}/state/${encoded}/transition`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        from_state: 'PLANNING',
        to_state: 'SUCCEEDED',
        reason: 'skip',
        actor: 'rogue',
      }),
    });
    assert.equal(res.status, 409);
    const body = await res.json();
    assert.equal(body.error, 'STATE_TRANSITION_INVALID');
  });

  it('should return 404 for non-existent entity', async () => {
    const encoded = encodeURIComponent('urn:test:nonexistent');
    const res = await fetch(`${baseUrl}/state/${encoded}`);
    assert.equal(res.status, 404);
  });

  it('should return stats with correct structure', async () => {
    const res = await fetch(`${baseUrl}/stats`);
    assert.equal(res.status, 200);
    const body = await res.json();
    assert.equal(typeof body.state_entities.total, 'number');
    assert.ok(body.state_entities.by_type);
    assert.ok(body.state_entities.by_state);
    assert.equal(typeof body.state_transitions.total, 'number');
  });

  it('should return introspect with correct structure', async () => {
    const res = await fetch(`${baseUrl}/introspect`);
    assert.equal(res.status, 200);
    const body = await res.json();
    assert.equal(body.db_path, ':memory:');
    assert.ok(Array.isArray(body.tables));
    assert.ok(body.tables.includes('state_machine_defs'));
    assert.ok(body.tables.includes('state_entities'));
    assert.ok(body.tables.includes('state_transitions'));
  });
});
