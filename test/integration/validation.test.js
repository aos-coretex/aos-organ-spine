/**
 * Integration test: Schema validation (tests 24-35).
 *
 * Exercises the validation middleware: valid messages accepted,
 * invalid messages rejected with field-level violations.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import {
  createTestServer, startServer, stopServer,
  postMessage, wait,
} from './helpers.js';

describe('Integration: Schema validation', () => {
  let setup, srv;

  before(async () => {
    setup = createTestServer();
    srv = await startServer(setup);
  });

  after(() => stopServer(setup, srv));

  // --- Valid messages (tests 24-28) ---

  it('24. Valid OTM → accepted (202)', async () => {
    const res = await postMessage(srv.baseUrl, {
      type: 'OTM',
      source_organ: 'Vigil',
      target_organ: 'Glia',
      payload: { event_type: 'health_check', data: { ok: true } },
    });
    assert.equal(res.status, 202);
    assert.ok(res.body.message_id.startsWith('urn:llm-ops:otm:'));

    // Verify persisted in events table
    const events = await (await fetch(`${srv.baseUrl}/events?type=OTM&limit=1`)).json();
    assert.ok(events.events.length > 0, 'OTM should be in events table');
  });

  it('25. Valid APM → accepted (202), APM-specific URN namespace', async () => {
    const res = await postMessage(srv.baseUrl, {
      type: 'APM',
      source_organ: 'Thalamus',
      target_organ: 'Nomos',
      payload: {
        action: 'deploy_service',
        targets: ['urn:llm-ops:service:test'],
        risk_tier: 'low',
        evidence_refs: ['urn:llm-ops:evidence:001'],
        rollback_plan: 'revert to v1',
        reason: 'performance fix',
      },
    });
    assert.equal(res.status, 202);
    assert.ok(res.body.message_id.startsWith('urn:llm-ops:apm:'), 'Should use APM URN namespace');
  });

  it('26. Valid PEM → accepted (202)', async () => {
    const res = await postMessage(srv.baseUrl, {
      type: 'PEM',
      source_organ: 'Nomos',
      target_organ: 'Senate',
      payload: {
        conflict_class: 'MSP_CONFLICT',
        blocked_action: 'urn:llm-ops:apm:blocked-001',
        blocking_rules: ['rule:no-deploy-during-freeze'],
        necessity: 'Critical fix required during freeze',
        proposed_change: 'Temporary exception for this deployment',
        risk_assessment: 'Low — targeted single service',
      },
    });
    assert.equal(res.status, 202);
    assert.ok(res.body.message_id.startsWith('urn:llm-ops:pem:'));
  });

  it('27. Valid ATM → accepted (202)', async () => {
    const res = await postMessage(srv.baseUrl, {
      type: 'ATM',
      source_organ: 'Nomos',
      target_organ: 'Thalamus',
      payload: {
        token_urn: 'urn:llm-ops:token:auth-test-001',
        scope: {
          targets: ['urn:llm-ops:service:test'],
          action_types: ['deploy_service'],
          ttl_seconds: 3600,
          conditions: [],
        },
        ap_ref: 'urn:llm-ops:apm:ref-001',
      },
    });
    assert.equal(res.status, 202);
    assert.ok(res.body.message_id.startsWith('urn:llm-ops:atm:'));
  });

  it('28. Valid HOM → accepted (202)', async () => {
    const res = await postMessage(srv.baseUrl, {
      type: 'HOM',
      source_organ: 'Arbiter',
      target_organ: 'human-principal',
      payload: {
        decision_type: 'bor_ambiguity',
        context: 'Conflicting rules detected during policy evaluation',
        question: 'Which rule takes precedence?',
        options: ['rule_a', 'rule_b', 'escalate'],
        deadline: null,
      },
    });
    assert.equal(res.status, 202);
    assert.ok(res.body.message_id.startsWith('urn:llm-ops:hom:'));
  });

  // --- Invalid messages (tests 29-35) ---

  it('29. Missing type field → rejected 400 SCHEMA_VALIDATION_FAILED', async () => {
    const res = await postMessage(srv.baseUrl, {
      source_organ: 'Vigil',
      target_organ: 'Glia',
      payload: { event_type: 'test' },
    });
    assert.equal(res.status, 400);
    assert.equal(res.body.error, 'SCHEMA_VALIDATION_FAILED');
    const violation = res.body.violations.find(v => v.field === 'type');
    assert.ok(violation, 'Should have type violation');
    assert.equal(violation.rule, 'required');
  });

  it('30. Invalid type value → rejected 400', async () => {
    const res = await postMessage(srv.baseUrl, {
      type: 'INVALID_TYPE',
      source_organ: 'Vigil',
      target_organ: 'Glia',
      payload: { event_type: 'test' },
    });
    assert.equal(res.status, 400);
    assert.equal(res.body.error, 'SCHEMA_VALIDATION_FAILED');
    const violation = res.body.violations.find(v => v.field === 'type');
    assert.ok(violation, 'Should have type violation');
    assert.equal(violation.rule, 'enum');
  });

  it('31. APM with invalid risk_tier enum → rejected 400 with field-level violation', async () => {
    const res = await postMessage(srv.baseUrl, {
      type: 'APM',
      source_organ: 'Thalamus',
      target_organ: 'Nomos',
      payload: {
        action: 'test',
        targets: ['t1'],
        risk_tier: 'extreme',  // Invalid enum value
        evidence_refs: ['e1'],
        rollback_plan: 'revert',
        reason: 'test',
      },
    });
    assert.equal(res.status, 400);
    assert.equal(res.body.error, 'SCHEMA_VALIDATION_FAILED');
    const violation = res.body.violations.find(v => v.field === 'risk_tier');
    assert.ok(violation, 'Should have risk_tier violation');
    assert.equal(violation.rule, 'enum');
    assert.ok(violation.expected.includes('low'), 'Expected should include valid values');
  });

  it('32. ATM with missing scope.targets → rejected 400 with nested field violation', async () => {
    const res = await postMessage(srv.baseUrl, {
      type: 'ATM',
      source_organ: 'Nomos',
      target_organ: 'Thalamus',
      payload: {
        token_urn: 'urn:test:token',
        scope: {
          // targets is missing
          action_types: ['deploy'],
          ttl_seconds: 3600,
        },
        ap_ref: 'urn:test:ap',
      },
    });
    assert.equal(res.status, 400);
    assert.equal(res.body.error, 'SCHEMA_VALIDATION_FAILED');
    const violation = res.body.violations.find(v => v.field === 'scope.targets');
    assert.ok(violation, 'Should have scope.targets nested violation');
    assert.equal(violation.rule, 'required');
  });

  it('33. OTM with extra unknown fields → accepted (forward-compatible)', async () => {
    const res = await postMessage(srv.baseUrl, {
      type: 'OTM',
      source_organ: 'Vigil',
      target_organ: 'Glia',
      payload: {
        event_type: 'test',
        unknown_field_1: 'value1',
        unknown_field_2: { nested: true },
        future_extension: [1, 2, 3],
      },
    });
    assert.equal(res.status, 202, 'Unknown fields should be accepted (forward-compatible)');
  });

  it('34. PEM with invalid conflict_class → rejected 400', async () => {
    const res = await postMessage(srv.baseUrl, {
      type: 'PEM',
      source_organ: 'Nomos',
      target_organ: 'Senate',
      payload: {
        conflict_class: 'INVALID_CONFLICT',  // Not MSP_CONFLICT or BOR_CONFLICT
        blocked_action: 'act1',
        blocking_rules: ['r1'],
        necessity: 'needed',
        proposed_change: 'change',
        risk_assessment: 'low',
      },
    });
    assert.equal(res.status, 400);
    assert.equal(res.body.error, 'SCHEMA_VALIDATION_FAILED');
    const violation = res.body.violations.find(v => v.field === 'conflict_class');
    assert.ok(violation, 'Should have conflict_class violation');
    assert.equal(violation.rule, 'enum');
  });

  it('35. HOM with invalid decision_type → rejected 400', async () => {
    const res = await postMessage(srv.baseUrl, {
      type: 'HOM',
      source_organ: 'Arbiter',
      target_organ: 'human-principal',
      payload: {
        decision_type: 'invalid_type',  // Not bor_ambiguity, amendment_proposal, or scope_clarification
        context: 'test',
        question: 'test?',
        options: ['a'],
      },
    });
    assert.equal(res.status, 400);
    assert.equal(res.body.error, 'SCHEMA_VALIDATION_FAILED');
    const violation = res.body.violations.find(v => v.field === 'decision_type');
    assert.ok(violation, 'Should have decision_type violation');
    assert.equal(violation.rule, 'enum');
  });
});
