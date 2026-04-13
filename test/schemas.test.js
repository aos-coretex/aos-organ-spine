/**
 * Relay 3 — Message type schema validation and query endpoint tests.
 *
 * 27 test cases covering:
 *   - Envelope validation (tests 1-6)
 *   - OTM payload validation (tests 7-9)
 *   - APM payload validation (tests 10-13)
 *   - PEM payload validation (tests 14-15)
 *   - ATM payload validation (tests 16-19)
 *   - HOM payload validation (tests 20-21)
 *   - Schema query endpoints (tests 22-25)
 *   - URN namespace verification (tests 26-27)
 *
 * Run: node --test test/schemas.test.js
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { initDatabase } from '../server/db/init.js';
import { createManifest } from '../server/manifest/manifest.js';
import { createMessagesRouter } from '../server/routes/messages.js';
import { createMailboxRouter } from '../server/routes/mailbox.js';
import { createSchemasRouter } from '../server/routes/schemas.js';
import { createWebSocketHandler } from '../server/ws/handler.js';

describe('Message Type Schemas', () => {
  let db, app, server, baseUrl, wsHandler;

  before(async () => {
    db = initDatabase(':memory:');
    const manifest = createManifest(db);
    wsHandler = createWebSocketHandler(db, manifest);

    app = express();
    app.use(express.json());
    app.use('/', createMessagesRouter(db, wsHandler, manifest));
    app.use('/mailbox', createMailboxRouter(db));
    app.use('/', createSchemasRouter(db));

    await new Promise((resolve) => {
      server = app.listen(0, '127.0.0.1', () => {
        const addr = server.address();
        baseUrl = `http://127.0.0.1:${addr.port}`;
        resolve();
      });
    });

    wsHandler.attach(server);

    // Glia and Nomos mailboxes are auto-created by manifest seed (Relay 4)
  });

  after(async () => {
    wsHandler.cleanup();
    await new Promise((resolve) => server.close(resolve));
    db.close();
  });

  // Helper: POST /messages with JSON body
  async function postMessage(body) {
    const res = await fetch(`${baseUrl}/messages`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    return { status: res.status, body: await res.json() };
  }

  // ── Envelope Validation ──────────────────────────────────────────

  describe('Envelope validation', () => {
    it('1. missing type returns 400 SCHEMA_VALIDATION_FAILED', async () => {
      const { status, body } = await postMessage({
        source_organ: 'Vigil',
        target_organ: 'Glia',
        payload: { event_type: 'test' },
      });

      assert.equal(status, 400);
      assert.equal(body.error, 'SCHEMA_VALIDATION_FAILED');
      const violation = body.violations.find((v) => v.field === 'type');
      assert.ok(violation, 'should have type violation');
      assert.equal(violation.rule, 'required');
    });

    it('2. invalid type value returns 400', async () => {
      const { status, body } = await postMessage({
        type: 'INVALID',
        source_organ: 'Vigil',
        target_organ: 'Glia',
        payload: { something: true },
      });

      assert.equal(status, 400);
      assert.equal(body.error, 'SCHEMA_VALIDATION_FAILED');
      const violation = body.violations.find((v) => v.field === 'type');
      assert.ok(violation, 'should have type violation');
      assert.equal(violation.rule, 'enum');
      assert.deepEqual(violation.expected, ['OTM', 'APM', 'PEM', 'ATM', 'HOM']);
      assert.equal(violation.actual, 'INVALID');
    });

    it('3. missing source_organ returns 400', async () => {
      const { status, body } = await postMessage({
        type: 'OTM',
        target_organ: 'Glia',
        payload: { event_type: 'test' },
      });

      assert.equal(status, 400);
      assert.equal(body.error, 'SCHEMA_VALIDATION_FAILED');
      const violation = body.violations.find((v) => v.field === 'source_organ');
      assert.ok(violation, 'should have source_organ violation');
    });

    it('4. missing payload returns 400', async () => {
      const { status, body } = await postMessage({
        type: 'OTM',
        source_organ: 'Vigil',
        target_organ: 'Glia',
      });

      assert.equal(status, 400);
      assert.equal(body.error, 'SCHEMA_VALIDATION_FAILED');
      const violation = body.violations.find((v) => v.field === 'payload');
      assert.ok(violation, 'should have payload violation');
    });

    it('5. payload is not an object (string, array, null) returns 400', async () => {
      // Test with string
      let result = await postMessage({
        type: 'OTM',
        source_organ: 'Vigil',
        target_organ: 'Glia',
        payload: 'not an object',
      });
      assert.equal(result.status, 400);
      let violation = result.body.violations.find((v) => v.field === 'payload');
      assert.ok(violation);
      assert.equal(violation.rule, 'type');

      // Test with array
      result = await postMessage({
        type: 'OTM',
        source_organ: 'Vigil',
        target_organ: 'Glia',
        payload: [1, 2, 3],
      });
      assert.equal(result.status, 400);
      violation = result.body.violations.find((v) => v.field === 'payload');
      assert.ok(violation);
      assert.equal(violation.actual, 'array');

      // Test with null
      result = await postMessage({
        type: 'OTM',
        source_organ: 'Vigil',
        target_organ: 'Glia',
        payload: null,
      });
      assert.equal(result.status, 400);
      violation = result.body.violations.find((v) => v.field === 'payload');
      assert.ok(violation);
    });

    it('6. valid envelope with all fields passes', async () => {
      const { status, body } = await postMessage({
        type: 'OTM',
        source_organ: 'Vigil',
        target_organ: 'Glia',
        correlation_id: 'urn:llm-ops:otm:2026-04-07T00:00:00.000Z-abcd',
        reply_to: 'Lobe',
        payload: { event_type: 'test_complete' },
      });

      assert.equal(status, 202);
      assert.equal(body.status, 'accepted');
    });
  });

  // ── OTM Validation ───────────────────────────────────────────────

  describe('OTM validation', () => {
    it('7. valid OTM with event_type passes', async () => {
      const { status, body } = await postMessage({
        type: 'OTM',
        source_organ: 'Vigil',
        target_organ: 'Glia',
        payload: { event_type: 'cv_failure', source: 'test-runner', data: { test_id: 1 } },
      });

      assert.equal(status, 202);
      assert.equal(body.status, 'accepted');
    });

    it('8. OTM missing event_type returns 400', async () => {
      const { status, body } = await postMessage({
        type: 'OTM',
        source_organ: 'Vigil',
        target_organ: 'Glia',
        payload: { source: 'test-runner' },
      });

      assert.equal(status, 400);
      assert.equal(body.error, 'SCHEMA_VALIDATION_FAILED');
      assert.equal(body.type, 'OTM');
      const violation = body.violations.find((v) => v.field === 'event_type');
      assert.ok(violation, 'should have event_type violation');
      assert.equal(violation.rule, 'required');
    });

    it('9. OTM with unknown extra fields passes (forward-compatible)', async () => {
      const { status, body } = await postMessage({
        type: 'OTM',
        source_organ: 'Vigil',
        target_organ: 'Glia',
        payload: {
          event_type: 'test',
          unknown_field: 'hello',
          another_future_field: { nested: true },
        },
      });

      assert.equal(status, 202);
      assert.equal(body.status, 'accepted');
    });
  });

  // ── APM Validation ───────────────────────────────────────────────

  describe('APM validation', () => {
    const validApmPayload = {
      action: 'deploy_service',
      targets: ['urn:llm-ops:entity:glia'],
      risk_tier: 'medium',
      evidence_refs: ['urn:llm-ops:radiant:analysis-001'],
      rollback_plan: 'revert to previous version',
      reason: 'performance improvement',
    };

    it('10. valid APM with all required fields passes', async () => {
      const { status, body } = await postMessage({
        type: 'APM',
        source_organ: 'Thalamus',
        target_organ: 'Nomos',
        payload: validApmPayload,
      });

      assert.equal(status, 202);
      assert.equal(body.status, 'accepted');
    });

    it('11. APM missing risk_tier returns 400', async () => {
      const { action, targets, evidence_refs, rollback_plan, reason } = validApmPayload;
      const { status, body } = await postMessage({
        type: 'APM',
        source_organ: 'Thalamus',
        target_organ: 'Nomos',
        payload: { action, targets, evidence_refs, rollback_plan, reason },
      });

      assert.equal(status, 400);
      assert.equal(body.error, 'SCHEMA_VALIDATION_FAILED');
      assert.equal(body.type, 'APM');
      const violation = body.violations.find((v) => v.field === 'risk_tier');
      assert.ok(violation, 'should have risk_tier violation');
      assert.equal(violation.rule, 'required');
    });

    it('12. APM with invalid risk_tier enum ("extreme") returns 400', async () => {
      const { status, body } = await postMessage({
        type: 'APM',
        source_organ: 'Thalamus',
        target_organ: 'Nomos',
        payload: { ...validApmPayload, risk_tier: 'extreme' },
      });

      assert.equal(status, 400);
      assert.equal(body.error, 'SCHEMA_VALIDATION_FAILED');
      const violation = body.violations.find((v) => v.field === 'risk_tier');
      assert.ok(violation, 'should have risk_tier violation');
      assert.equal(violation.rule, 'enum');
      assert.deepEqual(violation.expected, ['low', 'medium', 'high', 'critical']);
      assert.equal(violation.actual, 'extreme');
    });

    it('13. APM with targets as non-array returns 400', async () => {
      const { status, body } = await postMessage({
        type: 'APM',
        source_organ: 'Thalamus',
        target_organ: 'Nomos',
        payload: { ...validApmPayload, targets: 'not-an-array' },
      });

      assert.equal(status, 400);
      assert.equal(body.error, 'SCHEMA_VALIDATION_FAILED');
      const violation = body.violations.find((v) => v.field === 'targets');
      assert.ok(violation, 'should have targets violation');
      assert.equal(violation.rule, 'type');
      assert.equal(violation.expected, 'array');
    });
  });

  // ── PEM Validation ───────────────────────────────────────────────

  describe('PEM validation', () => {
    const validPemPayload = {
      conflict_class: 'MSP_CONFLICT',
      blocked_action: 'urn:llm-ops:apm:2026-04-07T10:00:00.000Z-ab12',
      blocking_rules: ['urn:llm-ops:rule:no-deploy-during-freeze'],
      necessity: 'Critical service update needed',
      proposed_change: 'Temporary rule exception for 2 hours',
      risk_assessment: 'Low risk — isolated service, rollback available',
    };

    it('14. valid PEM passes', async () => {
      const { status, body } = await postMessage({
        type: 'PEM',
        source_organ: 'Nomos',
        target_organ: 'Glia',
        payload: validPemPayload,
      });

      assert.equal(status, 202);
      assert.equal(body.status, 'accepted');
    });

    it('15. PEM with invalid conflict_class returns 400', async () => {
      const { status, body } = await postMessage({
        type: 'PEM',
        source_organ: 'Nomos',
        target_organ: 'Glia',
        payload: { ...validPemPayload, conflict_class: 'UNKNOWN_CONFLICT' },
      });

      assert.equal(status, 400);
      assert.equal(body.error, 'SCHEMA_VALIDATION_FAILED');
      const violation = body.violations.find((v) => v.field === 'conflict_class');
      assert.ok(violation, 'should have conflict_class violation');
      assert.equal(violation.rule, 'enum');
      assert.deepEqual(violation.expected, ['MSP_CONFLICT', 'BOR_CONFLICT']);
    });
  });

  // ── ATM Validation ───────────────────────────────────────────────

  describe('ATM validation', () => {
    const validAtmPayload = {
      token_urn: 'urn:graphheight:511:token:2026-04-07-abc1',
      scope: {
        targets: ['urn:llm-ops:entity:glia'],
        action_types: ['deploy', 'restart'],
        ttl_seconds: 3600,
        conditions: ['only during maintenance window'],
      },
      ap_ref: 'urn:llm-ops:apm:2026-04-07T10:00:00.000Z-ab12',
    };

    it('16. valid ATM with nested scope passes', async () => {
      const { status, body } = await postMessage({
        type: 'ATM',
        source_organ: 'Nomos',
        target_organ: 'Glia',
        payload: validAtmPayload,
      });

      assert.equal(status, 202);
      assert.equal(body.status, 'accepted');
    });

    it('17. ATM missing scope.targets returns 400', async () => {
      const { status, body } = await postMessage({
        type: 'ATM',
        source_organ: 'Nomos',
        target_organ: 'Glia',
        payload: {
          ...validAtmPayload,
          scope: {
            action_types: ['deploy'],
            ttl_seconds: 3600,
          },
        },
      });

      assert.equal(status, 400);
      assert.equal(body.error, 'SCHEMA_VALIDATION_FAILED');
      const violation = body.violations.find((v) => v.field === 'scope.targets');
      assert.ok(violation, 'should have scope.targets violation');
      assert.equal(violation.rule, 'required');
    });

    it('18. ATM missing token_urn returns 400', async () => {
      const { status, body } = await postMessage({
        type: 'ATM',
        source_organ: 'Nomos',
        target_organ: 'Glia',
        payload: {
          scope: validAtmPayload.scope,
          ap_ref: validAtmPayload.ap_ref,
        },
      });

      assert.equal(status, 400);
      assert.equal(body.error, 'SCHEMA_VALIDATION_FAILED');
      const violation = body.violations.find((v) => v.field === 'token_urn');
      assert.ok(violation, 'should have token_urn violation');
      assert.equal(violation.rule, 'required');
    });

    it('19. ATM with scope.ttl_seconds as string returns 400', async () => {
      const { status, body } = await postMessage({
        type: 'ATM',
        source_organ: 'Nomos',
        target_organ: 'Glia',
        payload: {
          ...validAtmPayload,
          scope: {
            ...validAtmPayload.scope,
            ttl_seconds: '3600',
          },
        },
      });

      assert.equal(status, 400);
      assert.equal(body.error, 'SCHEMA_VALIDATION_FAILED');
      const violation = body.violations.find((v) => v.field === 'scope.ttl_seconds');
      assert.ok(violation, 'should have scope.ttl_seconds violation');
      assert.equal(violation.rule, 'type');
      assert.equal(violation.expected, 'number');
    });
  });

  // ── HOM Validation ───────────────────────────────────────────────

  describe('HOM validation', () => {
    const validHomPayload = {
      decision_type: 'bor_ambiguity',
      context: 'Action crosses multiple BoR boundaries',
      question: 'Should this action be permitted under the current BoR?',
      options: ['approve', 'deny', 'defer'],
      deadline: '2026-04-08T12:00:00.000Z',
    };

    it('20. valid HOM passes', async () => {
      const { status, body } = await postMessage({
        type: 'HOM',
        source_organ: 'Nomos',
        target_organ: 'Glia',
        payload: validHomPayload,
      });

      assert.equal(status, 202);
      assert.equal(body.status, 'accepted');
    });

    it('21. HOM with invalid decision_type returns 400', async () => {
      const { status, body } = await postMessage({
        type: 'HOM',
        source_organ: 'Nomos',
        target_organ: 'Glia',
        payload: { ...validHomPayload, decision_type: 'unknown_type' },
      });

      assert.equal(status, 400);
      assert.equal(body.error, 'SCHEMA_VALIDATION_FAILED');
      const violation = body.violations.find((v) => v.field === 'decision_type');
      assert.ok(violation, 'should have decision_type violation');
      assert.equal(violation.rule, 'enum');
      assert.deepEqual(violation.expected, ['bor_ambiguity', 'amendment_proposal', 'scope_clarification']);
    });
  });

  // ── Schema Query Endpoints ───────────────────────────────────────

  describe('Schema query endpoints', () => {
    it('22. GET /schemas returns 5 schemas', async () => {
      const res = await fetch(`${baseUrl}/schemas`);
      assert.equal(res.status, 200);

      const body = await res.json();
      assert.equal(body.schemas.length, 5);

      const types = body.schemas.map((s) => s.type).sort();
      assert.deepEqual(types, ['APM', 'ATM', 'HOM', 'OTM', 'PEM']);

      // Each schema should have version, fields, description
      for (const schema of body.schemas) {
        assert.ok(schema.version, `${schema.type} should have version`);
        assert.ok(schema.fields, `${schema.type} should have fields`);
        assert.ok(schema.description, `${schema.type} should have description`);
      }
    });

    it('23. GET /schemas/OTM returns OTM schema', async () => {
      const res = await fetch(`${baseUrl}/schemas/OTM`);
      assert.equal(res.status, 200);

      const body = await res.json();
      assert.equal(body.type, 'OTM');
      assert.equal(body.version, '1.0');
      assert.ok(body.fields.event_type, 'should have event_type field');
      assert.equal(body.fields.event_type.required, true);
    });

    it('24. GET /schemas/INVALID returns 404', async () => {
      const res = await fetch(`${baseUrl}/schemas/INVALID`);
      assert.equal(res.status, 404);

      const body = await res.json();
      assert.equal(body.error, 'UNKNOWN_MESSAGE_TYPE');
      assert.ok(body.valid_types.includes('OTM'));
    });

    it('25. GET /schemas/changelog returns v1 entries for all 5 types', async () => {
      const res = await fetch(`${baseUrl}/schemas/changelog`);
      assert.equal(res.status, 200);

      const body = await res.json();
      assert.ok(body.entries.length >= 5, 'should have at least 5 changelog entries');

      const v1Types = body.entries
        .filter((e) => e.version === 1)
        .map((e) => e.message_type)
        .sort();
      assert.deepEqual(v1Types, ['APM', 'ATM', 'HOM', 'OTM', 'PEM']);

      // Each v1 entry should have initial description and fields_added
      for (const entry of body.entries.filter((e) => e.version === 1)) {
        assert.equal(entry.change_description, 'Initial schema definition');
        assert.ok(entry.fields_added, `${entry.message_type} should have fields_added`);
        const fields = JSON.parse(entry.fields_added);
        assert.ok(Array.isArray(fields), 'fields_added should be a JSON array');
        assert.ok(fields.length > 0, `${entry.message_type} should have at least one field`);
      }
    });
  });

  // ── URN Namespace ────────────────────────────────────────────────

  describe('URN namespace', () => {
    it('26. OTM message_id starts with urn:llm-ops:otm:', async () => {
      const { status, body } = await postMessage({
        type: 'OTM',
        source_organ: 'Vigil',
        target_organ: 'Glia',
        payload: { event_type: 'test_urn' },
      });

      assert.equal(status, 202);
      assert.ok(
        body.message_id.startsWith('urn:llm-ops:otm:'),
        `Expected OTM URN namespace, got: ${body.message_id}`
      );
    });

    it('27. APM message_id starts with urn:llm-ops:apm:', async () => {
      const { status, body } = await postMessage({
        type: 'APM',
        source_organ: 'Thalamus',
        target_organ: 'Nomos',
        payload: {
          action: 'test_urn',
          targets: ['urn:test:1'],
          risk_tier: 'low',
          evidence_refs: ['urn:evidence:1'],
          rollback_plan: 'revert',
          reason: 'URN test',
        },
      });

      assert.equal(status, 202);
      assert.ok(
        body.message_id.startsWith('urn:llm-ops:apm:'),
        `Expected APM URN namespace, got: ${body.message_id}`
      );
    });
  });
});
