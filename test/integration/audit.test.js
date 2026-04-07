/**
 * Integration test: StorageAdapter + audit trail (tests 58-67).
 *
 * Exercises the events audit trail, query filters, adapter coherence,
 * organ manifest seeding, and cross-contamination verification.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import { join, resolve } from 'node:path';
import {
  createTestServer, startServer, stopServer,
  connectWs, registerOrgan, postMessage, getJson, wait,
} from './helpers.js';

describe('Integration: StorageAdapter + audit trail', () => {
  let setup, srv;

  before(async () => {
    setup = createTestServer();
    srv = await startServer(setup);
  });

  after(() => stopServer(setup, srv));

  it('58. Every directed message appears in events table', async () => {
    const res = await postMessage(srv.baseUrl, {
      type: 'OTM',
      source_organ: 'Vigil',
      target_organ: 'Glia',
      payload: { event_type: 'audit_directed_test' },
    });

    assert.equal(res.status, 202);

    const events = await getJson(`${srv.baseUrl}/events?source_organ=Vigil`);
    const found = events.events.find(e =>
      e.envelope.message_id === res.body.message_id
    );
    assert.ok(found, 'Directed message should appear in events table');
    assert.equal(found.routing, 'directed');
    assert.equal(found.message_type, 'OTM');
    assert.equal(found.source_organ, 'Vigil');
    assert.equal(found.target_organ, 'Glia');
  });

  it('59. Every broadcast message appears in events table with routing: "broadcast"', async () => {
    const res = await postMessage(srv.baseUrl, {
      type: 'OTM',
      source_organ: 'Spine',
      target_organ: '*',
      payload: { event_type: 'audit_broadcast_test' },
    });

    assert.equal(res.status, 202);

    const events = await getJson(`${srv.baseUrl}/events?routing=broadcast`);
    const found = events.events.find(e =>
      e.envelope.payload?.event_type === 'audit_broadcast_test'
    );
    assert.ok(found, 'Broadcast message should appear in events table');
    assert.equal(found.routing, 'broadcast');
  });

  it('60. Events query with type filter → GET /events?type=APM returns only APMs', async () => {
    // Send an APM
    await postMessage(srv.baseUrl, {
      type: 'APM',
      source_organ: 'Thalamus',
      target_organ: 'Nomos',
      payload: {
        action: 'audit_filter_test', targets: ['t1'], risk_tier: 'low',
        evidence_refs: ['e1'], rollback_plan: 'revert', reason: 'test',
      },
    });

    const events = await getJson(`${srv.baseUrl}/events?type=APM`);
    assert.ok(events.events.length > 0, 'Should return APM events');
    for (const e of events.events) {
      assert.equal(e.message_type, 'APM', 'All events should be APM type');
    }
  });

  it('61. Events query with time range → GET /events?since=...&until=... returns correct window', async () => {
    const before = new Date().toISOString();
    await wait(10);

    await postMessage(srv.baseUrl, {
      type: 'OTM',
      source_organ: 'Vigil',
      target_organ: 'Glia',
      payload: { event_type: 'time_range_test' },
    });

    await wait(10);
    const after = new Date().toISOString();

    const events = await getJson(
      `${srv.baseUrl}/events?since=${encodeURIComponent(before)}&until=${encodeURIComponent(after)}`
    );

    // Should include the event we just created
    const found = events.events.find(e =>
      e.envelope.payload?.event_type === 'time_range_test'
    );
    assert.ok(found, 'Time range query should include our event');
  });

  it('62. Events query with source_organ filter → correct subset', async () => {
    await postMessage(srv.baseUrl, {
      type: 'OTM',
      source_organ: 'Engram',
      target_organ: 'Glia',
      payload: { event_type: 'source_filter_test' },
    });

    const events = await getJson(`${srv.baseUrl}/events?source_organ=Engram`);
    assert.ok(events.events.length > 0, 'Should return events from Engram');
    for (const e of events.events) {
      assert.equal(e.source_organ, 'Engram', 'All events should be from Engram');
    }
  });

  it('63. Events table is append-only → no deletes or updates observed', async () => {
    // Send 3 messages and verify count only grows
    const beforeCount = (await getJson(`${srv.baseUrl}/events`)).count;

    await postMessage(srv.baseUrl, {
      type: 'OTM', source_organ: 'Vigil', target_organ: 'Glia',
      payload: { event_type: 'append_test_1' },
    });
    await postMessage(srv.baseUrl, {
      type: 'OTM', source_organ: 'Vigil', target_organ: 'Glia',
      payload: { event_type: 'append_test_2' },
    });
    await postMessage(srv.baseUrl, {
      type: 'OTM', source_organ: 'Vigil', target_organ: 'Glia',
      payload: { event_type: 'append_test_3' },
    });

    const afterCount = (await getJson(`${srv.baseUrl}/events`)).count;
    assert.ok(afterCount >= beforeCount + 3, 'Event count should only grow (append-only)');
  });

  it('64. Zero references to ai-kb.db in codebase', async () => {
    const projectRoot = resolve(import.meta.dirname, '../../');
    const violations = [];

    function scanDir(dir, relativeTo) {
      let entries;
      try {
        entries = readdirSync(dir, { withFileTypes: true });
      } catch {
        return;
      }
      for (const entry of entries) {
        const full = join(dir, entry.name);
        const rel = full.slice(relativeTo.length + 1);
        if (entry.name === 'node_modules' || entry.name === '.git') continue;
        if (entry.isDirectory()) {
          scanDir(full, relativeTo);
        } else if (entry.isFile() && /\.(js|json|sh)$/i.test(entry.name)) {
          // Only scan production source code — exclude tests and docs
          if (rel.startsWith('test/')) continue;
          try {
            const content = readFileSync(full, 'utf-8');
            if (content.includes('ai-kb.db')) {
              violations.push(rel);
            }
          } catch { /* skip unreadable files */ }
        }
      }
    }

    scanDir(projectRoot, projectRoot);
    assert.equal(violations.length, 0,
      `Found ai-kb.db references in source code: ${violations.join(', ')}`);
  });

  it('65. Organ manifest seeded with 29 organs (28 required + human-principal)', async () => {
    const manifest = await getJson(`${srv.baseUrl}/manifest`);
    assert.equal(manifest.total, 29, 'Manifest should have 29 organs');

    const required = manifest.organs.filter(o => o.required === true);
    assert.equal(required.length, 28, '28 organs should be required');

    const optional = manifest.organs.filter(o => o.required === false);
    assert.equal(optional.length, 1, '1 organ should be optional');
    assert.equal(optional[0].organ_id, 'human-principal', 'Optional organ should be human-principal');
  });

  it('66. Subscription registry persists across server restart', async () => {
    // This test uses the in-memory adapter directly since we can't restart
    // a separate server process. We verify persistence at the adapter level:
    // subscribe → load fresh cache → verify subscription survived.

    const ws = await connectWs(srv.wsUrl);
    await registerOrgan(ws, 'Receptor');

    // Subscribe via WebSocket
    ws.send(JSON.stringify({ action: 'subscribe', filter: { event_type: 'persist_restart_test' } }));
    await wait(100);

    // Verify subscription is in the database via API
    const subs = await getJson(`${srv.baseUrl}/subscriptions/Receptor`);
    const found = subs.subscriptions.find(s => s.filter?.event_type === 'persist_restart_test');
    assert.ok(found, 'Subscription should be persisted in database');

    // Simulate cache reload (what happens on restart)
    setup.wsHandler.loadSubscriptionCache();

    // Verify subscription survived cache reload
    const cache = setup.wsHandler.getSubscriptionCache();
    const cached = cache.get('Receptor');
    assert.ok(cached, 'Subscription should survive cache reload');
    const cachedFilter = cached.find(f => f.event_type === 'persist_restart_test');
    assert.ok(cachedFilter, 'Subscription filter should be in reloaded cache');

    ws.close();
    await wait(50);
  });

  it('67. StorageAdapter is sole db accessor → no db.prepare outside adapter', async () => {
    const projectRoot = resolve(import.meta.dirname, '../../');
    const violations = [];

    function scanDir(dir, relativeTo) {
      let entries;
      try {
        entries = readdirSync(dir, { withFileTypes: true });
      } catch {
        return;
      }
      for (const entry of entries) {
        const full = join(dir, entry.name);
        const rel = full.slice(relativeTo.length + 1);
        if (entry.name === 'node_modules' || entry.name === '.git') continue;
        if (entry.isDirectory()) {
          scanDir(full, relativeTo);
        } else if (entry.isFile() && entry.name.endsWith('.js')) {
          // Skip the adapter directory and DB init (which creates the adapter)
          if (rel.includes('adapter/')) continue;
          if (rel.includes('db/init.js')) continue;
          // Skip test files (they may use adapter methods that internally call prepare)
          if (rel.startsWith('test/')) continue;

          try {
            const content = readFileSync(full, 'utf-8');
            // Look for direct db.prepare calls outside the adapter
            if (/\bdb\.prepare\b/.test(content) || /\._db\.prepare\b/.test(content)) {
              violations.push(rel);
            }
          } catch { /* skip */ }
        }
      }
    }

    scanDir(projectRoot, projectRoot);
    assert.equal(violations.length, 0,
      `Found db.prepare outside adapter in: ${violations.join(', ')}`);
  });
});
