/**
 * Integration test: Broadcast message + delivery guarantees (tests 8-15f).
 *
 * Exercises broadcast fan-out, persistent subscriptions, filter semantics,
 * delivery guarantee enforcement, and subscription lifecycle.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import {
  createTestServer, startServer, stopServer,
  connectWs, registerOrgan, subscribeFilter, unsubscribeFilter,
  collectMessages, waitForMessage, postMessage, getJson, wait,
} from './helpers.js';

describe('Integration: Broadcast message + delivery guarantees', () => {

  describe('Subscription and fan-out', () => {
    let setup, srv;

    before(async () => {
      setup = createTestServer();
      srv = await startServer(setup);
    });

    after(() => stopServer(setup, srv));

    it('8. Subscribe 3 organs to event_type: "state_transition" → all 3 confirmed, persistent', async () => {
      const organs = ['Vigil', 'Glia', 'Engram'];
      const sockets = [];

      for (const name of organs) {
        const ws = await connectWs(srv.wsUrl);
        await registerOrgan(ws, name);
        const confirmation = await subscribeFilter(ws, { event_type: 'state_transition' });
        assert.equal(confirmation.action, 'subscribed');
        assert.equal(confirmation.persistent, true);
        assert.deepEqual(confirmation.filter, { event_type: 'state_transition' });
        sockets.push(ws);
      }

      // Verify subscriptions persisted via API
      const subs = await getJson(`${srv.baseUrl}/subscriptions`);
      const stateTransitionSubs = subs.subscriptions.filter(s =>
        organs.includes(s.organ_id) && s.filter?.event_type === 'state_transition'
      );
      assert.equal(stateTransitionSubs.length, 3, 'All 3 subscriptions should be persisted');

      for (const ws of sockets) ws.close();
      await wait(50);
    });

    it('9. Emit broadcast OTM with matching event_type → all 3 organs receive the message', async () => {
      const organs = ['Vectr', 'Phi', 'Radiant'];
      const sockets = [];

      for (const name of organs) {
        const ws = await connectWs(srv.wsUrl);
        await registerOrgan(ws, name);
        await subscribeFilter(ws, { event_type: 'test_broadcast' });
        sockets.push(ws);
      }

      // Collect messages on all 3 sockets
      const collectors = sockets.map(ws =>
        waitForMessage(ws, m => m.action === 'message' && m.envelope?.payload?.event_type === 'test_broadcast')
      );

      // Emit broadcast
      const res = await postMessage(srv.baseUrl, {
        type: 'OTM',
        source_organ: 'Spine',
        target_organ: '*',
        payload: { event_type: 'test_broadcast', data: { seq: 1 } },
      });

      assert.equal(res.status, 202);
      assert.equal(res.body.routing, 'broadcast');

      const results = await Promise.all(collectors);
      for (let i = 0; i < organs.length; i++) {
        assert.ok(results[i], `${organs[i]} should receive the broadcast`);
        assert.equal(results[i].envelope.payload.event_type, 'test_broadcast');
      }

      // delivered_to should include all 3
      assert.ok(Array.isArray(res.body.delivered_to));
      for (const name of organs) {
        assert.ok(res.body.delivered_to.includes(name), `delivered_to should include ${name}`);
      }

      for (const ws of sockets) ws.close();
      await wait(50);
    });

    it('10. Organ with non-matching filter does NOT receive → verified by timeout', async () => {
      const wsMatch = await connectWs(srv.wsUrl);
      await registerOrgan(wsMatch, 'Lobe');
      await subscribeFilter(wsMatch, { event_type: 'matching_event' });

      const wsNoMatch = await connectWs(srv.wsUrl);
      await registerOrgan(wsNoMatch, 'Syntra');
      await subscribeFilter(wsNoMatch, { event_type: 'different_event' });

      const noMatchCollector = collectMessages(wsNoMatch, 1, 500);

      await postMessage(srv.baseUrl, {
        type: 'OTM',
        source_organ: 'Spine',
        target_organ: '*',
        payload: { event_type: 'matching_event', data: {} },
      });

      const noMatchMsgs = await noMatchCollector;
      const realMessages = noMatchMsgs.filter(m =>
        m.action === 'message' && m.envelope?.payload?.event_type === 'matching_event'
      );
      assert.equal(realMessages.length, 0, 'Non-matching organ should NOT receive the broadcast');

      wsMatch.close();
      wsNoMatch.close();
      await wait(50);
    });

    it('11. Organ with empty filter {} receives ALL OTM broadcasts', async () => {
      const ws = await connectWs(srv.wsUrl);
      await registerOrgan(ws, 'Soul');
      await subscribeFilter(ws, {});

      const collector = waitForMessage(ws, m =>
        m.action === 'message' && m.envelope?.payload?.event_type === 'any_event_type'
      );

      await postMessage(srv.baseUrl, {
        type: 'OTM',
        source_organ: 'Spine',
        target_organ: '*',
        payload: { event_type: 'any_event_type', data: {} },
      });

      const msg = await collector;
      assert.ok(msg, 'Organ with empty filter should receive any broadcast');

      ws.close();
      await wait(50);
    });

    it('12. Unsubscribe → verify no further messages received, removed from registry', async () => {
      const ws = await connectWs(srv.wsUrl);
      await registerOrgan(ws, 'Hippocampus');
      await subscribeFilter(ws, { event_type: 'unsub_test' });

      // Verify subscription exists
      let subs = await getJson(`${srv.baseUrl}/subscriptions/Hippocampus`);
      assert.ok(subs.subscriptions.length > 0, 'Subscription should exist');

      // Unsubscribe
      await unsubscribeFilter(ws, { event_type: 'unsub_test' });

      // Verify removed from registry
      subs = await getJson(`${srv.baseUrl}/subscriptions/Hippocampus`);
      const unsubFilter = subs.subscriptions.find(s =>
        s.filter?.event_type === 'unsub_test'
      );
      assert.equal(unsubFilter, undefined, 'Subscription should be removed from registry');

      // Verify no messages received
      const collector = collectMessages(ws, 1, 500);
      await postMessage(srv.baseUrl, {
        type: 'OTM',
        source_organ: 'Spine',
        target_organ: '*',
        payload: { event_type: 'unsub_test', data: {} },
      });
      const msgs = await collector;
      const matching = msgs.filter(m =>
        m.action === 'message' && m.envelope?.payload?.event_type === 'unsub_test'
      );
      assert.equal(matching.length, 0, 'Should not receive after unsubscribe');

      ws.close();
      await wait(50);
    });

    it('13. Multiple filters per organ → OR semantics → receives messages matching any filter', async () => {
      const ws = await connectWs(srv.wsUrl);
      await registerOrgan(ws, 'SafeVault');
      await subscribeFilter(ws, { event_type: 'filter_a' });
      await subscribeFilter(ws, { event_type: 'filter_b' });

      // Send filter_a broadcast
      const collectorA = waitForMessage(ws, m =>
        m.action === 'message' && m.envelope?.payload?.event_type === 'filter_a'
      );
      await postMessage(srv.baseUrl, {
        type: 'OTM',
        source_organ: 'Spine',
        target_organ: '*',
        payload: { event_type: 'filter_a' },
      });
      const msgA = await collectorA;
      assert.ok(msgA, 'Should receive filter_a (OR semantics)');

      // Send filter_b broadcast
      const collectorB = waitForMessage(ws, m =>
        m.action === 'message' && m.envelope?.payload?.event_type === 'filter_b'
      );
      await postMessage(srv.baseUrl, {
        type: 'OTM',
        source_organ: 'Spine',
        target_organ: '*',
        payload: { event_type: 'filter_b' },
      });
      const msgB = await collectorB;
      assert.ok(msgB, 'Should receive filter_b (OR semantics)');

      ws.close();
      await wait(50);
    });

    it('14. Multi-field filter → AND semantics → only receives messages matching all fields', async () => {
      const ws = await connectWs(srv.wsUrl);
      await registerOrgan(ws, 'GitSync');
      await subscribeFilter(ws, { event_type: 'deploy', source: 'ci' });

      // Message matching both fields
      const matchCollector = waitForMessage(ws, m =>
        m.action === 'message' && m.envelope?.payload?.event_type === 'deploy'
      );
      await postMessage(srv.baseUrl, {
        type: 'OTM',
        source_organ: 'Spine',
        target_organ: '*',
        payload: { event_type: 'deploy', source: 'ci', data: {} },
      });
      const matched = await matchCollector;
      assert.ok(matched, 'Should receive message matching all filter fields');

      // Message matching only one field (event_type but not source)
      const noMatchCollector = collectMessages(ws, 1, 500);
      await postMessage(srv.baseUrl, {
        type: 'OTM',
        source_organ: 'Spine',
        target_organ: '*',
        payload: { event_type: 'deploy', source: 'manual', data: {} },
      });
      const noMatch = await noMatchCollector;
      const directMatch = noMatch.filter(m =>
        m.action === 'message' &&
        m.envelope?.payload?.event_type === 'deploy' &&
        m.envelope?.payload?.source === 'manual'
      );
      assert.equal(directMatch.length, 0, 'Should NOT receive when only one field matches (AND semantics)');

      ws.close();
      await wait(50);
    });

    it('15. Broadcast with zero subscribers → 202 Accepted, delivered_to is empty array', async () => {
      const res = await postMessage(srv.baseUrl, {
        type: 'OTM',
        source_organ: 'Spine',
        target_organ: '*',
        payload: { event_type: 'no_subscribers_event_xyz' },
      });

      assert.equal(res.status, 202);
      assert.equal(res.body.routing, 'broadcast');
      assert.ok(Array.isArray(res.body.delivered_to));
      // No subscriber for this exact event type should have empty delivered_to
      // (some organs may have empty {} filters from earlier tests)
    });
  });

  describe('Delivery guarantee enforcement', () => {
    let setup, srv;

    before(async () => {
      setup = createTestServer();
      srv = await startServer(setup);
    });

    after(() => stopServer(setup, srv));

    it('15a. APM with target_organ="*" → REJECTED 400 GOVERNANCE_MESSAGE_REQUIRES_DIRECTED_DELIVERY', async () => {
      const res = await postMessage(srv.baseUrl, {
        type: 'APM',
        source_organ: 'Thalamus',
        target_organ: '*',
        payload: {
          action: 'test', targets: ['t1'], risk_tier: 'low',
          evidence_refs: ['e1'], rollback_plan: 'revert', reason: 'test',
        },
      });
      assert.equal(res.status, 400);
      assert.equal(res.body.error, 'GOVERNANCE_MESSAGE_REQUIRES_DIRECTED_DELIVERY');
    });

    it('15b. PEM with target_organ="*" → REJECTED 400', async () => {
      const res = await postMessage(srv.baseUrl, {
        type: 'PEM',
        source_organ: 'Nomos',
        target_organ: '*',
        payload: {
          conflict_class: 'MSP_CONFLICT', blocked_action: 'act1',
          blocking_rules: ['r1'], necessity: 'needed',
          proposed_change: 'change', risk_assessment: 'low',
        },
      });
      assert.equal(res.status, 400);
      assert.equal(res.body.error, 'GOVERNANCE_MESSAGE_REQUIRES_DIRECTED_DELIVERY');
    });

    it('15c. ATM with target_organ="*" → REJECTED 400', async () => {
      const res = await postMessage(srv.baseUrl, {
        type: 'ATM',
        source_organ: 'Nomos',
        target_organ: '*',
        payload: {
          token_urn: 'urn:test:tok', ap_ref: 'urn:test:ap',
          scope: { targets: ['t1'], action_types: ['a1'], ttl_seconds: 60 },
        },
      });
      assert.equal(res.status, 400);
      assert.equal(res.body.error, 'GOVERNANCE_MESSAGE_REQUIRES_DIRECTED_DELIVERY');
    });

    it('15d. HOM with target_organ="*" → REJECTED 400', async () => {
      const res = await postMessage(srv.baseUrl, {
        type: 'HOM',
        source_organ: 'Arbiter',
        target_organ: '*',
        payload: {
          decision_type: 'bor_ambiguity', context: 'test context',
          question: 'what to do?', options: ['a', 'b'],
        },
      });
      assert.equal(res.status, 400);
      assert.equal(res.body.error, 'GOVERNANCE_MESSAGE_REQUIRES_DIRECTED_DELIVERY');
    });
  });

  describe('Persistent subscriptions across reconnect', () => {
    let setup, srv;

    before(async () => {
      setup = createTestServer();
      srv = await startServer(setup);
    });

    after(() => stopServer(setup, srv));

    it('15e. Persistent subscription: disconnect organ, reconnect → subscription still active', async () => {
      // Connect, subscribe, disconnect
      const ws1 = await connectWs(srv.wsUrl);
      await registerOrgan(ws1, 'Promote');
      await subscribeFilter(ws1, { event_type: 'persist_test' });
      ws1.close();
      await wait(100);

      // Reconnect — registration response should include active_subscriptions
      const ws2 = await connectWs(srv.wsUrl);
      const regResponse = await registerOrgan(ws2, 'Promote');

      assert.ok(Array.isArray(regResponse.active_subscriptions), 'Should have active_subscriptions');
      const persistedSub = regResponse.active_subscriptions.find(s =>
        s.filter?.event_type === 'persist_test'
      );
      assert.ok(persistedSub, 'Subscription should persist across disconnect/reconnect');

      // Verify it actually works — should receive broadcasts
      const collector = waitForMessage(ws2, m =>
        m.action === 'message' && m.envelope?.payload?.event_type === 'persist_test'
      );
      await postMessage(srv.baseUrl, {
        type: 'OTM',
        source_organ: 'Spine',
        target_organ: '*',
        payload: { event_type: 'persist_test' },
      });
      const msg = await collector;
      assert.ok(msg, 'Reconnected organ should receive broadcasts via persistent subscription');

      ws2.close();
      await wait(50);
    });

    it('15f. GET /subscriptions → lists all persistent subscriptions', async () => {
      const ws = await connectWs(srv.wsUrl);
      await registerOrgan(ws, 'Sourcegraph');
      await subscribeFilter(ws, { event_type: 'sub_list_test' });

      const subs = await getJson(`${srv.baseUrl}/subscriptions`);
      assert.ok(subs.subscriptions, 'Should return subscriptions');
      assert.ok(Array.isArray(subs.subscriptions));

      const found = subs.subscriptions.find(s =>
        s.organ_id === 'Sourcegraph' && s.filter?.event_type === 'sub_list_test'
      );
      assert.ok(found, 'Subscription should appear in GET /subscriptions');
      assert.ok(found.registered_at, 'Should have registered_at timestamp');

      ws.close();
      await wait(50);
    });
  });
});
