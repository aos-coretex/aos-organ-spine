/**
 * WebSocket handler for the Spine organ.
 *
 * Organ-identified connections: organs register by name,
 * receive directed messages in real-time, and send acks.
 *
 * Relay 4 additions:
 * - Manifest validation: unknown organs rejected at registration
 * - Persistent subscriptions: subscribe/unsubscribe actions update via adapter + cache
 * - Reconnection recovery: active_subscriptions in response, mailbox backlog push
 *
 * Relay 6 additions:
 * - Heartbeat extracted to dedicated module (heartbeat.js)
 * - Organ lifecycle state management (REGISTERED -> ALIVE -> DEGRADED -> DISCONNECTED)
 * - Vigil notification OTM broadcasts on organ connect/disconnect
 * - Health self-check with periodic spine_health OTM broadcast
 * - Mailbox TTL expiry sweep (aligned with health check)
 * - Critical missing organ detection with HOM to human-principal mailbox
 *
 * All storage operations go through the StorageAdapter.
 *
 * Client state: Map<WebSocket, { organName, connectedAt, missedPongs, lastPongAt }>
 */

import { WebSocketServer } from 'ws';
import { createHeartbeatMonitor } from './heartbeat.js';
import { generateUrn } from '../../lib/urn.js';

function log(event, data = {}) {
  const entry = { timestamp: new Date().toISOString(), event, ...data };
  process.stdout.write(JSON.stringify(entry) + '\n');
}

export function createWebSocketHandler(adapter, manifest) {
  const clients = new Map();
  let wss = null;
  let heartbeat = null;

  // Relay 6: late-bound health dependencies (set via setHealthDependencies)
  let healthDeps = null;

  // Relay 6: tracking state for health monitoring
  const reconnectionTimers = new Map();   // organName -> setTimeout handle
  const criticalAlertsSent = new Set();   // organNames that have had HOMs sent
  const organDisconnectTimes = new Map(); // organName -> Date.now() of confirmed disconnect
  const startTime = Date.now();

  // --- Subscription cache: Map<organId, Array<filter>> ---
  // Loaded from subscription_registry at startup. Updated by subscribe/unsubscribe.
  // Only broadcast subscriptions are cached (directed uses mailbox lookup).
  const subscriptionCache = new Map();

  // --- Subscription cache management ---

  function loadSubscriptionCache() {
    subscriptionCache.clear();

    const rows = adapter.getAllSubscriptions();
    for (const row of rows) {
      const filter = row.topic_filter ? JSON.parse(row.topic_filter) : {};
      if (!subscriptionCache.has(row.organ_id)) {
        subscriptionCache.set(row.organ_id, []);
      }
      subscriptionCache.get(row.organ_id).push(filter);
    }

    log('subscription_cache_loaded', {
      organs: subscriptionCache.size,
      total_subscriptions: rows.length,
    });
  }

  function getSubscriptionCache() {
    return subscriptionCache;
  }

  // --- Safe send ---

  function safeSend(ws, obj) {
    try {
      if (ws.readyState === 1) { // WebSocket.OPEN
        ws.send(JSON.stringify(obj));
        return true;
      }
    } catch (err) {
      log('ws_send_error', { error: err.message });
    }
    return false;
  }

  // ================================================================
  // Relay 6: Organ lifecycle state management
  // ================================================================

  /**
   * Organ connect sequence — called when an organ registers via WebSocket.
   *
   * 1. Create or transition organ state entity to ALIVE
   * 2. Clear reconnection timer and critical alert state
   * 3. Emit organ_connected Vigil OTM broadcast
   */
  function executeConnectSequence(organName) {
    if (!healthDeps?.stateMachine) return;

    const entityUrn = `organ:${organName}`;
    const entity = healthDeps.stateMachine.getEntity(entityUrn);
    let previousState = null;

    if (!entity || entity.error) {
      // New organ: create at REGISTERED, then transition to ALIVE
      healthDeps.stateMachine.createEntity(entityUrn, 'organ');
      healthDeps.stateMachine.transition(
        entityUrn, 'REGISTERED', 'ALIVE', 'organ_connected', 'Spine',
      );
      previousState = 'REGISTERED';
    } else if (entity.current_state === 'DISCONNECTED') {
      healthDeps.stateMachine.transition(
        entityUrn, 'DISCONNECTED', 'ALIVE', 'organ_reconnected', 'Spine',
      );
      previousState = 'DISCONNECTED';
    } else if (entity.current_state === 'REGISTERED') {
      healthDeps.stateMachine.transition(
        entityUrn, 'REGISTERED', 'ALIVE', 'organ_connected', 'Spine',
      );
      previousState = 'REGISTERED';
    } else {
      // Already ALIVE or DEGRADED — no transition needed
      previousState = entity.current_state;
    }

    // Clear reconnection tracking
    if (reconnectionTimers.has(organName)) {
      clearTimeout(reconnectionTimers.get(organName));
      reconnectionTimers.delete(organName);
    }
    criticalAlertsSent.delete(organName);
    organDisconnectTimes.delete(organName);

    // Emit organ_connected Vigil OTM broadcast
    if (healthDeps.emitMessage) {
      const depth = adapter.getMailboxDepth(organName);
      healthDeps.emitMessage({
        type: 'OTM',
        source_organ: 'Spine',
        target_organ: '*',
        message_id: generateUrn('otm'),
        correlation_id: null,
        reply_to: 'Spine',
        timestamp: new Date().toISOString(),
        payload: {
          event_type: 'organ_connected',
          source: 'spine-health',
          data: {
            organ_name: organName,
            mailbox_depth: depth,
            previous_state: previousState,
          },
        },
      });
    }
  }

  /**
   * Organ disconnect sequence — called on heartbeat timeout or clean close.
   *
   * 1. Extract client state (lastSeen) before removal
   * 2. Remove from client map (prevents re-entry from close event)
   * 3. Close/terminate WebSocket
   * 4. Update mailbox status to disconnected
   * 5. Transition organ state to DISCONNECTED
   * 6. Check manifest — start reconnection timer for required organs
   * 7. Emit organ_disconnected Vigil OTM broadcast
   */
  function executeDisconnectSequence(organName, ws, reason) {
    const state = clients.get(ws);
    if (!state) return; // already handled (idempotent guard)

    const lastSeen = state.lastPongAt || state.connectedAt;

    // Remove from client map first (prevents re-entry from close event)
    clients.delete(ws);

    // Close/terminate WebSocket
    try {
      if (reason === 'heartbeat_timeout') {
        ws.terminate();
      } else {
        ws.close(1000, reason);
      }
    } catch { /* ignore errors on already-closed sockets */ }

    if (!organName) return;

    // Update mailbox status
    adapter.updateMailboxStatus(organName, 'disconnected');

    // Transition organ state to DISCONNECTED
    if (healthDeps?.stateMachine) {
      const entity = healthDeps.stateMachine.getEntity(`organ:${organName}`);
      if (entity && !entity.error) {
        const fromState = entity.current_state;
        if (fromState === 'ALIVE' || fromState === 'DEGRADED') {
          healthDeps.stateMachine.transition(
            `organ:${organName}`, fromState, 'DISCONNECTED',
            reason, 'Spine',
          );
        }
      }
    }

    // Check manifest — start reconnection timer for required organs
    if (manifest && manifest.isRequired(organName)) {
      const timeoutMs = healthDeps?.healthConfig?.reconnectionTimeoutMs || 30_000;
      const timer = setTimeout(() => {
        reconnectionTimers.delete(organName);
        // If still disconnected after grace period, start tracking for critical threshold
        if (!isOrganConnected(organName)) {
          organDisconnectTimes.set(organName, Date.now());
        }
      }, timeoutMs);
      reconnectionTimers.set(organName, timer);
    }

    // Emit organ_disconnected Vigil OTM broadcast
    if (healthDeps?.emitMessage) {
      const depth = adapter.getMailboxDepth(organName);
      healthDeps.emitMessage({
        type: 'OTM',
        source_organ: 'Spine',
        target_organ: '*',
        message_id: generateUrn('otm'),
        correlation_id: null,
        reply_to: 'Spine',
        timestamp: new Date().toISOString(),
        payload: {
          event_type: 'organ_disconnected',
          source: 'spine-health',
          data: {
            organ_name: organName,
            reason,
            last_seen: lastSeen,
            mailbox_depth: depth,
          },
        },
      });
    }

    log('ws_organ_disconnected', { organ: organName, reason });
  }

  /**
   * Handle organ entering DEGRADED state (1 missed pong).
   */
  function handleOrganDegraded(organName) {
    if (!healthDeps?.stateMachine || !organName) return;

    const entity = healthDeps.stateMachine.getEntity(`organ:${organName}`);
    if (entity && !entity.error && entity.current_state === 'ALIVE') {
      healthDeps.stateMachine.transition(
        `organ:${organName}`, 'ALIVE', 'DEGRADED',
        'missed_pong', 'Spine',
      );
    }
  }

  /**
   * Handle organ recovering from DEGRADED state (pong received after miss).
   */
  function handleOrganRecovered(organName) {
    if (!healthDeps?.stateMachine || !organName) return;

    const entity = healthDeps.stateMachine.getEntity(`organ:${organName}`);
    if (entity && !entity.error && entity.current_state === 'DEGRADED') {
      healthDeps.stateMachine.transition(
        `organ:${organName}`, 'DEGRADED', 'ALIVE',
        'pong_recovered', 'Spine',
      );
    }
  }

  // ================================================================
  // Relay 6: Health self-check
  // ================================================================

  /**
   * Periodic health self-check (runs every healthHeartbeatMs).
   *
   * 1. Check SQLite connectivity
   * 2. Count connected organs and total mailbox depth
   * 3. Run TTL expiry sweep
   * 4. Check critical missing required organs -> emit HOM to human-principal
   * 5. Emit spine_health OTM broadcast
   */
  function performHealthCheck() {
    const sqliteOk = adapter.healthCheck();
    const connectedOrgans = getClientCount();
    const totalMailboxDepth = adapter.getTotalMailboxDepth();

    // TTL expiry sweep
    const expiredCount = adapter.expireMessages();
    if (expiredCount > 0) {
      log('mailbox_ttl_expired', { expired_count: expiredCount });
    }

    // Check critical missing required organs
    let spineStatus = sqliteOk ? 'ok' : 'degraded';
    const criticalThreshold = healthDeps?.healthConfig?.criticalMissingThresholdMs || 300_000;
    const now = Date.now();

    for (const [organName, disconnectTime] of organDisconnectTimes) {
      // Clean up if organ reconnected
      if (isOrganConnected(organName)) {
        organDisconnectTimes.delete(organName);
        criticalAlertsSent.delete(organName);
        continue;
      }

      if (now - disconnectTime > criticalThreshold) {
        spineStatus = 'degraded';

        if (!criticalAlertsSent.has(organName)) {
          criticalAlertsSent.add(organName);
          emitCriticalMissingHom(organName, now - disconnectTime);
        }
      }
    }

    // Emit spine_health OTM broadcast
    if (healthDeps?.emitMessage) {
      healthDeps.emitMessage({
        type: 'OTM',
        source_organ: 'Spine',
        target_organ: '*',
        message_id: generateUrn('otm'),
        correlation_id: null,
        reply_to: 'Spine',
        timestamp: new Date().toISOString(),
        payload: {
          event_type: 'spine_health',
          source: 'spine-health',
          data: {
            status: spineStatus,
            uptime_s: Math.floor((Date.now() - startTime) / 1000),
            connected_organs: connectedOrgans,
            total_mailbox_depth: totalMailboxDepth,
            sqlite_ok: sqliteOk,
          },
        },
      });
    }
  }

  /**
   * Emit HOM for a critical missing required organ.
   * Directed to human-principal mailbox — persists until drained.
   */
  function emitCriticalMissingHom(organName, elapsedMs) {
    if (!healthDeps?.emitMessage) return;

    const elapsedMinutes = Math.floor(elapsedMs / 60_000);
    healthDeps.emitMessage({
      type: 'HOM',
      source_organ: 'Spine',
      target_organ: 'human-principal',
      message_id: generateUrn('hom'),
      correlation_id: null,
      reply_to: 'Spine',
      timestamp: new Date().toISOString(),
      payload: {
        decision_type: 'scope_clarification',
        context: `Required organ ${organName} has been disconnected for >${elapsedMinutes} minutes`,
        question: 'Investigate and restore, or acknowledge degraded operation?',
        options: ['investigate', 'acknowledge_degraded'],
        deadline: null,
      },
    });

    log('hom_critical_missing_organ', { organ: organName, elapsed_minutes: elapsedMinutes });
  }

  // ================================================================
  // Message handlers
  // ================================================================

  function handleRegister(ws, data) {
    const { organ_name } = data;
    if (!organ_name) {
      safeSend(ws, { action: 'error', message: 'organ_name required for register' });
      return;
    }

    // Relay 4: Manifest validation — reject unknown organs
    if (manifest && !manifest.isKnown(organ_name)) {
      safeSend(ws, {
        action: 'error',
        message: 'UNKNOWN_ORGAN',
        organ_name,
      });
      ws.close(4003, 'unknown organ');
      log('ws_organ_rejected', { organ: organ_name, reason: 'UNKNOWN_ORGAN' });
      return;
    }

    const mailbox = adapter.getMailbox(organ_name);
    if (!mailbox) {
      safeSend(ws, { action: 'error', message: `No mailbox registered for organ: ${organ_name}` });
      ws.close(4001, 'unregistered organ');
      return;
    }

    // Close any existing connection for this organ (replacement, not disconnect)
    for (const [existingWs, existingState] of clients) {
      if (existingState.organName === organ_name && existingWs !== ws) {
        safeSend(existingWs, { action: 'replaced', message: 'New connection registered for this organ' });
        clients.delete(existingWs);
        existingWs.close(4002, 'replaced');
      }
    }

    // Set organ identity on this connection
    const state = clients.get(ws);
    if (state) {
      state.organName = organ_name;
    }

    // Update mailbox status
    adapter.updateMailboxStatus(organ_name, 'active');

    // Relay 6: Organ connect sequence — state transition + Vigil notification
    executeConnectSequence(organ_name);

    // Relay 4: Get active subscriptions from persistent registry
    const activeSubscriptions = getOrganSubscriptions(organ_name);

    const depth = adapter.getMailboxDepth(organ_name);

    safeSend(ws, {
      action: 'registered',
      organ_name,
      mailbox_depth: depth,
      active_subscriptions: activeSubscriptions,
    });

    log('ws_organ_registered', {
      organ: organ_name,
      mailbox_depth: depth,
      active_subscriptions: activeSubscriptions.length,
    });

    // Relay 4: Reconnection recovery — push mailbox backlog
    if (depth > 0) {
      pushBacklog(ws, organ_name);
    }
  }

  /**
   * Push all pending mailbox messages to a newly connected organ.
   * Messages are delivered in FIFO order and marked as delivered.
   */
  function pushBacklog(ws, organName) {
    const pending = adapter.getPendingMessages(organName);
    let pushed = 0;

    for (const row of pending) {
      const envelope = JSON.parse(row.envelope);
      const sent = safeSend(ws, { action: 'message', envelope });
      if (sent) {
        adapter.markMessageDelivered(row.message_id);
        pushed++;
      }
    }

    if (pushed > 0) {
      log('ws_backlog_pushed', { organ: organName, messages: pushed });
    }
  }

  /**
   * Get an organ's active broadcast subscriptions as filter objects.
   */
  function getOrganSubscriptions(organId) {
    const rows = adapter.getSubscriptions(organId);
    return rows.map(r => ({
      filter: r.topic_filter ? JSON.parse(r.topic_filter) : {},
      registered_at: r.registered_at,
    }));
  }

  function handleSubscribe(ws, data) {
    const state = clients.get(ws);
    if (!state || !state.organName) {
      safeSend(ws, { action: 'error', message: 'Must register before subscribing' });
      return;
    }

    const organName = state.organName;
    const filter = data.filter || {};
    const filterKey = JSON.stringify(filter);

    // Persist via adapter
    adapter.addSubscription(organName, filterKey);

    // Update in-memory cache
    if (!subscriptionCache.has(organName)) {
      subscriptionCache.set(organName, []);
    }
    const filters = subscriptionCache.get(organName);
    // Avoid duplicates in cache
    const exists = filters.some(f => JSON.stringify(f) === filterKey);
    if (!exists) {
      filters.push(filter);
    }

    safeSend(ws, {
      action: 'subscribed',
      organ_name: organName,
      filter,
      persistent: true,
    });

    log('ws_organ_subscribed', { organ: organName, filter });
  }

  function handleUnsubscribe(ws, data) {
    const state = clients.get(ws);
    if (!state || !state.organName) {
      safeSend(ws, { action: 'error', message: 'Must register before unsubscribing' });
      return;
    }

    const organName = state.organName;
    const filter = data.filter || {};
    const filterKey = JSON.stringify(filter);

    // Remove via adapter
    adapter.removeSubscription(organName, filterKey);

    // Update in-memory cache
    if (subscriptionCache.has(organName)) {
      const filters = subscriptionCache.get(organName);
      const idx = filters.findIndex(f => JSON.stringify(f) === filterKey);
      if (idx !== -1) {
        filters.splice(idx, 1);
      }
      if (filters.length === 0) {
        subscriptionCache.delete(organName);
      }
    }

    safeSend(ws, {
      action: 'unsubscribed',
      organ_name: organName,
      filter,
    });

    log('ws_organ_unsubscribed', { organ: organName, filter });
  }

  function handleAck(ws, data) {
    const state = clients.get(ws);
    if (!state || !state.organName) {
      safeSend(ws, { action: 'error', message: 'Must register before acking' });
      return;
    }

    const { message_ids } = data;
    if (!Array.isArray(message_ids) || message_ids.length === 0) {
      safeSend(ws, { action: 'error', message: 'message_ids array required' });
      return;
    }

    const acknowledged = adapter.ackMessages(message_ids);
    safeSend(ws, { action: 'acked', acknowledged });
  }

  function handleMessage(ws, raw) {
    let data;
    try {
      data = JSON.parse(raw);
    } catch {
      safeSend(ws, { action: 'error', message: 'Invalid JSON' });
      return;
    }

    switch (data.action) {
      case 'register':
        handleRegister(ws, data);
        break;
      case 'subscribe':
        handleSubscribe(ws, data);
        break;
      case 'unsubscribe':
        handleUnsubscribe(ws, data);
        break;
      case 'ack':
        handleAck(ws, data);
        break;
      default:
        safeSend(ws, { action: 'error', message: `Unknown action: ${data.action}` });
    }
  }

  // ================================================================
  // Public API
  // ================================================================

  /**
   * Relay 6: Late-bind health monitoring dependencies.
   * Must be called before attach().
   *
   * @param {object} deps
   * @param {object} deps.stateMachine  - State machine engine (createStateMachine result)
   * @param {function} deps.emitMessage - function(envelope) that routes + persists an event
   * @param {object} deps.healthConfig  - Health monitoring configuration from config.js
   */
  function setHealthDependencies(deps) {
    healthDeps = deps;
  }

  function attach(httpServer) {
    loadSubscriptionCache();
    wss = new WebSocketServer({ server: httpServer, path: '/subscribe' });

    wss.on('connection', (ws) => {
      clients.set(ws, {
        organName: null,
        connectedAt: new Date().toISOString(),
        missedPongs: 0,
        lastPongAt: null,
      });

      ws.on('pong', () => {
        const state = clients.get(ws);
        if (state) {
          const wasDegraded = heartbeat.handlePong(state);
          // Relay 6: recover from DEGRADED -> ALIVE
          if (wasDegraded && state.organName) {
            handleOrganRecovered(state.organName);
          }
        }
      });

      ws.on('message', (raw) => handleMessage(ws, raw.toString()));

      ws.on('close', () => {
        const state = clients.get(ws);
        if (state && state.organName) {
          executeDisconnectSequence(state.organName, ws, 'clean_disconnect');
        } else {
          clients.delete(ws);
        }
      });

      ws.on('error', (err) => {
        log('ws_client_error', { error: err.message });
        const state = clients.get(ws);
        if (state && state.organName) {
          executeDisconnectSequence(state.organName, ws, 'error');
        } else {
          clients.delete(ws);
        }
      });
    });

    // Relay 6: Create heartbeat monitor with configurable intervals
    const hbConfig = healthDeps?.healthConfig || {};
    heartbeat = createHeartbeatMonitor({
      pingIntervalMs: hbConfig.pingIntervalMs || 30_000,
      maxMissedPongs: hbConfig.maxMissedPongs || 3,
      healthHeartbeatMs: hbConfig.healthHeartbeatMs || 60_000,
    });

    // Start heartbeat ping cycle with lifecycle callbacks
    heartbeat.startPingCycle(clients, {
      onDegraded: handleOrganDegraded,
      onDisconnect: (organName, ws) => executeDisconnectSequence(organName, ws, 'heartbeat_timeout'),
    });

    // Start health self-check cycle (includes TTL sweep)
    heartbeat.startHealthCheck(performHealthCheck);

    log('ws_server_attached', { path: '/subscribe' });
  }

  function pushToOrgan(organName, envelope) {
    if (!wss) return false;
    for (const [ws, state] of clients) {
      if (state.organName === organName && ws.readyState === 1) {
        return safeSend(ws, { action: 'message', envelope });
      }
    }
    return false;
  }

  function isOrganConnected(organName) {
    for (const [, state] of clients) {
      if (state.organName === organName) return true;
    }
    return false;
  }

  function getClientCount() {
    return clients.size;
  }

  /**
   * Get consumer status for all connected organs.
   */
  function getConsumers() {
    const consumers = [];
    for (const [, state] of clients) {
      if (state.organName) {
        const subs = subscriptionCache.get(state.organName) || [];
        consumers.push({
          organ_name: state.organName,
          connected_at: state.connectedAt,
          subscriptions: subs.length,
          status: 'connected',
        });
      }
    }
    return consumers;
  }

  function cleanup() {
    // Stop heartbeat timers
    if (heartbeat) heartbeat.stop();

    // Clear reconnection timers
    for (const timer of reconnectionTimers.values()) {
      clearTimeout(timer);
    }
    reconnectionTimers.clear();

    if (wss) {
      for (const [ws] of clients) {
        ws.terminate();
      }
      clients.clear();
      wss.close();
      wss = null;
    }
  }

  return {
    attach,
    pushToOrgan,
    isOrganConnected,
    getClientCount,
    getConsumers,
    getSubscriptionCache,
    getOrganSubscriptions,
    loadSubscriptionCache,
    cleanup,
    setHealthDependencies,
  };
}
