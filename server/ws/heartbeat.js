/**
 * Heartbeat monitor for the Spine ESB organ.
 *
 * Extracted from handler.js (Relay 2) and formalized with:
 * - Per-client ping/pong cycle with configurable intervals
 * - Degraded state detection (1 missed pong)
 * - Dead-producer detection (maxMissedPongs missed pongs)
 * - Spine self-health check timer (aligned with TTL expiry sweep)
 *
 * This module handles timing and detection only. Actions (state transitions,
 * Vigil notifications, broadcast emissions) are delegated to the caller
 * via callbacks.
 *
 * Relay 6.
 */

function log(event, data = {}) {
  const entry = { timestamp: new Date().toISOString(), event, ...data };
  process.stdout.write(JSON.stringify(entry) + '\n');
}

/**
 * @param {object} config
 * @param {number} config.pingIntervalMs  - Heartbeat ping interval (default 30s)
 * @param {number} config.maxMissedPongs  - Missed pongs before disconnect (default 3)
 * @param {number} config.healthHeartbeatMs - Self-health check interval (default 60s)
 */
export function createHeartbeatMonitor({
  pingIntervalMs = 30_000,
  maxMissedPongs = 3,
  healthHeartbeatMs = 60_000,
} = {}) {
  let pingTimer = null;
  let healthTimer = null;

  /**
   * Start the per-client heartbeat cycle.
   *
   * Every pingIntervalMs, for each connected WebSocket client:
   *   missedPongs >= maxMissedPongs  -> onDisconnect callback
   *   missedPongs reaches 2          -> onDegraded callback (sustained miss)
   *   Otherwise                      -> increment + send ping frame
   *
   * Phase-2 wedge-fix (2026-05-09 spine-wedge-fix-phase2-relay-prompt-body.md):
   * The DEGRADED threshold was previously `=== 1` which triggered onDegraded
   * for every healthy organ on every ping tick (BEFORE the pong-timeout window
   * elapsed). Healthy organs that responded to every ping experienced
   * defensive-bookkeeping round-trip (DEGRADED→ALIVE) every 30s, generating
   * 56 spurious state_transitions rows per tick (28 organs × 2 transitions),
   * which both wedged the event-loop on lifecycle-callback sync DB writes
   * (28 × 6 ops × ~30ms = 5040ms ≈ 5s observed stall) AND amplified the
   * Phase-1 SCAN-TABLE class via cumulative spurious rows.
   *
   * Threshold raised to `=== 2` so DEGRADED only fires after SECOND consecutive
   * missed ping (one missed ping is normal jitter; sustained miss signals
   * degradation). maxMissedPongs (default 3) still triggers disconnect at
   * 90s without pong; unchanged.
   *
   * Option B `degradedNotified` boolean (preferred over A: more robust to
   * future threshold changes): set when onDegraded fires; reset on pong;
   * handlePong returns wasDegraded based on this explicit signal rather
   * than missedPongs counter inference.
   *
   * @param {Map} clients - Map<WebSocket, { organName, connectedAt, missedPongs, lastPongAt, degradedNotified }>
   * @param {object} callbacks
   * @param {function} callbacks.onDegraded    - (organName) organ entered warning state
   * @param {function} callbacks.onDisconnect  - (organName, ws) organ is dead
   */
  function startPingCycle(clients, callbacks) {
    pingTimer = setInterval(() => {
      for (const [ws, state] of clients) {
        if (state.missedPongs >= maxMissedPongs) {
          log('heartbeat_timeout', { organ: state.organName, missed: state.missedPongs });
          callbacks.onDisconnect(state.organName, ws);
          continue;
        }

        state.missedPongs++;

        // Phase-2 wedge-fix: DEGRADED only on SECOND consecutive missed ping
        // (one missed ping is normal jitter; sustained miss signals degradation).
        if (state.missedPongs === 2 && state.organName) {
          callbacks.onDegraded(state.organName);
          state.degradedNotified = true;
        }

        try { ws.ping(); } catch { /* ignore send errors on dead sockets */ }
      }
    }, pingIntervalMs);
  }

  /**
   * Handle pong received from a client.
   * Updates lastPongAt and resets missedPongs.
   *
   * Phase-2 wedge-fix: wasDegraded is now driven by the explicit
   * `degradedNotified` boolean (Option B), not the missedPongs counter.
   * This decouples the recovery-signal from the threshold counter so future
   * threshold tuning doesn't break the recovery semantics. Healthy organs
   * (missedPongs 0→1→0 with no degraded firing) return wasDegraded=false,
   * eliminating the spurious onRecovered round-trip.
   *
   * @param {object} state - Client state object from the clients Map
   * @returns {boolean} true if onDegraded was called (recovery is meaningful)
   */
  function handlePong(state) {
    const wasDegraded = !!state.degradedNotified;
    state.missedPongs = 0;
    state.degradedNotified = false;
    state.lastPongAt = new Date().toISOString();
    return wasDegraded;
  }

  /**
   * Start the Spine self-health check cycle.
   *
   * The callback runs every healthHeartbeatMs and should:
   *   1. Check SQLite connectivity
   *   2. Count connected organs and total mailbox depth
   *   3. Run TTL expiry sweep
   *   4. Check critical missing required organs
   *   5. Emit spine_health OTM broadcast
   *
   * @param {function} healthCheckFn - Called on each cycle
   */
  function startHealthCheck(healthCheckFn) {
    healthTimer = setInterval(healthCheckFn, healthHeartbeatMs);
  }

  /** Stop all timers (ping cycle + health check). */
  function stop() {
    if (pingTimer) { clearInterval(pingTimer); pingTimer = null; }
    if (healthTimer) { clearInterval(healthTimer); healthTimer = null; }
  }

  return {
    startPingCycle,
    handlePong,
    startHealthCheck,
    stop,
    pingIntervalMs,
    maxMissedPongs,
    healthHeartbeatMs,
  };
}
