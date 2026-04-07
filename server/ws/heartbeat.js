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
   *   missedPongs incremented to 1   -> onDegraded callback
   *   Otherwise                      -> increment + send ping frame
   *
   * @param {Map} clients - Map<WebSocket, { organName, connectedAt, missedPongs, lastPongAt }>
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

        // Degraded detection at first missed pong
        if (state.missedPongs === 1 && state.organName) {
          callbacks.onDegraded(state.organName);
        }

        try { ws.ping(); } catch { /* ignore send errors on dead sockets */ }
      }
    }, pingIntervalMs);
  }

  /**
   * Handle pong received from a client.
   * Updates lastPongAt and resets missedPongs.
   *
   * @param {object} state - Client state object from the clients Map
   * @returns {boolean} true if the client had missed pongs (was degraded/recovering)
   */
  function handlePong(state) {
    const wasDegraded = state.missedPongs > 0;
    state.missedPongs = 0;
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
