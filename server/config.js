/**
 * Spine organ configuration.
 *
 * Ports: 3900 (SAAS) / 4000 (AOS)
 * Database: encapsulated at data/spine.db
 */

export const config = {
  port: parseInt(process.env.SPINE_PORT || '4000', 10),  // AOS default
  binding: '127.0.0.1',
  dbPath: process.env.SPINE_DB_PATH || './data/spine.db',

  // Relay 6: Health monitoring
  pingIntervalMs: 30_000,           // heartbeat ping every 30 seconds
  maxMissedPongs: 3,                // 3 misses = 90 seconds → DISCONNECTED
  healthHeartbeatMs: 60_000,        // Spine self-check + TTL sweep every 60 seconds
  defaultTtlSeconds: 3600,          // OTM mailbox TTL: 1 hour (governance types: null = never)
  mailboxPressureThreshold: 100,    // signal back-pressure above this depth
  criticalMissingThresholdMs: 5 * 60_000,  // HOM after required organ missing 5 minutes
  reconnectionTimeoutMs: 30_000,    // grace period before tracking missing required organ
};
