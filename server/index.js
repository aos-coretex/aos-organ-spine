/**
 * Spine Organ — ESB Communication Bus & State Machine
 *
 * Main entry: Express server initialization, route mounting,
 * WebSocket handler, manifest, routing engine, graceful shutdown.
 *
 * Port: SPINE_PORT (default 4000 for AOS, 3900 for SAAS)
 * DB:   SPINE_DB_PATH (default ./data/spine.db)
 *
 * All storage access goes through the StorageAdapter returned by initDatabase().
 */

import express from 'express';
import { config } from './config.js';
import { initDatabase } from './db/init.js';
import { createManifest } from './manifest/manifest.js';
import { createStateMachine } from './state/machine.js';
import { createStateRouter } from './routes/state.js';
import { createHealthRouter } from './routes/health.js';
import { createMessagesRouter } from './routes/messages.js';
import { createMailboxRouter } from './routes/mailbox.js';
import { createSchemasRouter } from './routes/schemas.js';
import { createEventsRouter } from './routes/events.js';
import { createWebSocketHandler } from './ws/handler.js';
import { routeMessage } from './routing/router.js';
import { generateUrn } from '../lib/urn.js';
import { loggingMiddleware } from './middleware/logging.js';

function log(event, data = {}) {
  const entry = { timestamp: new Date().toISOString(), event, ...data };
  process.stdout.write(JSON.stringify(entry) + '\n');
}

// Initialize database — returns the StorageAdapter (not the raw db)
const adapter = initDatabase();

// Initialize organ manifest (Relay 4)
const manifest = createManifest(adapter);

// Initialize WebSocket handler with adapter and manifest (Relay 4: manifest validation)
const wsHandler = createWebSocketHandler(adapter, manifest);

// Initialize state machine engine with transition callback (Relay 4)
const stateMachine = createStateMachine(adapter, {
  onTransition(result) {
    const envelope = {
      type: 'OTM',
      source_organ: 'Spine',
      target_organ: '*',
      message_id: generateUrn('otm'),
      correlation_id: null,
      reply_to: 'Spine',
      timestamp: new Date().toISOString(),
      payload: {
        event_type: 'state_transition',
        source: 'spine-state',
        data: {
          entity_urn: result.entity_urn,
          previous_state: result.previous_state,
          current_state: result.current_state,
          transition_id: result.transition_id,
          actor: result.actor,
          reason: result.reason,
        },
      },
    };

    routeMessage(envelope, {
      manifest,
      subscriptionCache: wsHandler.getSubscriptionCache(),
      pushToOrgan: wsHandler.pushToOrgan,
      isOrganConnected: wsHandler.isOrganConnected,
      adapter,
    });

    // Persist the state_transition OTM as an event (audit trail)
    adapter.persistEvent(envelope, 'broadcast');
  },
});

// Relay 6: Create message emission function for health monitoring.
// Routes the message through the routing engine and persists in audit trail.
// Used by the handler for Vigil OTMs, spine_health OTMs, and HOMs.
function emitMessage(envelope) {
  const result = routeMessage(envelope, {
    manifest,
    subscriptionCache: wsHandler.getSubscriptionCache(),
    pushToOrgan: wsHandler.pushToOrgan,
    isOrganConnected: wsHandler.isOrganConnected,
    adapter,
  });

  // Always persist in audit trail, even if routing failed (e.g., HOM to disconnected human)
  const routing = envelope.target_organ === '*' ? 'broadcast' : 'directed';
  adapter.persistEvent(envelope, routing);

  if (result.error) {
    log('emit_message_routing_failed', {
      message_id: envelope.message_id,
      target_organ: envelope.target_organ,
      error: result.error,
    });
  }

  return result;
}

// Phase-2 Strand 3 (binding-rule #40): async parallel of emitMessage that
// uses adapter.persistEventAsync (worker-thread offload) for the audit-trail
// persist. Routing remains in-memory sync (no DB ops). Used by lifecycle
// callbacks to keep DB writes off the event-loop main thread.
// Per EA Verdict 3 ratification 2026-05-09 1428 R.
async function emitMessageAsync(envelope) {
  const result = routeMessage(envelope, {
    manifest,
    subscriptionCache: wsHandler.getSubscriptionCache(),
    pushToOrgan: wsHandler.pushToOrgan,
    isOrganConnected: wsHandler.isOrganConnected,
    adapter,
  });

  const routing = envelope.target_organ === '*' ? 'broadcast' : 'directed';
  await adapter.persistEventAsync(envelope, routing);

  if (result.error) {
    log('emit_message_routing_failed', {
      message_id: envelope.message_id,
      target_organ: envelope.target_organ,
      error: result.error,
    });
  }

  return result;
}

// Relay 6: Wire health monitoring dependencies (late binding to resolve circular deps)
const healthConfig = {
  pingIntervalMs: config.pingIntervalMs,
  maxMissedPongs: config.maxMissedPongs,
  healthHeartbeatMs: config.healthHeartbeatMs,
  defaultTtlSeconds: config.defaultTtlSeconds,
  mailboxPressureThreshold: config.mailboxPressureThreshold,
  criticalMissingThresholdMs: config.criticalMissingThresholdMs,
  reconnectionTimeoutMs: config.reconnectionTimeoutMs,
};

wsHandler.setHealthDependencies({
  stateMachine,
  emitMessage,
  emitMessageAsync,
  adapter,  // expose adapter for direct *Async access in lifecycle callbacks
  healthConfig,
});

// Create Express app
const app = express();
app.use(express.json());
app.use(loggingMiddleware);

// Mount routes
app.use('/state', createStateRouter(stateMachine));
app.use('/', createMessagesRouter(adapter, wsHandler, manifest, healthConfig));
app.use('/mailbox', createMailboxRouter(adapter));
app.use('/', createSchemasRouter(adapter));
app.use('/', createEventsRouter(adapter));
app.use('/', createHealthRouter(adapter, config.dbPath, config.port, manifest, wsHandler, healthConfig));

const server = app.listen(config.port, config.binding, () => {
  log('spine_started', { port: config.port, db_path: config.dbPath });
  // Log manifest status at startup
  manifest.logStartupStatus(wsHandler.isOrganConnected);
});

// Attach WebSocket server to the HTTP server
wsHandler.attach(server);

// Graceful shutdown
function shutdown() {
  log('spine_shutdown');
  wsHandler.cleanup();
  server.close(() => {
    adapter.close();
    process.exit(0);
  });
  // Force exit after 5s if connections hang
  setTimeout(() => process.exit(1), 5000);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

export { app, server, adapter, stateMachine, wsHandler, manifest };
