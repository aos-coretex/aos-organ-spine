/**
 * POST /messages — primary message acceptance endpoint for the ESB Spine.
 *
 * Accepts typed envelopes with routing fields, assigns message_id (URN)
 * and timestamp, routes via the routing engine, persists audit event.
 *
 * Pipeline (after Relay 6):
 *   Request -> Logging middleware
 *           -> Envelope validation (Relay 3)
 *           -> Payload schema validation (Relay 3)
 *           -> Delivery guarantee check (Relay 4)
 *           -> TTL resolution (Relay 6)
 *           -> Routing engine (Relay 4) with TTL
 *             +-- Directed: validate manifest -> persist in mailbox (with TTL) -> push if connected
 *             +-- Broadcast (OTM only): match subscription cache -> push to matching
 *           -> Persist event (Relay 5 — audit trail)
 *           -> Back-pressure detection (Relay 6)
 *           -> Response (202 Accepted)
 *
 * All storage operations go through the StorageAdapter.
 */

import { Router } from 'express';
import { generateUrn } from '../../lib/urn.js';
import { createValidationMiddleware } from '../middleware/validate.js';
import { validateDeliveryGuarantee, routeMessage } from '../routing/router.js';

// Governance types: never expire (NULL TTL)
const GOVERNANCE_TYPES = ['APM', 'PEM', 'ATM', 'HOM'];

function log(event, data = {}) {
  const entry = { timestamp: new Date().toISOString(), event, ...data };
  process.stdout.write(JSON.stringify(entry) + '\n');
}

export function createMessagesRouter(adapter, wsHandler, manifest, healthConfig = {}) {
  const router = Router();
  const { defaultTtlSeconds = 3600, mailboxPressureThreshold = 100 } = healthConfig;

  // Validation middleware intercepts before the route handler
  const validateMessage = createValidationMiddleware();

  router.post('/messages', validateMessage, (req, res) => {
    const { source_organ, target_organ, payload, type, correlation_id, reply_to } = req.body;

    // 1. Generate message_id with type-specific URN namespace
    const message_id = generateUrn(type.toLowerCase());

    // 2. Assign timestamp
    const timestamp = new Date().toISOString();

    // 3. Set reply_to default
    const resolvedReplyTo = reply_to || source_organ;

    // 4. Construct full envelope
    const envelope = {
      type,
      source_organ,
      target_organ,
      message_id,
      correlation_id: correlation_id || null,
      reply_to: resolvedReplyTo,
      timestamp,
      payload,
    };

    // 5. Delivery guarantee check — governance messages cannot be broadcast
    const guaranteeError = validateDeliveryGuarantee(envelope);
    if (guaranteeError) {
      return res.status(400).json(guaranteeError);
    }

    // 6. Relay 6: Resolve TTL for mailbox persistence
    let ttlSeconds = null;
    if (GOVERNANCE_TYPES.includes(type)) {
      // Governance messages never expire — too important to lose
      ttlSeconds = null;
    } else {
      // Sender can override via request body; otherwise use configured default
      ttlSeconds = req.body.ttl_seconds !== undefined && req.body.ttl_seconds !== null
        ? req.body.ttl_seconds
        : defaultTtlSeconds;
    }

    // 7. Route via routing engine (Relay 6: TTL threaded through)
    const result = routeMessage(envelope, {
      manifest,
      subscriptionCache: wsHandler.getSubscriptionCache(),
      pushToOrgan: wsHandler.pushToOrgan,
      isOrganConnected: wsHandler.isOrganConnected,
      adapter,
    }, { ttlSeconds });

    // Check for routing errors
    if (result.error) {
      return res.status(400).json(result);
    }

    // 8. Persist event in audit trail (Relay 5)
    adapter.persistEvent(envelope, result.routing);

    log('message_accepted', {
      message_id,
      routing: result.routing,
      delivered_to: result.delivered_to,
    });

    // 9. Relay 6: Back-pressure detection for directed messages
    if (result.routing === 'directed') {
      const depth = adapter.getMailboxDepth(target_organ);
      if (depth > mailboxPressureThreshold) {
        res.set('X-Mailbox-Pressure', 'true');

        // Emit mailbox_pressure OTM broadcast (non-blocking signal)
        const pressureEnvelope = {
          type: 'OTM',
          source_organ: 'Spine',
          target_organ: '*',
          message_id: generateUrn('otm'),
          correlation_id: null,
          reply_to: 'Spine',
          timestamp: new Date().toISOString(),
          payload: {
            event_type: 'mailbox_pressure',
            source: 'spine-health',
            data: {
              organ_name: target_organ,
              depth,
              threshold: mailboxPressureThreshold,
              organ_status: wsHandler.isOrganConnected(target_organ) ? 'connected' : 'disconnected',
            },
          },
        };

        routeMessage(pressureEnvelope, {
          manifest,
          subscriptionCache: wsHandler.getSubscriptionCache(),
          pushToOrgan: wsHandler.pushToOrgan,
          isOrganConnected: wsHandler.isOrganConnected,
          adapter,
        });
        adapter.persistEvent(pressureEnvelope, 'broadcast');
      }
    }

    // 10. Response
    res.status(202).json({
      message_id,
      timestamp,
      status: 'accepted',
      routing: result.routing,
      ...(result.routing === 'directed'
        ? { target_organ }
        : { delivered_to: result.delivered_to }),
    });
  });

  return router;
}
