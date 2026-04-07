/**
 * Routing engine for the Spine ESB organ.
 *
 * Message type dictates the delivery pattern — the sender does not choose.
 * OTM: broadcast (target_organ = "*") or directed. All others: directed only.
 *
 * Broadcast: topic-based subscription filtering with fan-out.
 * Directed: manifest-validated mailbox persistence + WebSocket push.
 *
 * All storage operations go through the StorageAdapter.
 *
 * Relay 4.
 */

function log(event, data = {}) {
  const entry = { timestamp: new Date().toISOString(), event, ...data };
  process.stdout.write(JSON.stringify(entry) + '\n');
}

// Message types that require directed delivery (governance + human-critical)
const DIRECTED_ONLY_TYPES = ['APM', 'PEM', 'ATM', 'HOM'];

/**
 * Check if a message type requires directed delivery.
 */
export function requiresDirectedDelivery(type) {
  return DIRECTED_ONLY_TYPES.includes(type);
}

/**
 * Validate delivery guarantee constraints.
 * Returns null if valid, or an error object if violated.
 */
export function validateDeliveryGuarantee(envelope) {
  const { type, target_organ } = envelope;

  if (requiresDirectedDelivery(type)) {
    if (!target_organ || target_organ === '*') {
      return {
        error: 'GOVERNANCE_MESSAGE_REQUIRES_DIRECTED_DELIVERY',
        type,
        message: `Messages of type ${type} must have a specific target_organ. Broadcast is not permitted for governance messages.`,
      };
    }
  }

  return null;
}

/**
 * Match a message envelope against a subscription filter.
 *
 * AND semantics within a single filter: all specified fields must match.
 * Checks envelope-level fields first, then payload fields.
 *
 * @param {object} envelope - The full message envelope
 * @param {object} filter - The subscription filter object
 * @returns {boolean} true if all filter fields match
 */
export function matchesFilter(envelope, filter) {
  for (const [key, value] of Object.entries(filter)) {
    // Check envelope-level fields first
    if (key in envelope && envelope[key] === value) continue;
    // Then check payload fields
    if (envelope.payload && key in envelope.payload && envelope.payload[key] === value) continue;
    // No match on this field
    return false;
  }
  return true; // All filter fields matched (empty filter matches everything)
}

/**
 * Route a message through the ESB.
 *
 * @param {object} envelope - The validated message envelope
 * @param {object} deps - Dependencies
 * @param {object} deps.manifest - Manifest module
 * @param {object} deps.subscriptionCache - Map<organId, Array<filter>>
 * @param {function} deps.pushToOrgan - function(organId, envelope) => boolean
 * @param {function} deps.isOrganConnected - function(organId) => boolean
 * @param {import('../adapter/interface.js').StorageAdapter} deps.adapter - Storage adapter
 * @returns {object} { routing: "directed"|"broadcast", delivered_to: [...] }
 */
export function routeMessage(envelope, { manifest, subscriptionCache, pushToOrgan, isOrganConnected, adapter }, options = {}) {
  const { target_organ, type } = envelope;
  const { ttlSeconds = null } = options;

  // Defense-in-depth: double-check delivery guarantee (already enforced upstream)
  if (requiresDirectedDelivery(type) && (!target_organ || target_organ === '*')) {
    return {
      error: 'GOVERNANCE_MESSAGE_REQUIRES_DIRECTED_DELIVERY',
      type,
    };
  }

  if (target_organ !== '*') {
    return routeDirected(envelope, { manifest, pushToOrgan, adapter }, ttlSeconds);
  }

  // Broadcast: OTM only (other types are rejected upstream)
  return routeBroadcast(envelope, { subscriptionCache, pushToOrgan, isOrganConnected });
}

/**
 * Route a directed message to a specific organ.
 */
function routeDirected(envelope, { manifest, pushToOrgan, adapter }, ttlSeconds = null) {
  const { target_organ, source_organ, message_id } = envelope;

  // Validate target organ exists in manifest
  if (!manifest.isKnown(target_organ)) {
    return {
      error: 'ROUTING_FAILED',
      message: `Organ not in manifest: ${target_organ}`,
    };
  }

  // Push via WebSocket if connected, then persist in mailbox_messages
  const connected = pushToOrgan(target_organ, envelope);

  adapter.persistMailboxMessage(
    message_id,
    target_organ,
    source_organ,
    envelope,
    connected,
    ttlSeconds,
  );

  log('message_routed', {
    message_id,
    routing: 'directed',
    target_organ,
    ws_pushed: connected,
  });

  return {
    routing: 'directed',
    delivered_to: [target_organ],
  };
}

/**
 * Route a broadcast OTM to all matching subscribers.
 */
function routeBroadcast(envelope, { subscriptionCache, pushToOrgan, isOrganConnected }) {
  const deliveredTo = [];

  for (const [organId, filters] of subscriptionCache) {
    // Skip organs that aren't connected (broadcast is best-effort)
    if (!isOrganConnected(organId)) continue;

    // OR across filters: any matching filter qualifies
    const matches = filters.some(filter => matchesFilter(envelope, filter));

    if (matches) {
      const pushed = pushToOrgan(organId, envelope);
      if (pushed) {
        deliveredTo.push(organId);
      }
    }
  }

  log('message_routed', {
    message_id: envelope.message_id,
    routing: 'broadcast',
    subscriber_count: subscriptionCache.size,
    delivered_to: deliveredTo,
  });

  return {
    routing: 'broadcast',
    delivered_to: deliveredTo,
  };
}
