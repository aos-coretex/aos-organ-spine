/**
 * OTM (Operating Technical Message) — v1.0
 * Locked: 2026-04-12 (MP-16 relay v6t-8)
 * See: 01-Organs/20-Spine/message-schemas-v1.0.md
 *
 * The routine heartbeat of the DIO. All technical inter-organ communication.
 * OTM is the only type where payload structure varies widely — domain schemas
 * for specific event_type values are registered at runtime (Relay 5).
 */

export const otmSchema = {
  type: 'OTM',
  version: '1.0',
  description: 'Operating Technical Message — routine technical inter-organ communication',
  fields: {
    event_type: { type: 'string', required: true },
    source: { type: 'string', required: false },
    data: { type: 'object', required: false },
  },
};
