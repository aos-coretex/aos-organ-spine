/**
 * OTM (Operating Technical Message) — v1
 *
 * The routine heartbeat of the DIO. All technical inter-organ communication.
 * OTM is the only type where payload structure varies widely — domain schemas
 * for specific event_type values are registered at runtime (Relay 5).
 */

export const otmSchema = {
  type: 'OTM',
  version: 1,
  description: 'Operating Technical Message — routine technical inter-organ communication',
  fields: {
    event_type: { type: 'string', required: true },
    source: { type: 'string', required: false },
    data: { type: 'object', required: false },
  },
};
