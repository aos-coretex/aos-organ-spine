/**
 * Common envelope schema definition — v1.0
 * Locked: 2026-04-12 (MP-16 relay v6t-8)
 *
 * Every message on the ESB Spine MUST conform to this envelope.
 * Spine assigns message_id and timestamp — sender-provided values
 * are ignored and overwritten.
 *
 * Future changes require formal amendment via PEM (Senate governance).
 * See: 01-Organs/20-Spine/message-schemas-v1.0.md
 */

export const ENVELOPE_VERSION = '1.0';
export const VALID_MESSAGE_TYPES = ['OTM', 'APM', 'PEM', 'ATM', 'HOM'];

export const envelopeSchema = {
  type: { type: 'string', required: true, enum: VALID_MESSAGE_TYPES },
  source_organ: { type: 'string', required: true },
  target_organ: { type: 'string', required: true },
  payload: { type: 'object', required: true },
  correlation_id: { type: 'string', required: false },
  reply_to: { type: 'string', required: false },
};
