/**
 * Common envelope schema definition.
 *
 * Every message on the ESB Spine MUST conform to this envelope.
 * Spine assigns message_id and timestamp — sender-provided values
 * are ignored and overwritten.
 */

export const VALID_MESSAGE_TYPES = ['OTM', 'APM', 'PEM', 'ATM', 'HOM'];

export const envelopeSchema = {
  type: { type: 'string', required: true, enum: VALID_MESSAGE_TYPES },
  source_organ: { type: 'string', required: true },
  target_organ: { type: 'string', required: true },
  payload: { type: 'object', required: true },
  correlation_id: { type: 'string', required: false },
  reply_to: { type: 'string', required: false },
};
