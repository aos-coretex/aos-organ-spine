/**
 * PEM (Policy Exception Message) — v1
 *
 * Policy conflict escalation. Only Nomos produces. Only Senate consumes.
 * MSP_CONFLICT = Senate-resolvable; BOR_CONFLICT = requires human principal.
 */

export const pemSchema = {
  type: 'PEM',
  version: 1,
  description: 'Policy Exception Message — policy conflict escalation',
  fields: {
    conflict_class: { type: 'string', required: true, enum: ['MSP_CONFLICT', 'BOR_CONFLICT'] },
    blocked_action: { type: 'string', required: true },
    blocking_rules: { type: 'array', required: true },
    necessity: { type: 'string', required: true },
    proposed_change: { type: 'string', required: true },
    risk_assessment: { type: 'string', required: true },
  },
};
