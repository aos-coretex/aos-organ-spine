/**
 * PEM (Policy Exception Message) — v1.0
 * Locked: 2026-04-12 (MP-16 relay v6t-8)
 * See: 01-Organs/20-Spine/message-schemas-v1.0.md
 *
 * Policy conflict escalation. Only Nomos produces. Only Senate consumes.
 * MSP_CONFLICT = Senate-resolvable; BOR_CONFLICT = requires human principal.
 */

export const pemSchema = {
  type: 'PEM',
  version: '1.0',
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
