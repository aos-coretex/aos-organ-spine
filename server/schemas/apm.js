/**
 * APM (Action Proposal Message) — v1.0
 * Locked: 2026-04-12 (MP-16 relay v6t-8)
 * See: 01-Organs/20-Spine/message-schemas-v1.0.md
 *
 * Write-lane authorization request. Only Thalamus produces. Only Nomos consumes.
 * Spine validates structure only — not whether source_organ is actually Thalamus
 * (that is policy enforcement, not transport validation).
 */

export const apmSchema = {
  type: 'APM',
  version: '1.0',
  description: 'Action Proposal Message — write-lane authorization request',
  fields: {
    action: { type: 'string', required: true },
    targets: { type: 'array', required: true },
    risk_tier: { type: 'string', required: true, enum: ['low', 'medium', 'high', 'critical'] },
    evidence_refs: { type: 'array', required: true },
    rollback_plan: { type: 'string', required: true },
    reason: { type: 'string', required: true },
  },
};
