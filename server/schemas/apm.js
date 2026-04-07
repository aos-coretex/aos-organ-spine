/**
 * APM (Action Proposal Message) — v1
 *
 * Write-lane authorization request. Only Thalamus produces. Only Nomos consumes.
 * Spine validates structure only — not whether source_organ is actually Thalamus
 * (that is policy enforcement, not transport validation).
 */

export const apmSchema = {
  type: 'APM',
  version: 1,
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
