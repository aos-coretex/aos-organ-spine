/**
 * HOM (Human Oversight Message) — v1.0
 * Locked: 2026-04-12 (MP-16 relay v6t-8)
 * See: 01-Organs/20-Spine/message-schemas-v1.0.md
 *
 * Governance decisions requiring human authority. Only Arbiter produces.
 * The human principal is addressed via target_organ — how this maps to
 * a human interface is a Receptor/Axon concern.
 */

export const homSchema = {
  type: 'HOM',
  version: '1.0',
  description: 'Human Oversight Message — governance decisions requiring human authority',
  fields: {
    decision_type: { type: 'string', required: true, enum: ['bor_ambiguity', 'amendment_proposal', 'scope_clarification'] },
    context: { type: 'string', required: true },
    question: { type: 'string', required: true },
    options: { type: 'array', required: true },
    deadline: { type: 'string', required: false },
  },
};
