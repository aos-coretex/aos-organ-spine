/**
 * HOM (Human Oversight Message) — v1
 *
 * Governance decisions requiring human authority. Only Arbiter produces.
 * The human principal is addressed via target_organ — how this maps to
 * a human interface is a Receptor/Axon concern.
 */

export const homSchema = {
  type: 'HOM',
  version: 1,
  description: 'Human Oversight Message — governance decisions requiring human authority',
  fields: {
    decision_type: { type: 'string', required: true, enum: ['bor_ambiguity', 'amendment_proposal', 'scope_clarification'] },
    context: { type: 'string', required: true },
    question: { type: 'string', required: true },
    options: { type: 'array', required: true },
    deadline: { type: 'string', required: false },
  },
};
