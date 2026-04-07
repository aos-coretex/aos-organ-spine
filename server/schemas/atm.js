/**
 * ATM (Authorization Token Message) — v1
 *
 * Carries scoped authorization tokens. Only Nomos produces.
 * Thalamus routes to Cerberus. token_urn is a Graphheight URN
 * identifying the authorization token (minted by Graphheight 511).
 */

export const atmSchema = {
  type: 'ATM',
  version: 1,
  description: 'Authorization Token Message — scoped authorization tokens',
  fields: {
    token_urn: { type: 'string', required: true },
    scope: {
      type: 'object',
      required: true,
      properties: {
        targets: { type: 'array', required: true },
        action_types: { type: 'array', required: true },
        ttl_seconds: { type: 'number', required: true },
        conditions: { type: 'array', required: false },
      },
    },
    ap_ref: { type: 'string', required: true },
  },
};
