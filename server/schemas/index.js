/**
 * Schema registry — exports all message type schemas and envelope definition.
 *
 * These are code-level definitions, not database entries. The 5 message type
 * schemas are compiled into the validation middleware. They are not
 * user-registerable at runtime. Runtime domain event schemas (OTM subtypes)
 * are a Relay 5 concern.
 */

import { otmSchema } from './otm.js';
import { apmSchema } from './apm.js';
import { pemSchema } from './pem.js';
import { atmSchema } from './atm.js';
import { homSchema } from './hom.js';
import { VALID_MESSAGE_TYPES, envelopeSchema } from './envelope.js';

export const schemas = {
  OTM: otmSchema,
  APM: apmSchema,
  PEM: pemSchema,
  ATM: atmSchema,
  HOM: homSchema,
};

export { VALID_MESSAGE_TYPES, envelopeSchema };
