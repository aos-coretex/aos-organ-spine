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
import { VALID_MESSAGE_TYPES, envelopeSchema, ENVELOPE_VERSION } from './envelope.js';

export const SCHEMA_VERSION = '1.0';
export const SCHEMA_LOCK_DATE = '2026-04-12';

export const schemas = {
  OTM: otmSchema,
  APM: apmSchema,
  PEM: pemSchema,
  ATM: atmSchema,
  HOM: homSchema,
};

export { VALID_MESSAGE_TYPES, envelopeSchema, ENVELOPE_VERSION };
