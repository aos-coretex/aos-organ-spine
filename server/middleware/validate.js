/**
 * Message schema validation middleware for POST /messages.
 *
 * Validates the envelope structure (type, source_organ, target_organ, payload)
 * then validates the payload against the type-specific schema.
 *
 * Validation sequence:
 *   1. Envelope validation — required fields, type constraints
 *   2. Payload validation — per-type field presence, types, enums
 *   3. Nested validation — object properties (e.g. ATM scope)
 *
 * Unknown payload fields are always permitted (forward-compatible).
 * Spine validates structure only, not business semantics.
 */

import { schemas, VALID_MESSAGE_TYPES } from '../schemas/index.js';

/**
 * Validate a single field value against its schema definition.
 * Recurses into nested properties for object types.
 *
 * @param {*} value - The field value to validate
 * @param {object} fieldDef - Schema definition for this field
 * @param {string} fieldPath - Dot-delimited path for error reporting
 * @returns {object[]} Array of violation objects
 */
function validateField(value, fieldDef, fieldPath) {
  const violations = [];

  // Required check
  if (value === undefined || value === null) {
    if (fieldDef.required) {
      violations.push({ field: fieldPath, rule: 'required', message: 'Field is required' });
    }
    return violations;
  }

  // Type checks
  switch (fieldDef.type) {
    case 'string':
      if (typeof value !== 'string') {
        violations.push({ field: fieldPath, rule: 'type', expected: 'string', actual: typeof value });
      }
      break;
    case 'array':
      if (!Array.isArray(value)) {
        violations.push({ field: fieldPath, rule: 'type', expected: 'array', actual: typeof value });
      }
      break;
    case 'object':
      if (typeof value !== 'object' || Array.isArray(value)) {
        violations.push({
          field: fieldPath,
          rule: 'type',
          expected: 'object',
          actual: Array.isArray(value) ? 'array' : typeof value,
        });
      }
      break;
    case 'number':
      if (typeof value !== 'number') {
        violations.push({ field: fieldPath, rule: 'type', expected: 'number', actual: typeof value });
      }
      break;
  }

  // Enum check (only if type is correct — no violations yet)
  if (fieldDef.enum && violations.length === 0 && !fieldDef.enum.includes(value)) {
    violations.push({ field: fieldPath, rule: 'enum', expected: fieldDef.enum, actual: value });
  }

  // Nested properties (object types with sub-schema, e.g. ATM scope)
  if (
    fieldDef.properties &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    value !== null
  ) {
    for (const [propName, propDef] of Object.entries(fieldDef.properties)) {
      violations.push(...validateField(value[propName], propDef, `${fieldPath}.${propName}`));
    }
  }

  return violations;
}

/**
 * Express middleware factory. Returns middleware that validates
 * POST /messages bodies against envelope + per-type payload schemas.
 */
export function createValidationMiddleware() {
  return (req, res, next) => {
    const body = req.body;
    const envelopeViolations = [];

    // --- 1. Envelope validation ---

    // type: required, must be one of 5 valid values
    if (body.type === undefined || body.type === null) {
      envelopeViolations.push({ field: 'type', rule: 'required', message: 'Field is required' });
    } else if (typeof body.type !== 'string') {
      envelopeViolations.push({ field: 'type', rule: 'type', expected: 'string', actual: typeof body.type });
    } else if (!VALID_MESSAGE_TYPES.includes(body.type)) {
      envelopeViolations.push({ field: 'type', rule: 'enum', expected: VALID_MESSAGE_TYPES, actual: body.type });
    }

    // source_organ: required, non-empty string
    if (body.source_organ === undefined || body.source_organ === null) {
      envelopeViolations.push({ field: 'source_organ', rule: 'required', message: 'Field is required' });
    } else if (typeof body.source_organ !== 'string' || body.source_organ.length === 0) {
      envelopeViolations.push({ field: 'source_organ', rule: 'type', expected: 'non-empty string', actual: body.source_organ });
    }

    // target_organ: required, non-empty string ("*" is valid for broadcast)
    if (body.target_organ === undefined || body.target_organ === null) {
      envelopeViolations.push({ field: 'target_organ', rule: 'required', message: 'Field is required' });
    } else if (typeof body.target_organ !== 'string' || body.target_organ.length === 0) {
      envelopeViolations.push({ field: 'target_organ', rule: 'type', expected: 'non-empty string', actual: body.target_organ });
    }

    // payload: required, must be a JSON object (not null, not array, not scalar)
    if (body.payload === undefined || body.payload === null) {
      envelopeViolations.push({ field: 'payload', rule: 'required', message: 'Field is required' });
    } else if (typeof body.payload !== 'object' || Array.isArray(body.payload)) {
      envelopeViolations.push({
        field: 'payload',
        rule: 'type',
        expected: 'object',
        actual: Array.isArray(body.payload) ? 'array' : typeof body.payload,
      });
    }

    // correlation_id: optional, but if present must be non-empty string
    if (body.correlation_id !== undefined && body.correlation_id !== null) {
      if (typeof body.correlation_id !== 'string' || body.correlation_id.length === 0) {
        envelopeViolations.push({ field: 'correlation_id', rule: 'type', expected: 'non-empty string', actual: body.correlation_id });
      }
    }

    // reply_to: optional, but if present must be non-empty string
    if (body.reply_to !== undefined && body.reply_to !== null) {
      if (typeof body.reply_to !== 'string' || body.reply_to.length === 0) {
        envelopeViolations.push({ field: 'reply_to', rule: 'type', expected: 'non-empty string', actual: body.reply_to });
      }
    }

    // Fail fast on envelope errors — can't validate payload without valid type
    if (envelopeViolations.length > 0) {
      return res.status(400).json({
        error: 'SCHEMA_VALIDATION_FAILED',
        type: body.type || null,
        schema_version: 1,
        violations: envelopeViolations,
      });
    }

    // --- 2. Payload validation against type-specific schema ---

    const schema = schemas[body.type];
    const payloadViolations = [];

    for (const [fieldName, fieldDef] of Object.entries(schema.fields)) {
      payloadViolations.push(...validateField(body.payload[fieldName], fieldDef, fieldName));
    }

    if (payloadViolations.length > 0) {
      return res.status(400).json({
        error: 'SCHEMA_VALIDATION_FAILED',
        type: body.type,
        schema_version: schema.version,
        violations: payloadViolations,
      });
    }

    // --- 3. Valid — proceed to route handler ---
    next();
  };
}
