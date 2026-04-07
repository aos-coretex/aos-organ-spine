/**
 * Schema query endpoints — expose current message type schemas for introspection.
 *
 * These return code-level schema definitions and the changelog from the database.
 * They do NOT allow runtime registration of message type schemas (those are fixed
 * in code). Runtime registration of OTM domain event schemas is also provided
 * (Relay 5).
 *
 * All storage operations go through the StorageAdapter.
 */

import { Router } from 'express';
import { schemas } from '../schemas/index.js';

export function createSchemasRouter(adapter) {
  const router = Router();

  // GET /schemas — list all 5 message type schemas with current version
  router.get('/schemas', (req, res) => {
    const schemaList = Object.values(schemas).map((s) => ({
      type: s.type,
      version: s.version,
      fields: s.fields,
      description: s.description,
    }));

    res.json({ schemas: schemaList });
  });

  // GET /schemas/changelog — full schema changelog from database
  // Must be defined before :message_type to avoid matching "changelog" as a param
  router.get('/schemas/changelog', (req, res) => {
    const entries = adapter.getChangelog();
    res.json({ entries });
  });

  // --- Relay 5: Domain event schema registry (OTM subtypes) ---

  // POST /schemas/events — register a domain event schema
  router.post('/schemas/events', (req, res) => {
    const { event_type, version, fields, description } = req.body;

    if (!event_type || version === undefined || !fields) {
      return res.status(400).json({
        error: 'MISSING_FIELDS',
        required: ['event_type', 'version', 'fields'],
      });
    }

    if (typeof version !== 'number' || version < 1 || !Number.isInteger(version)) {
      return res.status(400).json({
        error: 'INVALID_VERSION',
        message: 'version must be a positive integer',
      });
    }

    if (typeof fields !== 'object' || Array.isArray(fields)) {
      return res.status(400).json({
        error: 'INVALID_FIELDS',
        message: 'fields must be a JSON object',
      });
    }

    try {
      const schema = adapter.registerEventSchema(event_type, version, fields, description || null);
      res.status(201).json(schema);
    } catch (err) {
      if (err.message && err.message.includes('UNIQUE constraint failed')) {
        return res.status(409).json({
          error: 'SCHEMA_VERSION_EXISTS',
          event_type,
          version,
        });
      }
      throw err;
    }
  });

  // GET /schemas/events — list all domain event schemas
  // Must be before /schemas/:message_type to avoid matching "events" as a param
  router.get('/schemas/events', (req, res) => {
    const eventSchemas = adapter.listEventSchemas();
    res.json({ schemas: eventSchemas });
  });

  // GET /schemas/events/:event_type — get latest domain event schema
  router.get('/schemas/events/:event_type', (req, res) => {
    const schema = adapter.getEventSchema(req.params.event_type);
    if (!schema) {
      return res.status(404).json({
        error: 'EVENT_SCHEMA_NOT_FOUND',
        event_type: req.params.event_type,
      });
    }
    res.json(schema);
  });

  // GET /schemas/:message_type — get schema for a specific message type
  router.get('/schemas/:message_type', (req, res) => {
    const type = req.params.message_type;
    const schema = schemas[type];

    if (!schema) {
      return res.status(404).json({
        error: 'UNKNOWN_MESSAGE_TYPE',
        message: `No schema defined for type: ${type}`,
        valid_types: Object.keys(schemas),
      });
    }

    res.json({
      type: schema.type,
      version: schema.version,
      fields: schema.fields,
      description: schema.description,
    });
  });

  return router;
}
