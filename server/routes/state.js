/**
 * spine-state HTTP endpoints.
 *
 * URNs in URL paths are encodeURIComponent-encoded by clients.
 * Route handlers decode them before use.
 */

import { Router } from 'express';

export function createStateRouter(stateMachine) {
  const router = Router();

  // List all registered state machine definitions
  router.get('/machines', (_req, res) => {
    const machines = stateMachine.getAllMachineDefinitions();
    res.json({ machines });
  });

  // Get a specific state machine definition
  router.get('/machines/:entity_type', (req, res) => {
    const machine = stateMachine.getMachineDefinition(req.params.entity_type);
    if (!machine) {
      return res.status(404).json({
        error: 'MACHINE_NOT_FOUND',
        entity_type: req.params.entity_type,
      });
    }
    res.json(machine);
  });

  // Create a new tracked entity at initial state
  router.post('/entities', (req, res) => {
    const { entity_urn, entity_type, metadata } = req.body;

    if (!entity_urn || !entity_type) {
      return res.status(400).json({
        error: 'MISSING_FIELDS',
        required: ['entity_urn', 'entity_type'],
      });
    }

    const result = stateMachine.createEntity(entity_urn, entity_type, metadata || null);

    if (result.error === 'MACHINE_NOT_FOUND') {
      return res.status(404).json(result);
    }
    if (result.error === 'ENTITY_EXISTS') {
      return res.status(409).json(result);
    }

    res.status(201).json(result);
  });

  // Get entity state and transition history
  router.get('/:entity_urn', (req, res) => {
    const entityUrn = decodeURIComponent(req.params.entity_urn);
    const result = stateMachine.getEntity(entityUrn);

    if (result.error === 'ENTITY_NOT_FOUND') {
      return res.status(404).json(result);
    }

    res.json(result);
  });

  // Execute a validated state transition
  router.post('/:entity_urn/transition', (req, res) => {
    const entityUrn = decodeURIComponent(req.params.entity_urn);
    const { from_state, to_state, reason, actor } = req.body;

    if (!from_state || !to_state || !reason || !actor) {
      return res.status(400).json({
        error: 'MISSING_FIELDS',
        required: ['from_state', 'to_state', 'reason', 'actor'],
      });
    }

    const result = stateMachine.transition(entityUrn, from_state, to_state, reason, actor);

    if (result.error === 'ENTITY_NOT_FOUND') {
      return res.status(404).json({ error: 'ENTITY_NOT_FOUND', entity_urn: entityUrn });
    }
    if (result.error) {
      return res.status(409).json(result);
    }

    res.json(result);
  });

  return router;
}
