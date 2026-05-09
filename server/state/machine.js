/**
 * State machine engine for the Spine organ.
 *
 * Provides: registerMachine, createEntity, transition, getEntity, getMachineDefinition
 * All storage operations go through the StorageAdapter.
 *
 * Relay 4: Successful transitions emit a broadcast OTM with event_type
 * "state_transition" via the onTransition callback.
 */

import { generateUrn } from '../../lib/urn.js';

/**
 * @param {import('../adapter/interface.js').StorageAdapter} adapter
 * @param {object} [options] - Optional configuration
 * @param {function} [options.onTransition] - Callback invoked after a successful transition.
 *   Receives (transitionResult) where transitionResult has entity_urn, previous_state,
 *   current_state, transition_id, timestamp, and actor/reason from the original call.
 */
export function createStateMachine(adapter, options = {}) {
  const { onTransition } = options;

  /**
   * Register a state machine definition.
   * Throws if entity_type already registered.
   */
  function registerMachine(entityType, definition) {
    return adapter.registerStateMachineDef(entityType, definition);
  }

  /**
   * Get a parsed state machine definition.
   */
  function getMachineDefinition(entityType) {
    return adapter.getStateMachineDef(entityType);
  }

  /**
   * Get all registered state machine definitions.
   */
  function getAllMachineDefinitions() {
    return adapter.listStateMachineDefs();
  }

  /**
   * Create a new tracked entity at the machine's initial state.
   */
  function createEntity(entityUrn, entityType, metadata = null) {
    const machine = getMachineDefinition(entityType);
    if (!machine) {
      return { error: 'MACHINE_NOT_FOUND', entity_type: entityType };
    }

    const existing = adapter.getEntity(entityUrn);
    if (existing) {
      return { error: 'ENTITY_EXISTS', entity_urn: entityUrn };
    }

    const entity = adapter.createEntity(entityUrn, entityType, machine.initial_state, metadata);
    return {
      entity_urn: entity.entity_urn,
      entity_type: entity.entity_type,
      current_state: entity.current_state,
      metadata: entity.metadata,
      created_at: entity.created_at,
    };
  }

  /**
   * Execute a validated state transition.
   *
   * Guards:
   * - Entity must exist (404)
   * - from_state must match current_state (409 STALE_STATE)
   * - Entity must not be in a terminal state (409 TERMINAL_STATE)
   * - to_state must be a legal transition from from_state (409 STATE_TRANSITION_INVALID)
   */
  function transition(entityUrn, fromState, toState, reason, actor) {
    const entity = adapter.getEntity(entityUrn);
    if (!entity) {
      return { error: 'ENTITY_NOT_FOUND', entity_urn: entityUrn };
    }

    const machine = getMachineDefinition(entity.entity_type);

    // Compare-and-swap guard
    if (entity.current_state !== fromState) {
      return {
        error: 'STALE_STATE',
        current_state: entity.current_state,
        requested: { from_state: fromState, to_state: toState },
        allowed_transitions: machine.transitions[entity.current_state] || [],
      };
    }

    // Terminal state guard
    if (machine.terminal_states.includes(entity.current_state)) {
      return {
        error: 'TERMINAL_STATE',
        current_state: entity.current_state,
        requested: { from_state: fromState, to_state: toState },
        allowed_transitions: [],
      };
    }

    // Valid transition guard
    const allowed = machine.transitions[fromState] || [];
    if (!allowed.includes(toState)) {
      return {
        error: 'STATE_TRANSITION_INVALID',
        current_state: entity.current_state,
        requested: { from_state: fromState, to_state: toState },
        allowed_transitions: allowed,
      };
    }

    // Execute transition atomically via adapter
    const transitionId = generateUrn('transition');
    const updated = adapter.transition(entityUrn, fromState, toState, transitionId, reason, actor);

    const result = {
      entity_urn: entityUrn,
      previous_state: fromState,
      current_state: updated.current_state,
      transition_id: transitionId,
      timestamp: updated.updated_at,
      actor,
      reason,
    };

    // Relay 4: emit state_transition OTM broadcast
    if (onTransition) {
      try {
        onTransition(result);
      } catch {
        // Do not let emission failure break the transition
      }
    }

    return result;
  }

  /**
   * Get entity state and full transition history.
   */
  function getEntity(entityUrn) {
    const entity = adapter.getEntity(entityUrn);
    if (!entity) {
      return { error: 'ENTITY_NOT_FOUND', entity_urn: entityUrn };
    }

    const history = adapter.getTransitions(entityUrn);

    return {
      entity_urn: entity.entity_urn,
      entity_type: entity.entity_type,
      current_state: entity.current_state,
      metadata: entity.metadata,
      history,
      created_at: entity.created_at,
      updated_at: entity.updated_at,
    };
  }

  /**
   * Async parallel of transition() — Phase-2 Strand 3 §10.2 patch
   * (binding-rule #40 architectural-class closure for lifecycle callback chain).
   *
   * Mirrors transition() shape but routes the persistence write through
   * adapter.transitionAsync (worker thread). Preserves the onTransition
   * callback emit chain — load-bearing for state_transition OTM consumers
   * Axon, Cortex, ModelBroker, Receptor (4-organ subscriber set verified
   * empirically by EA 1502 R architect-review grep).
   *
   * Validation pre-check is intentionally NOT duplicated here — lifecycle
   * callsite callers (handleOrganDegraded, handleOrganRecovered,
   * executeDisconnectSequence) perform inline current_state pre-check
   * matching the existing sync-path inline pattern, per EA 1502 R guidance.
   * Future async lifecycle callers can layer the same inline guard, OR
   * the wrapper can be extended with the full validation suite when async
   * migration completes for non-lifecycle callers.
   *
   * Per EA 1502 R architect-review verdict (path a ratified) + ESB-I
   * 1503 R dispatch + Verdict 4 staged-commit (Option β new commit
   * on top of e708566).
   *
   * @param {string} entityUrn - canonical entity URN
   * @param {string} fromState - expected current_state (compare-and-swap; caller pre-checks)
   * @param {string} toState - target state
   * @param {string} transitionId - URN for the new state_transitions row (caller-supplied)
   * @param {string} reason - human-readable transition reason
   * @param {string} actor - originating organ/system identifier
   * @returns {Promise<Object>} updated entity (mirrors sync transition() return shape)
   */
  async function transitionAsync(entityUrn, fromState, toState, transitionId, reason, actor) {
    // Route persistence through worker (binding-rule #40)
    const result = await adapter.transitionAsync(
      entityUrn, fromState, toState, transitionId, reason, actor,
    );

    // Relay 4: emit state_transition OTM broadcast (preserved across async path
    // per EA 1502 R path (a) ratification — load-bearing for 4-organ consumer set)
    if (onTransition && result && !result.error) {
      try {
        onTransition({
          entity_urn: entityUrn,
          previous_state: fromState,
          current_state: result.current_state,
          transition_id: transitionId,
          timestamp: result.updated_at,
          actor,
          reason,
        });
      } catch {
        // Do not let emission failure break the transition (matches sync path)
      }
    }

    return result;
  }

  return {
    registerMachine,
    getMachineDefinition,
    getAllMachineDefinitions,
    createEntity,
    transition,
    transitionAsync,
    getEntity,
  };
}
