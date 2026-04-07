/**
 * Predefined state machine definitions for the Spine organ.
 *
 * - job: Job lifecycle (8 states, 7 transitions, 3 terminal)
 * - organ: Organ lifecycle (4 states, 5 transitions, 0 terminal)
 */

const jobMachine = {
  entity_type: 'job',
  states: [
    'CREATED',
    'PLANNING',
    'AWAITING_AUTH',
    'DISPATCHED',
    'EXECUTING',
    'SUCCEEDED',
    'DENIED',
    'FAILED',
  ],
  transitions: {
    CREATED:       ['PLANNING'],
    PLANNING:      ['AWAITING_AUTH', 'DISPATCHED'],
    AWAITING_AUTH: ['DISPATCHED', 'DENIED'],
    DISPATCHED:    ['EXECUTING'],
    EXECUTING:     ['SUCCEEDED', 'FAILED'],
  },
  initial_state: 'CREATED',
  terminal_states: ['SUCCEEDED', 'DENIED', 'FAILED'],
};

const organMachine = {
  entity_type: 'organ',
  states: [
    'REGISTERED',
    'ALIVE',
    'DEGRADED',
    'DISCONNECTED',
  ],
  transitions: {
    REGISTERED:   ['ALIVE'],
    ALIVE:        ['DEGRADED', 'DISCONNECTED'],
    DEGRADED:     ['ALIVE', 'DISCONNECTED'],
    DISCONNECTED: ['ALIVE'],
  },
  initial_state: 'REGISTERED',
  terminal_states: [],
};

export function getStateMachineDefinitions() {
  return [jobMachine, organMachine];
}
