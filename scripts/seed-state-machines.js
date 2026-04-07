/**
 * Seed script — registers job and organ state machine definitions.
 *
 * This runs directly against the StateMachine module (not via HTTP).
 * Intended for manual re-seeding if needed; the normal path is that
 * db/init.js seeds on startup.
 *
 * Usage: node scripts/seed-state-machines.js
 */

import { initDatabase } from '../server/db/init.js';
import { createStateMachine } from '../server/state/machine.js';

const db = initDatabase();
const sm = createStateMachine(db);

const jobDef = sm.getMachineDefinition('job');
const organDef = sm.getMachineDefinition('organ');

console.log('State machines registered:');
console.log(`  job:   ${jobDef.states.length} states, ${Object.values(jobDef.transitions).flat().length} transitions, ${jobDef.terminal_states.length} terminal`);
console.log(`  organ: ${organDef.states.length} states, ${Object.values(organDef.transitions).flat().length} transitions, ${organDef.terminal_states.length} terminal`);

db.close();
