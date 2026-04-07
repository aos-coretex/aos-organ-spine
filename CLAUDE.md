# Spine — ESB Organ

## What this is

This is the **ESB Spine organ** — the nervous system of the DIO (Distributed Intelligence Organism). It is a completely independent platform from the monolith Spine at `AOS-software-dev/AOS-spine/`. The two share no code, no databases, and no ports.

## Architecture

- **Port:** 3900 (SAAS) / 4000 (AOS), binding `127.0.0.1`
- **Database:** `data/spine.db` — encapsulated SQLite, co-located inside the organ. WAL mode.
- **Runtime:** Node.js, Express 5, ES modules (`import`, not `require`)
- **Test runner:** Node.js built-in (`node --test`)

## Subsystems (by relay)

| Subsystem | Relay | Description |
|---|---|---|
| spine-state | 1 | State machine definitions, entity lifecycle tracking, validated transitions |
| spine-event (messaging) | 2 | WebSocket push, event queue, organ mailboxes |
| spine-schema | 3 | Message schema registry, validation |
| spine-routing | 4 | Broadcast/directed message routing |
| spine-adapter | 5 | StorageAdapter abstraction, event audit trail, domain event schemas |

## Zero Cross-Contamination Rules

- **Never** reference `ai-kb.db` or `AI-Datastore/`
- **Never** reference `AOS-software-dev/` paths
- **Never** use ports 3800-3851 (monolith range)
- **Never** import from monolith packages
- The ESB and monolith are two independent platforms sharing nothing

## URN Encoding in HTTP

URNs contain colons (`urn:llm-ops:transition:2026-04-07T14:30:00.000Z-a1b2`). Express 5 handles special characters in route parameters differently from Express 4. All URNs in URL paths must be `encodeURIComponent`-encoded by clients and are `decodeURIComponent`-decoded by route handlers. Internal code always works with raw URNs — encoding is transport-level only.

## Running

```bash
# Start (AOS default port 4000)
npm start

# Start on SAAS port
SPINE_PORT=3900 npm start

# Run tests (uses in-memory SQLite)
npm test

# Custom database path
SPINE_DB_PATH=/path/to/spine.db npm start
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `SPINE_PORT` | `4000` | Server port (4000 = AOS, 3900 = SAAS) |
| `SPINE_DB_PATH` | `./data/spine.db` | SQLite database path |
