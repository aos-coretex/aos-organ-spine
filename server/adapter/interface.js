/**
 * StorageAdapter interface — the boundary where SQLite swaps to Graphheight.
 *
 * Current:  SQLiteStorageAdapter (server/adapter/sqlite.js)
 * Future:   GraphheightStorageAdapter (HTTP calls to Graphheight 911 → 311)
 *
 * All spine.db access MUST go through this adapter. No direct db.prepare()
 * calls outside the adapter module.
 *
 * Relay 5.
 */

export class StorageAdapter {

  // --- Events (Relay 5) ---

  /** Store message in events table as permanent audit trail. */
  persistEvent(_envelope, _routing) {
    throw new Error('StorageAdapter.persistEvent() not implemented');
  }

  /** Query events by type/organ/time. Returns { events, count }. */
  queryEvents(_filters) {
    throw new Error('StorageAdapter.queryEvents() not implemented');
  }

  /** Get single event by URN. Returns event object or null. */
  getEvent(_urn) {
    throw new Error('StorageAdapter.getEvent() not implemented');
  }

  // --- State (Relay 1) ---

  /** Get state machine definition. Returns parsed def or null. */
  getStateMachineDef(_entityType) {
    throw new Error('StorageAdapter.getStateMachineDef() not implemented');
  }

  /** Register state machine definition. Returns parsed def. */
  registerStateMachineDef(_entityType, _def) {
    throw new Error('StorageAdapter.registerStateMachineDef() not implemented');
  }

  /** List all state machine definitions. Returns array of parsed defs. */
  listStateMachineDefs() {
    throw new Error('StorageAdapter.listStateMachineDefs() not implemented');
  }

  /** Create tracked entity at initial state. Returns entity row or null if exists. */
  createEntity(_entityUrn, _entityType, _initialState, _meta) {
    throw new Error('StorageAdapter.createEntity() not implemented');
  }

  /** Get entity row (without history). Returns row or null. */
  getEntity(_entityUrn) {
    throw new Error('StorageAdapter.getEntity() not implemented');
  }

  /** Get transition history for an entity. Returns array of transitions. */
  getTransitions(_entityUrn) {
    throw new Error('StorageAdapter.getTransitions() not implemented');
  }

  /** Atomic: update entity state + insert transition record. Returns transition row. */
  transition(_entityUrn, _fromState, _toState, _transitionId, _reason, _actor) {
    throw new Error('StorageAdapter.transition() not implemented');
  }

  // --- Mailboxes (Relay 2) ---

  /** Register organ mailbox (idempotent). Returns mailbox row. */
  registerMailbox(_organName) {
    throw new Error('StorageAdapter.registerMailbox() not implemented');
  }

  /** Get mailbox row by organ name. Returns row or null. */
  getMailbox(_organName) {
    throw new Error('StorageAdapter.getMailbox() not implemented');
  }

  /** Get pending message count for an organ. */
  getMailboxDepth(_organName) {
    throw new Error('StorageAdapter.getMailboxDepth() not implemented');
  }

  /** Get oldest pending message timestamp. Returns ISO string or null. */
  getOldestPending(_organName) {
    throw new Error('StorageAdapter.getOldestPending() not implemented');
  }

  /** Store directed message in mailbox queue. */
  persistMailboxMessage(_messageId, _targetOrgan, _sourceOrgan, _envelope, _delivered) {
    throw new Error('StorageAdapter.persistMailboxMessage() not implemented');
  }

  /** Get undelivered messages FIFO. Returns array of { message_id, envelope }. */
  drainMailbox(_organName, _limit) {
    throw new Error('StorageAdapter.drainMailbox() not implemented');
  }

  /** Update last_drain_at for a mailbox. */
  updateLastDrain(_organName) {
    throw new Error('StorageAdapter.updateLastDrain() not implemented');
  }

  /** Mark messages as delivered (atomic). Returns count of acknowledged. */
  ackMessages(_messageIds) {
    throw new Error('StorageAdapter.ackMessages() not implemented');
  }

  /** Set mailbox status to 'active' and update last_connected_at. */
  setMailboxActive(_organName) {
    throw new Error('StorageAdapter.setMailboxActive() not implemented');
  }

  /** Set mailbox status to 'disconnected'. */
  setMailboxDisconnected(_organName) {
    throw new Error('StorageAdapter.setMailboxDisconnected() not implemented');
  }

  /** Get all pending (undelivered) messages for an organ (no limit). */
  getPendingMessages(_organName) {
    throw new Error('StorageAdapter.getPendingMessages() not implemented');
  }

  /** Mark a single message as delivered. */
  markMessageDelivered(_messageId) {
    throw new Error('StorageAdapter.markMessageDelivered() not implemented');
  }

  // --- Event Schemas (Relay 5) ---

  /** Register a domain event schema for OTM subtypes. Returns schema row. */
  registerEventSchema(_eventType, _version, _fields, _description) {
    throw new Error('StorageAdapter.registerEventSchema() not implemented');
  }

  /** Get latest version schema for event_type. Returns row or null. */
  getEventSchema(_eventType) {
    throw new Error('StorageAdapter.getEventSchema() not implemented');
  }

  /** List all registered event schemas. */
  listEventSchemas() {
    throw new Error('StorageAdapter.listEventSchemas() not implemented');
  }

  // --- Organ Manifest (Relay 4) ---

  /** Get all manifest entries. */
  getManifest() {
    throw new Error('StorageAdapter.getManifest() not implemented');
  }

  /** Get single manifest entry or null. */
  getManifestEntry(_organId) {
    throw new Error('StorageAdapter.getManifestEntry() not implemented');
  }

  /** Add organ to manifest + create mailbox. Returns entry or null if exists. */
  addManifestEntry(_organId, _required) {
    throw new Error('StorageAdapter.addManifestEntry() not implemented');
  }

  // --- Subscription Registry (Relay 4) ---

  /** Get organ's persistent broadcast subscriptions. */
  getSubscriptions(_organId) {
    throw new Error('StorageAdapter.getSubscriptions() not implemented');
  }

  /** Get all broadcast subscriptions (for cache load). */
  getAllSubscriptions() {
    throw new Error('StorageAdapter.getAllSubscriptions() not implemented');
  }

  /** Persist a broadcast subscription. */
  addSubscription(_organId, _filter) {
    throw new Error('StorageAdapter.addSubscription() not implemented');
  }

  /** Remove a broadcast subscription. */
  removeSubscription(_organId, _filter) {
    throw new Error('StorageAdapter.removeSubscription() not implemented');
  }

  // --- Schema Changelog (Relay 3) ---

  /** Add a schema changelog entry. */
  addChangelogEntry(_messageType, _version, _description, _added, _removed, _modified) {
    throw new Error('StorageAdapter.addChangelogEntry() not implemented');
  }

  /** Get all changelog entries. */
  getChangelog() {
    throw new Error('StorageAdapter.getChangelog() not implemented');
  }

  // --- Relay 6: Health Monitoring ---

  /** Update mailbox status ('active' or 'disconnected'). */
  updateMailboxStatus(_organName, _status) {
    throw new Error('StorageAdapter.updateMailboxStatus() not implemented');
  }

  /** Delete expired messages (TTL exceeded, undelivered). Returns expired count. */
  expireMessages() {
    throw new Error('StorageAdapter.expireMessages() not implemented');
  }

  /** Get total pending (undelivered) message count across all mailboxes. */
  getTotalMailboxDepth() {
    throw new Error('StorageAdapter.getTotalMailboxDepth() not implemented');
  }

  /** Get organs whose mailbox depth exceeds the threshold. Returns array of { target_organ, depth }. */
  getMailboxesUnderPressure(_threshold) {
    throw new Error('StorageAdapter.getMailboxesUnderPressure() not implemented');
  }

  /** Get organ state counts grouped by current_state. Returns { ALIVE: n, DEGRADED: n, ... }. */
  getOrganStateCounts() {
    throw new Error('StorageAdapter.getOrganStateCounts() not implemented');
  }

  // --- Diagnostics ---

  /** Health check — returns true if storage is reachable. */
  healthCheck() {
    throw new Error('StorageAdapter.healthCheck() not implemented');
  }

  /** Get aggregate stats (entity counts, transition counts, etc.). */
  getStats() {
    throw new Error('StorageAdapter.getStats() not implemented');
  }

  /** Get list of table names (introspection). */
  getTables() {
    throw new Error('StorageAdapter.getTables() not implemented');
  }

  /** Close the underlying storage connection. */
  close() {
    throw new Error('StorageAdapter.close() not implemented');
  }
}
