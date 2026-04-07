/**
 * Organ manifest — the DIO's anatomy.
 *
 * Spine knows which organs must be connected. The manifest is the gatekeeper:
 * no organ connects without being listed. Required organs that are missing
 * degrade Spine's health status.
 *
 * All storage operations go through the StorageAdapter.
 *
 * Relay 4.
 */

function log(event, data = {}) {
  const entry = { timestamp: new Date().toISOString(), event, ...data };
  process.stdout.write(JSON.stringify(entry) + '\n');
}

export function createManifest(adapter) {
  // In-memory manifest map for fast lookups
  const manifestMap = new Map();

  /**
   * Load the full manifest from the adapter into memory.
   */
  function load() {
    manifestMap.clear();
    const rows = adapter.getManifest();
    for (const entry of rows) {
      manifestMap.set(entry.organ_id, entry);
    }
    return manifestMap.size;
  }

  /**
   * Check if an organ_id is known in the manifest.
   */
  function isKnown(organId) {
    return manifestMap.has(organId);
  }

  /**
   * Check if an organ_id is a required organ.
   */
  function isRequired(organId) {
    const entry = manifestMap.get(organId);
    return entry ? entry.required : false;
  }

  /**
   * Get a single manifest entry.
   */
  function getEntry(organId) {
    return manifestMap.get(organId) || null;
  }

  /**
   * Get all manifest entries.
   */
  function getAll() {
    return Array.from(manifestMap.values());
  }

  /**
   * Add a new organ to the manifest (provisioning-time).
   * Also creates a mailbox entry for the organ.
   * Returns the new entry or null if already exists.
   */
  function addOrgan(organId, required = false) {
    const entry = adapter.addManifestEntry(organId, required);
    if (!entry) return null; // already exists

    manifestMap.set(organId, entry);
    log('manifest_organ_added', { organ_id: organId, required });
    return entry;
  }

  /**
   * Get manifest status with connection information.
   * @param {function} isOrganConnected - function(organId) => boolean
   */
  function getStatus(isOrganConnected) {
    const organs = getAll();
    const connected = [];
    const missingRequired = [];

    for (const organ of organs) {
      if (isOrganConnected(organ.organ_id)) {
        connected.push(organ.organ_id);
      } else if (organ.required) {
        missingRequired.push(organ.organ_id);
      }
    }

    const healthStatus = missingRequired.length > 0 ? 'DEGRADED' : 'HEALTHY';

    return {
      organs,
      connected,
      missing_required: missingRequired,
      health_status: healthStatus,
      total: organs.length,
      required_count: organs.filter(o => o.required).length,
      connected_count: connected.length,
    };
  }

  /**
   * Log manifest status at startup.
   * @param {function} isOrganConnected - function(organId) => boolean
   */
  function logStartupStatus(isOrganConnected) {
    const status = getStatus(isOrganConnected);
    log('manifest_loaded', {
      total: status.total,
      required: status.required_count,
      connected: status.connected_count,
      missing: status.missing_required,
    });
    return status;
  }

  // Load manifest on creation
  const count = load();
  log('manifest_initialized', { organ_count: count });

  return {
    load,
    isKnown,
    isRequired,
    getEntry,
    getAll,
    addOrgan,
    getStatus,
    logStartupStatus,
  };
}
