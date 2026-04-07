/**
 * Health, stats, introspect, and diagnostic endpoints for the Spine organ.
 *
 * All storage operations go through the StorageAdapter.
 *
 * Relay 4 additions: /consumers, /manifest, /subscriptions, /subscriptions/:organ_id
 * Relay 6 additions: /health includes organ lifecycle stats, manifest info, mailbox pressure
 */

import { Router } from 'express';
import { statSync } from 'node:fs';

const startTime = Date.now();

export function createHealthRouter(adapter, dbPath, port, manifest, wsHandler, healthConfig = {}) {
  const router = Router();
  const { mailboxPressureThreshold = 100 } = healthConfig;

  router.get('/health', (_req, res) => {
    const sqliteConnected = adapter.healthCheck();
    const uptimeS = Math.floor((Date.now() - startTime) / 1000);

    // Relay 6: Manifest info
    let manifestInfo = null;
    if (manifest && wsHandler) {
      const status = manifest.getStatus(wsHandler.isOrganConnected);
      manifestInfo = {
        total_organs: status.total,
        required: status.required_count,
        connected: status.connected_count,
        missing_required: status.missing_required,
      };
    }

    // Relay 6: Organ state counts from state machine
    let organStats = {};
    try {
      organStats = adapter.getOrganStateCounts();
    } catch { /* state entities may not exist yet */ }

    // Relay 6: Mailbox info
    let mailboxInfo = { total_depth: 0, pressure_organs: [] };
    try {
      const totalDepth = adapter.getTotalMailboxDepth();
      const pressureOrgans = adapter.getMailboxesUnderPressure(mailboxPressureThreshold)
        .map(r => r.target_organ);
      mailboxInfo = { total_depth: totalDepth, pressure_organs: pressureOrgans };
    } catch { /* graceful fallback */ }

    // Determine overall status
    let status = 'ok';
    if (!sqliteConnected) {
      status = 'error';
    } else if (manifestInfo && manifestInfo.missing_required.length > 0) {
      status = 'degraded';
    }

    res.json({
      status,
      uptime_s: uptimeS,
      sqlite_connected: sqliteConnected,
      port,
      manifest: manifestInfo,
      organs: {
        alive: organStats.ALIVE || 0,
        degraded: organStats.DEGRADED || 0,
        disconnected: organStats.DISCONNECTED || 0,
      },
      mailbox: mailboxInfo,
    });
  });

  router.get('/stats', (_req, res) => {
    try {
      const stats = adapter.getStats();
      res.json(stats);
    } catch {
      res.status(500).json({ error: 'STATS_UNAVAILABLE' });
    }
  });

  router.get('/introspect', (_req, res) => {
    try {
      const tables = adapter.getTables();

      let dbSizeBytes = 0;
      if (dbPath !== ':memory:') {
        try {
          dbSizeBytes = statSync(dbPath).size;
        } catch {
          // file not accessible
        }
      }

      res.json({
        db_path: dbPath,
        db_size_bytes: dbSizeBytes,
        tables,
        uptime_s: Math.floor((Date.now() - startTime) / 1000),
      });
    } catch {
      res.status(500).json({ error: 'INTROSPECT_UNAVAILABLE' });
    }
  });

  // --- Relay 4: Diagnostic endpoints ---

  // GET /consumers — all connected organs with subscription counts
  router.get('/consumers', (_req, res) => {
    if (!wsHandler) {
      return res.json({ consumers: [] });
    }
    res.json({ consumers: wsHandler.getConsumers() });
  });

  // GET /manifest — full manifest with connection status
  router.get('/manifest', (_req, res) => {
    if (!manifest || !wsHandler) {
      return res.status(503).json({ error: 'MANIFEST_NOT_AVAILABLE' });
    }
    const status = manifest.getStatus(wsHandler.isOrganConnected);
    res.json(status);
  });

  // POST /manifest/:organ_id — add a new organ to the manifest (provisioning)
  router.post('/manifest/:organ_id', (req, res) => {
    if (!manifest) {
      return res.status(503).json({ error: 'MANIFEST_NOT_AVAILABLE' });
    }

    const { organ_id } = req.params;
    const required = req.body.required === true;

    const entry = manifest.addOrgan(organ_id, required);
    if (!entry) {
      // Already exists — return current entry
      const existing = manifest.getEntry(organ_id);
      return res.status(200).json({ ...existing, already_exists: true });
    }

    res.status(201).json(entry);
  });

  // GET /subscriptions — all persistent subscriptions
  router.get('/subscriptions', (_req, res) => {
    try {
      const rows = adapter.getSubscriptionsForDisplay();

      const subscriptions = rows.map(r => ({
        organ_id: r.organ_id,
        filter: r.topic_filter ? JSON.parse(r.topic_filter) : {},
        registered_at: r.registered_at,
      }));

      res.json({ subscriptions });
    } catch {
      res.status(500).json({ error: 'SUBSCRIPTIONS_UNAVAILABLE' });
    }
  });

  // GET /subscriptions/:organ_id — subscriptions for one organ
  router.get('/subscriptions/:organ_id', (req, res) => {
    const { organ_id } = req.params;

    // Validate organ exists in manifest
    if (manifest && !manifest.isKnown(organ_id)) {
      return res.status(404).json({ error: 'ORGAN_NOT_FOUND', organ_id });
    }

    try {
      const rows = adapter.getOrganSubscriptionsForDisplay(organ_id);

      const subscriptions = rows.map(r => ({
        filter: r.topic_filter ? JSON.parse(r.topic_filter) : {},
        registered_at: r.registered_at,
      }));

      res.json({ organ_id, subscriptions });
    } catch {
      res.status(500).json({ error: 'SUBSCRIPTIONS_UNAVAILABLE' });
    }
  });

  return router;
}
