/**
 * Event audit trail query endpoints for the Spine organ.
 *
 * GET /events — query events with filters (AND semantics)
 * GET /events/:urn — get a single event by URN
 *
 * Events are append-only. No updates, no deletes.
 *
 * Relay 5.
 */

import { Router } from 'express';

export function createEventsRouter(adapter) {
  const router = Router();

  // GET /events — query audit trail with filters
  router.get('/events', (req, res) => {
    const filters = {};

    if (req.query.type) filters.type = req.query.type;
    if (req.query.source_organ) filters.source_organ = req.query.source_organ;
    if (req.query.target_organ) filters.target_organ = req.query.target_organ;
    if (req.query.routing) filters.routing = req.query.routing;
    if (req.query.since) filters.since = req.query.since;
    if (req.query.until) filters.until = req.query.until;
    if (req.query.limit) filters.limit = req.query.limit;

    const result = adapter.queryEvents(filters);
    res.json(result);
  });

  // GET /events/:urn — get single event by URN
  router.get('/events/:urn', (req, res) => {
    const urn = decodeURIComponent(req.params.urn);
    const event = adapter.getEvent(urn);

    if (!event) {
      return res.status(404).json({
        error: 'EVENT_NOT_FOUND',
        urn,
      });
    }

    res.json(event);
  });

  return router;
}
