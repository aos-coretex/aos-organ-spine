/**
 * Mailbox management endpoints for the Spine organ.
 *
 * Organs register mailboxes, query depth, drain pending messages,
 * and acknowledge processed messages.
 *
 * All storage operations go through the StorageAdapter.
 */

import { Router } from 'express';

export function createMailboxRouter(adapter) {
  const router = Router();

  // POST /mailbox/:organ_name — register a mailbox (idempotent)
  router.post('/:organ_name', (req, res) => {
    const { organ_name } = req.params;

    const existing = adapter.getMailbox(organ_name);
    if (existing) {
      return res.status(200).json({
        mailbox: organ_name,
        status: existing.status,
        already_registered: true,
      });
    }

    const created = adapter.registerMailbox(organ_name);

    res.status(201).json({
      mailbox: organ_name,
      status: created.status,
      registered_at: created.registered_at,
    });
  });

  // GET /mailbox/:organ_name — query mailbox depth and status
  router.get('/:organ_name', (req, res) => {
    const { organ_name } = req.params;

    const mailbox = adapter.getMailbox(organ_name);
    if (!mailbox) {
      return res.status(404).json({ error: 'MAILBOX_NOT_FOUND', organ_name });
    }

    const depth = adapter.getMailboxDepth(organ_name);
    const oldest = adapter.getOldestPending(organ_name);

    res.json({
      mailbox: organ_name,
      depth,
      oldest_message_at: oldest,
      status: mailbox.status,
    });
  });

  // POST /mailbox/:organ_name/drain — extract messages in FIFO order
  router.post('/:organ_name/drain', (req, res) => {
    const { organ_name } = req.params;

    const mailbox = adapter.getMailbox(organ_name);
    if (!mailbox) {
      return res.status(404).json({ error: 'MAILBOX_NOT_FOUND', organ_name });
    }

    let limit = parseInt(req.body.limit, 10) || 10;
    if (limit < 1) limit = 1;
    if (limit > 100) limit = 100;

    const rows = adapter.drainMailbox(organ_name, limit);
    const messages = rows.map(r => JSON.parse(r.envelope));

    adapter.updateLastDrain(organ_name);

    res.json({ messages, count: messages.length });
  });

  // POST /mailbox/:organ_name/ack — mark messages as delivered/processed
  router.post('/:organ_name/ack', (req, res) => {
    const { organ_name } = req.params;

    const mailbox = adapter.getMailbox(organ_name);
    if (!mailbox) {
      return res.status(404).json({ error: 'MAILBOX_NOT_FOUND', organ_name });
    }

    const { message_ids } = req.body;
    if (!Array.isArray(message_ids) || message_ids.length === 0) {
      return res.status(400).json({ error: 'MISSING_FIELD', field: 'message_ids' });
    }

    const acknowledged = adapter.ackMessages(message_ids);
    res.json({ acknowledged });
  });

  return router;
}
