/**
 * URN generation utility for the Spine organ.
 *
 * Format: urn:llm-ops:<namespace>:<ISO-timestamp>-<random4>
 * Namespaces: transition, otm, apm, pem, atm, hom
 */

const CHARS = 'abcdefghijklmnopqrstuvwxyz0123456789';

function random4() {
  let result = '';
  for (let i = 0; i < 4; i++) {
    result += CHARS[Math.floor(Math.random() * CHARS.length)];
  }
  return result;
}

export function generateUrn(namespace) {
  const timestamp = new Date().toISOString();
  return `urn:llm-ops:${namespace}:${timestamp}-${random4()}`;
}
