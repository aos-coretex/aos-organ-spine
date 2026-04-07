#!/bin/bash
# spine-bash-client.sh — Bash 3.2 compatible Spine client library.
#
# Source this file and use the functions below.
# Uses curl only. Reads SPINE_URL env var (default http://127.0.0.1:4000).
#
# Usage:
#   source /path/to/spine-bash-client.sh
#   spine_health
#   spine_send '{"type":"OTM","source_organ":"Vigil","target_organ":"Glia","payload":{}}'
#   spine_register "Vigil"
#   spine_drain "Vigil" 10
#   spine_ack "Vigil" '["urn:llm-ops:otm:..."]'
#   spine_mailbox_status "Vigil"

SPINE_URL="${SPINE_URL:-http://127.0.0.1:4000}"

spine_health() {
  curl -s "${SPINE_URL}/health"
}

spine_send() {
  local body="$1"
  if [ -z "$body" ]; then
    echo '{"error":"spine_send requires a JSON body argument"}' >&2
    return 1
  fi
  curl -s -X POST "${SPINE_URL}/messages" \
    -H "Content-Type: application/json" \
    -d "$body"
}

spine_register() {
  local organ_name="$1"
  if [ -z "$organ_name" ]; then
    echo '{"error":"spine_register requires an organ_name argument"}' >&2
    return 1
  fi
  curl -s -X POST "${SPINE_URL}/mailbox/${organ_name}" \
    -H "Content-Type: application/json" \
    -d '{}'
}

spine_drain() {
  local organ_name="$1"
  local limit="${2:-10}"
  if [ -z "$organ_name" ]; then
    echo '{"error":"spine_drain requires an organ_name argument"}' >&2
    return 1
  fi
  curl -s -X POST "${SPINE_URL}/mailbox/${organ_name}/drain" \
    -H "Content-Type: application/json" \
    -d "{\"limit\":${limit}}"
}

spine_ack() {
  local organ_name="$1"
  local message_ids="$2"
  if [ -z "$organ_name" ] || [ -z "$message_ids" ]; then
    echo '{"error":"spine_ack requires organ_name and message_ids arguments"}' >&2
    return 1
  fi
  curl -s -X POST "${SPINE_URL}/mailbox/${organ_name}/ack" \
    -H "Content-Type: application/json" \
    -d "{\"message_ids\":${message_ids}}"
}

spine_mailbox_status() {
  local organ_name="$1"
  if [ -z "$organ_name" ]; then
    echo '{"error":"spine_mailbox_status requires an organ_name argument"}' >&2
    return 1
  fi
  curl -s "${SPINE_URL}/mailbox/${organ_name}"
}
