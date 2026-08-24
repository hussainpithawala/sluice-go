#!/usr/bin/env bash
#
# verify_bands.sh — checks whether your sluice band hash-tags land on
# distinct shards in a Valkey/Redis cluster (local docker-compose cluster
# by default, or point NODES at your real CME endpoint once it exists).
#
# Portable to bash 3.2 (macOS default /bin/bash) — uses indexed arrays and
# linear scans instead of associative arrays (`declare -A`, bash 4+ only),
# since band counts here are small enough (single/low-double digits) that
# O(n) lookups cost nothing in practice.
#
# Usage:
#   ./verify_bands.sh
#   BAND_COUNT=4 KEY_TEMPLATE='dirty:{%d}' ./verify_bands.sh
#   NODES="my-cme-endpoint.cache.amazonaws.com:6379" ./verify_bands.sh
#
# KEY_TEMPLATE does NOT need the sl:<namespace>: prefix. Redis Cluster hashes
# only the substring between the first '{' and the following '}', so
# 'dirty:{0}' and 'sl:nudge_inventory:dirty:{0}' resolve to the same slot. The
# default below therefore checks the same slots your real keys land on.
#
# What DOES matter is the hash-tag CONTENT: the %d must be the band number and
# nothing else, matching dirtyKeyForBand() in internal/shield/shield.go
# ("sl:%s:dirty:{%d}"). If that key format ever grows anything else inside the
# braces, update this template to match.

set -euo pipefail

NODES="${NODES:-127.0.0.1:7001}"                # any one node is enough; CLUSTER NODES returns full topology
BAND_COUNT="${BAND_COUNT:-4}"
KEY_TEMPLATE="${KEY_TEMPLATE:-dirty:{%d}}"       # CONFIRM this matches dirtyKeyForBand() in shield.go
CLI="${CLI:-valkey-cli}"                          # use redis-cli if targeting a Redis OSS node instead
MAX_SALT_ATTEMPTS="${MAX_SALT_ATTEMPTS:-50}"

HOST="${NODES%%:*}"
PORT="${NODES##*:}"

echo "== verify_bands.sh =="
echo "Target node: ${HOST}:${PORT}"
echo "Band count:  ${BAND_COUNT}"
echo "Key template: ${KEY_TEMPLATE}   (namespace prefix omitted on purpose — only the {tag} affects the slot)"
echo

# --- Step 1: pull cluster topology, build slot-range -> node-id map -------
NODES_OUTPUT="$(${CLI} -h "${HOST}" -p "${PORT}" CLUSTER NODES)"

SLOT_START=()
SLOT_END=()
SLOT_NODE=()

while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  flags="$(echo "$line" | awk '{print $3}')"
  if [[ "$flags" != *master* ]]; then
    continue
  fi
  node_id="$(echo "$line" | awk '{print $1}')"
  ranges="$(echo "$line" | awk '{for(i=9;i<=NF;i++) printf "%s ", $i}')"
  for r in $ranges; do
    if [[ "$r" == *"["* ]]; then
      continue
    fi
    if [[ "$r" == *-* ]]; then
      start="${r%%-*}"
      end="${r##*-}"
    else
      start="$r"
      end="$r"
    fi
    SLOT_START+=("$start")
    SLOT_END+=("$end")
    SLOT_NODE+=("$node_id")
  done
done <<< "$NODES_OUTPUT"

if [[ "${#SLOT_NODE[@]}" -eq 0 ]]; then
  echo "ERROR: could not parse any master slot ranges from CLUSTER NODES output."
  echo "Raw output was:"
  echo "$NODES_OUTPUT"
  exit 1
fi

# distinct master count, portable (no associative array)
DISTINCT_MASTERS="$(printf '%s\n' "${SLOT_NODE[@]}" | sort -u | wc -l | tr -d ' ')"
echo "Discovered ${DISTINCT_MASTERS} master(s) owning slots."
echo

slot_to_node() {
  local slot="$1"
  local i
  for i in "${!SLOT_START[@]}"; do
    if (( slot >= SLOT_START[i] && slot <= SLOT_END[i] )); then
      echo "${SLOT_NODE[i]}"
      return 0
    fi
  done
  echo "UNASSIGNED"
}

keyslot() {
  local key="$1"
  ${CLI} -h "${HOST}" -p "${PORT}" CLUSTER KEYSLOT "$key"
}

# --- Step 2: check each band's key against the topology -------------------
# BAND_NODE indexed by band number directly (plain indexed array — fine in
# bash 3.2, only *associative* arrays needed the bash-4 declare -A).
BAND_NODE=()

# node_in_list <needle> <list...>  -> 0 (found) or 1 (not found), portable
# equivalent of an associative-array membership check.
node_in_list() {
  local needle="$1"; shift
  local n
  for n in "$@"; do
    [[ "$n" == "$needle" ]] && return 0
  done
  return 1
}

SEEN_NODE_LIST=()
COLLISION=0

echo "-- Band -> Shard mapping --"
for ((b=0; b<BAND_COUNT; b++)); do
  key="$(printf "$KEY_TEMPLATE" "$b")"
  slot="$(keyslot "$key")"
  node="$(slot_to_node "$slot")"
  BAND_NODE[$b]="$node"
  printf "band=%-3s key=%-20s slot=%-6s shard(node_id)=%s\n" "$b" "$key" "$slot" "$node"
  if node_in_list "$node" "${SEEN_NODE_LIST[@]:-}"; then
    COLLISION=1
  fi
  SEEN_NODE_LIST+=("$node")
done
echo

DISTINCT_USED="$(printf '%s\n' "${SEEN_NODE_LIST[@]}" | sort -u | wc -l | tr -d ' ')"

if [[ "$COLLISION" -eq 0 && "$DISTINCT_USED" -eq "$BAND_COUNT" ]]; then
  echo "RESULT: OK — all ${BAND_COUNT} bands map to distinct shards. No key-tag change needed."
  exit 0
fi

echo "RESULT: COLLISION — ${DISTINCT_USED} distinct shard(s) used across ${BAND_COUNT} bands."
echo "Attempting to find alternate tag salts for colliding bands..."
echo

# --- Step 3: for bands sharing a shard with an earlier band, try salts ----
CLAIMED_NODE_LIST=()
CLAIMED_BAND_LIST=()

claim_node_index() {
  # prints the index in CLAIMED_NODE_LIST matching $1, or empty if none
  local needle="$1"
  local i
  for i in "${!CLAIMED_NODE_LIST[@]}"; do
    if [[ "${CLAIMED_NODE_LIST[i]}" == "$needle" ]]; then
      echo "$i"
      return 0
    fi
  done
  echo ""
}

for ((b=0; b<BAND_COUNT; b++)); do
  node="${BAND_NODE[$b]}"
  idx="$(claim_node_index "$node")"
  if [[ -z "$idx" ]]; then
    CLAIMED_NODE_LIST+=("$node")
    CLAIMED_BAND_LIST+=("$b")
    continue
  fi
  owner_band="${CLAIMED_BAND_LIST[$idx]}"
  echo "Band $b collides with band ${owner_band} on shard $node — searching for a salt..."
  found=0
  for ((salt=1; salt<=MAX_SALT_ATTEMPTS; salt++)); do
    # NOTE: this changes the hash-tag CONTENT (the part inside {}), which
    # changes the slot. Adjust to match exactly how you'd actually rewrite
    # dirtyKeyForBand if you go this route — this is a DEMONSTRATION of the
    # search, not a drop-in key format.
    candidate_key="$(printf "${KEY_TEMPLATE}" "${b}x${salt}")"
    cslot="$(keyslot "$candidate_key")"
    cnode="$(slot_to_node "$cslot")"
    cidx="$(claim_node_index "$cnode")"
    if [[ -z "$cidx" ]]; then
      echo "  -> found: salt=$salt  key=$candidate_key  slot=$cslot  shard=$cnode  (unclaimed)"
      CLAIMED_NODE_LIST+=("$cnode")
      CLAIMED_BAND_LIST+=("$b")
      found=1
      break
    fi
  done
  if [[ "$found" -eq 0 ]]; then
    echo "  -> no salt found within ${MAX_SALT_ATTEMPTS} attempts landing on an unclaimed shard."
    echo "     Consider increasing MAX_SALT_ATTEMPTS, or accept this band sharing a shard."
  fi
  echo
done

echo "Done. Review any suggested salts above against your actual dirtyKeyForBand()/payloadKey()"
echo "implementation before changing production key formats — this script only searches the"
echo "hash-tag content space, it does not modify your Go source."