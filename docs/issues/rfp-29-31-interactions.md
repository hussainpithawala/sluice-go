# RFPs #29–#31: Interactions with the Event Ledger and Strict Mode

Companion to `rfp-29-atomic-idempotency-guard.md`, `rfp-30-context-aware-drain.md` and `rfp-31-index-reconciliation.md`.

**Numbering note.** References use GitHub issue numbers: the **event ledger** is GitHub #18 and **strict mode** is GitHub #21. The files under `docs/issues/` at `b6966c4` use an earlier numbering (`event-log.md` is titled RFP #17, `sink-silent-loss.md` RFP #18, `strict-mode.md` RFP #19). Match by title, not number.

---

## 1. Shared surfaces

| Surface | Touched by | Conflict | Resolution |
|---|---|---|---|
| `atomicWriteLua` / `atomicDedupWriteLua` reply | #29, ledger Phase 0 (`{e, v}`), #31 (marker, code `3`) | Three different reply shapes | One shape, `{code, e, v}`, defined in #29. Ledger Phase 0 populates `e`; #31 adds code `3` |
| Epoch field `e` in the payload hash | Ledger Phase 0 (`HSETNX e`) | #29 needs to return it | #29 returns `e` if present, `0` otherwise, and does not write it |
| `hydrateLua` | #31 (`ixp` marker), strict mode Phase 3 (`e`, `v`) | Two extenders of one script | Optional flags in a coordinated `ARGV` layout; once strict mode lands, hydrated entries carry `v` |
| New journal-write script | Strict mode `casWriteLua` | Could skip the guard and marker | Script family rule (#29 §4.6): same reply, code enum, optional guard key and marker |
| `Version` type | Ledger (`ledger.Version`), strict mode (`sink.Version`), #29 (`writeResult.version`) | Three definitions of `{Epoch, Seq}` | Define once in a leaf package and alias |
| Batched-write result channel | #29 (`writeResult`), ledger (version to each waiter) | Two reworks of the same struct | One struct: `{written, status, version, err}` |

## 2. Behavioral interactions

| Topic | Interaction | Decision |
|---|---|---|
| Durable ack and idempotency (#29 × strict mode) | A failed `WAIT` after a successful script, then a retry of the same key, returns a duplicate with durability never confirmed | Duplicate path must also run `WAIT`/`WAITAOF` in a durable namespace (#29 §4.6, test 13) |
| Degraded mode (#29 × strict mode) | Strict mode forces `DegradedModeDirect(false)` | Exactly-once acceptance has no degraded exception in strict namespaces |
| Shutdown (#30 × ledger) | Ledger drain follows the sink close and has `Required`-mode waiters | Shared deadline; `LedgerRecordsDropped` reported, not an error (#30 §4.4) |
| Indexes after Redis loss (#31 × ledger) | Redis loss also loses indexes | `Reconcile{Correct: true}` re-writes through the journal and re-indexes; documented in #31 §4.6 |
| Idempotency and ledger (#29 × ledger) | Which writes are recorded | `EventID = idempotencyKey`; record only on code `1`, never on duplicate `2` |
| Limits wording | Ledger changes the durability bound (in `Required` mode, recoverable by reconcile); strict mode changes the ordering and idempotent-upsert limits (version guard) and adds durable ack | See the positioning notes; do not claim "zero data loss" |

## 3. Recommended landing order

1. **#29** (patch): claims the script reply shape.
2. Ledger Phase 0 (epoch, version propagation, `\x00` check), then strict mode Phase 1 (version guard, fixes the stale-flush regression for every namespace).
3. **#30** (minor): can land any time after #29; extend it with the ledger drain when the ledger's Phase 4 lands.
4. **#31**: after #29; coordinate the `hydrateLua` `ARGV` layout with strict mode Phase 3.
5. Strict mode Phase 2 (durable ack) after #29, so the duplicate-path rule is implemented once.
