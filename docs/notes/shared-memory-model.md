# Shared-memory publication — design note

Salvaged 2026-08-31 from the Sprint 2 3B.1 evidence tree; prose design thinking, no code survives it. Both review findings raised against it are resolved below — the `synchronizing` rule is now carried by a typed cell layer (§1), and candidate 2 is rejected outright (§3.2) — and §6 records what the first implementation pins.

## 1. Cell classes

Every cell of the ABI falls into exactly one of four classes. The class is a field of the schema
(`cell_class`), so the model below and the machine-readable ABI cannot disagree silently.

| Class | Definition | Access rule |
| --- | --- | --- |
| `immutable` | Written before the segment is attachable; never written again while the segment generation lives. | Plain reads after the header validates. No synchronization needed because there is no concurrent write. |
| `synchronizing` | Carries the happens-before edge between the writer and a reader. | Writer: `Release` store. Reader: `Acquire` load. Never a plain access. |
| `relaxed_data_word` | Ordered by a `synchronizing` cell of the same record; may be written while a reader is reading. | Writer and reader both use `Relaxed` atomic accesses on the 32-bit or 64-bit word. Ordering comes from the record's synchronizing cell, never from the data access itself. |
| `write_once_published` | Written before its record's `entry_revision` is released; never mutated while the segment generation lives. | Read only after an `Acquire` load of that `entry_revision` observes a non-zero value. Plain byte reads are then sound, because no write can be concurrent with them. |

**Finding 1, resolved: the rule is carried by types, not prose.** The rule above was violated by its
own crate's validator, which plainly read `publication_generation`. Prose cannot hold it, so the
region is reachable only through a cell layer that makes an illegal access unspellable:

- `SyncCell` wraps one 64-bit cell and exposes exactly two operations, `acquire_load` and
  `release_store`. There is no plain load, no `Deref`, and no accessor yielding the underlying
  atomic, so a plain or `Relaxed` read of a `synchronizing` cell cannot be written.
- `RelaxedWord64` and `RelaxedWord32` expose exactly `relaxed_load` and `relaxed_store` at their own
  width. They are the only way to reach a `relaxed_data_word`, and they cannot establish ordering
  because they offer no `Acquire` or `Release` operation.
- `write_once_published` bytes are unreachable except through a witness value that only an `Acquire`
  load of the governing `entry_revision` can produce, and only when that load observed a non-zero
  value. The witness is the sole path to the identity bytes and to `entry_state_slot_index`, so the
  class rule — read only after an `Acquire` load of that `entry_revision` observes a non-zero value
  — is a borrow-checked precondition rather than a convention.
- The even-odd counter of a seqlock-published record is its own cell class, `SeqCell`, because the
  seqlock is not expressible with one-way release/acquire stores (§3.1). It exposes only the four
  operations of that construction — open/close a write interval, open/close a read interval — with
  the fences inside them, so the sequence cannot be assembled without its barriers.
- No public path hands out a reference to a mutable cell, so no caller can assemble an access the
  cell layer did not sanction. Every byte of the region is reached at one fixed, naturally aligned
  offset and at one width; no byte is ever accessed at two widths, and the only non-atomic accesses
  are to `write_once_published` bytes, ordered by the release that publishes them and serialized by
  an atomic claim of the gate, so two threads can never write one record's payload.

**What this boundary does and does not achieve.** It makes a *plain* access to a mutable cell
unspellable anywhere outside the cell module: no other module of the crate touches a region byte,
and the crate root denies `unsafe_code` everywhere but that module. It does **not** make a
*wrong-class* access unspellable inside the shm module family. A field's offset is a plain integer,
so code there can hand a `synchronizing` offset to the relaxed-word accessor, or a data offset to
the sequence accessor, and the types will not object — the class is carried by which constant is
passed, not by the constant's type. Closing that would need per-field offset tokens minted by the
layout, which is a redesign this pass deliberately does not take. Until then the guarantee for
production offsets is audit plus a table test that pins every offset and width against an
independent implementation, and drift fails loudly rather than silently decoding the wrong bytes.

Two consequences are load-bearing:

- Every mutable cell must be a 32-bit or 64-bit word, or a composite made only of such words (the
  24-byte decimal cell is two 64-bit words plus two 32-bit words; the level cell is two decimal
  cells plus its side word, §6), over every record without exception. A `relaxed_data_word` is therefore never a
  plain non-atomic access to memory another thread or process may be writing, which is what would
  make the generation protocols below free of data races rather than merely unlikely to tear.
- No mutable cell is a single 128-bit value. The 128-bit decimal coefficient is carried as two
  explicit 64-bit halves whose joint consistency is established by the record's synchronizing cell,
  which is why this ABI needs no 128-bit atomic even though the target happens to provide one.

Byte arrays — the identity bytes and the connection value bytes — are `write_once_published` and are
therefore never read concurrently with a write. The retained-event payload is the one byte-shaped
region that *is* overwritten under a reader, and the ABI addresses it as
`event_payload_capacity / 8` 64-bit words for exactly that reason; its byte interpretation is the
little-endian concatenation of those words.

## 2. Every concurrently accessed cell

This is the complete list of cells a reader may touch while the writer is running, plus the one
write-once cell an entry's release publishes alongside its identity. Any other cell is `immutable`
and covered by the class rules above.

| Record | Field | Width | Class | Protocol |
| --- | --- | ---: | --- | --- |
| `header` | `publication_generation` | 64 | synchronizing | Writer `Release`-stores an increment after each publication round; a reader `Acquire`-loads it as a coalescible hint that newer data may exist. It is never the authority for any particular market. |
| `directory_entry` | `entry_revision` | 64 | synchronizing | `Release`-stored once, from zero to non-zero, after the entry's identity bytes and `entry_state_slot_index` are written. A reader that `Acquire`-loads a non-zero value may read them, and only then. |
| `directory_entry` | `entry_state_slot_index` | 32 | write_once_published | Written once at installation, before `entry_revision` is released; stable thereafter. The reserved value `0xffffffff` means no state slot is published for the market, and a reader treats it as "no current state" rather than as index 4,294,967,295. It was `synchronizing` only because candidate 2 made it the publication cell; with candidate 2 rejected it is written once and never again, which is exactly the write-once class. |
| `connection_entry` | `entry_revision` | 64 | synchronizing | `Release`-stored once after the connection value bytes are written. |
| `state_slot` | `slot_revision` | 64 | synchronizing (seqlock counter) | **Even-odd generation counter, fenced.** Writer: `Relaxed`-store an odd value, `Release` **fence**, `Relaxed`-store every data word, `Release`-store the next even value. Reader: `Acquire`-load; reject an odd value; `Relaxed`-load the data words; `Acquire` **fence**; `Relaxed`-reload; accept only if unchanged and even. The fences are load-bearing, not decoration — see §3.1. |
| `state_slot` | `directory_index` | 32 | relaxed data word | Fixed for the life of the slot's market assignment; a reader revalidates it against the entry it started from, as a direct ownership check rather than an inference. |
| `state_slot` | every remaining prefix word and every level cell | 32 / 64 | relaxed data word | Rewritten in place inside the odd interval of `slot_revision`. |
| `event_slot` | `slot_sequence` | 64 | synchronizing (seqlock counter) | The same fenced even-odd construction as `slot_revision`, over the slot's prefix and payload words. |
| `event_slot` | every remaining prefix word and every payload word | 32 / 64 | relaxed data word | Written inside the odd interval of `slot_sequence`. |
| `header` | `doorbell` | 32 | relaxed data word | Wake-only: the writer `Relaxed`-stores an increment before each `publication_generation` release and posts one OS wake per state publish or republish. A reader passes its last observed value to an equality-based OS wait and re-`Acquire`s `publication_generation` after every return; the cell itself is never read for meaning, data ordering rides the generation's release/acquire pair, and wake delivery rides the wait/wake syscalls. Wrap-around is harmless because the wait is equality-based. |
| `dirty_slot` | `sequence` | 64 | synchronizing (seqlock counter) | §3.3's fenced even-odd construction over the slot's three data words. |
| `dirty_slot` | `position` | 64 | relaxed data word | The entry's absolute, segment-lifetime-monotone position, written inside the odd interval. A reader classifies on it: equal to its cursor delivers, greater is the declared full-rescan signal (the cursor rebases to the current head, never silence), lower or unpublished is idle. |
| `dirty_slot` | `directory_index`, `book_revision` | 32 / 64 | relaxed data word | Written inside the odd interval. A delivered entry means "poll that market's surfaces"; `book_revision` is a state-read skip hint only, because a resolution republish advertises an unchanged revision while the market's event ring holds new deliveries. |

`slot_generation` is gone with candidate 2: a slot is bound to one market for the life of the segment
generation, so there is nothing for a generation counter to distinguish.

Every `synchronizing` and every `relaxed_data_word` field's declared alignment must equal `align_of`
of the atomic type of its width, and its offset must be a multiple of that alignment — so that no
byte of the region is ever reachable at two widths.

## 3. Writer and reader sequences

### 3.1 The publication sequence — atomic-cell generation snapshots

Writer, for one market:

1. `Relaxed`-load the slot's current `slot_revision` (the writer is the only writer, so no contention).
2. `Relaxed`-store `revision + 1` (odd), then execute a `Release` **fence**. The slot is now marked
   in flight, and the fence is what keeps step 3 from happening before that is visible.
3. `Relaxed`-store `state_revision`, the authority, continuity, lifecycle and subscription
   discriminants, the cursor pair, the provenance words, the level counts, the depth cells and the
   level cells.
4. `Release`-store `revision + 2` (even). The slot is stable again, and this release publishes every
   store made in step 3.
5. After the round, `Release`-store an incremented `publication_generation` and post a coalescible
   wakeup.

Reader, for one market:

1. `Acquire`-load `entry_revision` of the directory entry; a zero value means the market is not
   installed.
2. Read the entry's identity bytes and `entry_state_slot_index` through the witness step 1
   produced (a plain read is sound: both are `write_once_published` and the `Acquire` in step 1
   ordered their writes).
3. `Acquire`-load `slot_revision` of the slot named by `entry_state_slot_index`; if that index is
   the reserved `0xffffffff` the market has no published state and the reader stops here. If the
   revision is odd, the writer is mid-publication: retry.
4. `Relaxed`-load every data word it needs.
5. Execute an `Acquire` **fence**, then `Relaxed`-reload `slot_revision`. Accept only if it is
   unchanged; otherwise retry.

**Why the fences are load-bearing.** A release store orders only the operations that *precede* it,
and an acquire load only those that *follow* it. Both halves of a seqlock need the opposite. A
release store of the odd value would not stop the data stores that follow it from becoming visible
before the odd value does, so a reader could see the old even counter, then new data, then the same
even counter — a torn read with the counter check passing. Symmetrically, an acquire load as the
reader's closing check would not stop the data loads that precede it from being performed after the
recheck, with the same outcome. A `Release` fence after the odd store and an `Acquire` fence before
the recheck are the two-way barriers the counter's own accesses cannot supply. This is the standard
fenced seqlock; the earlier one-way-store-only version of this section was wrong.

**Why a torn read is then excluded.** A tear requires the reader to combine words from two different
publications. With the fences in place, any write to a data word happens strictly inside the
interval between the odd value becoming visible and the next even `Release` store. If the reader's
recheck returns the same even value it opened with, then no odd interval began and ended between
them, so no data word it read was written in between. If any write did occur, the counter differs or
is odd, and the reader retries. Because every data word is itself an atomic 32-bit or 64-bit cell
accessed `Relaxed`, a concurrent overlap is a well-defined stale-or-fresh value, never undefined
behaviour — and it is then discarded by the counter check.

**What the tests can and cannot show.** The multi-reader stress test is probabilistic: it detects a
tear only on the schedules it happens to sample. This was measured, not assumed — deleting the
writer's `Release` fence, and separately the reader's `Acquire` fence, both leave the stress test
passing on the `aarch64-apple-darwin` profile, while deleting the closing recheck entirely fails it
within milliseconds. The fences are therefore justified by the memory-model argument above and not
by any test this host can run; the stress test is a regression net for the recheck. The same caveat
applies to the atomic claim of §3.2's gates: a read-then-write claim also survives a two-thread race
sampler here, and is ruled out by the model rather than by measurement.

**The one-way gates are correct as they stand.** `publication_generation`, a `directory_entry`'s
`entry_revision`, and the header and trailer magics are one-way publication, not seqlocks: each
publishes state written *before* it and is never rewritten. A release store and an acquire load are
exactly right there, and no fence is needed. A gate additionally serializes its writers by an atomic
claim — compare-exchange from zero to a sentinel that can never validate — so the non-atomic payload
writes behind it have exactly one writer even if two threads hold the same region.

**Liveness.** The writer never waits. A reader that keeps losing the race retries; it cannot block
the writer, and the writer has no way to know a reader exists.

### 3.2 Candidate 2 — immutable published slots with bounded reuse — REJECTED

**Finding 2, resolved: candidate 2 is rejected.** An arbitrarily stalled reader can observe a torn
read once a slot is reused and republished; the nominal four-slot profile leaves no spare; and a
fixed "reuse distance" is a timer dressed as a proof, which is the same reasoning the daemon's
evidence-based authority rule already forbids. Repairing it needs either non-reused storage or a
reader-visible reclamation protocol, and both cost more than the even-odd counter candidate 1
already gives for free.

Candidate 1 is therefore the publication protocol, and the only one. Slot reuse, free lists,
retirement queues, reuse distance, and `slot_generation` leave this ABI entirely: a state slot is
bound to one directory entry for the life of the segment generation, and a market that goes away
leaves its slot unreused until that generation ends. Capacity is fixed at creation and exceeding it
is a typed failure at installation, never a reuse.

The two obligations candidate 2's revalidation appeared to carry survive on their own terms, because
they were never really its: a directory entry and its handle are never reused within a segment
generation, and every state slot carries the `directory_index` it serves. Both should be declared as
explicit preconditions wherever this ABI is specified — `handle_not_reused_within_a_generation` and
`handle_is_process_local` — not left as prose.

### 3.3 Retained events

Retention is independent of the state-publication choice and is unaffected by candidate 2's
rejection. It is the same fenced construction §3.1 pins, over a different record. Writer:
`Relaxed`-store an odd `slot_sequence`, `Release` fence, `Relaxed`-store the cursor pair, the
delivery discriminant, `payload_length`, and the payload words with zero fill to the end of the
final word, then `Release`-store the next even value. Reader: `Acquire`-load the sequence, reject an
odd value, copy, `Acquire` fence, `Relaxed`-reload, accept only if unchanged. Because the ring wraps
(required feature bit `event_ring_wraps`), a reader whose cursor has been overtaken sees a sequence
that has advanced past its expected value and reports continuity loss through the frozen seam's
overrun delivery rather than reading partial history. The partitioning is decided and pinned in
§6: one ring per directory entry, so overrun is a per-book fact and no market shares retention
fate with another.

### 3.4 Writer crash mid-publication

- **The slot.** The market's `slot_revision` is left odd. Every reader rejects an odd counter, so the
  market reads as unavailable rather than as a half-written book. Nothing in the segment is corrupt;
  the slot is simply never completed.
- **Telling a dead writer from a busy one.** A reader records the odd value it first observed and
  compares it on every retry: an odd counter that never changes is a stalled or dead writer and is
  reported as such, while an odd counter that keeps moving is contention and is retried to the
  reader's own bound. The two are different answers, not one timeout.
- **The process** that died holds the only writer role. A supervisor restart
  establishes a new `daemon_instance_id` and `segment_generation`; attached readers detect the change
  and reattach. A restarting writer never adopts an existing segment's slots, because it cannot prove
  what state a dead writer left them in. Making that concrete — segment replacement, stale-segment
  cleanup and cleanup failure — remains open work.
- **Reader crash.** Nothing. A reader owns no cell of the segment, holds no lock, and publishes no
  progress. Its death is invisible to the writer, which is the point.

## 4. Access model

### 4.1 Trust boundary

Shared state and the control channel are a **local** trust boundary. The deployment decides which
operating-system users may map a book or issue a command. The ABI's job is to make sure that the
bytes crossing that boundary carry no secret and that an incompatible or hostile reader fails closed
rather than reading ambiguous memory.

### 4.2 Permissions and mapping

- The backing object is created with restrictive owner and group permissions (owner read/write,
  group read where a deployment needs a reader group, no world access) and with a restrictive
  process umask so the mode is not widened at creation.
- Client mappings are **read-only**. No consumer needs write access to any cell of this ABI: state
  and events are written only by the single writer, and consumer cursors live in the consumer's own
  memory, not in the segment. A read-only mapping makes a buggy or hostile client unable to corrupt
  authoritative state at all, rather than merely discouraged from doing so.
- The segment name is **unpredictable and instance-scoped**: it is derived from the 128-bit
  `daemon_instance_id`, so a name is not guessable from the daemon's configuration and a stale name
  from a previous instance never resolves to the current segment.
- Attachment happens by **descriptor transfer over an authenticated local channel** rather than
  by name lookup, so that possession of the descriptor is itself the authorization and a client
  never needs a path it could race. The daemon answers an attach command on its control socket,
  after a same-user peer-credential check, with one `sendmsg` carrying the reply line and the
  read-only segment descriptor, and nothing else. Name-based attachment remains for the
  single-market tool.
- **The transferred descriptor is opened once, at creation, and never resolved from a name while
  serving.** The daemon opens its own segment file read-only in the same step that creates it —
  the one moment the name's identity is beyond doubt — checks the opened object against the file
  it just created (same device and inode, a regular file, the region's exact length), and holds
  that descriptor for its lifetime; every attach borrows it and `SCM_RIGHTS` duplicates it into
  the receiver. Re-opening the path per request would be two separate defects at once: an `open`
  on a mutable path is a *blocking* syscall on the runtime that also drives ingestion, so a FIFO
  planted at that path freezes the feed before any timeout begins, and a path quietly replaced
  with a byte-identical snapshot yields an attachment that validates every promise and is
  permanently stale, which a quiet book gives no signal of.
- **The doorbell page's descriptor is never transferred.** Parking requires a writable mapping
  of the page, and a writable mapping carries a writable *length*: a page descriptor in a
  consumer's hands is a truncation capability over an object the daemon's own writer stores
  through, and the store after `ftruncate` is a `SIGBUS` that ends ingestion. So a
  page-placement segment reached over the control channel is spin-or-poll: the reply still names
  the placement, and the consumer's first park is the typed unavailable-doorbell fault a reader
  whose page failed to open already receives. Parked waiting over a page-placement segment stays
  with same-user consumers that open the page by name. Lifting this needs a sealed or otherwise
  non-resizable shared object — Linux `memfd_create` with `F_SEAL_SHRINK`, with no macOS
  equivalent yet — which is the same object §6 names for widening the page's audience.
- **Files the daemon created are unlinked only while their paths still name them.** Both the
  control socket and every segment file record their device and inode at creation, and shutdown
  removes a path only when `lstat` still matches: a path taken over mid-run by an operator or a
  successor instance belongs to whoever holds it now, and a daemon that removed whatever its
  configured paths resolved to at exit would delete that.
- **Peer-credential validation where the platform supports it.** On this macOS host the local socket
  peer's user and process identity can be obtained, so the daemon can reject a peer outside the
  permitted user or group before transferring a descriptor. Peer identity is a check on top of file
  permissions, never a replacement for them, and process-identifier reuse is never treated as
  identity on its own.

### 4.3 No secrets

No field of this ABI carries a venue credential, an authentication token, or any other secret. The
schema declares this as the precondition `no_secrets_in_shared_state`. Venue-native market and
outcome identifiers — including a public `tokenId` — are public identity and are carried in full;
they are not secrets and are not redacted. Notification frames carry no payload at all beyond the
fact that newer data may exist.

### 4.4 Validation order a client must follow before any read

1. Map the segment **read-only**.
2. Call the header validator on the whole mapping and stop on any typed error. In particular the
   client must not read a directory entry, a state slot or a retained-event slot before this
   succeeds. The validator itself must read only the fixed header and the segment trailer: filling
   every byte between them with hostile patterns must leave the validated header identical.
3. Check the returned `daemon_instance_id` and `segment_generation` against what the attachment
   channel promised. A mismatch means the client attached to a different instance and must reattach.
4. Resolve the venue-native identities it wants to the current directory entries, taking a
   `(daemon_instance_id, segment_generation, entry_handle_generation, entry_handle)` tuple as the
   routing aid and keeping the venue-native identity as the identity it reports.
5. Establish a coherent state revision and event cursor as one logical attachment step, then consume
   events strictly after that position, re-reading state if the retained window was overtaken during
   attachment. That is a race-free attachment contract; this ABI supplies the cells it needs
   (`state_revision`, `cursor_epoch`, `cursor_position`, `slot_sequence`) but does not implement the
   step.
6. Re-run steps 2 and 3 after any notification that the segment generation changed.

A client that skips step 2 is not a client of this ABI. A foreign runtime never performs steps 2–5
itself: the Rust-owned reader helper does, and the Python and Node harnesses call it. Foreign
runtimes do not implement atomics or ABI traversal, which is why the helper exists.

## 5. Deferred Linux questions

Layout-relevant open questions only:

1. Whether the same byte layout, alignment and padding hold on x86-64 Linux, and whether the 128-byte
   region alignment chosen from this host's `hw.cachelinesize` is the right value there (x86-64
   typically reports 64).
2. Whether `target_has_atomic = "32"` and `"64"` hold for the Linux target, and whether the declared
   alignments still equal `align_of::<AtomicU32>()` and `align_of::<AtomicU64>()`.
3. Whether the directory, state-slot, level and retained-event capacities chosen here remain
   adequate under the Linux memory and scheduling profile.
4. Whether the 1,024-event, 256 KiB-per-market ring default holds under the Linux memory and
   scheduling profile.

## 6. What the first implementation pins

The first implementation was latest-state publication only; the mapped backing landed with S4 and
the retained-event ring with S6 (its pins follow the original three below). The connection entries
are still ahead, and the cells above are unchanged by all of it. Three layout facts differ from, or are new to, the text
above, and are recorded here rather than left as drift:

- **The level cell is 64 bytes, not 48.** Two 24-byte decimal cells leave the level's side with
  nowhere to live, and a side packed into a decimal cell's reserved word would make the decimal cell
  mean two things. The cell is therefore price decimal, quantity decimal, a 32-bit side discriminant
  and 32-bit reserve, then a 64-bit reserve — exactly half a cache line on this host.
- **`native_family` is a 32-bit length plus eight 64-bit words.** It is a mutable byte field, so the
  same rule the retained-event payload already follows applies: it is addressed as 64-bit words whose
  little-endian concatenation is the byte string, never as bytes under a concurrent write. A family
  that does not fit the capacity is a typed publication failure, never a truncation.
- **`region_alignment` is the alignment this build assumed, not a value read from the running host.**
  It is a compile-time constant per target architecture — 128 on `aarch64-apple-darwin`, matching
  this host's `hw.cachelinesize` — and the header carries it so a reader built with a different
  assumption fails validation instead of reading a differently padded segment. Reading the host's
  real cache-line size needs a platform call this crate does not yet take, which is the §5 question
  in its concrete form.

The S6 retained-event implementation (ABI version 3) pins the following:

- **One ring per directory entry.** Ring base for entry *i* is `event_offset + i * event_capacity *
  event_stride`; the slot for absolute position *p* is at `(p & (event_capacity - 1))` slots from the
  base. Capacity is a power of two (default 1,024, matching the in-process observer default, 256 KiB
  per market), the wrap is a mask, and there is no ring header and no head counter.
- **The event slot is a 256-byte typed record, not an opaque payload.** §3.3's `payload_length` is
  realized as a native-family length over eight 64-bit words, exactly as the state slot's
  `native_family`; provenance crosses as the same discriminant words the state slot uses. An opaque
  payload would have forced a second encoding layer on every independent decoder.
- **Classification is lexicographic on the slot's stored `(cursor_epoch, cursor_position)` pair**
  against the reader's expectation: equal delivers; same-epoch greater is an overrun; greater epoch
  is a rebase; a zero sequence or a lower pair is not-yet-written. Positions reset to zero on a new
  continuity epoch and a recovery base commits zero mutations, so a rebase is invisible inside the
  ring — a poll consults the state slot's continuity epoch first, or a parked reader would wait
  forever on a hole the ring can never fill.
- **Attachment needs no new protocol.** The state slot already publishes revision, continuity epoch
  and next position inside one seqlock interval, which is §4.4 step 5's coherent tuple; attachment
  is that read plus one overtake probe of the ring, retried to the reader's own bound.
- **The event carries a bounded provenance projection**: origin, derivation, representation, native
  family, daemon and subscription generations. Connection identity, source evidence and the source
  timestamp lexeme do not cross the segment.
- **Publication rides the book writer's ordered commit path, mutations before state**, so the ring
  can never hold a silent gap behind an advertised state; a refused publication latches, publishes
  nothing further, and ends the run loudly. One segment per supervisor lifetime; a failed
  attach or reattach never changes attachment state, so a native cursor is replaced if and only if
  the call fully succeeds.
- **The foreign-runtime helper is one cdylib serving both runtimes.** It exports the C ABI for
  Python `ctypes` and a hand-rolled `napi_register_module_v1` that resolves every `napi_*` entry
  point through `dlsym(RTLD_DEFAULT, …)` at register time, leaving zero undefined `napi_*` symbols
  so the identical artifact loads under `process.dlopen` and `ctypes.CDLL`. The S9 Windows
  equivalents: `GetProcAddress(GetModuleHandle(NULL), …)`, `.dll` discovery, ACLs in place of the
  POSIX owner-only mode.

The S7a wake-up implementation (ABI version 5) pins the following:

- **The doorbell is a dedicated 32-bit word, not a view of `publication_generation`.** Linux
  `futex(2)` waits on 32 bits and the width rule forbids reaching the 64-bit generation cell at two
  widths, so the wake cell is its own word at header offset 136, mirrored by `§2`'s protocol row.
- **Doorbell placement is probed by the writer at creation and declared as a feature bit.** The
  writer maps its own freshly formatted file a second time read-only and probes a wait there;
  exactly one of `doorbell_in_header` and `doorbell_page` is set, and a reader refuses a segment
  declaring neither or both. The probe exists because the answer is genuinely unstable: the same
  `aarch64-apple-darwin` host (macOS 26.5) has been measured answering both ways across builds of
  this tree — `os_sync_wait_on_address` refusing a read-only shared mapping with `EFAULT` at one
  measurement and accepting it at a later one — while Linux 6.6 consistently accepts a futex wait
  on a read-only mapping. Neither answer is assumed and no platform table exists — the probe runs
  at every creation and the declared bit is the contract; a wait-time transient fault is absorbed
  by the operational retry policy and a persistent one surfaces typed.
- **The doorbell page is an owner-only sibling file, created exclusively and never replaced.**
  Readers map it read-write because the OS wait primitive demands a writable mapping, and never
  store to it; authoritative state stays behind read-only mappings regardless of placement. It is
  0600 rather than group-writable because a writable mapping carries a writable *length*: any
  principal that can open the page can truncate it, and the writer's next mirrored store then
  takes `SIGBUS` and kills ingestion — the harmlessness of the doorbell's contents is not the
  question. So on a page-placement platform, parked waiting reaches same-user consumers that
  open the page **by name** only; a cross-user consumer's page open fails and surfaces as the
  typed attach fault its first park reports, and spin mode, which never touches the doorbell,
  still works. The same reasoning is why the descriptor-transfer channel (§4.2) sends the
  segment's descriptor alone: handing the page across would put that truncation capability in
  every attaching consumer's hands, same-user or not, so an attachment made that way spins or
  polls and reports the same typed fault on its first park. Widening the audience — or
  transferring the page at all — needs a sealed or otherwise non-resizable shared object, which
  this ABI does not have. The
  writer never unlinks anything at the page's path either — the segment path is an operator
  argument, so an occupied path is a typed refusal naming it, and clearing a page a dead writer
  left behind is an operator action.
- **One wake per state publish or republish.** The doorbell increments on every generation bump so
  a parking reader's pre-park recheck can never miss a round, but the wake syscall posts only at
  the terminal state publish of the commit path (mutations before state), so a commit of N
  mutations costs one syscall, not N+1. Wake posting never blocks, never latches, and is counted;
  a writer that dies between a mutation publish and its state publish leaves an already-parked
  consumer to its own timeout, the same window that leaves a slot odd.
- **The dirty-index ring is per segment, one entry per state publish.** 32-byte slots under the
  §3.3 fenced construction; capacity is a power of two (default 4,096); the entry is appended
  after the state slot's seqlock closes and before the generation bump, so a woken consumer always
  finds the entry that woke it. Head discovery is one bounded scan; overrun classification is by
  stored absolute position and rebases the cursor, never silently.
- **Arrival stamps are wall clock, absent as zero, carried never restamped.** The socket read loop
  captures one wall-clock stamp per frame; frame-driven publishes carry it into the state slot
  (offset 96) and event slot (offset 232); installs and evidence-based losses stamp zero; the
  resolution-triggered state republish carries the previous state stamp forward exactly as it
  carries `commit_time`. Wall clock because two processes share no monotonic origin; consumers
  discard non-positive or implausible deltas rather than clamp.
