# Per-target delivery is daemon-side by lease on per-shard streams

Each shard publishes one stream, and a target is a routing key rather than a stream: the daemon
delivers a published batch to a session only when the batch names a target that session leases, or
when a lifecycle event names a venue whose lifecycle feed the session leases. A batch is delivered
whole and at most once per session, so a consumer never filters and never sees a sliced event or a
second copy of one arrival; a venue that sends the same event on two paths produces two arrivals and
both are delivered. Each adapter declares how its venue's lifecycle feed is obtained, subscribing it
only while demand holds it where the venue commands it independently and gating it at delivery where
the venue couples it to a required family. Sequence, generation and retention stay per stream, a
batch never spans streams, and a session reads its stream's sequence with holes while a contiguous
per-attachment delivery sequence proves local completeness. Admission appends the encoded batch
once, resolves the named targets through one dense index to the interested sessions and leaves a
descriptor per session; the per-session writes happen off the admission path, so admission cost is
independent of the total target count and of uninterested sessions, and a slow session delays only
itself. Control records, gaps and generation changes reach every session attached to their stream; a
lifecycle event naming nothing leased and an unknown-family envelope are retained and counted,
delivered to nobody, and an unknown payload is never re-parsed to invent a target.

## Considered Options

- One stream per target, or one per market carrying both Polymarket assets: rejected. Ten thousand
  targets would mean ten to thirty thousand sequence counters and retention logs, memory stranded on
  cold markets, about twenty thousand shared-memory services on iceoryx2 and about 1.9 GiB of mapped
  logs on Aeron, so the topology would decide the transport comparison instead of the measurement;
  a multi-asset event would be published onto several streams, breaking batch atomicity; and the
  per-market shape hands a one-asset consumer the sibling asset's `book`, which is consumer-side
  filtering by another name.
- Delivering a two-asset event once per matched target: rejected. Nothing more is delivered than by
  delivering it once, the second copy costs a second write and a second kernel copy, and the reader
  would have to ignore a repeat that is indistinguishable from the venue genuinely sending the same
  event twice, which every valid arrival must publish.
- Loss reported by gap records alone: rejected. A gap record can itself be lost, and a transport
  such as iceoryx2 reports no loss of its own, so the reader would be trusting the daemon; a
  contiguous delivery sequence makes local loss provable at the reader for eight bytes.
- Routing an unknown-family envelope by market fields found in its payload: rejected. The daemon
  would be guessing a schema it does not know, and native identity would become a local delivery
  filter.

## Consequences

- A session leasing one asset of a Polymarket condition receives the complete `price_change` with the
  sibling asset's entries, and every session attached to a stream receives that stream's control
  records, because pm-ws never slices a venue frame and never withholds one from a session leasing a
  target it names; an unknown-family envelope names no target the daemon can trust, so it is retained
  and counted rather than delivered.
- Retention is per stream and therefore a window in time that shrinks as more markets share a
  connection; its size is a per-band setting, and a consumer that reconnects inside the window
  continues from its cursor with nothing lost.
- The data path is a second local socket beside the control socket, which carries commands and
  status only; a session opens exactly one data attachment, named by its session id, that lives
  exactly as long as its control connection, and an attach that would exceed the configured buffer
  budget is refused.
- A local egress loss and an upstream continuity boundary are distinct gap kinds, and only the first
  ever shows as a delivery-sequence jump; the record header carries both positions and is fixed
  before the transport comparison, with byte-identical records across candidates.
- The transport comparison is the Unix-socket baseline, iceoryx2 with one service per attachment
  rather than one per target, and the in-process yardstick; a transport whose reader holds a
  back-pressure switch over the publisher cannot carry this model.
