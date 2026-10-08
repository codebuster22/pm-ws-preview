# Local transport chosen by measurement, not assumed

No local transport is selected: the upstream handoff is transport-independent, so event meaning
and upstream ownership are fixed before any byte layout or public ABI. The candidates are a
brokerless Unix-domain-socket baseline, an existing shared-memory pub/sub implementation such as
iceoryx2, and a direct in-process receiver baseline that exposes the cost of leaving the process;
shared memory is a candidate rather than a constraint, existing implementations are preferred over
a custom ring or ABI, and semantic fit is judged before speed.

## Consequences

Every candidate, shared-memory or byte-stream, carries the same obligations:

- A torn successful read is impossible; the permitted outcomes are a complete validated batch, a
  retry that does not advance the cursor, or an explicit retention or generation gap.
- Storage reuse rests on a memory-model argument — widths, alignment, both ordering directions,
  wrap and retry checks — not on a stress-test pass.
- A reader validates magic, schema compatibility, byte order, sizes, checked offset arithmetic,
  alignment and instance or stream generation before it touches a record.
- Data mappings are read-only to consumers, attachment is permission-checked, and no writable or
  truncatable daemon object is handed to a reader.
- A slow or crashed reader never blocks ingestion, and a candidate whose retention, cursor or
  isolation semantics do not fit is an explicit decision, not a silent contract change.
