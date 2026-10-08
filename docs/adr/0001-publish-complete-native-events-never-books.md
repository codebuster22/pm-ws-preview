# Publish complete native events, never books

`pm-ws` reproduces complete venue-native events — every source field and array in source order,
exact numbers, native identity and provenance — and builds no order book, no latest-state object
and no locally derived deltas. A book is one interpretation of the source events, and a consumer
that needs one has to know which interpretation it got; publishing the events themselves keeps
venue truth intact and leaves book construction, freshness policy and economic normalization
downstream where they can differ per reader.

## Considered Options

- An authoritative order book with a shared-memory latest-state object and a mutation ring:
  rejected. It couples every read of an event to a coherent read of a separate mutable object,
  puts a book writer on the delivery path, and hides which source events produced a level.
- A separate `pm-book` product built on pm-ws: not in this crate's scope; nothing in the
  event path may exist only to serve it.
- A NATS compatibility bridge so existing strategies keep their current wire format:
  rejected. Strategies read the native events, rather than a translation whose fidelity would
  have to be proved event by event.
