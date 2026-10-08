# Bounded everything, drop never backpressure

Every queue, retention log, payload and diagnostic buffer has a declared bound and a declared
overflow outcome, and an overflow is explicit local loss reported as a gap on the affected stream
— never a truncated event, an invisible partial batch or an unbounded buffer. A consumer
therefore cannot backpressure ingestion: a slow or crashed reader loses events and receives an
explicit gap naming the affected stream, while the owning connection keeps draining its socket.
Loss reporting itself may not depend on room in a full data queue, so bounded sticky health and
gap state carry the loss even when the data path is refusing work.

## Considered Options

- Blocking admission, or backpressuring the venue connection, until a slow reader catches up:
  rejected. One slow reader would then stall every other reader and the socket, the venue does
  not resend what a full kernel buffer drops, and the resulting loss would be attributed to the
  venue instead of to this process.
