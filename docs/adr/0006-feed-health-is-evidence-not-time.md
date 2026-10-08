# Feed health is evidence, not time

A subscribed prediction market can legitimately publish nothing for hours, so an inactivity timer
would report a healthy feed as broken and provoke reconnects that spend venue budget for nothing.
Health is therefore evidence: connectivity, heartbeat, subscription evidence, local continuity
and observed target coverage, with clocks used only to detect the absence of something the venue
guarantees — a heartbeat, an acknowledgement deadline — never to invent a market-update cadence.
A quiet subscribed target stays healthy, and lifecycle data such as a resolution is a delivered
source event, not an instruction to change health or subscription state.

## Consequences

Limitless health requires a live heartbeat plus acknowledgement of the current market set;
Polymarket sends no acknowledgement, so its health is the sent set plus separately observed asset
coverage ([Limitless](../limitless.md), [Polymarket](../polymarket.md)).
