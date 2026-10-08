# One owning socket per market, every arrival published

Each current market is carried by exactly one venue WebSocket, and one WebSocket carries many
markets; every valid arrival on that owning connection is published, including a repeated native
identity, an arrival whose content equals the previous one, and the same identity carrying
different source content. Native identity — the venue timestamps, versions and hashes each family
carries ([Limitless](../limitless.md), [Polymarket](../polymarket.md)) — is delivered as data and
never used as a local filter, so no identity comparison, payload hashing or content witness runs
on the admission path and freshness or equality policy stays with the consumer that needs it.

## Considered Options

- Connection pools or replicas per market with deduplication or conflict fences: rejected.
  Suppressing an arrival makes pm-ws assert a delivery policy the venue never stated, a
  suppressed conflict destroys the evidence that the venue sent two different things, and a race
  between replicas turns into silent loss instead of an explicit gap.
