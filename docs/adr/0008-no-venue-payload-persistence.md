# No venue payload persistence

Live runs and benchmarks write only metrics, bounded witnesses, configuration and provenance; no
observed WebSocket payload reaches disk, and controlled synthetic load persists its generator seeds
and parameters rather than its events. Two bounded exceptions hold venue bytes: the curated observed
frames kept as decoder regression fixtures, one file per venue, each venue addition taking its
frames with the fixture probe under a standing bounded authorization of one connection, one frame
per documented family, a byte cap and a deadline, with frames kept only after curation; and
owner-authorized bounded probe samples.

## Considered Options

- A frame archive or replay file for regression tests and benchmark reruns: rejected. It turns a
  volatile local event path into a store of venue data with its own retention and distribution
  questions, while authored and generated fixtures plus the bounded observed frames already give
  deterministic decoder coverage.
