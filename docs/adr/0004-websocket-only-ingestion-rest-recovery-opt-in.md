# WebSocket-only ingestion, REST recovery opt-in

Every native event enters through a venue WebSocket; recovery is reconciliation of the desired
set and resubscription first, then reconnect, each behind a connection-generation fence that
exposes the resulting gap boundary. REST serves external discovery and selection under the
etiquette caps and stays off the update path; any REST recovery is an explicit opt-in, because a
REST read returns current state rather than the event stream, so admitting it would put data with
no event provenance or continuity into a stream whose guarantees are defined over events — and a
blocking HTTP call inside ingestion would stall the socket that is still receiving.

## Considered Options

- Fetching a REST snapshot to repair a continuity gap: rejected. A fresh snapshot does not
  restore the events that were missed, so it would replace an explicit gap with a silent one,
  and it adds venue request budget to a failure path that already needs pacing.
