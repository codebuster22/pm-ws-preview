# Demand is operator pins and connection-bound client leases

A target is wanted while an operator pin holds it or at least one client lease counts it. Pins
belong to the operator, survive every consumer, and change only through the operator's own verbs:
pin, unpin and replace. A client lease belongs to a session, one client's control connection, and
lives exactly as long as that connection: it is released by an explicit release or by the
connection closing, never by time. There is no renewal, no time-to-live, no expiry sweep and no
timer on either side. A session is not resumable, so a consumer whose control connection drops
opens a new session and leases again.

Drain is the operator's override. A drained target is unsubscribed regardless of the demand
standing on it, and it refuses new leases with the code `drained` until it is undrained. A
qualification run runs frozen: a command that would change its desired set is refused with the code
`frozen` and counted in the run's report rather than ending the run, while status and leases on
already-pinned targets continue to work. The controller supplies complete venue-native identity for
every target it names; the daemon never discovers.

## Considered Options

- Client leases with a time-to-live and a renew verb: rejected. It puts a timer in every consumer
  in three languages, while the daemon is reactive rather than time-driven and clocks serve absence
  detection only. What it guards against is bounded anyway: a hung consumer holds its leases, the
  venue subscriptions nobody is reading are visible in status, and drain or a restart of that
  consumer remedies them, while a crashed consumer releases everything at once through its closed
  socket.
- Resumable session tokens: rejected. Leasing again costs nothing, and a session holds nothing else
  to resume.
- A one-shot drain that a client may immediately lease back: rejected. A reactive controller undoes
  it with its next command, so it would be no override at all.
- Ending a qualification run whenever a command changes its demand: rejected. Refusal keeps a
  four-hour run alive, and the disqualifier still holds because the desired set never departs from
  the frozen selection.
- Resolving a Polymarket market's asset ids from its condition ID inside the daemon: rejected. It
  would add an HTTP client dependency and put REST on the control path; selection, databases and
  discovery stay external.

## Consequences

Reader libraries carry no renewal timer and lease again after a reconnect, so a consumer author
never sees a session. Neither the wire nor the configuration carries a renew verb or a lease
time-to-live. A hung consumer appears in status as a session still holding its leases. A drained
target answers `drained`, and a frozen qualification run reports the refusals it counted, so its
freeze flag alone is what holds its desired set to the frozen selection. Wanted is a set union over
live sessions, so releasing one session's lease never touches another session's claim on the same
target. In the glossary, Client lease reads releasable, not renewable.
