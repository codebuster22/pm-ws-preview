# Authenticated market-data feeds are in scope

A venue's market-data feed is in scope whether the venue serves it openly or behind a key: the
boundary pm-ws keeps is market data versus account data, so order-book, price, trade and
market-lifecycle feeds are in scope even when the handshake needs a credential, and account, order
and wallet feeds never are; which of a venue's market-data feeds are actually selected stays that
venue contract's call.

One credential per venue lives at `~/.pm-ws/configs/<venue>.key`, a JSON object holding the fields
that venue's adapter declares, mode 0600 in a mode 0700 directory, bounded in size, and read again
at every connect. The `pmwsd credential` subcommands capture, verify and clear it; verification is
one handshake that the daemon performs against the venue and whose outcome it reports. A handshake
refused with HTTP 401 or 403, or a venue message naming authentication, is the fault
`credential_rejected`; that venue makes no further attempt until the operator names it again, with
no retry and no timer. A lease at a venue without a usable credential is refused as
`credential_required`; an operator pin at the same venue is accepted. Clearing a credential leaves
its live connections up until they drop on their own or demand frees them.

## Considered Options

- Open feeds only: rejected. Kalshi signs every WebSocket handshake and predict.fun keys it, both
  for public order-book data, so the rule would exclude major venues for a reason unrelated to what
  the daemon does with the data.
- Account and order feeds under the same daemon: rejected. Those feeds carry a client's own orders
  and positions; they belong to a separate product so that a market-data daemon never holds trading
  authority.
- Retrying a rejected credential with backoff: rejected. The ledger would be spent on a cause only
  the operator can fix.
- The daemon receiving the secret over the control socket: rejected. The file is still needed across
  a restart, so that would be two paths for one fact.
- An opaque per-venue file the adapter alone parses: rejected. The core writes the file from the
  declared fields, so the format stays common.
- Etiquette keyed by credential: rejected. One credential per venue already makes it the venue.
- Refusing operator pins without a credential: rejected. Demand and credentials stay independent;
  the consumer is refused because it cannot fix the cause.
- Retiring live connections on clear: rejected, owner's call. A downstream consumer would be cut
  off.

## Consequences

Nothing on the update path changes: the credential file is read at connect, the verification
handshake and the `credential` command run outside it, and application messages are decoded and
admitted as on an open feed. The venue-addition sequence records a Credential section for every venue
and ships a synthetic-fixture parser test for a keyed one. Fault now covers the handshake as well as
admission. No venue in the tree uses a credential yet; the connect path gains header injection with
the first keyed venue, where decoding a PEM raises a dependency question for owner approval.
