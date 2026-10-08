# Latency target judged by tails

The optimization target is the lowest p99 the path achieves per load band, from complete local
receive to a complete usable event in a reference consumer, found by a ladder of qualification
runs at 0–10, 10–50, 50–100, 100–250, 250–500, 500–1000 and 1000–10000 markets per venue rather
than fixed in advance; the 100–250 µs p99 figure that preceded the ladder is the hypothesis its
first bands test. Each band names its upper bound inclusively and its lower bound exclusively, so
10–50 is 11 to 50 markets per venue and the accepted 2026-09-28 run is the 50–100 band's upstream
measurement. Every decision about the target uses p95 and p99 with each event over 1 ms
individually accounted for, never a mean or p50 — a path whose median is fast and whose tail is
milliseconds is the wrong path for this product. On 2026-09-28 the owner accepted the measured
upstream cost of that path over 4619.124 healthy measured seconds on an Apple M1, every handoff
over 1 ms accounted for and under 250 µs of thread CPU
([qualification](../../bench/reports/native-upstream-flat-packed-100-m1.md)).

The hero figure, one per band for each venue and book-data family, is the p99 from complete local
receive to the Rust reference consumer's complete typed event on the all-venues workload: daemon,
transport and Rust reader, the package as shipped. No hero figure exists while no consumer path
exists to measure it, and the Polymarket snapshot p99 already exceeds the 100–250 µs hypothesis at
the upstream boundary alone; the accepted figures are the upstream cost, not a hero figure.

## Considered Options

- A latency target fixed before measurement: rejected. No band had been measured when 100–250 µs
  was written, so the number was a wish, not a bound.
- The hero figure at the handoff: rejected. A number that stops inside the daemon is a cost no
  reader pays; the Rust reference consumer is the package as shipped, and the TypeScript on Bun
  and Python readers are quoted beside it with their binding cost.
- Tolerating a reconnect at the top bands: rejected. A band the venue cannot hold steady for the
  floor is a finding about that venue at that scale, recorded with its cause, not a pass with a
  footnote.
- A per-family arrival floor: rejected. Polymarket `book` is sparse and the venue declares no
  cadence — 580 arrivals against 200 subscribed assets in the accepted run — so it could meet no
  floor, while lumping families into one figure would hide it; families are reported apart and
  floored together per venue.
- Venue-side "most active" sorts: rejected. Neither venue documents one; selection ranks locally
  by a documented listing field so the policy can be quoted.

## Consequences

The ladder rules the owner settled on 2026-09-28. A band runs at its top with one selection per
venue and one workload per shape, each venue alone and all venues together, never pairs, which is
three shapes with two venues. A venue whose active population falls short of the top runs at its
full population, the count is recorded and the band above is not run for that venue. Every band
needs 900 continuous healthy measured seconds and 10,000 accepted book-data arrivals per venue
present; a band that cannot reach 10,000 inside the four-hour ceiling runs to the ceiling and
qualifies at 1,000, with p99.9 withheld and the sample count on every row. Disqualifiers are blanket
at every band. An unqualified band is attributed by cause before anything else: a local cause is a
defect fixed before the re-run, a venue-initiated cause changes the placement within etiquette and
re-runs, and a band is a venue's ceiling only once the permitted layouts are exhausted, every
attempt kept with its cause. Selection is most active first by a documented listing field, Limitless
`volume` and Polymarket `volume24hr`, ranked in the discovery store and recorded with the workload.
Every row names its machine, the measured window in UTC, per-venue messages received and events
decoded, per-family arrivals and their rate, and peak and end resident memory; memory is reported,
not gated, and machines are never compared: the M1 first, then the owner's Linux box, then EC2
instance types. Upstream figures are the rail's cost and are not re-run per transport; v1 acceptance
is the hero figure, never the upstream cost. The ladder table is generated from the run reports and
lives beside them under `bench/reports/`; this record links it once it exists.
