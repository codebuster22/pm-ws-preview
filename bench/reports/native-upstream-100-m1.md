# Single-source upstream measurement

Qualification: qualified
Reason: healthy_floors_reached; measured duration: 2348.221 s.
Configured workload: two connections; 100 Limitless identifiers + 100 Polymarket conditions / 200 tokens. No event deduplication.
Upstream-only: no production IPC or foreign-runtime latency claim.

Machine: Apple M1, arm64, macOS-26.5.2-arm64-arm-64bit-Mach-O; logical CPUs=8, RAM bytes=17179869184.
Binary SHA-256: 8c581f26f94f33da0d8b34eaef97bac1ed651f5ddd57c8588422e0fb5bc9f87f; source SHA-256: bec106568539ec392df425eb763aace926b863c145822f51a5c23b0dc8052317.

Timing quantiles are histogram upper bounds in microseconds; maxima are exact measured microseconds.
No-sample families show —. Batch stages are event-weighted; do not add stage percentiles.

## limitless

Book-data arrivals: 10001; total events: 14694; batches: 14694.
Subscription evidence: 100/100 (acknowledged); sent=True.
Faults: {}. Tail witnesses: 1; omitted=0.

| Family | Stage | Samples | p50 µs | p95 µs | p99 µs | p99.9 µs | max µs | >1ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ALL | json_decode | 14694 | ≤28 | ≤86 | ≤124 | ≤240 | 1302.583 | 1 |
| ALL | typed_validation | 14694 | ≤30 | ≤96 | ≤146 | ≤255 | 400.833 | 0 |
| ALL | admission_gate | 14694 | ≤4 | ≤8 | ≤12 | ≤58 | 315.334 | 0 |
| ALL | observer_audit | 14694 | ≤1 | ≤1 | ≤1 | ≤4 | 24.667 | 0 |
| ALL | receive_to_typed_handoff | 14694 | ≤61 | ≤191 | ≤268 | ≤392 | 1430.042 | 1 |
| ALL | receive_to_audited_observation | 14694 | ≤61 | ≤191 | ≤268 | ≤393 | 1430.417 | 1 |
| orderbookUpdate | json_decode | 10001 | ≤38 | ≤92 | ≤136 | ≤255 | 1302.583 | 1 |
| orderbookUpdate | typed_validation | 10001 | ≤39 | ≤104 | ≤160 | ≤274 | 400.833 | 0 |
| orderbookUpdate | admission_gate | 10001 | ≤4 | ≤9 | ≤13 | ≤63 | 315.334 | 0 |
| orderbookUpdate | observer_audit | 10001 | ≤1 | ≤1 | ≤1 | ≤4 | 24.667 | 0 |
| orderbookUpdate | receive_to_typed_handoff | 10001 | ≤81 | ≤207 | ≤284 | ≤402 | 1430.042 | 1 |
| orderbookUpdate | receive_to_audited_observation | 10001 | ≤81 | ≤207 | ≤284 | ≤402 | 1430.417 | 1 |
| newPriceData | json_decode | 0 | — | — | — | — | 0.000 | 0 |
| newPriceData | typed_validation | 0 | — | — | — | — | 0.000 | 0 |
| newPriceData | admission_gate | 0 | — | — | — | — | 0.000 | 0 |
| newPriceData | observer_audit | 0 | — | — | — | — | 0.000 | 0 |
| newPriceData | receive_to_typed_handoff | 0 | — | — | — | — | 0.000 | 0 |
| newPriceData | receive_to_audited_observation | 0 | — | — | — | — | 0.000 | 0 |
| marketCreated | json_decode | 21 | ≤11 | ≤28 | ≤31 | ≤31 | 31.000 | 0 |
| marketCreated | typed_validation | 21 | ≤12 | ≤43 | ≤59 | ≤59 | 58.417 | 0 |
| marketCreated | admission_gate | 21 | ≤3 | ≤7 | ≤8 | ≤8 | 7.959 | 0 |
| marketCreated | observer_audit | 21 | ≤1 | ≤1 | ≤1 | ≤1 | 0.416 | 0 |
| marketCreated | receive_to_typed_handoff | 21 | ≤24 | ≤80 | ≤88 | ≤88 | 87.334 | 0 |
| marketCreated | receive_to_audited_observation | 21 | ≤25 | ≤80 | ≤88 | ≤88 | 87.750 | 0 |
| marketResolved | json_decode | 25 | ≤14 | ≤25 | ≤26 | ≤26 | 25.083 | 0 |
| marketResolved | typed_validation | 25 | ≤15 | ≤42 | ≤54 | ≤54 | 53.167 | 0 |
| marketResolved | admission_gate | 25 | ≤5 | ≤12 | ≤12 | ≤12 | 11.958 | 0 |
| marketResolved | observer_audit | 25 | ≤1 | ≤1 | ≤1 | ≤1 | 0.709 | 0 |
| marketResolved | receive_to_typed_handoff | 25 | ≤32 | ≤78 | ≤90 | ≤90 | 89.083 | 0 |
| marketResolved | receive_to_audited_observation | 25 | ≤32 | ≤79 | ≤90 | ≤90 | 89.708 | 0 |
| system | json_decode | 0 | — | — | — | — | 0.000 | 0 |
| system | typed_validation | 0 | — | — | — | — | 0.000 | 0 |
| system | admission_gate | 0 | — | — | — | — | 0.000 | 0 |
| system | observer_audit | 0 | — | — | — | — | 0.000 | 0 |
| system | receive_to_typed_handoff | 0 | — | — | — | — | 0.000 | 0 |
| system | receive_to_audited_observation | 0 | — | — | — | — | 0.000 | 0 |
| exception | json_decode | 0 | — | — | — | — | 0.000 | 0 |
| exception | typed_validation | 0 | — | — | — | — | 0.000 | 0 |
| exception | admission_gate | 0 | — | — | — | — | 0.000 | 0 |
| exception | observer_audit | 0 | — | — | — | — | 0.000 | 0 |
| exception | receive_to_typed_handoff | 0 | — | — | — | — | 0.000 | 0 |
| exception | receive_to_audited_observation | 0 | — | — | — | — | 0.000 | 0 |
| unknown | json_decode | 4647 | ≤10 | ≤26 | ≤44 | ≤121 | 231.459 | 0 |
| unknown | typed_validation | 4647 | ≤11 | ≤29 | ≤46 | ≤120 | 367.958 | 0 |
| unknown | admission_gate | 4647 | ≤3 | ≤7 | ≤10 | ≤48 | 217.625 | 0 |
| unknown | observer_audit | 4647 | ≤1 | ≤1 | ≤1 | ≤5 | 12.209 | 0 |
| unknown | receive_to_typed_handoff | 4647 | ≤23 | ≤62 | ≤96 | ≤260 | 395.125 | 0 |
| unknown | receive_to_audited_observation | 4647 | ≤23 | ≤63 | ≤96 | ≤260 | 395.500 | 0 |

## polymarket

Book-data arrivals: 98223; total events: 100646; batches: 100646.
Subscription evidence: 200/200 (observed_data); sent=True.
Faults: {}. Tail witnesses: 4; omitted=0.

| Family | Stage | Samples | p50 µs | p95 µs | p99 µs | p99.9 µs | max µs | >1ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ALL | json_decode | 100646 | ≤19 | ≤46 | ≤85 | ≤200 | 7243.209 | 2 |
| ALL | typed_validation | 100646 | ≤17 | ≤51 | ≤79 | ≤210 | 535.208 | 0 |
| ALL | admission_gate | 100646 | ≤2 | ≤7 | ≤10 | ≤36 | 408.584 | 0 |
| ALL | observer_audit | 100646 | ≤1 | ≤1 | ≤1 | ≤4 | 47.375 | 0 |
| ALL | receive_to_typed_handoff | 100646 | ≤38 | ≤103 | ≤153 | ≤395 | 7286.250 | 4 |
| ALL | receive_to_audited_observation | 100646 | ≤38 | ≤104 | ≤153 | ≤395 | 7286.459 | 4 |
| book | json_decode | 328 | ≤114 | ≤321 | ≤422 | ≤656 | 655.125 | 0 |
| book | typed_validation | 328 | ≤121 | ≤372 | ≤466 | ≤536 | 535.208 | 0 |
| book | admission_gate | 328 | ≤7 | ≤19 | ≤23 | ≤26 | 25.334 | 0 |
| book | observer_audit | 328 | ≤1 | ≤1 | ≤1 | ≤4 | 3.041 | 0 |
| book | receive_to_typed_handoff | 328 | ≤245 | ≤729 | ≤917 | ≤2000 | 1091.458 | 2 |
| book | receive_to_audited_observation | 328 | ≤246 | ≤729 | ≤917 | ≤2000 | 1091.792 | 2 |
| price_change | json_decode | 97895 | ≤19 | ≤44 | ≤62 | ≤146 | 7243.209 | 2 |
| price_change | typed_validation | 97895 | ≤17 | ≤51 | ≤75 | ≤146 | 492.459 | 0 |
| price_change | admission_gate | 97895 | ≤2 | ≤7 | ≤10 | ≤36 | 408.584 | 0 |
| price_change | observer_audit | 97895 | ≤1 | ≤1 | ≤1 | ≤4 | 47.375 | 0 |
| price_change | receive_to_typed_handoff | 97895 | ≤38 | ≤101 | ≤137 | ≤245 | 7286.250 | 2 |
| price_change | receive_to_audited_observation | 97895 | ≤38 | ≤101 | ≤138 | ≤246 | 7286.459 | 2 |
| last_trade_price | json_decode | 164 | ≤11 | ≤31 | ≤36 | ≤37 | 36.083 | 0 |
| last_trade_price | typed_validation | 164 | ≤10 | ≤34 | ≤48 | ≤61 | 60.292 | 0 |
| last_trade_price | admission_gate | 164 | ≤2 | ≤6 | ≤8 | ≤9 | 8.500 | 0 |
| last_trade_price | observer_audit | 164 | ≤1 | ≤1 | ≤1 | ≤1 | 0.417 | 0 |
| last_trade_price | receive_to_typed_handoff | 164 | ≤23 | ≤68 | ≤89 | ≤99 | 98.875 | 0 |
| last_trade_price | receive_to_audited_observation | 164 | ≤23 | ≤69 | ≤89 | ≤100 | 99.209 | 0 |
| tick_size_change | json_decode | 0 | — | — | — | — | 0.000 | 0 |
| tick_size_change | typed_validation | 0 | — | — | — | — | 0.000 | 0 |
| tick_size_change | admission_gate | 0 | — | — | — | — | 0.000 | 0 |
| tick_size_change | observer_audit | 0 | — | — | — | — | 0.000 | 0 |
| tick_size_change | receive_to_typed_handoff | 0 | — | — | — | — | 0.000 | 0 |
| tick_size_change | receive_to_audited_observation | 0 | — | — | — | — | 0.000 | 0 |
| best_bid_ask | json_decode | 1118 | ≤10 | ≤27 | ≤37 | ≤126 | 142.000 | 0 |
| best_bid_ask | typed_validation | 1118 | ≤9 | ≤36 | ≤51 | ≤114 | 166.375 | 0 |
| best_bid_ask | admission_gate | 1118 | ≤2 | ≤5 | ≤7 | ≤11 | 25.042 | 0 |
| best_bid_ask | observer_audit | 1118 | ≤1 | ≤1 | ≤1 | ≤4 | 4.417 | 0 |
| best_bid_ask | receive_to_typed_handoff | 1118 | ≤20 | ≤69 | ≤91 | ≤179 | 186.167 | 0 |
| best_bid_ask | receive_to_audited_observation | 1118 | ≤20 | ≤69 | ≤91 | ≤180 | 186.375 | 0 |
| new_market | json_decode | 1141 | ≤68 | ≤145 | ≤182 | ≤300 | 317.625 | 0 |
| new_market | typed_validation | 1141 | ≤22 | ≤52 | ≤76 | ≤165 | 316.791 | 0 |
| new_market | admission_gate | 1141 | ≤4 | ≤8 | ≤10 | ≤25 | 29.500 | 0 |
| new_market | observer_audit | 1141 | ≤1 | ≤1 | ≤1 | ≤4 | 3.166 | 0 |
| new_market | receive_to_typed_handoff | 1141 | ≤96 | ≤197 | ≤262 | ≤368 | 428.500 | 0 |
| new_market | receive_to_audited_observation | 1141 | ≤96 | ≤197 | ≤263 | ≤369 | 429.125 | 0 |
| market_resolved | json_decode | 0 | — | — | — | — | 0.000 | 0 |
| market_resolved | typed_validation | 0 | — | — | — | — | 0.000 | 0 |
| market_resolved | admission_gate | 0 | — | — | — | — | 0.000 | 0 |
| market_resolved | observer_audit | 0 | — | — | — | — | 0.000 | 0 |
| market_resolved | receive_to_typed_handoff | 0 | — | — | — | — | 0.000 | 0 |
| market_resolved | receive_to_audited_observation | 0 | — | — | — | — | 0.000 | 0 |
| pong_control | json_decode | 0 | — | — | — | — | 0.000 | 0 |
| pong_control | typed_validation | 0 | — | — | — | — | 0.000 | 0 |
| pong_control | admission_gate | 0 | — | — | — | — | 0.000 | 0 |
| pong_control | observer_audit | 0 | — | — | — | — | 0.000 | 0 |
| pong_control | receive_to_typed_handoff | 0 | — | — | — | — | 0.000 | 0 |
| pong_control | receive_to_audited_observation | 0 | — | — | — | — | 0.000 | 0 |
| unknown | json_decode | 0 | — | — | — | — | 0.000 | 0 |
| unknown | typed_validation | 0 | — | — | — | — | 0.000 | 0 |
| unknown | admission_gate | 0 | — | — | — | — | 0.000 | 0 |
| unknown | observer_audit | 0 | — | — | — | — | 0.000 | 0 |
| unknown | receive_to_typed_handoff | 0 | — | — | — | — | 0.000 | 0 |
| unknown | receive_to_audited_observation | 0 | — | — | — | — | 0.000 | 0 |
