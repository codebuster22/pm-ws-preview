# Single-source upstream measurement

Qualification: qualified
Reason: healthy_floors_reached; measured duration: 7602.823 s.
Configured workload: two connections; 100 Limitless identifiers + 100 Polymarket conditions / 200 tokens. No event deduplication.
Upstream-only: no production IPC or foreign-runtime latency claim.

Machine: Apple M1, arm64, macOS-26.5.2-arm64-arm-64bit-Mach-O; logical CPUs=8, RAM bytes=17179869184.
Binary SHA-256: c1f9b2cd587ad79353ed5b6d322120600d7f548da4337944438d3baa32b6993c; source SHA-256: 567a7e17f14e840d8f3eea7f69fe80d4d5bd6b6aad4d27cfa275060826b4a972.

Power at launch: Now drawing from 'Battery Power';  -InternalBattery-0 (id=24313955)	82%; discharging; 10:16 remaining present: true

Timing quantiles are histogram upper bounds in microseconds; maxima are exact measured microseconds.
No-sample families show —. Batch stages are event-weighted; do not add stage percentiles.

Thread CPU timing enabled: CLOCK_THREAD_CPUTIME_ID; failed sampled reads=0; nonmonotonic spans=0.
Paired-read wall overhead (10000 samples), ns: p95=1042, p99=1084, max=5042; minimum observed nonzero CPU increment=333 ns.
Overhead is not subtracted. Elapsed minus thread CPU includes non-CPU delays and clock-sampling skew; it does not distinguish blocking from preemption. Missing CPU values are unknown, not zero.

Retained >1 ms handoff evidence: 40 batches / 40 events; thread CPU known for 40 batches.
Among CPU-known retained handoff batches: 39 below 250 µs CPU; 40 below 1 ms CPU.
Other retained audited tails: 1 batches; omitted tail witnesses: 0.
These are bounded tail samples, not a CPU-time distribution over all events.

## limitless

Book-data arrivals: 10007; total events: 17119; batches: 17119.
Subscription evidence: 100/100 (acknowledged); sent=True.
Faults: {}. Tail witnesses: 2; omitted=0.

| Family | Stage | Samples | p50 µs | p95 µs | p99 µs | p99.9 µs | max µs | >1ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ALL | json_decode | 17119 | ≤23 | ≤82 | ≤110 | ≤253 | 24717.125 | 1 |
| ALL | typed_validation | 17119 | ≤22 | ≤82 | ≤110 | ≤219 | 576.875 | 0 |
| ALL | admission_gate | 17119 | ≤4 | ≤8 | ≤12 | ≤69 | 517.958 | 0 |
| ALL | observer_audit | 17119 | ≤1 | ≤2 | ≤2 | ≤18 | 65.917 | 0 |
| ALL | receive_to_typed_handoff | 17119 | ≤49 | ≤173 | ≤225 | ≤409 | 24757.959 | 2 |
| ALL | receive_to_audited_observation | 17119 | ≤49 | ≤174 | ≤226 | ≤409 | 24758.750 | 2 |
| orderbookUpdate | json_decode | 10007 | ≤43 | ≤91 | ≤125 | ≤270 | 24717.125 | 1 |
| orderbookUpdate | typed_validation | 10007 | ≤40 | ≤91 | ≤126 | ≤266 | 576.875 | 0 |
| orderbookUpdate | admission_gate | 10007 | ≤5 | ≤9 | ≤13 | ≤72 | 323.792 | 0 |
| orderbookUpdate | observer_audit | 10007 | ≤1 | ≤2 | ≤2 | ≤13 | 65.917 | 0 |
| orderbookUpdate | receive_to_typed_handoff | 10007 | ≤89 | ≤190 | ≤247 | ≤435 | 24757.959 | 2 |
| orderbookUpdate | receive_to_audited_observation | 10007 | ≤89 | ≤191 | ≤247 | ≤436 | 24758.750 | 2 |
| newPriceData | json_decode | 0 | — | — | — | — | 0.000 | 0 |
| newPriceData | typed_validation | 0 | — | — | — | — | 0.000 | 0 |
| newPriceData | admission_gate | 0 | — | — | — | — | 0.000 | 0 |
| newPriceData | observer_audit | 0 | — | — | — | — | 0.000 | 0 |
| newPriceData | receive_to_typed_handoff | 0 | — | — | — | — | 0.000 | 0 |
| newPriceData | receive_to_audited_observation | 0 | — | — | — | — | 0.000 | 0 |
| marketCreated | json_decode | 82 | ≤12 | ≤25 | ≤41 | ≤41 | 40.083 | 0 |
| marketCreated | typed_validation | 82 | ≤12 | ≤29 | ≤32 | ≤32 | 31.292 | 0 |
| marketCreated | admission_gate | 82 | ≤3 | ≤7 | ≤30 | ≤30 | 29.417 | 0 |
| marketCreated | observer_audit | 82 | ≤1 | ≤2 | ≤2 | ≤2 | 1.500 | 0 |
| marketCreated | receive_to_typed_handoff | 82 | ≤27 | ≤63 | ≤72 | ≤72 | 71.958 | 0 |
| marketCreated | receive_to_audited_observation | 82 | ≤28 | ≤64 | ≤74 | ≤74 | 73.250 | 0 |
| marketResolved | json_decode | 127 | ≤13 | ≤23 | ≤30 | ≤110 | 109.709 | 0 |
| marketResolved | typed_validation | 127 | ≤17 | ≤31 | ≤42 | ≤133 | 132.083 | 0 |
| marketResolved | admission_gate | 127 | ≤6 | ≤10 | ≤43 | ≤116 | 115.709 | 0 |
| marketResolved | observer_audit | 127 | ≤1 | ≤2 | ≤2 | ≤5 | 4.083 | 0 |
| marketResolved | receive_to_typed_handoff | 127 | ≤34 | ≤63 | ≤151 | ≤168 | 167.167 | 0 |
| marketResolved | receive_to_audited_observation | 127 | ≤35 | ≤65 | ≤152 | ≤169 | 168.042 | 0 |
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
| unknown | json_decode | 6903 | ≤12 | ≤26 | ≤38 | ≤117 | 482.458 | 0 |
| unknown | typed_validation | 6903 | ≤12 | ≤26 | ≤41 | ≤102 | 252.292 | 0 |
| unknown | admission_gate | 6903 | ≤3 | ≤7 | ≤9 | ≤47 | 517.958 | 0 |
| unknown | observer_audit | 6903 | ≤1 | ≤2 | ≤2 | ≤20 | 46.833 | 0 |
| unknown | receive_to_typed_handoff | 6903 | ≤27 | ≤57 | ≤83 | ≤224 | 549.042 | 0 |
| unknown | receive_to_audited_observation | 6903 | ≤27 | ≤59 | ≤85 | ≤225 | 549.834 | 0 |

### Paired tail evidence

| Sequence | Stage | Elapsed µs | Thread CPU µs | Elapsed − CPU µs |
|---|---|---:|---:|---:|
| 4420 | json_decode | 24717.125 | 60.125 | 24657.000 |
| 4420 | typed_validation | 36.500 | 35.416 | 1.084 |
| 4420 | admission_gate | 4.334 | 4.334 | 0.000 |
| 4420 | observer_audit | 0.791 | 0.791 | 0.000 |
| 4420 | receive_to_typed_handoff | 24757.959 | 99.875 | 24658.084 |
| 4420 | receive_to_audited_observation | 24758.750 | 100.666 | 24658.084 |
| 4425 | json_decode | 164.500 | 83.875 | 80.625 |
| 4425 | typed_validation | 576.875 | 79.166 | 497.709 |
| 4425 | admission_gate | 323.792 | 15.292 | 308.500 |
| 4425 | observer_audit | 0.625 | 0.500 | 0.125 |
| 4425 | receive_to_typed_handoff | 1065.167 | 178.333 | 886.834 |
| 4425 | receive_to_audited_observation | 1065.792 | 178.833 | 886.959 |

## polymarket

Book-data arrivals: 525518; total events: 534300; batches: 534300.
Subscription evidence: 200/200 (observed_data); sent=True.
Faults: {}. Tail witnesses: 39; omitted=0.

| Family | Stage | Samples | p50 µs | p95 µs | p99 µs | p99.9 µs | max µs | >1ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ALL | json_decode | 534300 | ≤9 | ≤28 | ≤40 | ≤104 | 17854.292 | 10 |
| ALL | typed_validation | 534300 | ≤11 | ≤46 | ≤63 | ≤129 | 10360.667 | 21 |
| ALL | admission_gate | 534300 | ≤2 | ≤9 | ≤13 | ≤32 | 34532.625 | 6 |
| ALL | observer_audit | 534300 | ≤1 | ≤2 | ≤2 | ≤6 | 2025.834 | 1 |
| ALL | receive_to_typed_handoff | 534300 | ≤22 | ≤82 | ≤112 | ≤222 | 34583.125 | 38 |
| ALL | receive_to_audited_observation | 534300 | ≤23 | ≤83 | ≤114 | ≤224 | 34585.083 | 39 |
| book | json_decode | 530 | ≤70 | ≤182 | ≤280 | ≤371 | 370.292 | 0 |
| book | typed_validation | 530 | ≤72 | ≤191 | ≤303 | ≤832 | 831.291 | 0 |
| book | admission_gate | 530 | ≤6 | ≤14 | ≤21 | ≤41 | 40.166 | 0 |
| book | observer_audit | 530 | ≤1 | ≤2 | ≤2 | ≤6 | 5.416 | 0 |
| book | receive_to_typed_handoff | 530 | ≤149 | ≤407 | ≤613 | ≤2000 | 1128.417 | 1 |
| book | receive_to_audited_observation | 530 | ≤149 | ≤408 | ≤614 | ≤2000 | 1129.375 | 1 |
| price_change | json_decode | 524988 | ≤9 | ≤27 | ≤38 | ≤88 | 17854.292 | 10 |
| price_change | typed_validation | 524988 | ≤11 | ≤45 | ≤63 | ≤119 | 10360.667 | 21 |
| price_change | admission_gate | 524988 | ≤2 | ≤9 | ≤13 | ≤32 | 34532.625 | 6 |
| price_change | observer_audit | 524988 | ≤1 | ≤2 | ≤2 | ≤6 | 2025.834 | 1 |
| price_change | receive_to_typed_handoff | 524988 | ≤22 | ≤81 | ≤111 | ≤200 | 34583.125 | 37 |
| price_change | receive_to_audited_observation | 524988 | ≤23 | ≤83 | ≤113 | ≤201 | 34585.083 | 38 |
| last_trade_price | json_decode | 265 | ≤7 | ≤18 | ≤25 | ≤34 | 33.833 | 0 |
| last_trade_price | typed_validation | 265 | ≤10 | ≤31 | ≤50 | ≤55 | 55.000 | 0 |
| last_trade_price | admission_gate | 265 | ≤2 | ≤7 | ≤12 | ≤19 | 18.417 | 0 |
| last_trade_price | observer_audit | 265 | ≤1 | ≤1 | ≤2 | ≤5 | 4.750 | 0 |
| last_trade_price | receive_to_typed_handoff | 265 | ≤18 | ≤56 | ≤84 | ≤91 | 90.167 | 0 |
| last_trade_price | receive_to_audited_observation | 265 | ≤19 | ≤57 | ≤88 | ≤93 | 92.750 | 0 |
| tick_size_change | json_decode | 0 | — | — | — | — | 0.000 | 0 |
| tick_size_change | typed_validation | 0 | — | — | — | — | 0.000 | 0 |
| tick_size_change | admission_gate | 0 | — | — | — | — | 0.000 | 0 |
| tick_size_change | observer_audit | 0 | — | — | — | — | 0.000 | 0 |
| tick_size_change | receive_to_typed_handoff | 0 | — | — | — | — | 0.000 | 0 |
| tick_size_change | receive_to_audited_observation | 0 | — | — | — | — | 0.000 | 0 |
| best_bid_ask | json_decode | 4718 | ≤5 | ≤15 | ≤23 | ≤64 | 102.792 | 0 |
| best_bid_ask | typed_validation | 4718 | ≤8 | ≤31 | ≤48 | ≤105 | 267.542 | 0 |
| best_bid_ask | admission_gate | 4718 | ≤2 | ≤6 | ≤10 | ≤37 | 85.542 | 0 |
| best_bid_ask | observer_audit | 4718 | ≤1 | ≤1 | ≤2 | ≤29 | 53.959 | 0 |
| best_bid_ask | receive_to_typed_handoff | 4718 | ≤14 | ≤51 | ≤81 | ≤135 | 328.250 | 0 |
| best_bid_ask | receive_to_audited_observation | 4718 | ≤14 | ≤53 | ≤82 | ≤137 | 329.208 | 0 |
| new_market | json_decode | 3799 | ≤30 | ≤62 | ≤85 | ≤169 | 258.250 | 0 |
| new_market | typed_validation | 3799 | ≤13 | ≤39 | ≤54 | ≤97 | 157.166 | 0 |
| new_market | admission_gate | 3799 | ≤4 | ≤10 | ≤13 | ≤40 | 90.833 | 0 |
| new_market | observer_audit | 3799 | ≤1 | ≤2 | ≤2 | ≤8 | 52.250 | 0 |
| new_market | receive_to_typed_handoff | 3799 | ≤46 | ≤109 | ≤143 | ≤220 | 293.125 | 0 |
| new_market | receive_to_audited_observation | 3799 | ≤46 | ≤110 | ≤145 | ≤221 | 294.042 | 0 |
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

### Paired tail evidence

| Sequence | Stage | Elapsed µs | Thread CPU µs | Elapsed − CPU µs |
|---|---|---:|---:|---:|
| 16390 | json_decode | 5.250 | 5.209 | 0.041 |
| 16390 | typed_validation | 1190.959 | 14.666 | 1176.293 |
| 16390 | admission_gate | 5.000 | 4.500 | 0.500 |
| 16390 | observer_audit | 0.416 | 0.417 | -0.001 |
| 16390 | receive_to_typed_handoff | 1201.209 | 24.375 | 1176.834 |
| 16390 | receive_to_audited_observation | 1201.625 | 24.792 | 1176.833 |
| 126978 | json_decode | 23.375 | 22.375 | 1.000 |
| 126978 | typed_validation | 27.125 | 27.042 | 0.083 |
| 126978 | admission_gate | 34532.625 | 16.291 | 34516.334 |
| 126978 | observer_audit | 1.958 | 0.875 | 1.083 |
| 126978 | receive_to_typed_handoff | 34583.125 | 65.708 | 34517.417 |
| 126978 | receive_to_audited_observation | 34585.083 | 66.583 | 34518.500 |
| 127097 | json_decode | 3439.417 | 30.000 | 3409.417 |
| 127097 | typed_validation | 25.417 | 24.709 | 0.708 |
| 127097 | admission_gate | 4.416 | 4.416 | 0.000 |
| 127097 | observer_audit | 0.792 | 0.792 | 0.000 |
| 127097 | receive_to_typed_handoff | 3469.250 | 59.125 | 3410.125 |
| 127097 | receive_to_audited_observation | 3470.042 | 59.917 | 3410.125 |
| 127124 | json_decode | 14.625 | 13.958 | 0.667 |
| 127124 | typed_validation | 10360.667 | 60.875 | 10299.792 |
| 127124 | admission_gate | 9.333 | 8.667 | 0.666 |
| 127124 | observer_audit | 0.750 | 0.750 | 0.000 |
| 127124 | receive_to_typed_handoff | 10384.625 | 83.500 | 10301.125 |
| 127124 | receive_to_audited_observation | 10385.375 | 84.250 | 10301.125 |
| 127518 | json_decode | 21.041 | 20.375 | 0.666 |
| 127518 | typed_validation | 1150.459 | 67.875 | 1082.584 |
| 127518 | admission_gate | 7.916 | 7.334 | 0.582 |
| 127518 | observer_audit | 0.709 | 0.708 | 0.001 |
| 127518 | receive_to_typed_handoff | 1179.416 | 95.584 | 1083.832 |
| 127518 | receive_to_audited_observation | 1180.125 | 96.292 | 1083.833 |
| 127530 | json_decode | 7.208 | 7.291 | -0.083 |
| 127530 | typed_validation | 1925.083 | 52.625 | 1872.458 |
| 127530 | admission_gate | 167.542 | 18.625 | 148.917 |
| 127530 | observer_audit | 1.792 | 1.292 | 0.500 |
| 127530 | receive_to_typed_handoff | 2099.833 | 78.541 | 2021.292 |
| 127530 | receive_to_audited_observation | 2101.625 | 79.833 | 2021.792 |
| 128320 | json_decode | 15.917 | 15.125 | 0.792 |
| 128320 | typed_validation | 2279.000 | 94.292 | 2184.708 |
| 128320 | admission_gate | 5.791 | 5.083 | 0.708 |
| 128320 | observer_audit | 0.750 | 0.750 | 0.000 |
| 128320 | receive_to_typed_handoff | 2300.708 | 114.500 | 2186.208 |
| 128320 | receive_to_audited_observation | 2301.458 | 115.250 | 2186.208 |
| 128531 | json_decode | 16.500 | 16.084 | 0.416 |
| 128531 | typed_validation | 4300.834 | 76.833 | 4224.001 |
| 128531 | admission_gate | 11.750 | 11.000 | 0.750 |
| 128531 | observer_audit | 0.875 | 0.792 | 0.083 |
| 128531 | receive_to_typed_handoff | 4329.084 | 103.917 | 4225.167 |
| 128531 | receive_to_audited_observation | 4329.959 | 104.709 | 4225.250 |
| 128984 | json_decode | 20.333 | 13.292 | 7.041 |
| 128984 | typed_validation | 1060.292 | 21.041 | 1039.251 |
| 128984 | admission_gate | 6.625 | 6.125 | 0.500 |
| 128984 | observer_audit | 0.458 | 0.417 | 0.041 |
| 128984 | receive_to_typed_handoff | 1087.250 | 40.458 | 1046.792 |
| 128984 | receive_to_audited_observation | 1087.708 | 40.875 | 1046.833 |
| 129692 | json_decode | 1959.208 | 40.041 | 1919.167 |
| 129692 | typed_validation | 100.875 | 49.209 | 51.666 |
| 129692 | admission_gate | 5.209 | 5.083 | 0.126 |
| 129692 | observer_audit | 0.791 | 0.792 | -0.001 |
| 129692 | receive_to_typed_handoff | 2065.292 | 94.333 | 1970.959 |
| 129692 | receive_to_audited_observation | 2066.083 | 95.125 | 1970.958 |
| 129968 | json_decode | 11.708 | 11.083 | 0.625 |
| 129968 | typed_validation | 4456.584 | 29.459 | 4427.125 |
| 129968 | admission_gate | 3.458 | 3.000 | 0.458 |
| 129968 | observer_audit | 0.542 | 0.583 | -0.041 |
| 129968 | receive_to_typed_handoff | 4471.750 | 43.542 | 4428.208 |
| 129968 | receive_to_audited_observation | 4472.292 | 44.125 | 4428.167 |
| 132332 | json_decode | 14.042 | 14.000 | 0.042 |
| 132332 | typed_validation | 130.708 | 57.958 | 72.750 |
| 132332 | admission_gate | 7.833 | 7.333 | 0.500 |
| 132332 | observer_audit | 2025.834 | 33.959 | 1991.875 |
| 132332 | receive_to_typed_handoff | 152.583 | 79.291 | 73.292 |
| 132332 | receive_to_audited_observation | 2178.417 | 113.250 | 2065.167 |
| 132333 | json_decode | 18.792 | 18.833 | -0.041 |
| 132333 | typed_validation | 28.583 | 39.834 | -11.251 |
| 132333 | admission_gate | 1137.125 | 27.375 | 1109.750 |
| 132333 | observer_audit | 1.834 | 1.000 | 0.834 |
| 132333 | receive_to_typed_handoff | 1184.500 | 86.042 | 1098.458 |
| 132333 | receive_to_audited_observation | 1186.334 | 87.042 | 1099.292 |
| 133614 | json_decode | 1718.583 | 40.917 | 1677.666 |
| 133614 | typed_validation | 42.750 | 33.333 | 9.417 |
| 133614 | admission_gate | 6.000 | 5.917 | 0.083 |
| 133614 | observer_audit | 0.708 | 0.708 | 0.000 |
| 133614 | receive_to_typed_handoff | 1767.333 | 80.167 | 1687.166 |
| 133614 | receive_to_audited_observation | 1768.041 | 80.875 | 1687.166 |
| 139905 | json_decode | 215.917 | 26.375 | 189.542 |
| 139905 | typed_validation | 1422.291 | 57.250 | 1365.041 |
| 139905 | admission_gate | 64.459 | 24.333 | 40.126 |
| 139905 | observer_audit | 1.625 | 0.917 | 0.708 |
| 139905 | receive_to_typed_handoff | 1702.667 | 107.958 | 1594.709 |
| 139905 | receive_to_audited_observation | 1704.292 | 108.875 | 1595.417 |
| 147049 | json_decode | 26.500 | 21.833 | 4.667 |
| 147049 | typed_validation | 1310.458 | 103.750 | 1206.708 |
| 147049 | admission_gate | 9.583 | 9.083 | 0.500 |
| 147049 | observer_audit | 1.125 | 1.084 | 0.041 |
| 147049 | receive_to_typed_handoff | 1346.541 | 134.666 | 1211.875 |
| 147049 | receive_to_audited_observation | 1347.666 | 135.750 | 1211.916 |
| 148104 | json_decode | 27.750 | 13.708 | 14.042 |
| 148104 | typed_validation | 1445.583 | 25.125 | 1420.458 |
| 148104 | admission_gate | 7.584 | 6.917 | 0.667 |
| 148104 | observer_audit | 0.916 | 0.916 | 0.000 |
| 148104 | receive_to_typed_handoff | 1480.917 | 45.750 | 1435.167 |
| 148104 | receive_to_audited_observation | 1481.833 | 46.666 | 1435.167 |
| 149358 | json_decode | 19.375 | 18.875 | 0.500 |
| 149358 | typed_validation | 1560.042 | 40.625 | 1519.417 |
| 149358 | admission_gate | 412.666 | 16.500 | 396.166 |
| 149358 | observer_audit | 1.667 | 1.000 | 0.667 |
| 149358 | receive_to_typed_handoff | 1992.083 | 76.000 | 1916.083 |
| 149358 | receive_to_audited_observation | 1993.750 | 77.000 | 1916.750 |
| 156928 | json_decode | 59.792 | 47.791 | 12.001 |
| 156928 | typed_validation | 77.833 | 60.417 | 17.416 |
| 156928 | admission_gate | 2009.542 | 40.542 | 1969.000 |
| 156928 | observer_audit | 2.667 | 1.458 | 1.209 |
| 156928 | receive_to_typed_handoff | 2147.167 | 148.750 | 1998.417 |
| 156928 | receive_to_audited_observation | 2149.834 | 150.208 | 1999.626 |
| 162840 | json_decode | 279.542 | 254.667 | 24.875 |
| 162840 | typed_validation | 831.291 | 286.625 | 544.666 |
| 162840 | admission_gate | 17.584 | 16.708 | 0.876 |
| 162840 | observer_audit | 0.958 | 0.875 | 0.083 |
| 162840 | receive_to_typed_handoff | 1128.417 | 558.000 | 570.417 |
| 162840 | receive_to_audited_observation | 1129.375 | 558.875 | 570.500 |
| 172862 | json_decode | 1622.416 | 29.500 | 1592.916 |
| 172862 | typed_validation | 25.167 | 24.083 | 1.084 |
| 172862 | admission_gate | 4.667 | 4.584 | 0.083 |
| 172862 | observer_audit | 0.708 | 0.708 | 0.000 |
| 172862 | receive_to_typed_handoff | 1652.250 | 58.167 | 1594.083 |
| 172862 | receive_to_audited_observation | 1652.958 | 58.875 | 1594.083 |
| 228350 | json_decode | 17854.292 | 49.791 | 17804.501 |
| 228350 | typed_validation | 28.125 | 27.250 | 0.875 |
| 228350 | admission_gate | 4.542 | 4.542 | 0.000 |
| 228350 | observer_audit | 0.750 | 0.750 | 0.000 |
| 228350 | receive_to_typed_handoff | 17886.959 | 81.583 | 17805.376 |
| 228350 | receive_to_audited_observation | 17887.709 | 82.333 | 17805.376 |
| 228535 | json_decode | 1494.167 | 14.875 | 1479.292 |
| 228535 | typed_validation | 14.958 | 14.750 | 0.208 |
| 228535 | admission_gate | 2.708 | 2.708 | 0.000 |
| 228535 | observer_audit | 0.459 | 0.417 | 0.042 |
| 228535 | receive_to_typed_handoff | 1511.833 | 32.333 | 1479.500 |
| 228535 | receive_to_audited_observation | 1512.292 | 32.750 | 1479.542 |
| 228617 | json_decode | 6.083 | 6.083 | 0.000 |
| 228617 | typed_validation | 3688.667 | 18.875 | 3669.792 |
| 228617 | admission_gate | 9.125 | 7.875 | 1.250 |
| 228617 | observer_audit | 0.583 | 0.584 | -0.001 |
| 228617 | receive_to_typed_handoff | 3703.875 | 32.833 | 3671.042 |
| 228617 | receive_to_audited_observation | 3704.458 | 33.417 | 3671.041 |
| 229100 | json_decode | 2165.167 | 17.709 | 2147.458 |
| 229100 | typed_validation | 20.625 | 20.083 | 0.542 |
| 229100 | admission_gate | 3.167 | 3.125 | 0.042 |
| 229100 | observer_audit | 0.541 | 0.542 | -0.001 |
| 229100 | receive_to_typed_handoff | 2188.959 | 40.917 | 2148.042 |
| 229100 | receive_to_audited_observation | 2189.500 | 41.459 | 2148.041 |
| 256146 | json_decode | 8.916 | 8.959 | -0.043 |
| 256146 | typed_validation | 4834.084 | 49.958 | 4784.126 |
| 256146 | admission_gate | 10.500 | 9.708 | 0.792 |
| 256146 | observer_audit | 3.625 | 3.542 | 0.083 |
| 256146 | receive_to_typed_handoff | 4853.500 | 68.625 | 4784.875 |
| 256146 | receive_to_audited_observation | 4857.125 | 72.167 | 4784.958 |
| 306327 | json_decode | 1274.416 | 21.667 | 1252.749 |
| 306327 | typed_validation | 21.584 | 20.875 | 0.709 |
| 306327 | admission_gate | 3.541 | 3.500 | 0.041 |
| 306327 | observer_audit | 0.625 | 0.625 | 0.000 |
| 306327 | receive_to_typed_handoff | 1299.541 | 46.042 | 1253.499 |
| 306327 | receive_to_audited_observation | 1300.166 | 46.667 | 1253.499 |
| 328470 | json_decode | 15.917 | 15.416 | 0.501 |
| 328470 | typed_validation | 28.292 | 28.334 | -0.042 |
| 328470 | admission_gate | 5796.583 | 41.875 | 5754.708 |
| 328470 | observer_audit | 1.708 | 0.833 | 0.875 |
| 328470 | receive_to_typed_handoff | 5840.792 | 85.625 | 5755.167 |
| 328470 | receive_to_audited_observation | 5842.500 | 86.458 | 5756.042 |
| 328488 | json_decode | 5.500 | 5.500 | 0.000 |
| 328488 | typed_validation | 5.458 | 5.667 | -0.209 |
| 328488 | admission_gate | 1272.833 | 22.833 | 1250.000 |
| 328488 | observer_audit | 1.250 | 0.625 | 0.625 |
| 328488 | receive_to_typed_handoff | 1283.791 | 34.000 | 1249.791 |
| 328488 | receive_to_audited_observation | 1285.041 | 34.625 | 1250.416 |
| 328587 | json_decode | 18.209 | 17.125 | 1.084 |
| 328587 | typed_validation | 2261.208 | 47.125 | 2214.083 |
| 328587 | admission_gate | 6.667 | 5.667 | 1.000 |
| 328587 | observer_audit | 0.708 | 0.667 | 0.041 |
| 328587 | receive_to_typed_handoff | 2286.084 | 69.917 | 2216.167 |
| 328587 | receive_to_audited_observation | 2286.792 | 70.584 | 2216.208 |
| 328595 | json_decode | 8.458 | 8.458 | 0.000 |
| 328595 | typed_validation | 1196.500 | 29.833 | 1166.667 |
| 328595 | admission_gate | 1534.333 | 19.959 | 1514.374 |
| 328595 | observer_audit | 1.209 | 0.791 | 0.418 |
| 328595 | receive_to_typed_handoff | 2739.291 | 58.250 | 2681.041 |
| 328595 | receive_to_audited_observation | 2740.500 | 59.041 | 2681.459 |
| 328603 | json_decode | 20.167 | 19.000 | 1.167 |
| 328603 | typed_validation | 4465.500 | 52.917 | 4412.583 |
| 328603 | admission_gate | 11.208 | 10.375 | 0.833 |
| 328603 | observer_audit | 0.917 | 0.917 | 0.000 |
| 328603 | receive_to_typed_handoff | 4496.875 | 82.292 | 4414.583 |
| 328603 | receive_to_audited_observation | 4497.792 | 83.209 | 4414.583 |
| 349770 | json_decode | 261.750 | 36.625 | 225.125 |
| 349770 | typed_validation | 977.792 | 48.708 | 929.084 |
| 349770 | admission_gate | 261.750 | 15.833 | 245.917 |
| 349770 | observer_audit | 2.625 | 0.625 | 2.000 |
| 349770 | receive_to_typed_handoff | 1501.292 | 101.166 | 1400.126 |
| 349770 | receive_to_audited_observation | 1503.917 | 101.791 | 1402.126 |
| 368928 | json_decode | 143.625 | 32.250 | 111.375 |
| 368928 | typed_validation | 1543.916 | 56.125 | 1487.791 |
| 368928 | admission_gate | 133.334 | 10.041 | 123.293 |
| 368928 | observer_audit | 0.875 | 0.750 | 0.125 |
| 368928 | receive_to_typed_handoff | 1820.875 | 98.416 | 1722.459 |
| 368928 | receive_to_audited_observation | 1821.750 | 99.166 | 1722.584 |
| 461257 | json_decode | 37.500 | 31.500 | 6.000 |
| 461257 | typed_validation | 3819.417 | 49.875 | 3769.542 |
| 461257 | admission_gate | 6.375 | 5.625 | 0.750 |
| 461257 | observer_audit | 0.791 | 0.750 | 0.041 |
| 461257 | receive_to_typed_handoff | 3863.292 | 87.000 | 3776.292 |
| 461257 | receive_to_audited_observation | 3864.083 | 87.750 | 3776.333 |
| 465157 | json_decode | 33.208 | 28.375 | 4.833 |
| 465157 | typed_validation | 2063.709 | 84.750 | 1978.959 |
| 465157 | admission_gate | 9.833 | 8.958 | 0.875 |
| 465157 | observer_audit | 0.917 | 0.875 | 0.042 |
| 465157 | receive_to_typed_handoff | 2106.750 | 122.083 | 1984.667 |
| 465157 | receive_to_audited_observation | 2107.667 | 122.958 | 1984.709 |
| 472750 | json_decode | 3370.167 | 40.792 | 3329.375 |
| 472750 | typed_validation | 44.625 | 43.292 | 1.333 |
| 472750 | admission_gate | 6.333 | 6.291 | 0.042 |
| 472750 | observer_audit | 0.833 | 0.834 | -0.001 |
| 472750 | receive_to_typed_handoff | 3421.125 | 90.375 | 3330.750 |
| 472750 | receive_to_audited_observation | 3421.958 | 91.209 | 3330.749 |
| 515725 | json_decode | 17.750 | 17.000 | 0.750 |
| 515725 | typed_validation | 1902.125 | 31.125 | 1871.000 |
| 515725 | admission_gate | 4.417 | 3.834 | 0.583 |
| 515725 | observer_audit | 0.500 | 0.458 | 0.042 |
| 515725 | receive_to_typed_handoff | 1924.292 | 51.959 | 1872.333 |
| 515725 | receive_to_audited_observation | 1924.792 | 52.417 | 1872.375 |
| 522862 | json_decode | 1077.000 | 43.250 | 1033.750 |
| 522862 | typed_validation | 114.292 | 47.750 | 66.542 |
| 522862 | admission_gate | 7.250 | 6.959 | 0.291 |
| 522862 | observer_audit | 1.083 | 1.041 | 0.042 |
| 522862 | receive_to_typed_handoff | 1198.542 | 97.959 | 1100.583 |
| 522862 | receive_to_audited_observation | 1199.625 | 99.000 | 1100.625 |
