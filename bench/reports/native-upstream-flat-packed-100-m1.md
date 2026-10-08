# Single-source upstream measurement

Qualification: qualified
Reason: healthy_floors_reached; measured duration: 4619.124 s.
Configured workload: two connections; 100 Limitless identifiers + 100 Polymarket conditions / 200 tokens. No event deduplication.
Upstream-only: no production IPC or foreign-runtime latency claim.

Machine: Apple M1, arm64, macOS-26.5.2-arm64-arm-64bit-Mach-O; logical CPUs=8, RAM bytes=17179869184.
Binary SHA-256: d57205c6d0d2509ec946ee5b32074d19c2f2495195173a4d18e2f7ff702a400e; source SHA-256: c50818248a7cae292db20af562350d70df3dceb807c696a9ae519c7501ece35b.

Power at launch: Now drawing from 'Battery Power';  -InternalBattery-0 (id=24313955)	100%; discharging; 11:00 remaining present: true

Timing quantiles are histogram upper bounds in microseconds; maxima are exact measured microseconds.
No-sample families show —. Batch stages are event-weighted; do not add stage percentiles.

Thread CPU timing enabled: CLOCK_THREAD_CPUTIME_ID; failed sampled reads=0; nonmonotonic spans=0.
Paired-read wall overhead (10000 samples), ns: p95=1083, p99=1125, max=59000; minimum observed nonzero CPU increment=291 ns.
Overhead is not subtracted. Elapsed minus thread CPU includes non-CPU delays and clock-sampling skew; it does not distinguish blocking from preemption. Missing CPU values are unknown, not zero.

Retained >1 ms handoff evidence: 9 batches / 9 events; thread CPU known for 9 batches.
Among CPU-known retained handoff batches: 9 below 250 µs CPU; 9 below 1 ms CPU.
Other retained audited tails: 0 batches; omitted tail witnesses: 0.
These are bounded tail samples, not a CPU-time distribution over all events.

## limitless

Book-data arrivals: 10004; total events: 22944; batches: 22944.
Subscription evidence: 100/100 (acknowledged); sent=True.
Faults: {}. Tail witnesses: 0; omitted=0.

| Family | Stage | Samples | p50 µs | p95 µs | p99 µs | p99.9 µs | max µs | >1ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ALL | json_decode | 22944 | ≤21 | ≤73 | ≤95 | ≤185 | 574.166 | 0 |
| ALL | typed_validation | 22944 | ≤10 | ≤35 | ≤43 | ≤97 | 214.291 | 0 |
| ALL | admission_gate | 22944 | ≤2 | ≤6 | ≤8 | ≤27 | 411.833 | 0 |
| ALL | observer_audit | 22944 | ≤1 | ≤2 | ≤2 | ≤9 | 151.917 | 0 |
| ALL | receive_to_typed_handoff | 22944 | ≤32 | ≤112 | ≤143 | ≤276 | 590.166 | 0 |
| ALL | receive_to_audited_observation | 22944 | ≤33 | ≤113 | ≤144 | ≤278 | 590.791 | 0 |
| orderbookUpdate | json_decode | 10004 | ≤45 | ≤83 | ≤107 | ≤214 | 463.500 | 0 |
| orderbookUpdate | typed_validation | 10004 | ≤20 | ≤39 | ≤49 | ≤110 | 214.291 | 0 |
| orderbookUpdate | admission_gate | 10004 | ≤3 | ≤6 | ≤8 | ≤30 | 172.542 | 0 |
| orderbookUpdate | observer_audit | 10004 | ≤1 | ≤2 | ≤2 | ≤9 | 95.167 | 0 |
| orderbookUpdate | receive_to_typed_handoff | 10004 | ≤68 | ≤126 | ≤158 | ≤308 | 502.042 | 0 |
| orderbookUpdate | receive_to_audited_observation | 10004 | ≤68 | ≤127 | ≤160 | ≤316 | 502.917 | 0 |
| newPriceData | json_decode | 0 | — | — | — | — | 0.000 | 0 |
| newPriceData | typed_validation | 0 | — | — | — | — | 0.000 | 0 |
| newPriceData | admission_gate | 0 | — | — | — | — | 0.000 | 0 |
| newPriceData | observer_audit | 0 | — | — | — | — | 0.000 | 0 |
| newPriceData | receive_to_typed_handoff | 0 | — | — | — | — | 0.000 | 0 |
| newPriceData | receive_to_audited_observation | 0 | — | — | — | — | 0.000 | 0 |
| marketCreated | json_decode | 57 | ≤14 | ≤31 | ≤39 | ≤39 | 38.666 | 0 |
| marketCreated | typed_validation | 57 | ≤8 | ≤17 | ≤21 | ≤21 | 20.125 | 0 |
| marketCreated | admission_gate | 57 | ≤2 | ≤5 | ≤10 | ≤10 | 9.375 | 0 |
| marketCreated | observer_audit | 57 | ≤1 | ≤2 | ≤2 | ≤2 | 1.292 | 0 |
| marketCreated | receive_to_typed_handoff | 57 | ≤24 | ≤51 | ≤63 | ≤63 | 62.083 | 0 |
| marketCreated | receive_to_audited_observation | 57 | ≤24 | ≤52 | ≤64 | ≤64 | 63.250 | 0 |
| marketResolved | json_decode | 52 | ≤13 | ≤31 | ≤35 | ≤35 | 34.333 | 0 |
| marketResolved | typed_validation | 52 | ≤12 | ≤26 | ≤32 | ≤32 | 32.000 | 0 |
| marketResolved | admission_gate | 52 | ≤5 | ≤10 | ≤11 | ≤11 | 10.417 | 0 |
| marketResolved | observer_audit | 52 | ≤1 | ≤2 | ≤5 | ≤5 | 4.959 | 0 |
| marketResolved | receive_to_typed_handoff | 52 | ≤29 | ≤63 | ≤65 | ≤65 | 65.000 | 0 |
| marketResolved | receive_to_audited_observation | 52 | ≤29 | ≤64 | ≤67 | ≤67 | 66.125 | 0 |
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
| unknown | json_decode | 12831 | ≤9 | ≤31 | ≤41 | ≤128 | 574.166 | 0 |
| unknown | typed_validation | 12831 | ≤4 | ≤15 | ≤18 | ≤56 | 155.709 | 0 |
| unknown | admission_gate | 12831 | ≤2 | ≤5 | ≤8 | ≤26 | 411.833 | 0 |
| unknown | observer_audit | 12831 | ≤1 | ≤2 | ≤2 | ≤6 | 151.917 | 0 |
| unknown | receive_to_typed_handoff | 12831 | ≤14 | ≤50 | ≤66 | ≤161 | 590.166 | 0 |
| unknown | receive_to_audited_observation | 12831 | ≤14 | ≤51 | ≤67 | ≤173 | 590.791 | 0 |

## polymarket

Book-data arrivals: 236255; total events: 243899; batches: 243899.
Subscription evidence: 200/200 (observed_data); sent=True.
Faults: {}. Tail witnesses: 9; omitted=0.

| Family | Stage | Samples | p50 µs | p95 µs | p99 µs | p99.9 µs | max µs | >1ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ALL | json_decode | 243899 | ≤11 | ≤30 | ≤42 | ≤120 | 21492.208 | 4 |
| ALL | typed_validation | 243899 | ≤16 | ≤49 | ≤68 | ≤149 | 32546.375 | 5 |
| ALL | admission_gate | 243899 | ≤2 | ≤5 | ≤8 | ≤14 | 193.625 | 0 |
| ALL | observer_audit | 243899 | ≤1 | ≤2 | ≤2 | ≤6 | 306.292 | 0 |
| ALL | receive_to_typed_handoff | 243899 | ≤28 | ≤85 | ≤113 | ≤258 | 32570.333 | 9 |
| ALL | receive_to_audited_observation | 243899 | ≤28 | ≤86 | ≤115 | ≤260 | 32571.125 | 9 |
| book | json_decode | 580 | ≤67 | ≤230 | ≤402 | ≤630 | 629.291 | 0 |
| book | typed_validation | 580 | ≤90 | ≤234 | ≤279 | ≤530 | 529.209 | 0 |
| book | admission_gate | 580 | ≤2 | ≤8 | ≤11 | ≤12 | 11.875 | 0 |
| book | observer_audit | 580 | ≤1 | ≤2 | ≤2 | ≤5 | 5.000 | 0 |
| book | receive_to_typed_handoff | 580 | ≤158 | ≤432 | ≤628 | ≤903 | 902.541 | 0 |
| book | receive_to_audited_observation | 580 | ≤159 | ≤433 | ≤628 | ≤904 | 903.750 | 0 |
| price_change | json_decode | 235675 | ≤11 | ≤30 | ≤40 | ≤90 | 21492.208 | 3 |
| price_change | typed_validation | 235675 | ≤16 | ≤49 | ≤67 | ≤108 | 32546.375 | 5 |
| price_change | admission_gate | 235675 | ≤2 | ≤5 | ≤8 | ≤14 | 193.625 | 0 |
| price_change | observer_audit | 235675 | ≤1 | ≤2 | ≤2 | ≤6 | 306.292 | 0 |
| price_change | receive_to_typed_handoff | 235675 | ≤28 | ≤84 | ≤111 | ≤185 | 32570.333 | 8 |
| price_change | receive_to_audited_observation | 235675 | ≤28 | ≤85 | ≤112 | ≤187 | 32571.125 | 8 |
| last_trade_price | json_decode | 288 | ≤8 | ≤19 | ≤26 | ≤36 | 35.958 | 0 |
| last_trade_price | typed_validation | 288 | ≤11 | ≤28 | ≤43 | ≤55 | 55.000 | 0 |
| last_trade_price | admission_gate | 288 | ≤1 | ≤4 | ≤6 | ≤9 | 8.125 | 0 |
| last_trade_price | observer_audit | 288 | ≤1 | ≤1 | ≤2 | ≤2 | 1.333 | 0 |
| last_trade_price | receive_to_typed_handoff | 288 | ≤19 | ≤50 | ≤73 | ≤86 | 85.541 | 0 |
| last_trade_price | receive_to_audited_observation | 288 | ≤20 | ≤51 | ≤74 | ≤87 | 86.416 | 0 |
| tick_size_change | json_decode | 0 | — | — | — | — | 0.000 | 0 |
| tick_size_change | typed_validation | 0 | — | — | — | — | 0.000 | 0 |
| tick_size_change | admission_gate | 0 | — | — | — | — | 0.000 | 0 |
| tick_size_change | observer_audit | 0 | — | — | — | — | 0.000 | 0 |
| tick_size_change | receive_to_typed_handoff | 0 | — | — | — | — | 0.000 | 0 |
| tick_size_change | receive_to_audited_observation | 0 | — | — | — | — | 0.000 | 0 |
| best_bid_ask | json_decode | 6448 | ≤6 | ≤18 | ≤27 | ≤55 | 327.167 | 0 |
| best_bid_ask | typed_validation | 6448 | ≤9 | ≤32 | ≤47 | ≤80 | 380.500 | 0 |
| best_bid_ask | admission_gate | 6448 | ≤1 | ≤4 | ≤7 | ≤14 | 76.375 | 0 |
| best_bid_ask | observer_audit | 6448 | ≤1 | ≤2 | ≤2 | ≤5 | 83.542 | 0 |
| best_bid_ask | receive_to_typed_handoff | 6448 | ≤15 | ≤55 | ≤79 | ≤129 | 397.916 | 0 |
| best_bid_ask | receive_to_audited_observation | 6448 | ≤16 | ≤56 | ≤80 | ≤132 | 399.041 | 0 |
| new_market | json_decode | 908 | ≤41 | ≤78 | ≤101 | ≤2000 | 1234.792 | 1 |
| new_market | typed_validation | 908 | ≤24 | ≤46 | ≤57 | ≤83 | 82.709 | 0 |
| new_market | admission_gate | 908 | ≤2 | ≤6 | ≤8 | ≤10 | 9.042 | 0 |
| new_market | observer_audit | 908 | ≤1 | ≤2 | ≤2 | ≤13 | 12.667 | 0 |
| new_market | receive_to_typed_handoff | 908 | ≤66 | ≤125 | ≤154 | ≤2000 | 1286.000 | 1 |
| new_market | receive_to_audited_observation | 908 | ≤67 | ≤126 | ≤155 | ≤2000 | 1287.125 | 1 |
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
| 10262 | json_decode | 30.667 | 30.041 | 0.626 |
| 10262 | typed_validation | 1073.833 | 55.792 | 1018.041 |
| 10262 | admission_gate | 6.167 | 5.542 | 0.625 |
| 10262 | observer_audit | 1.167 | 1.166 | 0.001 |
| 10262 | receive_to_typed_handoff | 1110.667 | 91.375 | 1019.292 |
| 10262 | receive_to_audited_observation | 1111.834 | 92.541 | 1019.293 |
| 24559 | json_decode | 34.125 | 25.333 | 8.792 |
| 24559 | typed_validation | 1407.625 | 41.625 | 1366.000 |
| 24559 | admission_gate | 159.375 | 14.875 | 144.500 |
| 24559 | observer_audit | 0.958 | 0.750 | 0.208 |
| 24559 | receive_to_typed_handoff | 1601.125 | 81.833 | 1519.292 |
| 24559 | receive_to_audited_observation | 1602.083 | 82.583 | 1519.500 |
| 47469 | json_decode | 14.709 | 14.667 | 0.042 |
| 47469 | typed_validation | 1277.041 | 41.542 | 1235.499 |
| 47469 | admission_gate | 3.042 | 2.666 | 0.376 |
| 47469 | observer_audit | 1.000 | 1.000 | 0.000 |
| 47469 | receive_to_typed_handoff | 1294.792 | 58.875 | 1235.917 |
| 47469 | receive_to_audited_observation | 1295.792 | 59.875 | 1235.917 |
| 120011 | json_decode | 18.042 | 17.167 | 0.875 |
| 120011 | typed_validation | 1925.208 | 36.208 | 1889.000 |
| 120011 | admission_gate | 3.417 | 2.959 | 0.458 |
| 120011 | observer_audit | 0.458 | 0.458 | 0.000 |
| 120011 | receive_to_typed_handoff | 1946.667 | 56.334 | 1890.333 |
| 120011 | receive_to_audited_observation | 1947.125 | 56.792 | 1890.333 |
| 120014 | json_decode | 18.125 | 17.375 | 0.750 |
| 120014 | typed_validation | 32546.375 | 51.708 | 32494.667 |
| 120014 | admission_gate | 5.833 | 4.584 | 1.249 |
| 120014 | observer_audit | 0.792 | 0.791 | 0.001 |
| 120014 | receive_to_typed_handoff | 32570.333 | 73.667 | 32496.666 |
| 120014 | receive_to_audited_observation | 32571.125 | 74.458 | 32496.667 |
| 120017 | json_decode | 1822.667 | 21.750 | 1800.917 |
| 120017 | typed_validation | 18.833 | 18.459 | 0.374 |
| 120017 | admission_gate | 1.750 | 1.791 | -0.041 |
| 120017 | observer_audit | 0.417 | 0.375 | 0.042 |
| 120017 | receive_to_typed_handoff | 1843.250 | 42.000 | 1801.250 |
| 120017 | receive_to_audited_observation | 1843.667 | 42.375 | 1801.292 |
| 120020 | json_decode | 21492.208 | 54.333 | 21437.875 |
| 120020 | typed_validation | 34.792 | 33.917 | 0.875 |
| 120020 | admission_gate | 3.625 | 3.625 | 0.000 |
| 120020 | observer_audit | 0.750 | 0.750 | 0.000 |
| 120020 | receive_to_typed_handoff | 21530.625 | 91.875 | 21438.750 |
| 120020 | receive_to_audited_observation | 21531.375 | 92.625 | 21438.750 |
| 120022 | json_decode | 3109.000 | 34.583 | 3074.417 |
| 120022 | typed_validation | 33.125 | 31.959 | 1.166 |
| 120022 | admission_gate | 3.625 | 3.625 | 0.000 |
| 120022 | observer_audit | 0.666 | 0.666 | 0.000 |
| 120022 | receive_to_typed_handoff | 3145.750 | 70.167 | 3075.583 |
| 120022 | receive_to_audited_observation | 3146.416 | 70.833 | 3075.583 |
| 222996 | json_decode | 1234.792 | 73.334 | 1161.458 |
| 222996 | typed_validation | 45.875 | 45.458 | 0.417 |
| 222996 | admission_gate | 5.333 | 5.250 | 0.083 |
| 222996 | observer_audit | 1.125 | 1.125 | 0.000 |
| 222996 | receive_to_typed_handoff | 1286.000 | 124.042 | 1161.958 |
| 222996 | receive_to_audited_observation | 1287.125 | 125.167 | 1161.958 |
