# Generated-input native decoder comparison

Machine: Apple M1, 8 logical CPUs; macOS-26.5.2-arm64-arm-64bit-Mach-O.
Boundary: generated-input decode, validation and result destruction; not live handoff or IPC.
Samples per case per run: 20000; order: baseline, candidate, candidate, baseline.

Each row is one complete run. Timings are microseconds; no live or consumer claim.

| Run | Variant | Generated case | Bytes | Samples | p95 | p99 | Max |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | baseline | polymarket_price_change_2 | 319 | 20000 | 5.334 | 5.792 | 32.750 |
| 1 | baseline | polymarket_book_256x2 | 17298 | 20000 | 197.959 | 207.875 | 722.625 |
| 1 | baseline | limitless_orderbook_256x2 | 15281 | 20000 | 157.792 | 166.417 | 470.375 |
| 2 | candidate | polymarket_price_change_2 | 319 | 20000 | 7.292 | 7.708 | 55.333 |
| 2 | candidate | polymarket_book_256x2 | 17298 | 20000 | 248.166 | 261.167 | 834.709 |
| 2 | candidate | limitless_orderbook_256x2 | 15281 | 20000 | 239.375 | 243.542 | 6644.083 |
| 3 | candidate | polymarket_price_change_2 | 319 | 20000 | 6.375 | 12.709 | 33.416 |
| 3 | candidate | polymarket_book_256x2 | 17298 | 20000 | 249.291 | 261.041 | 3822.875 |
| 3 | candidate | limitless_orderbook_256x2 | 15281 | 20000 | 217.750 | 226.583 | 4611.708 |
| 4 | baseline | polymarket_price_change_2 | 319 | 20000 | 5.167 | 5.250 | 31.542 |
| 4 | baseline | polymarket_book_256x2 | 17298 | 20000 | 197.042 | 206.792 | 1287.500 |
| 4 | baseline | limitless_orderbook_256x2 | 15281 | 20000 | 156.917 | 166.916 | 1525.625 |

## Same-call stages

Stages are recorded within each complete call. Do not add independent percentiles.
Parse includes complete document construction; validation includes exact economic
string conversion and venue metadata. Destruction is timed separately. These are
not the live runner's broader routing/admission stage boundaries.

| Run | Variant | Case | Stage | p95 µs | p99 µs | Max µs |
| --- | --- | --- | --- | ---: | ---: | ---: |
| 1 | baseline | polymarket_price_change_2 | destruction | 0.375 | 0.417 | 15.083 |
| 1 | baseline | polymarket_price_change_2 | parse_document | 1.959 | 2.167 | 21.333 |
| 1 | baseline | polymarket_price_change_2 | venue_validation | 3.042 | 3.250 | 23.708 |
| 1 | baseline | polymarket_book_256x2 | destruction | 0.416 | 0.458 | 18.875 |
| 1 | baseline | polymarket_book_256x2 | parse_document | 72.709 | 79.875 | 592.917 |
| 1 | baseline | polymarket_book_256x2 | venue_validation | 123.542 | 133.417 | 245.084 |
| 1 | baseline | limitless_orderbook_256x2 | destruction | 0.416 | 0.417 | 15.750 |
| 1 | baseline | limitless_orderbook_256x2 | parse_document | 133.542 | 142.125 | 445.125 |
| 1 | baseline | limitless_orderbook_256x2 | venue_validation | 23.333 | 24.208 | 71.708 |
| 2 | candidate | polymarket_price_change_2 | destruction | 0.458 | 0.500 | 15.792 |
| 2 | candidate | polymarket_price_change_2 | parse_document | 3.500 | 3.708 | 46.958 |
| 2 | candidate | polymarket_price_change_2 | venue_validation | 3.417 | 3.542 | 38.917 |
| 2 | candidate | polymarket_book_256x2 | destruction | 0.500 | 0.541 | 7.583 |
| 2 | candidate | polymarket_book_256x2 | parse_document | 127.625 | 134.541 | 253.833 |
| 2 | candidate | polymarket_book_256x2 | venue_validation | 122.375 | 132.542 | 677.750 |
| 2 | candidate | limitless_orderbook_256x2 | destruction | 0.417 | 0.459 | 19.834 |
| 2 | candidate | limitless_orderbook_256x2 | parse_document | 213.667 | 217.292 | 6612.542 |
| 2 | candidate | limitless_orderbook_256x2 | venue_validation | 25.292 | 25.958 | 107.834 |
| 3 | candidate | polymarket_price_change_2 | destruction | 0.375 | 0.417 | 13.334 |
| 3 | candidate | polymarket_price_change_2 | parse_document | 3.084 | 9.416 | 18.667 |
| 3 | candidate | polymarket_price_change_2 | venue_validation | 2.959 | 3.000 | 30.125 |
| 3 | candidate | polymarket_book_256x2 | destruction | 0.459 | 0.500 | 32.500 |
| 3 | candidate | polymarket_book_256x2 | parse_document | 128.542 | 134.167 | 197.542 |
| 3 | candidate | polymarket_book_256x2 | venue_validation | 122.500 | 132.416 | 3697.583 |
| 3 | candidate | limitless_orderbook_256x2 | destruction | 0.375 | 0.417 | 10.792 |
| 3 | candidate | limitless_orderbook_256x2 | parse_document | 194.625 | 203.250 | 4582.833 |
| 3 | candidate | limitless_orderbook_256x2 | venue_validation | 22.459 | 22.834 | 72.916 |
| 4 | baseline | polymarket_price_change_2 | destruction | 0.375 | 0.375 | 4.417 |
| 4 | baseline | polymarket_price_change_2 | parse_document | 1.917 | 2.000 | 15.875 |
| 4 | baseline | polymarket_price_change_2 | venue_validation | 2.959 | 3.000 | 29.375 |
| 4 | baseline | polymarket_book_256x2 | destruction | 0.459 | 0.500 | 11.500 |
| 4 | baseline | polymarket_book_256x2 | parse_document | 71.875 | 79.291 | 200.916 |
| 4 | baseline | polymarket_book_256x2 | venue_validation | 123.250 | 132.709 | 1214.917 |
| 4 | baseline | limitless_orderbook_256x2 | destruction | 0.375 | 0.417 | 4.458 |
| 4 | baseline | limitless_orderbook_256x2 | parse_document | 132.875 | 142.041 | 1497.708 |
| 4 | baseline | limitless_orderbook_256x2 | venue_validation | 23.291 | 23.750 | 68.583 |

## Provenance

- baseline binary SHA-256: `b5d5be703b38d63f87c96ca7787f650683e81a43b7baf0d164c7b843a2ce57e6`.
- candidate binary SHA-256: `99daa2eb88f98f3002064fb441065ddd34df53ffa64f064778967f7194aaa155`.

## Power state

power before:

```text
Now drawing from 'Battery Power'
 -InternalBattery-0 (id=24313955)	82%; discharging; 9:20 remaining present: true
```

power after:

```text
Now drawing from 'Battery Power'
 -InternalBattery-0 (id=24313955)	81%; discharging; 9:10 remaining present: true
```

power settings before:

```text
Battery Power:
 Sleep On Power Button 1
 lowpowermode         1
 standby              1
 ttyskeepawake        1
 hibernatemode        3
 powernap             1
 hibernatefile        /var/vm/sleepimage
 displaysleep         0
 womp                 0
 networkoversleep     0
 sleep                1
 lessbright           1
 tcpkeepalive         1
 disksleep            10
AC Power:
 Sleep On Power Button 1
 lowpowermode         1
 standby              1
 ttyskeepawake        1
 hibernatemode        3
 powernap             1
 hibernatefile        /var/vm/sleepimage
 displaysleep         0
 womp                 0
 networkoversleep     0
 sleep                0
 tcpkeepalive         1
 disksleep            0
```

power settings after:

```text
Battery Power:
 Sleep On Power Button 1
 lowpowermode         1
 standby              1
 ttyskeepawake        1
 hibernatemode        3
 powernap             1
 hibernatefile        /var/vm/sleepimage
 displaysleep         0
 womp                 0
 networkoversleep     0
 sleep                1
 lessbright           1
 tcpkeepalive         1
 disksleep            10
AC Power:
 Sleep On Power Button 1
 lowpowermode         1
 standby              1
 ttyskeepawake        1
 hibernatemode        3
 powernap             1
 hibernatefile        /var/vm/sleepimage
 displaysleep         0
 womp                 0
 networkoversleep     0
 sleep                0
 tcpkeepalive         1
 disksleep            0
```
