# Generated-input native decoder comparison

Machine: Apple M1, 8 logical CPUs; macOS-26.5.2-arm64-arm-64bit-Mach-O.
Boundary: generated-input decode, validation and result destruction; not live handoff or IPC.
Samples per case per run: 20000; order: baseline, candidate, candidate, baseline.

Each row is one complete run. Timings are microseconds; no live or consumer claim.

| Run | Variant | Generated case | Bytes | Samples | p95 | p99 | Max |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | baseline | polymarket_price_change_2 | 319 | 20000 | 8.833 | 10.916 | 51.708 |
| 1 | baseline | polymarket_book_256x2 | 17298 | 20000 | 262.167 | 271.792 | 5749.000 |
| 1 | baseline | limitless_orderbook_256x2 | 15281 | 20000 | 230.708 | 240.334 | 1682.542 |
| 2 | candidate | polymarket_price_change_2 | 319 | 20000 | 5.083 | 7.834 | 49.833 |
| 2 | candidate | polymarket_book_256x2 | 17298 | 20000 | 285.500 | 288.792 | 769.333 |
| 2 | candidate | limitless_orderbook_256x2 | 15281 | 20000 | 242.584 | 252.541 | 4332.084 |
| 3 | candidate | polymarket_price_change_2 | 319 | 20000 | 4.500 | 4.708 | 24.000 |
| 3 | candidate | polymarket_book_256x2 | 17298 | 20000 | 260.125 | 271.500 | 3911.542 |
| 3 | candidate | limitless_orderbook_256x2 | 15281 | 20000 | 242.542 | 253.709 | 354.083 |
| 4 | baseline | polymarket_price_change_2 | 319 | 20000 | 4.875 | 5.083 | 37.416 |
| 4 | baseline | polymarket_book_256x2 | 17298 | 20000 | 264.583 | 290.125 | 3582.125 |
| 4 | baseline | limitless_orderbook_256x2 | 15281 | 20000 | 234.000 | 257.583 | 356.084 |

## Provenance

- baseline binary SHA-256: `cf5274b76b8705501d908417ad6afd51fb48581357619f0e921eb5fb667dcaf5`.
- candidate binary SHA-256: `f86d719bcc7370e1e26abd290a056d66cbb45c568d9b01ddfccaef0f4a659382`.

## Power state

power before:

```text
Now drawing from 'Battery Power'
 -InternalBattery-0 (id=24313955)	30%; discharging; 3:40 remaining present: true
```

power after:

```text
Now drawing from 'Battery Power'
 -InternalBattery-0 (id=24313955)	30%; discharging; 3:30 remaining present: true
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
