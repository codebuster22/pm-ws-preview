# Generated-input native decoder comparison

Machine: Apple M1, 8 logical CPUs; macOS-26.5.2-arm64-arm-64bit-Mach-O.
Boundary: generated-input decode, validation and result destruction; not live handoff or IPC.
Samples per case per run: 20000; order: baseline, candidate, candidate, baseline.

Each row is one complete run. Timings are microseconds; no live or consumer claim.

| Run | Variant | Generated case | Bytes | Samples | p95 | p99 | Max |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | baseline | polymarket_price_change_2 | 319 | 20000 | 4.875 | 5.084 | 41.667 |
| 1 | baseline | polymarket_book_256x2 | 17298 | 20000 | 262.167 | 273.792 | 1507.042 |
| 1 | baseline | limitless_orderbook_256x2 | 15281 | 20000 | 231.042 | 242.417 | 4030.209 |
| 2 | candidate | polymarket_price_change_2 | 319 | 20000 | 6.167 | 7.333 | 45.458 |
| 2 | candidate | polymarket_book_256x2 | 17298 | 20000 | 262.000 | 272.791 | 4981.458 |
| 2 | candidate | limitless_orderbook_256x2 | 15281 | 20000 | 232.542 | 243.875 | 5222.833 |
| 3 | candidate | polymarket_price_change_2 | 319 | 20000 | 4.834 | 5.083 | 31.625 |
| 3 | candidate | polymarket_book_256x2 | 17298 | 20000 | 262.875 | 280.584 | 361.667 |
| 3 | candidate | limitless_orderbook_256x2 | 15281 | 20000 | 231.708 | 244.000 | 4798.917 |
| 4 | baseline | polymarket_price_change_2 | 319 | 20000 | 4.875 | 5.167 | 37.291 |
| 4 | baseline | polymarket_book_256x2 | 17298 | 20000 | 287.500 | 347.875 | 5717.334 |
| 4 | baseline | limitless_orderbook_256x2 | 15281 | 20000 | 231.583 | 257.750 | 1822.459 |

## Provenance

- baseline binary SHA-256: `cf5274b76b8705501d908417ad6afd51fb48581357619f0e921eb5fb667dcaf5`.
- candidate binary SHA-256: `d91d44cadc1fdf2d9b1ef9d1f9cd3bbb2609c6549813f84e8ca1705f4ff5aa3a`.

## Power state

power before:

```text
Now drawing from 'Battery Power'
 -InternalBattery-0 (id=24313955)	27%; discharging; 3:32 remaining present: true
```

power after:

```text
Now drawing from 'Battery Power'
 -InternalBattery-0 (id=24313955)	27%; discharging; 3:22 remaining present: true
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
