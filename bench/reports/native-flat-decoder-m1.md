# Generated-input native decoder comparison

Machine: Apple M1, 8 logical CPUs; macOS-26.5.2-arm64-arm-64bit-Mach-O.
Boundary: generated-input decode, validation and result destruction; not live handoff or IPC.
Samples per case per run: 20000; order: baseline, candidate, candidate, baseline.

Each row is one complete run. Timings are microseconds; no live or consumer claim.

| Run | Variant | Generated case | Bytes | Samples | p95 | p99 | Max |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | baseline | polymarket_price_change_2 | 319 | 20000 | 8.959 | 11.958 | 64.458 |
| 1 | baseline | polymarket_book_256x2 | 17298 | 20000 | 290.584 | 334.459 | 449.291 |
| 1 | baseline | limitless_orderbook_256x2 | 15281 | 20000 | 232.333 | 243.500 | 1357.584 |
| 2 | candidate | polymarket_price_change_2 | 319 | 20000 | 9.875 | 10.125 | 48.375 |
| 2 | candidate | polymarket_book_256x2 | 17298 | 20000 | 238.834 | 242.667 | 3557.333 |
| 2 | candidate | limitless_orderbook_256x2 | 15281 | 20000 | 221.625 | 314.375 | 1698.292 |
| 3 | candidate | polymarket_price_change_2 | 319 | 20000 | 5.417 | 5.500 | 32.209 |
| 3 | candidate | polymarket_book_256x2 | 17298 | 20000 | 216.417 | 232.166 | 3740.291 |
| 3 | candidate | limitless_orderbook_256x2 | 15281 | 20000 | 174.208 | 183.333 | 4335.708 |
| 4 | baseline | polymarket_price_change_2 | 319 | 20000 | 4.875 | 5.125 | 52.208 |
| 4 | baseline | polymarket_book_256x2 | 17298 | 20000 | 262.916 | 273.292 | 355.541 |
| 4 | baseline | limitless_orderbook_256x2 | 15281 | 20000 | 231.542 | 243.250 | 3932.167 |

## Provenance

- baseline binary SHA-256: `d91d44cadc1fdf2d9b1ef9d1f9cd3bbb2609c6549813f84e8ca1705f4ff5aa3a`.
- candidate binary SHA-256: `d95ba6d8f8967ea0b240927899e534931a5f4048485dddcb4046abd5a0161d82`.

## Power state

power before:

```text
Now drawing from 'Battery Power'
 -InternalBattery-0 (id=24313955)	19%; discharging; 2:11 remaining present: true
```

power after:

```text
Now drawing from 'Battery Power'
 -InternalBattery-0 (id=24313955)	19%; discharging; 2:01 remaining present: true
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
