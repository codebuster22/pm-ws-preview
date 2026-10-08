# Generated-input native decoder comparison

Machine: Apple M1, 8 logical CPUs; macOS-26.5.2-arm64-arm-64bit-Mach-O.
Boundary: generated-input decode, validation and result destruction; not live handoff or IPC.
Samples per case per run: 20000; order: baseline, candidate, candidate, baseline.

Each row is one complete run. Timings are microseconds; no live or consumer claim.

| Run | Variant | Generated case | Bytes | Samples | p95 | p99 | Max |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | baseline | polymarket_price_change_2 | 319 | 20000 | 4.834 | 5.084 | 470.875 |
| 1 | baseline | polymarket_book_256x2 | 17298 | 20000 | 262.208 | 274.500 | 578.375 |
| 1 | baseline | limitless_orderbook_256x2 | 15281 | 20000 | 255.541 | 264.208 | 1241.541 |
| 2 | candidate | polymarket_price_change_2 | 319 | 20000 | 9.542 | 9.916 | 62.250 |
| 2 | candidate | polymarket_book_256x2 | 17298 | 20000 | 199.083 | 208.458 | 315.000 |
| 2 | candidate | limitless_orderbook_256x2 | 15281 | 20000 | 160.500 | 176.250 | 5919.917 |
| 3 | candidate | polymarket_price_change_2 | 319 | 20000 | 6.041 | 9.792 | 6790.833 |
| 3 | candidate | polymarket_book_256x2 | 17298 | 20000 | 218.292 | 221.541 | 551.291 |
| 3 | candidate | limitless_orderbook_256x2 | 15281 | 20000 | 158.541 | 168.041 | 634.875 |
| 4 | baseline | polymarket_price_change_2 | 319 | 20000 | 4.792 | 5.042 | 44.667 |
| 4 | baseline | polymarket_book_256x2 | 17298 | 20000 | 262.625 | 273.750 | 5005.459 |
| 4 | baseline | limitless_orderbook_256x2 | 15281 | 20000 | 232.708 | 242.292 | 1737.292 |

## Provenance

- baseline binary SHA-256: `d91d44cadc1fdf2d9b1ef9d1f9cd3bbb2609c6549813f84e8ca1705f4ff5aa3a`.
- candidate binary SHA-256: `f21c8e4fd88525d69c7d7f73e0feaec11eb83152f54b84747192aa95748b5727`.

## Power state

power before:

```text
Now drawing from 'Battery Power'
 -InternalBattery-0 (id=24313955)	16%; discharging; 2:08 remaining present: true
```

power after:

```text
Now drawing from 'Battery Power'
 -InternalBattery-0 (id=24313955)	16%; discharging; 2:08 remaining present: true
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
