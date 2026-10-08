# Decode once into a source-preserving document

Each application message is scanned once into one owned document with contiguous source storage
and preorder value nodes; exact numbers are parsed during that scan, venue validation promotes
economic strings without changing their source spelling, and typed views expose fields,
source-order children, exact decimals and source text without a second parse. One in-tree scanner
serves both native application documents and lexical transport and control decoding, so a
consumer never reparses venue JSON and no projection of the source shape is baked into decoding.

## Considered Options

- The Sonic SIMD parser as the document builder: rejected on this workload's own evidence.
  Measured against the in-tree decoder on three generated cases, its best-run complete
  decode-and-validation p99 regressed in every one — 5.250 to 7.708 µs, 206.792 to 261.041 µs,
  and 166.417 to 226.583 µs ([report](../../bench/reports/native-sonic-decoder-m1.md)); the
  prototype staging also broke the large-nested-input allocation guard, which admits at most
  four allocations.
- SIMD parsers, allocators or queue frameworks adopted on a microbenchmark from a different
  workload: rejected. A candidate enters on measurements of this decode boundary.
