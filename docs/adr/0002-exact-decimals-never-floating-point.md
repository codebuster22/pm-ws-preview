# Exact decimals, never floating point

Venue prices and sizes arrive as decimal lexemes whose precision a binary float cannot hold, so
every economic number and every other numeric field is parsed from its source lexeme into an
exact integer coefficient plus scale, and the lexeme is preserved beside the value. A value
outside the declared grammar — digits, scale, exponent range, lexeme length — is rejected
explicitly rather than rounded, because a rounded price is a number the venue never published.

## Considered Options

- `f64` (or any binary float) for prices and sizes: rejected. It rounds silently, and the error
  is invisible at the point it is introduced.
- Rounding or normalizing to a fixed tick: rejected. It invents economics; scaling is a
  downstream decision.
- The numeric types of a venue SDK: rejected. They convert through floating point, which makes
  them unusable as the numeric authority for pm-ws.
