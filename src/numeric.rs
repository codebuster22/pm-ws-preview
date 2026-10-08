use core::{cmp::Ordering, fmt};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct DecimalGrammar {
    pub max_scale: u16,
    pub max_digits: u16,
    pub allow_exponent: bool,
    pub allow_negative: bool,
    pub max_lexeme_bytes: usize,
    pub max_exponent_digits: u8,
    pub max_exponent_magnitude: u32,
}

impl DecimalGrammar {
    pub fn new(
        max_scale: u16,
        max_digits: u16,
        allow_exponent: bool,
        allow_negative: bool,
    ) -> Result<Self, DecimalError> {
        if max_digits == 0 || max_digits > 39 {
            return Err(DecimalError::RepresentationLimit);
        }
        Ok(Self {
            max_scale,
            max_digits,
            allow_exponent,
            allow_negative,
            max_lexeme_bytes: 128,
            max_exponent_digits: 5,
            max_exponent_magnitude: 99_999,
        })
    }
    pub fn with_limits(
        mut self,
        max_lexeme_bytes: usize,
        max_exponent_digits: u8,
        max_exponent_magnitude: u32,
    ) -> Result<Self, DecimalError> {
        if max_lexeme_bytes == 0 || max_exponent_digits == 0 || max_exponent_magnitude == 0 {
            return Err(DecimalError::RepresentationLimit);
        }
        self.max_lexeme_bytes = max_lexeme_bytes;
        self.max_exponent_digits = max_exponent_digits;
        self.max_exponent_magnitude = max_exponent_magnitude;
        Ok(self)
    }
}
impl<'de> Deserialize<'de> for DecimalGrammar {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Wire {
            max_scale: u16,
            max_digits: u16,
            allow_exponent: bool,
            allow_negative: bool,
            max_lexeme_bytes: usize,
            max_exponent_digits: u8,
            max_exponent_magnitude: u32,
        }
        let wire = Wire::deserialize(deserializer)?;
        Self::new(
            wire.max_scale,
            wire.max_digits,
            wire.allow_exponent,
            wire.allow_negative,
        )
        .and_then(|grammar| {
            grammar.with_limits(
                wire.max_lexeme_bytes,
                wire.max_exponent_digits,
                wire.max_exponent_magnitude,
            )
        })
        .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecimalError {
    Empty,
    InvalidLexeme,
    ExponentForbidden,
    ScaleOverflow,
    PrecisionExceeded,
    CoefficientOverflow,
    NegativeForbidden,
    ArithmeticOverflow,
    RepresentationLimit,
    LexemeTooLong,
    ExponentTooLarge,
}

impl fmt::Display for DecimalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid exact decimal: {self:?}")
    }
}
impl std::error::Error for DecimalError {}

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct ExactDecimal {
    coefficient: i128,
    scale: u16,
}

impl fmt::Debug for ExactDecimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ExactDecimal")
            .field(&self.canonical())
            .finish()
    }
}
impl fmt::Display for ExactDecimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.canonical())
    }
}

/// Orders `left * 10^-left_scale` against `right * 10^-right_scale` exactly and without
/// allocating.
///
/// Both magnitudes are unsigned, so this orders non-negative values only; callers order
/// signs first. Alignment scales the lower-scale magnitude by `10^scale_difference`. A
/// failed alignment is itself decisive rather than an error: the exact product then
/// exceeds `u128::MAX`, and the magnitude it is compared against is a `u128`, so the
/// scaled side is strictly greater. A zero magnitude is answered before alignment,
/// because zero scaled by any factor stays zero.
fn compare_scaled_magnitudes(
    left: u128,
    left_scale: u16,
    right: u128,
    right_scale: u16,
) -> Ordering {
    if left_scale == right_scale || left == 0 || right == 0 {
        return left.cmp(&right);
    }
    let factor = 10_u128.checked_pow(u32::from(left_scale.abs_diff(right_scale)));
    if left_scale < right_scale {
        factor
            .and_then(|factor| left.checked_mul(factor))
            .map_or(Ordering::Greater, |scaled| scaled.cmp(&right))
    } else {
        factor
            .and_then(|factor| right.checked_mul(factor))
            .map_or(Ordering::Less, |scaled| left.cmp(&scaled))
    }
}

impl Ord for ExactDecimal {
    fn cmp(&self, other: &Self) -> Ordering {
        let sign = self.coefficient.signum().cmp(&other.coefficient.signum());
        if sign != Ordering::Equal {
            return sign;
        }
        if self.coefficient == 0 {
            return Ordering::Equal;
        }
        let magnitude = compare_scaled_magnitudes(
            self.coefficient.unsigned_abs(),
            self.scale,
            other.coefficient.unsigned_abs(),
            other.scale,
        );
        if self.coefficient < 0 {
            magnitude.reverse()
        } else {
            magnitude
        }
    }
}
impl PartialOrd for ExactDecimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl ExactDecimal {
    pub fn parse(lexeme: &str, grammar: DecimalGrammar) -> Result<Self, DecimalError> {
        if lexeme.is_empty() {
            return Err(DecimalError::Empty);
        }
        if lexeme.len() > grammar.max_lexeme_bytes {
            return Err(DecimalError::LexemeTooLong);
        }
        let (negative, unsigned) = match lexeme.strip_prefix('-') {
            Some(rest) => (true, rest),
            None if lexeme.starts_with('+') => return Err(DecimalError::InvalidLexeme),
            None => (false, lexeme),
        };
        if negative && !grammar.allow_negative {
            return Err(DecimalError::NegativeForbidden);
        }
        let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
            Some(index) => {
                if !grammar.allow_exponent || unsigned[index + 1..].contains(['e', 'E']) {
                    return Err(DecimalError::ExponentForbidden);
                }
                let raw = &unsigned[index + 1..];
                let digits = raw.strip_prefix(['+', '-']).unwrap_or(raw);
                if digits.is_empty()
                    || digits.len() > grammar.max_exponent_digits as usize
                    || !digits.bytes().all(|byte| byte.is_ascii_digit())
                {
                    return Err(DecimalError::InvalidLexeme);
                }
                let exponent = raw
                    .parse::<i32>()
                    .map_err(|_| DecimalError::InvalidLexeme)?;
                if exponent.unsigned_abs() > grammar.max_exponent_magnitude {
                    return Err(DecimalError::ExponentTooLarge);
                }
                (&unsigned[..index], exponent)
            }
            None => (unsigned, 0),
        };
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        if whole.is_empty()
            || !whole.bytes().all(|byte| byte.is_ascii_digit())
            || !fraction.bytes().all(|byte| byte.is_ascii_digit())
            || (mantissa.contains('.') && fraction.is_empty())
            || (whole.len() > 1 && whole.starts_with('0'))
        {
            return Err(DecimalError::InvalidLexeme);
        }
        let digits = format!("{whole}{fraction}");
        if digits.len() > grammar.max_digits as usize {
            return Err(DecimalError::PrecisionExceeded);
        }
        let coefficient = digits
            .parse::<i128>()
            .map_err(|_| DecimalError::CoefficientOverflow)?;
        if coefficient == 0 {
            return Self::normalize(0, 0, grammar);
        }
        let scale = i32::try_from(fraction.len())
            .map_err(|_| DecimalError::ScaleOverflow)
            .and_then(|scale| {
                scale
                    .checked_sub(exponent)
                    .ok_or(DecimalError::ScaleOverflow)
            })?;
        let coefficient = if scale < 0 && coefficient != 0 {
            coefficient
                .checked_mul(
                    10_i128
                        .checked_pow(scale.unsigned_abs())
                        .ok_or(DecimalError::CoefficientOverflow)?,
                )
                .ok_or(DecimalError::CoefficientOverflow)?
        } else {
            coefficient
        };
        let coefficient = if negative {
            coefficient
                .checked_neg()
                .ok_or(DecimalError::CoefficientOverflow)?
        } else {
            coefficient
        };
        Self::normalize(
            coefficient,
            u16::try_from(scale.max(0)).map_err(|_| DecimalError::ScaleOverflow)?,
            grammar,
        )
    }

    fn normalize(
        mut coefficient: i128,
        mut scale: u16,
        grammar: DecimalGrammar,
    ) -> Result<Self, DecimalError> {
        if coefficient < 0 && !grammar.allow_negative {
            return Err(DecimalError::NegativeForbidden);
        }
        if scale > grammar.max_scale {
            return Err(DecimalError::PrecisionExceeded);
        }
        while scale > 0 && coefficient % 10 == 0 {
            coefficient /= 10;
            scale -= 1;
        }
        if coefficient.unsigned_abs().to_string().len() > grammar.max_digits as usize {
            return Err(DecimalError::PrecisionExceeded);
        }
        Ok(Self { coefficient, scale })
    }

    /// Rebuilds a decimal from the parts a fixed-width encoding stores it as: a 128-bit
    /// two's-complement coefficient split into two 64-bit halves, and a decimal scale in
    /// digits.
    ///
    /// The inverse of [`Self::coefficient`] and [`Self::scale`], and the safe way back from
    /// a byte layout: no lexeme is built, so neither the lexeme-length limit nor a large
    /// scale can turn a value this crate legitimately produced into a parse failure, and no
    /// allocation is sized from `scale`. The result is normalized exactly as parsing
    /// normalizes, so a value round-trips to itself.
    ///
    /// Fails with [`DecimalError::PrecisionExceeded`] when `scale` exceeds the grammar's
    /// `max_scale` or the coefficient carries more digits than it allows, and with
    /// [`DecimalError::NegativeForbidden`] for a negative coefficient under a grammar that
    /// forbids one.
    pub fn from_parts(
        coefficient_low: u64,
        coefficient_high: u64,
        scale: u16,
        grammar: DecimalGrammar,
    ) -> Result<Self, DecimalError> {
        let magnitude = (u128::from(coefficient_high) << 64) | u128::from(coefficient_low);
        Self::normalize(magnitude as i128, scale, grammar)
    }

    pub fn canonical(&self) -> String {
        let sign = if self.coefficient < 0 { "-" } else { "" };
        let digits = self.coefficient.unsigned_abs().to_string();
        if self.scale == 0 {
            return format!("{sign}{digits}");
        }
        let scale = self.scale as usize;
        if digits.len() <= scale {
            format!("{sign}0.{}{}", "0".repeat(scale - digits.len()), digits)
        } else {
            let split = digits.len() - scale;
            format!("{sign}{}.{}", &digits[..split], &digits[split..])
        }
    }
    pub fn scale(&self) -> u16 {
        self.scale
    }
    pub fn coefficient(&self) -> i128 {
        self.coefficient
    }
    pub fn checked_add(&self, other: &Self) -> Result<Self, DecimalError> {
        self.checked_add_with(
            other,
            DecimalGrammar::new(u16::MAX, 39, true, true).expect("representation grammar"),
        )
    }
    pub fn checked_add_with(
        &self,
        other: &Self,
        grammar: DecimalGrammar,
    ) -> Result<Self, DecimalError> {
        let scale = self.scale.max(other.scale);
        let left = self
            .coefficient
            .checked_mul(
                10_i128
                    .checked_pow((scale - self.scale) as u32)
                    .ok_or(DecimalError::ArithmeticOverflow)?,
            )
            .ok_or(DecimalError::ArithmeticOverflow)?;
        let right = other
            .coefficient
            .checked_mul(
                10_i128
                    .checked_pow((scale - other.scale) as u32)
                    .ok_or(DecimalError::ArithmeticOverflow)?,
            )
            .ok_or(DecimalError::ArithmeticOverflow)?;
        Self::normalize(
            left.checked_add(right)
                .ok_or(DecimalError::ArithmeticOverflow)?,
            scale,
            grammar,
        )
    }
    pub fn complement(&self, unit: &Self, grammar: DecimalGrammar) -> Result<Self, DecimalError> {
        let negated = self
            .coefficient
            .checked_neg()
            .ok_or(DecimalError::ArithmeticOverflow)?;
        unit.checked_add_with(
            &Self {
                coefficient: negated,
                scale: self.scale,
            },
            grammar,
        )
    }
}

impl Serialize for ExactDecimal {
    /// Emits the canonical decimal lexeme as a string: exactly what `canonical` produces
    /// and what `parse` accepts back under a grammar that admits the value. There is
    /// deliberately no `Deserialize` counterpart, so every inbound decimal is parsed
    /// against an explicit `DecimalGrammar`.
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.canonical())
    }
}

macro_rules! decimal_newtype {
    ($name:ident, $negative:expr) => {
        #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
        pub struct $name(ExactDecimal);
        impl $name {
            pub fn parse(value: &str, mut grammar: DecimalGrammar) -> Result<Self, DecimalError> {
                grammar.allow_negative = $negative;
                Ok(Self(ExactDecimal::parse(value, grammar)?))
            }
            pub fn value(&self) -> &ExactDecimal {
                &self.0
            }
            /// Rebuilds the value from its stored parts under this newtype's sign rule; see
            /// [`ExactDecimal::from_parts`].
            pub fn from_parts(
                coefficient_low: u64,
                coefficient_high: u64,
                scale: u16,
                mut grammar: DecimalGrammar,
            ) -> Result<Self, DecimalError> {
                grammar.allow_negative = $negative;
                Ok(Self(ExactDecimal::from_parts(
                    coefficient_low,
                    coefficient_high,
                    scale,
                    grammar,
                )?))
            }
            pub fn checked_add(
                &self,
                other: &Self,
                grammar: DecimalGrammar,
            ) -> Result<Self, DecimalError> {
                Ok(Self(self.0.checked_add_with(&other.0, grammar)?))
            }
        }
    };
}
decimal_newtype!(Price, false);
decimal_newtype!(Quantity, false);
decimal_newtype!(Depth, false);

#[cfg(test)]
mod tests {
    use super::*;

    const WIDEST: u128 = i128::MAX.unsigned_abs();

    #[test]
    fn numeric_scaled_magnitude_comparison_answers_zero_before_alignment() {
        assert_eq!(compare_scaled_magnitudes(0, 0, 1, 65_535), Ordering::Less);
        assert_eq!(
            compare_scaled_magnitudes(1, 65_535, 0, 0),
            Ordering::Greater
        );
        assert_eq!(compare_scaled_magnitudes(0, 0, 0, 7), Ordering::Equal);
    }

    #[test]
    fn numeric_scaled_magnitude_comparison_treats_failed_alignment_as_decisive() {
        assert_eq!(
            compare_scaled_magnitudes(9, 0, WIDEST, 38),
            Ordering::Greater
        );
        assert_eq!(compare_scaled_magnitudes(WIDEST, 38, 9, 0), Ordering::Less);
        assert_eq!(compare_scaled_magnitudes(1, 0, 1, 39), Ordering::Greater);
        assert_eq!(compare_scaled_magnitudes(1, 39, 1, 0), Ordering::Less);
    }
}
