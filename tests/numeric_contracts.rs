use pm_ws::*;
use proptest::prelude::*;
use std::cmp::Ordering;

fn grammar() -> DecimalGrammar {
    DecimalGrammar::new(18, 39, true, true).unwrap()
}

fn decimal(lexeme: &str) -> ExactDecimal {
    ExactDecimal::parse(lexeme, grammar()).unwrap()
}

#[test]
fn decimal_grammar_accepts_exact_forms_and_rejects_hostile_lexemes() {
    for lexeme in ["0", "0.5", "10.25", "-10.25", "5e-1", "5E+2"] {
        assert!(ExactDecimal::parse(lexeme, grammar()).is_ok(), "{lexeme}");
    }
    for lexeme in [
        "", "+1", ".5", "1.", "01", "-", "1e", "1e+", "1e1e2", "1..0", "1_0", "NaN", "inf", " 1",
        "1 ",
    ] {
        let expected = match lexeme {
            "" => DecimalError::Empty,
            "1e1e2" => DecimalError::ExponentForbidden,
            _ => DecimalError::InvalidLexeme,
        };
        assert_eq!(
            ExactDecimal::parse(lexeme, grammar()),
            Err(expected),
            "{lexeme}"
        );
    }
    let no_exponents = DecimalGrammar::new(18, 39, false, true).unwrap();
    assert_eq!(
        ExactDecimal::parse("1e2", no_exponents),
        Err(DecimalError::ExponentForbidden)
    );
    let unsigned = DecimalGrammar::new(18, 39, true, false).unwrap();
    assert_eq!(
        ExactDecimal::parse("-1", unsigned),
        Err(DecimalError::NegativeForbidden)
    );
}

#[test]
fn equivalent_lexemes_have_equal_canonical_values() {
    let expected = decimal("0.5");
    for lexeme in ["0.50", "0.5000", "5e-1", "50e-2", "000.5"] {
        if lexeme.starts_with("000") {
            assert_eq!(
                ExactDecimal::parse(lexeme, grammar()),
                Err(DecimalError::InvalidLexeme)
            );
        } else {
            let value = decimal(lexeme);
            assert_eq!(value, expected);
            assert_eq!(value.canonical(), "0.5");
        }
    }
    assert_eq!(decimal("0e99999").canonical(), "0");
    assert_eq!(decimal("0.000e99999").canonical(), "0");
}

#[test]
fn zero_exponents_canonicalize_without_expansion() {
    let grammar = DecimalGrammar::new(0, 1, true, false)
        .unwrap()
        .with_limits(16, 5, 99_999)
        .unwrap();
    for lexeme in ["0e99999", "0e-99999"] {
        let value = ExactDecimal::parse(lexeme, grammar).unwrap();
        assert_eq!(value.canonical(), "0");
        assert_eq!(value.coefficient(), 0);
        assert_eq!(value.scale(), 0);
    }
    assert_eq!(
        ExactDecimal::parse("0e1000", grammar.with_limits(16, 5, 999).unwrap()),
        Err(DecimalError::ExponentTooLarge)
    );
    assert_eq!(
        ExactDecimal::parse("-0e1", grammar),
        Err(DecimalError::NegativeForbidden)
    );
}

proptest! {
    #[test]
    fn signed_cross_scale_ordering_matches_integer_oracle(
        left in -10_000i32..10_001,
        right in -10_000i32..10_001,
        left_scale in 0u32..=4,
        right_scale in 0u32..=4,
    ) {
        let left_lexeme = format_scaled(left, left_scale);
        let right_lexeme = format_scaled(right, right_scale);
        let left_value = decimal(&left_lexeme);
        let right_value = decimal(&right_lexeme);
        let common_scale = left_scale.max(right_scale);
        let left_integer = i64::from(left) * 10_i64.pow(common_scale - left_scale);
        let right_integer = i64::from(right) * 10_i64.pow(common_scale - right_scale);
        prop_assert_eq!(left_value.cmp(&right_value), left_integer.cmp(&right_integer));
    }
}

fn format_scaled(value: i32, scale: u32) -> String {
    if scale == 0 {
        return value.to_string();
    }
    let negative = value < 0;
    let digits = value.unsigned_abs().to_string();
    let scale = scale as usize;
    let body = if digits.len() <= scale {
        format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
    } else {
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    if negative { format!("-{body}") } else { body }
}

#[test]
fn checked_add_is_exact_and_reports_overflow_without_rounding() {
    let sum = decimal("1.20").checked_add(&decimal("0.03")).unwrap();
    assert_eq!(sum.canonical(), "1.23");

    let narrow = DecimalGrammar::new(2, 3, true, true).unwrap();
    let nine_ninety_nine = ExactDecimal::parse("9.99", narrow).unwrap();
    let two_cents = ExactDecimal::parse("0.02", narrow).unwrap();
    assert_eq!(
        nine_ninety_nine.checked_add_with(&two_cents, narrow),
        Err(DecimalError::PrecisionExceeded)
    );

    let max = decimal("170141183460469231731687303715884105727");
    assert_eq!(
        max.checked_add(&decimal("1")),
        Err(DecimalError::ArithmeticOverflow)
    );
}

#[test]
fn checked_arithmetic_enforces_the_output_grammar_sign_domain() {
    let signed = DecimalGrammar::new(18, 39, true, true).unwrap();
    let nonnegative = DecimalGrammar::new(18, 39, true, false).unwrap();
    let negative_two = ExactDecimal::parse("-2", signed).unwrap();
    let negative_half = ExactDecimal::parse("-0.5", signed).unwrap();
    let one = ExactDecimal::parse("1", signed).unwrap();
    let two = ExactDecimal::parse("2", signed).unwrap();

    assert_eq!(
        negative_two.checked_add_with(&one, nonnegative),
        Err(DecimalError::NegativeForbidden)
    );
    assert_eq!(
        negative_half
            .checked_add_with(&two, nonnegative)
            .unwrap()
            .canonical(),
        "1.5"
    );
    assert_eq!(
        two.complement(&one, nonnegative),
        Err(DecimalError::NegativeForbidden)
    );
}

proptest! {
    #[test]
    fn complement_is_an_involution_for_small_exact_values(
        value in 0i32..=100,
        unit in 1i32..=100,
    ) {
        let value = decimal(&format!("{}.25", value));
        let unit = decimal(&format!("{}.25", unit));
        let first = value.complement(&unit, grammar()).unwrap();
        let second = first.complement(&unit, grammar()).unwrap();
        prop_assert_eq!(second, value);
    }
}

#[test]
fn complement_preserves_exact_domain_and_rejects_arithmetic_overflow() {
    let unit = decimal("1");
    assert_eq!(
        decimal("0.25")
            .complement(&unit, grammar())
            .unwrap()
            .canonical(),
        "0.75"
    );
    assert_eq!(
        decimal("1")
            .complement(&unit, grammar())
            .unwrap()
            .canonical(),
        "0"
    );
    let min = decimal("-0.1");
    let max = decimal("170141183460469231731687303715884105727");
    assert_eq!(
        min.complement(&max, grammar()),
        Err(DecimalError::ArithmeticOverflow)
    );
}

#[test]
fn parser_rejects_precision_scale_coefficient_and_exponent_limits() {
    let low_scale = DecimalGrammar::new(2, 39, true, true).unwrap();
    assert_eq!(
        ExactDecimal::parse("1.234", low_scale),
        Err(DecimalError::PrecisionExceeded)
    );
    let low_precision = DecimalGrammar::new(18, 3, true, true).unwrap();
    assert_eq!(
        ExactDecimal::parse("12.34", low_precision),
        Err(DecimalError::PrecisionExceeded)
    );
    assert_eq!(
        ExactDecimal::parse("170141183460469231731687303715884105728", grammar()),
        Err(DecimalError::CoefficientOverflow)
    );
    assert_eq!(
        ExactDecimal::parse("9e38", grammar()),
        Err(DecimalError::CoefficientOverflow)
    );
    let bounded = grammar().with_limits(128, 3, 99).unwrap();
    assert_eq!(
        ExactDecimal::parse("1e100", bounded),
        Err(DecimalError::ExponentTooLarge)
    );
    let short_exponent = grammar().with_limits(128, 2, 99).unwrap();
    assert_eq!(
        ExactDecimal::parse("1e123", short_exponent),
        Err(DecimalError::InvalidLexeme)
    );
}

#[test]
fn rejected_parse_does_not_mutate_existing_state() {
    let mut state = Some(decimal("12.50"));
    let before = state.clone();
    let result = ExactDecimal::parse("12.345", DecimalGrammar::new(2, 39, true, true).unwrap());
    if let Ok(value) = &result {
        state = Some(value.clone());
    }
    assert_eq!(result, Err(DecimalError::PrecisionExceeded));
    assert_eq!(state, before);
}

fn wide_grammar() -> DecimalGrammar {
    DecimalGrammar::new(u16::MAX, 39, true, true).unwrap()
}

fn scaled(coefficient: i128, scale: u16) -> ExactDecimal {
    let lexeme = if scale == 0 {
        coefficient.to_string()
    } else {
        format!("{coefficient}e-{scale}")
    };
    ExactDecimal::parse(&lexeme, wide_grammar()).expect("adversarial lexeme parses")
}

fn reference_ordering(left: &ExactDecimal, right: &ExactDecimal) -> Ordering {
    let left = left.canonical();
    let right = right.canonical();
    let left_negative = left.starts_with('-');
    let right_negative = right.starts_with('-');
    if left_negative != right_negative {
        return if left_negative {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    let magnitude =
        compare_unsigned_lexemes(left.trim_start_matches('-'), right.trim_start_matches('-'));
    if left_negative {
        magnitude.reverse()
    } else {
        magnitude
    }
}

fn compare_unsigned_lexemes(left: &str, right: &str) -> Ordering {
    let (left_whole, left_fraction) = left.split_once('.').unwrap_or((left, ""));
    let (right_whole, right_fraction) = right.split_once('.').unwrap_or((right, ""));
    let left_whole = left_whole.trim_start_matches('0');
    let right_whole = right_whole.trim_start_matches('0');
    let width = left_fraction.len().max(right_fraction.len());
    left_whole
        .len()
        .cmp(&right_whole.len())
        .then_with(|| left_whole.cmp(right_whole))
        .then_with(|| {
            format!("{left_fraction:0<width$}").cmp(&format!("{right_fraction:0<width$}"))
        })
}

fn adversarial_coefficient() -> impl Strategy<Value = i128> {
    prop_oneof![
        Just(0i128),
        Just(i128::MAX),
        (i128::MIN + 1)..=(i128::MIN + 4097),
        (i128::MAX - 4096)..=i128::MAX,
        -4096i128..=4096,
        10i128.pow(29)..10i128.pow(30),
        -(10i128.pow(30))..-(10i128.pow(29)),
        (i128::MIN + 1)..=i128::MAX,
    ]
}

fn adversarial_scale() -> impl Strategy<Value = u16> {
    prop_oneof![
        Just(0u16),
        Just(1u16),
        Just(18u16),
        Just(38u16),
        0u16..=40,
        65_000u16..=u16::MAX,
        0u16..=u16::MAX,
    ]
}

proptest! {
    #[test]
    fn numeric_ordering_matches_the_lexeme_oracle_across_adversarial_pairs(
        left_coefficient in adversarial_coefficient(),
        left_scale in adversarial_scale(),
        right_coefficient in adversarial_coefficient(),
        right_scale in adversarial_scale(),
    ) {
        let left = scaled(left_coefficient, left_scale);
        let right = scaled(right_coefficient, right_scale);
        prop_assert_eq!(left.cmp(&right), reference_ordering(&left, &right));
        prop_assert_eq!(right.cmp(&left), reference_ordering(&right, &left));
        prop_assert_eq!(left.cmp(&left), Ordering::Equal);
    }
}

proptest! {
    #[test]
    fn numeric_ordering_matches_the_lexeme_oracle_for_long_shared_prefixes(
        prefix in 10i128.pow(37)..(i128::MAX / 10),
        tail in 0i128..=9,
        scale in 0u16..=65_000,
        negative in any::<bool>(),
    ) {
        let sign = if negative { -1 } else { 1 };
        let long = scaled(sign * (prefix * 10 + tail), scale + 1);
        let short = scaled(sign * prefix, scale);
        prop_assert_eq!(long.cmp(&short), reference_ordering(&long, &short));
        prop_assert_eq!(short.cmp(&long), reference_ordering(&short, &long));
        let expected = match (tail, negative) {
            (0, _) => Ordering::Equal,
            (_, true) => Ordering::Less,
            (_, false) => Ordering::Greater,
        };
        prop_assert_eq!(long.cmp(&short), expected);
    }
}

proptest! {
    #[test]
    fn numeric_ordering_is_exact_when_scale_alignment_overflows(
        magnitude in 10i128.pow(37)..=i128::MAX,
        other in 1i128..=i128::MAX,
        low_scale in 0u16..=32_000,
        difference in 2u16..=38,
    ) {
        let low = scaled(magnitude, low_scale);
        let high = scaled(other, low_scale + difference);
        prop_assert_eq!(low.cmp(&high), reference_ordering(&low, &high));
        prop_assert_eq!(high.cmp(&low), reference_ordering(&high, &low));
        prop_assert_eq!(low.cmp(&high), Ordering::Greater);
    }
}

#[test]
fn numeric_ordering_resolves_pairs_whose_scale_alignment_overflows() {
    let widest = 170_141_183_460_469_231_731_687_303_715_884_105_727i128;

    let nine = scaled(9, 0);
    let near_two = scaled(widest, 38);
    assert_eq!(nine.cmp(&near_two), Ordering::Greater);
    assert_eq!(near_two.cmp(&nine), Ordering::Less);
    assert_eq!(reference_ordering(&nine, &near_two), Ordering::Greater);

    let one = scaled(1, 0);
    let vanishing = scaled(widest, 65_535);
    assert_eq!(one.cmp(&vanishing), Ordering::Greater);
    assert_eq!(vanishing.cmp(&one), Ordering::Less);
    assert_eq!(reference_ordering(&one, &vanishing), Ordering::Greater);

    let negative_nine = scaled(-9, 0);
    let negative_near_two = scaled(-widest, 38);
    assert_eq!(negative_nine.cmp(&negative_near_two), Ordering::Less);
    assert_eq!(negative_near_two.cmp(&negative_nine), Ordering::Greater);
    assert_eq!(
        reference_ordering(&negative_nine, &negative_near_two),
        Ordering::Less
    );

    assert_eq!(scaled(0, 0).cmp(&vanishing), Ordering::Less);
    assert_eq!(vanishing.cmp(&scaled(0, 0)), Ordering::Greater);
}

#[test]
fn numeric_serialization_is_the_canonical_lexeme() {
    for lexeme in ["0", "-3", "0.5", "0.012", "1.23", "-0.000000000000000001"] {
        let value = ExactDecimal::parse(lexeme, grammar()).unwrap();
        assert_eq!(
            serde_json::to_string(&value).unwrap(),
            format!("\"{}\"", value.canonical())
        );
    }
    assert_eq!(
        serde_json::to_string(&ExactDecimal::parse("1e-18", grammar()).unwrap()).unwrap(),
        "\"0.000000000000000001\""
    );
    assert_eq!(
        serde_json::to_string(&Price::parse("0.012", grammar()).unwrap()).unwrap(),
        "\"0.012\""
    );
    assert_eq!(
        serde_json::to_string(&Quantity::parse("1e-18", grammar()).unwrap()).unwrap(),
        "\"0.000000000000000001\""
    );
    assert_eq!(
        serde_json::to_string(&Depth::parse("250", grammar()).unwrap()).unwrap(),
        "\"250\""
    );
}
