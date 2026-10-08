use pm_ws::{
    DecimalError, DecimalGrammar, ExactDecimal,
    native::document::{DocumentError, NativeDocument, NativeKind},
    wire::lexical::LexicalLimits,
};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    hint::black_box,
};

thread_local! {
    static ALLOCATIONS: Cell<Option<AllocationCounts>> = const { Cell::new(None) };
}

struct CountingAllocator;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct AllocationCounts {
    allocations: usize,
    reallocations: usize,
}

fn record_allocation(reallocation: bool) {
    let _ = ALLOCATIONS.try_with(|count| {
        if let Some(mut value) = count.get() {
            if reallocation {
                value.reallocations += 1;
            } else {
                value.allocations += 1;
            }
            count.set(Some(value));
        }
    });
}

// SAFETY: Every operation delegates the unchanged allocation contract to System.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation(false);
        // SAFETY: The caller supplies a valid layout, forwarded unchanged to System.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation(false);
        // SAFETY: The caller supplies a valid layout, forwarded unchanged to System.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_allocation(true);
        // SAFETY: The live allocation and valid layouts are forwarded unchanged to System.
        unsafe { System.realloc(pointer, layout, new_size) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: The caller's matching allocation and layout are forwarded to System.
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[test]
fn exact_parse_and_reconstruction_do_not_allocate() {
    let grammar = DecimalGrammar::new(30, 39, true, true).unwrap();
    let lexemes = [
        "0",
        "0.000",
        "-0",
        "0.000001",
        "1.2300",
        "-123.456",
        "1e2",
        "1e-2",
        "1e+2",
        "0e99999",
        "1.000000000000000000000000000000",
        "170141183460469231731687303715884105727",
    ];
    ALLOCATIONS.with(|count| count.set(Some(AllocationCounts::default())));
    let results = lexemes.map(|lexeme| ExactDecimal::parse(black_box(lexeme), black_box(grammar)));
    let rebuilt = ExactDecimal::from_parts(black_box(12300), 0, black_box(4), black_box(grammar));
    let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    assert_eq!(allocations, AllocationCounts::default());
    assert!(black_box(results).iter().all(Result::is_ok));
    assert_eq!(rebuilt.unwrap().to_string(), "1.23");
}

fn large_nested_fixture() -> String {
    let mut fixture = String::from(r#"{"rows":["#);
    for row in 0..32 {
        if row != 0 {
            fixture.push(',');
        }
        fixture.push_str(r#"{"groups":["#);
        for group in 0..8 {
            if group != 0 {
                fixture.push(',');
            }
            fixture.push_str(r#"{"levels":["#);
            for level in 0..16 {
                if level != 0 {
                    fixture.push(',');
                }
                fixture.push_str(r#"{"price":0.000001,"size":100,"tag":"quoted"}"#);
            }
            fixture.push_str("]}");
        }
        fixture.push_str("]}");
    }
    fixture.push_str("]}");
    fixture
}

#[test]
fn flat_document_allocations_do_not_scale_with_fields_or_containers() {
    let fixture = large_nested_fixture();
    let grammar = DecimalGrammar::new(30, 39, true, true).unwrap();
    ALLOCATIONS.with(|count| count.set(Some(AllocationCounts::default())));
    let native = NativeDocument::parse(
        black_box(fixture.as_bytes()),
        LexicalLimits::venue_payload(),
        grammar,
    )
    .unwrap();
    let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    println!(
        "complete flat parse allocations={} reallocations={}",
        allocations.allocations, allocations.reallocations
    );
    assert_eq!(native.root_view().kind(), NativeKind::Object);
    assert!(
        allocations.allocations <= 4,
        "unescaped fields must reference the retained source, not allocate independently"
    );
    assert!(
        allocations.reallocations <= 24,
        "growth belongs to shared buffers, not individual containers"
    );
}

#[test]
fn flat_document_handles_empty_containers() {
    let native = NativeDocument::parse(
        br#"{"array":[],"object":{}}"#,
        LexicalLimits::venue_payload(),
        DecimalGrammar::new(30, 39, true, true).unwrap(),
    )
    .unwrap();
    assert_eq!(
        native.root_view().field("array").unwrap().children().len(),
        0
    );
    assert_eq!(
        native.root_view().field("object").unwrap().entries().len(),
        0
    );
}

#[test]
fn flat_document_rejects_a_late_unrepresentable_number() {
    let result = NativeDocument::parse(
        br#"{"values":[1,2,3,4.567]}"#,
        LexicalLimits::venue_payload(),
        DecimalGrammar::new(2, 39, true, true).unwrap(),
    );
    assert!(matches!(
        result,
        Err(DocumentError::Decimal(DecimalError::PrecisionExceeded))
    ));
}
