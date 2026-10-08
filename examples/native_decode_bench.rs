//! In-memory native decoder timing; generated fixtures and JSON metrics only.

use pm_ws::{
    limitless::native::decode_native_frame,
    native::NativeSource,
    polymarket::native::{decode_document, parse_document},
    wire::{
        lexical::LexicalLimits,
        socketio::{WebSocketOpcode, decode_limitless_frame},
    },
};
use std::{hint::black_box, sync::Arc, time::Instant};

const LEVELS: usize = 256;

#[derive(Clone, Copy)]
enum Venue {
    Limitless,
    Polymarket,
}

struct Case {
    name: &'static str,
    venue: Venue,
    input: String,
}

fn source() -> NativeSource {
    NativeSource {
        stream: Arc::from("benchmark"),
        source: Arc::from("generated"),
        slot: 0,
        generation: 1,
        stream_generation: 1,
        sequence: 1,
        received_ns: 1,
        validated_ns: 2,
    }
}

fn polymarket_price_change() -> String {
    r#"{"event_type":"price_change","market":"market","timestamp":"1","price_changes":[{"asset_id":"yes","price":"0.500000","size":"100","side":"BUY","hash":"a","best_bid":"0.499000","best_ask":"0.501000"},{"asset_id":"no","price":"0.500000","size":"100","side":"SELL","hash":"b","best_bid":"0.499000","best_ask":"0.501000"}]}"#.into()
}

fn polymarket_book() -> String {
    let levels = (1..=LEVELS)
        .map(|n| format!(r#"{{"price":"0.{n:06}","size":"{n}"}}"#))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        r#"{{"event_type":"book","market":"market","asset_id":"asset","bids":[{levels}],"asks":[{levels}],"timestamp":"1","hash":"book"}}"#
    )
}

fn limitless_book() -> String {
    let levels = (1..=LEVELS)
        .map(|n| format!(r#"{{"price":0.{n:06},"size":{n}}}"#))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        r#"42/markets,["orderbookUpdate",{{"marketSlug":"market","orderbook":{{"bids":[{levels}],"asks":[{levels}]}},"version":1,"timestamp":"2026-09-12T00:00:00.000Z"}}]"#
    )
}

fn decode(case: &Case, input: &[u8], provenance: &NativeSource) -> [u64; 4] {
    let started = Instant::now();
    let (parsed, batch) = match case.venue {
        Venue::Polymarket => {
            let document = parse_document(input, LexicalLimits::venue_payload()).unwrap();
            let parsed = Instant::now();
            let batch = decode_document(document, provenance.clone(), input.len()).unwrap();
            (parsed, batch)
        }
        Venue::Limitless => {
            let frame = decode_limitless_frame(
                input,
                WebSocketOpcode::Text,
                LexicalLimits::venue_payload(),
                pm_ws::DecimalGrammar::new(30, 39, true, true).unwrap(),
            )
            .unwrap();
            let parsed = Instant::now();
            let batch = decode_native_frame(frame, provenance.clone(), input.len())
                .unwrap()
                .unwrap();
            (parsed, batch)
        }
    };
    let validated = Instant::now();
    drop(black_box(batch));
    let finished = Instant::now();
    [
        u64::try_from(finished.duration_since(started).as_nanos()).unwrap(),
        u64::try_from(parsed.duration_since(started).as_nanos()).unwrap(),
        u64::try_from(validated.duration_since(parsed).as_nanos()).unwrap(),
        u64::try_from(finished.duration_since(validated).as_nanos()).unwrap(),
    ]
}

fn percentile(samples: &[u64], percent: usize) -> u64 {
    samples[(samples.len() - 1) * percent / 100]
}

fn iterations() -> usize {
    let mut args = std::env::args().skip(1);
    let value = match (args.next().as_deref(), args.next()) {
        (None, None) => 3_000,
        (Some("--iterations"), Some(value)) if args.next().is_none() => value.parse().unwrap_or(0),
        _ => 0,
    };
    assert!(
        (1..=100_000).contains(&value),
        "usage: native_decode_bench [--iterations 1..100000]"
    );
    value
}

fn main() {
    let iterations = iterations();
    let cases = [
        Case {
            name: "polymarket_price_change_2",
            venue: Venue::Polymarket,
            input: polymarket_price_change(),
        },
        Case {
            name: "polymarket_book_256x2",
            venue: Venue::Polymarket,
            input: polymarket_book(),
        },
        Case {
            name: "limitless_orderbook_256x2",
            venue: Venue::Limitless,
            input: limitless_book(),
        },
    ];
    let provenance = source();
    let results = cases
        .iter()
        .map(|case| {
            for _ in 0..100 {
                decode(case, case.input.as_bytes(), &provenance);
            }
            let mut samples = std::array::from_fn::<_, 4, _>(|_| Vec::with_capacity(iterations));
            for _ in 0..iterations {
                let elapsed = decode(case, black_box(case.input.as_bytes()), &provenance);
                for (samples, elapsed) in samples.iter_mut().zip(elapsed) {
                    samples.push(elapsed);
                }
            }
            for samples in &mut samples {
                samples.sort_unstable();
            }
            let stages = ["parse_document", "venue_validation", "destruction"]
                .into_iter()
                .zip(samples.iter().skip(1))
                .map(|(stage, samples)| {
                    (
                        stage.to_owned(),
                        serde_json::json!({"count":iterations,
                        "p95_ns":percentile(samples,95),"p99_ns":percentile(samples,99),
                        "max_ns":samples[iterations-1]}),
                    )
                })
                .collect::<serde_json::Map<_, _>>();
            let samples = &samples[0];
            serde_json::json!({"case":case.name,"input_bytes":case.input.len(),"count":iterations,
            "p50_ns":percentile(samples,50),"p95_ns":percentile(samples,95),
            "p99_ns":percentile(samples,99),"max_ns":samples[iterations-1],"stages":stages})
        })
        .collect::<Vec<_>>();
    println!(
        "{}",
        serde_json::json!({"boundary":"in-memory source decode, validation, and result destruction; Limitless includes Socket.IO frame decode","cases":results})
    );
}
