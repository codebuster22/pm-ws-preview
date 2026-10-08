use pm_ws::{
    DecimalGrammar,
    native::{
        NativePayload,
        document::{DocumentError, NativeDocument, NativeKind},
    },
    wire::lexical::{LexicalError, LexicalLimits},
};

fn grammar() -> DecimalGrammar {
    DecimalGrammar::new(18, 30, false, false).unwrap()
}

#[test]
fn typed_views_preserve_complete_source_order_types_and_lexemes() {
    let document = NativeDocument::parse(
        br#"{"unknown":["source text",0.5000,true,null],"second":{"nested":"value"}}"#,
        LexicalLimits::venue_payload(),
        grammar(),
    )
    .unwrap();
    let payload = NativePayload::from_document(document);
    let root = payload.view();
    assert_eq!(
        root.entries().map(|(key, _)| key).collect::<Vec<_>>(),
        ["unknown", "second"]
    );
    let values = root
        .field("unknown")
        .unwrap()
        .children()
        .collect::<Vec<_>>();
    assert_eq!(values.len(), 4);
    assert_eq!(values[0].as_text(), Some("source text"));
    assert_eq!(values[1].number_lexeme(), Some("0.5000"));
    assert_eq!(values[1].exact_decimal().unwrap().to_string(), "0.5");
    assert_eq!(values[2].as_bool(), Some(true));
    assert_eq!(values[3].kind(), NativeKind::Null);
    assert_eq!(
        root.field("second")
            .unwrap()
            .field("nested")
            .unwrap()
            .as_text(),
        Some("value")
    );
}

#[test]
fn escaped_text_and_equivalent_keys_use_decoded_semantics() {
    let document = NativeDocument::parse(
        br#"{"a\u0062":"\uD834\uDD1E\n","plain":"ab"}"#,
        LexicalLimits::venue_payload(),
        grammar(),
    )
    .unwrap();
    assert_eq!(
        document.root_view().field("ab").unwrap().as_text(),
        Some("\u{1D11E}\n")
    );
    assert!(matches!(
        NativeDocument::parse(
            br#"{"a":1,"\u0061":2}"#,
            LexicalLimits::venue_payload(),
            grammar()
        ),
        Err(DocumentError::Lexical(LexicalError::DuplicateField { .. }))
    ));
}

#[test]
fn unrepresentable_trailing_number_rejects_the_whole_document() {
    assert!(matches!(
        NativeDocument::parse(
            br#"["valid",{"unknown":0.0000000000000000001}]"#,
            LexicalLimits::venue_payload(),
            grammar()
        ),
        Err(DocumentError::Decimal(_))
    ));
}

#[test]
fn non_objects_have_no_entries_and_leaf_iterators_are_empty() {
    for input in [b"null".as_slice(), b"true", b"1", br#""text""#, b"[1,2]"] {
        let document =
            NativeDocument::parse(input, LexicalLimits::venue_payload(), grammar()).unwrap();
        assert_eq!(document.root_view().entries().len(), 0);
        assert!(document.root_view().field("unknown").is_none());
    }
}
