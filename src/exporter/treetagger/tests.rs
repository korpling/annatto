use graphannis::update::{GraphUpdate, UpdateEvent};
use insta::assert_snapshot;

use super::*;
use crate::{
    test_util::export_to_string,
    util::example_generator::{self, add_node_label, make_span},
};

fn create_test_corpus_base_token() -> AnnotationGraph {
    let mut u = GraphUpdate::new();
    example_generator::create_corpus_structure_simple(&mut u);
    example_generator::create_tokens(&mut u, Some("root/doc1"));

    add_node_label(&mut u, "root", "", "corpus-name", "root");

    // Add POS annotations
    add_node_label(&mut u, "root/doc1#tok0", "default_ns", "pos", "VBZ");
    add_node_label(&mut u, "root/doc1#tok1", "default_ns", "pos", "DT");
    add_node_label(&mut u, "root/doc1#tok2", "default_ns", "pos", "NN");
    add_node_label(&mut u, "root/doc1#tok3", "default_ns", "pos", "RBR");
    add_node_label(&mut u, "root/doc1#tok4", "default_ns", "pos", "JJ");
    add_node_label(&mut u, "root/doc1#tok5", "default_ns", "pos", "IN");
    add_node_label(&mut u, "root/doc1#tok6", "default_ns", "pos", "PP");
    add_node_label(&mut u, "root/doc1#tok7", "default_ns", "pos", "VBZ");
    add_node_label(&mut u, "root/doc1#tok8", "default_ns", "pos", "TO");
    add_node_label(&mut u, "root/doc1#tok9", "default_ns", "pos", "VB");
    add_node_label(&mut u, "root/doc1#tok10", "default_ns", "pos", "SENT");

    // Add lemma annotations
    add_node_label(&mut u, "root/doc1#tok0", "default_ns", "lemma", "be");
    add_node_label(&mut u, "root/doc1#tok1", "default_ns", "lemma", "this");
    add_node_label(&mut u, "root/doc1#tok2", "default_ns", "lemma", "example");
    add_node_label(&mut u, "root/doc1#tok3", "default_ns", "lemma", "more");
    add_node_label(
        &mut u,
        "root/doc1#tok4",
        "default_ns",
        "lemma",
        "complicated",
    );
    add_node_label(&mut u, "root/doc1#tok5", "default_ns", "lemma", "than");
    add_node_label(&mut u, "root/doc1#tok6", "default_ns", "lemma", "it");
    add_node_label(&mut u, "root/doc1#tok7", "default_ns", "lemma", "appear");
    add_node_label(&mut u, "root/doc1#tok8", "default_ns", "lemma", "to");
    add_node_label(&mut u, "root/doc1#tok9", "default_ns", "lemma", "be");
    add_node_label(&mut u, "root/doc1#tok10", "default_ns", "lemma", "?");

    // Add overlapping spans
    make_span(
        &mut u,
        &"root/doc1#span1",
        &["root/doc1#tok0", "root/doc1#tok1", "root/doc1#tok2"],
        true,
    );
    add_node_label(
        &mut u,
        "root/doc1#span1",
        "default_ns",
        "something",
        "annotated",
    );
    make_span(
        &mut u,
        &"root/doc1#span2",
        &["root/doc1#tok2", "root/doc1#tok3"],
        true,
    );
    add_node_label(
        &mut u,
        "root/doc1#span2",
        "default_ns",
        "anotherthing",
        "annotated",
    );
    add_node_label(
        &mut u,
        "root/doc1#span2",
        "default_ns",
        "evenmore",
        "<unknown-values>",
    );
    // Span without a label
    make_span(&mut u, &"root/doc1#span3", &["root/doc1#tok5"], true);

    // Add some additional metadata to the document
    add_node_label(&mut u, "root/doc1", "ignored", "author", "<unknown>");
    add_node_label(&mut u, "root/doc1", "default_ns", "year", "1984");

    let g = AnnotationGraph::with_default_graphstorages(true);
    assert!(g.is_ok());
    let mut graph = g.unwrap();
    assert!(graph.apply_update(&mut u, |_| {}).is_ok());
    graph
}

fn create_test_corpus_segmentations(
    remove_tok_label: bool,
    add_additional_ordering_edges: bool,
) -> AnnotationGraph {
    let mut u = GraphUpdate::new();
    example_generator::create_corpus_structure_simple(&mut u);
    example_generator::create_multiple_segmentations(&mut u, "root/doc1");

    if remove_tok_label {
        for a in 1..=4 {
            u.add_event(UpdateEvent::DeleteNodeLabel {
                node_name: format!("root/doc1#a{a}").into(),
                anno_ns: ANNIS_NS.into(),
                anno_name: "tok".into(),
            })
            .unwrap();
        }
        for b in 1..=4 {
            u.add_event(UpdateEvent::DeleteNodeLabel {
                node_name: format!("root/doc1#b{b}").into(),
                anno_ns: ANNIS_NS.into(),
                anno_name: "tok".into(),
            })
            .unwrap();
        }
    }
    if add_additional_ordering_edges {
        for a in 1..4 {
            u.add_event(UpdateEvent::AddEdge {
                source_node: format!("root/doc1#a{a}").into(),
                target_node: format!("root/doc1#a{}", a + 1).into(),
                layer: "LAYER_SHOULD_BE_IGNORED".into(),
                component_type: AnnotationComponentType::Ordering.to_string(),
                component_name: "a".into(),
            })
            .unwrap();
        }
        for b in 1..4 {
            u.add_event(UpdateEvent::AddEdge {
                source_node: format!("root/doc1#b{b}").into(),
                target_node: format!("root/doc1#b{}", b + 1).into(),
                layer: "LAYER_SHOULD_BE_IGNORED".into(),
                component_type: AnnotationComponentType::Ordering.to_string(),
                component_name: "b".into(),
            })
            .unwrap();
        }
    }

    // Add some additional metadata to the document
    add_node_label(&mut u, "root/doc1", "ignored", "author", "<unknown>");
    add_node_label(&mut u, "root/doc1", "default_ns", "year", "1984");

    let g = AnnotationGraph::with_default_graphstorages(true);
    assert!(g.is_ok());
    let mut graph = g.unwrap();
    assert!(graph.apply_update(&mut u, |_| {}).is_ok());
    graph
}

#[test]
fn core() {
    let graph = create_test_corpus_base_token();

    let export_config: ExportTreeTagger = toml::from_str("").unwrap();

    let export = export_to_string(&graph, export_config);
    assert!(export.is_ok(), "error: {:?}", export.err());
    assert_snapshot!(export.unwrap());
}

#[test]
fn core_different_doc_name() {
    let graph = create_test_corpus_base_token();

    let export_config: ExportTreeTagger = toml::from_str(r#"doc_anno = "corpus-name""#).unwrap();

    let export = export_to_string(&graph, export_config);
    assert!(export.is_ok(), "error: {:?}", export.err());
    assert_snapshot!(export.unwrap());
}

#[test]
fn core_no_metadata() {
    let graph = create_test_corpus_base_token();

    let export_config: ExportTreeTagger = toml::from_str(r#"skip_meta = true"#).unwrap();

    let export = export_to_string(&graph, export_config);
    assert!(export.is_ok(), "error: {:?}", export.err());
    assert_snapshot!(export.unwrap());
}

#[test]
fn core_no_spans() {
    let graph = create_test_corpus_base_token();

    let export_config: ExportTreeTagger = toml::from_str(r#"skip_spans = true"#).unwrap();

    let export = export_to_string(&graph, export_config);
    assert!(export.is_ok(), "error: {:?}", export.err());
    assert_snapshot!(export.unwrap());
}

#[test]
fn fixed_tag_name() {
    let graph = create_test_corpus_base_token();

    let export_config: ExportTreeTagger = toml::from_str(
        r#"
        span_names = { strategy = "fixed", name = "mytagname"}
        "#,
    )
    .unwrap();

    let export = export_to_string(&graph, export_config);
    assert!(export.is_ok(), "error: {:?}", export.err());
    assert_snapshot!(export.unwrap());
}

#[test]
fn tag_name_from_anno_name() {
    let graph = create_test_corpus_base_token();

    let export_config: ExportTreeTagger = toml::from_str(
        r#"
        span_names = { strategy = "first_anno_name"}
        "#,
    )
    .unwrap();

    let export = export_to_string(&graph, export_config);
    assert!(export.is_ok(), "error: {:?}", export.err());
    assert_snapshot!(export.unwrap());
}

#[test]
fn tag_name_from_anno_namespace() {
    let graph = create_test_corpus_base_token();

    let export_config: ExportTreeTagger = toml::from_str(
        r#"
        span_names = { strategy = "first_anno_namespace"}
        "#,
    )
    .unwrap();

    let export = export_to_string(&graph, export_config);
    assert!(export.is_ok(), "error: {:?}", export.err());
    assert_snapshot!(export.unwrap());
}

#[test]
fn segmentation() {
    let graph = create_test_corpus_segmentations(false, false);

    let mut export_config = ExportTreeTagger::default();
    export_config.segmentation = Some("b".to_string());

    let export = export_to_string(&graph, export_config);
    assert!(export.is_ok(), "error: {:?}", export.err());
    assert_snapshot!(export.unwrap());
}

#[test]
fn segmentation_without_tok_label() {
    let graph = create_test_corpus_segmentations(true, true);

    let mut export_config = ExportTreeTagger::default();
    export_config.segmentation = Some("b".to_string());

    let export = export_to_string(&graph, export_config);
    assert!(export.is_ok(), "error: {:?}", export.err());
    assert_snapshot!(export.unwrap());
}
