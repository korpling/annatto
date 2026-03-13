use std::path::Path;

use graphannis::{
    AnnotationGraph,
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::{ANNIS_NS, DEFAULT_NS};
use insta::assert_snapshot;

use crate::{
    StepID,
    exporter::{exmaralda::ExportExmaralda, graphml::GraphMLExporter, sequence::ExportSequence},
    importer::{ImportRunConfiguration, Importer, exmaralda::ImportEXMARaLDA},
    manipulator::{Manipulator, diff::DiffSubgraphs},
    test_util::export_to_string,
    util::update_graph_silent,
};

#[test]
fn with_data() {
    let import: Result<ImportEXMARaLDA, _> = toml::from_str("");
    assert!(import.is_ok());
    let import = import.unwrap();
    let u = import.import_corpus(
        Path::new("tests/data/graph_op/diff/merge"),
        StepID {
            module_name: "test_import".to_string(),
            path: None,
        },
        ImportRunConfiguration::default(),
        None,
    );
    assert!(u.is_ok());
    let mut update = u.unwrap();
    let g = AnnotationGraph::with_default_graphstorages(true);
    assert!(g.is_ok());
    let mut graph = g.unwrap();
    assert!(update_graph_silent(&mut graph, &mut update).is_ok());
    assert!(graph.calculate_all_statistics().is_ok());
    let d: Result<DiffSubgraphs, _> = toml::from_str(
        r#"
        target_parent = "merge/b"
        target_component = { ctype = "Ordering", layer = "annis", name = "norm" }
        target_key = "norm::norm"
        source_parent = "merge/a"
        source_component = { ctype = "Ordering", layer = "annis", name = "norm" }
        source_key = "norm::norm"
        merge = true
        "#,
    );
    assert!(d.is_ok());
    let diff = d.unwrap();
    let manip = diff.manipulate_corpus(
        &mut graph,
        Path::new("./"),
        StepID {
            module_name: "test_manip".to_string(),
            path: None,
        },
        None,
    );
    assert!(manip.is_ok(), "Err: {:?}", manip.err().unwrap());
    let export: Result<GraphMLExporter, _> = toml::from_str("stable_order = true");
    assert!(export.is_ok());
    let export = export.unwrap();
    let actual_graphml = export_to_string(&graph, export).unwrap();
    let export2 = ExportExmaralda::default();
    let actual_exb = export_to_string(&graph, export2).unwrap();
    let export3: ExportSequence = toml::from_str(
        r#"
        component = { ctype = "Ordering", layer = "annis", name = "norm" }
        anno = "norm::norm"
        "#,
    )
    .unwrap();
    let actual_seq = export_to_string(&graph, export3).unwrap();
    let mut snapshot_string =
        String::with_capacity(actual_exb.len() + actual_graphml.len() + actual_seq.len() + 2);
    snapshot_string.push_str(&actual_graphml);
    snapshot_string.push_str("\n");
    snapshot_string.push_str(&actual_exb);
    snapshot_string.push_str("\n");
    snapshot_string.push_str(&actual_seq);
    assert_snapshot!(snapshot_string);
}

#[test]
fn single_tok() {
    let mut graph = AnnotationGraph::with_default_graphstorages(true).unwrap();
    let mut update = GraphUpdate::default();
    assert!(
        update
            .add_event(UpdateEvent::AddNode {
                node_name: "corpus".to_string(),
                node_type: "corpus".to_string()
            })
            .is_ok()
    );
    let tokens = [
        vec!["This", "is", "a", "test"],
        vec!["This", "was", "a", "surprisingly", "successful", "test"],
    ];
    let pos = [
        vec!["PRON", "VERB", "DET", "NOUN"],
        vec!["PRON", "VERB", "DET", "ADV", "ADJ", "NOUN"],
    ];
    let deps = [
        vec![(0, 1, "subj"), (2, 3, "det"), (3, 1, "pred")],
        vec![
            (0, 1, "subj"),
            (2, 5, "det"),
            (3, 4, "mod"),
            (4, 5, "mod"),
            (5, 1, "pred"),
        ],
    ];
    for (((subcorpus, tok_values), pos_annos), relations) in
        ["a", "b"].into_iter().zip(tokens).zip(pos).zip(deps)
    {
        let name = format!("corpus/{subcorpus}");
        assert!(
            update
                .add_event(UpdateEvent::AddNode {
                    node_name: name.to_string(),
                    node_type: "corpus".to_string()
                })
                .is_ok()
        );
        assert!(
            update
                .add_event(UpdateEvent::AddEdge {
                    source_node: name.to_string(),
                    target_node: "corpus".to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::PartOf.to_string(),
                    component_name: "".to_string()
                })
                .is_ok()
        );
        let name = format!("{name}/doc");
        assert!(
            update
                .add_event(UpdateEvent::AddNode {
                    node_name: name.to_string(),
                    node_type: "corpus".to_string()
                })
                .is_ok()
        );
        assert!(
            update
                .add_event(UpdateEvent::AddNodeLabel {
                    node_name: name.to_string(),
                    anno_ns: ANNIS_NS.to_string(),
                    anno_name: "doc".to_string(),
                    anno_value: "test".to_string()
                })
                .is_ok()
        );
        assert!(
            update
                .add_event(UpdateEvent::AddEdge {
                    source_node: name.to_string(),
                    target_node: format!("corpus/{subcorpus}"),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::PartOf.to_string(),
                    component_name: "".to_string()
                })
                .is_ok()
        );
        let s_name = format!("{name}#s1");
        assert!(
            update
                .add_event(UpdateEvent::AddNode {
                    node_name: s_name.to_string(),
                    node_type: "node".to_string()
                })
                .is_ok()
        );
        assert!(
            update
                .add_event(UpdateEvent::AddEdge {
                    source_node: s_name.to_string(),
                    target_node: name.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::PartOf.to_string(),
                    component_name: "".to_string()
                })
                .is_ok()
        );
        for (ti, (ts, pos_v)) in tok_values.into_iter().zip(pos_annos).enumerate() {
            let tok_name = format!("{name}#t{ti}");
            assert!(
                update
                    .add_event(UpdateEvent::AddNode {
                        node_name: tok_name.to_string(),
                        node_type: "node".to_string()
                    })
                    .is_ok()
            );
            assert!(
                update
                    .add_event(UpdateEvent::AddNodeLabel {
                        node_name: tok_name.to_string(),
                        anno_ns: ANNIS_NS.to_string(),
                        anno_name: "tok".to_string(),
                        anno_value: ts.to_string()
                    })
                    .is_ok()
            );
            assert!(
                update
                    .add_event(UpdateEvent::AddEdge {
                        source_node: tok_name.to_string(),
                        target_node: name.to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::PartOf.to_string(),
                        component_name: "".to_string()
                    })
                    .is_ok()
            );
            assert!(
                update
                    .add_event(UpdateEvent::AddEdge {
                        source_node: s_name.to_string(),
                        target_node: tok_name.to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::Coverage.to_string(),
                        component_name: "".to_string()
                    })
                    .is_ok()
            );
            if ti > 0 {
                assert!(
                    update
                        .add_event(UpdateEvent::AddEdge {
                            source_node: format!("{name}#t{}", ti - 1),
                            target_node: tok_name.to_string(),
                            layer: ANNIS_NS.to_string(),
                            component_type: AnnotationComponentType::Ordering.to_string(),
                            component_name: "".to_string()
                        })
                        .is_ok()
                );
            }
            let pos_name = format!("{name}#pos{ti}");
            assert!(
                update
                    .add_event(UpdateEvent::AddNode {
                        node_name: pos_name.to_string(),
                        node_type: "node".to_string()
                    })
                    .is_ok()
            );
            assert!(
                update
                    .add_event(UpdateEvent::AddNodeLabel {
                        node_name: pos_name.to_string(),
                        anno_ns: "".to_string(),
                        anno_name: "pos".to_string(),
                        anno_value: pos_v.to_string()
                    })
                    .is_ok()
            );
            assert!(
                update
                    .add_event(UpdateEvent::AddEdge {
                        source_node: pos_name.to_string(),
                        target_node: name.to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::PartOf.to_string(),
                        component_name: "".to_string()
                    })
                    .is_ok()
            );
            assert!(
                update
                    .add_event(UpdateEvent::AddEdge {
                        source_node: pos_name.to_string(),
                        target_node: tok_name.to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::Coverage.to_string(),
                        component_name: "".to_string()
                    })
                    .is_ok()
            );
        }
        for (tgt, src, rel) in relations {
            let source = format!("{name}#t{src}");
            let target = format!("{name}#t{tgt}");
            assert!(
                update
                    .add_event(UpdateEvent::AddEdge {
                        source_node: source.to_string(),
                        target_node: target.to_string(),
                        layer: "".to_string(),
                        component_type: AnnotationComponentType::Pointing.to_string(),
                        component_name: "dep".to_string()
                    })
                    .is_ok()
            );
            assert!(
                update
                    .add_event(UpdateEvent::AddEdgeLabel {
                        source_node: source.to_string(),
                        target_node: target.to_string(),
                        layer: "".to_string(),
                        component_type: AnnotationComponentType::Pointing.to_string(),
                        component_name: "dep".to_string(),
                        anno_ns: "".to_string(),
                        anno_name: "deprel".to_string(),
                        anno_value: rel.to_string()
                    })
                    .is_ok()
            );
        }
    }
    assert!(graph.apply_update(&mut update, |_| {}).is_ok());
    let toml_str = r#"
        target_parent = "corpus/b"
        target_component = { ctype = "Ordering", layer = "annis", name = "" }
        target_key = "annis::tok"
        source_parent = "corpus/a"
        source_component = { ctype = "Ordering", layer = "annis", name = "" }
        source_key = "annis::tok"
        merge = true
    "#;
    let m: Result<DiffSubgraphs, _> = toml::from_str(toml_str);
    assert!(m.is_ok());
    let module = m.unwrap();
    let run = module.manipulate_corpus(
        &mut graph,
        Path::new("./"),
        StepID {
            module_name: "test".to_string(),
            path: None,
        },
        None,
    );
    assert!(
        run.is_ok(),
        "Error in diff execution: {:?}",
        run.err().unwrap()
    );
    let actual = export_to_string(
        &graph,
        toml::from_str::<GraphMLExporter>("stable_order = true").unwrap(),
    );
    assert_snapshot!(actual.unwrap());
}

#[test]
fn multiple_segmentations() {
    let mut graph = AnnotationGraph::with_default_graphstorages(true).unwrap();
    let mut update = GraphUpdate::default();
    // assert!(update.add_event().is_ok());
    assert!(
        update
            .add_event(UpdateEvent::AddNode {
                node_name: "corpus".to_string(),
                node_type: "corpus".to_string()
            })
            .is_ok()
    );
    let segments_a = [
        vec![
            ("I", 1),
            ("live", 1),
            ("in", 1),
            ("New York", 2),
            ("for", 1),
            ("my", 1),
            ("entire", 1),
            ("life", 1),
        ],
        vec![
            (" I", 1), // initial space intended, should look like a correction
            ("live", 1),
            ("in", 1),
            ("New", 1),
            ("York", 1),
            ("for", 1),
            ("my", 1),
            ("entire", 1),
            ("life", 1),
            (".", 1),
        ],
    ];
    let pos_a = [
        None,
        Some(vec![
            "PRON", "VERB", "ADP", "PROPN", "PROPN", "ADP", "PRON", "ADJ", "NOUN", "PUNCT",
        ]),
    ];
    let dep_a = [
        None,
        Some(vec![
            (0, 1, "subj"),
            (2, 1, "obl"),
            (3, 2, "comp"),
            (4, 3, "flat"),
            (5, 1, "mod"),
            (6, 8, "det"),
            (7, 8, "mod"),
            (8, 5, "comp"),
        ]),
    ];
    let segments_b = [
        vec![
            ("I", 1),
            ("have", 1),
            ("been", 1),
            ("living", 1),
            ("in", 1),
            ("New Jersey", 2),
            ("my", 1),
            ("whole", 1),
            ("life", 1),
        ],
        vec![
            ("I", 1),
            ("have", 1),
            ("been", 1),
            ("living", 1),
            ("in", 1),
            ("New", 1),
            ("Jersey", 1),
            ("my", 1),
            ("whole", 1),
            ("life", 1),
            (".", 1),
        ],
    ];
    let pos_b = [
        None,
        Some(vec![
            "PRON", "VERB", "VERB", "VERB", "ADP", "PROPN", "PROPN", "PRON", "ADJ", "NOUN", "PUNCT",
        ]),
    ];
    let dep_b = [
        None,
        Some(vec![
            (0, 1, "subj"),
            (2, 1, "comp"),
            (3, 2, "comp"),
            (4, 3, "obl"),
            (5, 4, "comp"),
            (6, 5, "flat"),
            (7, 9, "det"),
            (8, 9, "mod"),
            (9, 1, "mod"),
        ]),
    ];
    // build
    assert!(
        update
            .add_event(UpdateEvent::AddNode {
                node_name: "corpus".to_string(),
                node_type: "corpus".to_string()
            })
            .is_ok()
    );
    assert!(
        update
            .add_event(UpdateEvent::AddNode {
                node_name: "corpus/a".to_string(),
                node_type: "corpus".to_string()
            })
            .is_ok()
    );
    assert!(
        update
            .add_event(UpdateEvent::AddEdge {
                source_node: "corpus/a".to_string(),
                target_node: "corpus".to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string()
            })
            .is_ok()
    );
    assert!(
        update
            .add_event(UpdateEvent::AddNode {
                node_name: "corpus/b".to_string(),
                node_type: "corpus".to_string()
            })
            .is_ok()
    );
    assert!(
        update
            .add_event(UpdateEvent::AddEdge {
                source_node: "corpus/b".to_string(),
                target_node: "corpus".to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string()
            })
            .is_ok()
    );
    assert!(
        update
            .add_event(UpdateEvent::AddNode {
                node_name: "corpus/a/doc".to_string(),
                node_type: "corpus".to_string()
            })
            .is_ok()
    );
    assert!(
        update
            .add_event(UpdateEvent::AddNodeLabel {
                node_name: "corpus/a/doc".to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "doc".to_string(),
                anno_value: "test".to_string()
            })
            .is_ok()
    );
    assert!(
        update
            .add_event(UpdateEvent::AddEdge {
                source_node: "corpus/a/doc".to_string(),
                target_node: "corpus/a".to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string()
            })
            .is_ok()
    );
    assert!(
        update
            .add_event(UpdateEvent::AddNode {
                node_name: "corpus/b/doc".to_string(),
                node_type: "corpus".to_string()
            })
            .is_ok()
    );
    assert!(
        update
            .add_event(UpdateEvent::AddNodeLabel {
                node_name: "corpus/b/doc".to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "doc".to_string(),
                anno_value: "test".to_string()
            })
            .is_ok()
    );
    assert!(
        update
            .add_event(UpdateEvent::AddEdge {
                source_node: "corpus/b/doc".to_string(),
                target_node: "corpus/b".to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string()
            })
            .is_ok()
    );
    let n_tokens = 11;
    for (((branch, segments), pos), deps) in ["a", "b"]
        .into_iter()
        .zip([segments_a, segments_b])
        .zip([pos_a, pos_b])
        .zip([dep_a, dep_b])
    {
        let name = format!("corpus/{branch}/doc");
        let s_name = format!("{name}#s1");
        for ti in 0..n_tokens {
            let tok_name = format!("{name}#t{ti}");
            assert!(
                update
                    .add_event(UpdateEvent::AddNode {
                        node_name: tok_name.to_string(),
                        node_type: "node".to_string()
                    })
                    .is_ok()
            );
            assert!(
                update
                    .add_event(UpdateEvent::AddNodeLabel {
                        node_name: tok_name.to_string(),
                        anno_ns: ANNIS_NS.to_string(),
                        anno_name: "tok".to_string(),
                        anno_value: " ".to_string()
                    })
                    .is_ok()
            );
            assert!(
                update
                    .add_event(UpdateEvent::AddEdge {
                        source_node: tok_name.to_string(),
                        target_node: name.to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::PartOf.to_string(),
                        component_name: "".to_string()
                    })
                    .is_ok()
            );
            assert!(
                update
                    .add_event(UpdateEvent::AddEdge {
                        source_node: s_name.to_string(),
                        target_node: tok_name.to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::Coverage.to_string(),
                        component_name: "".to_string()
                    })
                    .is_ok()
            );
            if ti > 0 {
                assert!(
                    update
                        .add_event(UpdateEvent::AddEdge {
                            source_node: format!("{name}#t{}", ti - 1),
                            target_node: tok_name.to_string(),
                            layer: ANNIS_NS.to_string(),
                            component_type: AnnotationComponentType::Ordering.to_string(),
                            component_name: "".to_string()
                        })
                        .is_ok()
                );
            }
        }
        for (((seg_name, seg_elements), seg_pos), dep_rels) in ["dipl", "norm"]
            .into_iter()
            .zip(segments)
            .zip(pos)
            .zip(deps)
        {
            let mut l = 0;
            let mut pos_iter = seg_pos.unwrap_or_default().into_iter();
            for (i, (value, length)) in seg_elements.into_iter().enumerate() {
                let node_name = format!("{name}#{seg_name}{i}");
                let pos_node_name = format!("{name}#{seg_name}_pos_{i}");
                let pos_value = pos_iter.next();
                assert!(
                    update
                        .add_event(UpdateEvent::AddNode {
                            node_name: node_name.to_string(),
                            node_type: "node".to_string()
                        })
                        .is_ok()
                );
                assert!(
                    update
                        .add_event(UpdateEvent::AddNodeLabel {
                            node_name: node_name.to_string(),
                            anno_ns: "default_ns".to_string(),
                            anno_name: seg_name.to_string(),
                            anno_value: value.to_string()
                        })
                        .is_ok()
                );
                assert!(
                    update
                        .add_event(UpdateEvent::AddEdge {
                            source_node: node_name.to_string(),
                            target_node: name.to_string(),
                            layer: ANNIS_NS.to_string(),
                            component_type: AnnotationComponentType::PartOf.to_string(),
                            component_name: "".to_string()
                        })
                        .is_ok()
                );
                if let Some(v) = &pos_value {
                    assert!(
                        update
                            .add_event(UpdateEvent::AddNode {
                                node_name: pos_node_name.to_string(),
                                node_type: "node".to_string()
                            })
                            .is_ok()
                    );
                    assert!(
                        update
                            .add_event(UpdateEvent::AddEdge {
                                source_node: pos_node_name.to_string(),
                                target_node: name.to_string(),
                                layer: ANNIS_NS.to_string(),
                                component_type: AnnotationComponentType::PartOf.to_string(),
                                component_name: "".to_string()
                            })
                            .is_ok()
                    );
                    assert!(
                        update
                            .add_event(UpdateEvent::AddNodeLabel {
                                node_name: pos_node_name.to_string(),
                                anno_ns: "".to_string(),
                                anno_name: "pos".to_string(),
                                anno_value: v.to_string()
                            })
                            .is_ok()
                    );
                }
                for covered_t in l..l + length {
                    let target_node = format!("{name}#t{covered_t}");
                    assert!(
                        update
                            .add_event(UpdateEvent::AddEdge {
                                source_node: node_name.to_string(),
                                target_node,
                                layer: ANNIS_NS.to_string(),
                                component_type: AnnotationComponentType::Coverage.to_string(),
                                component_name: "".to_string()
                            })
                            .is_ok()
                    );
                }
                if i > 0 {
                    assert!(
                        update
                            .add_event(UpdateEvent::AddEdge {
                                source_node: format!("{name}#{seg_name}{}", i - 1),
                                target_node: node_name.to_string(),
                                layer: DEFAULT_NS.to_string(),
                                component_type: AnnotationComponentType::Ordering.to_string(),
                                component_name: seg_name.to_string()
                            })
                            .is_ok()
                    );
                }
                l += length;
            }
            if let Some(relations) = dep_rels {
                for (tgt, src, rel) in relations {
                    let source = format!("{name}#{seg_name}{src}");
                    let target = format!("{name}#{seg_name}{tgt}");
                    assert!(
                        update
                            .add_event(UpdateEvent::AddEdge {
                                source_node: source.to_string(),
                                target_node: target.to_string(),
                                layer: "".to_string(),
                                component_type: AnnotationComponentType::Pointing.to_string(),
                                component_name: "dep".to_string()
                            })
                            .is_ok()
                    );
                    assert!(
                        update
                            .add_event(UpdateEvent::AddEdgeLabel {
                                source_node: source.to_string(),
                                target_node: target.to_string(),
                                layer: "".to_string(),
                                component_type: AnnotationComponentType::Pointing.to_string(),
                                component_name: "dep".to_string(),
                                anno_ns: "".to_string(),
                                anno_name: "deprel".to_string(),
                                anno_value: rel.to_string()
                            })
                            .is_ok()
                    );
                }
            }
        }
    }
    assert!(graph.apply_update(&mut update, |_| {}).is_ok());
    let toml_str = r#"
        target_parent = "corpus/b"
        target_component = { ctype = "Ordering", layer = "default_ns", name = "norm" }
        target_key = "default_ns::norm"
        source_parent = "corpus/a"
        source_component = { ctype = "Ordering", layer = "default_ns", name = "norm" }
        source_key = "default_ns::norm"
        merge = true
    "#;
    let m: Result<DiffSubgraphs, _> = toml::from_str(toml_str);
    assert!(m.is_ok());
    let module = m.unwrap();
    let run = module.manipulate_corpus(
        &mut graph,
        Path::new("./"),
        StepID {
            module_name: "test".to_string(),
            path: None,
        },
        None,
    );
    assert!(
        run.is_ok(),
        "Error in diff execution: {:?}",
        run.err().unwrap()
    );
    let actual = export_to_string(
        &graph,
        toml::from_str::<GraphMLExporter>("stable_order = true\nguess_vis = true").unwrap(),
    );
    assert_snapshot!(actual.unwrap());
}
