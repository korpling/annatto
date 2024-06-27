use std::{collections::BTreeSet, convert::TryFrom};

use anyhow::{anyhow, Ok};
use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::ANNIS_NS;
use itertools::Itertools;

use crate::progress::ProgressReporter;

use super::{get_element_id, resolve_element, SaltObject, SaltType};

pub(super) struct SaltCorpusStructureMapper {
    reporter: ProgressReporter,
}

impl SaltCorpusStructureMapper {
    pub(super) fn new(reporter: ProgressReporter) -> SaltCorpusStructureMapper {
        SaltCorpusStructureMapper { reporter }
    }

    pub(super) fn map_corpus_structure(
        &self,
        input: &str,
        updates: &mut GraphUpdate,
    ) -> anyhow::Result<BTreeSet<String>> {
        let doc = roxmltree::Document::parse(input)?;

        let root = doc.root_element();
        if root.tag_name().name() != "SaltProject" {
            return Err(anyhow!(
                "SaltXML project file must start with <SaltProject> tag"
            ));
        }

        let mut documents = BTreeSet::new();

        // Iterate over all corpus graphs
        for cg in root
            .children()
            .filter(|t| t.tag_name().name() == "sCorpusGraphs")
        {
            // TODO: map corpus graph labels

            // Get all nodes
            let nodes = cg
                .children()
                .filter(|t| t.tag_name().name() == "nodes")
                .collect_vec();

            for node in nodes.iter() {
                let salt_type = SaltType::from(*node);
                match salt_type {
                    SaltType::Corpus | SaltType::Document => {
                        // Get the element ID from the label
                        let node_name = get_element_id(node)
                            .ok_or_else(|| anyhow!("Missing element ID for corpus graph node"))?;
                        // Create the element with the collected properties
                        updates.add_event(UpdateEvent::AddNode {
                            node_name: node_name.to_string(),
                            node_type: "corpus".into(),
                        })?;

                        // Add the document ID to the result
                        if SaltType::Document == salt_type {
                            documents.insert(node_name.to_string());
                        }

                        // Add features as annotations
                        let features = node.children().filter(|n| {
                            n.tag_name().name() == "labels"
                                && SaltType::from(*n) == SaltType::Feature
                        });
                        for feature_node in features {
                            let annos_ns = feature_node.attribute("namespace");
                            let anno_name = feature_node.attribute("name").ok_or_else(|| {
                                anyhow!("Missing \"name\" attribute for node \"{node_name}\"")
                            })?;
                            let anno_value = SaltObject::try_from(
                                feature_node.attribute("value").unwrap_or_default(),
                            )?;

                            if salt_type == SaltType::Document
                                && annos_ns == Some("salt")
                                && anno_name == "SNAME"
                            {
                                updates.add_event(UpdateEvent::AddNodeLabel {
                                    node_name: node_name.to_string(),
                                    anno_ns: ANNIS_NS.to_string(),
                                    anno_name: "doc".to_string(),
                                    anno_value: anno_value.to_string(),
                                })?;
                            } else {
                                updates.add_event(UpdateEvent::AddNodeLabel {
                                    node_name: node_name.to_string(),
                                    anno_ns: annos_ns.unwrap_or_default().to_string(),
                                    anno_name: anno_name.to_string(),
                                    anno_value: anno_value.to_string(),
                                })?;
                            }
                        }

                        // TODO: map annotations (that are not features)
                    }
                    _ => {}
                }
            }

            // Add a PartOf Edge between parent corpora and the sub-corpora/documents
            for e in cg.children().filter(|n| n.tag_name().name() == "edges") {
                match SaltType::from(e) {
                    SaltType::CorpusRelation | SaltType::DocumentRelation => {
                        let source_ref = e.attribute("source").unwrap_or_default();
                        let target_ref = e.attribute("target").unwrap_or_default();

                        let source_node = resolve_element(source_ref, "nodes", &nodes)
                            .and_then(|n| get_element_id(&n));
                        let target_node = resolve_element(target_ref, "nodes", &nodes)
                            .and_then(|n| get_element_id(&n));

                        if let (Some(source_node), Some(target_node)) = (source_node, target_node) {
                            // PartOf has the inverse meaning of the corpus and documentation relation in Salt
                            updates.add_event(UpdateEvent::AddEdge {
                                source_node: target_node,
                                target_node: source_node,
                                layer: ANNIS_NS.to_string(),
                                component_type: AnnotationComponentType::PartOf.to_string(),
                                component_name: "".into(),
                            })?;
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(documents)
    }
}
