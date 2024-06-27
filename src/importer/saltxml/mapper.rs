use std::{collections::BTreeMap, convert::TryFrom, path::PathBuf};

use anyhow::{anyhow, Ok};
use graphannis::update::{GraphUpdate, UpdateEvent};
use itertools::Itertools;
use roxmltree::Node;

use crate::progress::ProgressReporter;

const XSI_NAMESPACE: &str = "http://www.w3.org/2001/XMLSchema-instance";

#[derive(Debug, Clone, PartialEq)]
enum SaltType {
    Corpus,
    Document,
    ElementId,
    Feature,
    CorpusRelation,
    DocumentRelation,
    Unknown,
}

impl<'a, 'input> From<Node<'a, 'input>> for SaltType {
    fn from(n: Node) -> Self {
        // Use the xsi:type attribute to determine the type
        if let Some(type_id) = n.attribute((XSI_NAMESPACE, "type")) {
            match type_id {
                "sCorpusStructure:SCorpus" => SaltType::Corpus,
                "sCorpusStructure:SDocument" => SaltType::Document,
                "saltCore:SElementId" => SaltType::ElementId,
                "saltCore:SFeature" => SaltType::Feature,
                "sCorpusStructure:SCorpusRelation" => SaltType::CorpusRelation,
                "sCorpusStructure:SCorpusDocumentRelation" => SaltType::DocumentRelation,
                _ => SaltType::Unknown,
            }
        } else {
            SaltType::Unknown
        }
    }
}

enum SaltObject {
    Text(String),
    Boolean(bool),
}

impl TryFrom<&str> for SaltObject {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if let Some(value) = value.strip_prefix("T::") {
            Ok(SaltObject::Text(value.to_string()))
        } else if let Some(_value) = value.strip_prefix("B::") {
            let value = value.to_ascii_lowercase() == "true";
            Ok(SaltObject::Boolean(value))
        } else {
            Err(anyhow!("Could not create Salt object from \"{value}\""))
        }
    }
}

impl std::fmt::Display for SaltObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SaltObject::Text(val) => write!(f, "{val}"),
            SaltObject::Boolean(val) => write!(f, "{val}"),
        }
    }
}

fn get_element_id(n: &Node) -> Option<String> {
    for element_id_label in n
        .children()
        .filter(|c| c.tag_name().name() == "labels" && SaltType::from(*c) == SaltType::ElementId)
    {
        if let Some(id) = element_id_label.attribute("value") {
            let id = SaltObject::try_from(id).ok()?;
            return Some(id.to_string().trim_start_matches("salt:/").to_string());
        }
    }
    None
}

pub(crate) struct SaltXmlMapper {
    pub(crate) reporter: ProgressReporter,
}

impl SaltXmlMapper {
    pub(crate) fn new(reporter: ProgressReporter) -> SaltXmlMapper {
        SaltXmlMapper { reporter }
    }

    pub(crate) fn map_corpus_structure(
        &self,
        input: &str,
        updates: &mut GraphUpdate,
    ) -> anyhow::Result<BTreeMap<String, PathBuf>> {
        let doc = roxmltree::Document::parse(input)?;

        let root = doc.root_element();
        if root.tag_name().name() != "SaltProject" {
            return Err(anyhow!(
                "SaltXML project file must start with <SaltProject> tag"
            ));
        }

        let result = BTreeMap::new();

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
                match SaltType::from(*node) {
                    SaltType::Corpus | SaltType::Document => {
                        // Get the element ID from the label
                        let node_name = get_element_id(node)
                            .ok_or_else(|| anyhow!("Missing element ID for corpus graph node"))?;
                        // Create the element with the collected properties
                        updates.add_event(UpdateEvent::AddNode {
                            node_name: node_name.to_string(),
                            node_type: "corpus".into(),
                        })?;

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

                            updates.add_event(UpdateEvent::AddNodeLabel {
                                node_name: node_name.to_string(),
                                anno_ns: annos_ns.unwrap_or_default().to_string(),
                                anno_name: anno_name.to_string(),
                                anno_value: anno_value.to_string(),
                            })?;
                        }
                    }
                    _ => {}
                }
            }

            // Add a PartOf Edge between parent corpora and the sub-corpora/documents
            for e in cg.children().filter(|n| n.tag_name().name() == "edges") {
                match SaltType::from(e) {
                    SaltType::CorpusRelation => {}
                    SaltType::DocumentRelation => {}
                    _ => {}
                }
            }
        }

        Ok(result)
    }

    pub(crate) fn read_document<R: std::io::Read>(
        &self,
        _input: &mut R,
        _document_node_name: &str,
        _updates: &mut GraphUpdate,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
