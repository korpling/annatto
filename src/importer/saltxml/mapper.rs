use std::{collections::BTreeMap, convert::TryFrom, io::BufReader, path::PathBuf};

use anyhow::{anyhow, Ok};
use graphannis::update::{GraphUpdate, UpdateEvent};
use graphannis_core::util::{join_qname, split_qname};
use quick_xml::{
    events::{attributes::Attributes, BytesStart},
    Reader,
};

use crate::{
    progress::ProgressReporter,
    util::xml::{consume_start_tag_with_name, get_attribute_by_local_name, get_attribute_by_qname},
};

#[derive(Clone)]
enum SaltType {
    Corpus,
    Document,
    ElementId,
    Feature,
    CorpusRelation,
    DocumentRelation,
}

impl<'a> TryFrom<Attributes<'a>> for SaltType {
    type Error = anyhow::Error;

    fn try_from(value: Attributes<'a>) -> Result<Self, Self::Error> {
        // Use the xsi:type attribute to determine the type
        if let Some(type_id) = get_attribute_by_qname(value, "xsi", "type")? {
            match type_id.as_str() {
                "sCorpusStructure:SCorpus" => Ok(SaltType::Corpus),
                "sCorpusStructure:SDocument" => Ok(SaltType::Document),
                "saltCore:SElementId" => Ok(SaltType::ElementId),
                "saltCore:SFeature" => Ok(SaltType::Feature),
                "sCorpusStructure:SCorpusRelation" => Ok(SaltType::CorpusRelation),
                "sCorpusStructure:SCorpusDocumentRelation" => Ok(SaltType::DocumentRelation),
                _ => Err(anyhow!("Unknown Salt type {type_id}")),
            }
        } else {
            Err(anyhow!("Missing attribute xsi:type"))
        }
    }
}

fn get_label(e: &BytesStart) -> anyhow::Result<(String, String, SaltObject)> {
    let namespace = get_attribute_by_local_name(e.attributes(), "namespace")?
        .ok_or_else(|| anyhow!("Missing \"namespace\" attribute for label"))?;
    let name = get_attribute_by_local_name(e.attributes(), "name")?
        .ok_or_else(|| anyhow!("Missing \"name\" attribute for label"))?;
    let value = get_attribute_by_local_name(e.attributes(), "value")?
        .ok_or_else(|| anyhow!("Missing \"value\" attribute for label"))?;
    let value = SaltObject::try_from(value.as_str())?;
    Ok((namespace, name, value))
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

pub(crate) struct SaltXmlMapper {
    pub(crate) reporter: ProgressReporter,
}

impl SaltXmlMapper {
    pub(crate) fn new(reporter: ProgressReporter) -> SaltXmlMapper {
        SaltXmlMapper { reporter }
    }

    pub(crate) fn map_corpus_structure<R: std::io::Read>(
        &self,
        input: &mut R,
        updates: &mut GraphUpdate,
    ) -> anyhow::Result<BTreeMap<String, PathBuf>> {
        let input = BufReader::new(input);
        let mut reader = Reader::from_reader(input);
        reader.config_mut().expand_empty_elements = true;

        let mut buf = Vec::new();

        // Consume the root SaltProject and sCorpusGraphs XML elements, which do not have the "xsi:type" attribute
        consume_start_tag_with_name(&mut reader, "SaltProject")?;
        consume_start_tag_with_name(&mut reader, "sCorpusGraphs")?;

        // TODO: map corpus graph labels

        // Iterate over all child elements of the corpus graph, which are the corpus and document nodes
        let result = BTreeMap::new();
        let mut salt_type_stack = Vec::new();
        let mut current_element_id = None;
        let mut features = BTreeMap::new();
        loop {
            match reader.read_event_into(&mut buf)? {
                quick_xml::events::Event::Start(e) => {
                    let salt_type = SaltType::try_from(e.attributes())?;
                    salt_type_stack.push(salt_type.clone());

                    match salt_type {
                        SaltType::ElementId => {
                            current_element_id = None;

                            let (namespace, name, value) = get_label(&e)?;
                            if namespace == "salt" && name == "id" {
                                if let SaltObject::Text(id) = value {
                                    current_element_id =
                                        Some(id.trim_start_matches("salt:/").to_string());
                                }
                            }
                        }
                        SaltType::Feature => {
                            let (namespace, name, value) = get_label(&e)?;
                            let qname = join_qname(&namespace, &name);
                            features.insert(qname, value);
                        }
                        _ => {}
                    }
                }
                quick_xml::events::Event::End(_e) => {
                    if let Some(salt_type) = salt_type_stack.pop() {
                        match salt_type {
                            SaltType::Corpus | SaltType::Document => {
                                let node_name = current_element_id.clone().ok_or_else(|| {
                                    anyhow!("Missing element ID for corpus graph node")
                                })?;
                                // Create the element with the collected properties
                                updates.add_event(UpdateEvent::AddNode {
                                    node_name: node_name.clone(),
                                    node_type: "corpus".into(),
                                })?;

                                // Add features as annotations
                                for (feat_qname, value) in features.iter() {
                                    let (annos_ns, anno_name) = split_qname(feat_qname);

                                    updates.add_event(UpdateEvent::AddNodeLabel {
                                        node_name: node_name.clone(),
                                        anno_ns: annos_ns.unwrap_or_default().to_string(),
                                        anno_name: anno_name.to_string(),
                                        anno_value: value.to_string(),
                                    })?;
                                }

                                // Reset state
                                features.clear();
                                current_element_id = None;
                            }

                            _ => {}
                        }
                    }
                }
                quick_xml::events::Event::Eof => break,
                _ => {}
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
