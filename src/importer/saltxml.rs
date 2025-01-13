use document::DocumentMapper;
use documented::{Documented, DocumentedFields};
use graphannis::update::GraphUpdate;
use roxmltree::Node;
use serde::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use crate::progress::ProgressReporter;

use super::Importer;

/// Imports the SaltXML format used by Pepper (<https://corpus-tools.org/pepper/>).
/// SaltXML is an XMI serialization of the [Salt model](https://raw.githubusercontent.com/korpling/salt/master/gh-site/doc/salt_modelGuide.pdf).
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(default, deny_unknown_fields)]
pub struct ImportSaltXml {
    /// If `true`, use the layer name as fallback for the namespace annotations
    /// if none is given. This is consistent with how the ANNIS tree visualizer
    /// handles annotations without any namespace. If `false`, use the
    /// `default_ns` namespace as fallback.
    missing_anno_ns_from_layer: bool,
}

impl Default for ImportSaltXml {
    fn default() -> Self {
        Self {
            missing_anno_ns_from_layer: true,
        }
    }
}

impl Importer for ImportSaltXml {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut updates = GraphUpdate::new();
        // Start  with an undetermined progress reporter
        let reporter = ProgressReporter::new_unknown_total_work(tx.clone(), step_id.clone())?;
        let mapper = corpus_structure::SaltCorpusStructureMapper::new();

        // Read the corpus structure from the Salt project and get the number of documents to create
        reporter.info("Reading SaltXML project structure")?;
        let project_file = std::fs::read_to_string(input_path.join("saltProject.salt"))?;
        let documents = mapper.map_corpus_structure(&project_file, &mut updates)?;

        // Create a new progress reporter that can now estimate the work based on the number of documents
        let reporter = ProgressReporter::new(tx, step_id, documents.len())?;
        for document_node_name in documents {
            reporter.info(&format!("Reading document {document_node_name}"))?;

            let mut relative_document_path = document_node_name.clone();
            relative_document_path.push_str(".salt");
            // Get the path from the node name
            let document_path = input_path.join(relative_document_path);
            let document_file = std::fs::read_to_string(&document_path)?;
            DocumentMapper::read_document(
                &document_file,
                &document_path,
                self.missing_anno_ns_from_layer,
                &mut updates,
            )?;
            reporter.worked(1)?;
        }

        Ok(updates)
    }

    fn file_extensions(&self) -> &[&str] {
        &[]
    }
}

const XSI_NAMESPACE: &str = "http://www.w3.org/2001/XMLSchema-instance";

#[derive(Debug, Clone, Copy, PartialEq)]
enum SaltType {
    Annotation,
    Corpus,
    CorpusRelation,
    Document,
    DocumentRelation,
    DominanceRelation,
    ElementId,
    Feature,
    Layer,
    MediaDs,
    MediaRelation,
    MetaAnnotation,
    PointingRelation,
    Span,
    SpanningRelation,
    Structure,
    TextualDs,
    TextualRelation,
    Timeline,
    TimelineRelation,
    Token,
    Unknown,
}

impl SaltType {
    fn from_node(n: &Node) -> SaltType {
        // Use the xsi:type attribute to determine the type
        if let Some(type_id) = n.attribute((XSI_NAMESPACE, "type")) {
            match type_id {
                "saltCore:SAnnotation" => SaltType::Annotation,
                "saltCore:SElementId" => SaltType::ElementId,
                "saltCore:SFeature" => SaltType::Feature,
                "saltCore:SLayer" => SaltType::Layer,
                "saltCore:SMetaAnnotation" => SaltType::MetaAnnotation,
                "sCorpusStructure:SCorpus" => SaltType::Corpus,
                "sCorpusStructure:SCorpusDocumentRelation" => SaltType::DocumentRelation,
                "sCorpusStructure:SCorpusRelation" => SaltType::CorpusRelation,
                "sCorpusStructure:SDocument" => SaltType::Document,
                "sDocumentStructure:SAudioDS" => SaltType::MediaDs,
                "sDocumentStructure:SAudioRelation" => SaltType::MediaRelation,
                "sDocumentStructure:SDominanceRelation" => SaltType::DominanceRelation,
                "sDocumentStructure:SPointingRelation" => SaltType::PointingRelation,
                "sDocumentStructure:SSpan" => SaltType::Span,
                "sDocumentStructure:SSpanningRelation" => SaltType::SpanningRelation,
                "sDocumentStructure:SStructure" => SaltType::Structure,
                "sDocumentStructure:STextualDS" => SaltType::TextualDs,
                "sDocumentStructure:STextualRelation" => SaltType::TextualRelation,
                "sDocumentStructure:STimeline" => SaltType::Timeline,
                "sDocumentStructure:STimelineRelation" => SaltType::TimelineRelation,
                "sDocumentStructure:SToken" => SaltType::Token,

                _ => SaltType::Unknown,
            }
        } else {
            SaltType::Unknown
        }
    }
}

#[derive(Debug)]
pub(crate) enum SaltObject {
    Text(String),
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Url(String),
    Null,
}

impl SaltObject {
    /// Create a string representation that can be used as attribute value in SaltXML.
    pub(crate) fn marshall(&self) -> String {
        match self {
            SaltObject::Text(v) => format!("T::{v}"),
            SaltObject::Boolean(v) => format!("B::{v}"),
            SaltObject::Integer(v) => format!("N::{v}"),
            SaltObject::Float(v) => format!("F::{v}"),
            SaltObject::Url(v) => format!("U::{v}"),
            SaltObject::Null => String::default(),
        }
    }
}

impl From<&str> for SaltObject {
    fn from(value: &str) -> Self {
        if let Some(value) = value.strip_prefix("T::") {
            SaltObject::Text(value.to_string())
        } else if let Some(value) = value.strip_prefix("U::") {
            SaltObject::Url(value.to_string())
        } else if let Some(value) = value.strip_prefix("B::") {
            let value = value.eq_ignore_ascii_case("true");
            SaltObject::Boolean(value)
        } else if let Some(value) = value.strip_prefix("N::") {
            let value = value.parse::<i64>().unwrap_or_default();
            SaltObject::Integer(value)
        } else if let Some(value) = value.strip_prefix("F::") {
            let value = value.parse::<f64>().unwrap_or_default();
            SaltObject::Float(value)
        } else {
            SaltObject::Null
        }
    }
}

impl std::fmt::Display for SaltObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SaltObject::Text(val) => write!(f, "{val}"),
            SaltObject::Url(val) => write!(f, "{val}"),
            SaltObject::Boolean(val) => write!(f, "{val}"),
            SaltObject::Integer(val) => write!(f, "{val}"),
            SaltObject::Float(val) => write!(f, "{val}"),
            SaltObject::Null => write!(f, ""),
        }
    }
}

fn get_element_id(n: &Node) -> Option<String> {
    for element_id_label in n.children().filter(|c| {
        c.tag_name().name() == "labels" && SaltType::from_node(c) == SaltType::ElementId
    }) {
        if let Some(id) = element_id_label.attribute("value") {
            let id = SaltObject::from(id);
            return Some(id.to_string().trim_start_matches("salt:/").to_string());
        }
    }
    None
}

fn get_features<'a, 'input>(n: &'a Node<'a, 'input>) -> impl Iterator<Item = Node<'a, 'input>> {
    n.children()
        .filter(|n| n.tag_name().name() == "labels" && SaltType::from_node(n) == SaltType::Feature)
}

fn get_annotations<'a, 'input>(n: &'a Node<'a, 'input>) -> impl Iterator<Item = Node<'a, 'input>> {
    n.children().filter(|n| {
        n.tag_name().name() == "labels" && SaltType::from_node(n) == SaltType::Annotation
    })
}

fn get_meta_annotations<'a, 'input>(
    n: &'a Node<'a, 'input>,
) -> impl Iterator<Item = Node<'a, 'input>> {
    n.children().filter(|n| {
        n.tag_name().name() == "labels" && SaltType::from_node(n) == SaltType::MetaAnnotation
    })
}

fn get_feature_by_qname(n: &Node, namespace: &str, name: &str) -> Option<SaltObject> {
    get_features(n)
        .filter(|f| {
            f.attribute("namespace") == Some(namespace) && f.attribute("name") == Some(name)
        })
        .filter_map(|f| f.attribute("value"))
        .map(SaltObject::from)
        .next()
}

fn get_referenced_index(attribute_value: &str, tag_name: &str) -> Option<usize> {
    let mut pattern = String::with_capacity(tag_name.len() + 4);
    pattern.push_str("//@");
    pattern.push_str(tag_name);
    pattern.push('.');

    let index_as_str = attribute_value.strip_prefix(&pattern)?;
    let idx = index_as_str.parse::<usize>().ok()?;
    Some(idx)
}

fn resolve_element<'a>(
    attribute_value: &str,
    tag_name: &str,
    elements: &'a [Node],
) -> Option<Node<'a, 'a>> {
    let idx = get_referenced_index(attribute_value, tag_name)?;
    elements.get(idx).copied()
}

mod corpus_structure;
mod document;

#[cfg(test)]
mod tests;
