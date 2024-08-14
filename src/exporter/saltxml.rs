mod corpus_structure;
mod document;
#[cfg(test)]
mod tests;

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, HashMap},
};

use crate::progress::ProgressReporter;

use super::Exporter;
use anyhow::{anyhow, bail, Context, Result};
use bimap::BiBTreeMap;
use corpus_structure::SaltCorpusStructureMapper;
use document::SaltDocumentGraphMapper;
use documented::{Documented, DocumentedFields};
use graphannis::{
    graph::{AnnoKey, Annotation, Edge, NodeID},
    model::{AnnotationComponent, AnnotationComponentType},
    AnnotationGraph,
};
use graphannis_core::graph::{ANNIS_NS, NODE_NAME_KEY};
use itertools::Itertools;
use lazy_static::lazy_static;
use serde::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;
use xml::{attribute::OwnedAttribute, name::OwnedName, writer::XmlEvent, EventWriter};

/// Exports to the SaltXML format used by Pepper (<https://corpus-tools.org/pepper/>).
/// SaltXML is an XMI serialization of the [Salt model](https://raw.githubusercontent.com/korpling/salt/master/gh-site/doc/salt_modelGuide.pdf).
/// ```
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields)]
pub struct ExportSaltXml {}

impl Exporter for ExportSaltXml {
    fn export_corpus(
        &self,
        graph: &graphannis::AnnotationGraph,
        output_path: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let progress = ProgressReporter::new_unknown_total_work(tx.clone(), step_id.clone())?;
        let corpus_mapper = SaltCorpusStructureMapper::new();
        progress.info("Mapping SaltXML corpus structure")?;
        let document_node_ids = corpus_mapper.map_corpus_structure(graph, output_path)?;
        let progress = ProgressReporter::new(tx, step_id, document_node_ids.len())?;
        for id in document_node_ids {
            let doc_mapper = SaltDocumentGraphMapper::new();
            doc_mapper.map_document_graph(graph, id, output_path)?;
            progress.worked(1)?;
        }

        Ok(())
    }

    fn file_extension(&self) -> &str {
        ".salt"
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
enum NodeType {
    Id(NodeID),
    Custom(String),
}

struct SaltWriter<'a, W> {
    graph: &'a AnnotationGraph,
    xml: &'a mut EventWriter<W>,
    layer_positions: BiBTreeMap<String, usize>,
    node_positions: BTreeMap<NodeType, usize>,
    number_of_edges: usize,
    nodes_in_layer: HashMap<String, Vec<usize>>,
    edges_in_layer: HashMap<String, Vec<usize>>,
}

lazy_static! {
    static ref LAYER_KEY: AnnoKey = {
        AnnoKey {
            ns: ANNIS_NS.into(),
            name: "layer".into(),
        }
    };
    static ref DOC_KEY: AnnoKey = {
        AnnoKey {
            ns: ANNIS_NS.into(),
            name: "doc".into(),
        }
    };
    static ref TOK_WHITESPACE_BEFORE_KEY: AnnoKey = {
        AnnoKey {
            ns: ANNIS_NS.into(),
            name: "tok-whitespace-before".into(),
        }
    };
    static ref TOK_WHITESPACE_AFTER_KEY: AnnoKey = {
        AnnoKey {
            ns: ANNIS_NS.into(),
            name: "tok-whitespace-after".into(),
        }
    };
}

fn parse_attr_name<S>(name: S) -> Result<OwnedName>
where
    S: AsRef<str>,
{
    let result = name
        .as_ref()
        .parse()
        .map_err(|_| anyhow!("Invalid attribute name '{}'", name.as_ref()))?;
    Ok(result)
}

impl<'a, W> SaltWriter<'a, W>
where
    W: std::io::Write,
{
    fn new(graph: &'a AnnotationGraph, writer: &'a mut EventWriter<W>) -> Result<Self> {
        // Collect node and edge layer names
        let mut layer_names = BTreeSet::new();
        layer_names.extend(
            graph
                .get_node_annos()
                .get_all_values(&LAYER_KEY, false)?
                .into_iter()
                .filter(|l| !l.is_empty())
                .map(|l| l.to_string()),
        );
        layer_names.extend(
            graph
                .get_all_components(None, None)
                .into_iter()
                .filter(|c| !c.layer.is_empty())
                .map(|c| c.layer.to_string()),
        );
        // Create a map of all layer names to their position in the XML file.
        let layer_positions = layer_names
            .into_iter()
            .enumerate()
            .map(|(pos, layer)| (layer, pos + 1))
            .collect();

        Ok(SaltWriter {
            graph,
            xml: writer,
            layer_positions,
            number_of_edges: 0,
            node_positions: BTreeMap::new(),
            nodes_in_layer: HashMap::new(),
            edges_in_layer: HashMap::new(),
        })
    }

    fn write_label(&mut self, anno: &Annotation, salt_type: &str) -> Result<()> {
        let anno_ns: &str = &anno.key.ns;
        let anno_name: &str = &anno.key.name;

        let mut label = XmlEvent::start_element("labels").attr("xsi:type", salt_type);
        if !anno_ns.is_empty() {
            if anno_name == "SDATA" {
                label = label.attr("namespace", "saltCommon");
            } else {
                label = label.attr("namespace", anno_ns);
            }
        }
        if !anno_name.is_empty() {
            label = label.attr("name", anno_name);
        }
        let value = format!("T::{}", anno.val);
        label = label.attr("value", &value);
        self.xml.write(label)?;
        self.xml.write(XmlEvent::end_element())?;
        Ok(())
    }

    fn write_graphannis_node(&mut self, n: NodeID, salt_type: &str) -> Result<()> {
        // Get the layer from the attribute
        let layer = self
            .graph
            .get_node_annos()
            .get_value_for_item(&n, &LAYER_KEY)?
            .map(|l| l.to_string());

        // Collect all annotations for this nodes labels
        let annotations = self.graph.get_node_annos().get_annotations_for_item(&n)?;

        // Use the "annis:doc" label as SNAME or the fragment of the URI as fallback
        let sname = if salt_type == "sCorpusStructure:SDocument" {
            self.graph
                .get_node_annos()
                .get_value_for_item(&n, &DOC_KEY)?
                .context("Missing annis:doc annotation for document node")?
        } else {
            let node_name = self
                .graph
                .get_node_annos()
                .get_value_for_item(&n, &NODE_NAME_KEY)?
                .context("Missing node name")?;
            Cow::Owned(node_name.split('#').last().unwrap_or_default().to_string())
        };

        // Use the more general method to actual write the XML
        self.write_node(NodeType::Id(n), &sname, salt_type, &annotations, layer)?;
        Ok(())
    }

    fn write_node(
        &mut self,
        n: NodeType,
        sname: &str,
        salt_type: &str,
        output_annotations: &[Annotation],
        layer: Option<String>,
    ) -> Result<()> {
        // Remember the position of this node in the XML file
        let node_position = self.node_positions.len() + 1;
        self.node_positions.insert(n.clone(), node_position);

        let mut attributes = Vec::new();
        attributes.push(OwnedAttribute::new(parse_attr_name("xsi:type")?, salt_type));

        // Add the layer reference to the attributes
        if let Some(layer) = layer {
            let pos = self
                .layer_positions
                .get_by_left(&layer)
                .context("Unknown position for layer")?;
            let layer_att_value = format!("//@layers.{pos}");
            attributes.push(OwnedAttribute::new(
                parse_attr_name("layer")?,
                layer_att_value,
            ));
            self.nodes_in_layer
                .entry(layer.to_string())
                .or_default()
                .push(node_position);
        }
        self.xml.write(XmlEvent::StartElement {
            name: "nodes".into(),
            attributes: Cow::Borrowed(&attributes.iter().map(|a| a.borrow()).collect_vec()),
            namespace: Cow::Owned(xml::namespace::Namespace::empty()),
        })?;

        // Write Salt ID and SNAME
        let node_name = match &n {
            NodeType::Id(n) => self
                .graph
                .get_node_annos()
                .get_value_for_item(&n, &NODE_NAME_KEY)?
                .context("Missing node name")?
                .to_string(),
            NodeType::Custom(node_name) => node_name.clone(),
        };
        let salt_id = format!("T::salt:/{node_name}");
        self.xml.write(
            XmlEvent::start_element("labels")
                .attr("xsi:type", "saltCore:SElementId")
                .attr("namespace", "salt")
                .attr("name", "id")
                .attr("value", &salt_id),
        )?;
        self.xml.write(XmlEvent::end_element())?;

        // Get the last part of the URI path

        self.xml.write(
            XmlEvent::start_element("labels")
                .attr("xsi:type", "saltCore:SFeature")
                .attr("namespace", "salt")
                .attr("name", "SNAME")
                .attr("value", sname),
        )?;
        self.xml.write(XmlEvent::end_element())?;

        // Write all other annotations as labels
        for anno in output_annotations {
            if anno.key.ns != "annis" {
                let label_type = if salt_type == "sCorpusStructure:SCorpus"
                    || salt_type == "sCorpusStructure:SDocument"
                {
                    "saltCore:SMetaAnnotation"
                } else {
                    "saltCore:SAnnotation"
                };
                self.write_label(&anno, label_type)?;
            }
        }

        self.xml.write(XmlEvent::end_element())?;

        Ok(())
    }

    fn write_edge(&mut self, edge: Edge, component: &AnnotationComponent) -> Result<()> {
        self.number_of_edges += 1;

        let gs = self
            .graph
            .get_graphstorage_as_ref(component)
            .context("Missing graph storage for edge component")?;
        let salt_type = match component.get_type() {
            AnnotationComponentType::Coverage => "sDocumentStructure:SSpanningRelation",
            AnnotationComponentType::Dominance => "sDocumentStructure:SDominanceRelation",
            AnnotationComponentType::Pointing => "sDocumentStructure:SPointingRelation",
            AnnotationComponentType::Ordering => "sDocumentStructure:SOrderRelation",
            AnnotationComponentType::PartOf => {
                // Check if this is a document or a (sub)-corpus by testing if there are any incoming PartOfEdges
                if gs.has_ingoing_edges(edge.source)? {
                    "sCorpusStructure:SCorpusDocumentRelation"
                } else {
                    "sCorpusStructure:SCorpusRelation"
                }
            }
            _ => {
                bail!(
                    "Invalid component type {} for SaltXML",
                    component.get_type()
                )
            }
        };
        // Invert edge for PartOf components
        let output_edge = if component.get_type() == AnnotationComponentType::PartOf {
            edge.inverse()
        } else {
            edge.clone()
        };

        let mut attributes = Vec::new();
        attributes.push(OwnedAttribute::new(parse_attr_name("xsi:type")?, salt_type));

        let source_position = self
            .node_positions
            .get(&NodeType::Id(output_edge.source))
            .context("Missing position for source node")?;

        let target_position = self
            .node_positions
            .get(&NodeType::Id(output_edge.target))
            .context("Missing position for target node")?;

        attributes.push(OwnedAttribute::new(
            parse_attr_name("source")?,
            format!("//@nodes.{source_position}"),
        ));
        attributes.push(OwnedAttribute::new(
            parse_attr_name("source")?,
            format!("//@nodes.{target_position}"),
        ));

        // Add the layer reference to the attributes
        if !component.layer.is_empty() {
            let pos = self
                .layer_positions
                .get_by_left(component.layer.as_str())
                .context("Unknown position for layer")?;
            let layer_att_value = format!("//@layers.{pos}");
            attributes.push(OwnedAttribute::new(
                parse_attr_name("layer")?,
                layer_att_value,
            ));
            self.edges_in_layer
                .entry(component.layer.to_string())
                .or_default()
                .push(self.number_of_edges);
        }
        self.xml.write(XmlEvent::StartElement {
            name: "edges".into(),
            attributes: Cow::Borrowed(&attributes.iter().map(|a| a.borrow()).collect_vec()),
            namespace: Cow::Owned(xml::namespace::Namespace::empty()),
        })?;
        // add all edge labels
        for anno in gs.get_anno_storage().get_annotations_for_item(&edge)? {
            if anno.key.ns != "annis" {
                self.write_label(&anno, "saltCore:SAnnotation")?;
            }
        }
        self.xml.write(XmlEvent::end_element())?;

        Ok(())
    }

    fn write_all_layers(&mut self) -> Result<()> {
        // Iterate over the layers in order of their position
        for (layer, pos) in self.layer_positions.right_range(..) {
            let mut attributes = Vec::new();
            attributes.push(OwnedAttribute::new(
                parse_attr_name("xsi:type")?,
                "saltCore:SLayer",
            ));

            // Write nodes as attribute
            if let Some(included_positions) = self.nodes_in_layer.get(layer) {
                let att_value = position_references_to_string(included_positions, "nodes");
                attributes.push(OwnedAttribute::new(parse_attr_name("nodes")?, att_value));
            }

            // Write edges as attributes
            if let Some(included_positions) = self.edges_in_layer.get(layer) {
                let att_value = position_references_to_string(included_positions, "edges");
                attributes.push(OwnedAttribute::new(parse_attr_name("edges")?, att_value));
            }

            self.xml.write(XmlEvent::StartElement {
                name: "layers".into(),
                attributes: Cow::Borrowed(&attributes.iter().map(|a| a.borrow()).collect_vec()),
                namespace: Cow::Owned(xml::namespace::Namespace::empty()),
            })?;

            let marshalled_id = format!("T::l{pos}");
            let id_label = XmlEvent::start_element("labels")
                .attr("xsi:type", "saltCore:SElementId")
                .attr("namespace", "salt")
                .attr("name", "id")
                .attr("value", &marshalled_id);
            self.xml.write(id_label)?;
            self.xml.write(XmlEvent::end_element())?;

            let marshalled_name = format!("T::{layer}");
            let id_label = XmlEvent::start_element("labels")
                .attr("xsi:type", "saltCore:SFeature")
                .attr("namespace", "salt")
                .attr("name", "SNAME")
                .attr("value", &marshalled_name);
            self.xml.write(id_label)?;
            self.xml.write(XmlEvent::end_element())?;

            self.xml.write(XmlEvent::end_element())?;
        }
        Ok(())
    }
}

fn position_references_to_string(included_positions: &[usize], att_name: &str) -> String {
    let mut att_value = String::new();
    for (i, pos) in included_positions.iter().enumerate() {
        if i > 0 {
            att_value.push(' ');
        }
        att_value.push_str("//@");
        att_value.push_str(att_name);
        att_value.push('.');
        att_value.push_str(&pos.to_string());
    }
    att_value
}
