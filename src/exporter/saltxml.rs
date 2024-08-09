mod corpus_structure;
mod document;
#[cfg(test)]
mod tests;

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
};

use super::Exporter;
use anyhow::{anyhow, bail, Context, Result};
use bimap::BiBTreeMap;
use corpus_structure::SaltCorpusStructureMapper;
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
        _step_id: crate::StepID,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mapper = SaltCorpusStructureMapper::new();
        mapper.map_corpus_structure(graph, output_path)?;

        Ok(())
    }

    fn file_extension(&self) -> &str {
        ".salt"
    }
}

struct SaltWriter<'a, W> {
    graph: &'a AnnotationGraph,
    xml: &'a mut EventWriter<W>,
    layer_positions: BiBTreeMap<String, usize>,
    node_positions: BTreeMap<NodeID, usize>,
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
                .map(|l| l.to_string()),
        );
        layer_names.extend(
            graph
                .get_all_components(None, None)
                .into_iter()
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
            node_positions: BTreeMap::new(),
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

    fn write_node(&mut self, n: NodeID, salt_type: &str) -> Result<()> {
        let mut node_attributes = Vec::new();
        node_attributes.push(OwnedAttribute::new(parse_attr_name("xsi:type")?, salt_type));

        // Add the layer reference to the attributes
        if let Some(layer) = self
            .graph
            .get_node_annos()
            .get_value_for_item(&n, &LAYER_KEY)?
        {
            let pos = self
                .layer_positions
                .get_by_left(layer.as_ref())
                .context("Unknown position for layer")?;
            let layer_att_value = format!("//@layers.{pos}");
            node_attributes.push(OwnedAttribute::new(
                parse_attr_name("layer")?,
                layer_att_value,
            ));
        }
        self.xml.write(XmlEvent::StartElement {
            name: "nodes".into(),
            attributes: Cow::Borrowed(&node_attributes.iter().map(|a| a.borrow()).collect_vec()),
            namespace: Cow::Owned(xml::namespace::Namespace::empty()),
        })?;

        // Write Salt ID and SNAME
        let node_name = self
            .graph
            .get_node_annos()
            .get_value_for_item(&n, &NODE_NAME_KEY)?
            .context("Missing node name")?;
        let salt_id = format!("T::salt:/{node_name}");
        self.xml.write(
            XmlEvent::start_element("labels")
                .attr("xsi:type", "saltCore:SElementId")
                .attr("namespace", "salt")
                .attr("name", "id")
                .attr("value", &salt_id),
        )?;
        self.xml.write(XmlEvent::end_element())?;
        let short_node_name = if salt_type == "sCorpusStructure:SDocument" {
            self.graph
                .get_node_annos()
                .get_value_for_item(&n, &DOC_KEY)?
                .context("Missing annis:doc annotation for document node")?
        } else {
            // Get the last part of the URI path
            Cow::Borrowed(node_name.split('/').last().unwrap_or_default())
        };
        self.xml.write(
            XmlEvent::start_element("labels")
                .attr("xsi:type", "saltCore:SFeature")
                .attr("namespace", "salt")
                .attr("name", "SNAME")
                .attr("value", &short_node_name),
        )?;
        self.xml.write(XmlEvent::end_element())?;

        // Write all other annotations as labels
        for anno in self.graph.get_node_annos().get_annotations_for_item(&n)? {
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

        // Remember the position of this node in the XML file
        self.node_positions.insert(n, self.node_positions.len() + 1);

        Ok(())
    }

    fn write_edge(&mut self, edge: Edge, component: &AnnotationComponent) -> Result<()> {
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

        let mut edge_attributes = Vec::new();
        edge_attributes.push(OwnedAttribute::new(parse_attr_name("xsi:type")?, salt_type));

        let source_position = self
            .node_positions
            .get(&output_edge.source)
            .context("Missing position for source node")?;

        let target_position = self
            .node_positions
            .get(&output_edge.target)
            .context("Missing position for target node")?;

        edge_attributes.push(OwnedAttribute::new(
            parse_attr_name("source")?,
            format!("//@nodes.{source_position}"),
        ));
        edge_attributes.push(OwnedAttribute::new(
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
            edge_attributes.push(OwnedAttribute::new(
                parse_attr_name("layer")?,
                layer_att_value,
            ));
        }
        self.xml.write(XmlEvent::StartElement {
            name: "edges".into(),
            attributes: Cow::Borrowed(&edge_attributes.iter().map(|a| a.borrow()).collect_vec()),
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
}
