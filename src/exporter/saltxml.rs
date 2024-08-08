mod corpus_structure;
mod document;
#[cfg(test)]
mod tests;

use std::{borrow::Cow, collections::BTreeMap};

use super::Exporter;
use anyhow::{anyhow, Context, Result};
use corpus_structure::SaltCorpusStructureMapper;
use documented::{Documented, DocumentedFields};
use graphannis::{
    graph::{AnnoKey, Annotation, NodeID},
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
    layer_positions: BTreeMap<String, usize>,
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
        // Stores the position of a single layer in the list of layers to
        // refer them in a relation.
        let mut layer_positions = BTreeMap::new();
        // Node layers

        let node_layers = graph.get_node_annos().get_all_values(&LAYER_KEY, false)?;
        for (i, l) in node_layers.iter().enumerate() {
            layer_positions.insert(l.to_string(), i);
        }

        // Find all layers and remember their position
        Ok(SaltWriter {
            graph,
            xml: writer,
            layer_positions,
        })
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
                .get(layer.as_ref())
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

        Ok(())
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
}
