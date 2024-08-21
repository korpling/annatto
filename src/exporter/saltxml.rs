mod corpus_structure;
mod document;
#[cfg(test)]
mod tests;

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, HashMap},
    path::PathBuf,
};

use crate::{importer::saltxml::SaltObject, progress::ProgressReporter};

use super::Exporter;
use anyhow::{bail, Context, Result};
use bimap::BiBTreeMap;
use corpus_structure::SaltCorpusStructureMapper;
use document::SaltDocumentGraphMapper;
use documented::{Documented, DocumentedFields};
use graphannis::{
    graph::{AnnoKey, Edge, NodeID},
    model::{AnnotationComponent, AnnotationComponentType},
    AnnotationGraph,
};
use graphannis_core::graph::{ANNIS_NS, NODE_NAME_KEY};

use lazy_static::lazy_static;
use quick_xml::{
    events::{BytesStart, Event},
    Writer,
};
use serde::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

/// Exports to the SaltXML format used by Pepper
/// (<https://corpus-tools.org/pepper/>). SaltXML is an XMI serialization of the
/// [Salt
/// model](https://raw.githubusercontent.com/korpling/salt/master/gh-site/doc/salt_modelGuide.pdf).
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice, Default)]
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

        std::fs::create_dir_all(output_path)?;

        progress.info("Writing SaltXML corpus structure")?;
        let document_node_ids =
            corpus_mapper.map_corpus_structure(graph, output_path, &progress)?;
        let progress = ProgressReporter::new(tx, step_id, document_node_ids.len())?;
        for id in document_node_ids {
            let mut doc_mapper = SaltDocumentGraphMapper::new();
            doc_mapper.map_document_graph(graph, id, output_path, &progress)?;
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
    xml: &'a mut Writer<W>,
    output_path: PathBuf,
    progress: &'a ProgressReporter,
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

impl<'a, W> SaltWriter<'a, W>
where
    W: std::io::Write,
{
    fn new(
        graph: &'a AnnotationGraph,
        writer: &'a mut Writer<W>,
        output_path: &std::path::Path,
        progress: &'a ProgressReporter,
    ) -> Result<Self> {
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
            .map(|(pos, layer)| (layer, pos))
            .collect();

        Ok(SaltWriter {
            graph,
            xml: writer,
            output_path: output_path.to_path_buf(),
            progress,
            layer_positions,
            number_of_edges: 0,
            node_positions: BTreeMap::new(),
            nodes_in_layer: HashMap::new(),
            edges_in_layer: HashMap::new(),
        })
    }

    fn write_label(&mut self, key: &AnnoKey, value: &SaltObject, salt_type: &str) -> Result<()> {
        let anno_ns: &str = &key.ns;
        let anno_name: &str = &key.name;

        let mut label = self
            .xml
            .create_element("labels")
            .with_attribute(("xsi:type", salt_type));

        if !anno_ns.is_empty() {
            if anno_name == "SDATA" {
                label = label.with_attribute(("namespace", "saltCommon"));
            } else {
                label = label.with_attribute(("namespace", anno_ns));
            }
        }
        if anno_name.is_empty() {
            // Ignore labels that have no name
            self.progress.warn(&format!(
                "Label ({:?}={}) with empty name is ignored for file {}",
                key,
                value,
                self.output_path.to_string_lossy()
            ))?;
        } else {
            label = label.with_attribute(("name", anno_name));
            label = label.with_attribute(("value", value.marshall().as_str()));
            label.write_empty()?;
        }

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
        let annotations: Vec<_> = self
            .graph
            .get_node_annos()
            .get_annotations_for_item(&n)?
            .into_iter()
            .filter(|a| a.key.ns != "annis" || a.key.name != "tok")
            .collect();

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
        let annotations: Vec<_> = annotations
            .into_iter()
            .map(|a| (a.key, SaltObject::Text(a.val.to_string())))
            .collect();
        self.write_node(NodeType::Id(n), &sname, salt_type, &annotations, &[], layer)?;
        Ok(())
    }

    fn write_node(
        &mut self,
        n: NodeType,
        sname: &str,
        salt_type: &str,
        output_annotations: &[(AnnoKey, SaltObject)],
        output_features: &[(AnnoKey, SaltObject)],
        layer: Option<String>,
    ) -> Result<()> {
        // Remember the position of this node in the XML file
        let node_position = self.node_positions.len();
        self.node_positions.insert(n.clone(), node_position);

        let mut attributes: Vec<(String, String)> = Vec::new();
        attributes.push(("xsi:type".to_string(), salt_type.to_string()));

        // Add the layer reference to the attributes
        if let Some(layer) = layer {
            let pos = self
                .layer_positions
                .get_by_left(&layer)
                .context("Unknown position for layer")?;
            let layer_att_value = format!("//@layers.{pos}");
            attributes.push(("layers".to_string(), layer_att_value));
            self.nodes_in_layer
                .entry(layer.to_string())
                .or_default()
                .push(node_position);
        }
        let node_name = match &n {
            NodeType::Id(n) => self
                .graph
                .get_node_annos()
                .get_value_for_item(n, &NODE_NAME_KEY)?
                .context("Missing node name")?
                .to_string(),
            NodeType::Custom(node_name) => node_name.clone(),
        };
        let nodes_tag = BytesStart::new("nodes")
            .with_attributes(attributes.iter().map(|(n, v)| (n.as_str(), v.as_str())));
        self.xml.write_event(Event::Start(nodes_tag.borrow()))?;

        // Write Salt ID and SNAME
        let salt_id = format!("T::salt:/{node_name}");
        self.xml
            .create_element("labels")
            .with_attribute(("xsi:type", "saltCore:SElementId"))
            .with_attribute(("namespace", "salt"))
            .with_attribute(("name", "id"))
            .with_attribute(("value", salt_id.as_str()))
            .write_empty()?;

        // Get the last part of the URI path
        self.xml
            .create_element("labels")
            .with_attribute(("xsi:type", "saltCore:SFeature"))
            .with_attribute(("namespace", "salt"))
            .with_attribute(("name", "SNAME"))
            .with_attribute(("value", format!("T::{sname}").as_str()))
            .write_empty()?;

        // Write all other annotations as labels
        for (key, value) in output_annotations {
            if key.ns != "annis" {
                let label_type = if salt_type == "sCorpusStructure:SCorpus"
                    || salt_type == "sCorpusStructure:SDocument"
                {
                    "saltCore:SMetaAnnotation"
                } else {
                    "saltCore:SAnnotation"
                };
                self.write_label(key, value, label_type)?;
            }
        }
        for (key, value) in output_features {
            self.write_label(key, value, "saltCore:SFeature")?;
        }
        self.xml.write_event(Event::End(nodes_tag.to_end()))?;

        Ok(())
    }

    fn write_graphannis_edge(&mut self, edge: Edge, component: &AnnotationComponent) -> Result<()> {
        // Invert edge for PartOf components
        let output_edge = if component.get_type() == AnnotationComponentType::PartOf {
            edge.inverse()
        } else {
            edge.clone()
        };

        let source = NodeType::Id(output_edge.source);
        let target = NodeType::Id(output_edge.target);

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

        let output_annotations = gs.get_anno_storage().get_annotations_for_item(&edge)?;
        let output_annotations: Vec<_> = output_annotations
            .into_iter()
            .map(|a| (a.key, SaltObject::Text(a.val.to_string())))
            .collect();

        let layer = if component.layer.is_empty() {
            None
        } else {
            Some(component.layer.to_string())
        };

        self.write_edge(source, target, salt_type, &output_annotations, &[], layer)?;

        Ok(())
    }

    fn write_edge(
        &mut self,
        source: NodeType,
        target: NodeType,
        salt_type: &str,
        output_annotations: &[(AnnoKey, SaltObject)],
        output_features: &[(AnnoKey, SaltObject)],
        layer: Option<String>,
    ) -> Result<()> {
        let mut attributes = Vec::new();
        attributes.push(("xsi:type".to_string(), salt_type.to_string()));

        let source_position = self
            .node_positions
            .get(&source)
            .with_context(|| format!("Missing position for source node {:?}", source))?;

        let target_position = self
            .node_positions
            .get(&target)
            .with_context(|| format!("Missing position for target node {:?}", target))?;

        attributes.push(("source".to_string(), format!("//@nodes.{source_position}")));
        attributes.push(("target".to_string(), format!("//@nodes.{target_position}")));

        // Add the layer reference to the attributes
        if let Some(layer) = layer {
            let pos = self
                .layer_positions
                .get_by_left(&layer)
                .context("Unknown position for layer")?;
            let layer_att_value = format!("//@layers.{pos}");
            attributes.push(("layers".to_string(), layer_att_value));
            self.edges_in_layer
                .entry(layer)
                .or_default()
                .push(self.number_of_edges);
        }

        let edges_tag = BytesStart::new("edges")
            .with_attributes(attributes.iter().map(|(n, v)| (n.as_str(), v.as_str())));

        if output_annotations.is_empty() && output_features.is_empty() {
            self.xml.write_event(Event::Empty(edges_tag))?;
        } else {
            self.xml.write_event(Event::Start(edges_tag.borrow()))?;

            // add all edge labels
            for (key, value) in output_annotations {
                if key.ns != "annis" {
                    self.write_label(key, value, "saltCore:SAnnotation")?;
                }
            }
            for (key, value) in output_features {
                if key.ns != "annis" {
                    self.write_label(key, value, "saltCore:SFeature")?;
                }
            }
            self.xml.write_event(Event::End(edges_tag.to_end()))?;
        }

        self.number_of_edges += 1;

        Ok(())
    }

    fn write_all_layers(&mut self) -> Result<()> {
        // Iterate over the layers in order of their position
        for (layer, pos) in self.layer_positions.right_range(..) {
            let mut attributes = Vec::new();
            attributes.push(("xsi:type".to_string(), "saltCore:SLayer".to_string()));

            // Write nodes as attribute
            if let Some(included_positions) = self.nodes_in_layer.get(layer) {
                let att_value = position_references_to_string(included_positions, "nodes");
                attributes.push(("nodes".to_string(), att_value));
            }

            // Write edges as attributes
            if let Some(included_positions) = self.edges_in_layer.get(layer) {
                let att_value = position_references_to_string(included_positions, "edges");
                attributes.push(("edges".to_string(), att_value));
            }

            let layers_tag = BytesStart::new("layers")
                .with_attributes(attributes.iter().map(|(n, v)| (n.as_str(), v.as_str())));
            self.xml.write_event(Event::Start(layers_tag.borrow()))?;

            let marshalled_id = format!("T::l{pos}");
            self.xml
                .create_element("labels")
                .with_attribute(("xsi:type", "saltCore:SElementId"))
                .with_attribute(("namespace", "salt"))
                .with_attribute(("name", "id"))
                .with_attribute(("value", marshalled_id.as_str()))
                .write_empty()?;

            let marshalled_name = format!("T::{layer}");
            self.xml
                .create_element("labels")
                .with_attribute(("xsi:type", "saltCore:SFeature"))
                .with_attribute(("namespace", "salt"))
                .with_attribute(("name", "SNAME"))
                .with_attribute(("value", marshalled_name.as_str()))
                .write_empty()?;
            self.xml.write_event(Event::End(layers_tag.to_end()))?;
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
