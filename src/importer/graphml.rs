use graphannis_core::{graph::NODE_TYPE_KEY, util::split_qname};
use quick_xml::{
    events::{attributes::Attributes, Event},
    Reader,
};
use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::BufReader,
    path::Path,
    str::FromStr,
};

use graphannis::{
    graph::AnnoKey,
    model::AnnotationComponent,
    update::{GraphUpdate, UpdateEvent},
};

use crate::{
    error::AnnattoError, importer::Importer, progress::ProgressReporter, workflow::StatusSender,
    Module,
};

pub struct GraphMLImporter {}

fn add_node(
    node_updates: &mut GraphUpdate,
    current_node_id: &Option<String>,
    data: &mut HashMap<AnnoKey, String>,
) -> Result<(), AnnattoError> {
    if let Some(node_name) = current_node_id {
        // Insert graph update for node
        let node_type = data
            .remove(&NODE_TYPE_KEY)
            .unwrap_or_else(|| "node".to_string());
        node_updates.add_event(UpdateEvent::AddNode {
            node_name: node_name.clone(),
            node_type,
        })?;
        // Add all remaining data entries as annotations
        for (key, value) in data.drain() {
            node_updates.add_event(UpdateEvent::AddNodeLabel {
                node_name: node_name.clone(),
                anno_ns: key.ns.to_string(),
                anno_name: key.name.to_string(),
                anno_value: value,
            })?;
        }
    }
    Ok(())
}

fn add_edge(
    edge_updates: &mut GraphUpdate,
    current_source_id: &Option<String>,
    current_target_id: &Option<String>,
    current_component: &Option<String>,
    data: &mut HashMap<AnnoKey, String>,
) -> Result<(), AnnattoError> {
    if let (Some(source), Some(target), Some(component)) =
        (current_source_id, current_target_id, current_component)
    {
        // Insert graph update for this edge
        if let Ok(component) = AnnotationComponent::from_str(component) {
            edge_updates.add_event(UpdateEvent::AddEdge {
                source_node: source.clone(),
                target_node: target.clone(),
                layer: component.layer.to_string(),
                component_type: component.get_type().to_string(),
                component_name: component.name.to_string(),
            })?;

            // Add all remaining data entries as annotations
            for (key, value) in data.drain() {
                edge_updates.add_event(UpdateEvent::AddEdgeLabel {
                    source_node: source.clone(),
                    target_node: target.clone(),
                    layer: component.layer.to_string(),
                    component_type: component.get_type().to_string(),
                    component_name: component.name.to_string(),
                    anno_ns: key.ns.to_string(),
                    anno_name: key.name.to_string(),
                    anno_value: value,
                })?;
            }
        }
    }
    Ok(())
}

fn add_annotation_key(
    keys: &mut BTreeMap<String, AnnoKey>,
    attributes: Attributes,
) -> Result<(), AnnattoError> {
    // resolve the ID to the fully qualified annotation name
    let mut id: Option<String> = None;
    let mut anno_key: Option<AnnoKey> = None;

    for att in attributes {
        let att = att?;

        let att_value = String::from_utf8_lossy(&att.value);

        match att.key {
            b"id" => {
                id = Some(att_value.to_string());
            }
            b"attr.name" => {
                let (ns, name) = split_qname(att_value.as_ref());
                anno_key = Some(AnnoKey {
                    ns: ns.unwrap_or_default().into(),
                    name: name.into(),
                });
            }
            _ => {}
        }
    }

    if let (Some(id), Some(anno_key)) = (id, anno_key) {
        keys.insert(id, anno_key);
    }
    Ok(())
}

fn read_graphml<R: std::io::BufRead>(
    input: &mut R,
    node_updates: &mut GraphUpdate,
    edge_updates: &mut GraphUpdate,
) -> Result<Option<String>, AnnattoError> {
    let mut reader = Reader::from_reader(input);
    reader.expand_empty_elements(true);

    let mut buf = Vec::new();

    let mut keys = BTreeMap::new();

    let mut level = 0;
    let mut in_graph = false;
    let mut current_node_id: Option<String> = None;
    let mut current_data_key: Option<String> = None;
    let mut current_source_id: Option<String> = None;
    let mut current_target_id: Option<String> = None;
    let mut current_component: Option<String> = None;
    let mut data: HashMap<AnnoKey, String> = HashMap::new();

    let mut config = None;

    loop {
        match reader.read_event(&mut buf)? {
            Event::Start(ref e) => {
                level += 1;

                match e.name() {
                    b"graph" => {
                        if level == 2 {
                            in_graph = true;
                        }
                    }
                    b"key" => {
                        if level == 2 {
                            add_annotation_key(&mut keys, e.attributes())?;
                        }
                    }
                    b"node" => {
                        if in_graph && level == 3 {
                            // Get the ID of this node
                            for att in e.attributes() {
                                let att = att?;
                                if att.key == b"id" {
                                    current_node_id =
                                        Some(String::from_utf8_lossy(&att.value).to_string());
                                }
                            }
                        }
                    }
                    b"edge" => {
                        if in_graph && level == 3 {
                            // Get the source and target node IDs
                            for att in e.attributes() {
                                let att = att?;
                                if att.key == b"source" {
                                    current_source_id =
                                        Some(String::from_utf8_lossy(&att.value).to_string());
                                } else if att.key == b"target" {
                                    current_target_id =
                                        Some(String::from_utf8_lossy(&att.value).to_string());
                                } else if att.key == b"label" {
                                    current_component =
                                        Some(String::from_utf8_lossy(&att.value).to_string());
                                }
                            }
                        }
                    }
                    b"data" => {
                        for att in e.attributes() {
                            let att = att?;
                            if att.key == b"key" {
                                current_data_key =
                                    Some(String::from_utf8_lossy(&att.value).to_string());
                            }
                        }
                    }
                    _ => {}
                }
            }
            Event::Text(t) => {
                if let Some(current_data_key) = &current_data_key {
                    if in_graph && level == 4 {
                        if let Some(anno_key) = keys.get(current_data_key) {
                            // Copy all data attributes into our own map
                            data.insert(anno_key.clone(), t.unescape_and_decode(&reader)?);
                        }
                    }
                }
            }
            Event::CData(t) => {
                if let Some(current_data_key) = &current_data_key {
                    if in_graph && level == 3 && current_data_key == "k0" {
                        // This is the configuration content
                        config = Some(String::from_utf8_lossy(&t).to_string());
                    }
                }
            }
            Event::End(ref e) => {
                match e.name() {
                    b"graph" => {
                        in_graph = false;
                    }
                    b"node" => {
                        add_node(node_updates, &current_node_id, &mut data)?;
                        current_node_id = None;
                    }
                    b"edge" => {
                        add_edge(
                            edge_updates,
                            &current_source_id,
                            &current_target_id,
                            &current_component,
                            &mut data,
                        )?;

                        current_source_id = None;
                        current_target_id = None;
                        current_component = None;
                    }
                    b"data" => {
                        current_data_key = None;
                    }
                    _ => {}
                }

                level -= 1;
            }
            Event::Eof => {
                break;
            }
            _ => {}
        }
    }
    Ok(config)
}

impl Default for GraphMLImporter {
    fn default() -> Self {
        GraphMLImporter {}
    }
}

impl Importer for GraphMLImporter {
    fn import_corpus(
        &self,
        path: &Path,
        _properties: &BTreeMap<String, String>,
        tx: Option<StatusSender>,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>> {
        let reporter = ProgressReporter::new(tx, self as &dyn Module, Some(path), 2)?;

        // TODO: support multiple GraphML and connected binary files
        // TODO: refactor the graphannis_core create to expose the needed functionality directly

        // Load the GraphML files (could be a ZIP file, too) from the given location
        let input = File::open(path)?;
        let mut input = BufReader::new(input);
        let mut updates = GraphUpdate::default();
        let mut edge_updates = GraphUpdate::default();
        read_graphml(&mut input, &mut updates, &mut edge_updates)?;
        reporter.worked(1)?;
        // Append all edges updates after the node updates:
        // edges would not be added if the nodes they are referring do not exist
        for u in edge_updates.iter()? {
            let (_, event) = u?;
            updates.add_event(event)?;
        }
        reporter.worked(1)?;

        Ok(updates)
    }
}

impl Module for GraphMLImporter {
    fn module_name(&self) -> &str {
        "GraphMLImporter"
    }
}
