use std::str::FromStr;

use graphannis::{
    graph::AnnoKey,
    model::{AnnotationComponent, AnnotationComponentType},
    update::UpdateEvent,
};
use itertools::Itertools;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::estarde::IntoInner;

impl TryFrom<&UpdateEvent> for SerdeUE {
    type Error = anyhow::Error;

    fn try_from(value: &UpdateEvent) -> Result<Self, Self::Error> {
        let sue = match value {
            UpdateEvent::AddNode {
                node_name,
                node_type,
            } => SerdeUE::Add(RawAdd::Node {
                nodes: vec![node_name.to_string()],
                node_type: node_type.to_string(),
            }),
            UpdateEvent::DeleteNode { node_name } => SerdeUE::RM(RawRemove::Nodes {
                nodes: vec![node_name.to_string()],
            }),
            UpdateEvent::AddNodeLabel {
                node_name,
                anno_ns,
                anno_name,
                anno_value,
            } => SerdeUE::Add(RawAdd::NodeLabels {
                nodes: vec![node_name.to_string()],
                anno: AnnoKey {
                    name: anno_name.to_string(),
                    ns: anno_ns.to_string(),
                },
                value: anno_value.to_string(),
            }),
            UpdateEvent::DeleteNodeLabel {
                node_name,
                anno_ns,
                anno_name,
            } => SerdeUE::RM(RawRemove::NodeLabels {
                nodes: vec![node_name.to_string()],
                annos: vec![AnnoKey {
                    ns: anno_ns.to_string(),
                    name: anno_name.to_string(),
                }],
            }),
            UpdateEvent::AddEdge {
                source_node,
                target_node,
                layer,
                component_type,
                component_name,
            } => SerdeUE::Add(RawAdd::Edges {
                edges: vec![EdgeConfig {
                    source: source_node.to_string(),
                    target: target_node.to_string(),
                }],
                component: AnnotationComponent::new(
                    AnnotationComponentType::from_str(component_type)?,
                    layer.to_string(),
                    component_name.to_string(),
                ),
            }),
            UpdateEvent::DeleteEdge {
                source_node,
                target_node,
                layer,
                component_type,
                component_name,
            } => SerdeUE::RM(RawRemove::Edges {
                edges: vec![EdgeConfig {
                    source: source_node.to_string(),
                    target: target_node.to_string(),
                }],
                component: AnnotationComponent::new(
                    AnnotationComponentType::from_str(component_type)?,
                    layer.to_string(),
                    component_name.to_string(),
                ),
            }),
            UpdateEvent::AddEdgeLabel {
                source_node,
                target_node,
                layer,
                component_type,
                component_name,
                anno_ns,
                anno_name,
                anno_value,
            } => SerdeUE::Add(RawAdd::EdgeLabels {
                edges: vec![EdgeConfig {
                    source: source_node.to_string(),
                    target: target_node.to_string(),
                }],
                component: AnnotationComponent::new(
                    AnnotationComponentType::from_str(component_type)?,
                    layer.to_string(),
                    component_name.to_string(),
                ),
                anno: AnnoKey {
                    name: anno_name.to_string(),
                    ns: anno_ns.to_string(),
                },
                value: anno_value.to_string(),
            }),
            UpdateEvent::DeleteEdgeLabel {
                source_node,
                target_node,
                layer,
                component_type,
                component_name,
                anno_ns,
                anno_name,
            } => SerdeUE::RM(RawRemove::EdgeLabels {
                edges: vec![EdgeConfig {
                    source: source_node.to_string(),
                    target: target_node.to_string(),
                }],
                component: AnnotationComponent::new(
                    AnnotationComponentType::from_str(component_type)?,
                    layer.to_string(),
                    component_name.to_string(),
                ),
                annos: vec![AnnoKey {
                    ns: anno_ns.to_string(),
                    name: anno_name.to_string(),
                }],
            }),
        };
        Ok(sue)
    }
}

impl IntoInner for SerdeUE {
    type I = Vec<UpdateEvent>;

    fn into_inner(self) -> Self::I {
        match self {
            SerdeUE::Add(raw_add) => raw_add.into_inner(),
            SerdeUE::RM(raw_remove) => raw_remove.into_inner(),
        }
    }
}

impl IntoInner for RawAdd {
    type I = Vec<UpdateEvent>;

    fn into_inner(self) -> Self::I {
        match self {
            RawAdd::Node {
                nodes: names,
                node_type,
            } => names
                .into_iter()
                .map(|name| UpdateEvent::AddNode {
                    node_name: name,
                    node_type: node_type.clone(),
                })
                .collect_vec(),
            RawAdd::NodeLabels {
                nodes: names,
                anno,
                value,
            } => names
                .into_iter()
                .map(|name| UpdateEvent::AddNodeLabel {
                    node_name: name,
                    anno_ns: anno.ns.to_string(),
                    anno_name: anno.name.to_string(),
                    anno_value: value.to_string(),
                })
                .collect_vec(),
            RawAdd::Edges { edges, component } => edges
                .into_iter()
                .map(|edge| UpdateEvent::AddEdge {
                    source_node: edge.source,
                    target_node: edge.target,
                    layer: component.layer.to_string(),
                    component_type: component.get_type().to_string(),
                    component_name: component.name.to_string(),
                })
                .collect_vec(),
            RawAdd::EdgeLabels {
                edges,
                component,
                anno,
                value,
            } => edges
                .into_iter()
                .map(|edge| UpdateEvent::AddEdgeLabel {
                    source_node: edge.source,
                    target_node: edge.target,
                    layer: component.layer.to_string(),
                    component_type: component.get_type().to_string(),
                    component_name: component.name.to_string(),
                    anno_ns: anno.ns.to_string(),
                    anno_name: anno.name.to_string(),
                    anno_value: value.to_string(),
                })
                .collect_vec(),
        }
    }
}

impl IntoInner for RawRemove {
    type I = Vec<UpdateEvent>;

    fn into_inner(self) -> Self::I {
        match self {
            RawRemove::Nodes { nodes } => nodes
                .into_iter()
                .map(|node| UpdateEvent::DeleteNode { node_name: node })
                .collect_vec(),
            RawRemove::NodeLabels { nodes, annos } => nodes
                .into_iter()
                .cartesian_product(annos)
                .map(|(node, anno)| UpdateEvent::DeleteNodeLabel {
                    node_name: node,
                    anno_ns: anno.ns.to_string(),
                    anno_name: anno.name.to_string(),
                })
                .collect_vec(),
            RawRemove::Edges { edges, component } => edges
                .into_iter()
                .map(|edge| UpdateEvent::DeleteEdge {
                    source_node: edge.source,
                    target_node: edge.target,
                    layer: component.layer.to_string(),
                    component_type: component.get_type().to_string(),
                    component_name: component.name.to_string(),
                })
                .collect_vec(),
            RawRemove::EdgeLabels {
                edges,
                component,
                annos,
            } => edges
                .into_iter()
                .cartesian_product(annos)
                .map(|(edge, anno)| UpdateEvent::DeleteEdgeLabel {
                    source_node: edge.source,
                    target_node: edge.target,
                    layer: component.layer.to_string(),
                    component_type: component.get_type().to_string(),
                    component_name: component.name.to_string(),
                    anno_ns: anno.ns.to_string(),
                    anno_name: anno.name.to_string(),
                })
                .collect_vec(),
        }
    }
}

pub fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<UpdateEvent>, D::Error> {
    let raw = Vec::<SerdeUE>::deserialize(deserializer)?;
    Ok(raw
        .into_iter()
        .flat_map(|entry| entry.into_inner())
        .collect_vec())
}

pub fn serialize<'a, S: Serializer, T: IntoIterator<Item = &'a UpdateEvent>>(
    value: T,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let anno_component_vec = value.into_iter().flat_map(SerdeUE::try_from).collect_vec();
    anno_component_vec.serialize(serializer)
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "lowercase", tag = "do", deny_unknown_fields)]
enum SerdeUE {
    Add(RawAdd),
    RM(RawRemove),
}

#[derive(Deserialize, Serialize)]
#[serde(untagged, deny_unknown_fields)]
enum RawAdd {
    Node {
        nodes: Vec<String>,
        #[serde(alias = "type", default = "default_node_type")]
        node_type: String,
    },
    NodeLabels {
        nodes: Vec<String>,
        #[serde(with = "crate::estarde::anno_key")]
        anno: AnnoKey,
        value: String,
    },
    Edges {
        edges: Vec<EdgeConfig>,
        #[serde(with = "crate::estarde::annotation_component")]
        component: AnnotationComponent,
    },
    EdgeLabels {
        edges: Vec<EdgeConfig>,
        #[serde(with = "crate::estarde::annotation_component")]
        component: AnnotationComponent,
        #[serde(with = "crate::estarde::anno_key")]
        anno: AnnoKey,
        value: String,
    },
}

fn default_node_type() -> String {
    "node".to_string()
}

#[derive(Deserialize, Serialize)]
#[serde(untagged, deny_unknown_fields)]
enum RawRemove {
    Nodes {
        nodes: Vec<String>,
    },
    NodeLabels {
        nodes: Vec<String>,
        #[serde(with = "crate::estarde::anno_key::in_sequence")]
        annos: Vec<AnnoKey>,
    },
    Edges {
        edges: Vec<EdgeConfig>,
        #[serde(with = "crate::estarde::annotation_component")]
        component: AnnotationComponent,
    },
    EdgeLabels {
        edges: Vec<EdgeConfig>,
        #[serde(with = "crate::estarde::annotation_component")]
        component: AnnotationComponent,
        #[serde(with = "crate::estarde::anno_key::in_sequence")]
        annos: Vec<AnnoKey>,
    },
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct EdgeConfig {
    source: String,
    target: String,
}
