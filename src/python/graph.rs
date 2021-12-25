use graphannis::update::{GraphUpdate as GraphAnnoUpdate, UpdateEvent};
use pyo3::prelude::*;

use crate::error::PepperError;

#[pyclass]
pub struct GraphUpdate {
    u: GraphAnnoUpdate,
}

#[pymethods]
impl GraphUpdate {
    #[new]
    fn new() -> Self {
        GraphUpdate {
            u: GraphAnnoUpdate::default(),
        }
    }

    #[args(node_type = "\"node\"")]
    fn add_node(&mut self, node_name: &str, node_type: &str) -> PyResult<()> {
        self.u
            .add_event(UpdateEvent::AddNode {
                node_name: node_name.to_string(),
                node_type: node_type.to_string(),
            })
            .map_err(PepperError::from)?;
        Ok(())
    }

    fn delete_node(&mut self, node_name: &str) -> PyResult<()> {
        self.u
            .add_event(UpdateEvent::DeleteNode {
                node_name: node_name.to_string(),
            })
            .map_err(PepperError::from)?;
        Ok(())
    }

    fn add_node_label(
        &mut self,
        node_name: &str,
        anno_ns: &str,
        anno_name: &str,
        anno_value: &str,
    ) -> PyResult<()> {
        self.u
            .add_event(UpdateEvent::AddNodeLabel {
                node_name: node_name.to_string(),
                anno_ns: anno_ns.to_string(),
                anno_name: anno_name.to_string(),
                anno_value: anno_value.to_string(),
            })
            .map_err(PepperError::from)?;
        Ok(())
    }

    fn delete_node_label(
        &mut self,
        node_name: &str,
        anno_ns: &str,
        anno_name: &str,
    ) -> PyResult<()> {
        self.u
            .add_event(UpdateEvent::DeleteNodeLabel {
                node_name: node_name.to_string(),
                anno_ns: anno_ns.to_string(),
                anno_name: anno_name.to_string(),
            })
            .map_err(PepperError::from)?;
        Ok(())
    }

    fn add_edge(
        &mut self,
        source_node: &str,
        target_node: &str,
        layer: &str,
        component_type: &str,
        component_name: &str,
    ) -> PyResult<()> {
        self.u
            .add_event(UpdateEvent::AddEdge {
                source_node: source_node.to_string(),
                target_node: target_node.to_string(),
                layer: layer.to_string(),
                component_type: component_type.to_string(),
                component_name: component_name.to_string(),
            })
            .map_err(PepperError::from)?;
        Ok(())
    }

    fn delete_edge(
        &mut self,
        source_node: &str,
        target_node: &str,
        layer: &str,
        component_type: &str,
        component_name: &str,
    ) -> PyResult<()> {
        self.u
            .add_event(UpdateEvent::DeleteEdge {
                source_node: source_node.to_string(),
                target_node: target_node.to_string(),
                layer: layer.to_string(),
                component_type: component_type.to_string(),
                component_name: component_name.to_string(),
            })
            .map_err(PepperError::from)?;
        Ok(())
    }

    fn add_edge_label(
        &mut self,
        source_node: &str,
        target_node: &str,
        layer: &str,
        component_type: &str,
        component_name: &str,
        anno_ns: &str,
        anno_name: &str,
        anno_value: &str,
    ) -> PyResult<()> {
        self.u
            .add_event(UpdateEvent::AddEdgeLabel {
                source_node: source_node.to_string(),
                target_node: target_node.to_string(),
                layer: layer.to_string(),
                component_type: component_type.to_string(),
                component_name: component_name.to_string(),
                anno_ns: anno_ns.to_string(),
                anno_name: anno_name.to_string(),
                anno_value: anno_value.to_string(),
            })
            .map_err(PepperError::from)?;
        Ok(())
    }

    fn delete_edge_label(
        &mut self,
        source_node: &str,
        target_node: &str,
        layer: &str,
        component_type: &str,
        component_name: &str,
        anno_ns: &str,
        anno_name: &str,
    ) -> PyResult<()> {
        self.u
            .add_event(UpdateEvent::DeleteEdgeLabel {
                source_node: source_node.to_string(),
                target_node: target_node.to_string(),
                layer: layer.to_string(),
                component_type: component_type.to_string(),
                component_name: component_name.to_string(),
                anno_ns: anno_ns.to_string(),
                anno_name: anno_name.to_string(),
            })
            .map_err(PepperError::from)?;
        Ok(())
    }
}
