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
            .map_err(|e| PepperError::from(e))?;
        Ok(())
    }
}
