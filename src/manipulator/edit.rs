use graphannis::update::UpdateEvent;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct EditGraph {
    #[serde(with = "crate::estarde::update_event")]
    instructions: Vec<UpdateEvent>,
}

#[cfg(test)]
mod tests {
    use std::fs;

    use insta::assert_snapshot;

    use crate::manipulator::edit::EditGraph;

    #[test]
    fn serde() {
        let toml_str = fs::read_to_string("tests/data/graph_op/edit/config.toml");

        let m: Result<EditGraph, _> = toml::from_str(&toml_str.unwrap());
        assert!(m.is_ok(), "Deserialization error: {:?}", m.err().unwrap());
        let serialized = toml::to_string(&m.unwrap());
        assert!(
            serialized.is_ok(),
            "Serialization error: {:?}",
            serialized.err().unwrap()
        );
        assert_snapshot!(serialized.unwrap());
    }
}
