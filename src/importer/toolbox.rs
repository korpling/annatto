use std::collections::BTreeMap;

use serde::Deserialize;

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ImportToolBox {
    layer_map: BTreeMap<String, Vec<String>>
}

impl Default for ImportToolBox {
    fn default() -> Self {
        let mut layer_map = BTreeMap::default();

        Self { layer_map }
    }
}