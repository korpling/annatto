// This module provides intermediate structs for types that cannot be deserialized easily.

use graphannis::{
    graph::AnnoKey,
    model::{AnnotationComponent, AnnotationComponentType},
};
use serde::{Deserialize, Deserializer};

pub trait IntoInner {
    type I;
    fn into_inner(self) -> Self::I;
}

/// graphannis' annotation components cannot be deserialized from toml as they use the C representation.
/// Fortunately, their parts can.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeserializableComponent {
    ctype: AnnotationComponentType,
    layer: String,
    name: String,
}

impl IntoInner for DeserializableComponent {
    type I = AnnotationComponent;

    fn into_inner(self) -> Self::I {
        AnnotationComponent::new(self.ctype, self.layer.into(), self.name.into())
    }
}

pub fn deserialize_annotation_component<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<AnnotationComponent, D::Error> {
    let dc = DeserializableComponent::deserialize(deserializer)?;
    Ok(dc.into_inner())
}

pub fn deserialize_annotation_component_opt<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<AnnotationComponent>, D::Error> {
    let dc_opt = Option::<DeserializableComponent>::deserialize(deserializer)?;
    Ok(dc_opt.map(|d| d.into_inner()))
}

#[cfg(test)]
mod tests {
    use graphannis::model::AnnotationComponentType;

    use crate::deserialize::IntoInner;

    use super::DeserializableComponent;

    #[test]
    fn component() {
        let toml_str = r#"
ctype = "Pointing"
layer = "syntax"
name = "dependency"
"#;
        let r: Result<DeserializableComponent, _> = toml::from_str(toml_str);
        assert!(r.is_ok());
        let dc = r.unwrap();
        let c = dc.into_inner();
        assert!(matches!(c.get_type(), AnnotationComponentType::Pointing));
        assert_eq!(c.layer.as_str(), "syntax");
        assert_eq!(c.name.as_str(), "dependency");
    }
}
