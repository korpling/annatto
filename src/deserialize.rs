// This module provides intermediate structs for types that cannot be deserialized easily.

use graphannis::model::{AnnotationComponent, AnnotationComponentType};
use serde::Deserialize;

pub trait AsInner {
    type I;
    fn as_inner(&self) -> Self::I;
}

/// graphannis' annotation components cannot be deserialized from toml as they use the C representation.
/// Fortunately, their parts can.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeserializableComponent {
    pub ctype: AnnotationComponentType,
    pub layer: String,
    pub name: String,
}

impl AsInner for DeserializableComponent {
    type I = AnnotationComponent;
    fn as_inner(&self) -> graphannis::graph::Component<AnnotationComponentType> {
        AnnotationComponent::new(
            self.ctype.clone(),
            self.layer.clone().into(),
            self.name.clone().into(),
        )
    }
}

impl From<AnnotationComponent> for DeserializableComponent {
    fn from(value: AnnotationComponent) -> Self {
        DeserializableComponent {
            ctype: value.get_type(),
            layer: value.layer.to_string(),
            name: value.name.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use graphannis::model::AnnotationComponentType;

    use crate::deserialize::AsInner;

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
        let c = dc.as_inner();
        assert!(matches!(c.get_type(), AnnotationComponentType::Pointing));
        assert_eq!(c.layer.as_str(), "syntax");
        assert_eq!(c.name.as_str(), "dependency");
    }
}
