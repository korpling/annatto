use graphannis::model::{AnnotationComponent, AnnotationComponentType};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::IntoInner;

/// graphannis' annotation components cannot be deserialized from toml as they use the C representation.
/// Fortunately, their parts can.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SerdeComponent {
    #[serde(alias = "type")]
    ctype: AnnotationComponentType,
    layer: String,
    name: String,
}

impl IntoInner for SerdeComponent {
    type I = AnnotationComponent;

    fn into_inner(self) -> Self::I {
        AnnotationComponent::new(self.ctype, self.layer, self.name)
    }
}

impl From<&AnnotationComponent> for SerdeComponent {
    fn from(value: &AnnotationComponent) -> Self {
        SerdeComponent {
            ctype: value.get_type(),
            layer: value.layer.to_string(),
            name: value.name.to_string(),
        }
    }
}

pub fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<AnnotationComponent, D::Error> {
    let dc = SerdeComponent::deserialize(deserializer)?;
    Ok(dc.into_inner())
}

pub fn serialize<S: Serializer>(
    value: &AnnotationComponent,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let serializable = SerdeComponent::from(value);
    serializable.serialize(serializer)
}

pub(crate) mod as_option {
    use graphannis::model::AnnotationComponent;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::estarde::IntoInner;

    use super::SerdeComponent;

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<AnnotationComponent>, D::Error> {
        let dc_opt = Option::<SerdeComponent>::deserialize(deserializer)?;
        Ok(dc_opt.map(|d| d.into_inner()))
    }

    pub fn serialize<S: Serializer>(
        value: &Option<AnnotationComponent>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        if let Some(component) = value {
            let serializable = SerdeComponent::from(component);
            serializable.serialize(serializer)
        } else {
            serializer.serialize_none()
        }
    }
}

pub(crate) mod in_sequence {
    use std::iter::FromIterator;

    use graphannis::model::AnnotationComponent;
    use itertools::Itertools;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::estarde::IntoInner;

    use super::SerdeComponent;

    pub fn deserialize<'de, D: Deserializer<'de>, T: FromIterator<AnnotationComponent>>(
        deserializer: D,
    ) -> Result<T, D::Error> {
        let component_seq = Vec::<SerdeComponent>::deserialize(deserializer)?;
        Ok(component_seq
            .into_iter()
            .map(|dc| dc.into_inner())
            .collect::<T>())
    }

    pub fn serialize<'a, S: Serializer, T: IntoIterator<Item = &'a AnnotationComponent>>(
        value: T,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let anno_component_vec = value.into_iter().map(SerdeComponent::from).collect_vec();
        anno_component_vec.serialize(serializer)
    }
}
