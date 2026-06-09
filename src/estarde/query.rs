use std::borrow::Cow;

use graphannis::{AnnotationGraph, aql};
use graphannis_core::errors::GraphAnnisCoreError;
use lazy_static::lazy_static;
use serde::{Deserialize, Deserializer};

use crate::error::AnnattoError;

lazy_static! {
    static ref empty_graph: Result<AnnotationGraph, GraphAnnisCoreError> =
        AnnotationGraph::new(false);
}

pub(crate) fn deserialize_and_check<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<String, D::Error> {
    let query = Cow::<str>::deserialize(deserializer)?;
    check_deserialized_query(&query).map_err(serde::de::Error::custom)?;
    Ok(query.to_string())
}

pub(crate) fn check_deserialized_query(query: &str) -> Result<(), AnnattoError> {
    // checks syntax
    let dj = aql::parse(query, false).map_err(AnnattoError::invalid_query)?;
    // checks semantics
    if let Ok(graph) = &*empty_graph {
        aql::execute_query_on_graph(graph, &dj, true, None)
            .map_err(AnnattoError::invalid_query)?
            .next();
    }
    Ok(())
}

pub(crate) mod in_sequence {
    use super::*;

    pub(crate) fn deserialize_and_check<'de, D: Deserializer<'de>, T: FromIterator<String>>(
        deserializer: D,
    ) -> Result<T, D::Error> {
        let queries = Vec::<String>::deserialize(deserializer)?;
        queries
            .iter()
            .enumerate()
            .try_for_each(|(i, query)| {
                check_deserialized_query(query).map_err(|e| match e {
                    AnnattoError::InvalidQuery { error, .. } => AnnattoError::InvalidQuery {
                        index: Some(i as u16 + 1u16),
                        error,
                    },
                    _ => e,
                })
            })
            .map_err(serde::de::Error::custom)?;
        Ok(queries.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use insta::assert_snapshot;

    use super::*;

    #[derive(Deserialize)]
    struct QueryHolder {
        #[serde(deserialize_with = "crate::estarde::query::deserialize_and_check")]
        query: String,
    }

    #[test]
    fn fails() {
        let probe: Result<QueryHolder, _> = toml::from_str("query = 'annis:tok'");
        assert!(probe.is_err());
        assert_snapshot!(probe.err().unwrap());
    }

    #[test]
    fn passes() {
        let probe: Result<QueryHolder, _> = toml::from_str("query = 'tok'");
        assert!(probe.is_ok());
        assert_snapshot!(probe.unwrap().query)
    }
}
