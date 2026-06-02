use std::borrow::Cow;

use graphannis::aql;
use serde::{Deserialize, Deserializer};

use crate::error::AnnattoError;

pub(crate) fn deserialize_and_check<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<String, D::Error> {
    let query = Cow::<str>::deserialize(deserializer)?;
    check_deserialized_query(&query).map_err(serde::de::Error::custom)?;
    Ok(query.to_string())
}

pub(crate) fn check_deserialized_query(query: &str) -> Result<(), AnnattoError> {
    aql::parse(query, false).map_err(|e| AnnattoError::InvalidQuery(query.to_string(), e))?;
    Ok(())
}

pub(crate) mod in_sequence {
    use super::*;

    pub(crate) fn deserialize_and_check<'de, D: Deserializer<'de>, T: FromIterator<String>>(
        deserializer: D,
    ) -> Result<T, D::Error> {
        let queries = Vec::<String>::deserialize(deserializer)?;
        queries.iter().try_for_each(|query| {
            aql::parse(query, false).map_err(serde::de::Error::custom)?;
            Ok(())
        })?;
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
