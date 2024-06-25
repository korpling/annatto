use std::fmt::Display;

use graphannis::AnnotationGraph;

use crate::{
    error::AnnattoError,
    workflow::{StatusMessage, StatusSender},
    Result,
};

pub enum EnvVars {
    InMemory,
}

impl Display for EnvVars {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            EnvVars::InMemory => "ANNATTO_IN_MEMORY",
        };
        write!(f, "{s}")
    }
}

pub fn initialize_graph(tx: &Option<StatusSender>) -> Result<AnnotationGraph> {
    let env_var = EnvVars::InMemory.to_string();
    let on_disk = match std::env::var(env_var) {
        Ok(value) => match value.parse::<bool>() {
            Ok(v) => !v,
            Err(_) => on_error_default_storage(tx)?,
        },
        Err(_) => on_error_default_storage(&None)?, // silent, because not setting the env var is not a user error
    };
    let g = AnnotationGraph::with_default_graphstorages(on_disk)
        .map_err(|e| AnnattoError::CreateGraph(e.to_string()))?;
    Ok(g)
}

fn on_error_default_storage(tx: &Option<StatusSender>) -> Result<bool> {
    if let Some(sender) = &tx {
        sender.send(StatusMessage::Warning(format!(
            "Could not read value of environment variable {}, working on disk.",
            EnvVars::InMemory
        )))?;
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::initialize_graph;
    use crate::runtime::EnvVars;

    #[test]
    fn test_storage_mode_unset() {
        // todo once graphannis exposes whether or not it's storage is disk based, let's test that here
        let tx = None;
        let g = initialize_graph(&tx);
        assert!(g.is_ok());
    }

    #[test]
    fn test_storage_mode_set_correctly() {
        // todo once graphannis exposes whether or not it's storage is disk based, let's test that here
        std::env::set_var(EnvVars::InMemory.to_string(), "true".to_string());
        let tx = None;
        let g = initialize_graph(&tx);
        assert!(g.is_ok());
    }

    #[test]
    fn test_storage_mode_set_incorrectly() {
        // todo once graphannis exposes whether or not it's storage is disk based, let's test that here
        std::env::set_var(EnvVars::InMemory.to_string(), "ehm".to_string());
        let tx = None;
        let g = initialize_graph(&tx);
        assert!(g.is_ok());
    }
}
