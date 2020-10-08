use std::path::PathBuf;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, PepperError>;

#[derive(Error, Debug)]
pub enum PepperError {
    #[error("Error during exporting corpus from {path} with {exporter:?}: {reason:?}")]
    Export {
        reason: String,
        exporter: String,
        path: PathBuf,
    },
    #[error("Error during importing corpus to {path} with {importer:?}: {reason:?}")]
    Import {
        reason: String,
        importer: String,
        path: PathBuf,
    },
    #[error("Error when manipulating corpus with {manipulator:?}: {reason:?}")]
    Manipulator { reason: String, manipulator: String },
    #[error("Cannot create new graph object: {0}")]
    CreateGraph(String),
    #[error("Cannot open workflow file {file}: {reason}")]
    OpenWorkflowFile {
        file: PathBuf,
        reason: std::io::Error,
    },
    #[error("Error when updating corpus graph: {0}")]
    UpdateGraph(String),
    #[error("Unknown error {0}")]
    Unknown(String),
}

impl Into<PepperError> for Box<dyn std::error::Error> {
    fn into(self) -> PepperError {
        PepperError::Unknown(self.to_string())
    }
}
