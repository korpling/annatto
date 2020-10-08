use std::path::PathBuf;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, PepperError>;

#[derive(Error, Debug)]
pub enum PepperError {
    #[error("Error during exporting corpus from {path} with {exporter:?}: {reason:?}")]
    Export {
        reason: Box<dyn std::error::Error>,
        exporter: String,
        path: PathBuf,
    },
    #[error("Error during importing corpus to {path} with {importer:?}: {reason:?}")]
    Import {
        reason: Box<dyn std::error::Error>,
        importer: String,
        path: PathBuf,
    },
    #[error("Error when manipulating corpus with {manipulator:?}: {reason:?}")]
    Manipulator {
        reason: Box<dyn std::error::Error>,
        manipulator: String,
    },
    #[error("Cannot create new graph object: {0}")]
    CreateGraph(Box<dyn std::error::Error>),
    #[error("Cannot open workflow file {file}: {reason}")]
    OpenWorkflowFile {
        file: PathBuf,
        reason: std::io::Error,
    },
}
