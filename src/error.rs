use std::{path::PathBuf, sync::mpsc::SendError};

use graphannis::errors::GraphAnnisError;
use graphannis_core::errors::GraphAnnisCoreError;
use thiserror::Error;

use crate::workflow::StatusMessage;

pub type Result<T> = std::result::Result<T, PepperError>;

#[derive(Error, Debug)]
#[non_exhaustive]
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
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),
    #[error("No module with name {0} found")]
    NoSuchModule(String),
    #[error("Cannot read workflow file: {0}")]
    ReadWorkflowFile(String),
    #[error("Error when updating corpus graph: {0}")]
    UpdateGraph(String),
    #[error("Could not send status message: {0}")]
    SendingStatusMessageFailed(String),
    #[error("XML error: {0}")]
    XML(#[from] quick_xml::Error),
    #[error("Java virtual machine: {0}")]
    JVM(#[from] j4rs::errors::J4RsError),
    #[error("Could not iterate over directory: {0}")]
    IteratingDirectory(#[from] walkdir::Error),
    #[error(transparent)]
    Regex(#[from] regex::Error),
    #[error("Invalid (poisoned) lock")]
    LockPoisoning,
    #[error(transparent)]
    GraphAnnisCore(#[from] GraphAnnisCoreError),
    #[error(transparent)]
    GraphAnnis(#[from] GraphAnnisError),
    #[error(transparent)]
    Infallible(std::convert::Infallible),
}

impl<T> From<std::sync::PoisonError<T>> for PepperError {
    fn from(_: std::sync::PoisonError<T>) -> Self {
        PepperError::LockPoisoning
    }
}

impl From<SendError<StatusMessage>> for PepperError {
    fn from(e: SendError<StatusMessage>) -> Self {
        PepperError::SendingStatusMessageFailed(e.to_string())
    }
}
