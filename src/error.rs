use std::{path::PathBuf, sync::mpsc::SendError};

use graphannis::errors::GraphAnnisError;
use graphannis_core::errors::GraphAnnisCoreError;
use pyo3::{exceptions::PyOSError, PyErr};
use thiserror::Error;

use crate::workflow::StatusMessage;

pub type Result<T> = std::result::Result<T, AnnattoError>;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum AnnattoError {
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
    #[error("XML Attribute error: {0}")]
    XMLAttr(#[from] quick_xml::events::attributes::AttrError),
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
    #[error("CSV error: {0}")]
    CSV(#[from] csv::Error),
}

impl<T> From<std::sync::PoisonError<T>> for AnnattoError {
    fn from(_: std::sync::PoisonError<T>) -> Self {
        AnnattoError::LockPoisoning
    }
}

impl From<SendError<StatusMessage>> for AnnattoError {
    fn from(e: SendError<StatusMessage>) -> Self {
        AnnattoError::SendingStatusMessageFailed(e.to_string())
    }
}

impl From<AnnattoError> for PyErr {
    fn from(e: AnnattoError) -> Self {
        PyOSError::new_err(e.to_string())
    }
}
