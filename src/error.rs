use std::{path::PathBuf, sync::mpsc::SendError};

use thiserror::Error;

use crate::workflow::StatusMessage;

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
    #[error("No module with name {0} found")]
    NoSuchModule(String),
    #[error("Cannot read workflow file: {0}")]
    ReadWorkflowFile(String),
    #[error("Error when updating corpus graph: {0}")]
    UpdateGraph(String),
    #[error("Could not send status message: {0}")]
    SendingStatusMessageFailed(String),
    #[error("XML error: {0}")]
    XML(quick_xml::Error),
    #[error("Java virtual machine: {0}")]
    JVM(j4rs::errors::J4RsError),
    #[error("Could not iterate over directory: {0}")]
    IteratingDirectory(walkdir::Error),
    #[error("Regular expression error: {0}")]
    Regex(regex::Error),
    #[error("Invalid (poisoned) lock")]
    LockPoisoning,
    #[error("Unknown error {0}")]
    Unknown(String),
}

impl From<Box<dyn std::error::Error>> for PepperError {
    fn from(e: Box<dyn std::error::Error>) -> Self {
        PepperError::Unknown(e.to_string())
    }
}

impl From<SendError<StatusMessage>> for PepperError {
    fn from(e: SendError<StatusMessage>) -> Self {
        PepperError::SendingStatusMessageFailed(e.to_string())
    }
}

impl From<anyhow::Error> for PepperError {
    fn from(e: anyhow::Error) -> Self {
        PepperError::Unknown(e.to_string())
    }
}

impl From<quick_xml::Error> for PepperError {
    fn from(e: quick_xml::Error) -> Self {
        PepperError::XML(e)
    }
}

impl From<j4rs::errors::J4RsError> for PepperError {
    fn from(e: j4rs::errors::J4RsError) -> Self {
        PepperError::JVM(e)
    }
}

impl From<walkdir::Error> for PepperError {
    fn from(e: walkdir::Error) -> Self {
        PepperError::IteratingDirectory(e)
    }
}

impl From<regex::Error> for PepperError {
    fn from(e: regex::Error) -> Self {
        PepperError::Regex(e)
    }
}

impl<T> From<std::sync::PoisonError<T>> for PepperError {
    fn from(e : std::sync::PoisonError<T>) -> Self {
        PepperError::LockPoisoning
    }
}
