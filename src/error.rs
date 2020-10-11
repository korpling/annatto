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
    #[error("Error when updating corpus graph: {0}")]
    UpdateGraph(String),
    #[error("Could not send status message: {0}")]
    SendingStatusMessageFailed(String),
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
