use std::{io::BufWriter, path::PathBuf, string::FromUtf8Error, sync::mpsc::SendError};

use graphannis::errors::GraphAnnisError;
use graphannis_core::errors::GraphAnnisCoreError;
use itertools::Itertools;
use thiserror::Error;

use crate::workflow::StatusMessage;

pub type Result<T> = std::result::Result<T, AnnattoError>;
pub type StandardErrorResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum AnnattoError {
    #[error("Conversion failed with errors: {}", errors.iter().map(|e| e.to_string()).join("\n"))]
    ConversionFailed { errors: Vec<AnnattoError> },
    #[error("Error during exporting corpus to {path} with {exporter}: {reason}")]
    Export {
        reason: String,
        exporter: String,
        path: PathBuf,
    },
    #[error("Error during importing corpus from {path} with {importer}: {reason}")]
    Import {
        reason: String,
        importer: String,
        path: PathBuf,
    },
    #[error("Error when manipulating corpus with {manipulator}: {reason}")]
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
    #[error("Checks failed:\n{}", failed_checks.iter().join("\n"))]
    ChecksFailed { failed_checks: Vec<String> },
    #[error("Time for end of the token ({end}) is larger than for the start ({start})")]
    EndTokenTimeLargerThanStart { start: f64, end: f64 },
    #[error("Invalid Property value: {property}={value}")]
    InvalidPropertyValue { property: String, value: String },
    #[error(transparent)]
    ConvertBufWriterAsByteVector(#[from] std::io::IntoInnerError<BufWriter<Vec<u8>>>),
    #[error(transparent)]
    InvalidUtf8(#[from] FromUtf8Error),
    #[error("Could not parse TOML workflow file: {error}")]
    TOMLError { error: String },
    #[error("Could not read XSLS file: {0}")]
    XlsxRead(#[from] umya_spreadsheet::XlsxError),
    #[error("Error waiting for finished conversion thread")]
    JoinHandle,
    #[error("Glob pattern caused an error: {0}")]
    GlobError(String),
    #[error(transparent)]
    Reflection(#[from] facet_reflect::ReflectError),
    #[error(transparent)]
    FacetVariantError(#[from] facet_reflect::VariantError),
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
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

impl From<toml::de::Error> for AnnattoError {
    fn from(value: toml::de::Error) -> Self {
        AnnattoError::TOMLError {
            error: value.to_string(),
        }
    }
}

impl From<glob::GlobError> for AnnattoError {
    fn from(value: glob::GlobError) -> Self {
        AnnattoError::GlobError(value.to_string())
    }
}

impl From<glob::PatternError> for AnnattoError {
    fn from(value: glob::PatternError) -> Self {
        AnnattoError::GlobError(value.msg.to_string())
    }
}
