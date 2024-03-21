//! # Introduction to using annatto
//!
//! ## Command line
//!
//! The main usage of annatto is through the command line interface. Run
//! ```bash
//! annatto --help
//! ```
//!
//! to get more help on the sub-commands.
//! The most important command is `annatto run <workflow-file>`, which runs all the modules as defined in the given [workflow] file.
//!
//! ## Modules
//!
//! Annatto comes with a number of modules, which have different types:
//!
//! [**Importer**](importer) modules allow importing files from different formats.
//! More than one importer can be used in a workflow, but then the corpus data needs
//! to be merged using one of the merger manipulators.
//! When running a workflow, the importers are executed first and in parallel.
//!   
//!
//!
//! [**Graph operation**](manipulator) modules change the imported corpus data.
//! They are executed one after another (non-parallel) and in the order they have been defined in the workflow.
//!
//! [**Exporter**](exporter) modules export the data into different formats.
//! More than one exporter can be used in a workflow.
//! When running a workflow, the exporters are executed last and in parallel.
//!

pub mod error;
pub mod exporter;
pub mod importer;
pub mod manipulator;
pub mod models;
pub mod progress;
pub mod runtime;
#[cfg(test)]
pub(crate) mod test_util;
pub(crate) mod util;
pub mod workflow;

use std::{fmt::Display, path::PathBuf};

use error::Result;
use exporter::{exmaralda::ExportExmaralda, graphml::ExportGraphML, xlsx::XlsxExporter, Exporter};
use importer::{
    conllu::ImportCoNLLU, exmaralda::ImportEXMARaLDA, file_nodes::CreateFileNodes,
    graphml::GraphMLImporter, meta::AnnotateCorpus, none::CreateEmptyCorpus, opus::ImportOpusLinks,
    ptb::ImportPTB, textgrid::ImportTextgrid, treetagger::ImportTreeTagger,
    xlsx::ImportSpreadsheet, xml::ImportXML, Importer,
};
use manipulator::{
    check::Check, chunker::Chunk, collapse::Collapse, enumerate::EnumerateMatches, link::LinkNodes,
    map::MapAnnos, merge::Merge, no_op::NoOp, re::Revise, Manipulator,
};
use serde_derive::Deserialize;
use strum::{AsRefStr, EnumDiscriminants, EnumIter};

#[derive(Deserialize, EnumDiscriminants, AsRefStr)]
#[strum_discriminants(derive(EnumIter, AsRefStr))]
#[serde(tag = "format", rename_all = "lowercase", content = "config")]
pub enum WriteAs {
    GraphML(#[serde(default)] ExportGraphML), // the purpose of serde(default) here is, that an empty `[export.config]` table can be omited
    EXMARaLDA(#[serde(default)] ExportExmaralda),
    Xlsx(#[serde(default)] XlsxExporter),
}

impl Default for WriteAs {
    // the purpose of this default is to allow to omit `format` in an `[[export]]` table
    fn default() -> Self {
        WriteAs::GraphML(ExportGraphML::default())
    }
}

impl WriteAs {
    fn writer(&self) -> &dyn Exporter {
        match self {
            WriteAs::GraphML(m) => m,
            WriteAs::EXMARaLDA(m) => m,
            WriteAs::Xlsx(m) => m,
        }
    }
}

#[derive(Deserialize, EnumDiscriminants, AsRefStr)]
#[strum_discriminants(derive(EnumIter, AsRefStr))]
#[serde(tag = "format", rename_all = "lowercase", content = "config")]
pub enum ReadFrom {
    CoNLLU(#[serde(default)] ImportCoNLLU),
    EXMARaLDA(#[serde(default)] ImportEXMARaLDA),
    GraphML(#[serde(default)] GraphMLImporter),
    Meta(#[serde(default)] AnnotateCorpus),
    None(#[serde(default)] CreateEmptyCorpus),
    Opus(#[serde(default)] ImportOpusLinks),
    Path(#[serde(default)] CreateFileNodes),
    PTB(#[serde(default)] ImportPTB),
    TextGrid(#[serde(default)] ImportTextgrid),
    TreeTagger(#[serde(default)] ImportTreeTagger),
    Xlsx(#[serde(default)] ImportSpreadsheet),
    Xml(ImportXML),
}

impl Default for ReadFrom {
    // the purpose of this default is to allow to omit `format` in an `[[import]]` table
    fn default() -> Self {
        ReadFrom::None(CreateEmptyCorpus::default())
    }
}

impl ReadFrom {
    fn reader(&self) -> &dyn Importer {
        match self {
            ReadFrom::CoNLLU(m) => m,
            ReadFrom::EXMARaLDA(m) => m,
            ReadFrom::PTB(m) => m,
            ReadFrom::TextGrid(m) => m,
            ReadFrom::TreeTagger(m) => m,
            ReadFrom::None(m) => m,
            ReadFrom::Meta(m) => m,
            ReadFrom::Xlsx(m) => m,
            ReadFrom::GraphML(m) => m,
            ReadFrom::Path(m) => m,
            ReadFrom::Xml(m) => m,
            ReadFrom::Opus(m) => m,
        }
    }
}

#[derive(Deserialize, EnumDiscriminants, AsRefStr)]
#[strum_discriminants(derive(EnumIter, AsRefStr))]
#[serde(tag = "action", rename_all = "lowercase", content = "config")]
pub enum GraphOp {
    Check(Check),       // no default, has a (required) path attribute
    Collapse(Collapse), // no default, there is no such thing as a default component
    Enumerate(#[serde(default)] EnumerateMatches),
    Link(LinkNodes),                  // no default, has required attributes
    Map(MapAnnos),                    // no default, has a (required) path attribute
    Merge(Merge),                     // no default, has required attributes
    Revise(#[serde(default)] Revise), // does nothing on default
    Chunk(#[serde(default)] Chunk),
    None(#[serde(default)] NoOp), // has no attributes
}

impl Default for GraphOp {
    // the purpose of this default is to allow to omit `format` in an `[[graph_op]]` table
    fn default() -> Self {
        GraphOp::None(NoOp::default())
    }
}

impl GraphOp {
    fn processor(&self) -> &dyn Manipulator {
        match self {
            GraphOp::Check(m) => m,
            GraphOp::Collapse(m) => m,
            GraphOp::Link(m) => m,
            GraphOp::Map(m) => m,
            GraphOp::Merge(m) => m,
            GraphOp::Revise(m) => m,
            GraphOp::None(m) => m,
            GraphOp::Enumerate(m) => m,
            GraphOp::Chunk(m) => m,
        }
    }
}

/// Unique ID of a single step in the conversion pipeline.
#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct StepID {
    /// The name of the module used in this step.
    pub module_name: String,
    /// The path (input or output) used in this step.
    pub path: Option<PathBuf>,
}

impl StepID {
    pub fn from_importer_module(m: &ReadFrom, path: Option<PathBuf>) -> StepID {
        StepID {
            module_name: format!("import_{}", m.as_ref().to_lowercase()),
            path,
        }
    }

    pub fn from_graph_op_module(m: &GraphOp) -> StepID {
        StepID {
            module_name: m.as_ref().to_lowercase(),
            path: None,
        }
    }

    pub fn from_exporter_module(m: &WriteAs, path: Option<PathBuf>) -> StepID {
        StepID {
            module_name: format!("export_{}", m.as_ref().to_lowercase()),
            path,
        }
    }
}

impl Display for StepID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(path) = &self.path {
            write!(f, "{} ({})", self.module_name, path.to_string_lossy())
        } else {
            write!(f, "{}", self.module_name)
        }
    }
}

/// Represents a single step in a conversion pipeline.
pub trait Step {
    fn get_step_id(&self) -> StepID;
}

#[derive(Deserialize)]
pub struct ImporterStep {
    #[serde(flatten)]
    module: ReadFrom,
    path: PathBuf,
}

impl Step for ImporterStep {
    fn get_step_id(&self) -> StepID {
        StepID::from_importer_module(&self.module, Some(self.path.clone()))
    }
}

#[derive(Deserialize)]
pub struct ExporterStep {
    #[serde(flatten)]
    module: WriteAs,
    path: PathBuf,
}

impl Step for ExporterStep {
    fn get_step_id(&self) -> StepID {
        StepID::from_exporter_module(&self.module, Some(self.path.clone()))
    }
}

#[derive(Deserialize)]
pub struct ManipulatorStep {
    #[serde(flatten)]
    module: GraphOp,
    workflow_directory: Option<PathBuf>,
}

impl Step for ManipulatorStep {
    fn get_step_id(&self) -> StepID {
        StepID::from_graph_op_module(&self.module)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde::de::DeserializeOwned;

    use crate::{GraphOp, ReadFrom, WriteAs};

    #[test]
    fn deser_read_from_pass() {
        assert!(deserialize_toml::<ReadFrom>("tests/deser/deser_read_from.toml").is_ok());
    }

    #[test]
    fn deser_read_from_fail_unknown() {
        assert!(deserialize_toml::<ReadFrom>("tests/deser/deser_read_from_fail.toml").is_err());
    }

    #[test]
    fn deser_graph_op_pass() {
        assert!(deserialize_toml::<GraphOp>("tests/deser/deser_graph_op.toml").is_ok());
    }

    #[test]
    fn deser_graph_op_fail_unknown() {
        assert!(deserialize_toml::<GraphOp>("tests/deser/deser_graph_op_fail.toml").is_err());
    }

    #[test]
    fn deser_write_as_pass() {
        assert!(deserialize_toml::<WriteAs>("tests/deser/deser_write_as.toml").is_ok());
    }

    #[test]
    fn deser_write_as_fail_unknown() {
        assert!(deserialize_toml::<WriteAs>("tests/deser/deser_write_as_fail.toml").is_err());
    }

    fn deserialize_toml<E: DeserializeOwned>(path: &str) -> Result<E, toml::de::Error> {
        let toml_string = fs::read_to_string(path);
        assert!(toml_string.is_ok());
        toml::from_str(&toml_string.unwrap())
    }
}
