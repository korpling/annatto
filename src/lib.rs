#![cfg_attr(not(test), warn(clippy::unwrap_used))]

pub mod error;
pub mod estarde;
pub mod exporter;
pub mod importer;
pub mod manipulator;
pub mod models;
pub mod progress;
#[cfg(test)]
pub(crate) mod test_util;
pub mod util;
pub mod workflow;

use std::{
    fmt::Display,
    path::{Path, PathBuf},
};

use error::Result;
use exporter::{
    Exporter, conllu::ExportCoNLLU, exmaralda::ExportExmaralda, graphml::GraphMLExporter,
    meta::ExportMeta, saltxml::ExportSaltXml, sequence::ExportSequence, table::ExportTable,
    textgrid::ExportTextGrid, xlsx::ExportXlsx,
};
use facet::Facet;
use facet_reflect::Peek;
use graphannis::AnnotationGraph;
use importer::{
    Importer, conllu::ImportCoNLLU, exmaralda::ImportEXMARaLDA, file_nodes::CreateFileNodes,
    graphml::GraphMLImporter, meta::AnnotateCorpus, none::CreateEmptyCorpus, opus::ImportOpusLinks,
    ptb::ImportPTB, relannis::ImportRelAnnis, saltxml::ImportSaltXml, table::ImportTable,
    textgrid::ImportTextgrid, toolbox::ImportToolBox, treetagger::ImportTreeTagger,
    webanno::ImportWebAnnoTSV, whisper::ImportWhisper, xlsx::ImportSpreadsheet, xml::ImportXML,
};
use manipulator::{
    Manipulator, align::AlignNodes, check::Check, chunker::Chunk, collapse::Collapse,
    enumerate::EnumerateMatches, filter::FilterNodes, link::LinkNodes, map::MapAnnos, no_op::NoOp,
    re::Revise, sleep::Sleep, split::SplitValues, time::Filltime, visualize::Visualize,
};
use serde::Serialize;
use serde_derive::Deserialize;
use tabled::Tabled;
use workflow::StatusSender;

use crate::{
    exporter::treetagger::ExportTreeTagger,
    importer::{git::ImportGitMetadata, text::ImportText},
};

#[derive(Tabled)]
pub struct ModuleConfiguration {
    pub name: String,
    pub description: String,
}

#[derive(Facet, Deserialize, Serialize, Clone, PartialEq)]
#[repr(u16)]
#[serde(tag = "format", rename_all = "lowercase", content = "config")]
pub enum WriteAs {
    CoNLLU(#[serde(default)] Box<ExportCoNLLU>),
    EXMARaLDA(#[serde(default)] ExportExmaralda),
    GraphML(#[serde(default)] GraphMLExporter), // the purpose of serde(default) here is, that an empty `[export.config]` table can be omited in the future
    Meta(#[serde(default)] ExportMeta),
    SaltXml(#[serde(default)] ExportSaltXml),
    Sequence(#[serde(default)] ExportSequence),
    Table(#[serde(default)] ExportTable),
    TextGrid(ExportTextGrid), // do not use default, as all attributes have their individual defaults
    TreeTagger(#[serde(default)] ExportTreeTagger),
    Xlsx(#[serde(default)] ExportXlsx),
}

impl Default for WriteAs {
    // the purpose of this default is to allow to omit `format` in an `[[export]]` table
    fn default() -> Self {
        WriteAs::GraphML(GraphMLExporter::default())
    }
}

impl WriteAs {
    fn writer(&self) -> &dyn Exporter {
        match self {
            WriteAs::EXMARaLDA(m) => m,
            WriteAs::GraphML(m) => m,
            WriteAs::SaltXml(m) => m,
            WriteAs::Sequence(m) => m,
            WriteAs::Table(m) => m,
            WriteAs::TextGrid(m) => m,
            WriteAs::TreeTagger(m) => m,
            WriteAs::Xlsx(m) => m,
            WriteAs::CoNLLU(m) => m.as_ref(),
            WriteAs::Meta(m) => m,
        }
    }

    /// Gets the external name of this module (in lowercase).
    pub fn name(&self) -> Result<String> {
        let parent_enum = Peek::new(self).into_enum()?;
        let variant = parent_enum.active_variant()?;
        Ok(variant.name.to_lowercase())
    }
}

#[derive(Facet, Deserialize, Serialize, Clone, PartialEq)]
#[serde(tag = "format", rename_all = "lowercase", content = "config")]
#[repr(u16)]
pub enum ReadFrom {
    CoNLLU(#[serde(default)] ImportCoNLLU),
    EXMARaLDA(#[serde(default)] ImportEXMARaLDA),
    Git(ImportGitMetadata),
    GraphML(#[serde(default)] GraphMLImporter),
    Meta(#[serde(default)] AnnotateCorpus),
    None(#[serde(default)] CreateEmptyCorpus),
    Opus(#[serde(default)] ImportOpusLinks),
    Path(#[serde(default)] CreateFileNodes),
    PTB(#[serde(default)] ImportPTB),
    RelAnnis(#[serde(default)] ImportRelAnnis),
    SaltXml(#[serde(default)] ImportSaltXml),
    Table(#[serde(default)] ImportTable),
    Text(#[serde(default)] ImportText),
    TextGrid(#[serde(default)] ImportTextgrid),
    Toolbox(#[serde(default)] ImportToolBox),
    TreeTagger(#[serde(default)] ImportTreeTagger),
    Webanno(#[serde(default)] ImportWebAnnoTSV),
    Whisper(#[serde(default)] ImportWhisper),
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
            ReadFrom::GraphML(m) => m,
            ReadFrom::Meta(m) => m,
            ReadFrom::None(m) => m,
            ReadFrom::Opus(m) => m,
            ReadFrom::Path(m) => m,
            ReadFrom::PTB(m) => m,
            ReadFrom::RelAnnis(m) => m,
            ReadFrom::SaltXml(m) => m,
            ReadFrom::Table(m) => m,
            ReadFrom::Text(m) => m,
            ReadFrom::TextGrid(m) => m,
            ReadFrom::Toolbox(m) => m,
            ReadFrom::TreeTagger(m) => m,
            ReadFrom::Whisper(m) => m,
            ReadFrom::Xlsx(m) => m,
            ReadFrom::Xml(m) => m,
            ReadFrom::Webanno(m) => m,
            ReadFrom::Git(m) => m,
        }
    }

    /// Gets the external name of this module (in lowercase).
    pub fn name(&self) -> Result<String> {
        let parent_enum = Peek::new(self).into_enum()?;
        let variant = parent_enum.active_variant()?;
        Ok(variant.name.to_lowercase())
    }
}

#[derive(Facet, Deserialize, Serialize, Clone, PartialEq)]
#[serde(tag = "action", rename_all = "lowercase", content = "config")]
#[repr(u16)]
pub enum GraphOp {
    Align(AlignNodes),  // no default
    Check(Check),       // no default, has a (required) path attribute
    Collapse(Collapse), // no default, there is no such thing as a default component
    Filter(FilterNodes),
    Visualize(#[serde(default)] Visualize),
    Enumerate(#[serde(default)] EnumerateMatches),
    Link(LinkNodes),                  // no default, has required attributes
    Map(MapAnnos),                    // no default, has a (required) path attribute
    Revise(#[serde(default)] Revise), // does nothing on default
    Time(#[serde(default)] Filltime),
    Chunk(#[serde(default)] Chunk),
    Split(#[serde(default)] SplitValues), // default does nothing
    Sleep(#[serde(default)] Sleep),
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
            GraphOp::Visualize(m) => m,
            GraphOp::Link(m) => m,
            GraphOp::Map(m) => m,
            GraphOp::Revise(m) => m,
            GraphOp::None(m) => m,
            GraphOp::Enumerate(m) => m,
            GraphOp::Chunk(m) => m,
            GraphOp::Split(m) => m,
            GraphOp::Filter(m) => m,
            GraphOp::Time(m) => m,
            GraphOp::Sleep(m) => m,
            GraphOp::Align(m) => m,
        }
    }

    /// Gets the external name of this module (in lowercase).
    pub fn name(&self) -> Result<String> {
        let parent_enum = Peek::new(self).into_enum()?;
        let variant = parent_enum.active_variant()?;
        Ok(variant.name.to_lowercase())
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
    pub fn from_importer_step(step: &ImporterStep) -> StepID {
        StepID {
            module_name: format!("import_{}", step.module.name().unwrap_or_default()),
            path: Some(step.path.clone()),
        }
    }

    pub fn from_graphop_step(step: &ManipulatorStep, position_in_workflow: usize) -> StepID {
        StepID {
            module_name: format!(
                "{position_in_workflow}_{}",
                step.module.name().unwrap_or_default()
            ),
            path: None,
        }
    }

    pub fn from_exporter_step(step: &ExporterStep) -> StepID {
        StepID {
            module_name: format!("export_{}", step.module.name().unwrap_or_default()),
            path: Some(step.path.clone()),
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
pub trait Step {}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ImporterStep {
    #[serde(flatten)]
    module: ReadFrom,
    path: PathBuf,
    #[serde(default)]
    label: Option<String>,
}

impl ImporterStep {
    /// Create a new importer step with the given module and input path.
    pub fn new<P>(module: ReadFrom, path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            module,
            path: path.into(),
            label: None,
        }
    }

    #[cfg(test)]
    fn execute(
        &self,
        tx: Option<StatusSender>,
    ) -> std::result::Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        self.module
            .reader()
            .import_corpus(&self.path, StepID::from_importer_step(&self), tx)
    }
}

impl Step for ImporterStep {}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExporterStep {
    #[serde(flatten)]
    module: WriteAs,
    path: PathBuf,
    #[serde(default)]
    label: Option<String>,
}

impl ExporterStep {
    /// Create a new exporter step with the given module and output path.
    pub fn new<P>(module: WriteAs, path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            module,
            path: path.into(),
            label: None,
        }
    }

    #[cfg(test)]
    fn execute(
        &self,
        graph: &AnnotationGraph,
        tx: Option<StatusSender>,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        self.module
            .writer()
            .export_corpus(graph, &self.path, StepID::from_exporter_step(&self), tx)
    }
}

impl Step for ExporterStep {}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ManipulatorStep {
    #[serde(flatten)]
    module: GraphOp,
    workflow_directory: Option<PathBuf>,
    #[serde(default)]
    label: Option<String>,
}

impl ManipulatorStep {
    /// Create a new graph operation step with the given module and an optional working directory.
    pub fn new<P>(module: GraphOp, workflow_directory: Option<P>) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            module,
            workflow_directory: workflow_directory.map(|d| d.into()),
            label: None,
        }
    }

    fn execute(
        &self,
        graph: &mut AnnotationGraph,
        workflow_directory: &Path,
        position_in_workflow: usize,
        tx: Option<StatusSender>,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let step_id = StepID::from_graphop_step(self, position_in_workflow);
        self.module
            .processor()
            .validate_graph(graph, step_id.clone(), tx.clone())?;
        self.module
            .processor()
            .manipulate_corpus(graph, workflow_directory, step_id, tx)
    }
}

impl Step for ManipulatorStep {}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde::de::DeserializeOwned;

    use crate::{GraphOp, ReadFrom, WriteAs, workflow::Workflow};

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

    #[test]
    fn deserialize_with_custom_id() {
        let d = deserialize_toml::<Workflow>("tests/deser/workflow-with-custom-labels.toml");
        assert!(d.is_ok(), "Err: {:?}", d.err().unwrap());
    }
}
