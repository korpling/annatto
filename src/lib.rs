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

use documented::{Documented, DocumentedFields};
use error::Result;
use exporter::{
    exmaralda::ExportExmaralda, graphml::GraphMLExporter, sequence::ExportSequence,
    xlsx::XlsxExporter, Exporter,
};
use importer::{
    conllu::ImportCoNLLU, exmaralda::ImportEXMARaLDA, file_nodes::CreateFileNodes,
    graphml::GraphMLImporter, meta::AnnotateCorpus, none::CreateEmptyCorpus, opus::ImportOpusLinks,
    ptb::ImportPTB, textgrid::ImportTextgrid, toolbox::ImportToolBox, treetagger::ImportTreeTagger,
    xlsx::ImportSpreadsheet, xml::ImportXML, Importer,
};
use manipulator::{
    check::Check, chunker::Chunk, collapse::Collapse, enumerate::EnumerateMatches, link::LinkNodes,
    map::MapAnnos, merge::Merge, no_op::NoOp, re::Revise, split::SplitValues, Manipulator,
};
use serde_derive::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;
use strum::{AsRefStr, EnumDiscriminants, EnumIter};
use tabled::Tabled;

#[derive(Tabled)]
pub struct ModuleConfiguration {
    pub name: String,
    pub description: String,
}

#[derive(Deserialize, EnumDiscriminants, AsRefStr)]
#[strum(serialize_all = "lowercase")]
#[strum_discriminants(derive(EnumIter, AsRefStr), strum(serialize_all = "lowercase"))]
#[serde(tag = "format", rename_all = "lowercase", content = "config")]
pub enum WriteAs {
    GraphML(#[serde(default)] GraphMLExporter), // the purpose of serde(default) here is, that an empty `[export.config]` table can be omited
    EXMARaLDA(#[serde(default)] ExportExmaralda),
    Sequence(#[serde(default)] ExportSequence),
    Xlsx(#[serde(default)] XlsxExporter),
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
            WriteAs::GraphML(m) => m,
            WriteAs::EXMARaLDA(m) => m,
            WriteAs::Sequence(m) => m,
            WriteAs::Xlsx(m) => m,
        }
    }
}

impl WriteAsDiscriminants {
    pub fn module_doc(&self) -> &str {
        match self {
            WriteAsDiscriminants::GraphML => GraphMLExporter::DOCS,
            WriteAsDiscriminants::EXMARaLDA => ExportExmaralda::DOCS,
            WriteAsDiscriminants::Sequence => ExportSequence::DOCS,
            WriteAsDiscriminants::Xlsx => XlsxExporter::DOCS,
        }
    }

    pub fn module_configs(&self) -> Vec<ModuleConfiguration> {
        let mut result = Vec::new();
        let (field_names, field_docs) = match self {
            WriteAsDiscriminants::GraphML => (
                GraphMLExporter::FIELD_NAMES_AS_SLICE,
                GraphMLExporter::FIELD_DOCS,
            ),
            WriteAsDiscriminants::EXMARaLDA => (
                ExportExmaralda::FIELD_NAMES_AS_SLICE,
                ExportExmaralda::FIELD_DOCS,
            ),
            WriteAsDiscriminants::Sequence => (
                ExportSequence::FIELD_NAMES_AS_SLICE,
                ExportSequence::FIELD_DOCS,
            ),
            WriteAsDiscriminants::Xlsx => {
                (XlsxExporter::FIELD_NAMES_AS_SLICE, XlsxExporter::FIELD_DOCS)
            }
        };
        for (idx, n) in field_names.iter().enumerate() {
            if idx < field_docs.len() {
                result.push(ModuleConfiguration {
                    name: n.to_string(),
                    description: field_docs[idx].unwrap_or_default().to_string(),
                });
            } else {
                result.push(ModuleConfiguration {
                    name: n.to_string(),
                    description: String::default(),
                });
            }
        }
        result
    }
}

#[derive(Deserialize, EnumDiscriminants, AsRefStr)]
#[strum(serialize_all = "lowercase")]
#[strum_discriminants(derive(EnumIter, AsRefStr), strum(serialize_all = "lowercase"))]
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
    Toolbox(#[serde(default)] ImportToolBox),
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
            ReadFrom::Toolbox(m) => m,
        }
    }
}

impl ReadFromDiscriminants {
    pub fn module_doc(&self) -> &str {
        match self {
            ReadFromDiscriminants::CoNLLU => ImportCoNLLU::DOCS,
            ReadFromDiscriminants::EXMARaLDA => ImportEXMARaLDA::DOCS,
            ReadFromDiscriminants::GraphML => GraphMLImporter::DOCS,
            ReadFromDiscriminants::Meta => AnnotateCorpus::DOCS,
            ReadFromDiscriminants::None => CreateEmptyCorpus::DOCS,
            ReadFromDiscriminants::Opus => ImportOpusLinks::DOCS,
            ReadFromDiscriminants::Path => CreateFileNodes::DOCS,
            ReadFromDiscriminants::PTB => ImportPTB::DOCS,
            ReadFromDiscriminants::TextGrid => ImportTextgrid::DOCS,
            ReadFromDiscriminants::Toolbox => ImportToolBox::DOCS,
            ReadFromDiscriminants::TreeTagger => ImportTreeTagger::DOCS,
            ReadFromDiscriminants::Xlsx => ImportSpreadsheet::DOCS,
            ReadFromDiscriminants::Xml => ImportXML::DOCS,
        }
    }

    pub fn module_configs(&self) -> Vec<ModuleConfiguration> {
        let mut result = Vec::new();
        let (field_names, field_docs) = match self {
            ReadFromDiscriminants::CoNLLU => {
                (ImportCoNLLU::FIELD_NAMES_AS_SLICE, ImportCoNLLU::FIELD_DOCS)
            }
            ReadFromDiscriminants::EXMARaLDA => (
                ImportEXMARaLDA::FIELD_NAMES_AS_SLICE,
                ImportEXMARaLDA::FIELD_DOCS,
            ),
            ReadFromDiscriminants::GraphML => (
                GraphMLImporter::FIELD_NAMES_AS_SLICE,
                GraphMLImporter::FIELD_DOCS,
            ),
            ReadFromDiscriminants::Meta => (
                AnnotateCorpus::FIELD_NAMES_AS_SLICE,
                AnnotateCorpus::FIELD_DOCS,
            ),
            ReadFromDiscriminants::None => (
                CreateEmptyCorpus::FIELD_NAMES_AS_SLICE,
                CreateEmptyCorpus::FIELD_DOCS,
            ),
            ReadFromDiscriminants::Opus => (
                ImportOpusLinks::FIELD_NAMES_AS_SLICE,
                ImportOpusLinks::FIELD_DOCS,
            ),
            ReadFromDiscriminants::Path => (
                CreateFileNodes::FIELD_NAMES_AS_SLICE,
                CreateFileNodes::FIELD_DOCS,
            ),
            ReadFromDiscriminants::PTB => (ImportPTB::FIELD_NAMES_AS_SLICE, ImportPTB::FIELD_DOCS),
            ReadFromDiscriminants::TextGrid => (
                ImportTextgrid::FIELD_NAMES_AS_SLICE,
                ImportTextgrid::FIELD_DOCS,
            ),
            ReadFromDiscriminants::TreeTagger => (
                ImportTreeTagger::FIELD_NAMES_AS_SLICE,
                ImportTreeTagger::FIELD_DOCS,
            ),
            ReadFromDiscriminants::Xlsx => (
                ImportSpreadsheet::FIELD_NAMES_AS_SLICE,
                ImportSpreadsheet::FIELD_DOCS,
            ),
            ReadFromDiscriminants::Xml => (ImportXML::FIELD_NAMES_AS_SLICE, ImportXML::FIELD_DOCS),
            ReadFromDiscriminants::Toolbox => (
                ImportToolBox::FIELD_NAMES_AS_SLICE,
                ImportToolBox::FIELD_DOCS,
            ),
        };
        for (idx, n) in field_names.iter().enumerate() {
            if idx < field_docs.len() {
                result.push(ModuleConfiguration {
                    name: n.to_string(),
                    description: field_docs[idx].unwrap_or_default().to_string(),
                });
            } else {
                result.push(ModuleConfiguration {
                    name: n.to_string(),
                    description: String::default(),
                });
            }
        }
        result
    }
}

#[derive(Deserialize, EnumDiscriminants, AsRefStr)]
#[strum(serialize_all = "lowercase")]
#[strum_discriminants(derive(EnumIter, AsRefStr), strum(serialize_all = "lowercase"))]
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
    Split(SplitValues),           // no default
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
            GraphOp::Split(m) => m,
        }
    }
}

impl GraphOpDiscriminants {
    pub fn module_doc(&self) -> &str {
        match self {
            GraphOpDiscriminants::Check => Check::DOCS,
            GraphOpDiscriminants::Collapse => Collapse::DOCS,
            GraphOpDiscriminants::Enumerate => EnumerateMatches::DOCS,
            GraphOpDiscriminants::Link => LinkNodes::DOCS,
            GraphOpDiscriminants::Map => MapAnnos::DOCS,
            GraphOpDiscriminants::Merge => Merge::DOCS,
            GraphOpDiscriminants::Revise => Revise::DOCS,
            GraphOpDiscriminants::Chunk => Chunk::DOCS,
            GraphOpDiscriminants::None => NoOp::DOCS,
            GraphOpDiscriminants::Split => SplitValues::DOCS,
        }
    }

    pub fn module_configs(&self) -> Vec<ModuleConfiguration> {
        let mut result = Vec::new();
        let (field_names, field_docs) = match self {
            GraphOpDiscriminants::Check => (Check::FIELD_NAMES_AS_SLICE, Check::FIELD_DOCS),
            GraphOpDiscriminants::Collapse => {
                (Collapse::FIELD_NAMES_AS_SLICE, Collapse::FIELD_DOCS)
            }
            GraphOpDiscriminants::Enumerate => (
                EnumerateMatches::FIELD_NAMES_AS_SLICE,
                EnumerateMatches::FIELD_DOCS,
            ),
            GraphOpDiscriminants::Link => (LinkNodes::FIELD_NAMES_AS_SLICE, LinkNodes::FIELD_DOCS),
            GraphOpDiscriminants::Map => (MapAnnos::FIELD_NAMES_AS_SLICE, MapAnnos::FIELD_DOCS),
            GraphOpDiscriminants::Merge => (Merge::FIELD_NAMES_AS_SLICE, Merge::FIELD_DOCS),
            GraphOpDiscriminants::Revise => (Revise::FIELD_NAMES_AS_SLICE, Revise::FIELD_DOCS),
            GraphOpDiscriminants::Chunk => (Chunk::FIELD_NAMES_AS_SLICE, Chunk::FIELD_DOCS),
            GraphOpDiscriminants::None => (NoOp::FIELD_NAMES_AS_SLICE, NoOp::FIELD_DOCS),
            GraphOpDiscriminants::Split => {
                (SplitValues::FIELD_NAMES_AS_SLICE, SplitValues::FIELD_DOCS)
            }
        };
        for (idx, n) in field_names.iter().enumerate() {
            if idx < field_docs.len() {
                result.push(ModuleConfiguration {
                    name: n.to_string(),
                    description: field_docs[idx].unwrap_or_default().to_string(),
                });
            } else {
                result.push(ModuleConfiguration {
                    name: n.to_string(),
                    description: String::default(),
                });
            }
        }
        result
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
