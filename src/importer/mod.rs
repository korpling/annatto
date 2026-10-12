//! Importer modules allow importing files from different formats.
pub mod conllu;
pub mod elan;
#[allow(deprecated)] // TODO remove deprecated features from exmaralda import in the future
pub mod exmaralda;
pub mod file_nodes;
pub mod git;
pub mod graphml;
pub mod meta;
pub mod none;
pub mod opus;
pub mod ptb;
pub mod relannis;
pub mod saltxml;
pub mod table;
pub mod text;
#[allow(deprecated)] // TODO remove deprecated features from textgrid import in the future
pub mod textgrid;
pub mod toolbox;
pub mod treetagger;
pub mod webanno;
pub mod whisper;
pub mod xlsx;
pub mod xml;

use crate::{StepID, util::graphupdate::UpdateWriter, workflow::StatusSender};
use graphannis::{
    model::{AnnotationComponent, AnnotationComponentType},
    update::GraphUpdate,
};
use graphannis_core::graph::ANNIS_NS;
use lazy_static::lazy_static;
use percent_encoding::{AsciiSet, CONTROLS};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

/// An importer is a module that takes a path and produces a list of graph update events.
/// Using the graph update event list allows to execute several importers in parallel and join them to a single annotation graph.
pub trait Importer: Sync + DefaultImportConfiguration {
    /// Returns a list of graph update events for a single corpus.
    ///
    /// # Arguments
    ///
    /// * `input_path` - The path to the corpus files to import. Can be a single file or a directory. For directories, the importer should be able to find all relevant files in the directory.
    /// * `step_id` - The ID of the step.
    /// * `properties` - A map of configuration properties as given in the workflow description.
    /// * `tx` - If supported by the caller, this is a sender object that allows to send [status updates](../workflow/enum.StatusMessage.html) (like information messages, warnings and module progress) to the calling entity.
    ///
    fn import_corpus(
        &self,
        input_path: &Path,
        step_id: StepID,
        update: UpdateWriter,
        tx: Option<StatusSender>,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>>;
}

pub trait DefaultImportConfiguration {
    fn default_configuration(&self) -> GenericImportConfiguration {
        GenericImportConfiguration {
            root_as: None, // default root name does not need to be trait derived, there is no meaningful alternative to None
            extensions: self
                .default_file_extensions()
                .iter()
                .map(<&str>::to_string)
                .collect(),
            documents: None, // default document list does not need to be trait derived, there is no meaningful alternative to None
            default_ns: self.preset_default_namespace().map(ToString::to_string),
            default_components: Some(DefaultComponents {
                ordering: self.preset_default_ordering().cloned(),
                coverage: self.preset_default_coverage().cloned(),
                part_of: self.preset_default_part_of().cloned(),
                dominance: self.preset_default_dominance().cloned(),
                pointing: self.preset_default_pointing().cloned(),
            }),
        }
    }

    fn default_file_extensions(&self) -> &[&str];

    /// This method returns an optional default setting for the default namespace.
    /// Each module implementation is free to choose how to use it, but should use
    /// this method for future modifications and maintenance.
    ///
    /// This is an option, as returning `None` indicates that a default namespace
    /// is not a useful concept for the particular module. For example, for data,
    /// that provide fully qualified annotation names already and a default
    /// namespace cannot be used or would have to overwrite existing namespaces,
    /// which is usually undesired behaviour.
    ///
    /// For the empty namespace, `Some("")` should be returned.
    fn preset_default_namespace(&self) -> Option<&str>;

    /// Importers can define a default `Ordering` they use for ordering nodes.
    /// This can be overwritten by the user (even with a different type).
    /// If a preset is `None`, it either indicates that a default is meaningless
    /// or that no `Ordering` is built.
    ///
    /// Only override this implementation if the module can set a meaningful
    /// default value and uses it.
    fn preset_default_ordering(&self) -> Option<&AnnotationComponent> {
        None
    }

    /// Importers can define a default `Coverage` component they use.
    /// This can be overwritten by the user (even with a different type).
    /// If a preset is `None`, it either indicates that a default is
    /// meaningless or that no `Coverage` edges are built.
    ///
    /// Only override this implementation if the module can set a meaningful
    /// default value and uses it.
    fn preset_default_coverage(&self) -> Option<&AnnotationComponent> {
        None
    }

    /// Importers can define a default `Dominance` component they use.
    /// This can be overwritten by the user (even with a different type).
    /// If a preset is `None`, it either indicates that a default is
    /// meaningless or that no `Dominance` edges are built.
    ///
    /// Only override this implementation if the module can set a meaningful
    /// default value and uses it.
    fn preset_default_dominance(&self) -> Option<&AnnotationComponent> {
        None
    }

    /// Importers can define a default `PartOf` component they use.
    /// This can be overwritten by the user (even with a different type).
    /// If a preset is `None`, it either indicates that a default is
    /// meaningless or that no `PartOf` edges are built.
    ///
    /// Only override this implementation if the module can set a meaningful
    /// default value and uses it.
    fn preset_default_part_of(&self) -> Option<&AnnotationComponent> {
        None
    }

    /// Importers can define a default `Pointing` component they use.
    /// This can be overwritten by the user (even with a different type).
    ///  If a preset is `None`, it either indicates that a default is
    /// meaningless or that no `Pointing` edges are built.
    ///
    /// Only override this implementation if the module can set a meaningful
    /// default value and uses it.
    fn preset_default_pointing(&self) -> Option<&AnnotationComponent> {
        None
    }
}

/// An encoding set for node names.
///
/// This disallows `:` to avoid any possible ambiguities with the `::` annotation
/// match seperator. `/` disallowed so this separator can be used to build
/// hierarchical node IDs and simplifies using node names as file names.
/// Spaces ` ` are encoded to avoid problems with annotation names in the AQL syntax.
/// Since node names might be used as file names, all reserved charactes for
/// Windows file names are encoded as well.
pub const NODE_NAME_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b':')
    .add(b'/')
    .add(b' ')
    .add(b'%')
    .add(b'\\')
    .add(b'<')
    .add(b'>')
    .add(b'"')
    .add(b'|')
    .add(b'?')
    .add(b'*');

// NOTE: fields of this should never be read directly; if you require access please write and use some sort of getter
#[derive(Clone, Default, Deserialize, PartialEq, Serialize)]
pub struct GenericImportConfiguration {
    #[serde(alias = "as", default)]
    root_as: Option<String>,
    #[serde(default)]
    extensions: Vec<String>, // this is a vec for smoother interoperability with the internal api, semantically this behaves like a set down the line
    /// This is a document filter. If none provided, all documents will be imported. If provided, only documents matching the document stem or path will be imported.
    /// Extension is optional.
    #[serde(default)]
    documents: Option<BTreeSet<String>>, // this is an option to have strictly linear semantics on the set: more entries mean more documents starting at 0 meaning 0 documents (not a sensible use-case, but could be used for building subcorpus structure from paths, i. e., to license a corpus hack)
    /// There is a general namespace, that each module uses, that can be set here.
    /// The default value depends on the implementation and the format model.
    #[serde(default)]
    default_ns: Option<String>, // This is an option only for the simple reason that we need to distinguish whether the user SET an empty value or did not set a value (so deserialization forces this upon us). Therefore, this field should never be read directly, there is a method extracting the value.
    /// Per component type one default component can be configured.
    ///
    /// Example:
    /// ```toml
    /// [import.default_components]
    /// part_of = { layer = "alternative_layer", name = "alternative_name" }
    /// pointing = { layer = "", name = "dependency" }
    /// ...
    /// ```
    #[serde(default)]
    default_components: Option<DefaultComponents>,
}

lazy_static! {
    pub(crate) static ref GLOBAL_DEFAULT_ORDERING: AnnotationComponent = AnnotationComponent::new(
        AnnotationComponentType::Ordering,
        ANNIS_NS.to_string(),
        "".to_string()
    );
    pub(crate) static ref GLOBAL_DEFAULT_COVERAGE: AnnotationComponent = AnnotationComponent::new(
        AnnotationComponentType::Coverage,
        ANNIS_NS.to_string(),
        "".to_string()
    );
    pub(crate) static ref GLOBAL_DEFAULT_DOMINANCE: AnnotationComponent = AnnotationComponent::new(
        AnnotationComponentType::Dominance,
        ANNIS_NS.to_string(),
        "".to_string()
    );
    pub(crate) static ref GLOBAL_DEFAULT_PART_OF: AnnotationComponent = AnnotationComponent::new(
        AnnotationComponentType::PartOf,
        ANNIS_NS.to_string(),
        "".to_string()
    );
    pub(crate) static ref GLOBAL_DEFAULT_POINTING: AnnotationComponent = AnnotationComponent::new(
        AnnotationComponentType::Pointing,
        "".to_string(),
        "point".to_string()
    );
}

/// This object models the user interface, i. e., which components the user
/// can and has to define (none). Actual default component values are taken
/// from the individual import steps on deserialization.
#[derive(Default, Deserialize, Clone, PartialEq, Serialize)]
pub struct DefaultComponents {
    #[serde(with = "crate::estarde::annotation_component::as_option")]
    pub(crate) ordering: Option<AnnotationComponent>,
    #[serde(with = "crate::estarde::annotation_component::as_option")]
    pub(crate) coverage: Option<AnnotationComponent>,
    #[serde(with = "crate::estarde::annotation_component::as_option")]
    pub(crate) part_of: Option<AnnotationComponent>,
    #[serde(with = "crate::estarde::annotation_component::as_option")]
    pub(crate) dominance: Option<AnnotationComponent>,
    #[serde(with = "crate::estarde::annotation_component::as_option")]
    pub(crate) pointing: Option<AnnotationComponent>,
}

impl DefaultComponents {
    fn new(
        ordering: Option<AnnotationComponent>,
        coverage: Option<AnnotationComponent>,
        part_of: Option<AnnotationComponent>,
        dominance: Option<AnnotationComponent>,
        pointing: Option<AnnotationComponent>,
    ) -> Self {
        Self {
            ordering,
            coverage,
            part_of,
            dominance,
            pointing,
        }
    }

    pub fn from_configured_importer(
        importer: &dyn Importer,
        config: &GenericImportConfiguration,
    ) -> Self {
        Self::new(
            config
                .configured_ordering()
                .or(importer.preset_default_ordering())
                .cloned(),
            config
                .configured_coverage()
                .or(importer.preset_default_coverage())
                .cloned(),
            config
                .configured_part_of()
                .or(importer.preset_default_part_of())
                .cloned(),
            config
                .configured_dominance()
                .or(importer.preset_default_dominance())
                .cloned(),
            config
                .configured_pointing()
                .or(importer.preset_default_pointing())
                .cloned(),
        )
    }
}

impl<'a> GenericImportConfiguration {
    pub fn custom_root_name(&'a self) -> Option<String> {
        self.root_as.clone()
    }

    pub fn extensions(&'a self) -> &'a Vec<String> {
        self.extensions.as_ref()
    }

    pub fn document_list(&self) -> Option<&BTreeSet<String>> {
        self.documents.as_ref()
    }

    pub fn new(
        root_as: Option<String>,
        extensions: Vec<String>,
        documents: Option<BTreeSet<String>>,
        default_ns: Option<String>,
        default_components: DefaultComponents,
    ) -> Self {
        GenericImportConfiguration {
            root_as,
            extensions,
            documents,
            default_ns,
            default_components: Some(default_components),
        }
    }

    #[cfg(test)]
    pub fn new_with_root_name(root_name: String) -> GenericImportConfiguration {
        GenericImportConfiguration {
            root_as: Some(root_name),
            extensions: vec![],
            documents: None,
            default_ns: Default::default(),
            default_components: Default::default(),
        }
    }

    #[cfg(test)]
    pub fn new_with_extensions(extensions: Vec<String>) -> GenericImportConfiguration {
        GenericImportConfiguration {
            root_as: None,
            extensions,
            documents: None,
            default_ns: Default::default(),
            default_components: Default::default(),
        }
    }

    #[cfg(test)]
    pub fn new_with_default_extensions(
        importer: &dyn DefaultImportConfiguration,
    ) -> GenericImportConfiguration {
        use itertools::Itertools;

        GenericImportConfiguration {
            root_as: None,
            extensions: importer
                .default_file_extensions()
                .iter()
                .map(<&str>::to_string)
                .collect_vec(),
            documents: None,
            default_ns: Default::default(),
            default_components: Default::default(),
        }
    }

    #[cfg(test)]
    pub fn and_extensions(self, extensions: Vec<String>) -> GenericImportConfiguration {
        GenericImportConfiguration {
            root_as: self.root_as,
            extensions,
            documents: self.documents,
            default_ns: Default::default(),
            default_components: Default::default(),
        }
    }

    const EMPTY_NS: &'a str = "";

    pub fn default_namespace(&self) -> &str {
        if let Some(v) = &self.default_ns {
            v.as_str()
        } else {
            GenericImportConfiguration::EMPTY_NS
        }
    }

    pub fn customizes_default_namespace(&self) -> bool {
        self.default_ns.is_some()
    }

    pub fn configured_ordering(&self) -> Option<&AnnotationComponent> {
        if let Some(default_components) = &self.default_components {
            default_components.ordering.as_ref()
        } else {
            None
        }
    }

    pub fn default_ordering(&self) -> &AnnotationComponent {
        if let Some(default_components) = &self.default_components {
            default_components
                .ordering
                .as_ref()
                .unwrap_or(&GLOBAL_DEFAULT_ORDERING)
        } else {
            &GLOBAL_DEFAULT_ORDERING
        }
    }

    pub fn configured_coverage(&self) -> Option<&AnnotationComponent> {
        if let Some(default_components) = &self.default_components {
            default_components.coverage.as_ref()
        } else {
            None
        }
    }

    pub fn default_coverage(&self) -> &AnnotationComponent {
        if let Some(default_components) = &self.default_components {
            default_components
                .coverage
                .as_ref()
                .unwrap_or(&GLOBAL_DEFAULT_COVERAGE)
        } else {
            &GLOBAL_DEFAULT_COVERAGE
        }
    }

    pub fn configured_part_of(&self) -> Option<&AnnotationComponent> {
        if let Some(default_components) = &self.default_components {
            default_components.part_of.as_ref()
        } else {
            None
        }
    }

    pub fn default_part_of(&self) -> &AnnotationComponent {
        if let Some(default_components) = &self.default_components {
            default_components
                .part_of
                .as_ref()
                .unwrap_or(&GLOBAL_DEFAULT_PART_OF)
        } else {
            &GLOBAL_DEFAULT_PART_OF
        }
    }

    pub fn configured_dominance(&self) -> Option<&AnnotationComponent> {
        if let Some(default_components) = &self.default_components {
            default_components.dominance.as_ref()
        } else {
            None
        }
    }

    pub fn default_dominance(&self) -> &AnnotationComponent {
        if let Some(default_components) = &self.default_components {
            default_components
                .dominance
                .as_ref()
                .unwrap_or(&GLOBAL_DEFAULT_DOMINANCE)
        } else {
            &GLOBAL_DEFAULT_DOMINANCE
        }
    }

    pub fn configured_pointing(&self) -> Option<&AnnotationComponent> {
        if let Some(default_components) = &self.default_components {
            default_components.pointing.as_ref()
        } else {
            None
        }
    }

    pub fn default_pointing(&self) -> &AnnotationComponent {
        if let Some(default_components) = &self.default_components {
            default_components
                .pointing
                .as_ref()
                .unwrap_or(&GLOBAL_DEFAULT_POINTING)
        } else {
            &GLOBAL_DEFAULT_POINTING
        }
    }
}

pub type NamedPath = (PathBuf, String);

pub type NamedPaths = Vec<NamedPath>;
