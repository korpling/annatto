use std::{collections::BTreeSet, path::PathBuf};

use graphannis::model::AnnotationComponent;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    ImporterStep, ReadFrom,
    importer::{
        DefaultComponents, GLOBAL_DEFAULT_COVERAGE, GLOBAL_DEFAULT_DOMINANCE,
        GLOBAL_DEFAULT_ORDERING, GLOBAL_DEFAULT_PART_OF, GLOBAL_DEFAULT_POINTING,
        GenericImportConfiguration, ModelConfiguration,
    },
};

#[derive(Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ImporterStepUI {
    #[serde(flatten)]
    module: ReadFrom,
    path: PathBuf,
    #[serde(alias = "label")]
    description: Option<String>,
    #[serde(flatten)]
    generic_config: GenericConfigUI,
}

impl TryFrom<ImporterStepUI> for ImporterStep {
    type Error = anyhow::Error;

    fn try_from(value: ImporterStepUI) -> Result<Self, Self::Error> {
        let ImporterStepUI {
            module,
            path,
            description,
            generic_config,
        } = value;
        let GenericConfigUI {
            root_as,
            extensions,
            documents,
            model,
        } = generic_config;
        let resolved_config = {
            let resolved_extensions = if let Some(configured_extensions) = extensions
                && !configured_extensions.is_empty()
            {
                configured_extensions
            } else {
                module
                    .reader()
                    .default_file_extensions()
                    .iter()
                    .map(ToString::to_string)
                    .collect_vec()
            };
            let resolved_model = if let Some(ModelConfigurationUI {
                default_ns,
                default_components,
            }) = model
            {
                let resolved_default_ns = if let Some(ns) = default_ns {
                    ns
                } else {
                    module
                        .reader()
                        .default_configuration()
                        .default_namespace()
                        .to_string()
                };
                let resolved_components = {
                    let DefaultComponentsUI {
                        ordering,
                        coverage,
                        part_of,
                        dominance,
                        pointing,
                    } = default_components;
                    let resolved_ordering = ordering
                        .or(module.reader().preset_default_ordering().cloned())
                        .unwrap_or((*GLOBAL_DEFAULT_ORDERING).clone());
                    let resolved_coverage = coverage
                        .or(module.reader().preset_default_coverage().cloned())
                        .unwrap_or((*GLOBAL_DEFAULT_COVERAGE).clone());
                    let resolved_part_of = part_of
                        .or(module.reader().preset_default_part_of().cloned())
                        .unwrap_or((*GLOBAL_DEFAULT_PART_OF).clone());
                    let resolved_dominance = dominance
                        .or(module.reader().preset_default_dominance().cloned())
                        .unwrap_or((*GLOBAL_DEFAULT_DOMINANCE).clone());
                    let resolved_pointing = pointing
                        .or(module.reader().preset_default_pointing().cloned())
                        .unwrap_or((*GLOBAL_DEFAULT_POINTING).clone());
                    DefaultComponents::new(
                        resolved_ordering,
                        resolved_coverage,
                        resolved_part_of,
                        resolved_dominance,
                        resolved_pointing,
                    )
                };
                ModelConfiguration::new(resolved_default_ns, resolved_components)
            } else {
                let resolved_ordering = module
                    .reader()
                    .preset_default_ordering()
                    .unwrap_or(&GLOBAL_DEFAULT_ORDERING)
                    .clone();
                let resolved_coverage = module
                    .reader()
                    .preset_default_coverage()
                    .unwrap_or(&GLOBAL_DEFAULT_COVERAGE)
                    .clone();
                let resolved_part_of = module
                    .reader()
                    .preset_default_part_of()
                    .unwrap_or(&GLOBAL_DEFAULT_PART_OF)
                    .clone();
                let resolved_dominance = module
                    .reader()
                    .preset_default_dominance()
                    .unwrap_or(&GLOBAL_DEFAULT_DOMINANCE)
                    .clone();
                let resolved_pointing = module
                    .reader()
                    .preset_default_pointing()
                    .unwrap_or(&GLOBAL_DEFAULT_POINTING)
                    .clone();
                let default_components = DefaultComponents::new(
                    resolved_ordering,
                    resolved_coverage,
                    resolved_part_of,
                    resolved_dominance,
                    resolved_pointing,
                );
                ModelConfiguration::new(
                    module
                        .reader()
                        .default_configuration()
                        .default_namespace()
                        .to_string(),
                    default_components,
                )
            };
            GenericImportConfiguration::new(root_as, resolved_extensions, documents, resolved_model)
        };
        Ok(ImporterStep {
            module,
            path,
            description,
            generic_config: resolved_config,
        })
    }
}

impl From<ImporterStep> for ImporterStepUI {
    fn from(value: ImporterStep) -> Self {
        let ImporterStep {
            module,
            path,
            description,
            generic_config,
        } = value;
        let generic_config = {
            GenericConfigUI {
                root_as: generic_config.custom_root_name(),
                extensions: Some(generic_config.extensions().clone()),
                documents: generic_config.document_list().cloned(),
                model: Some(ModelConfigurationUI {
                    default_ns: Some(generic_config.default_namespace().to_string()),
                    default_components: DefaultComponentsUI {
                        ordering: Some(generic_config.default_ordering().clone()),
                        coverage: Some(generic_config.default_coverage().clone()),
                        dominance: Some(generic_config.default_dominance().clone()),
                        part_of: Some(generic_config.default_part_of().clone()),
                        pointing: Some(generic_config.default_pointing().clone()),
                    },
                }),
            }
        };
        Self {
            module,
            path,
            description,
            generic_config,
        }
    }
}

#[derive(Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct GenericConfigUI {
    #[serde(rename = "as")]
    root_as: Option<String>,
    #[serde(default)]
    extensions: Option<Vec<String>>,
    /// This is a document filter. If none provided, all documents will be imported. If provided, only documents matching the document stem or path will be imported.
    /// Extension is optional.
    documents: Option<BTreeSet<String>>, // this is an option to have strictly linear semantics on the set: more entries mean more documents starting at 0 meaning 0 documents (not a sensible use-case, but could be used for building subcorpus structure from paths, i. e., to license a corpus hack)
    /// This is a sub section of configurations that restrains the model, that will be derived from the data
    /// in certain ways, e. g. by setting a default annotation namespace or default components to be used.
    // the user must be allowed to leave out this section, as it is for advanced level scenarios only.
    model: Option<ModelConfigurationUI>,
}

#[derive(Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ModelConfigurationUI {
    /// There is a general namespace, that each module uses, that can be set here.
    /// The default value depends on the implementation and the format model.
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
    default_components: DefaultComponentsUI,
}

#[derive(Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DefaultComponentsUI {
    #[serde(with = "crate::estarde::annotation_component::as_option", default)]
    pub(crate) ordering: Option<AnnotationComponent>,
    #[serde(with = "crate::estarde::annotation_component::as_option", default)]
    pub(crate) coverage: Option<AnnotationComponent>,
    #[serde(with = "crate::estarde::annotation_component::as_option", default)]
    pub(crate) part_of: Option<AnnotationComponent>,
    #[serde(with = "crate::estarde::annotation_component::as_option", default)]
    pub(crate) dominance: Option<AnnotationComponent>,
    #[serde(with = "crate::estarde::annotation_component::as_option", default)]
    pub(crate) pointing: Option<AnnotationComponent>,
}
