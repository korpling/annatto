pub mod optional_sequence {
    use serde::{Deserialize, Deserializer};

    use crate::{
        ImporterStep,
        importer::{DefaultComponents, GenericImportConfiguration},
    };

    pub fn deserialize<'de, D: Deserializer<'de>, T: FromIterator<ImporterStep>>(
        deserializer: D,
    ) -> Result<Option<T>, D::Error> {
        let opt_step_vec = Option::<Vec<ImporterStep>>::deserialize(deserializer)?;
        Ok(opt_step_vec.map(|inner| {
            inner
                .into_iter()
                .map(|step| {
                    let ImporterStep {
                        module,
                        path,
                        description,
                        generic_config,
                    } = step;
                    let resolved_config = if let Some(given_config) = generic_config {
                        let extensions = given_config.extensions();
                        let resolved_extensions = if extensions.is_empty() {
                            module
                                .reader()
                                .default_file_extensions()
                                .iter()
                                .map(<&str>::to_string)
                                .collect()
                        } else {
                            extensions.clone()
                        };
                        let resolved_ns = if given_config.customizes_default_namespace() {
                            Some(given_config.default_namespace().to_string())
                        } else {
                            module
                                .reader()
                                .preset_default_namespace()
                                .map(ToString::to_string)
                        };
                        let resolved_default_components = if let Some(DefaultComponents {
                            ordering,
                            coverage,
                            part_of,
                            dominance,
                            pointing,
                        }) = given_config.default_components()
                        {
                            DefaultComponents {
                                ordering: ordering
                                    .as_ref()
                                    .or(module.reader().preset_default_ordering())
                                    .cloned(),
                                coverage: coverage
                                    .as_ref()
                                    .or(module.reader().preset_default_coverage())
                                    .cloned(),
                                part_of: part_of
                                    .as_ref()
                                    .or(module.reader().preset_default_part_of())
                                    .cloned(),
                                dominance: dominance
                                    .as_ref()
                                    .or(module.reader().preset_default_dominance())
                                    .cloned(),
                                pointing: pointing
                                    .as_ref()
                                    .or(module.reader().preset_default_pointing())
                                    .cloned(),
                            }
                        } else {
                            DefaultComponents {
                                ordering: module.reader().preset_default_ordering().cloned(),
                                coverage: module.reader().preset_default_coverage().cloned(),
                                part_of: module.reader().preset_default_part_of().cloned(),
                                dominance: module.reader().preset_default_dominance().cloned(),
                                pointing: module.reader().preset_default_pointing().cloned(),
                            }
                        };
                        Some(GenericImportConfiguration::new(
                            given_config.custom_root_name(),
                            resolved_extensions,
                            given_config.document_list().cloned(),
                            resolved_ns,
                            resolved_default_components,
                        ))
                    } else {
                        None
                    };
                    ImporterStep {
                        module,
                        path,
                        description,
                        generic_config: resolved_config,
                    }
                })
                .collect::<T>()
        }))
    }
}
