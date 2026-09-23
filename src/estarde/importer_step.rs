pub mod optional_sequence {
    use serde::{Deserialize, Deserializer};

    use crate::{ImporterStep, importer::GenericImportConfiguration};

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
                        Some(GenericImportConfiguration::new(
                            given_config.custom_root_name(),
                            resolved_extensions,
                            given_config.document_list().cloned(),
                            resolved_ns,
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
