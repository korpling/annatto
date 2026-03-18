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
                    let resolved_config = if let Some(GenericImportConfiguration {
                        root_as,
                        extensions,
                    }) = generic_config
                    {
                        let resolved_extensions = if extensions.is_empty() {
                            module
                                .reader()
                                .default_file_extensions()
                                .iter()
                                .map(<&str>::to_string)
                                .collect()
                        } else {
                            extensions
                        };
                        Some(GenericImportConfiguration {
                            root_as,
                            extensions: resolved_extensions,
                        })
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
