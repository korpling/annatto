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
                        documents,
                        default_ns,
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
                        let resolved_ns = if default_ns.is_none() {
                            module.reader().default_namespace().map(ToString::to_string)
                        } else {
                            default_ns
                        };
                        Some(GenericImportConfiguration {
                            root_as,
                            extensions: resolved_extensions,
                            documents,
                            default_ns: resolved_ns,
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
