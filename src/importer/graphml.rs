use facet::Facet;
use graphannis::{model::AnnotationComponentType, update::GraphUpdate};
use serde::Serialize;
use serde_derive::Deserialize;
use std::{fs::File, io::BufReader, path::Path};

use crate::{
    StepID,
    importer::{GenericImportConfiguration, Importer},
    progress::ProgressReporter,
    workflow::StatusSender,
};

/// Imports files in the [GraphML](http://graphml.graphdrawing.org/) file which
/// have to conform to the
/// [graphANNIS data model](https://korpling.github.io/graphANNIS/docs/v2/data-model.html).
#[derive(Facet, Default, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GraphMLImporter {}

const FILE_EXTENSIONS: [&str; 1] = ["graphml"];

impl Importer for GraphMLImporter {
    fn import_corpus(
        &self,
        path: &Path,
        step_id: StepID,
        config: GenericImportConfiguration,
        tx: Option<StatusSender>,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>> {
        let reporter = ProgressReporter::new(tx, step_id, 2)?;

        if config != self.default_configuration() {
            reporter
                .warn("Generic configuration keys are currently ignored for GraphML imports.")?;
        }

        // TODO: support multiple GraphML and connected binary files

        // Load the GraphML files (could be a ZIP file, too) from the given location
        let input = File::open(path)?;
        let mut input = BufReader::new(input);
        let mut updates = GraphUpdate::default();
        let mut edge_updates = GraphUpdate::default();

        graphannis_core::graph::serialization::graphml::read_graphml::<
            AnnotationComponentType,
            _,
            _,
        >(&mut input, &mut updates, &mut edge_updates, &|_| {})?;

        reporter.worked(1)?;
        // Append all edges updates after the node updates:
        // edges would not be added if the nodes they are referring do not exist
        for u in edge_updates.iter()? {
            let (_, event) = u?;
            updates.add_event(event)?;
        }
        reporter.worked(1)?;

        Ok(updates)
    }

    fn default_file_extensions(&self) -> &[&str] {
        &FILE_EXTENSIONS
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::mpsc};

    use insta::assert_snapshot;
    use itertools::Itertools;

    use crate::{
        importer::{GenericImportConfiguration, Importer, graphml::GraphMLImporter},
        test_util::import_as_graphml_string,
    };

    #[test]
    fn single_sentence() {
        let actual = import_as_graphml_string(
            GraphMLImporter::default(),
            Path::new("tests/data/import/graphml/single_sentence.graphml"),
            None,
        )
        .unwrap();

        assert_snapshot!(actual);
    }

    #[test]
    fn generic_config_warning() {
        let input_path = Path::new("tests/data/import/graphml/single_sentence.graphml");
        let import = GraphMLImporter::default();
        let (tx, rx) = mpsc::channel();
        let import = import.import_corpus(
            input_path,
            crate::StepID {
                module_name: "test_import".to_string(),
                path: None,
            },
            GenericImportConfiguration::new_with_root_name("custom_root".to_string()),
            Some(tx),
        );
        assert!(import.is_ok());
        assert_snapshot!(
            rx.into_iter()
                .map(|m| match m {
                    crate::workflow::StatusMessage::Warning(w) => w,
                    _ => "".to_string(),
                })
                .join("\n")
        );
    }
}
