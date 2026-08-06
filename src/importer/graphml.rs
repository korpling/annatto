use facet::Facet;
use graphannis::{model::AnnotationComponentType, update::GraphUpdate};
use graphannis_core::graph::serialization;

use crate::{
    StepID,
    importer::{GenericImportConfiguration, Importer},
    progress::ProgressReporter,
    workflow::StatusSender,
};
use serde::Serialize;
use serde_derive::Deserialize;
use std::{fs::File, io::BufReader, path::Path};

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
        let reporter = ProgressReporter::new_unknown_total_work(tx.clone(), step_id.clone())?;

        if config != self.default_configuration() {
            reporter
                .warn("Generic configuration keys are currently ignored for GraphML imports.")?;
        }

        // TODO: connected binary files

        // Load the GraphML files (could be a ZIP file, too) from the given location
        let mut updates = GraphUpdate::default();
        let mut edge_updates = GraphUpdate::default();

        // Find all the files that belong to this corpus file (if it is partitioned)
        let graphml_files = serialization::graphml::files_for_corpus(path)?;

        // Update the progress: we have to read in all files and also join the edge and node updates.
        let reporter = ProgressReporter::new(tx, step_id, 1 + graphml_files.len())?;
        reporter.info("Reading in GraphML file")?;
        for input_path in graphml_files {
            // Always buffer the read operations
            let input_file = File::open(input_path)?;
            let mut input = BufReader::new(input_file);

            serialization::graphml::read_graphml::<AnnotationComponentType, _, _>(
                &mut input,
                &mut updates,
                &mut edge_updates,
                &|_| {},
            )?;

            reporter.worked(1)?;
        }

        // Append all edges updates after the node updates:
        // edges would not be added if the nodes they are referring do not exist
        reporter.info("Joining node and edge updates into one list")?;
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
