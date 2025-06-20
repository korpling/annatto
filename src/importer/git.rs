use std::path::PathBuf;

use anyhow::anyhow;
use documented::{Documented, DocumentedFields};
use git2::{Repository, StatusOptions};
use graphannis::update::{GraphUpdate, UpdateEvent};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use struct_field_names_as_array::FieldNamesAsSlice;

use crate::importer::Importer;

/// This importer can enrich a corpus with commit metadata. The import path needs
/// to be the root directory of the local git repository.
#[derive(Deserialize, Serialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields)]
pub struct ImportGitMetadata {
    /// The relative to the data folder that is being imported with another
    /// module in your workflow.
    folder: PathBuf,
}

const FILE_EXTENSIONS: [&str; 0] = [];
const GIT_NS: &str = "git";

impl Importer for ImportGitMetadata {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        _step_id: crate::StepID,
        _tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let repo = Repository::open(input_path)?;
        let head_sha = repo
            .revparse("HEAD")?
            .from()
            .ok_or(anyhow!("Could not resolve HEAD."))?
            .id();
        let data_path = input_path.join(&self.folder);
        let mut status_options = StatusOptions::new();
        status_options.pathspec(&self.folder);
        status_options.include_untracked(true);
        let repo_status = repo.statuses(Some(&mut status_options))?;
        let critical_status = repo_status
            .iter()
            .filter(|e| e.status() != git2::Status::CURRENT)
            .collect_vec();
        if !critical_status.is_empty() {
            let paths = critical_status
                .iter()
                .filter_map(|e| {
                    if let Some(p) = e.path() {
                        Some(format!("{}\t{:?}", p, e.status()))
                    } else {
                        None
                    }
                })
                .join("\n");

            return Err(anyhow!(
                "The repository sub directory has uncommitted changes and/or untracked files:\n{}",
                paths
            )
            .into());
        };
        let corpus_root = data_path
            .file_name()
            .ok_or(anyhow!("Could not determine root directory of input path."))?
            .to_string_lossy();
        update.add_event(UpdateEvent::AddNode {
            node_name: corpus_root.to_string(),
            node_type: "corpus".to_string(),
        })?;
        update.add_event(UpdateEvent::AddNodeLabel {
            node_name: corpus_root.to_string(),
            anno_ns: GIT_NS.to_string(),
            anno_name: "revision".to_string(),
            anno_value: head_sha.to_string(),
        })?;
        Ok(update)
    }

    fn file_extensions(&self) -> &[&str] {
        &FILE_EXTENSIONS
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use git2::{Commit, IndexAddOption, Oid, Repository, Signature, Time};
    use graphannis::AnnotationGraph;
    use insta::assert_snapshot;
    use tempfile::tempdir;

    use crate::{
        exporter::graphml::GraphMLExporter,
        importer::{git::ImportGitMetadata, Importer},
        test_util::export_to_string,
        StepID,
    };

    #[test]
    fn serialize() {}

    #[test]
    fn deserialize() {}

    #[test]
    fn clean() {
        feature_test(true);
    }

    #[test]
    fn dirty() {
        feature_test(false);
    }

    fn feature_test(commit: bool) {
        let trd = tempdir();
        assert!(trd.is_ok());
        let tmp_repo_dir = trd.unwrap();
        let subfolder = "data";
        let r = Repository::init(tmp_repo_dir.path());
        assert!(r.is_ok());
        let repo = r.unwrap();
        let oid = {
            // even for the dirty case (uncommitted changes),
            // the repository needs a root commit and the subfolder
            assert!(fs::create_dir(tmp_repo_dir.path().join(subfolder)).is_ok());
            let first_file = tmp_repo_dir.path().join(subfolder).join(".gitkeep");
            assert!(fs::write(&first_file, "").is_ok());
            let oid = commit_all(&repo, 0, "root commit", &[]);
            assert!(oid.is_ok());
            oid.unwrap()
        };
        // add a data file in the subfolder
        let file_path = tmp_repo_dir.path().join(subfolder).join("doc.csv");
        assert!(fs::write(&file_path, "").is_ok());
        if commit {
            let parent = repo.find_commit(oid);
            assert!(parent.is_ok());
            let commit_created = commit_all(&repo, 1, "added data file", &[&parent.unwrap()]);
            assert!(commit_created.is_ok(), "Error: {:?}", commit_created.err());
        }
        let gitmeta = ImportGitMetadata {
            folder: Path::new(subfolder).to_path_buf(),
        };
        let u = gitmeta.import_corpus(
            tmp_repo_dir.path(),
            StepID {
                module_name: "test_git".to_string(),
                path: None,
            },
            None,
        );
        assert_eq!(u.is_ok(), commit, "Result: {:?}", u.err());
        if commit {
            let mut update = u.unwrap();
            let g = AnnotationGraph::with_default_graphstorages(true);
            assert!(g.is_ok());
            let mut graph = g.unwrap();
            assert!(graph.apply_update(&mut update, |_| {}).is_ok());
            let exporter: Result<GraphMLExporter, _> = toml::from_str("stable_order = true");
            assert!(exporter.is_ok());
            let actual = export_to_string(&graph, exporter.unwrap());
            assert!(actual.is_ok());
            assert_snapshot!(actual.unwrap());
        } else {
            assert_snapshot!(
                "error_message_uncommitted_changes",
                u.err().unwrap().to_string()
            );
        }
    }

    fn commit_all(
        repo: &Repository,
        time: i64,
        message: &str,
        parents: &[&Commit],
    ) -> Result<Oid, git2::Error> {
        let time = Time::new(time, 0);
        let signature = Signature::new("testlab", "testlab@corpus-tools.org", &time)?;
        let mut index = repo.index()?;
        index.add_all(vec!["*"].iter(), IndexAddOption::DEFAULT, None)?;
        index.write()?;
        let tree_id = index.write_tree()?;
        let tree = repo.find_tree(tree_id)?;
        repo.commit(
            Some("HEAD"),
            &signature,
            &signature,
            message,
            &tree,
            parents,
        )
    }
}
