use std::{collections::BTreeMap, fs, path::Path};

use anyhow::anyhow;
use facet::Facet;
use graphannis_core::graph::ANNIS_NS;
use serde::{Deserialize, Serialize};

use crate::{
    importer::{DefaultImportConfiguration, GenericImportConfiguration},
    progress::ProgressReporter,
    util::graphupdate::UpdateWriter,
};

use super::Importer;

/// This module imports OpenAI's whisper json format.
///
/// Example:
/// ```toml
/// [[import]]
/// format = "whisper"
/// path = "..."
///
/// [import.config]
/// skip_tokens = true
/// ```
#[derive(Facet, Default, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ImportWhisper {
    /// With this attribute the tokenization in the output will not be imported,
    /// instead the full text of each segment will serve as a token.
    #[serde(default)]
    skip_tokens: bool,
}

#[derive(Deserialize)]
struct WhisperJSON {
    text: Option<String>,
    segments: Vec<WhisperSegment>,
    language: String,
}

#[derive(Deserialize)]
struct WhisperSegment {
    id: Option<usize>,
    seek: Option<usize>,
    start: f64,
    end: f64,
    text: String,
    #[serde(alias = "words")]
    tokens: Option<Vec<WhisperToken>>,
    temperature: Option<f64>,
    avg_logprob: Option<f64>,
    compression_ratio: Option<f64>,
    no_speech_prob: Option<f64>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum WhisperToken {
    Id(usize),
    Word {
        word: String,
        start: Option<f64>,
        end: Option<f64>,
        score: Option<f64>,
    },
}

const FILE_EXTENSIONS: [&str; 1] = ["json"];

impl Importer for ImportWhisper {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        step_id: crate::StepID,
        config: GenericImportConfiguration,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = UpdateWriter::from(config);
        let paths_and_node_names = update.write_corpus_graph_from_file_tree(input_path)?;
        let progress =
            ProgressReporter::new(tx.clone(), step_id.clone(), paths_and_node_names.len())?;
        for (pathbuf, doc_node_name) in paths_and_node_names {
            self.import_document(&mut update, pathbuf.as_path(), &doc_node_name)?;
            progress.worked(1)?;
        }
        Ok(update.into())
    }
}

impl DefaultImportConfiguration for ImportWhisper {
    fn default_file_extensions(&self) -> &[&str] {
        &FILE_EXTENSIONS
    }

    fn preset_default_namespace(&self) -> Option<&str> {
        Some(WHISPER_NS)
    }
}

const WHISPER_NS: &str = "whisper";

impl ImportWhisper {
    fn import_document(
        &self,
        update: &mut UpdateWriter,
        path: &Path,
        node_name: &str,
    ) -> Result<(), anyhow::Error> {
        let data = load_json(path)?;
        update.node_label(
            node_name.to_string(),
            update.config.default_namespace().to_string(),
            "language".to_string(),
            data.language.to_string(),
        )?;
        let ds = format!("{node_name}#datasource");
        update.node(ds.to_string(), "datasource".to_string())?;
        if let Some(text) = &data.text {
            update.node_label(
                ds.to_string(),
                update.config.default_namespace().to_string(),
                "text".to_string(),
                text.trim().to_string(),
            )?;
        }
        update.part_of(ds, node_name.to_string())?;
        if self.skip_tokens {
            self.import_segments_only(update, data, node_name)
        } else {
            self.import_with_tokens(update, data, node_name)
        }
    }

    fn import_with_tokens(
        &self,
        update: &mut UpdateWriter,
        data: WhisperJSON,
        node_name: &str,
    ) -> Result<(), anyhow::Error> {
        let vocabulary = load_vocabulary()?;
        for (s, segment) in data.segments.iter().enumerate() {
            let span = format!("{node_name}#s{s}");
            update.labeled_node(
                span.to_string(),
                "node".to_string(),
                update.config.default_namespace().to_string(),
                "segment".to_string(),
                segment.text.trim().to_string(),
            )?;
            if let Some(id_val) = &segment.id {
                update.node_label(
                    span.to_string(),
                    update.config.default_namespace().to_string(),
                    "segment_id".to_string(),
                    id_val.to_string(),
                )?;
            }
            if let Some(seek) = &segment.seek {
                update.node_label(
                    span.to_string(),
                    update.config.default_namespace().to_string(),
                    "seek".to_string(),
                    seek.to_string(),
                )?;
            }
            update.node_label(
                span.to_string(),
                ANNIS_NS.to_string(),
                "time".to_string(),
                format!("{}-{}", segment.start, segment.end),
            )?;
            if let Some(temperature) = &segment.temperature {
                update.node_label(
                    span.to_string(),
                    update.config.default_namespace().to_string(),
                    "temperature".to_string(),
                    temperature.to_string(),
                )?;
            }
            if let Some(avg_logprob) = &segment.avg_logprob {
                update.node_label(
                    span.to_string(),
                    update.config.default_namespace().to_string(),
                    "avg_logprob".to_string(),
                    avg_logprob.to_string(),
                )?;
            }
            if let Some(compression_ratio) = segment.compression_ratio {
                update.node_label(
                    span.to_string(),
                    update.config.default_namespace().to_string(),
                    "compression_ratio".to_string(),
                    compression_ratio.to_string(),
                )?;
            }
            if let Some(no_speech_prob) = &segment.no_speech_prob {
                update.node_label(
                    span.to_string(),
                    update.config.default_namespace().to_string(),
                    "no_speech_prob".to_string(),
                    no_speech_prob.to_string(),
                )?;
            }
            update.part_of(span.to_string(), node_name.to_string())?;
            if let Some(tokens) = &segment.tokens {
                for (t, token) in tokens.iter().enumerate() {
                    let tok_name = format!("{node_name}#t{s}-{t}");
                    update.labeled_node(
                        tok_name.to_string(),
                        "node".to_string(),
                        ANNIS_NS.to_string(),
                        "layer".to_string(),
                        "default_layer".to_string(),
                    )?;
                    let text_value = match token {
                        WhisperToken::Id(token_index) => {
                            if let Some(w) = vocabulary.get(token_index) {
                                w.to_string() // remove token delimiter
                            } else {
                                "<UNK>".to_string()
                            }
                        }
                        WhisperToken::Word {
                            word,
                            start,
                            end,
                            score,
                        } => {
                            if let Some(start_val) = start
                                && let Some(end_val) = end
                            {
                                update.node_label(
                                    tok_name.to_string(),
                                    ANNIS_NS.to_string(),
                                    "time".to_string(),
                                    format!("{start_val}-{end_val}"),
                                )?;
                            }
                            if let Some(score_val) = score {
                                update.node_label(
                                    tok_name.to_string(),
                                    update.config.default_namespace().to_string(),
                                    "score".to_string(),
                                    score_val.to_string(),
                                )?;
                            }
                            word.to_string()
                        }
                    };
                    update.node_label(
                        tok_name.to_string(),
                        ANNIS_NS.to_string(),
                        "tok".to_string(),
                        text_value.to_string(),
                    )?;
                    update.coverage(span.to_string(), tok_name.to_string())?;
                    if t > 0 {
                        update.ordering(
                            format!("{node_name}#t{s}-{}", t - 1),
                            tok_name.to_string(),
                        )?;
                    }
                    update.part_of(tok_name.to_string(), node_name.to_string())?;
                }
            }
        }
        Ok(())
    }

    fn import_segments_only(
        &self,
        update: &mut UpdateWriter,
        data: WhisperJSON,
        node_name: &str,
    ) -> Result<(), anyhow::Error> {
        for (s, segment) in data.segments.iter().enumerate() {
            let span = format!("{node_name}#s{s}");
            update.labeled_node(
                span.to_string(),
                "node".to_string(),
                ANNIS_NS.to_string(),
                "tok".to_string(),
                segment.text.trim().to_string(),
            )?;
            update.node_label(
                span.to_string(),
                ANNIS_NS.to_string(),
                "layer".to_string(),
                "default_layer".to_string(),
            )?;
            update.node_label(
                span.to_string(),
                ANNIS_NS.to_string(),
                "time".to_string(),
                format!("{}-{}", segment.start, segment.end),
            )?;
            update.part_of(span.to_string(), node_name.to_string())?;
            if s > 0 {
                update.ordering(format!("{node_name}#s{}", s - 1), span.to_string())?;
            }
        }
        Ok(())
    }
}

fn load_json(path: &Path) -> Result<WhisperJSON, anyhow::Error> {
    let data = fs::read_to_string(path)?;
    serde_json::from_str(&data).map_err(|e| anyhow!("Could not parse json file: {:?}", e))
}

const VOCAB_DATA: &[u8] = include_bytes!("whisper/vocab.json");

fn load_vocabulary() -> Result<BTreeMap<usize, String>, anyhow::Error> {
    serde_json::from_slice(VOCAB_DATA)
        .map_err(|e| anyhow!("Could not read vocabulary file: {:?}", e))
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use graphannis::AnnotationGraph;
    use insta::assert_snapshot;

    use crate::{
        exporter::graphml::GraphMLExporter,
        importer::{DefaultImportConfiguration, Importer},
        test_util::export_to_string,
    };

    use super::ImportWhisper;

    #[test]
    fn serialize() {
        let module = ImportWhisper::default();
        let serialization = toml::to_string(&module);
        assert!(
            serialization.is_ok(),
            "Serialization failed: {:?}",
            serialization.err()
        );
        assert_snapshot!(serialization.unwrap());
    }

    #[test]
    fn serialize_custom() {
        let module = ImportWhisper { skip_tokens: true };
        let serialization = toml::to_string(&module);
        assert!(
            serialization.is_ok(),
            "Serialization failed: {:?}",
            serialization.err()
        );
        assert_snapshot!(serialization.unwrap());
    }

    #[test]
    fn segments_only() {
        let actual = run_test("skip_tokens = true");
        assert!(actual.is_ok(), "An error occured: {:?}", actual.err());
        assert_snapshot!(actual.unwrap());
    }

    #[test]
    fn with_tokens() {
        let actual = run_test("");
        assert!(actual.is_ok(), "An error occured: {:?}", actual.err());
        assert_snapshot!(actual.unwrap());
    }

    fn run_test(serialization: &str) -> Result<String, anyhow::Error> {
        let module: ImportWhisper = toml::from_str(serialization)?;
        let path = std::path::Path::new("./tests/data/import/whisper/whisper/");
        let config = module.default_configuration();
        let mut u = module
            .import_corpus(
                path,
                crate::StepID {
                    module_name: "test_whisper".to_string(),
                    path: Some(path.to_path_buf()),
                },
                config,
                None,
            )
            .map_err(|e| anyhow!("An error occured: {:?}", e))?;
        let mut g = AnnotationGraph::with_default_graphstorages(true)?;
        g.apply_update(&mut u, |_| {})?;
        let actual = export_to_string(
            &g,
            toml::from_str::<GraphMLExporter>("stable_order = true")?,
        )?;
        Ok(actual)
    }

    #[test]
    fn flexibility() {
        let module = ImportWhisper { skip_tokens: false };
        let path = std::path::Path::new("./tests/data/import/whisper/flexibility/");
        let u = module
            .import_corpus(
                path,
                crate::StepID {
                    module_name: "test_whisper".to_string(),
                    path: Some(path.to_path_buf()),
                },
                module.default_configuration(),
                None,
            )
            .map_err(|e| anyhow!("An error occured: {:?}", e));
        assert!(u.is_ok());
        let mut g = AnnotationGraph::with_default_graphstorages(true).unwrap();
        assert!(g.apply_update(&mut u.unwrap(), |_| {}).is_ok());
        let actual = export_to_string(
            &g,
            toml::from_str::<GraphMLExporter>("stable_order = true").unwrap(),
        );
        assert_snapshot!(actual.unwrap());
    }
}
