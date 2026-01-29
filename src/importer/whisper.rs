use std::{collections::BTreeMap, fs, path::Path};

use anyhow::anyhow;
use facet::Facet;
use graphannis::{
    model::AnnotationComponentType,
    update::{GraphUpdate, UpdateEvent},
};
use graphannis_core::graph::ANNIS_NS;
use serde::{Deserialize, Serialize};

use crate::{progress::ProgressReporter, util::graphupdate::import_corpus_graph_from_files};

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
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<graphannis::update::GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();
        let paths_and_node_names =
            import_corpus_graph_from_files(&mut update, input_path, self.file_extensions())?;
        let progress =
            ProgressReporter::new(tx.clone(), step_id.clone(), paths_and_node_names.len())?;
        for (pathbuf, doc_node_name) in paths_and_node_names {
            self.import_document(&mut update, pathbuf.as_path(), &doc_node_name)?;
            progress.worked(1)?;
        }
        Ok(update)
    }

    fn file_extensions(&self) -> &[&str] {
        &FILE_EXTENSIONS
    }
}

const WHISPER_NS: &str = "whisper";

impl ImportWhisper {
    fn import_document(
        &self,
        update: &mut GraphUpdate,
        path: &Path,
        node_name: &str,
    ) -> Result<(), anyhow::Error> {
        let data = load_json(path)?;
        update.add_event(UpdateEvent::AddNodeLabel {
            node_name: node_name.to_string(),
            anno_ns: WHISPER_NS.to_string(),
            anno_name: "language".to_string(),
            anno_value: data.language.to_string(),
        })?;
        let ds = format!("{node_name}#datasource");
        update.add_event(UpdateEvent::AddNode {
            node_name: ds.to_string(),
            node_type: "datasource".to_string(),
        })?;
        if let Some(text) = &data.text {
            update.add_event(UpdateEvent::AddNodeLabel {
                node_name: ds.to_string(),
                anno_ns: WHISPER_NS.to_string(),
                anno_name: "text".to_string(),
                anno_value: text.trim().to_string(),
            })?;
        }
        update.add_event(UpdateEvent::AddEdge {
            source_node: ds,
            target_node: node_name.to_string(),
            layer: ANNIS_NS.to_string(),
            component_type: AnnotationComponentType::PartOf.to_string(),
            component_name: "".to_string(),
        })?;
        if self.skip_tokens {
            self.import_segments_only(update, data, node_name)
        } else {
            self.import_with_tokens(update, data, node_name)
        }
    }

    fn import_with_tokens(
        &self,
        update: &mut GraphUpdate,
        data: WhisperJSON,
        node_name: &str,
    ) -> Result<(), anyhow::Error> {
        let vocabulary = load_vocabulary()?;
        for (s, segment) in data.segments.iter().enumerate() {
            let span = format!("{node_name}#s{s}");
            update.add_event(UpdateEvent::AddNode {
                node_name: span.to_string(),
                node_type: "node".to_string(),
            })?;
            update.add_event(UpdateEvent::AddNodeLabel {
                node_name: span.to_string(),
                anno_ns: WHISPER_NS.to_string(),
                anno_name: "segment".to_string(),
                anno_value: segment.text.trim().to_string(),
            })?;
            if let Some(id_val) = &segment.id {
                update.add_event(UpdateEvent::AddNodeLabel {
                    node_name: span.to_string(),
                    anno_ns: WHISPER_NS.to_string(),
                    anno_name: "segment_id".to_string(),
                    anno_value: id_val.to_string(),
                })?;
            }
            if let Some(seek) = &segment.seek {
                update.add_event(UpdateEvent::AddNodeLabel {
                    node_name: span.to_string(),
                    anno_ns: WHISPER_NS.to_string(),
                    anno_name: "seek".to_string(),
                    anno_value: seek.to_string(),
                })?;
            }
            update.add_event(UpdateEvent::AddNodeLabel {
                node_name: span.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "time".to_string(),
                anno_value: format!("{}-{}", segment.start, segment.end),
            })?;
            if let Some(temperature) = &segment.temperature {
                update.add_event(UpdateEvent::AddNodeLabel {
                    node_name: span.to_string(),
                    anno_ns: WHISPER_NS.to_string(),
                    anno_name: "temperature".to_string(),
                    anno_value: temperature.to_string(),
                })?;
            }
            if let Some(avg_logprob) = &segment.avg_logprob {
                update.add_event(UpdateEvent::AddNodeLabel {
                    node_name: span.to_string(),
                    anno_ns: WHISPER_NS.to_string(),
                    anno_name: "avg_logprob".to_string(),
                    anno_value: avg_logprob.to_string(),
                })?;
            }
            if let Some(compression_ratio) = segment.compression_ratio {
                update.add_event(UpdateEvent::AddNodeLabel {
                    node_name: span.to_string(),
                    anno_ns: WHISPER_NS.to_string(),
                    anno_name: "compression_ratio".to_string(),
                    anno_value: compression_ratio.to_string(),
                })?;
            }
            if let Some(no_speech_prob) = &segment.no_speech_prob {
                update.add_event(UpdateEvent::AddNodeLabel {
                    node_name: span.to_string(),
                    anno_ns: WHISPER_NS.to_string(),
                    anno_name: "no_speech_prob".to_string(),
                    anno_value: no_speech_prob.to_string(),
                })?;
            }
            update.add_event(UpdateEvent::AddEdge {
                source_node: span.to_string(),
                target_node: node_name.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;
            if let Some(tokens) = &segment.tokens {
                for (t, token) in tokens.iter().enumerate() {
                    let tok_name = format!("{node_name}#t{s}-{t}");
                    update.add_event(UpdateEvent::AddNode {
                        node_name: tok_name.to_string(),
                        node_type: "node".to_string(),
                    })?;
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: tok_name.to_string(),
                        anno_ns: ANNIS_NS.to_string(),
                        anno_name: "layer".to_string(),
                        anno_value: "default_layer".to_string(),
                    })?;
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
                                update.add_event(UpdateEvent::AddNodeLabel {
                                    node_name: tok_name.to_string(),
                                    anno_ns: ANNIS_NS.to_string(),
                                    anno_name: "time".to_string(),
                                    anno_value: format!("{start_val}-{end_val}"),
                                })?;
                            }
                            if let Some(score_val) = score {
                                update.add_event(UpdateEvent::AddNodeLabel {
                                    node_name: tok_name.to_string(),
                                    anno_ns: WHISPER_NS.to_string(),
                                    anno_name: "score".to_string(),
                                    anno_value: score_val.to_string(),
                                })?;
                            }
                            word.to_string()
                        }
                    };
                    update.add_event(UpdateEvent::AddNodeLabel {
                        node_name: tok_name.to_string(),
                        anno_ns: ANNIS_NS.to_string(),
                        anno_name: "tok".to_string(),
                        anno_value: text_value.to_string(),
                    })?;
                    update.add_event(UpdateEvent::AddEdge {
                        source_node: span.to_string(),
                        target_node: tok_name.to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::Coverage.to_string(),
                        component_name: "".to_string(),
                    })?;
                    if t > 0 {
                        update.add_event(UpdateEvent::AddEdge {
                            source_node: format!("{node_name}#t{s}-{}", t - 1),
                            target_node: tok_name.to_string(),
                            layer: ANNIS_NS.to_string(),
                            component_type: AnnotationComponentType::Ordering.to_string(),
                            component_name: "".to_string(),
                        })?;
                    }
                    update.add_event(UpdateEvent::AddEdge {
                        source_node: tok_name.to_string(),
                        target_node: node_name.to_string(),
                        layer: ANNIS_NS.to_string(),
                        component_type: AnnotationComponentType::PartOf.to_string(),
                        component_name: "".to_string(),
                    })?;
                }
            }
        }
        Ok(())
    }

    fn import_segments_only(
        &self,
        update: &mut GraphUpdate,
        data: WhisperJSON,
        node_name: &str,
    ) -> Result<(), anyhow::Error> {
        for (s, segment) in data.segments.iter().enumerate() {
            let span = format!("{node_name}#s{s}");
            update.add_event(UpdateEvent::AddNode {
                node_name: span.to_string(),
                node_type: "node".to_string(),
            })?;
            update.add_event(UpdateEvent::AddNodeLabel {
                node_name: span.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "tok".to_string(),
                anno_value: segment.text.trim().to_string(),
            })?;
            update.add_event(UpdateEvent::AddNodeLabel {
                node_name: span.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "layer".to_string(),
                anno_value: "default_layer".to_string(),
            })?;
            update.add_event(UpdateEvent::AddNodeLabel {
                node_name: span.to_string(),
                anno_ns: ANNIS_NS.to_string(),
                anno_name: "time".to_string(),
                anno_value: format!("{}-{}", segment.start, segment.end),
            })?;
            update.add_event(UpdateEvent::AddEdge {
                source_node: span.to_string(),
                target_node: node_name.to_string(),
                layer: ANNIS_NS.to_string(),
                component_type: AnnotationComponentType::PartOf.to_string(),
                component_name: "".to_string(),
            })?;
            if s > 0 {
                update.add_event(UpdateEvent::AddEdge {
                    source_node: format!("{node_name}#s{}", s - 1),
                    target_node: span.to_string(),
                    layer: ANNIS_NS.to_string(),
                    component_type: AnnotationComponentType::Ordering.to_string(),
                    component_name: "".to_string(),
                })?;
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
        exporter::graphml::GraphMLExporter, importer::Importer, test_util::export_to_string,
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
        let mut u = module
            .import_corpus(
                path,
                crate::StepID {
                    module_name: "test_whisper".to_string(),
                    path: Some(path.to_path_buf()),
                },
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
