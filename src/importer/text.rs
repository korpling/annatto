use std::path::PathBuf;

use encoding_rs::Encoding;
use encoding_rs_io::DecodeReaderBytesBuilder;
use facet::Facet;
use graphannis::update::{GraphUpdate, UpdateEvent};
use graphannis_core::graph::ANNIS_NS;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    importer::{
        Importer,
        text::tokenizer::{Token, TreeTaggerTokenizer},
    },
    progress::ProgressReporter,
    util,
};

mod tokenizer;

/// Importer for plain text files.
///
/// Example:
/// ```toml
/// [[import]]
/// format = "text"
/// path = "..."
///
/// [import.config]
/// tokenizer = { strategy = "treetagger", language="fr" }
/// file_encoding = "UTF-8"
/// ```
#[derive(Facet, Deserialize, Serialize, Clone, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct ImportText {
    /// The encoding to use when for the input files. Defaults to UTF-8.
    #[serde(default)]
    file_encoding: Option<String>,
    /// Which tokenizer implementation to use.
    /// In general, this is configured with the name of the `strategy` and
    /// additional configuration values specific to this strategy.
    ///
    /// ```toml
    /// [import.config]
    /// tokenizer = { strategy = "treetagger", language="fr" }
    /// ```
    ///
    /// Currently, only the `treetagger` strategy is available. It imitates the
    /// behavior of the `utf8-tokenize.perl` script from the
    /// [TreeTagger](https://www.cis.uni-muenchen.de/~schmid/tools/TreeTagger/)
    /// and can be configured to use a language specific configuration with the
    /// additional `language` parameter.
    ///
    /// The `language` field is the ISO 639-1 language code and the following languages have specific implementations:
    /// - English (en),
    /// - Romanian (ro),
    /// - Italian (it),
    /// - French (fr),
    /// - Portuguese (pt),
    /// - Galician (gl),
    /// - Catalan (ca)
    ///
    /// The default is a generic language configuration, which works well with German texts.
    #[serde(default)]
    tokenizer: Tokenizer,
}

#[derive(Facet, Deserialize, Serialize, Clone, PartialEq)]
#[repr(u8)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum Tokenizer {
    /// A tokenizer that imitates the behavior of the `utf8-tokenize.perl` of
    /// the
    /// [TreeTagger](https://www.cis.uni-muenchen.de/~schmid/tools/TreeTagger/).
    Treetagger {
        /// ISO 639-1 language code to use for language-specific behavior of the tokenizer.
        /// Leave empty for a generic handling.
        /// Language-specific behavior exists for English, Romanian, Italian, French, Portoguese, Galician and Catalan,
        language: String,
    },
}

impl Default for Tokenizer {
    fn default() -> Self {
        Tokenizer::Treetagger {
            language: "".to_string(),
        }
    }
}

impl Importer for ImportText {
    fn import_corpus(
        &self,
        input_path: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<GraphUpdate, Box<dyn std::error::Error>> {
        let mut update = GraphUpdate::default();

        let all_files = util::graphupdate::import_corpus_graph_from_files(
            &mut update,
            input_path,
            self.file_extensions(),
        )?;

        let tokenizer = match &self.tokenizer {
            Tokenizer::Treetagger { language } => TreeTaggerTokenizer::new(language.into())?,
        };

        // Each file is a work step
        let reporter = ProgressReporter::new(tx, step_id.clone(), all_files.len())?;
        let mapper_vec = all_files
            .into_iter()
            .map(|(p, d)| TextfileMapper {
                progress: &reporter,
                path: p.to_path_buf(),
                doc_node_name: d.to_string(),
                file_encoding: self.file_encoding.clone(),
                tokenizer: tokenizer.clone(),
            })
            .collect_vec();
        mapper_vec
            .into_iter()
            .try_for_each(|m| m.import_textfile(&mut update))?;
        Ok(update)
    }

    fn file_extensions(&self) -> &[&str] {
        &["txt"]
    }
}

struct TextfileMapper<'a> {
    progress: &'a ProgressReporter,
    path: PathBuf,
    doc_node_name: String,
    file_encoding: Option<String>,
    tokenizer: TreeTaggerTokenizer,
}

impl<'a> TextfileMapper<'a> {
    fn import_textfile(&self, update: &mut GraphUpdate) -> anyhow::Result<()> {
        self.progress
            .info(format!("Processing {}", &self.path.to_string_lossy()))?;

        let prefix = format!("{}#", self.doc_node_name);

        let decoder_builder = if let Some(encoding) = &self.file_encoding {
            DecodeReaderBytesBuilder::new()
                .encoding(Encoding::for_label(encoding.as_bytes()))
                .clone()
        } else {
            DecodeReaderBytesBuilder::new()
        };
        let f = std::fs::File::open(&self.path)?;
        let reader = decoder_builder.build(&f);
        let token_strings = self.tokenizer.tokenize(reader)?;

        for (i, t) in token_strings.iter().enumerate() {
            let Token {
                value: t,
                whitespace_after: ws,
            } = t;
            create_token_node(
                update,
                &format!("{}tok{}", prefix, i),
                t,
                ws.as_ref(),
                &self.doc_node_name,
            )?;
        }

        // add the order relations
        for i in 0..(token_strings.len() - 1) {
            update.add_event(UpdateEvent::AddEdge {
                source_node: format!("{}tok{}", prefix, i),
                target_node: format!("{}tok{}", prefix, i + 1),
                layer: ANNIS_NS.to_string(),
                component_type: "Ordering".to_string(),
                component_name: "".to_string(),
            })?;
        }
        Ok(())
    }
}

pub fn create_token_node(
    update: &mut GraphUpdate,
    node_name: &str,
    token_value: &str,
    whitespace_after: Option<&String>,
    document_node: &str,
) -> anyhow::Result<()> {
    update.add_event(UpdateEvent::AddNode {
        node_name: node_name.to_string(),
        node_type: "node".to_string(),
    })?;
    update.add_event(UpdateEvent::AddNodeLabel {
        node_name: node_name.to_string(),
        anno_ns: ANNIS_NS.to_string(),
        anno_name: "tok".to_string(),
        anno_value: token_value.to_string(),
    })?;

    if let Some(ws) = whitespace_after {
        update.add_event(UpdateEvent::AddNodeLabel {
            node_name: node_name.to_string(),
            anno_ns: ANNIS_NS.to_string(),
            anno_name: "tok-whitespace-after".to_string(),
            anno_value: ws.to_string(),
        })?;
    }

    // add the token node to the document
    update.add_event(UpdateEvent::AddEdge {
        source_node: node_name.to_string(),
        target_node: document_node.to_string(),
        layer: ANNIS_NS.to_string(),
        component_type: "PartOf".to_string(),
        component_name: "".to_string(),
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use graphannis::AnnotationGraph;
    use insta::assert_snapshot;

    use crate::{
        exporter::graphml::GraphMLExporter,
        importer::{Importer, text::ImportText},
        test_util::export_to_string,
        util::update_graph_silent,
    };

    #[test]
    fn import_text_with_default_tokenizer() {
        let import_path = Path::new("tests/data/import/text/example/");
        let importer: ImportText = toml::from_str("").unwrap();
        let u = importer.import_corpus(
            import_path,
            crate::StepID {
                module_name: "test_text".to_string(),
                path: Some(import_path.to_path_buf()),
            },
            None,
        );
        assert!(u.is_ok(), "Err: {:?}", u.err());
        let mut update = u.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(update_graph_silent(&mut graph, &mut update).is_ok());
        let exporter: GraphMLExporter = toml::from_str("stable_order = true").unwrap();
        let actual = export_to_string(&graph, exporter);
        assert!(actual.is_ok());
        assert_snapshot!(actual.unwrap());
    }
    #[test]
    fn import_text_with_french_tokenizer() {
        let import_path = Path::new("tests/data/import/text/example_french/");
        let importer: ImportText =
            toml::from_str(r#"tokenizer = {strategy = "treetagger", language="fr"}"#).unwrap();
        let u = importer.import_corpus(
            import_path,
            crate::StepID {
                module_name: "test_text".to_string(),
                path: Some(import_path.to_path_buf()),
            },
            None,
        );
        assert!(u.is_ok(), "Err: {:?}", u.err());
        let mut update = u.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(update_graph_silent(&mut graph, &mut update).is_ok());
        let exporter: GraphMLExporter = toml::from_str("stable_order = true").unwrap();
        let actual = export_to_string(&graph, exporter);
        assert!(actual.is_ok());
        assert_snapshot!(actual.unwrap());
    }
}
