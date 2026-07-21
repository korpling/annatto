use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use anyhow::{Result, anyhow, bail};
use regex::regex;
use roxmltree::{Document, ParsingOptions};

/// Represents the PAULA XML files in a directory.
#[derive(Default)]
pub(super) struct PaulaDirectory {
    /// Maps a file with the path to the raw XML content
    xml_content_by_path: HashMap<PathBuf, String>,
}

/// Represents the parsed XML files of a PAULA document.
/// Must be used in conjunction with [`PaulaDirectory`].
pub(super) struct PaulaDocument<'input> {
    /// Maps a file with the given PAULA ID to the parsed XML content
    pub(super) xml_document_by_id: HashMap<String, Document<'input>>,
    /// Maps a file name to the PAULA ID
    pub(super) id_by_filename: HashMap<String, String>,
}

impl PaulaDirectory {
    pub fn open_directory<'a, P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut result = PaulaDirectory::default();
        // List all XML files in the directory
        for entry in std::fs::read_dir(&path)? {
            let entry = entry?;
            if entry.file_type()?.is_file()
                && let Some(ext) = entry.path().extension()
                && (ext == "paula" || ext == "xml")
            {
                let content = std::fs::read_to_string(entry.path())?;
                result.xml_content_by_path.insert(entry.path(), content);
            }
        }

        Ok(result)
    }
}

impl<'input> PaulaDocument<'input> {
    pub fn from_directory(dir: &'input PaulaDirectory) -> Result<Self> {
        let mut result = PaulaDocument {
            xml_document_by_id: HashMap::new(),
            id_by_filename: HashMap::new(),
        };
        for (path, content) in &dir.xml_content_by_path {
            let mut parsing_options = ParsingOptions::default();
            parsing_options.allow_dtd = true;

            let doc = Document::parse_with_options(content, parsing_options)?;
            // Get the Paula ID from the header element
            let header = doc
                .root_element()
                .children()
                .filter(|n| n.tag_name().name() == "header")
                .next();
            let paula_id = if let Some(header) = header
                && let Some(attr) = header.attribute("paula_id")
            {
                attr.to_string()
            } else if let Some(file_stem) = path.file_stem() {
                // Use the file name as backup
                file_stem.to_string_lossy().to_string()
            } else {
                bail!("Invalid PAULA XML: no paula_id attribute in element <header>");
            };
            result.xml_document_by_id.insert(paula_id.clone(), doc);
            if let Some(file_name) = path.file_name() {
                result
                    .id_by_filename
                    .insert(file_name.to_string_lossy().to_string(), paula_id);
            }
        }
        Ok(result)
    }

    pub fn by_paula_id(&self, id: &str) -> Option<&Document<'input>> {
        self.xml_document_by_id.get(id)
    }

    /// Get an XML document by its file name. This should have the same name as
    /// the Paula ID, but this is only a recommendation by the PaulaXML spec.
    pub fn by_file_name(&self, file_name: &str) -> Option<&Document<'input>> {
        self.id_by_filename
            .get(file_name)
            .and_then(|id| self.xml_document_by_id.get(id))
    }

    pub fn resolve_xpointer(&self, base_uri: &str, href: &str) -> Result<Option<String>> {
        let base_doc = self
            .by_file_name(base_uri)
            .ok_or_else(|| anyhow!("No such file for PAULA XML document"))?;
        if let Some(func_def) = regex!(r"#xpointer\([^)]*\)")
            .captures(href)
            .and_then(|c| c.get(1))
            && let Some(c) = regex!(r"([^()]+)\([^)]*\)").captures(func_def.as_str())
            && let Some(func_name) = c.get(1)
            && let Some(func_body) = c.get(2)
        {}
        Ok(None)
    }

    /// Get all documents that have the given type in the header
    pub fn by_header_type(&self, paula_type: &str) -> Vec<&Document<'input>> {
        self.xml_document_by_id
            .values()
            .filter(|d| {
                d.root_element().children().any(|n| {
                    n.is_element()
                        && n.has_tag_name("header")
                        && n.attribute("type")
                            .is_some_and(|v| v.to_lowercase() == paula_type)
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paula_documents_by_header_type() {
        let paula_dir =
            PaulaDirectory::open_directory("tests/data/import/paulaxml/rootCorpus/subCorpus1/doc1")
                .unwrap();
        let paula_doc = PaulaDocument::from_directory(&paula_dir).unwrap();

        let texts = paula_doc.by_header_type("text");
        assert_eq!(1, texts.len());
        assert_eq!(
            "doc1.text",
            texts[0]
                .root_element()
                .children()
                .filter(|n| n.has_tag_name("header"))
                .next()
                .unwrap()
                .attribute("paula_id")
                .unwrap()
        );
    }

    #[test]
    fn paula_documents_by_id() {
        let paula_dir =
            PaulaDirectory::open_directory("tests/data/import/paulaxml/rootCorpus/subCorpus1/doc1")
                .unwrap();
        let paula_doc = PaulaDocument::from_directory(&paula_dir).unwrap();

        let result = paula_doc.by_paula_id("doc1.tok_lemma");
        assert_eq!(true, result.is_some());
    }

    #[test]
    fn paula_documents_by_filename() {
        let paula_dir =
            PaulaDirectory::open_directory("tests/data/import/paulaxml/rootCorpus/subCorpus1/doc1")
                .unwrap();
        let paula_doc = PaulaDocument::from_directory(&paula_dir).unwrap();

        let result = paula_doc.by_file_name("morphology.doc1.tok_lemma.xml");
        assert_eq!(true, result.is_some());
    }

    #[test]
    fn resolve_xpointer_string_range() {
        let paula_dir =
            PaulaDirectory::open_directory("tests/data/import/paulaxml/rootCorpus/subCorpus1/doc1")
                .unwrap();
        let paula_doc = PaulaDocument::from_directory(&paula_dir).unwrap();
        let resolved = paula_doc
            .resolve_xpointer("doc1.tok.xml", "#xpointer(string-range(//body,'',39,2))")
            .unwrap()
            .unwrap();
        assert_eq!("it", resolved);
    }
}
