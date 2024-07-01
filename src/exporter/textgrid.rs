use std::{collections::BTreeMap, fs, io::Write, path::PathBuf};

use anyhow::{anyhow, bail};
use documented::{Documented, DocumentedFields};
use graphannis::{
    graph::{AnnoKey, NodeID},
    model::{AnnotationComponent, AnnotationComponentType},
    AnnotationGraph,
};
use graphannis_core::{annostorage::ValueSearch, graph::ANNIS_NS, util::join_qname};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use serde::Deserialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use crate::progress::ProgressReporter;

use super::Exporter;

/// This exports annotation graphs to PRAAT TextGrids. Use is as follows:
/// ```toml
/// [[export]]
/// format = "textgrid"
/// path = "your/target/path"
///
/// [export.config]
/// file_key = { ns = "my_namespace", name = "my_file_name_anno_name" }
/// time_key = { ns = "another_namespace", name = "the_name_of_time_values" }
/// point_tiers = [ { ns = "phonetic", "name" = "boundary_tone" } ]
/// remove_ns = true
///
/// ```
#[derive(Deserialize, Documented, DocumentedFields, FieldNamesAsSlice)]
#[serde(deny_unknown_fields)]
pub struct ExportTextGrid {
    /// This anno key determines which nodes in the part of subgraph bundle all contents for a file.
    /// Example:
    /// ```toml
    /// [export.config]
    /// file_key = { ns = "annis", name = "doc" }  # this is the default and can be omitted
    /// ``````
    #[serde(default = "default_file_key")]
    file_key: AnnoKey,
    /// This anno key is used to determine the time values.
    /// Example:
    /// ```toml
    /// [export.config]
    /// time_key = { ns = "annis", key = "time" }  # this is the default and can be omitted
    /// ```
    #[serde(default = "default_time_key")]
    time_key: AnnoKey,
    /// The annotation keys provided here will be exported as point tiers. The ones that are not mentioned will be exported as interval tiers.
    /// Example:
    /// ```toml
    /// [export.config]
    /// point_tiers = [
    ///   {ns = "phonetics", name = "pitch_accent"},
    ///   {ns = "phonetics", name = "boundary_tone"}
    /// ]
    /// ```
    #[serde(default)]
    point_tiers: Vec<AnnoKey>,
    /// This attribute configures whether or not to keep the namespace in tier names. If `true`, the namespace will not be exported.
    /// Only set this to `true` if you know that an unqualified annotation name is not used for more than one annotation layer.
    /// If used incorrectly, more than one layer could be merged into a single tier.
    /// Example:
    /// ```toml
    /// [export.config]
    /// remove_ns = "true"
    /// ```
    #[serde(default)]
    remove_ns: bool,
}

fn default_file_key() -> AnnoKey {
    AnnoKey {
        name: "doc".into(),
        ns: ANNIS_NS.into(),
    }
}

fn default_time_key() -> AnnoKey {
    AnnoKey {
        name: "time".into(),
        ns: ANNIS_NS.into(),
    }
}

const FILE_EXTENSION: &str = "TextGrid";

impl Exporter for ExportTextGrid {
    fn export_corpus(
        &self,
        graph: &graphannis::AnnotationGraph,
        output_path: &std::path::Path,
        step_id: crate::StepID,
        tx: Option<crate::workflow::StatusSender>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let file_nodes = graph
            .get_node_annos()
            .exact_anno_search(
                if self.file_key.ns.is_empty() {
                    None
                } else {
                    Some(self.file_key.ns.as_str())
                },
                self.file_key.name.as_str(),
                ValueSearch::Any,
            )
            .flatten()
            .collect_vec();
        let part_of_storage = if let Some(storage) = graph.get_graphstorage(
            &AnnotationComponent::new(AnnotationComponentType::PartOf, ANNIS_NS.into(), "".into()),
        ) {
            storage
        } else {
            return Err(anyhow!("Could not obtain part of storage.").into());
        };
        let progress = ProgressReporter::new(tx, step_id, file_nodes.len())?;
        for mtch in file_nodes {
            if let Some(file_name) = graph
                .get_node_annos()
                .get_value_for_item(&mtch.node, &mtch.anno_key)?
            {
                let nodes_in_subgraph = part_of_storage
                    .find_connected_inverse(mtch.node, 0, std::ops::Bound::Unbounded)
                    .flatten();
                let path = output_path.join(format!("{file_name}.{}", self.file_extension()));
                self.export_document(graph, nodes_in_subgraph, path)?;
            } else {
                return Err(anyhow!(
                    "Could not determine file name from annotations with file key {:?}.",
                    &mtch.anno_key
                )
                .into());
            }
            progress.worked(1)?;
        }
        Ok(())
    }

    fn file_extension(&self) -> &str {
        FILE_EXTENSION
    }
}

fn parse_time_tuple(
    value: &str,
    delimiter: &str,
) -> Result<(OrderedFloat<f64>, OrderedFloat<f64>), Box<dyn std::error::Error>> {
    if let Some((start, end)) = value.split_once(delimiter) {
        Ok((start.parse()?, end.parse()?))
    } else {
        Err(anyhow!("Could not parse time values from input {value}").into())
    }
}

impl ExportTextGrid {
    fn export_document<I: Iterator<Item = NodeID>>(
        &self,
        graph: &AnnotationGraph,
        nodes: I,
        path: PathBuf,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // gather data
        let mut tier_data: BTreeMap<AnnoKey, Vec<(OrderedFloat<f64>, OrderedFloat<f64>, String)>> =
            BTreeMap::default();
        let node_annos = graph.get_node_annos();
        let mut xmin: OrderedFloat<f64> = f64::MAX.into();
        let mut xmax: OrderedFloat<f64> = 0f64.into();
        for node in nodes {
            // for now only export nodes with time values (TODO extend by following coverage until times are found)
            if let Some(value) = node_annos.get_value_for_item(&node, &self.time_key)? {
                let (start, end) = parse_time_tuple(&value, "-")?; // TODO make configurable
                xmin = xmin.min(start);
                xmax = xmax.max(end);
                for annotation in node_annos.get_annotations_for_item(&node)? {
                    if annotation.key.ns == ANNIS_NS {
                        continue;
                    }
                    let anno_val = annotation.val.to_string();
                    let tuple = (start, end, anno_val);
                    match tier_data.entry(annotation.key) {
                        std::collections::btree_map::Entry::Vacant(e) => {
                            e.insert(vec![tuple]);
                        }
                        std::collections::btree_map::Entry::Occupied(mut e) => {
                            e.get_mut().push(tuple);
                        }
                    }
                }
            }
        }
        let mut textgrid_tiers = Vec::with_capacity(tier_data.len());
        for (key, mut tuples) in tier_data {
            tuples.sort();
            let is_point_tier = self.point_tiers.contains(&key);
            let mut entries = Vec::with_capacity(tuples.len());
            for (start, end, value) in tuples {
                let entry: TierEntry = if is_point_tier {
                    (start, value).into()
                } else {
                    (start, end, value).into()
                };
                entries.push(entry);
            }
            let tier_name = if self.remove_ns {
                key.name.to_string()
            } else {
                join_qname(&key.ns, &key.name)
            };
            textgrid_tiers.push(Tier {
                name: tier_name,
                entries,
            });
        }
        let textgrid = TextGrid {
            xmin: xmin.into_inner(),
            xmax: xmax.into_inner(),
            items: textgrid_tiers,
        };
        let writer = TextGridWriter::new(path)?;
        writer.write(textgrid)?;
        Ok(())
    }
}

#[derive(Debug)]
struct TextGrid {
    xmin: f64,
    xmax: f64,
    items: Vec<Tier>,
}

struct TextGridWriter {
    file: fs::File,
}

impl TextGridWriter {
    const LF_BYTES: &'static [u8] = "\n".as_bytes();
    const INDENT_BYTES: &'static [u8] = "    ".as_bytes();
    const HEADER_LINES: [&'static [u8]; 2] = [
        "File type = \"ooTextFile\"".as_bytes(),
        "Object class = \"TextGrid\"".as_bytes(),
    ];

    fn new(path: PathBuf) -> Result<Self, anyhow::Error> {
        Ok(TextGridWriter {
            file: fs::File::create(path)?,
        })
    }

    fn write(mut self, textgrid: TextGrid) -> Result<(), anyhow::Error> {
        for header_bytes in Self::HEADER_LINES {
            self.write_bytes(header_bytes, 0, true)?;
        }
        self.write_bytes(Self::LF_BYTES, 0, false)?;
        let xmin = textgrid.xmin.to_string();
        let xmax = textgrid.xmax.to_string();
        self.write_bytes("xmin = ".as_bytes(), 0, false)?;
        self.write_bytes(xmin.as_bytes(), 0, true)?;
        self.write_bytes("xmax = ".as_bytes(), 0, false)?;
        self.write_bytes(xmax.as_bytes(), 0, true)?;
        self.write_bytes("tiers? <exists>".as_bytes(), 0, true)?;
        self.write_bytes("size = ".as_bytes(), 0, false)?;
        self.write_bytes(textgrid.items.len().to_string().as_bytes(), 0, true)?;
        self.write_bytes("item []:".as_bytes(), 0, true)?;
        for (i, tier) in textgrid.items.into_iter().enumerate() {
            self.write_tier(tier, (i + 1).to_string(), xmin.as_bytes(), xmax.as_bytes())?;
        }
        self.write_bytes(Self::LF_BYTES, 0, false)?;
        self.file.flush()?;
        Ok(())
    }

    fn write_tier(
        &mut self,
        tier: Tier,
        id: String,
        xmin: &[u8],
        xmax: &[u8],
    ) -> Result<(), anyhow::Error> {
        self.write_bytes("item [".as_bytes(), 1, false)?;
        self.write_bytes(id.as_bytes(), 0, false)?;
        self.write_bytes("]:".as_bytes(), 0, true)?;
        self.write_bytes("class = \"".as_bytes(), 2, false)?;
        let (entry_type, tier_type) = if let Some(probe_entry) = tier.entries.last() {
            match probe_entry {
                TierEntry::Interval { .. } => ("intervals", "IntervalTier"),
                TierEntry::Point { .. } => ("points", "TextTier"),
            }
        } else {
            bail!("Empty tiers are a deal breaker, I am sorry.");
        };
        self.write_bytes(tier_type.as_bytes(), 0, false)?;
        self.write_bytes("\"".as_bytes(), 0, true)?;
        self.write_bytes("name = \"".as_bytes(), 2, false)?;
        self.write_bytes(tier.name.as_bytes(), 0, false)?;
        self.write_bytes("\"".as_bytes(), 0, true)?;
        self.write_bytes("xmin = ".as_bytes(), 2, false)?;
        self.write_bytes(xmin, 0, true)?;
        self.write_bytes("xmax = ".as_bytes(), 2, false)?;
        self.write_bytes(xmax, 0, true)?;
        self.write_bytes(entry_type.as_bytes(), 2, false)?;
        self.write_bytes(": size = ".as_bytes(), 0, false)?;
        self.write_bytes(tier.entries.len().to_string().as_bytes(), 0, true)?;
        for (i, entry) in tier.entries.into_iter().enumerate() {
            self.write_tier_entry(entry, entry_type.as_bytes(), (i + 1).to_string().as_bytes())?;
        }
        Ok(())
    }

    fn write_tier_entry(
        &mut self,
        entry: TierEntry,
        entry_bytes: &[u8],
        id: &[u8],
    ) -> Result<(), anyhow::Error> {
        self.write_bytes(entry_bytes, 2, false)?;
        self.write_bytes(" [".as_bytes(), 0, false)?;
        self.write_bytes(id, 0, false)?;
        self.write_bytes("]:".as_bytes(), 0, true)?;
        match entry {
            TierEntry::Interval { xmin, xmax, text } => {
                self.write_bytes("xmin = ".as_bytes(), 3, false)?;
                self.write_bytes(xmin.to_string().as_bytes(), 0, true)?;
                self.write_bytes("xmax = ".as_bytes(), 3, false)?;
                self.write_bytes(xmax.to_string().as_bytes(), 0, true)?;
                self.write_bytes("text = \"".as_bytes(), 3, false)?;
                self.write_bytes(text.as_bytes(), 0, false)?;
                self.write_bytes("\"".as_bytes(), 0, true)?;
            }
            TierEntry::Point { number, mark } => {
                self.write_bytes("number = ".as_bytes(), 3, false)?;
                self.write_bytes(number.to_string().as_bytes(), 0, true)?;
                self.write_bytes("mark = \"".as_bytes(), 3, false)?;
                self.write_bytes(mark.as_bytes(), 0, false)?;
                self.write_bytes("\"".as_bytes(), 0, true)?;
            }
        }
        Ok(())
    }

    fn write_bytes(
        &mut self,
        bytes: &[u8],
        indent_level: usize,
        newline_after: bool,
    ) -> Result<(), anyhow::Error> {
        for _ in 0..indent_level {
            self.file.write_all(Self::INDENT_BYTES)?;
        }
        self.file.write_all(bytes)?;
        if newline_after {
            self.file.write_all(Self::LF_BYTES)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Tier {
    name: String,
    entries: Vec<TierEntry>,
}

#[derive(Debug)]
enum TierEntry {
    Interval { xmin: f64, xmax: f64, text: String },
    Point { number: f64, mark: String },
}

impl From<(OrderedFloat<f64>, OrderedFloat<f64>, String)> for TierEntry {
    fn from(value: (OrderedFloat<f64>, OrderedFloat<f64>, String)) -> Self {
        TierEntry::Interval {
            xmin: value.0.into_inner(),
            xmax: value.1.into_inner(),
            text: value.2,
        }
    }
}

impl From<(OrderedFloat<f64>, String)> for TierEntry {
    fn from(value: (OrderedFloat<f64>, String)) -> Self {
        TierEntry::Point {
            number: value.0.into_inner(),
            mark: value.1,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use graphannis::AnnotationGraph;
    use insta::assert_snapshot;

    use crate::{
        exporter::textgrid::{default_file_key, default_time_key},
        importer::{exmaralda::ImportEXMARaLDA, Importer},
        test_util::export_to_string,
        StepID,
    };

    use super::ExportTextGrid;

    #[test]
    fn deserialize_default() {
        let toml_str = "";
        let exp: Result<ExportTextGrid, _> = toml::from_str(&toml_str);
        assert!(exp.is_ok());
    }

    #[test]
    fn deserialize_custom() {
        let toml_str = r#"
file_key = { ns = "", name = "file_name" }
point_tiers = [
  {ns = "phonetics", name = "pitch_accent"},
  {ns = "phonetics", name = "boundary_tone"}
]
"#;
        let exp: Result<ExportTextGrid, _> = toml::from_str(&toml_str);
        assert!(exp.is_ok());
        let export = exp.unwrap();
        assert!(export.file_key.ns.is_empty());
        assert_eq!(export.file_key.name.as_str(), "file_name");
        assert_eq!(export.point_tiers.len(), 2);
        assert_eq!(export.point_tiers[0].ns.as_str(), "phonetics");
        assert_eq!(export.point_tiers[0].name.as_str(), "pitch_accent");
        assert_eq!(export.point_tiers[1].ns.as_str(), "phonetics");
        assert_eq!(export.point_tiers[1].name.as_str(), "boundary_tone");
    }

    #[test]
    fn core_functionality() {
        let exmaralda = ImportEXMARaLDA {};
        let mprt = exmaralda.import_corpus(
            Path::new("tests/data/import/exmaralda/clean/import/exmaralda/"),
            StepID {
                module_name: "test_import_exb".to_string(),
                path: None,
            },
            None,
        );
        assert!(mprt.is_ok());
        let mut update_import = mprt.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(true);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(graph.apply_update(&mut update_import, |_| {}).is_ok());
        let export = export_to_string(
            &graph,
            ExportTextGrid {
                file_key: default_file_key(),
                time_key: default_time_key(),
                point_tiers: vec![],
                remove_ns: true,
            },
        );
        assert!(export.is_ok());
        dbg!(&export);
        assert_snapshot!(export.unwrap());
    }
}
