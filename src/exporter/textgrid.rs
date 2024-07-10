use std::{cmp::Ordering, collections::BTreeMap, fs, io::Write, path::PathBuf};

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
    /// remove_ns = true
    /// ```
    #[serde(default)]
    remove_ns: bool,
    /// Use this attribute to provide a list of anno keys in the order that you would like them to appear in the textgrid file.
    /// If you want this to be an explicit allow list, i. e. you do not want to export other names than the ones in this list,
    /// additionally set `ignore_others` to `true`.
    /// Example:
    /// ```toml
    /// [export.config]
    /// tier_order = [
    ///   { ns = "", name = "norm" },
    ///   { ns = "norm", name = "pos" }
    ///   { ns = "norm", name = "lemma" }
    /// ]
    /// ignore_others = true
    /// ```
    #[serde(default)]
    tier_order: Vec<AnnoKey>,
    /// Set this attribute to `true` to ignore all annotations whose key is not mentioned in attribute `tier_order` or `point_tiers`.
    /// Example:
    /// ```toml
    /// [export.config]
    /// point_tiers = [ { ns = "phonetics", name = "boundary_tone" } ]
    /// tier_order = [
    ///   { ns = "", name = "norm" },
    ///   { ns = "norm", name = "pos" }
    ///   { ns = "norm", name = "lemma" }
    /// ]
    /// ignore_others = true    ///
    /// ```
    #[serde(default)]
    ignore_others: bool,
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

type AnnisInterval = (OrderedFloat<f64>, Option<OrderedFloat<f64>>);

fn parse_time_tuple(
    value: &str,
    delimiter: &str,
) -> Result<AnnisInterval, Box<dyn std::error::Error>> {
    if let Some((start, end)) = value.split_once(delimiter) {
        Ok((start.parse()?, end.parse().ok()))
    } else {
        Err(anyhow!("Could not parse time values from input {value}").into())
    }
}

type TierData = BTreeMap<AnnoKey, Vec<(OrderedFloat<f64>, OrderedFloat<f64>, String)>>;

impl ExportTextGrid {
    fn export_document<I: Iterator<Item = NodeID>>(
        &self,
        graph: &AnnotationGraph,
        nodes: I,
        path: PathBuf,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // gather data
        let mut tier_data: TierData = BTreeMap::default();
        let node_annos = graph.get_node_annos();
        let mut xmin: OrderedFloat<f64> = f64::MAX.into();
        let mut xmax: OrderedFloat<f64> = 0f64.into();
        for node in nodes {
            let (start, end_opt) =
                if let Some(value) = node_annos.get_value_for_item(&node, &self.time_key)? {
                    parse_time_tuple(&value, "-")?
                } else {
                    // follow coverage edges to terminals
                    let mut time_annos = Vec::new();
                    for coverage_component in
                        graph.get_all_components(Some(AnnotationComponentType::Coverage), None)
                    {
                        if let Some(storage) = graph.get_graphstorage(&coverage_component) {
                            for connected_node in
                                storage.find_connected(node, 1, std::ops::Bound::Included(1))
                            {
                                if let Some(time_tuple) = node_annos
                                    .get_value_for_item(&connected_node?, &self.time_key)?
                                {
                                    time_annos.push(time_tuple);
                                }
                            }
                        }
                    }
                    let mut start = OrderedFloat::from(f64::MAX);
                    let mut end = OrderedFloat::from(f64::MIN);
                    let mut untouched = true;
                    for time_tuple in time_annos {
                        let (start_v, end_v) = parse_time_tuple(&time_tuple, "-")?;
                        if let Some(ev) = end_v {
                            // also only consider start value with fully defined intervals
                            untouched = false;
                            start = start.min(start_v);
                            end = end.max(ev);
                        }
                    }
                    if untouched {
                        continue;
                    }
                    (start, Some(end))
                };
            if let Some(end) = end_opt {
                xmin = xmin.min(start);
                xmax = xmax.max(end);
                for annotation in node_annos.get_annotations_for_item(&node)? {
                    if annotation.key.ns == ANNIS_NS
                        || (!self.tier_order.contains(&annotation.key)
                            && !self.point_tiers.contains(&annotation.key)
                            && self.ignore_others)
                    {
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
        let sorted_data = if self.tier_order.is_empty() {
            tier_data.into_iter().sorted_by(|a, b| a.0.cmp(&b.0))
        } else {
            let index_map: BTreeMap<&AnnoKey, usize> = self
                .tier_order
                .iter()
                .enumerate()
                .map(|(i, k)| (k, i))
                .collect();
            tier_data.into_iter().sorted_by(|a, b| {
                let ka = &a.0;
                let kb = &b.0;
                if let (Some(i), Some(j)) = (index_map.get(ka), index_map.get(kb)) {
                    (*i).cmp(j)
                } else if !self.tier_order.contains(ka) && !self.tier_order.contains(kb) {
                    (*ka).cmp(kb)
                } else if self.tier_order.contains(ka) {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            })
        };
        for (key, mut tuples) in sorted_data {
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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use graphannis::{graph::AnnoKey, AnnotationGraph};
    use insta::assert_snapshot;
    use ordered_float::OrderedFloat;

    use crate::{
        exporter::textgrid::{default_file_key, default_time_key},
        importer::{exmaralda::ImportEXMARaLDA, textgrid::ImportTextgrid, Importer},
        test_util::export_to_string,
        StepID,
    };

    use super::{parse_time_tuple, ExportTextGrid};

    // we only need this implementation for test purposes (shorter code)
    impl Default for ExportTextGrid {
        fn default() -> Self {
            Self {
                file_key: default_file_key(),
                time_key: default_time_key(),
                point_tiers: Vec::default(),
                remove_ns: bool::default(),
                tier_order: Vec::default(),
                ignore_others: bool::default(),
            }
        }
    }

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
ignore_others = true
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
        assert!(export.ignore_others);
    }

    #[test]
    fn default_functionality() {
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
        let export = export_to_string(&graph, ExportTextGrid::default());
        assert!(export.is_ok());
        assert_snapshot!(export.unwrap());
    }

    #[test]
    fn customization() {
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
                ignore_others: true,
                tier_order: vec![
                    AnnoKey {
                        ns: "dipl".into(),
                        name: "dipl".into(),
                    },
                    AnnoKey {
                        ns: "dipl".into(),
                        name: "sentence".into(),
                    },
                ],
                ..Default::default()
            },
        );
        assert!(export.is_ok());
        assert_snapshot!(export.unwrap());
    }

    #[test]
    fn customization_no_ignore() {
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
                ignore_others: false,
                tier_order: vec![
                    AnnoKey {
                        ns: "dipl".into(),
                        name: "dipl".into(),
                    },
                    AnnoKey {
                        ns: "dipl".into(),
                        name: "sentence".into(),
                    },
                ],
                ..Default::default()
            },
        );
        assert!(export.is_ok());
        assert_snapshot!(export.unwrap());
    }

    #[test]
    fn with_point_tiers() {
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
                ignore_others: false,
                tier_order: vec![
                    AnnoKey {
                        ns: "dipl".into(),
                        name: "dipl".into(),
                    },
                    AnnoKey {
                        ns: "dipl".into(),
                        name: "sentence".into(),
                    },
                ],
                point_tiers: vec![AnnoKey {
                    ns: "norm".into(),
                    name: "norm".into(),
                }],
                ..Default::default()
            },
        );
        assert!(export.is_ok());
        assert_snapshot!(export.unwrap());
    }

    #[test]
    fn ignore_with_point_tiers() {
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
                ignore_others: true,
                point_tiers: vec![
                    AnnoKey {
                        ns: "dipl".into(),
                        name: "dipl".into(),
                    },
                    AnnoKey {
                        ns: "dipl".into(),
                        name: "sentence".into(),
                    },
                    AnnoKey {
                        ns: "norm".into(),
                        name: "norm".into(),
                    },
                ],
                ..Default::default()
            },
        );
        assert!(export.is_ok());
        assert_snapshot!(export.unwrap());
    }

    #[test]
    fn ignore_only() {
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
                ignore_others: true,
                ..Default::default()
            },
        );
        assert!(export.is_ok());
        assert_snapshot!(export.unwrap());
    }

    #[test]
    fn test_parse_time_anno() {
        let interval = ".123-1";
        let r = parse_time_tuple(interval, "-");
        assert!(r.is_ok());
        let (start, end) = r.unwrap();
        assert_eq!(start, 0.123);
        assert!(end.is_some());
        let expected = OrderedFloat::from(1.0);
        assert_eq!(expected, end.unwrap());
        let interval2 = "0.5-";
        let r2 = parse_time_tuple(interval2, "-");
        assert!(r2.is_ok());
        let (start2, end2) = r2.unwrap();
        let expected2 = OrderedFloat::from(0.5);
        assert_eq!(expected2, start2);
        assert!(end2.is_none());
        assert!(parse_time_tuple("-", "-").is_err());
    }

    #[test]
    fn textgrid_to_textgrid() {
        let import_path = Path::new("tests/data/import/textgrid/singleSpeaker/");
        let import_config = r#"
skip_audio = true
tier_groups = { tok = ["pos", "lemma", "Inf-Struct"] }
        "#;
        let import_textgrid: Result<ImportTextgrid, _> = toml::from_str(import_config);
        assert!(import_textgrid.is_ok());
        let u = import_textgrid.unwrap().import_corpus(
            import_path,
            StepID {
                module_name: "test_import_textgrid".to_string(),
                path: None,
            },
            None,
        );
        assert!(u.is_ok());
        let mut update = u.unwrap();
        let g = AnnotationGraph::with_default_graphstorages(false);
        assert!(g.is_ok());
        let mut graph = g.unwrap();
        assert!(graph.apply_update(&mut update, |_| {}).is_ok());
        let export_textgrid = ExportTextGrid {
            tier_order: vec![
                AnnoKey {
                    ns: "".into(),
                    name: "tok".into(),
                },
                AnnoKey {
                    ns: "".into(),
                    name: "pos".into(),
                },
                AnnoKey {
                    ns: "".into(),
                    name: "lemma".into(),
                },
                AnnoKey {
                    ns: "".into(),
                    name: "Inf-Struct".into(),
                },
            ],
            ..Default::default()
        };
        let a = export_to_string(&graph, export_textgrid);
        assert!(a.is_ok());
        assert_snapshot!(a.unwrap());
    }
}
