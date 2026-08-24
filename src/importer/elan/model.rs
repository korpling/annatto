use std::{cmp::Ordering, collections::BTreeMap, fmt::Debug, path::PathBuf};

use itertools::Itertools;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(super) struct AnnotationDocument {
    _license: Option<License>,
    header: Header,
    time_order: TimeOrder,
    #[serde(rename = "TIER")]
    tiers: Vec<Tier>,
    #[serde(default)]
    _linguistic_types: Vec<LinguisticType>,
    #[serde(default)]
    _locales: Vec<Locale>,
    #[serde(default)]
    _languages: Vec<Language>,
    #[serde(default)]
    _constraints: Vec<Constraint>,
    #[serde(default)]
    _controlled_vocabularies: Vec<ControlledVocabulary>,
    #[serde(default)]
    _lexicon_refs: Vec<LexiconRef>,
    #[serde(default)]
    _external_refs: Vec<ExternalRef>,
    #[serde(rename = "@DATE")]
    _date: String,
    #[serde(rename = "@AUTHOR")]
    _author: String,
    #[serde(rename = "@VERSION")]
    _version: String,
    #[serde(rename = "@FORMAT")]
    _format: Option<String>,
    #[serde(skip, default)]
    is_sorted: bool,
}

struct RecursiveLookupCmp<'a> {
    tier_lookup: BTreeMap<&'a str, &'a Tier>,
}

impl<'a> RecursiveLookupCmp<'a> {
    fn new(tier_vec: &'a [Tier]) -> Self {
        let tier_lookup = tier_vec.iter().map(|t| (t.id(), t)).collect();
        RecursiveLookupCmp { tier_lookup }
    }

    fn cmp(&self, a: &Tier, b: &Tier) -> Ordering {
        match (a.parent_ref(), b.parent_ref()) {
            (Some(pa), Some(pb)) => {
                if pa == b.id() {
                    Ordering::Greater
                } else if pb == a.id() {
                    Ordering::Less
                } else {
                    if let Some(pa_tier) = self.tier_lookup.get(pa)
                        && let Some(pb_tier) = self.tier_lookup.get(pb)
                    {
                        self.cmp(pa_tier, pb_tier)
                    } else {
                        Ordering::Equal
                    }
                }
            }
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (None, None) => Ordering::Equal,
        }
    }
}

impl AnnotationDocument {
    pub(super) fn timeline(&self) -> &Vec<TimeSlot> {
        &self.time_order.time_slots
    }

    pub(super) fn time_units(&self) -> &TimeUnits {
        &self.header.time_units
    }

    fn sort_tiers(&mut self) {
        let recursive_search = RecursiveLookupCmp::new(&self.tiers);
        let reordered = self
            .tiers
            .iter()
            .sorted_by(|a, b| recursive_search.cmp(a, b))
            .map(|t| t.id().to_string())
            .collect_vec();
        self.tiers.sort_by(|a, b| {
            match (
                reordered.iter().position(|e| e == a.id()),
                reordered.iter().position(|e| e == b.id()),
            ) {
                (Some(i), Some(j)) => i.cmp(&j),
                _ => Ordering::Equal,
            }
        });
        self.is_sorted = true;
    }

    pub(super) fn tiers(&mut self) -> &Vec<Tier> {
        if !self.is_sorted {
            self.sort_tiers();
        }
        &self.tiers
    }
}

#[derive(Deserialize)]
struct License {
    #[serde(rename = "@LICENSE_URL")]
    _license_url: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(super) struct Header {
    #[serde(default)]
    _media_descriptors: Vec<MediaDescriptor>,
    #[serde(default)]
    _linked_file_descriptors: Vec<LinkedFileDescriptor>,
    #[serde(default)]
    _properties: Vec<Property>,
    #[serde(rename = "@MEDIA_FILE", default)]
    _media_file: Option<PathBuf>,
    #[serde(rename = "@TIME_UNITS", default)]
    pub(super) time_units: TimeUnits,
}

#[derive(Deserialize, Default)]
pub(super) enum TimeUnits {
    #[serde(rename = "NTSC-frames")]
    NTSCframes,
    #[serde(rename = "PAL-frames")]
    PALframes,
    #[default]
    #[serde(rename = "milliseconds")]
    Milliseconds,
}

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct MediaDescriptor {
    #[serde(rename = "@MEDIA_URL")]
    _media_url: PathBuf,
    #[serde(rename = "@RELATIVE_MEDIA_URL", default)]
    _relative_media_url: Option<PathBuf>,
    #[serde(rename = "@MIME_TYPE")]
    _mime_type: String,
    #[serde(rename = "@TIME_ORIGIN")]
    _time_origin: Option<String>,
    #[serde(rename = "@EXTRACTED_FROM")]
    _extracted_from: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct LinkedFileDescriptor {
    #[serde(rename = "@LINK_URL")]
    _link_url: PathBuf,
    #[serde(rename = "@RELATIVE_LINK_URL")]
    _relative_link_url: Option<PathBuf>,
    #[serde(rename = "@MIME_TYPE")]
    _mime_type: String,
    #[serde(rename = "@TIME_ORIGIN")]
    _time_origin: Option<String>,
    #[serde(rename = "@ASSOCIATED_WITH")]
    _associated_with: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct Property {
    #[serde(rename = "@NAME")]
    _name: Option<String>,
}

#[derive(Deserialize)]
struct TimeOrder {
    #[serde(rename = "TIME_SLOT")]
    time_slots: Vec<TimeSlot>,
}

#[derive(Deserialize)]
#[serde(rename = "TIME_SLOT")]
pub(super) struct TimeSlot {
    #[serde(rename = "@TIME_SLOT_ID")]
    pub(super) time_slot_id: String,
    #[serde(rename = "@TIME_VALUE")]
    pub(super) time_value: Option<usize>, // yes!
}

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(super) struct Tier {
    #[serde(rename = "@TIER_ID")]
    tier_id: String,
    #[serde(rename = "@PARTICIPANT")]
    _participant: Option<String>,
    #[serde(rename = "@ANNOTATOR")]
    _annotator: Option<String>,
    #[serde(rename = "@LINGUISTIC_TYPE_REF")]
    _linguistic_type_ref: String,
    #[serde(rename = "@DEFAULT_LOCALE")]
    _default_locale: Option<String>,
    #[serde(rename = "@PARENT_REF")]
    parent_ref: Option<String>,
    #[serde(rename = "@EXT_REF")]
    _ext_ref: Option<String>,
    #[serde(rename = "@LANG_REF")]
    _lang_ref: Option<String>,
    #[serde(rename = "ANNOTATION", default)]
    annotations: Vec<OuterAnnotation>,
}

impl Debug for Tier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<TIER .. TIER_ID=\"{}\">", self.id())
    }
}

impl Tier {
    pub(super) fn id(&self) -> &str {
        &self.tier_id
    }

    pub(super) fn parent_ref(&self) -> Option<&str> {
        self.parent_ref.as_deref()
    }

    pub(super) fn annotations(&self) -> Vec<&Annotation> {
        self.annotations
            .iter()
            .map(|OuterAnnotation(a)| a)
            .collect()
    }
}

#[derive(Deserialize)]
struct OuterAnnotation(Annotation);

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(super) enum Annotation {
    AlignableAnnotation {
        #[serde(rename = "ANNOTATION_VALUE")]
        annotation_value: String,
        #[serde(rename = "@TIME_SLOT_REF1")]
        time_slot_ref1: String,
        #[serde(rename = "@TIME_SLOT_REF2")]
        time_slot_ref2: String,
        #[serde(rename = "@SVG_REF")]
        _svg_ref: Option<String>,
        #[serde(rename = "@ANNOTATION_ID")]
        annotation_id: String,
        #[serde(rename = "@EXT_REF")]
        _ext_ref: Option<String>,
        #[serde(rename = "@LANG_REF")]
        _lang_ref: Option<String>,
        #[serde(rename = "@CVE_REF")]
        _cve_ref: Option<String>,
    },
    RefAnnotation {
        #[serde(rename = "ANNOTATION_VALUE")]
        annotation_value: String,
        #[serde(rename = "@ANNOTATION_REF")]
        annotation_ref: String,
        #[serde(rename = "@PREVIOUS_ANNOTATION")]
        _svg_ref: Option<String>,
        #[serde(rename = "@ANNOTATION_ID")]
        annotation_id: String,
        #[serde(rename = "@EXT_REF")]
        _ext_ref: Option<String>,
        #[serde(rename = "@LANG_REF")]
        _lang_ref: Option<String>,
        #[serde(rename = "@CVE_REF")]
        _cve_ref: Option<String>,
    },
}

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct LinguisticType {
    #[serde(rename = "@LINGUISTIC_TYPE_ID")]
    _linguistic_type_id: String,
    #[serde(rename = "@TIME_ALIGNABLE")]
    _time_alignable: Option<bool>,
    #[serde(rename = "@CONSTRAINTS")]
    _constraints: Option<String>,
    #[serde(rename = "@GRAPHIC_REFERENCES")]
    _graphic_references: Option<bool>,
    #[serde(rename = "@CONTROLLED_VOCABULARY_REF")]
    _controlled_vocabulary_ref: Option<String>,
    #[serde(rename = "@EXT_REF")]
    _ext_ref: Option<String>,
    #[serde(rename = "@LEXICON_REF")]
    _lexicon_ref: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct Locale {
    #[serde(rename = "@LANGUAGE_CODE")]
    _language_code: String,
    #[serde(rename = "@COUNTRY_CODE")]
    _country_code: Option<String>,
    #[serde(rename = "@VARIANT")]
    _variant: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct Language {
    #[serde(rename = "@LANG_ID")]
    _lang_id: String,
    #[serde(rename = "@LANG_DEF", default)]
    _lang_def: Option<String>,
    #[serde(rename = "@LANG_LABEL", default)]
    _lang_label: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct Constraint {
    #[serde(rename = "@STEREOTYPE")]
    _stereotype: String,
    #[serde(rename = "@DESCRIPTION")]
    _description: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct ControlledVocabulary {
    _description: Vec<Description>,
    _cv_entries: Vec<CVEntryML>,
    #[serde(rename = "@CV_ID")]
    _cv_id: String,
    #[serde(rename = "@EXT_REF")]
    _ext_ref: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct Description {
    #[serde(rename = "#content")]
    _description: String,
    #[serde(rename = "@LANG_REF")]
    _lang_ref: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct CVEntryML {
    _cve_value: Vec<CVEValue>,
    #[serde(rename = "@CVE_ID")]
    _cve_id: String,
    #[serde(rename = "@EXT_REF")]
    _ext_ref: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct CVEValue {
    #[serde(rename = "@LANG_REF")]
    _lang_ref: String,
    #[serde(rename = "@DESCRIPTION")]
    _description: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct LexiconRef {
    #[serde(rename = "@LEX_REF_ID")]
    _lex_ref_id: String,
    #[serde(rename = "@NAME")]
    _name: String,
    #[serde(rename = "@TYPE")]
    _letype: String,
    #[serde(rename = "@URL")]
    _url: String,
    #[serde(rename = "@LEXICON_ID")]
    _lexicon_id: String,
    #[serde(rename = "@LEXICON_NAME")]
    _lexicon_name: String,
    #[serde(rename = "@DATCAT_ID", default)]
    _datcat_id: Option<String>,
    #[serde(rename = "@DATCAT_NAME", default)]
    _datcat_name: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct ExternalRef {
    #[serde(rename = "@EXT_REF_ID")]
    _ext_ref_id: String,
    #[serde(rename = "@TYPE")]
    _rtype: String,
    #[serde(rename = "@VALUE")]
    _value: String,
}
