use encoding_rs::UTF_8;
use regex::Regex;
use std::borrow::Cow;
use std::collections::HashSet;
use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Write};

pub(super) enum Language {
    Unknown,
    English,
    Romanian,
    Italian,
    French,
    Portoguese,
    Galician,
    Catalan,
}

impl Into<LanguageConfig> for Language {
    fn into(self) -> LanguageConfig {
        // Start with the defaults
        let mut p_char = "\\[\\{\\(´`\"»«‚„†‡‹‘’“”•–—›";
        let mut f_char = "\\]\\}'`\"\\),;:!\\?%»«‚„…†‡‰‹‘’“”•–—›";
        let mut p_clitic = "";
        let mut f_clitic = "";

        match self {
            Language::Unknown => { /* use the default values */ }
            Language::English => {
                f_clitic = "['’´](s|re|ve|d|m|em|ll)|n['’´]t";
            }
            Language::Romanian => todo!(),
            Language::Italian => {
                p_clitic = "(?:d[ae]ll|nell|all|[ld]|sull|quest|un|senz|tutt|c|s)['´’]";
            }
            Language::French => {
                p_clitic = "(?:[dcjlmnst]|qu|jusqu|lorsqu|quoiqu|puisqu)['’´]";
                f_clitic = "-t-elles?|-t-ils?|-t-on|-ce|-elles?|-ils?|-je|-la|-les?|-leur|-lui|-mêmes?|-m['’´]|-moi|-nous|-on|-toi|-tu|-t['’´]|-vous|-en|-y|-ci|-là";
            }
            Language::Portoguese => {
                f_clitic = "-a|-as|-la|-las|-lha|-lhas|-lhe|-lhes|-lho|-lhos|-lo|-los|-ma|-mas|-me|-mo|-mos|-na|-nas|-no|-no-la|-no-las|-no-lo|-no-los|-nos|-o|-os|-s|-se|-se-á|-se-ão|-se-é|-se-ia|-se-lha|-se-lhas|-se-lhe|-se-lhes|-se-lho|-se-lhos|-se-nos|-se-vos|-ta|-tas|-te|-to|-tos|-vo-la|-vo-las|-vo-lo|-vo-los|-vos";
            }
            Language::Galician => {
                f_clitic = "-la|-las|-lo|-los|-nos";
            }
            Language::Catalan => {
                p_clitic = "[dlmnst]['’´]";
                f_clitic =
                    "['’´](n|s|ls|l|hi|ns|t|m|ho)|-(se|lo|la|li|los|les|hi|ho|ne|nos|me|s|te|m)";
            }
        }

        // TODO: make this configurable
        let mut abbreviations: HashSet<String> = HashSet::new();

        LanguageConfig {
            p_char: p_char.to_string(),
            f_char: f_char.to_string(),
            p_clitic: p_clitic.to_string(),
            f_clitic: f_clitic.to_string(),
            abbreviations,
        }
    }
}

struct LanguageConfig {
    /// Punctuation characters to cut of at a beginning of a word
    p_char: String,
    // Punctuation characters to cut of at the ending of a word
    f_char: String,
    p_clitic: String,
    f_clitic: String,
    abbreviations: HashSet<String>,
}

/// A character to pre-mark locations to split. The algorithm will determine
/// places like spaces where to insert splits. But we can't use the space
/// character for this, because inside SGML tags there should actually not be a
/// split when there is a whitespace character.
/// To workaround this problem, a special character is defined instead of a whitespace that takes the role of marking where to split.
const SPLIT_MARKER: char = '\u{0179}';
const SPLIT_MARKER_STR: &'static str = "\u{0179}";

pub(super) fn tokenize<R: Read>(
    reader: R,
    language: Language,
) -> anyhow::Result<Vec<(String, Option<String>)>> {
    let mut result = Vec::new();

    let config: LanguageConfig = language.into();

    // Compile the regular expressions once instead repeatingly when iterating over the lines.
    let re_bom = Regex::new("^\u{FEFF}")?;
    let re_newline_tab = Regex::new("[\n\t]")?;
    let re_space_inside_sgml = Regex::new("(<[^<> ]*) ([^<>]*>)")?;
    let re_sgml_tags = Regex::new("(<[^<>]*>)")?;
    let re_repeating_split_marker =
        Regex::new(&format!("{SPLIT_MARKER}{SPLIT_MARKER}{SPLIT_MARKER}*"))?;

    let mut buffered_reader = BufReader::new(reader);

    // Tokenize line by line
    let mut line = String::new();
    let mut is_first_line = true;
    while buffered_reader.read_line(&mut line)? > 0 {
        if is_first_line {
            // The first line might contain a byte order marker (BOM)
            line = re_bom.replace(&line, "").to_string();
            is_first_line = false;
        }
    }

    // Replace newline and tab charachters with spaces, so we don't have to distinguish them later
    line = re_newline_tab.replace_all(&line, " ").to_string();

    // Spaces *inside* SGML tags (e.g. `<mytag a=" " b = "">` should be
    // protected and not create new separate token. Replace all spaces within a
    // special character, then replace all other spaces with another character
    // and restore the original spaces inside the SGML tags.
    while let Cow::Owned(new_line) = re_space_inside_sgml.replace_all(&line, "${1}\u{0179}${2}") {
        line = new_line;
    }
    line = line.replace(' ', "\u{178}");
    line = line
        .replace('\u{0179}', " ")
        .replace('\u{178}', SPLIT_MARKER_STR);

    // Mark SGML tags as split points for the tokenization
    line = re_sgml_tags
        .replace_all(&line, &format!("{SPLIT_MARKER}$1{SPLIT_MARKER}"))
        .to_string();

    // Remove split marks at beginning and end of the line, and also repeating ones
    line = line.trim_matches(SPLIT_MARKER).to_string();
    line = re_repeating_split_marker
        .replace_all(&line, SPLIT_MARKER_STR)
        .to_string();

    // Split by the prepared split marker
    for segment in line.split(SPLIT_MARKER) {
        todo!()
    }
    Ok(result)
}
