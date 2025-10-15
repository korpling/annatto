use regex::{Regex, RegexBuilder};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Read};
use std::sync::{LazyLock, Mutex};

pub(super) enum Language {
    Unknown,
    English,
    Romanian,
    Italian,
    French,
    Portuguese,
    Galician,
    Catalan,
}

impl From<Language> for LanguageConfig {
    fn from(value: Language) -> Self {
        // Start with the defaults
        let mut p_char = r#"\[¿¡{'`"‚„†‡‹‘’“”•–—›»«"#;
        let mut f_char = r#"\]}'`",;:!?؟%‚„…†‡‰‹‘’“”•–—›»«"#;
        let mut p_clitic = "";
        let mut f_clitic = "";

        match value {
            Language::Unknown => { /* use the default values */ }
            Language::English => {
                f_clitic = "['’´](s|re|ve|d|m|em|ll)|n['’´]t";
            }
            Language::Romanian => {
                p_char = r#"\[¿¡{`"‚„†‡‹‘’“”•–—›»«"#;
                f_char = r#"\]}`",;:!?\%‚„…†‡‰‹‘’“”•–—›»«"#
            }
            Language::Italian => {
                p_clitic = "(?:d[ae]ll|nell|all|[ld]|sull|quest|un|senz|tutt|c|s)['´’]";
            }
            Language::French => {
                p_clitic = "(?:[dcjlmnst]|qu|jusqu|lorsqu|quoiqu|puisqu)['’´]";
                f_clitic = "-t-elles?|-t-ils?|-t-on|-ce|-elles?|-ils?|-je|-la|-les?|-leur|-lui|-mêmes?|-m['’´]|-moi|-nous|-on|-toi|-tu|-t['’´]|-vous|-en|-y|-ci|-là";
            }
            Language::Portuguese => {
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
        let abbreviations: HashSet<String> = HashSet::new();

        LanguageConfig {
            p_char: p_char.to_string(),
            f_char: f_char.to_string(),
            p_clitic: p_clitic.to_string(),
            f_clitic: f_clitic.to_string(),
            abbreviations,
        }
    }
}

impl<S> From<S> for Language
where
    S: AsRef<str>,
{
    fn from(value: S) -> Self {
        if value.as_ref().len() >= 2 {
            // Match the first two letters
            match &value.as_ref()[0..2] {
                "ca" => Language::Catalan,
                "en" => Language::English,
                "fr" => Language::French,
                "gl" => Language::Galician,
                "it" => Language::Italian,
                "pt" => Language::Portuguese,
                "ro" => Language::Romanian,
                _ => Language::Unknown,
            }
        } else {
            Language::Unknown
        }
    }
}

#[derive(Clone)]
struct LanguageConfig {
    /// Punctuation characters to cut of at a beginning of a word. Must be in a
    /// form that can be inserted into a Regex character class `[p_char]`.
    p_char: String,
    /// Punctuation characters to cut of at the ending of a word. Must be in a
    /// form that can be inserted into a Regex character class `[f_char]`.
    f_char: String,
    p_clitic: String,
    f_clitic: String,
    abbreviations: HashSet<String>,
}

/// A character to pre-mark locations to split. The algorithm will determine
/// places like spaces where to insert splits. But we can't use the space
/// character for this, because inside SGML tags there should actually not be a
/// split when there is a space character (blank).
/// To workaround this problem, a special character is defined instead of a blank that takes the role of marking where to split.
const SPLIT_MARKER: char = '\u{0179}';
const SPLIT_MARKER_STR: &str = "\u{0179}";

static COMPILED_REGEX_CACHE: LazyLock<Mutex<HashMap<String, Regex>>> =
    LazyLock::new(Mutex::default);

fn cached_regex(p: &str) -> crate::error::Result<Regex> {
    let mut cache = COMPILED_REGEX_CACHE.lock()?;
    if let Some(existing) = cache.get(p) {
        Ok(existing.clone())
    } else {
        let compiled = Regex::new(p)?;
        cache.insert(p.to_string(), compiled.clone());
        Ok(compiled)
    }
}

fn cached_regex_case_insensitive(p: &str) -> crate::error::Result<Regex> {
    let mut cache = COMPILED_REGEX_CACHE.lock()?;
    if let Some(existing) = cache.get(p) {
        Ok(existing.clone())
    } else {
        let compiled = RegexBuilder::new(p).case_insensitive(true).build()?;
        cache.insert(p.to_string(), compiled.clone());
        Ok(compiled)
    }
}

#[derive(Clone)]
pub(super) struct TreeTaggerTokenizer {
    config: LanguageConfig,
}

#[derive(Clone)]
pub(super) struct Token {
    pub value: String,
    pub whitespace_after: Option<String>,
}

impl Token {
    fn new_val<S: ToString>(value: S) -> Self {
        Self {
            value: value.to_string(),
            whitespace_after: None,
        }
    }
}

impl TreeTaggerTokenizer {
    pub(super) fn new(language: Language) -> anyhow::Result<Self> {
        let config: LanguageConfig = language.into();
        Ok(Self { config })
    }

    /// Returns a list of token and the possible whitespace that comes after each token
    pub(super) fn tokenize<R: Read>(&self, reader: R) -> anyhow::Result<Vec<Token>> {
        let mut result = Vec::new();

        let mut buffered_reader = BufReader::new(reader);

        // Tokenize line by line
        let mut line = String::new();
        let mut is_first_line = true;
        while buffered_reader.read_line(&mut line)? > 0 {
            if is_first_line {
                // The first line might contain a byte order marker (BOM)
                line = cached_regex("^\u{FEFF}")?.replace(&line, "").to_string();
                is_first_line = false;
            }
        }

        // Replace newline and tab charachters with spaces, so we don't have to distinguish them later
        line = cached_regex("[\n\t]")?.replace_all(&line, " ").to_string();

        // Spaces *inside* SGML tags (e.g. `<mytag a=" " b = "">` should be
        // protected and not create new separate token. Replace all spaces within a
        // special character, then replace all other spaces with another character
        // and restore the original spaces inside the SGML tags.
        while let Cow::Owned(new_line) =
            cached_regex("(<[^<> ]*) ([^<>]*>)")?.replace_all(&line, "${1}\u{0179}${2}")
        {
            line = new_line;
        }
        line = line.replace(' ', "\u{178}");
        line = line
            .replace('\u{0179}', " ")
            .replace('\u{178}', SPLIT_MARKER_STR);

        // Mark SGML tags as split points for the tokenization
        line = cached_regex("(<[^<>]*>)")?
            .replace_all(&line, &format!("{SPLIT_MARKER}$1{SPLIT_MARKER}"))
            .to_string();

        // Remove split marks at beginning and end of the line, and also repeating ones
        line = line.trim_matches(SPLIT_MARKER).to_string();
        line = cached_regex(&format!("{SPLIT_MARKER}{SPLIT_MARKER}{SPLIT_MARKER}*"))?
            .replace_all(&line, SPLIT_MARKER_STR)
            .to_string();

        // Split by the prepared split marker
        for segment in line.split(SPLIT_MARKER) {
            let mut segment = segment.to_string();
            if cached_regex("^<.*>$")?.is_match(&segment) {
                // The complete segment (not line) is an SGML tag and can be added as one token
                result.push(Token::new_val(segment));
            } else {
                // The pre-splitted segment can contain more than one token, e.g.
                // because of punctuation.

                // Special handling for "..."
                segment = cached_regex("(\\.\\.\\.)")?
                    .replace_all(&segment, " ... ")
                    .to_string();
                // Add missing blanks between certain punctuation characters
                segment = cached_regex("([;!?])([^ ])")?
                    .replace_all(&segment, "$1 $2")
                    .to_string();

                // Split the remaining string at blanks and use this as initial
                // token list. Since we already know this is not an SGML-Tag, no
                // special whitespace handling is necessary.
                for mut current_token in segment.split(' ').map(str::to_string) {
                    let mut suffix = Vec::new();
                    // Separate punctuation and parentheses from words
                    let mut finished = false;
                    while !finished {
                        if let Some(m) =
                            substitute("^(\\()([^\\)]*)(.)$", "$2$3", &mut current_token)?
                        {
                            // Separate preceding parentheses
                            result.push(Token::new_val(m.get(1).map_or("", |m| m.as_str())));
                        } else if let Some(m) =
                            substitute("^([^(]+)(\\))$", "$1", &mut current_token)?
                        {
                            // Separate following preceding parentheses
                            suffix.insert(0, Token::new_val(m.get(2).map_or("", |m| m.as_str())));
                        } else if let Some(m) = substitute(
                            &format!("^([{}])(.)", &self.config.p_char),
                            "$2",
                            &mut current_token,
                        )? {
                            // Separate preceding punctuation
                            result.push(Token::new_val(m.get(1).map_or("", |m| m.as_str())));
                        } else if let Some(m) = substitute(
                            &format!("(.)([{}])$", &self.config.f_char),
                            "$1",
                            &mut current_token,
                        )? {
                            // Separate trailing punctuation
                            suffix.insert(0, Token::new_val(m.get(2).map_or("", |m| m.as_str())));
                        } else if let Some(m) = substitute(
                            &format!("([{}]|\\))\\.$", &self.config.f_char),
                            "",
                            &mut current_token,
                        )? {
                            // Separate trailing periods if punctuation precedes
                            suffix.insert(0, Token::new_val("."));
                            let punction_before_period =
                                m.get(1).map_or("", |m| m.as_str()).to_string();
                            if current_token.is_empty() {
                                current_token = punction_before_period;
                            } else {
                                suffix.insert(0, Token::new_val(punction_before_period));
                            }
                        } else {
                            finished = true;
                        }
                    }

                    // handle explicitly listed tokens
                    if self.config.abbreviations.contains(&current_token) {
                        result.push(Token::new_val(&current_token));
                        result.extend(suffix.iter().cloned());
                        continue;
                    }
                    // abbreviations of the form A. or U.S.A.
                    if cached_regex("^([A-Za-z-]\\.)+$")?.is_match(&current_token) {
                        result.push(Token::new_val(&current_token));
                        result.extend(suffix.iter().cloned());
                        continue;
                    }
                    // disambiguate periods
                    if let Some(m) = cached_regex("^(..*)\\.$")?.captures(&current_token.clone())
                        && current_token != "..."
                    {
                        current_token = m.get(1).map_or("", |m| m.as_str()).to_string();
                        suffix.insert(0, Token::new_val("."));
                        if self.config.abbreviations.contains(&current_token) {
                            result.push(Token::new_val(&current_token));
                            result.extend(suffix.iter().cloned());
                            continue;
                        }
                    }
                    // cut off clitics
                    while let Some(m) = substitute("^(--)(.)", "$2", &mut current_token)? {
                        result.push(Token::new_val(m.get(1).map_or("", |m| m.as_str())));
                    }
                    if !self.config.p_clitic.is_empty() {
                        while let Some(m) = substitute_i(
                            &format!("^({})(.)", self.config.p_clitic),
                            "$2",
                            &mut current_token,
                        )? {
                            result.push(Token::new_val(m.get(1).map_or("", |m| m.as_str())));
                        }
                    }
                    while let Some(m) = substitute("(.)(--)$", "$1", &mut current_token)? {
                        suffix.insert(0, Token::new_val(m.get(2).map_or("", |m| m.as_str())));
                    }
                    if !self.config.f_clitic.is_empty() {
                        while let Some(m) = substitute_i(
                            &format!("(.)({})$", self.config.f_clitic),
                            "$1",
                            &mut current_token,
                        )? {
                            suffix.insert(0, Token::new_val(m.get(2).map_or("", |m| m.as_str())));
                        }
                    }
                    result.push(Token::new_val(current_token));
                    result.extend(suffix.into_iter());
                }
            }
        }

        let result = result
            .into_iter()
            .filter(|t| !t.value.is_empty() || t.whitespace_after.is_some())
            .collect();

        Ok(result)
    }
}

/// Substitute the first match for the `pattern` in the `buffer` with the `replacement`.
/// The `replacement` can contain back-references to the pattern.
/// Returns `Ok(Some(Vec<String>))` with the captured values as string vector if there was a match.
fn substitute(
    pattern: &str,
    replacement: &str,
    buffer: &mut String,
) -> anyhow::Result<Option<Vec<String>>> {
    let pattern = cached_regex(pattern)?;
    // The first capture group is always the whole match
    if let Some(caps) = pattern.captures(buffer)
        && let Some(whole_match) = caps.get(0)
    {
        // Collect the captured values before we replace the original buffer
        let captured_values = caps
            .iter()
            .map(|c| {
                if let Some(m) = c {
                    m.as_str().to_string()
                } else {
                    String::new()
                }
            })
            .collect();

        // Get the expanded captures as string
        let mut expanded_replacment = String::new();
        caps.expand(replacement, &mut expanded_replacment);
        // Add the left and right context from the original buffer to the replacement
        expanded_replacment.insert_str(0, &buffer[0..whole_match.start()]);
        expanded_replacment.push_str(&buffer[whole_match.end()..]);
        // Replace the whole match with the expanded string in the original buffer
        *buffer = expanded_replacment;

        return Ok(Some(captured_values));
    }
    Ok(None)
}

/// Substitute the first match for the `pattern` in the `buffer` with the `replacement`.
/// The search is **case-insensitive**.
/// The `replacement` can contain back-references to the pattern.
/// Returns `Ok(Some(Vec<String>))` with the captured values as string vector if there was a match.
fn substitute_i(
    pattern: &str,
    replacement: &str,
    buffer: &mut String,
) -> anyhow::Result<Option<Vec<String>>> {
    let pattern = cached_regex_case_insensitive(pattern)?;
    // The first capture group is always the whole match
    if let Some(caps) = pattern.captures(buffer)
        && let Some(whole_match) = caps.get(0)
    {
        // Collect the captured values before we replace the original buffer
        let captured_values = caps
            .iter()
            .map(|c| {
                if let Some(m) = c {
                    m.as_str().to_string()
                } else {
                    String::new()
                }
            })
            .collect();

        // Get the expanded captures as string
        let mut expanded_replacment = String::new();
        caps.expand(replacement, &mut expanded_replacment);
        // Add the left and right context from the original buffer to the replacement
        expanded_replacment.insert_str(0, &buffer[0..whole_match.start()]);
        expanded_replacment.push_str(&buffer[whole_match.end()..]);
        // Replace the whole match with the expanded string in the original buffer
        *buffer = expanded_replacment;

        return Ok(Some(captured_values));
    }
    Ok(None)
}

#[cfg(test)]
mod tests;
