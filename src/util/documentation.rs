use crate::ModuleConfiguration;
use facet::{Type, UserType, Variant};
use itertools::Itertools;

/// Gets a clean documentation string for a [`Shape`].
/// It removes any whitespace at the start and unescapes backlashes.
pub fn clean_string(raw: &[&str]) -> String {
    raw.iter()
        .map(|line| line.trim_start())
        .map(unescape_backslash)
        .join("\n")
}

/// Replaces characters that are escaped with a backslash with the actual
/// character. Only unescapes the characters that are escaped by facet.
fn unescape_backslash(val: &str) -> String {
    let mut chars = val.chars().peekable();
    let mut unescaped = String::new();

    loop {
        match chars.next() {
            None => break,
            Some(c) => {
                let escaped_char = if c == '\\' {
                    if let Some(escaped_char) = chars.peek() {
                        let escaped_char = *escaped_char;
                        match escaped_char {
                            _ if escaped_char == '\\'
                                || escaped_char == '\''
                                || escaped_char == '`'
                                || escaped_char == '$' =>
                            {
                                Some(escaped_char)
                            }
                            'n' => Some('\n'),
                            'r' => Some('\r'),
                            't' => Some('\t'),
                            _ => None,
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some(escaped_char) = escaped_char {
                    unescaped.push(escaped_char);
                    // skip the escaped character instead of outputting it again
                    chars.next();
                } else {
                    unescaped.push(c);
                };
            }
        }
    }

    unescaped
}

pub struct ModuleInfo {
    pub name: String,
    pub doc: String,
    pub configs: Vec<ModuleConfiguration>,
}

impl From<&Variant> for ModuleInfo {
    fn from(module: &Variant) -> Self {
        // The name of the module is taken from the wrapper enum
        let module_name = module.name.to_lowercase();
        // Get the inner type wrapped by the graph operations enum and use
        // its documentation and fields
        let mut result = Self {
            name: module_name,
            doc: "".to_string(),
            configs: Vec::new(),
        };
        if let Some(inner_field) = module.data.fields.first().map(|m| m.shape())
            && let Type::User(module_type) = inner_field.ty
            && let UserType::Struct(module_impl) = module_type
        {
            result.doc = clean_string(inner_field.doc);

            result.configs = module_impl
                .fields
                .iter()
                .map(|f| ModuleConfiguration {
                    name: f.name.to_lowercase(),
                    description: clean_string(f.doc),
                })
                .collect();
        }
        result
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_unescape_backslash() {
        assert_eq!(unescape_backslash("ab\\$c"), "ab$c");
        assert_eq!(unescape_backslash("ab\\\\cd\\\\"), "ab\\cd\\",);
        assert_eq!(unescape_backslash("ab\\'cd\\te"), "ab'cd\te");
        assert_eq!(unescape_backslash("a\\n"), "a\n");
    }
}
