use std::io::BufRead;

use anyhow::{anyhow, Result};
use quick_xml::{
    events::{attributes::Attributes, Event},
    Reader,
};

/// Extract an attribute for an XML element by the namespace and name.
pub(crate) fn get_attribute_by_qname<'a>(
    attribute_list: Attributes<'a>,
    namespace: &str,
    name: &str,
) -> Result<Option<String>> {
    for att in attribute_list {
        let att = att?;
        if let Some(prefix) = att.key.prefix() {
            if prefix.as_ref() == namespace.as_bytes()
                && att.key.local_name().as_ref() == name.as_bytes()
            {
                let value = String::from_utf8_lossy(&att.value).to_string();
                return Ok(Some(value));
            }
        }
    }
    Ok(None)
}

/// Extract an attribute for an XML element by the name.
pub(crate) fn get_attribute_by_local_name<'a>(
    attribute_list: Attributes<'a>,
    name: &str,
) -> Result<Option<String>> {
    for att in attribute_list {
        let att = att?;
        if att.key.local_name().as_ref() == name.as_bytes() {
            let value = String::from_utf8_lossy(&att.value).to_string();
            return Ok(Some(value));
        }
    }
    Ok(None)
}

/// Read the next event. Will fail if the next event is not a start tag and does
/// not have the given name. All non tag elements (XML declaration, text nodes,
/// comments, ...) are ignored and skipped.
pub(crate) fn consume_start_tag_with_name<R>(reader: &mut Reader<R>, name: &str) -> Result<()>
where
    R: BufRead,
{
    let mut buf = Vec::new();
    loop {
        let event = reader.read_event_into(&mut buf)?;
        let result = match event {
            Event::Start(tag) => {
                if tag.local_name().as_ref() == name.as_bytes() {
                    Ok(())
                } else {
                    Err(anyhow!(
                        "Expected <{name}> but got <{}>",
                        String::from_utf8_lossy(tag.local_name().as_ref())
                    ))
                }
            }
            Event::End(_) => Err(anyhow!(
                "Expected \"<{name}>\" but got closing tag instead."
            )),
            Event::Empty(_) => Err(anyhow!("Expected \"<{name}>\" but got empty tag instead.")),

            Event::Comment(_)
            | Event::Decl(_)
            | Event::PI(_)
            | Event::DocType(_)
            | Event::CData(_)
            | Event::Text(_) => continue,
            Event::Eof => Err(anyhow!(
                "Expected <{name} but the file is already at its end."
            )),
        };
        return result;
    }
}
