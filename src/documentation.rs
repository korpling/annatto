use serde::Deserialize;
pub trait FieldsHaveDefault: Deserialize<'static> {
    const FIELDS_HAVE_DEFAULT: &'static [bool];
}
