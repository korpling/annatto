pub(crate) mod anno_key;
pub(crate) mod annotation_component;
pub(crate) mod update_event;

pub trait IntoInner {
    type I;
    fn into_inner(self) -> Self::I;
}

#[cfg(test)]
mod tests;
