use std::num::{ParseFloatError, ParseIntError};

use pest::{iterators::Pairs, Parser};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, TextGridError>;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum TextGridError {
    #[error(transparent)]
    Parser(#[from] pest::error::Error<Rule>),
    #[error("TextGrid item value {0} is missing")]
    MissingValue(String),
    #[error("File exists, but contains no TextGrid")]
    MissingTextGrid,
    #[error(transparent)]
    ParseFloat(#[from] ParseFloatError),
    #[error(transparent)]
    ParseInt(#[from] ParseIntError),
}

struct DocumentHeader {
    xmin: f64,
    xmax: f64,
    number_items: u64,
}

pub struct Point {
    pub number: f64,
    pub mark: String,
}

pub struct Interval {
    pub xmin: f64,
    pub xmax: f64,
    pub text: String,
}

pub enum TextGridItem {
    Interval {
        name: String,
        xmin: f64,
        xmax: f64,
        intervals: Vec<Interval>,
    },
    Text {
        name: String,
        xmin: f64,
        xmax: f64,
        points: Vec<Point>,
    },
}

pub struct TextGrid {
    pub xmin: f64,
    pub xmax: f64,
    pub items: Vec<TextGridItem>,
}

#[derive(Parser)]
#[grammar = "models/textgrid.pest"]
pub struct OoTextfileParser;

impl TextGrid {
    pub fn parse(value: &str) -> Result<TextGrid> {
        let textgrid = OoTextfileParser::parse(Rule::textgrid, &value)?
            .next()
            .ok_or(TextGridError::MissingTextGrid)?;

        // The text grid is a flat sequence of numbers, texts or flags.
        let mut items = textgrid.into_inner();

        // Consume and the items for the document
        let header = consume_document_items(&mut items)?;

        // Map all tier items
        for _ in 0..header.number_items {
            let item = consume_tier_item(&mut items)?;
        }
        Ok(TextGrid {
            xmin: header.xmin,
            xmax: header.xmax,
            items: vec![],
        })
    }
}

fn consume_document_items<'a>(items: &mut Pairs<'a, Rule>) -> Result<DocumentHeader> {
    let xmin = items
        .next()
        .ok_or_else(|| TextGridError::MissingValue("xmin".to_string()))?;

    let xmax = items
        .next()
        .ok_or_else(|| TextGridError::MissingValue("xmax".to_string()))?;

    let mut number_items = 0;

    // Check that this document has a tier
    if let Some(tier_flag) = items.next() {
        if tier_flag.as_rule() == Rule::flag && tier_flag.as_str() == "exists" {
            // Get the number of items
            let size = items
                .next()
                .ok_or_else(|| TextGridError::MissingValue("size".to_string()))?;
            if size.as_rule() == Rule::number {
                number_items = size.as_str().parse::<u64>()?;
            }
        }
    }

    // No tier has been detected
    let header = DocumentHeader {
        xmin: xmin.as_str().parse::<f64>()?,
        xmax: xmax.as_str().parse::<f64>()?,
        number_items,
    };
    Ok(header)
}

fn consume_tier_item<'a>(items: &mut Pairs<'a, Rule>) -> Result<TextGridItem> {
    let class = items
        .next()
        .ok_or_else(|| TextGridError::MissingValue("class".to_string()))?;
    if class.as_rule() == Rule::text {}
    todo!()
}

#[cfg(test)]
mod tests;
