use std::num::{ParseFloatError, ParseIntError};

use pest::{
    iterators::{Pair, Pairs},
    Parser,
};
use pest_derive::Parser;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, TextGridError>;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum TextGridError {
    #[error(transparent)]
    Parser(#[from] Box<pest::error::Error<Rule>>),
    #[error("TextGrid item value {0} is missing")]
    MissingValue(&'static str),
    #[error("File exists, but contains no TextGrid")]
    MissingTextGrid,
    #[error(transparent)]
    ParseFloat(#[from] ParseFloatError),
    #[error(transparent)]
    ParseInt(#[from] ParseIntError),
    #[error("TextGrid entity should have been a number")]
    NumberExpected,
    #[error("TextGrid entity should have been text")]
    TextExpected,
    #[error("Unknown class '{0}' for item. Must be either 'IntervalTier' or 'TextTier'.")]
    UnknownItemClass(String),
}

struct DocumentHeader {
    xmin: f64,
    xmax: f64,
    number_items: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Point {
    pub number: f64,
    pub mark: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Interval {
    pub xmin: f64,
    pub xmax: f64,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq)]
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
pub struct TextGridParser;

impl TextGrid {
    pub fn parse(value: &str) -> Result<TextGrid> {
        let textgrid = TextGridParser::parse(Rule::textgrid, value)
            .map_err(Box::new)?
            .next()
            .ok_or(TextGridError::MissingTextGrid)?;

        // The text grid is a flat sequence of numbers, texts or flags.
        let mut parsed_items = textgrid.into_inner();

        // Consume and the items for the document
        let header = consume_document_items(&mut parsed_items)?;

        // Map all tier items
        let mut items = Vec::default();
        for _ in 0..header.number_items {
            let i = consume_tier_item(&mut parsed_items)?;
            items.push(i);
        }
        Ok(TextGrid {
            xmin: header.xmin,
            xmax: header.xmax,
            items,
        })
    }
}

fn consume_document_items(items: &mut Pairs<Rule>) -> Result<DocumentHeader> {
    let xmin = items.next().ok_or(TextGridError::MissingValue("xmin"))?;

    let xmax = items.next().ok_or(TextGridError::MissingValue("xmax"))?;

    let mut number_items = 0;

    // Check that this document has a tier
    if let Some(tier_flag) = items.next() {
        if tier_flag.as_rule() == Rule::flag && tier_flag.as_str() == "<exists>" {
            // Get the number of items
            let size = items.next().ok_or(TextGridError::MissingValue("size"))?;
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

fn get_text(v: &Pair<Rule>) -> Result<String> {
    if v.as_rule() == Rule::text {
        let v_str = v.as_str();
        // Remove the prefix/postfix quotation marks and unescape double quotes
        let result = v_str[1..(v_str.len() - 1)]
            .to_string()
            .replace("\"\"", "\"");
        Ok(result)
    } else {
        Err(TextGridError::TextExpected)
    }
}

fn get_number(v: &Pair<Rule>) -> Result<f64> {
    if v.as_rule() == Rule::number {
        let result = v.as_str().parse::<f64>()?;
        Ok(result)
    } else {
        Err(TextGridError::NumberExpected)
    }
}

fn get_integer(v: &Pair<Rule>) -> Result<i64> {
    if v.as_rule() == Rule::number {
        let result = v.as_str().parse::<i64>()?;
        Ok(result)
    } else {
        Err(TextGridError::NumberExpected)
    }
}

fn consume_interval(items: &mut Pairs<Rule>) -> Result<Interval> {
    let xmin = items.next().ok_or(TextGridError::MissingValue("xmin"))?;
    let xmax = items.next().ok_or(TextGridError::MissingValue("xmax"))?;
    let text = items.next().ok_or(TextGridError::MissingValue("text"))?;

    let xmin = get_number(&xmin)?;
    let xmax = get_number(&xmax)?;
    let text = get_text(&text)?;

    let result = Interval { xmin, xmax, text };
    Ok(result)
}

fn consume_point(items: &mut Pairs<Rule>) -> Result<Point> {
    let number = items.next().ok_or(TextGridError::MissingValue("number"))?;
    let mark = items.next().ok_or(TextGridError::MissingValue("mark"))?;

    let number = get_number(&number)?;
    let mark = get_text(&mark)?;
    let result = Point { number, mark };
    Ok(result)
}

fn consume_tier_item(items: &mut Pairs<Rule>) -> Result<TextGridItem> {
    // Get the fields needed for both valid types of items (Interval, Tier)
    let class = items.next().ok_or(TextGridError::MissingValue("class"))?;
    let name = items.next().ok_or(TextGridError::MissingValue("name"))?;
    let xmin = items.next().ok_or(TextGridError::MissingValue("xmin"))?;
    let xmax = items.next().ok_or(TextGridError::MissingValue("xmax"))?;
    let size = items.next().ok_or(TextGridError::MissingValue("size"))?;

    // Convert the fields to the rust types
    let class = get_text(&class)?;
    let name = get_text(&name)?;
    let xmin = get_number(&xmin)?;
    let xmax = get_number(&xmax)?;
    let size = get_integer(&size)?;

    let result = match class.as_str() {
        "IntervalTier" => {
            let mut intervals = Vec::default();
            for _ in 0..size {
                let i = consume_interval(items)?;
                intervals.push(i);
            }
            TextGridItem::Interval {
                name,
                xmin,
                xmax,
                intervals,
            }
        }
        "TextTier" => {
            let mut points = Vec::default();
            for _ in 0..size {
                let p = consume_point(items)?;
                points.push(p);
            }
            TextGridItem::Text {
                name,
                xmin,
                xmax,
                points,
            }
        }
        val => {
            return Err(TextGridError::UnknownItemClass(val.to_string()));
        }
    };
    Ok(result)
}

#[cfg(test)]
mod tests;
