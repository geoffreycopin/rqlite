use anyhow::Context;

use crate::{cursor::Scanner, value::OwnedValue};

#[derive(Debug)]
pub enum Operator {
    SeqScan(SeqScan),
}

impl Operator {
    pub fn next_row(&mut self) -> anyhow::Result<Option<&[OwnedValue]>> {
        match self {
            Operator::SeqScan(s) => s.next_row(),
        }
    }
}

#[derive(Debug)]
pub struct SeqScan {
    fields: Vec<usize>,
    scanner: Scanner,
    row_buffer: Vec<OwnedValue>,
}

impl SeqScan {
    pub fn new(fields: Vec<usize>, scanner: Scanner) -> Self {
        let row_buffer = vec![OwnedValue::Null; fields.len()];

        Self {
            fields,
            scanner,
            row_buffer,
        }
    }

    fn next_row(&mut self) -> anyhow::Result<Option<&[OwnedValue]>> {
        let Some(record) = self.scanner.next_record()? else {
            return Ok(None);
        };

        for (i, &n) in self.fields.iter().enumerate() {
            self.row_buffer[i] = record.owned_field(n).context("missing record field")?;
        }

        Ok(Some(&self.row_buffer))
    }
}
