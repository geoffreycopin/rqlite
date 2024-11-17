use std::borrow::Cow;

use crate::{
    page::{Cell, Page, PageType},
    pager::Pager,
    value::Value,
};

#[derive(Debug, Copy, Clone)]
pub enum RecordFieldType {
    Null,
    I8,
    I16,
    I24,
    I32,
    I48,
    I64,
    Float,
    Zero,
    One,
    String(usize),
    Blob(usize),
}

#[derive(Debug, Clone)]
pub struct RecordField {
    pub offset: usize,
    pub field_type: RecordFieldType,
}

#[derive(Debug, Clone)]
pub struct RecordHeader {
    pub fields: Vec<RecordField>,
}

fn parse_record_header(mut buffer: &[u8]) -> anyhow::Result<RecordHeader> {
    let (varint_size, header_length) = crate::pager::read_varint_at(buffer, 0);
    buffer = &buffer[varint_size as usize..header_length as usize];

    let mut fields = Vec::new();
    let mut current_offset = header_length as usize;

    while !buffer.is_empty() {
        let (discriminant_size, discriminant) = crate::pager::read_varint_at(buffer, 0);
        buffer = &buffer[discriminant_size as usize..];

        let (field_type, field_size) = match discriminant {
            0 => (RecordFieldType::Null, 0),
            1 => (RecordFieldType::I8, 1),
            2 => (RecordFieldType::I16, 2),
            3 => (RecordFieldType::I24, 3),
            4 => (RecordFieldType::I32, 4),
            5 => (RecordFieldType::I48, 6),
            6 => (RecordFieldType::I64, 8),
            7 => (RecordFieldType::Float, 8),
            8 => (RecordFieldType::Zero, 0),
            9 => (RecordFieldType::One, 0),
            n if n >= 12 && n % 2 == 0 => {
                let size = ((n - 12) / 2) as usize;
                (RecordFieldType::Blob(size), size)
            }
            n if n >= 13 && n % 2 == 1 => {
                let size = ((n - 13) / 2) as usize;
                (RecordFieldType::String(size), size)
            }
            n => anyhow::bail!("unsupported field type: {}", n),
        };

        fields.push(RecordField {
            offset: current_offset,
            field_type,
        });

        current_offset += field_size;
    }

    Ok(RecordHeader { fields })
}

#[derive(Debug)]
pub struct Cursor {
    header: RecordHeader,
    payload: Vec<u8>,
}

impl Cursor {
    pub fn field(&mut self, n: usize) -> Option<Value> {
        let record_field = self.header.fields.get(n)?;

        match record_field.field_type {
            RecordFieldType::Null => Some(Value::Null),
            RecordFieldType::I8 => Some(Value::Int(read_i8_at(&self.payload, record_field.offset))),
            RecordFieldType::I16 => {
                Some(Value::Int(read_i16_at(&self.payload, record_field.offset)))
            }
            RecordFieldType::I24 => {
                Some(Value::Int(read_i24_at(&self.payload, record_field.offset)))
            }
            RecordFieldType::I32 => {
                Some(Value::Int(read_i32_at(&self.payload, record_field.offset)))
            }
            RecordFieldType::I48 => {
                Some(Value::Int(read_i48_at(&self.payload, record_field.offset)))
            }
            RecordFieldType::I64 => {
                Some(Value::Int(read_i64_at(&self.payload, record_field.offset)))
            }
            RecordFieldType::Float => Some(Value::Float(read_f64_at(
                &self.payload,
                record_field.offset,
            ))),
            RecordFieldType::String(length) => {
                let value = std::str::from_utf8(
                    &self.payload[record_field.offset..record_field.offset + length],
                )
                .expect("invalid utf8");
                Some(Value::String(Cow::Borrowed(value)))
            }
            RecordFieldType::Blob(length) => {
                let value = &self.payload[record_field.offset..record_field.offset + length];
                Some(Value::Blob(Cow::Borrowed(value)))
            }
            _ => panic!("unimplemented"),
        }
    }
}

fn read_i8_at(input: &[u8], offset: usize) -> i64 {
    input[offset] as i64
}

fn read_i16_at(input: &[u8], offset: usize) -> i64 {
    i16::from_be_bytes(input[offset..offset + 2].try_into().unwrap()) as i64
}

fn read_i24_at(input: &[u8], offset: usize) -> i64 {
    (i32::from_be_bytes(input[offset..offset + 3].try_into().unwrap()) & 0x00FFFFFF) as i64
}

fn read_i32_at(input: &[u8], offset: usize) -> i64 {
    i32::from_be_bytes(input[offset..offset + 4].try_into().unwrap()) as i64
}

fn read_i48_at(input: &[u8], offset: usize) -> i64 {
    i64::from_be_bytes(input[offset..offset + 6].try_into().unwrap()) & 0x0000FFFFFFFFFFFF
}

fn read_i64_at(input: &[u8], offset: usize) -> i64 {
    i64::from_be_bytes(input[offset..offset + 8].try_into().unwrap())
}

fn read_f64_at(input: &[u8], offset: usize) -> f64 {
    f64::from_be_bytes(input[offset..offset + 8].try_into().unwrap())
}

#[derive(Debug)]
pub struct PositionedPage {
    pub page: Page,
    pub cell: usize,
}

impl PositionedPage {
    pub fn next_cell(&mut self) -> Option<&Cell> {
        let cell = self.page.get(self.cell);
        self.cell += 1;
        cell
    }

    pub fn next_page(&mut self) -> Option<u32> {
        if self.page.header.page_type == PageType::TableInterior
            && self.cell == self.page.cells.len()
        {
            self.cell += 1;
            self.page.header.rightmost_pointer
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct Scanner<'p> {
    pager: &'p mut Pager,
    initial_page: usize,
    page_stack: Vec<PositionedPage>,
}

impl<'p> Scanner<'p> {
    pub fn new(pager: &'p mut Pager, page: usize) -> Scanner<'p> {
        Scanner {
            pager,
            initial_page: page,
            page_stack: Vec::new(),
        }
    }

    pub fn next_record(&mut self) -> anyhow::Result<Option<Cursor>> {
        loop {
            match self.next_elem() {
                Ok(Some(ScannerElem::Cursor(cursor))) => return Ok(Some(cursor)),
                Ok(Some(ScannerElem::Page(page_num))) => {
                    let new_page = self.pager.read_page(page_num as usize)?.clone();
                    self.page_stack.push(PositionedPage {
                        page: new_page,
                        cell: 0,
                    });
                }
                Ok(None) if self.page_stack.len() > 1 => {
                    self.page_stack.pop();
                }
                Ok(None) => return Ok(None),
                Err(e) => return Err(e),
            }
        }
    }

    fn next_elem(&mut self) -> anyhow::Result<Option<ScannerElem>> {
        let Some(page) = self.current_page()? else {
            return Ok(None);
        };

        if let Some(page) = page.next_page() {
            return Ok(Some(ScannerElem::Page(page)));
        }

        let Some(cell) = page.next_cell() else {
            return Ok(None);
        };

        match cell {
            Cell::TableLeaf(cell) => {
                let header = parse_record_header(&cell.payload)?;
                Ok(Some(ScannerElem::Cursor(Cursor {
                    header,
                    payload: cell.payload.clone(),
                })))
            }
            Cell::TableInterior(cell) => Ok(Some(ScannerElem::Page(cell.left_child_page))),
        }
    }

    fn current_page(&mut self) -> anyhow::Result<Option<&mut PositionedPage>> {
        if self.page_stack.is_empty() {
            let page = match self.pager.read_page(self.initial_page) {
                Ok(page) => page.clone(),
                Err(e) => return Err(e),
            };

            self.page_stack.push(PositionedPage { page, cell: 0 });
        }

        Ok(self.page_stack.last_mut())
    }
}

#[derive(Debug)]
enum ScannerElem {
    Page(u32),
    Cursor(Cursor),
}
