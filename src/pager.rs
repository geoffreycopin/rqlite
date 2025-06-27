use std::{
    collections::HashMap,
    io::{Read, Seek, SeekFrom},
    sync::{Arc, Mutex, RwLock},
};

use anyhow::{anyhow, Context};

use crate::page;

pub const HEADER_SIZE: usize = 100;
const HEADER_PREFIX: &[u8] = b"SQLite format 3\0";
const HEADER_PAGE_SIZE_OFFSET: usize = 16;

const PAGE_MAX_SIZE: u32 = 65536;

const PAGE_LEAF_TABLE_ID: u8 = 0x0d;
const PAGE_INTERIOR_TABLE_ID: u8 = 0x05;

const PAGE_FIRST_FREEBLOCK_OFFSET: usize = 1;
const PAGE_CELL_COUNT_OFFSET: usize = 3;
const PAGE_CELL_CONTENT_OFFSET: usize = 5;
const PAGE_FRAGMENTED_BYTES_COUNT_OFFSET: usize = 7;
const PAGE_RIGHTMOST_POINTER_OFFSET: usize = 8;

#[derive(Debug)]
pub struct Pager<I: Read + Seek = std::fs::File> {
    input: Arc<Mutex<I>>,
    page_size: usize,
    pages: Arc<RwLock<HashMap<usize, Arc<page::Page>>>>,
}

impl<I: Read + Seek> Pager<I> {
    pub fn new(input: I, page_size: usize) -> Self {
        Self {
            input: Arc::new(Mutex::new(input)),
            page_size,
            pages: Arc::default(),
        }
    }

    pub fn read_page(&self, n: usize) -> anyhow::Result<Arc<page::Page>> {
        {
            let read_pages = self
                .pages
                .read()
                .map_err(|_| anyhow!("failed to acquire pager read lock"))?;

            if let Some(page) = read_pages.get(&n) {
                return Ok(page.clone());
            }
        }

        let mut write_pages = self
            .pages
            .write()
            .map_err(|_| anyhow!("failed to acquire pager write lock"))?;

        if let Some(page) = write_pages.get(&n) {
            return Ok(page.clone());
        }

        let page = self.load_page(n)?;
        write_pages.insert(n, page.clone());
        Ok(page)
    }

    fn load_page(&self, n: usize) -> anyhow::Result<Arc<page::Page>> {
        let offset = n.saturating_sub(1) * self.page_size;

        let mut input_guard = self
            .input
            .lock()
            .map_err(|_| anyhow!("failed to lock pager mutex"))?;

        input_guard
            .seek(SeekFrom::Start(offset as u64))
            .context("seek to page start")?;

        let mut buffer = vec![0; self.page_size];
        input_guard.read_exact(&mut buffer).context("read page")?;

        Ok(Arc::new(parse_page(&buffer, n)?))
    }
}

impl Clone for Pager {
    fn clone(&self) -> Self {
        Self {
            input: self.input.clone(),
            page_size: self.page_size,
            pages: self.pages.clone(),
        }
    }
}

pub fn parse_header(buffer: &[u8]) -> anyhow::Result<page::DbHeader> {
    if !buffer.starts_with(HEADER_PREFIX) {
        let prefix = String::from_utf8_lossy(&buffer[..HEADER_PREFIX.len()]);
        anyhow::bail!("invalid header prefix: {prefix}");
    }

    let page_size_raw = read_be_word_at(buffer, HEADER_PAGE_SIZE_OFFSET);
    let page_size = match page_size_raw {
        1 => PAGE_MAX_SIZE,
        n if n.is_power_of_two() => n as u32,
        _ => anyhow::bail!("page size is not a power of 2: {}", page_size_raw),
    };

    Ok(page::DbHeader { page_size })
}

fn parse_page(buffer: &[u8], page_num: usize) -> anyhow::Result<page::Page> {
    let ptr_offset = if page_num == 1 { HEADER_SIZE as u16 } else { 0 };
    let content_buffer = &buffer[ptr_offset as usize..];
    let header = parse_page_header(content_buffer)?;
    let cell_pointers = parse_cell_pointers(
        &content_buffer[header.byte_size()..],
        header.cell_count as usize,
        ptr_offset,
    );

    let cells_parsing_fn = match header.page_type {
        page::PageType::TableLeaf => parse_table_leaf_cell,
        page::PageType::TableInterior => parse_table_interior_cell,
    };

    let cells = parse_cells(content_buffer, &cell_pointers, cells_parsing_fn)?;

    Ok(page::Page {
        header,
        cell_pointers,
        cells,
    })
}

fn parse_cells(
    buffer: &[u8],
    cell_pointers: &[u16],
    parse_fn: impl Fn(&[u8]) -> anyhow::Result<page::Cell>,
) -> anyhow::Result<Vec<page::Cell>> {
    cell_pointers
        .iter()
        .map(|&ptr| parse_fn(&buffer[ptr as usize..]))
        .collect()
}

fn parse_table_leaf_cell(mut buffer: &[u8]) -> anyhow::Result<page::Cell> {
    let (n, size) = read_varint_at(buffer, 0);
    buffer = &buffer[n as usize..];

    let (n, row_id) = read_varint_at(buffer, 0);
    buffer = &buffer[n as usize..];

    let payload = buffer[..size as usize].to_vec();

    Ok(page::TableLeafCell {
        size,
        row_id,
        payload,
    }
    .into())
}

fn parse_table_interior_cell(mut buffer: &[u8]) -> anyhow::Result<page::Cell> {
    let left_child_page = read_be_double_at(buffer, 0);
    buffer = &buffer[4..];

    let (_, key) = read_varint_at(buffer, 0);

    Ok(page::TableInteriorCell {
        left_child_page,
        key,
    }
    .into())
}

fn parse_page_header(buffer: &[u8]) -> anyhow::Result<page::PageHeader> {
    let (page_type, rightmost_ptr) = match buffer[0] {
        PAGE_LEAF_TABLE_ID => (page::PageType::TableLeaf, false),
        PAGE_INTERIOR_TABLE_ID => (page::PageType::TableInterior, true),
        _ => anyhow::bail!("unknown page type: {}", buffer[0]),
    };

    let first_freeblock = read_be_word_at(buffer, PAGE_FIRST_FREEBLOCK_OFFSET);
    let cell_count = read_be_word_at(buffer, PAGE_CELL_COUNT_OFFSET);
    let cell_content_offset = match read_be_word_at(buffer, PAGE_CELL_CONTENT_OFFSET) {
        0 => 65536,
        n => n as u32,
    };
    let fragmented_bytes_count = buffer[PAGE_FRAGMENTED_BYTES_COUNT_OFFSET];

    let rightmost_pointer = if rightmost_ptr {
        Some(read_be_double_at(buffer, PAGE_RIGHTMOST_POINTER_OFFSET))
    } else {
        None
    };

    Ok(page::PageHeader {
        page_type,
        first_freeblock,
        cell_count,
        cell_content_offset,
        fragmented_bytes_count,
        rightmost_pointer,
    })
}

fn parse_cell_pointers(buffer: &[u8], n: usize, ptr_offset: u16) -> Vec<u16> {
    let mut pointers = Vec::with_capacity(n);
    for i in 0..n {
        pointers.push(read_be_word_at(buffer, 2 * i) - ptr_offset);
    }
    pointers
}

pub fn read_varint_at(buffer: &[u8], mut offset: usize) -> (u8, i64) {
    let mut size = 0;
    let mut result = 0;

    while size < 9 {
        let current_byte = buffer[offset] as i64;
        if size == 8 {
            result = (result << 8) | current_byte;
        } else {
            result = (result << 7) | (current_byte & 0b0111_1111);
        }

        offset += 1;
        size += 1;

        if current_byte & 0b1000_0000 == 0 {
            break;
        }
    }

    (size, result)
}

fn read_be_double_at(input: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(input[offset..offset + 4].try_into().unwrap())
}

fn read_be_word_at(input: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes(input[offset..offset + 2].try_into().unwrap())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn short_varint() {
        let buffer = [0b0000_0001];
        assert_eq!(read_varint_at(&buffer, 0), (1, 1));
    }

    #[test]
    fn middle_varint() {
        let buffer = [0b1000_0001, 0b0111_1111];
        assert_eq!(read_varint_at(&buffer, 0), (2, 255));
    }

    #[test]
    fn long_varint() {
        let buffer = [
            0b1000_0000,
            0b1111_1111,
            0b1000_0000,
            0b1000_0000,
            0b1000_0000,
            0b1000_0000,
            0b1000_0000,
            0b1000_0000,
            0b0110_1101,
        ];
        assert_eq!(
            read_varint_at(&buffer, 0),
            (
                9,
                0b00000001_11111100_00000000_00000000_00000000_00000000_00000000_01101101,
            )
        );
    }

    #[test]
    fn minus_one() {
        let buffer = [
            0b1111_1111,
            0b1111_1111,
            0b1111_1111,
            0b1111_1111,
            0b1111_1111,
            0b1111_1111,
            0b1111_1111,
            0b1111_1111,
            0b1111_1111,
        ];
        assert_eq!(read_varint_at(&buffer, 0), (9, -1));
    }
}
