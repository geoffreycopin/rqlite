use std::{
    collections::HashMap,
    io::{Read, Seek, SeekFrom},
    sync::{Arc, Mutex, RwLock},
};

use anyhow::{Context, anyhow, bail};

use crate::page::{self, DbHeader, PageHeader};

pub const HEADER_SIZE: usize = 100;
const HEADER_PREFIX: &[u8] = b"SQLite format 3\0";
const HEADER_PAGE_SIZE_OFFSET: usize = 16;
const HEADER_PAGE_RESERVED_SIZE_OFFSET: usize = 20;

const PAGE_MAX_SIZE: u32 = 65536;

const PAGE_LEAF_TABLE_ID: u8 = 0x0d;
const PAGE_INTERIOR_TABLE_ID: u8 = 0x05;

const PAGE_CELL_COUNT_OFFSET: usize = 3;
const PAGE_RIGHTMOST_POINTER_OFFSET: usize = 8;

#[derive(Debug, Clone)]
enum CachedPage {
    Page(Arc<page::Page>),
    Overflow(Arc<page::OverflowPage>),
}

impl From<Arc<page::Page>> for CachedPage {
    fn from(value: Arc<page::Page>) -> Self {
        CachedPage::Page(value)
    }
}

impl TryFrom<CachedPage> for Arc<page::Page> {
    type Error = anyhow::Error;

    fn try_from(value: CachedPage) -> Result<Self, Self::Error> {
        if let CachedPage::Page(p) = value {
            Ok(p.clone())
        } else {
            bail!("expected a regular page")
        }
    }
}

impl From<Arc<page::OverflowPage>> for CachedPage {
    fn from(value: Arc<page::OverflowPage>) -> Self {
        CachedPage::Overflow(value)
    }
}

impl TryFrom<CachedPage> for Arc<page::OverflowPage> {
    type Error = anyhow::Error;

    fn try_from(value: CachedPage) -> Result<Self, Self::Error> {
        if let CachedPage::Overflow(o) = value {
            Ok(o.clone())
        } else {
            bail!("expected an overflow page")
        }
    }
}

#[derive(Debug)]
pub struct Pager<I: Read + Seek = std::fs::File> {
    input: Arc<Mutex<I>>,
    pages: Arc<RwLock<HashMap<usize, CachedPage>>>,
    header: DbHeader,
}

impl<I: Read + Seek> Pager<I> {
    pub fn new(header: DbHeader, input: I) -> Self {
        Self {
            input: Arc::new(Mutex::new(input)),
            pages: Arc::default(),
            header,
        }
    }

    pub fn read_overflow(&self, n: usize) -> anyhow::Result<Arc<page::OverflowPage>> {
        self.load(n, |buffer| Ok(parse_overflow_page(buffer)))
    }

    pub fn read_page(&self, n: usize) -> anyhow::Result<Arc<page::Page>> {
        self.load(n, |buffer| parse_page(&self.header, buffer, n))
    }

    fn load<T>(&self, n: usize, f: impl Fn(&[u8]) -> anyhow::Result<T>) -> anyhow::Result<Arc<T>>
    where
        Arc<T>: Into<CachedPage>,
        CachedPage: TryInto<Arc<T>, Error = anyhow::Error>,
    {
        {
            let read_pages = self
                .pages
                .read()
                .map_err(|_| anyhow!("poisoned page cache lock"))?;

            if let Some(page) = read_pages.get(&n).cloned() {
                return page.try_into();
            }
        }

        let mut write_pages = self
            .pages
            .write()
            .map_err(|_| anyhow!("failed to acquire pager write lock"))?;

        if let Some(page) = write_pages.get(&n).cloned() {
            return page.try_into();
        }

        let buffer = self.load_raw(n)?;
        let parsed = f(&buffer[0..self.header.usable_page_size()])?;
        let ptr = Arc::new(parsed);

        write_pages.insert(n, ptr.clone().into());

        Ok(ptr)
    }

    fn load_raw(&self, n: usize) -> anyhow::Result<Vec<u8>> {
        let offset = n.saturating_sub(1) * self.header.page_size as usize;

        let mut input_guard = self
            .input
            .lock()
            .map_err(|_| anyhow!("poisoned pager mutex"))?;

        input_guard
            .seek(SeekFrom::Start(offset as u64))
            .context("seek to page start")?;

        let mut buffer = vec![0; self.header.page_size as usize];
        input_guard.read_exact(&mut buffer).context("read page")?;

        Ok(buffer)
    }
}

impl Clone for Pager {
    fn clone(&self) -> Self {
        Self {
            input: self.input.clone(),
            pages: self.pages.clone(),
            header: self.header,
        }
    }
}

fn parse_overflow_page(buffer: &[u8]) -> page::OverflowPage {
    let next = read_be_double_at(buffer, 0);
    page::OverflowPage {
        payload: buffer[4..].to_vec(),
        next: if next != 0 { Some(next as usize) } else { None },
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

    let page_reserved_size = buffer[HEADER_PAGE_RESERVED_SIZE_OFFSET];

    Ok(page::DbHeader {
        page_size,
        page_reserved_size,
    })
}

fn parse_page(db_header: &DbHeader, buffer: &[u8], page_num: usize) -> anyhow::Result<page::Page> {
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

    let cells = parse_cells(
        db_header,
        &header,
        content_buffer,
        &cell_pointers,
        cells_parsing_fn,
    )?;

    Ok(page::Page { header, cells })
}

fn parse_cells(
    db_header: &DbHeader,
    header: &PageHeader,
    buffer: &[u8],
    cell_pointers: &[u16],
    parse_fn: impl Fn(&DbHeader, &PageHeader, &[u8]) -> anyhow::Result<page::Cell>,
) -> anyhow::Result<Vec<page::Cell>> {
    cell_pointers
        .iter()
        .map(|&ptr| parse_fn(db_header, header, &buffer[ptr as usize..]))
        .collect()
}

fn parse_table_leaf_cell(
    db_header: &DbHeader,
    header: &PageHeader,
    mut buffer: &[u8],
) -> anyhow::Result<page::Cell> {
    let (n, size) = read_varint_at(buffer, 0);
    buffer = &buffer[n as usize..];

    let (n, _) = read_varint_at(buffer, 0);
    buffer = &buffer[n as usize..];

    let (local_size, overflow_size) = header.local_and_overflow_size(db_header, size as usize)?;
    let first_overflow = overflow_size.map(|_| read_be_double_at(buffer, local_size) as usize);

    let payload = buffer[..local_size].to_vec();

    Ok(page::TableLeafCell {
        payload,
        first_overflow,
    }
    .into())
}

fn parse_table_interior_cell(
    _: &DbHeader,
    _: &PageHeader,
    buffer: &[u8],
) -> anyhow::Result<page::Cell> {
    Ok(page::TableInteriorCell {
        left_child_page: read_be_double_at(buffer, 0),
    }
    .into())
}

fn parse_page_header(buffer: &[u8]) -> anyhow::Result<page::PageHeader> {
    let (page_type, rightmost_ptr) = match buffer[0] {
        PAGE_LEAF_TABLE_ID => (page::PageType::TableLeaf, false),
        PAGE_INTERIOR_TABLE_ID => (page::PageType::TableInterior, true),
        _ => anyhow::bail!("unknown page type: {}", buffer[0]),
    };

    let cell_count = read_be_word_at(buffer, PAGE_CELL_COUNT_OFFSET);

    let rightmost_pointer = if rightmost_ptr {
        Some(read_be_double_at(buffer, PAGE_RIGHTMOST_POINTER_OFFSET))
    } else {
        None
    };

    Ok(page::PageHeader {
        page_type,
        cell_count,
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

pub fn read_be_double_at(input: &[u8], offset: usize) -> u32 {
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
