#[derive(Debug, Copy, Clone)]
pub struct DbHeader {
    pub page_size: u32,
}

#[derive(Debug, Copy, Clone)]
pub enum PageType {
    TableLeaf,
}

#[derive(Debug, Copy, Clone)]
pub struct PageHeader {
    pub page_type: PageType,
    pub first_freeblock: u16,
    pub cell_count: u16,
    pub cell_content_offset: u32,
    pub fragmented_bytes_count: u8,
}

#[derive(Debug, Clone)]
pub struct PageData<C> {
    pub header: PageHeader,
    pub cell_pointers: Vec<u16>,
    pub cells: Vec<C>,
}

#[derive(Debug, Clone)]
pub struct TableLeafCell {
    pub size: i64,
    pub row_id: i64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum Page {
    TableLeaf(PageData<TableLeafCell>),
}
