#[derive(Debug, Copy, Clone)]
pub struct DbHeader {
    pub page_size: u32,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum PageType {
    TableLeaf,
    TableInterior,
}

#[derive(Debug, Copy, Clone)]
pub struct PageHeader {
    pub page_type: PageType,
    pub first_freeblock: u16,
    pub cell_count: u16,
    pub cell_content_offset: u32,
    pub fragmented_bytes_count: u8,
    pub rightmost_pointer: Option<u32>,
}

impl PageHeader {
    pub fn byte_size(&self) -> usize {
        if self.rightmost_pointer.is_some() {
            12
        } else {
            8
        }
    }
}

#[derive(Debug, Clone)]
pub struct Page {
    pub header: PageHeader,
    pub cell_pointers: Vec<CellPointer>,
    pub cells: Vec<Cell>,
}

impl Page {
    pub fn get(&self, n: usize) -> Option<&Cell> {
        self.cells.get(n)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct CellPointer {
    pub index: usize,
    pub offset: u16,
}

#[derive(Debug, Clone)]
pub struct TableLeafCell {
    pub size: i64,
    pub row_id: i64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct TableInteriorCell {
    pub left_child_page: u32,
    pub key: i64,
}

#[derive(Debug, Clone)]
pub enum Cell {
    TableLeaf(TableLeafCell),
    TableInterior(TableInteriorCell),
}

impl From<TableLeafCell> for Cell {
    fn from(cell: TableLeafCell) -> Self {
        Cell::TableLeaf(cell)
    }
}

impl From<TableInteriorCell> for Cell {
    fn from(cell: TableInteriorCell) -> Self {
        Cell::TableInterior(cell)
    }
}
