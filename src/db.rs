use std::{io::Read, path::Path};

use anyhow::Context;

use crate::{
    cursor::{Cursor, Scanner},
    page::DbHeader,
    pager::{self, Pager},
    sql::{self, ast, ast::CreateTableStatement},
};

#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub name: String,
    pub columns: Vec<ast::ColumnDef>,
    pub first_page: usize,
}

impl TryFrom<Cursor> for Option<TableMetadata> {
    type Error = anyhow::Error;

    fn try_from(cursor: Cursor) -> Result<Self, Self::Error> {
        let type_value = cursor
            .field(0)
            .context("missing type field")
            .context("invalid type field")?;

        if type_value.as_str() != Some("table") {
            return Ok(None);
        }

        let create_stmt = cursor
            .field(4)
            .context("missing create statement")
            .context("invalid create statement")?
            .as_str()
            .context("table create statement should be a string")?
            .to_owned();

        let create = sql::parse_create_statement(&create_stmt)?;

        let first_page = cursor
            .field(3)
            .context("missing table first page")?
            .as_int()
            .context("table first page should be an integer")? as usize;

        Ok(Some(TableMetadata {
            name: create.name,
            columns: create.columns,
            first_page,
        }))
    }
}

impl From<CreateTableStatement> for TableMetadata {
    fn from(value: CreateTableStatement) -> Self {
        TableMetadata {
            name: value.name,
            columns: value.columns,
            first_page: 0,
        }
    }
}

pub struct Db {
    pub header: DbHeader,
    pub tables_metadata: Vec<TableMetadata>,
    pager: Pager,
}

impl Db {
    pub fn from_file(filename: impl AsRef<Path>) -> anyhow::Result<Db> {
        let mut file = std::fs::File::open(filename.as_ref()).context("open db file")?;

        let mut header_buffer = [0; pager::HEADER_SIZE];
        file.read_exact(&mut header_buffer)
            .context("read db header")?;

        let header = pager::parse_header(&header_buffer).context("parse db header")?;

        let tables_metadata = Self::collect_tables_metadata(&mut Pager::new(
            file.try_clone()?,
            header.page_size as usize,
        ))?;

        let pager = Pager::new(file, header.page_size as usize);

        Ok(Db {
            header,
            pager,
            tables_metadata,
        })
    }

    pub fn scanner(&mut self, page: usize) -> Scanner {
        Scanner::new(&mut self.pager, page)
    }

    fn collect_tables_metadata(pager: &mut Pager) -> anyhow::Result<Vec<TableMetadata>> {
        let mut metadata = Vec::new();
        let mut scanner = Scanner::new(pager, 1);

        while let Some(record) = scanner.next_record()? {
            if let Some(m) = record.try_into()? {
                metadata.push(m);
            }
        }

        Ok(metadata)
    }
}
