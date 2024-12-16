use std::{io::Read, path::Path};

use anyhow::Context;

use crate::sql::ast::CreateTableStatement;
use crate::{
    cursor::Scanner,
    page::DbHeader,
    pager::{self, Pager},
    sql,
};

#[derive(Debug, Clone)]
pub struct TableMetadata {
    name: String,
    columns: Vec<String>,
}

impl From<CreateTableStatement> for TableMetadata {
    fn from(value: CreateTableStatement) -> Self {
        TableMetadata {
            name: value.name,
            columns: value.columns.into_iter().map(|c| c.name).collect(),
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

        while let Some(mut record) = scanner.next_record()? {
            let type_value = record
                .field(0)
                .context("missing type field")
                .context("invalid type field")?;

            if type_value.as_str() == Some("table") {
                let create_stmt = record
                    .field(4)
                    .context("missing create statement")
                    .context("invalid create statement")?
                    .as_str()
                    .context("table create statement should be a string")?
                    .to_owned();

                let create = sql::parse_create_statement(&create_stmt)?;

                metadata.push(create.into());
            }
        }

        Ok(metadata)
    }
}
