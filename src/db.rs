use std::{io::Read, path::Path};

use anyhow::Context;

use crate::{cursor::Scanner, page::DbHeader, pager, pager::Pager};

pub struct Db {
    pub header: DbHeader,
    pager: Pager,
}

impl Db {
    pub fn from_file(filename: impl AsRef<Path>) -> anyhow::Result<Db> {
        let mut file = std::fs::File::open(filename.as_ref()).context("open db file")?;

        let mut header_buffer = [0; pager::HEADER_SIZE];
        file.read_exact(&mut header_buffer)
            .context("read db header")?;

        let header = pager::parse_header(&header_buffer).context("parse db header")?;

        let pager = Pager::new(file, header.page_size as usize);

        Ok(Db { header, pager })
    }

    pub fn scanner(&mut self, page: usize) -> Scanner {
        Scanner::new(&mut self.pager, page)
    }
}
