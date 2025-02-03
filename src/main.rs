use std::io::{stdin, BufRead, Write};

use anyhow::Context;

mod cursor;
mod db;
mod page;
mod pager;
mod sql;
mod value;

fn main() -> anyhow::Result<()> {
    let database = db::Db::from_file(std::env::args().nth(1).context("missing db file")?)?;
    cli(database)
}

fn cli(mut db: db::Db) -> anyhow::Result<()> {
    print_flushed("rqlite> ")?;

    let mut line_buffer = String::new();

    while stdin().lock().read_line(&mut line_buffer).is_ok() {
        match line_buffer.trim() {
            ".exit" => break,
            ".tables" => display_tables(&mut db)?,
            stmt => match sql::parse_statement(stmt, true) {
                Ok(stmt) => {
                    println!("{:?}", stmt);
                }
                Err(e) => {
                    println!("Error: {}", e);
                }
            },
        }

        print_flushed("\nrqlite> ")?;

        line_buffer.clear();
    }

    Ok(())
}

fn display_tables(db: &mut db::Db) -> anyhow::Result<()> {
    for table in &db.tables_metadata {
        print!("{} ", &table.name)
    }
    Ok(())
}

fn print_flushed(s: &str) -> anyhow::Result<()> {
    print!("{}", s);
    std::io::stdout().flush().context("flush stdout")
}
