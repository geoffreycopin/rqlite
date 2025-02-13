use anyhow::{bail, Context, Ok};

use crate::{
    db::Db,
    sql::ast::{self, SelectFrom},
};

use super::operator::{Operator, SeqScan};

pub struct Planner<'d> {
    db: &'d Db,
}

impl<'d> Planner<'d> {
    pub fn new(db: &'d Db) -> Self {
        Self { db }
    }
    pub fn compile(self, statement: &ast::Statement) -> anyhow::Result<Operator> {
        match statement {
            ast::Statement::Select(s) => self.compile_select(s),
            stmt => bail!("unsupported statement: {stmt:?}"),
        }
    }

    fn compile_select(self, select: &ast::SelectStatement) -> anyhow::Result<Operator> {
        let SelectFrom::Table(table_name) = &select.core.from;

        let table = self
            .db
            .tables_metadata
            .iter()
            .find(|m| &m.name == table_name)
            .with_context(|| format!("invalid table name: {table_name}"))?;

        let mut columns = Vec::new();

        for res_col in &select.core.result_columns {
            match res_col {
                ast::ResultColumn::Star => {
                    for i in 0..table.columns.len() {
                        columns.push(i);
                    }
                }
                ast::ResultColumn::Expr(e) => {
                    let ast::Expr::Column(col) = &e.expr;
                    let (index, _) = table
                        .columns
                        .iter()
                        .enumerate()
                        .find(|(_, c)| c.name == col.name)
                        .with_context(|| format!("invalid column name: {}", col.name))?;
                    columns.push(index);
                }
            }
        }

        Ok(Operator::SeqScan(SeqScan::new(
            columns,
            self.db.scanner(table.first_page),
        )))
    }
}
