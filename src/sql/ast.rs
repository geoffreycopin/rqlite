#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SelectStatement {
    pub core: SelectCore,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SelectCore {
    pub result_columns: Vec<ResultColumn>,
    pub from: SelectFrom,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ResultColumn {
    Star,
    Expr(Expr),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Expr {
    Column(Column),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Column {
    pub name: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SelectFrom {
    Table(String),
}
