use anyhow::{bail, Context};

use crate::sql::{
    ast::{
        Column, ColumnDef, CreateTableStatement, Expr, ExprResultColumn, ResultColumn, SelectCore,
        SelectFrom, SelectStatement, Statement, Type,
    },
    tokenizer::{self, Token},
};

#[derive(Debug)]
struct ParserState {
    tokens: Vec<Token>,
    pos: usize,
}

impl ParserState {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn parse_statement(&mut self) -> anyhow::Result<Statement> {
        match self.peak_next_token().context("unexpected end of input")? {
            Token::Select => self.parse_select().map(Statement::Select),
            Token::Create => self.parse_create_table().map(Statement::CreateTable),
            token => bail!("unexpected token: {token:?}"),
        }
    }

    fn parse_create_table(&mut self) -> anyhow::Result<CreateTableStatement> {
        self.expect_eq(Token::Create)?;
        self.expect_eq(Token::Table)?;
        let name = self.expect_identifier()?.to_string();
        self.expect_eq(Token::LPar)?;
        let mut columns = vec![self.parse_column_def()?];
        while self.next_token_is(Token::Comma) {
            self.advance();
            columns.push(self.parse_column_def()?);
        }
        self.expect_eq(Token::RPar)?;
        Ok(CreateTableStatement { name, columns })
    }

    fn parse_column_def(&mut self) -> anyhow::Result<ColumnDef> {
        Ok(ColumnDef {
            name: self.expect_identifier()?.to_string(),
            col_type: self.parse_type()?,
        })
    }

    fn parse_type(&mut self) -> anyhow::Result<Type> {
        let type_name = self.expect_identifier()?;
        let t = match type_name.to_lowercase().as_str() {
            "integer" => Type::Integer,
            "real" => Type::Real,
            "blob" => Type::Blob,
            "text" | "string" => Type::Text,
            _ => bail!("unsupported type: {type_name}"),
        };
        Ok(t)
    }

    fn parse_select(&mut self) -> anyhow::Result<SelectStatement> {
        self.expect_eq(Token::Select)?;
        let result_columns = self.parse_result_columns()?;
        self.expect_eq(Token::From)?;
        let from = self.parse_select_from()?;
        Ok(SelectStatement {
            core: SelectCore {
                result_columns,
                from,
            },
        })
    }

    fn parse_select_from(&mut self) -> anyhow::Result<SelectFrom> {
        let table = self.expect_identifier()?;
        Ok(SelectFrom::Table(table.to_string()))
    }

    fn parse_result_columns(&mut self) -> anyhow::Result<Vec<ResultColumn>> {
        let mut result_coluns = vec![self.parse_result_column()?];
        while self.next_token_is(Token::Comma) {
            self.advance();
            result_coluns.push(self.parse_result_column()?);
        }
        Ok(result_coluns)
    }

    fn parse_result_column(&mut self) -> anyhow::Result<ResultColumn> {
        if self.peak_next_token()? == &Token::Star {
            self.advance();
            return Ok(ResultColumn::Star);
        }

        Ok(ResultColumn::Expr(self.parse_expr_result_column()?))
    }

    fn parse_expr_result_column(&mut self) -> anyhow::Result<ExprResultColumn> {
        let expr = self.parse_expr()?;
        let alias = if self.next_token_is(Token::As) {
            self.advance();
            Some(self.expect_identifier()?.to_string())
        } else {
            None
        };
        Ok(ExprResultColumn { expr, alias })
    }

    fn parse_expr(&mut self) -> anyhow::Result<Expr> {
        Ok(Expr::Column(Column {
            name: self.expect_identifier()?.to_string(),
        }))
    }

    fn next_token_is(&self, expected: Token) -> bool {
        self.tokens.get(self.pos) == Some(&expected)
    }

    fn expect_identifier(&mut self) -> anyhow::Result<&str> {
        self.expect_matching(|t| matches!(t, Token::Identifier(_)))
            .map(|t| t.as_identifier().unwrap())
    }

    fn expect_eq(&mut self, expected: Token) -> anyhow::Result<&Token> {
        self.expect_matching(|t| *t == expected)
    }

    fn expect_matching(&mut self, f: impl Fn(&Token) -> bool) -> anyhow::Result<&Token> {
        match self.next_token() {
            Some(token) if f(token) => Ok(token),
            Some(token) => bail!("unexpected token: {:?}", token),
            None => bail!("unexpected end of input"),
        }
    }

    fn peak_next_token(&self) -> anyhow::Result<&Token> {
        self.tokens.get(self.pos).context("unexpected end of input")
    }

    fn next_token(&mut self) -> Option<&Token> {
        let token = self.tokens.get(self.pos);
        if token.is_some() {
            self.pos += 1;
        }
        token
    }

    fn advance(&mut self) {
        self.pos += 1;
    }
}

pub fn parse_statement(input: &str) -> anyhow::Result<Statement> {
    let tokens = tokenizer::tokenize(input)?;
    let mut state = ParserState::new(tokens);
    let statement = state.parse_statement()?;
    Ok(statement)
}

pub fn parse_create_statement(input: &str) -> anyhow::Result<CreateTableStatement> {
    match parse_statement(input)? {
        Statement::CreateTable(c) => Ok(c),
        Statement::Select(_) => bail!("expected a create statement"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_table() {
        let input = "create table table1(key integer, value text)";
        let statement = parse_statement(input).unwrap();
        assert_eq!(
            statement,
            Statement::CreateTable(CreateTableStatement {
                name: "table1".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "key".to_string(),
                        col_type: Type::Integer,
                    },
                    ColumnDef {
                        name: "value".to_string(),
                        col_type: Type::Text,
                    }
                ]
            })
        )
    }

    #[test]
    fn select_star_from_table() {
        let input = "select * from table1";
        let statement = parse_statement(input).unwrap();
        assert_eq!(
            statement,
            Statement::Select(SelectStatement {
                core: SelectCore {
                    result_columns: vec![ResultColumn::Star],
                    from: SelectFrom::Table("table1".to_string()),
                },
            })
        );
    }

    #[test]
    fn select_columns_from_table() {
        let input = "select col1 as first, col2 from table1";
        let statement = parse_statement(input).unwrap();
        assert_eq!(
            statement,
            Statement::Select(SelectStatement {
                core: SelectCore {
                    result_columns: vec![
                        ResultColumn::Expr(ExprResultColumn {
                            expr: Expr::Column(Column {
                                name: "col1".to_string()
                            }),
                            alias: Some("first".to_string())
                        }),
                        ResultColumn::Expr(ExprResultColumn {
                            expr: Expr::Column(Column {
                                name: "col2".to_string()
                            }),
                            alias: None
                        }),
                    ],
                    from: SelectFrom::Table("table".to_string()),
                },
            })
        );
    }
}
