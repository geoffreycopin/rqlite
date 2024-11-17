use anyhow::{bail, Context};

use crate::sql::{
    ast,
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

    fn parse_statement(&mut self) -> anyhow::Result<ast::Statement> {
        Ok(ast::Statement::Select(self.parse_select()?))
    }

    fn parse_select(&mut self) -> anyhow::Result<ast::SelectStatement> {
        self.expect_eq(Token::Select)?;
        let result_columns = self.parse_result_columns()?;
        self.expect_eq(Token::From)?;
        let from = self.parse_select_from()?;
        Ok(ast::SelectStatement {
            core: ast::SelectCore {
                result_columns,
                from,
            },
        })
    }

    fn parse_select_from(&mut self) -> anyhow::Result<ast::SelectFrom> {
        let table = self.expect_identifier()?;
        Ok(ast::SelectFrom::Table(table.to_string()))
    }

    fn parse_result_columns(&mut self) -> anyhow::Result<Vec<ast::ResultColumn>> {
        let mut result_coluns = vec![self.parse_result_column()?];
        while self.current_token_is(Token::Comma) {
            self.advance();
            result_coluns.push(self.parse_result_column()?);
        }
        Ok(result_coluns)
    }

    fn parse_result_column(&mut self) -> anyhow::Result<ast::ResultColumn> {
        match self.peak_next_token()? {
            Token::Star => {
                self.advance();
                Ok(ast::ResultColumn::Star)
            }
            _ => Ok(ast::ResultColumn::Expr(self.parse_expr()?)),
        }
    }

    fn parse_expr(&mut self) -> anyhow::Result<ast::Expr> {
        let identifier = self.expect_identifier()?;
        Ok(ast::Expr::Column(ast::Column {
            name: identifier.to_string(),
        }))
    }

    fn current_token_is(&self, expected: Token) -> bool {
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

pub fn parse_statement(input: &str) -> anyhow::Result<ast::Statement> {
    let tokens = tokenizer::tokenize(input)?;
    let mut state = ParserState::new(tokens);
    let statement = state.parse_statement()?;
    state.expect_eq(Token::SemiColon)?;
    Ok(statement)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_star_from_table() {
        let input = "select * from table1;";
        let statement = parse_statement(input).unwrap();
        assert_eq!(
            statement,
            ast::Statement::Select(ast::SelectStatement {
                core: ast::SelectCore {
                    result_columns: vec![ast::ResultColumn::Star],
                    from: ast::SelectFrom::Table("table1".to_string()),
                },
            })
        );
    }

    #[test]
    fn select_columns_from_table() {
        let input = "select col1, col2 from table;";
        let statement = parse_statement(input).unwrap();
        assert_eq!(
            statement,
            ast::Statement::Select(ast::SelectStatement {
                core: ast::SelectCore {
                    result_columns: vec![
                        ast::ResultColumn::Expr(ast::Expr::Column(ast::Column {
                            name: "col1".to_string(),
                        })),
                        ast::ResultColumn::Expr(ast::Expr::Column(ast::Column {
                            name: "col2".to_string(),
                        })),
                    ],
                    from: ast::SelectFrom::Table("table".to_string()),
                },
            })
        );
    }
}
