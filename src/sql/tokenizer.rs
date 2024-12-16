use anyhow::bail;

#[derive(Debug, Eq, PartialEq)]
pub enum Token {
    Create,
    Table,
    Select,
    As,
    From,
    LPar,
    RPar,
    Star,
    Comma,
    SemiColon,
    Identifier(String),
}

impl Token {
    pub fn as_identifier(&self) -> Option<&str> {
        match self {
            Token::Identifier(ident) => Some(ident),
            _ => None,
        }
    }
}

pub fn tokenize(input: &str) -> anyhow::Result<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '(' => tokens.push(Token::LPar),
            ')' => tokens.push(Token::RPar),
            '*' => tokens.push(Token::Star),
            ',' => tokens.push(Token::Comma),
            ';' => tokens.push(Token::SemiColon),
            c if c.is_whitespace() => continue,
            c if c.is_alphabetic() => {
                let mut ident = c.to_string().to_lowercase();
                while let Some(cc) = chars.next_if(|&cc| cc.is_alphanumeric() || cc == '_') {
                    ident.extend(cc.to_lowercase());
                }

                match ident.as_str() {
                    "create" => tokens.push(Token::Create),
                    "table" => tokens.push(Token::Table),
                    "select" => tokens.push(Token::Select),
                    "as" => tokens.push(Token::As),
                    "from" => tokens.push(Token::From),
                    _ => tokens.push(Token::Identifier(ident)),
                }
            }
            _ => bail!("unexpected character: {}", c),
        }
    }

    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_select() {
        let input = "SeLect *, col as c FroM TableName_1;";
        let expected = vec![
            Token::Select,
            Token::Star,
            Token::Comma,
            Token::Identifier("col".to_string()),
            Token::As,
            Token::Identifier("c".to_string()),
            Token::From,
            Token::Identifier("tablename_1".to_string()),
            Token::SemiColon,
        ];
        assert_eq!(tokenize(input).unwrap(), expected);
    }

    #[test]
    fn tokenize_invalid_char() {
        let input = "select @ from table;";
        assert!(tokenize(input).is_err());
    }
}
