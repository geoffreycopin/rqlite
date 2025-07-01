### Build your own SQLite, Part 4: reading tables metadata

As we saw in the [opening post](/build-your-own-sqlite-part-1-listing-tables),
SQLite stores metadata about tables in a special "schema table" starting on page 1.
We've been reading records from this table to list the tables in the current database,
but before we can start evaluating SQL queries against user-defined tables, we need to
extract more information from the schema table.

For each table, we need to know:

* the table name
* the root page
* the name and type of each column

The first two are very easy to extract, as they are directly stored in fields 1 and 3
of the schema table's records. But column names and types will be a bit trickier, as they are
not neatly separated into record fields, but are stored in a single field in the
form of a `CREATE TABLE` statement that we'll need to parse.

The complete source code is available
on [GitHub](https://github.com/geoffreycopin/rqlite/tree/4e098ca03b814448eb1a2650d64cda12227e9300).

## Parsing `CREATE TABLE` statements

The first step in extending our SQL parser to support `CREATE TABLE` statements it to
add the necessary token types to the tokenizer. We'll support `CREATE TABLE` statements
of the following form:

```sql
CREATE TABLE table_name
(
    column1_name column1_type,
    column2_name column2_type, .
    .
    .
)
```

The following tokens are new and need to be added to the `Token` enum: `CREATE`, `TABLE`, `(`, `)`.

```diff
// sql/tokenizer.rs

#[derive(Debug, Eq, PartialEq)]
pub enum Token {
+   Create,
+   Table,
    Select,
    As,
    From,
+   LPar,
+   RPar,
    Star,
    Comma,
    SemiColon,
    Identifier(String),
}

//[...]

pub fn tokenize(input: &str) -> anyhow::Result<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
+           '(' => tokens.push(Token::LPar),
+           ')' => tokens.push(Token::RPar),
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
+                   "create" => tokens.push(Token::Create),
+                   "table" => tokens.push(Token::Table),
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
```

Next, we need to extend our AST to represent the new statement type.
Our representation will be based on the [SQLite documentation](https://www.sqlite.org/lang_createtable.html).

```diff
// sql/ast.rs

//[...]

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
+   CreateTable(CreateTableStatement),
}
+
+#[derive(Debug, Clone, Eq, PartialEq)]
+pub struct CreateTableStatement {
+    pub name: String,
+    pub columns: Vec<ColumnDef>,
+}
+
+#[derive(Debug, Clone, Eq, PartialEq)]
+pub struct ColumnDef {
+    pub name: String,
+    pub col_type: Type,
+}
+
+#[derive(Debug, Clone, Eq, PartialEq)]
+pub enum Type {
+    Integer,
+    Real,
+    Text,
+    Blob,
+}

//[...]
```

Parsing types is straightforward: we can simply match the incoming identifier
token with a predefined set of types. For now, we'll restrict ourselves to
`INTEGER`, `REAL`, `TEXT`, `STRING`, and `BLOB`.
Once our `parse_type` method is implemented, constructing `ColumnDef` nodes
is trivial.

```rust
// sql/parser.rs

//[...]
impl ParserState {
    // [...]
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
    // [...]
}

//[...]
```

In our implementation if the `parse_create_table` method, we'll parse column definitions
using the same pattern as in the `parse_result_colums` method:

```rust
// sql/parser.rs

//[...]
impl ParserState {
    // [...]
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
    // [...]
}
//[...]
```

Finally, we need to update the `parse_statement` method to handle the new statement type.
We'll also update the `parse_statement` utility function to make the semicolon terminator
optional, as the `CREATE TABLE` statements stored in the schema table lack a trailing semicolon.

```diff
// sql/parser.rs

//[...]

impl ParserState {
    // [...]
        
    fn parse_statement(&mut self) -> anyhow::Result<Statement> {
-       Ok(ast::Statement::Select(self.parse_select()?))
+       match self.peak_next_token().context("unexpected end of input")? {
+           Token::Select => self.parse_select().map(Statement::Select),
+           Token::Create => self.parse_create_table().map(Statement::CreateTable),
+           token => bail!("unexpected token: {token:?}"),
+       }
    }    
        
    // [...]
}

// [...]

-pub fn parse_statement(input: &str) -> anyhow::Result<Statement> {
+pub fn parse_statement(input: &str, trailing_semicolon: bool) -> anyhow::Result<Statement> {
    let tokens = tokenizer::tokenize(input)?;
    let mut state = ParserState::new(tokens);
    let statement = state.parse_statement()?;
+   if trailing_semicolon {
        state.expect_eq(Token::SemiColon)?;
+   }
    Ok(statement)
}

+pub fn parse_create_statement(
+    input: &str,
+) -> anyhow::Result<CreateTableStatement> {
+    match parse_statement(input, false)? {
+        Statement::CreateTable(c) => Ok(c),
+        Statement::Select(_) => bail!("expected a create statement"),
+    }
+}
```

## Reading metadata

Now that we have the necessary building blocks to read table metadata,
we can extend our `Database` struct to store this information.
The `TableMetadata::from_cursor` method builds a `TableMetadata` struct
from a `Cursor` object, which represents a record in the schema table.
The create statement and first page are extracted from fields 4 and 3, respectively.

As records from the schema table contain informations about other kinds
of objects, such as triggers, we check the `type` field at index 0 to ensure
we're dealing with a table.

Finally, in `Db::collect_metadata`, we iterate over all the records in the schema table,
collecting table metadata for each table record we encounter.

```diff
// db.rs

+#[derive(Debug, Clone)]
+pub struct TableMetadata {
+    pub name: String,
+    pub columns: Vec<ast::ColumnDef>,
+    pub first_page: usize,
+}

+impl TableMetadata {
+   fn from_cursor(cursor: Cursor) -> anyhow::Result<Option<Self>> {
+       let type_value = cursor
+           .field(0)
+           .context("missing type field")
+           .context("invalid type field")?;

+       if type_value.as_str() != Some("table") {
+           return Ok(None);
+       }

+       let create_stmt = cursor
+           .field(4)
+           .context("missing create statement")
+           .context("invalid create statement")?
+           .as_str()
+           .context("table create statement should be a string")?
+           .to_owned();

+       let create = sql::parse_create_statement(&create_stmt)?;

+       let first_page = cursor
+           .field(3)
+           .context("missing table first page")?
+           .as_int()
+           .context("table first page should be an integer")? as usize;

+       Ok(Some(TableMetadata {
+           name: create.name,
+           columns: create.columns,
+           first_page,
+       }))
+    }
+}

pub struct Db {
    pub header: DbHeader,
+   pub tables_metadata: Vec<TableMetadata>,
    pager: Pager,
}

impl Db {
    pub fn from_file(filename: impl AsRef<Path>) -> anyhow::Result<Db> {
        let mut file = std::fs::File::open(filename.as_ref()).context("open db file")?;

        let mut header_buffer = [0; pager::HEADER_SIZE];
        file.read_exact(&mut header_buffer)
            .context("read db header")?;

        let header = pager::parse_header(&header_buffer).context("parse db header")?;

+       let tables_metadata = Self::collect_tables_metadata(&mut Pager::new(
+           file.try_clone()?,
+           header.page_size as usize,
+       ))?;

        let pager = Pager::new(file, header.page_size as usize);

        Ok(Db {
            header,
            pager,
+           tables_metadata,
        })
    }
    
+   fn collect_tables_metadata(pager: &mut Pager) -> anyhow::Result<Vec<TableMetadata>> {
+       let mut metadata = Vec::new();
+       let mut scanner = Scanner::new(pager, 1);

+       while let Some(record) = scanner.next_record()? {
+           if let Some(m) = TableMetadata::from_cursor(record)? {
+               metadata.push(m);
+           }
+       }

+       Ok(metadata)
+   }

    // [...]
}
```

Our initial implementation of the `.table` command can be updated to use the new metadata:

```diff
// main.rs

fn display_tables(db: &mut db::Db) -> anyhow::Result<()> {
-   let mut scanner = db.scanner(1);
-
-   while let Some(mut record) = scanner.next_record()? {
-       let type_value = record
-           .field(0)
-           .context("missing type field")
-           .context("invalid type field")?;

-       if type_value.as_str() == Some("table") {
-           let name_value = record
-               .field(1)
-               .context("missing name field")
-               .context("invalid name field")?;

-           print!("{} ", name_value.as_str().unwrap());
-       }
-   }
+   for table in &db.tables_metadata {
+       print!("{} ", &table.name)
+   }
    
    Ok(())
}
```

## Conclusion

We've extended our SQL parser to support `CREATE TABLE` statements and used it to
extract metadata from the schema table. By parsing the schema, we now have a
way to understand the structure of tables in our database.

In the next post, we'll leverage this metadata to build a query evaluator
that can execute simple `SELECT` queries against user-defined tables,
bringing us one step closer to a fully functional database engine.
