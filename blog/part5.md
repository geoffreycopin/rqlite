### Build your own SQLite, Part 5: Evaluating queries

In the previous posts, we've explored the
[SQLite file format](/build-your-own-sqlite-part-1-listing-tables) and built a
simple [SQL parser](/build-your-own-sqlite-part-3-sql-parsing-101). It's time
to put these pieces together and implement a query evaluator!
In this post, we'll lay the groundwork for evaluating SQL queries and build a
query evaluator that can handle basic SELECT statements. While our initial implementation
won't support filtering, sorting, grouping, or joins yet, it will give us the
foundation to add these features in future posts.

As usual, the complete source code for this post is available
on [GitHub](https://github.com/geoffreycopin/rqlite/commit/c7dfeeea6956e209ccbd50a727c2b9352c246082).

## Setting up our test database

Before we can evaluate queries, we need a database to query. We'll start by
creating a simple database with a single table, `table1`, with two columns,
`id` and `value`:

```bash
sqlite3 queries_test.db
sqlite> create table table1(id integer, value text);
sqlite> insert into table1(id, value) values
    ...> (1, '11'),
    ...> (2, '12'),
    ...> (3, '13');
sqlite> .exit
```

⚠️ You might be tempted to use an existing SQLite database to test your queries,
but keep in mind that our implementation does not support overflow pages yet,
so it might not be able to read the data from your database file.

## Making the pager shareable

---
This section is specific to the Rust implementation. If you're following along
with another language, you can safely skip it!

---

Currently, our pager can only be used through an exclusive mutable reference.
This was fine for our initial use cases, but as we start building more complex
features, maintaining this restriction will constrain our design.
We'll make the pager shareable by wrapping its inner mutable fields in an
`Arc<Mutex<_>>` and `Arc<RwLock<_>>`. This will allow us to effectively clone the pager and
use it from multiple places without running into borrow checker issues.
At this stage of the project we could have chosen to use a simple `Rc<RefCell<_>>`,
but we'll eventually need to support concurrent access to the pager, so we'll
use thread-safe counterparts from the start.

```diff
// src/pager.rs

- #[derive(Debug, Clone)]
+ #[derive(Debug)]
pub struct Pager<I: Read + Seek = std::fs::File> {
-   input: I,
+   input: Arc<Mutex<I>>
    page_size: usize,
-   pages: HashMap<usize, page::Page>,
+   pages: Arc<RwLock<HashMap<usize, Arc<page::Page>>>>,
}
```

The `read_page` and `load_page` methods need to be updated accordingly:

```rust
impl<I: Read + Seek> Pager<I> {
    // [...] 
    pub fn read_page(&self, n: usize) -> anyhow::Result<Arc<page::Page>> {
        {
            let read_pages = self
                .pages
                .read()
                .map_err(|_| anyhow!("failed to acquire pager read lock"))?;

            if let Some(page) = read_pages.get(&n) {
                return Ok(page.clone());
            }
        }

        let mut write_pages = self
            .pages
            .write()
            .map_err(|_| anyhow!("failed to acquire pager write lock"))?;

        if let Some(page) = write_pages.get(&n) {
            return Ok(page.clone());
        }

        let page = self.load_page(n)?;
        write_pages.insert(n, page.clone());
        Ok(page)
    }

    fn load_page(&self, n: usize) -> anyhow::Result<Arc<page::Page>> {
        let offset = n.saturating_sub(1) * self.page_size;

        let mut input_guard = self
            .input
            .lock()
            .map_err(|_| anyhow!("failed to lock pager mutex"))?;

        input_guard
            .seek(SeekFrom::Start(offset as u64))
            .context("seek to page start")?;

        let mut buffer = vec![0; self.page_size];
        input_guard.read_exact(&mut buffer).context("read page")?;

        Ok(Arc::new(parse_page(&buffer, n)?))
    }
}
```

Two things to note regarding the `read_page` method:

- the initial attempt to read the page from the cache is nested in a block to
  limit the scope of the read lock, ensuring that it is released before we try
  to acquire the write lock
- after acquiring the write lock, we check again if the page is already in the
  cache, in case it was inserted in between the two lock acquisitions

Similarly, we'll define an owned version of our `Value` enum that we'll use
in the query evaluator:

```rust
// src/value.rs

// [...]

#[derive(Debug, Clone)]
pub enum OwnedValue {
    Null,
    String(Rc<String>),
    Blob(Rc<Vec<u8>>),
    Int(i64),
    Float(f64),
}

impl<'p> From<Value<'p>> for OwnedValue {
    fn from(value: Value<'p>) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Int(i) => Self::Int(i),
            Value::Float(f) => Self::Float(f),
            Value::Blob(b) => Self::Blob(Rc::new(b.into_owned())),
            Value::String(s) => Self::String(Rc::new(s.into_owned())),
        }
    }
}

impl std::fmt::Display for OwnedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OwnedValue::Null => write!(f, "null"),
            OwnedValue::String(s) => s.fmt(f),
            OwnedValue::Blob(items) => {
                write!(
                    f,
                    "{}",
                    items
                        .iter()
                        .filter_map(|&n| char::from_u32(n as u32).filter(char::is_ascii))
                        .collect::<String>()
                )
            }
            OwnedValue::Int(i) => i.fmt(f),
            OwnedValue::Float(x) => x.fmt(f),
        }
    }
}
```

Finally, we'll enrich our `Cursor` struct with a method that returns the value
of a field as an `OwnedValue`:

```rust
// src/cursor.rs

impl Cursor {
    // [...] 
    pub fn owned_field(&self, n: usize) -> Option<OwnedValue> {
        self.field(n).map(Into::into)
    }
    // [...]
}
```

## Evaluating `SELECT` statements

Our query engine will be composed of two main components:

- an iterator-like `Operator` enum that represents nestable operations on the
  database, such as scanning a table or filtering rows. Our initial implementation
  will only contain a `SeqScan` operator that yields all rows from a table.
- a `Planner` struct that takes a parsed SQL query and produces an `Operator` that
  can be evaluated to produce the query result.

Let's start by defining the `Operator` enum:

```rust
// src/engine/operator.rs
use anyhow::Context;

use crate::{cursor::Scanner, value::OwnedValue};

#[derive(Debug)]
pub enum Operator {
    SeqScan(SeqScan),
}

impl Operator {
    pub fn next_row(&mut self) -> anyhow::Result<Option<&[OwnedValue]>> {
        match self {
            Operator::SeqScan(s) => s.next_row(),
        }
    }
}
```

The result of evaluating a query will be obtained by repeatedly calling the
`next_row` method on the `Operator` until it returns `None`. Each value
in the returned slice corresponds to a column in the query result.

The `SeqScan` struct will be responsible for scanning a table and yielding
its rows:

```rust
// src/engine/operator.rs

// [...]

#[derive(Debug)]
pub struct SeqScan {
    fields: Vec<usize>,
    scanner: Scanner,
    row_buffer: Vec<OwnedValue>,
}

impl SeqScan {
    pub fn new(fields: Vec<usize>, scanner: Scanner) -> Self {
        let row_buffer = vec![OwnedValue::Null; fields.len()];

        Self {
            fields,
            scanner,
            row_buffer,
        }
    }

    fn next_row(&mut self) -> anyhow::Result<Option<&[OwnedValue]>> {
        let Some(record) = self.scanner.next_record()? else {
            return Ok(None);
        };

        for (i, &n) in self.fields.iter().enumerate() {
            self.row_buffer[i] = record.owned_field(n).context("missing record field")?;
        }

        Ok(Some(&self.row_buffer))
    }
}
```

The `SeqScan` struct is initialized with a list of field indices to read from
each record and a `Scanner` that will yield the records for every row in the
table to be scanned. As the number of fields to read is identical for every row,
we can preallocate a buffer to store the values of the selected fields.
The next_row method retrieves the next record from the scanner, extracts
the requested fields (specified by their indices), and stores them in our buffer.

Now that we have an `Operator` to evaluate `SELECT` statements, let's move on
to the `Planner` struct that will produce the `Operator` from a parsed SQL query:

```rust
// src/engine/plan.rs

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
}
```

The `Planner` struct is initialized with a reference to the database and
provides a `compile` method that takes a parsed SQL statement and returns
the corresponding `Operator`.
The `compile` method dispatches to a specific method for each type of SQL statement.

Let's see how to build an `Operator` for a `SELECT` statement:

```rust

// src/engine/plan.rs

impl<'d> Planner<'d> {
    // [...] 

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
```

First, we find a table metadata entry that matches the table name in the `SELECT`
statement. Then we iterate over the statement's result columns and build a list of
field indices to read from each record, either by expanding `*` to all columns or
by looking up the column name in the table metadata.

Finally, we create a `SeqScan` operator that will scan the entire tabl and yield
the selected fields for each row.

## Query evaluation in the REPL

It's time to put our query evaluator to the test!
We'll create a simple function that reads a raw SQL query and evaluates it:

```rust

// src/main.rs

// [...]

fn eval_query(db: &db::Db, query: &str) -> anyhow::Result<()> {
    let parsed_query = sql::parse_statement(query, false)?;
    let mut op = engine::plan::Planner::new(db).compile(&parsed_query)?;

    while let Some(values) = op.next_row()? {
        let formated = values
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("|");

        println!("{formated}");
    }

    Ok(())
}
```

This function creates a pipeline: it parses the SQL query, builds an
`Operator` with our Planner, and then repeatedly calls next_row() on the resulting operator
to retrieve and display each row of the result.

The final step is to use this function in the REPL loop:

```diff
// src/main.rs

// [...]

 fn cli(mut db: db::Db) -> anyhow::Result<()> {
     print_flushed("rqlite> ")?;
 
     let mut line_buffer = String::new();
 
     while stdin().lock().read_line(&mut line_buffer).is_ok() {
         match line_buffer.trim() {
             ".exit" => break,
             ".tables" => display_tables(&mut db)?,
+            stmt => eval_query(&db, stmt)?, 
-            stmt => match sql::parse_statement(stmt, true) {
-                Ok(stmt) => {
-                    println!("{:?}", stmt);
-                }
-                Err(e) => {
-                    println!("Error: {}", e);
-                }
-            },
         }
 
         print_flushed("\nrqlite> ")?;
 
         line_buffer.clear();
     }
 
     Ok(())
 }
```

Now we can run the REPL and evaluate some simple `SELECT` statements:

```bash
cargo run -- queries_test.db
rqlite> select * from table1;
```

If everything went well, you should see the following output:

```bash
1|11
2|12
3|13
```

## Conclusion

Our small database engine is starting to take shape! We can now parse and evaluate
simple `SELECT` queries. But there's still a lot to cover before we can call it
a fully functional database engine.
In the next posts, we'll discover how to filter rows, read indexes, and implement
sorting and grouping. 
