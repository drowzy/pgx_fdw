use lazy_static::lazy_static;
use pgx::*;
use std::sync::RwLock;

pg_module_magic!();

lazy_static! {
    static ref TABLE: RwLock<Vec<Row>> = RwLock::new(vec![]);
}

#[derive(Debug, Default, Clone)]
struct Row {
    id: String,
    name: String,
    email: String,
}

struct InMemTable {}

impl pgx_fdw::ForeignData for InMemTable {
    type Item = String;
    type RowIterator = std::vec::IntoIter<Vec<Self::Item>>;

    fn begin(_opts: &pgx_fdw::fdw_options::Options) -> Self {
        InMemTable {}
    }

    fn execute(&mut self, _desc: &PgTupleDesc) -> Self::RowIterator {
        let rows: Vec<Vec<String>> = TABLE
            .read()
            .unwrap()
            .iter()
            .map(|r| vec![r.id.clone(), r.name.clone(), r.email.clone()])
            .collect();

        rows.into_iter()
    }

    fn insert(
        &mut self,
        _desc: &PgTupleDesc,
        tuple: Vec<pgx_fdw::Tuple>,
    ) -> Option<Vec<pgx_fdw::Tuple>> {
        let row = tuple
            .iter()
            .try_fold(Row::default(), |mut t, (name, datum, oid)| {
                match (name.to_string().as_str(), datum) {
                    ("id", Some(i)) => {
                        let i = unsafe {
                            String::from_datum(i.to_owned(), false, oid.value()).unwrap()
                        };
                        t.id = i;
                        Some(t)
                    }
                    ("name", Some(d)) => {
                        let s = unsafe {
                            String::from_datum(d.to_owned(), false, oid.value()).unwrap()
                        };
                        log!("GOT {:?}", s);
                        t.name = s;
                        Some(t)
                    }
                    ("email", Some(d)) => {
                        let s = unsafe {
                            String::from_datum(d.to_owned(), false, oid.value()).unwrap()
                        };
                        log!("GOT {:?}", s);
                        t.email = s;
                        Some(t)
                    }
                    _ => {
                        error!("no match");
                    }
                }
            });

        let mut rows = TABLE.write().unwrap();
        rows.push(row.unwrap().clone());

        None
    }
}

/// ```sql
/// CREATE FUNCTION in_mem_table_handler() RETURNS fdw_handler LANGUAGE c AS 'MODULE_PATHNAME', 'in_mem_table_handler_wrapper';
/// ```
#[pg_extern]
fn in_mem_table_handler() -> pg_sys::Datum {
    pgx_fdw::FdwState::<InMemTable>::into_datum()
}

extension_sql!(
    r#"
    CREATE FOREIGN DATA WRAPPER in_mem_table_handler handler in_mem_table_handler NO VALIDATOR;
    CREATE SERVER in_mem_table_srv foreign data wrapper in_mem_table_handler;
    create foreign table users (
        id text,
        name text,
        email text
    ) server in_mem_table_srv options (
        table_option '1',
        table_option2 '2'
    );
"#
);
