use lazy_static::lazy_static;
use pgx::*;
use std::sync::RwLock;

pg_module_magic!();

lazy_static! {
    static ref TABLE: RwLock<Vec<User>> = RwLock::new(vec![]);
}

#[derive(Debug, Default, Clone)]
struct User {
    id: String,
    name: String,
    email: String,
}

fn into_value<T: FromDatum>(datum: Option<pg_sys::Datum>, typoid: pgx::PgOid) -> Option<T> {
    match datum {
        Some(d) => unsafe { T::from_datum(d, false, typoid.value()) },
        None => None,
    }
}
struct InMemTable {}

impl pgx_fdw::ForeignData for InMemTable {
    type Item = String;
    type RowIterator = std::vec::IntoIter<Vec<Self::Item>>;

    fn begin(_opts: &pgx_fdw::fdw_options::Options) -> Self {
        InMemTable {}
    }

    fn indices(_opts: &pgx_fdw::fdw_options::Options) -> Option<Vec<String>> {
        Some(vec![String::from("id")])
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
            .try_fold(User::default(), |mut t, (name, datum, typoid)| {
                match (name.to_string().as_str()) {
                    "id" => t.id = into_value::<String>(*datum, *typoid).unwrap(),
                    "name" => t.name = into_value::<String>(*datum, *typoid).unwrap(),
                    "email" => t.email = into_value::<String>(*datum, *typoid).unwrap(),
                    _ => error!(""),
                }

                Some(t)
            });

        let mut rows = TABLE.write().unwrap();
        rows.push(row.unwrap().clone());

        None
    }

    fn delete(
        &self,
        desc: &PgTupleDesc,
        tuples: Vec<pgx_fdw::Tuple>,
    ) -> Option<Vec<pgx_fdw::Tuple>> {
        if let Some((name, datum, oid)) = tuples.first() {
            match name.to_string().as_str() {
                "id" => {
                    let predicate = |u: &User| u.id == into_value::<String>(*datum, *oid).unwrap();
                    let mut rows = TABLE.write().unwrap();
                    let vec = std::mem::replace(&mut *rows, vec![]);

                    *rows = vec.into_iter().filter(|r| !predicate(r)).collect();
                }
                _ => error!(""),
            }
        }

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
