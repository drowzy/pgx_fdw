# pgx-fdw

Experimental [Foreign Data Wrapper](https://www.postgresql.org/docs/13/fdwhandler.html) support for [pgx](https://github.com/zombodb/pgx).

## Implementing a FDW 

1. Impl the trait `pgx_fdw::ForeignData`

```rust
struct MyFdw {}
impl pgx_fdw::ForeignData for MyFdw {
    ...
}
```
2. Create handler function

```rust
/// ```sql
/// CREATE FUNCTION my_handler() RETURNS fdw_handler LANGUAGE c AS 'MODULE_PATHNAME', 'my_handler_wrapper';
/// ```
#[pg_extern]
fn my_handler() -> pg_sys::Datum {
    pgx_fdw::FdwState::<MyFdw>::into_datum()
}
```

3. Create wrapper + server

```sql
CREATE FOREIGN DATA WRAPPER my_handler handler my_handler NO VALIDATOR;
CREATE SERVER my_fdw_srv FOREIGN DATA WRAPPER my_handler OPTIONS (server_option '1', server_option '2');
CREATE FOREIGN TABLE users (
    id text,
    name text,
    email text
) SERVER my_fdw_server OPTIONS (
    table_option '1',
    table_option2 '2'
);
```

## Examples
* `inmem_table` - Simple in-memory table fdw using `Vec`
