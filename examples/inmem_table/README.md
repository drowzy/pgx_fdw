# Inmem-table

```bash
cargo pgx run pg13
```

```sql
CREATE EXTENSION inmem_table;

INSERT INTO users (id, name, email) VALUES ('1', 'name', 'name@name.com');

SELECT * FROM users;
```
