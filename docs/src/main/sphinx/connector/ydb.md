# YDB connector

The YDB connector queries row-oriented tables in a
[YDB](https://ydb.tech/) database using the YDB JDBC driver and YQL.
The coordinator and workers must be able to reach the database's gRPC
endpoint and the endpoints returned by YDB discovery.

## Configuration

Create `etc/catalog/example.properties` with the following configuration:

```properties
connector.name=ydb
connection-url=jdbc:ydb:grpcs://ydb.example.net:2135/database/path?useQueryService=true
```

Use `grpc` only for an endpoint that does not require TLS. Configure
authentication using the
[YDB JDBC connection options](https://github.com/ydb-platform/ydb-jdbc-driver).
Use {doc}`secrets </security/secrets>` for credentials.

Each catalog connects to one YDB database. The connector exposes one virtual
schema named `default`; it does not map Trino schemas to YDB directories:

```sql
SHOW TABLES FROM example.default;
SELECT * FROM example.default.events;
```

YDB table names are database-relative paths. Quote a path containing `/` as
one Trino identifier, for example `example.default."analytics/events"`.
Schema creation, deletion, and renaming are not supported.

## Type mapping

| YDB type | Trino type |
| --- | --- |
| `Bool` | `boolean` |
| `Int8`, `Int16`, `Int32`, `Int64` | `tinyint`, `smallint`, `integer`, `bigint` |
| `Uint8`, `Uint16`, `Uint32` | `smallint`, `integer`, `bigint` |
| `Uint64` | `decimal(20,0)` |
| `Float`, `Double` | `real`, `double` |
| `Utf8`, `Text` | Unbounded `varchar` |
| `String`, `Bytes` | `varbinary` |
| `Decimal(p,s)` | `decimal(p,s)` |
| `Date`, `Date32` | `date` |
| `Datetime`, `Datetime64`, `Timestamp`, `Timestamp64` | `timestamp(6)` |

Unsigned integers retain their numeric value, including the full `Uint64`
range from `0` to `18446744073709551615`. Binary values are not decoded as
UTF-8. Timestamp values use UTC rather than the coordinator's or worker's
default time zone.

YDB decimal NaN and infinity values cannot be represented by Trino decimals
and fail when read. YDB decimal precision is limited to 35.

Unsupported source types are omitted by default. The JDBC
`unsupported-type-handling=CONVERT_TO_VARCHAR` option exposes them as
read-only strings without predicate pushdown.

## Creating and writing tables

YDB requires an explicit primary key. Supply its columns in order using the
table property `primary_key`. The key may be composite, and key columns do not
have to be the first columns in the table:

```sql
CREATE TABLE example.default.events (
    payload varchar,
    tenant bigint,
    event_id bigint
)
WITH (primary_key = ARRAY['event_id', 'tenant']);

INSERT INTO example.default.events VALUES ('created', 1, 10);
SELECT * FROM example.default.events;
DROP TABLE example.default.events;
```

`CREATE TABLE AS SELECT` also requires the primary-key property and can name
its output columns explicitly:

```sql
CREATE TABLE example.default.event_ids (event_id)
WITH (primary_key = ARRAY['event_id'])
AS SELECT BIGINT '10';
```

Writes use signed integer, boolean, floating-point, varchar, varbinary, decimal,
`date`, and `timestamp(3)` / `timestamp(6)` mappings. New dates and timestamps
use YDB `Date32` and `Timestamp64`. Decimal precision must not exceed 35.

By default, INSERT writes batches to a temporary table and publishes them with
one native INSERT SELECT statement. A duplicate key makes that statement fail
without publishing the staged rows. CTAS publishes its temporary table only
after the write succeeds. Query and task retries are not supported.

The JDBC `non_transactional_insert` session property bypasses staging when
explicitly enabled. In that mode, completed batches can remain after a failure.
Every INSERT in that mode must explicitly supply all primary-key columns,
including nullable key columns. Default staged INSERT supplies NULL for an
omitted nullable primary-key column.

### Table and column changes

The connector supports renaming a table within `default`, adding nullable
columns at the end of a table, dropping non-key columns, and dropping `NOT NULL`
from non-key columns. Primary-key columns cannot be dropped or made nullable.
Adding a column with `NOT NULL`, a default, a comment, or an explicit position
is not supported.

## Limitations

Schema changes, renaming columns, changing column types, `TRUNCATE`, `UPDATE`,
`DELETE`, and `MERGE` are not supported.

## Pushdown

The connector can push down supported comparisons, `IN`, null checks, and
`NULLIF`. Supported projections include `lower`, `upper`, and `strpos`. Case
conversion and substring positions use Unicode operations. Integral division
and modulus can be pushed down for a nonzero constant divisor other than `-1`.

Arithmetic that can overflow or divide by zero, general arithmetic expressions,
varchar concatenation, `LIKE`, and unsupported functions remain in Trino.
Concatenation retains Trino's result-length limit and error behavior.
Bounded `varchar` projections also remain in Trino because YDB text result
columns map to unbounded `varchar`.
Expressions over unsigned,
floating-point, native decimal, date, and legacy temporal columns also remain
in Trino where native value or comparison semantics differ. Disabling the JDBC
`complex_expression_pushdown` session property retains expression evaluation in
Trino.

Joins, aggregations, sorting, and limits are evaluated by Trino. Domain
predicates continue to use the JDBC connector's type-specific pushdown
controllers.

The connector defaults the JDBC driver option `cacheConnectionsInDriver` to
`false` because JDBC 2.4.1 can race cached context lookup against last-close
shutdown. Do not override this option to `true` on this driver version.
It also defaults `replaceJdbcInByYqlList` to `false` because JDBC 2.4.1's
optimized list parameter cannot bind native Timestamp64 SDK values.
