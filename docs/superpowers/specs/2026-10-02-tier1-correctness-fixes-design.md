# Tier 1 correctness fixes — design

Date: 2026-10-02
Branch: `fix/tier1-correctness`
Audit: <https://claude.ai/code/artifact/b1a73d73-6818-4b61-ab28-deedbe6a1aa0>
Baselines: Iceberg table spec, Apache Iceberg Java (`api/.../transforms`, `util/DateTimeUtil`), apache/iceberg-rust `ffa864b`.

## Goal

Make partition transforms, partitioned writes, partition pruning, equality-delete scans and
file bounds behave exactly as the Iceberg spec and the Java implementation do.

There is no production data, so no migration or repair tooling is in scope. Tier 2 and Tier 3
audit items are out of scope.

## Issues addressed

| # | Issue | Location |
|---|---|---|
| 1 | Partition filters rewritten as `part <op> transform(lit)` for every operator; bucket/truncate literals not transformed | `datafusion_iceberg/src/pruning_statistics.rs:329-393` |
| 2 | Query `LIMIT` pushed into the delete-file scan and pre-anti-join data scans | `datafusion_iceberg/src/table/mod.rs:1093,1106,1184` |
| 3 | Equality-delete anti-join uses `NullEqualsNothing` | `datafusion_iceberg/src/table/mod.rs:1147` |
| 4 | Lower/upper bounds for non-numeric types taken from the first row group only | `iceberg-rust/src/file_format/parquet.rs:250-330` |
| 5 | Bucket hash not spec-compliant; `Value` and Arrow paths disagree | `iceberg-rust-spec/src/spec/values.rs:382`, `iceberg-rust/src/arrow/transform.rs` |
| 6 | Month transform off by one | `values.rs:922-937`, `transform.rs:215` |
| 7 | Pre-1970 day/hour truncate toward zero; year/month panic pre-1970; string truncate panics on multi-byte UTF-8 | `values.rs:392,918`, `transform.rs:200-207` |
| 8 | Rows whose partition source column is NULL are silently dropped on write; partitioner supports only Int32/Int64/Utf8 partition values | `iceberg-rust/src/arrow/partition.rs` |

All eight were reproduced.

## Success criteria

- Every transform × type pair the spec allows matches Java, including the spec's Appendix B hash vectors.
- `Value::transform` and `transform_arrow` agree on every supported pair.
- The audit reproductions are permanent regression tests and pass.
- For every transform × comparison operator, a pruned scan returns exactly the rows of an unpruned scan.
- `make test` and `cargo clippy --all-targets --all-features -- -D warnings` are green after each PR.

## Part 1 — single transform implementation (issues 5, 6, 7)

New module `iceberg-rust-spec/src/spec/transform.rs` (re-exported from `spec`), containing pure
scalar functions. Both `Value::transform` and `iceberg-rust/src/arrow/transform.rs` call it; neither
keeps its own arithmetic.

### Bucket

`hash_*` functions return the murmur3 x86 32-bit hash (seed 0) as `i32`:

| Iceberg type | Hashed bytes |
|---|---|
| int, long, date, time, timestamp, timestamptz | value as `i64`, 8 bytes little-endian |
| timestamp_ns, timestamptz_ns | `nanos.div_euclid(1000)` as `i64`, 8 bytes little-endian |
| decimal | unscaled `i128`, minimal big-endian two's-complement bytes |
| string | UTF-8 bytes |
| uuid | 16 bytes big-endian |
| fixed, binary | raw bytes |

`bucket(hash, n) = (hash & i32::MAX) % n`.

### Temporal (floor semantics, matching Java `DateTimeUtil.convertDays/convertMicros/convertNanos`)

| Transform | Input | Result |
|---|---|---|
| year | date (days) / timestamp (µs, ns) | calendar year − 1970 |
| month | same | (year − 1970) × 12 + (month − 1) |
| day | date | the date itself |
| day | timestamp µs / ns | `div_euclid(MICROS_PER_DAY / NANOS_PER_DAY)` |
| hour | timestamp µs / ns | `div_euclid(MICROS_PER_HOUR / NANOS_PER_HOUR)` |

Calendar fields come from chrono on the floored date, so no `years_since` (which returns `None`
before 1970).

### Truncate

| Type | Result |
|---|---|
| int, long | `v - v.rem_euclid(w)` |
| decimal | same on the unscaled value, scale unchanged |
| string | first `w` Unicode code points |
| binary | first `w` bytes |

### Void

Always null.

### `Value::transform`

Rewritten on top of the module. Supported pairs return a `Value` of the spec result type
(`Int` for bucket and temporal transforms; source type for identity and truncate). Unsupported
pairs return `Error::NotSupported` naming the transform and type. `Transform::Void` is not
representable as a `Value` and returns `Error::NotSupported`; callers that handle void use `None`.

### `transform_arrow`

Supported Arrow inputs (every type this crate maps to or from Iceberg):

| Arrow type | Iceberg meaning | Transforms |
|---|---|---|
| Int8, Int16, Int32 | int | identity, bucket, truncate |
| Int64 | long | identity, bucket, truncate |
| Date32 | date | identity, bucket, year, month, day |
| Time64(µs) | time | identity, bucket |
| Timestamp(µs, tz?) | timestamp(tz) | identity, bucket, year, month, day, hour |
| Timestamp(ns, tz?) | timestamp_ns(tz) | identity, bucket, year, month, day, hour |
| Utf8, LargeUtf8, Utf8View | string | identity, bucket, truncate |
| Utf8 holding a uuid | uuid | identity, bucket (parsed to `Uuid` before hashing) |
| Binary, LargeBinary, BinaryView | binary | identity, bucket, truncate |
| FixedSizeBinary | fixed | identity, bucket |
| Decimal128 | decimal | identity, bucket, truncate |
| any | — | void → null array of the source type |

uuid columns are Arrow `Utf8` (see `iceberg-rust-spec/src/arrow/schema.rs`), so the string and uuid
bucket hashes differ for the same Arrow type. `transform_arrow` therefore takes the source Iceberg
type as well: `transform_arrow(array, transform, source_type: &Type)`. Both call sites
(`arrow/partition.rs`, `datafusion_iceberg/src/pruning_statistics.rs`) have the partition field's
source type available.

Result types: Int32 for bucket and temporal transforms; source type for identity and truncate.
Nulls stay null (today the string bucket maps NULL to 0). Unsupported pairs return
`ArrowError::ComputeError` naming the transform and type; no panics.

## Part 2 — partitioned writes (issue 8)

`partition_record_batch` (`iceberg-rust/src/arrow/partition.rs`) is rewritten:

1. Transform each partition source column with `transform_arrow`.
2. Encode the transformed columns with `arrow::row::RowConverter`. Rows with equal encoded keys are
   one partition; NULL is an ordinary key component.
3. Group row indices by key in one pass (`HashMap<OwnedRow, Vec<u32>>` keyed on the row bytes),
   then `take` each group's rows from the batch.
4. Build the partition tuple from the group's first row with a new helper
   `arrow_scalar_to_value(array: &dyn Array, row: usize, ty: &Type) -> Result<Option<Value>, Error>`
   in `iceberg-rust-spec/src/arrow/`. The type is the partition field's result type
   (`field_type.tranform(transform)`), so an identity partition on a date yields `Value::Date`,
   and a NULL yields `None`.

The public return type changes from `Vec<Value>` to `Vec<Option<Value>>` per partition, so null
partition values reach the `Struct` as `None`. Callers in `arrow/write.rs` are updated; partition
path generation renders `None` as `null`, matching Java (`field=null`).

The data file's partition tuple comes from this grouping, not from Parquet statistics.
`parquet_to_datafile` gains a `partition_values: Option<&[Option<Value>]>` argument; the writer
(`arrow/write.rs:564`) passes the group's tuple. Today the tuple is re-derived from each column's
min/max, which fails for long strings because Parquet truncates statistics to 64 bytes, and
depends on `Value::transform` agreeing with `transform_arrow`. Callers that register existing
files (`datafusion_iceberg/src/table/mod.rs:1763`) pass `None` and keep the statistics-based
derivation (see Part 5).

The `DistinctValues` enum and its helpers are removed.

## Part 3 — equality-delete scan (issues 2, 3)

In `datafusion_iceberg/src/table/mod.rs`, equality-delete plan:

- The delete-file `FileScanConfig` and the data-file `FileScanConfig`s inside partitions that have
  equality deletes get `.with_limit(None)`. Partitions without equality deletes keep the limit. The
  outer `LIMIT` operator still bounds the result.
- `HashJoinExec` uses `NullEquality::NullEqualsNull`. This is per-key-column, so a delete row
  `(a = 1, b = NULL)` deletes data rows with `a = 1 AND b IS NULL`, as the spec requires.

## Part 4 — partition filter projection (issue 1)

`transform_predicate`, `transform_literal` and the `DateTransform` UDF are replaced by a new module
`datafusion_iceberg/src/partition_projection.rs`:

```rust
pub(crate) fn project(expr: &Expr, partition_fields: &[BoundPartitionField<'_>]) -> Option<Expr>
```

`None` means "this expression cannot prune". The result is an *inclusive* projection: it may keep
partitions without matching rows, never drop partitions with matching rows.

### Leaf predicates

A leaf is `column <op> literal`, `literal <op> column` (operator flipped first), `column IN (lits)`,
`column IS NULL` or `column IS NOT NULL`. For each partition field whose source column is `column`
(a column may feed several partition fields), project:

| Transform | `=` | `<`, `<=` | `>`, `>=` | `!=` | `IN` | `IS [NOT] NULL` |
|---|---|---|---|---|---|---|
| identity | `p = v` | same op | same op | `p != v` | `p IN (vs)` | same |
| year, month, day, hour, truncate(int/long/decimal) | `p = t(v)` | `p <= t(v)` | `p >= t(v)` | — | `p IN (t(vs))` | same |
| truncate(string/binary) | `p = t(v)` | — | — | — | `p IN (t(vs))` | same |
| bucket | `p = t(v)` | — | — | — | `p IN (t(vs))` | same |
| void | — | — | — | — | — | — |

`—` means the leaf projects to nothing for that field. `t(v)` is `Value::transform` evaluated at
planning time; the literal is first cast to the source column's Iceberg type. If any step fails, the
leaf projects to nothing. Projections from several fields on the same column are ANDed.

### Combining

- `AND`: AND of the parts that project; `None` if none do.
- `OR`: `None` if any branch is `None`, else OR of the branches.
- `NOT`, casts, arithmetic, functions, or anything else: `None`.

### Integration

In `datafusion_iceberg/src/table/mod.rs` the per-filter `transform_predicate(...).unwrap()` is
replaced by `project`, applied to each pushed filter; the projected filters are ANDed and used only
when at least one projects. The filter pre-selection by `column_refs ⊆ partition source columns`
is removed (projection already handles mixed expressions). No `.unwrap()` remains on this path.

## Part 5 — bounds merge (issue 4)

In `iceberg-rust/src/file_format/parquet.rs`:

- Lower bound: keep the smaller of the current and new value when both are the same `Value`
  variant (using `Value: Ord`); upper bound: the larger. Different variants: drop the bound for
  that column.
- Parquet truncates statistics to 64 bytes. A min that is not exact (`min_is_exact() == false`)
  is still a valid lower bound (it is a prefix); a max that is not exact is still a valid upper
  bound (parquet rounds it up). Both are used.
- When no partition tuple is passed in (registering existing files), partition values are still
  derived from statistics, but only from exact min and max. If a partitioned column has inexact
  statistics, or min and max transform to different values, return an error naming the file and
  column. Never leave the value unset: a missing value would read as NULL and be pruned wrongly.
  A column whose statistics are all-null (no min/max, null count = row count) yields `None`.

## Testing

- `iceberg-rust-spec`: unit tests for every scalar function — Appendix B hash vectors (int, long,
  decimal, date, time, timestamp, timestamptz, timestamp_ns, string, uuid, fixed, binary), Java
  pre-1970 results (year, month, day, hour), Unicode and byte truncate, decimal truncate. The
  existing month tests asserting 641 are corrected to 640.
- `iceberg-rust`: parity test — for each supported Arrow type × transform, generated inputs
  (including nulls and pre-1970 values) give the same result through `transform_arrow` and
  `Value::transform`. `partition_record_batch` tests for NULL partition values and each
  partition result type. Multi-row-group bounds test for string, decimal, binary, uuid, boolean.
- `datafusion_iceberg`: regression tests from the audit (LIMIT with equality deletes, NULL
  equality-delete keys, day-partition range filters, bucket equality filter, NULL partition
  source on INSERT). Differential pruning test: for every transform × operator, a row on a
  partition boundary; assert the filtered scan equals the result of the same filter over an
  unpartitioned copy of the data.
- Cross-engine: extend `integration_spark.rs` / `integration_trino.rs` with month- and
  bucket-partitioned tables read in both directions (requires Docker; run before merging PR 1
  and PR 3).

## Delivery

Four PRs from `fix/tier1-correctness`-based branches, in order, each with `make test` and clippy
green:

1. Transforms and partitioned writes (Parts 1, 2)
2. Equality-delete scan (Part 3)
3. Partition filter projection (Part 4; depends on PR 1)
4. Bounds merge (Part 5)
