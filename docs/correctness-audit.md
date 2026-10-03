# iceberg-rust-jk: Correctness Audit & Fix Progress

Last updated: 2026-10-03

## Summary

All Tier 1 bugs are fixed in [PR #4](https://github.com/andts/iceberg-rust-jk/pull/4) (14 commits on `fix/tier1-correctness`), together with an eighth Tier 1 bug and five related bugs found during the work. Tier 2 and Tier 3 remain mostly open; the Fix progress table below tracks every item.

- **Scope:** `iceberg-rust-spec`, `iceberg-rust`, `datafusion_iceberg` and the catalogs, at `origin/main` `42fce57`.
- **Baseline:** [apache/iceberg-rust](https://github.com/apache/iceberg-rust) `main` at `ffa864b`, plus the Iceberg table spec.
- **Tiers:** Tier 1 = silent wrong results or data loss. Tier 2 = commit and metadata integrity, from code reading. Tier 3 = loud failures and robustness gaps.
- **Where the bugs are:** mostly in features this fork adds on top of Apache's scope (merge-append, partition pruning, equality-delete scans), and in the partition transforms.

## Fix progress

Tier 1 is fixed in full; the rest is open unless marked. Commits are on `fix/tier1-correctness` ([PR #4](https://github.com/andts/iceberg-rust-jk/pull/4)). Update this table as items are fixed.

| Item | Issue | Status | Where |
| --- | --- | --- | --- |
| T1-1 | Partition filters rewritten wrongly, matching files skipped | Fixed | `1571c99` inclusive projection |
| T1-2 | `LIMIT` brought back equality-deleted rows | Fixed | `8dcbd8a` |
| T1-3 | Equality deletes ignored NULL keys | Fixed | `8dcbd8a` |
| T1-4 | Non-numeric bounds from the first row group only | Fixed | `18faba1` |
| T1-5 | Bucket hash not spec-compliant | Fixed | `9cc3a4b`, `7a6608a` |
| T1-6 | Month transform off by one | Fixed | `9cc3a4b`, `7a6608a` |
| T1-7 | Pre-1970 rounding, year/month and truncate panics | Fixed | `9cc3a4b`, `7a6608a` |
| T1-8 | INSERT dropped rows with a NULL partition column (found during the fix) | Fixed | `9e4b2d2` |
| New | Decimal, fixed and binary partitions could not be written to manifests | Fixed | `e994575` |
| New | Partition values re-derived from truncated Parquet statistics | Fixed | `cc532c4`, `6f7ad08` |
| New | Partition values untyped on read (decimal, uuid, Utf8View) | Fixed | `ba5f39e` |
| New | Manifest summaries never set `contains_null`, so `IS NULL` pruned files | Fixed | `9e4b2d2` |
| New | `clippy -D warnings` red on `main` | Fixed | `72fb2cc` |
| T2-8 | Manifest rewrite on append: resurrects DELETED entries, drops entries, ignores spec id | Partly fixed | Dropped entries fixed in `e994575`; resurrection and spec id open |
| T2-9 | Operations in one transaction share one base | Open | |
| T2-10 | First commit and AddSchema unguarded | Open | |
| T2-11 | Global equality deletes ignored; manifest row counts wrong | Partly fixed | Row counts fixed in `1571c99`; global deletes open |
| T2-12 | Overwrite writes no DELETED entries | Open | |
| T3 | No commit retry | Open | |
| T3 | Time travel to an unknown snapshot reads current | Open | |
| T3 | Tables without a `main` ref can't be opened | Open | |
| T3 | `add_schema` never makes the schema current | Open | |
| T3 | SQL catalog builds queries with `format!` | Open | |
| T3 | Panics in library code | Partly fixed | Pruning and derivation unwraps removed; `UpgradeFormatVersion` still panics |
| T3 | Some transforms could not be written | Fixed | `7a6608a` |
| T3 | Minor metadata issues (file catalog `metadata-log`, v1 `last_partition_id`) | Open | |
| Follow-up | Tables written before PR #4 with month, bucket, string-truncate or pre-1970 partitions mis-prune | Known limitation | Rewrite such tables; noted in PR #4 |
| Follow-up | Java manifests with uuid partitions are rejected (apache-avro 0.21) | Known limitation | Clear error instead of a misread |
| Follow-up | uuid and float partitions are never pruned | Open | |
| Follow-up | NULL-partition manifest fallback ignores partition-spec history | Open | |
| Follow-up | No v1/v3 round-trip tests for the new manifest encodings | Open | |
| Follow-up | Identity partition on `timestamp_ns` unsupported | Known limitation | Errors on write |

## Tier 1: silent wrong results or data loss

All seven were reproduced before the fix. The first three are scan-side and affect only this engine. Items 4–7 change what gets written, so they also mislead Spark and Trino. Line numbers refer to `origin/main` `42fce57`.

| # | Bug | Location | Evidence | Apache iceberg-rust |
| --- | --- | --- | --- | --- |
| 1 | Partition filters are rewritten as `partition_column <op> transform(literal)` for any operator. Wrong for `<`, `>`, `!=` on day, month, year and hour. The bucket and truncate literals aren't transformed at all. | `datafusion_iceberg/src/pruning_statistics.rs:329-393` | Day-partitioned row at 2023-05-15 12:00: `ts > '…10:00'` and `ts < '…14:00'` both returned 0 rows. Bucket `id = 5` failed with `Int32 <= Int64`. | Spec inclusive/strict projection per transform (`expr/visitors/inclusive_projection.rs`) |
| 2 | The query's `LIMIT` is pushed into the delete-file scan and the data scans before the equality-delete anti-join | `datafusion_iceberg/src/table/mod.rs:1093,1106,1184` | Rows 1–10, deleted {1, 9, 10}: `LIMIT 2` returned deleted id 1. `LIMIT 6` returned 5 rows. | Deletes applied per row in the reader; a limit can't skip them |
| 3 | Equality-delete anti-join uses `NullEqualsNothing`, so a NULL delete key never matches | `datafusion_iceberg/src/table/mod.rs:1147` | Row with `k = NULL` survived an equality delete on `k = NULL` | Null-aware matching (`arrow/caching_delete_file_loader.rs:627`) |
| 4 | Bounds merge across row groups handles only int, long, float, double, date, time and timestamp. Strings, decimals, binary, uuid, fixed and boolean keep the first row group's value. | `iceberg-rust/src/file_format/parquet.rs:250-330` | Row groups `[m..z]` + `[a..b]` recorded lower bound `"m"`. Any file over 1M rows has more than one row group. | `MinMaxColAggregator` merges every type |
| 5 | Bucket hashing is not spec-compliant, and the two implementations disagree. `Value::transform` hashes int/date as 4 bytes and takes the modulo unsigned. `transform_arrow` uses `rem_euclid` instead of `hash & i32::MAX`. | `iceberg-rust-spec/src/spec/values.rs:382`, `iceberg-rust/src/arrow/transform.rs:123-190` | N=10, int 34: spec 9, Value 3, arrow 9. Date 2017-11-16: spec 6, Value 5, arrow 8. | `(hash & i32::MAX) % n`, ints hashed as longs |
| 6 | Month transform is off by one: it adds the 1-based month | `values.rs:922-937`, `transform.rs:215` | 2023-05 gives 641; the spec says 640. The unit tests asserted 641. | Correct; tested against Java values |
| 7 | Day and hour round toward zero before 1970. Year and month panic on any pre-1970 date. Truncate on a string panics on multi-byte UTF-8 and counts bytes, not characters. | `values.rs:392, 918`, `transform.rs:200-207` | −1µs gives day 0 (spec −1). `year(1969-12-31)` panics. `truncate[1]("ééé")` panics. | Floor division; truncation by code point |

## Tier 2: commit and metadata integrity

These come from code reading and weren't reproduced. Items 8–10 can corrupt table state; 11–12 drop deletes or lose history.

| # | Bug | Location | Apache iceberg-rust |
| --- | --- | --- | --- |
| 8 | When an append adds files to an existing manifest, that manifest is rewritten unsafely. Every entry is set to `Existing`, including `DELETED` entries from Spark/Java manifests, so deleted files come back. `.filter_map(Result::ok)` silently drops entries that fail to serialize. The manifest is chosen without checking its partition spec id and rewritten with the default spec's header. | `iceberg-rust/src/table/manifest.rs:371-385, 512-537`, `transaction/append.rs:120` | Fast append never rewrites existing manifests |
| 9 | Every operation in one transaction is computed from the same starting metadata. Append plus overwrite yields two snapshots with the same parent and sequence number, and one is lost from `main`. `apply_table_updates` doesn't check sequence numbers or parents. | `iceberg-rust/src/table/transaction/mod.rs:502`, `catalog/commit.rs` | `TableMetadataBuilder` checks both |
| 10 | A table's first snapshot is committed with no requirement: `AssertRefSnapshotId.snapshot_id` is `i64` and can't say "ref must not exist". `AddSchema` sends no requirement, and `schemas.insert` silently replaces a schema with the same id. | `catalog/commit.rs`, `transaction/operation.rs` | `RefSnapshotIdMatch { snapshot_id: Option<i64> }` |
| 11 | Equality deletes are grouped by exact partition value, so global deletes in a partitioned table are never applied and spec ids are ignored. Manifest `row_counts` uses `added + existing − deleted`; at 0, DataFusion may treat the manifest as all-null and prune it. | `datafusion_iceberg/src/table/mod.rs:772-840`, `pruning_statistics.rs:149` | Delete-file index handles global deletes and spec ids |
| 12 | Overwrite drops removed files instead of writing `DELETED` entries. Incremental reads miss removals and Java snapshot expiry leaks the files. | `manifest.rs:512-537` | Writes `DELETED` entries |

## Tier 3: loud failures and robustness gaps

These fail visibly or need unusual inputs, so they rank below Tiers 1 and 2.

- **No commit retry.** After a conflict, the DataFusion table keeps stale metadata, so every later INSERT through it also fails. Apache retries with exponential backoff.
- **Time travel to an unknown snapshot silently reads the current snapshot** (`iceberg-rust/src/table/mod.rs:213`).
- **Some tables from other engines can't be opened.** `current_snapshot` errors when snapshots exist but there's no `main` ref, the state Java's `REPLACE TABLE` and WAP staging leave (`iceberg-rust-spec/src/spec/table_metadata.rs:322`).
- **`Transaction::add_schema` never makes the new schema current,** and the transaction can't set it.
- **The SQL catalog builds 21 queries with `format!`.** Names containing quotes break them and allow SQL injection.
- **Panics in library code:** `UpgradeFormatVersion` calls `unimplemented!()`, and `transform_predicate` and equality-delete planning called `.unwrap()`.
- **Some transforms couldn't be written:** truncate on strings, and bucket on timestamp, decimal or uuid, returned an error.
- **Minor metadata issues:** the file catalog doesn't maintain `metadata-log`, and v1 metadata derives `last_partition_id` from spec ids.

## Comparison with apache/iceberg-rust

Apache does less but does it correctly; this fork does more, and most defects sat in what it adds. The table describes the fork before PR #4.

| Area | This fork | Apache iceberg-rust |
| --- | --- | --- |
| Partition transforms | Bucket, month, pre-1970 and truncate bugs; two implementations that disagree | One implementation, tested against Java reference values |
| Filters to partition filters | Same operator applied to the transformed literal | Inclusive and strict projection per transform |
| Append | Merge-append that rewrites an existing manifest | Fast append; existing manifests untouched |
| Overwrite / replace / DML | Supported, with the integrity gaps above | More limited |
| Metadata updates | Applied without checks | `TableMetadataBuilder` validates every update |
| Commit conflicts | No retry; first commit unguarded | Retry with backoff; `Option` ref assertion |
| File statistics | Bounds merged only for numeric and temporal types | Merged for every type |
| Equality deletes | Hash anti-join; misses NULL keys and global deletes; breaks under LIMIT | Applied per row in the reader, null-aware |
| Extras | Deletion vectors, materialized views, sort-order scans | Mostly absent |

## Tier 1 fixes: what shipped

[PR #4](https://github.com/andts/iceberg-rust-jk/pull/4) carries the fix as 14 commits, one per fix, so each rebases onto upstream on its own. The first (formatting and lint) and last (design docs) can be dropped if upstream does not want them.

1. `72fb2cc` style: cargo fmt and clippy fixes to pre-existing code
2. `9cc3a4b` fix(spec): spec-compliant partition transforms
3. `7a6608a` fix(arrow): spec-compliant Arrow transform kernels for every type
4. `9e4b2d2` fix(write): keep rows with NULL partition values
5. `cc532c4` fix(write): record the computed partition tuple in each data file
6. `ba5f39e` fix(scan): type partition values by their column
7. `e994575` fix(manifest): valid Avro schema and encodings for partition values
8. `96824e7` test: Spark and DataFusion agree on partition transforms
9. `8dcbd8a` fix(scan): apply equality deletes before LIMIT and match NULL keys
10. `1571c99` fix(scan): prune partitions with an inclusive projection
11. `18faba1` fix(stats): merge lower/upper bounds of every type across row groups
12. `6f7ad08` fix(parquet): derive every partition field from complete statistics
13. `307e5f0` docs: state the contracts of the partition and manifest APIs
14. `7ace75d` docs: design and plan for Tier 1 correctness fixes

`make test` passes, including the Spark, Trino and pyiceberg Docker suites; `cargo fmt --check` and `clippy -D warnings` are clean. A differential test compares pruned and unpruned scans for 16 transforms × 46 filters × 3 rows.

Public API changes: `transform_arrow` takes the source type; `parquet_to_datafile` takes the partition tuple; `partition_record_batch` and `generate_partition_path` use `Option<Value>`; manifest `Struct` serde is the partition Avro form. The crate version is not bumped.

Design and plan: `docs/superpowers/specs/2026-10-02-tier1-correctness-fixes-design.md` and `docs/superpowers/plans/2026-10-02-tier1-correctness-fixes.md`.

### Decision: repairing tables already written (resolved)

Tables already written with month or bucket partitions carry wrong partition values, so the corrected filters skip those files. Resolved: there is no production data, so no repair tool or feature flag was built. Test tables written before PR #4 need rewriting.
