# `datafusion_iceberg`: Serialize `IcebergDvExec` for Shipped Plans — Design / Handoff

**Date:** 2026-10-04
**Status:** Proposed (handoff — not implemented)
**Crate:** `datafusion_iceberg` (feature `proto`)
**Builds on:** `2026-10-04-physical-plan-codec-design.md` (branch
`feat/physical-plan-codec`: `IcebergPhysicalExtensionCodec`,
`object_store_url_for_location`)
**Upstream intent:** applicable to JanKaul/iceberg-rust unchanged; nothing here is
specific to any downstream project.

## Summary

Plans that scan Iceberg tables with **row-level deletes** — v2 position-delete files
or v3 deletion vectors — cannot be serialized with `datafusion-proto`, because the
operator that applies those deletes, `IcebergDvExec`, has no encoding.
`IcebergPhysicalExtensionCodec::try_encode` currently rejects it by name
(`tests/position_delete.rs` pins that). Merge-on-read is the default for
row-level `DELETE`/`UPDATE`/`MERGE` in Spark, Trino and Flink, so in practice this
excludes most tables that have ever been updated.

This change teaches `IcebergPhysicalExtensionCodec` to encode and decode
`IcebergDvExec`, shipping **only the delete bitmaps for the data files the encoded
plan actually scans**.

## Background: how row deletes are applied today

`table_scan` (`datafusion_iceberg/src/table/mod.rs`):

1. Collects position-delete manifest entries and deletion-vector entries while
   planning (`:743–:850`).
2. Loads them **at planning time** into one map,
   `HashMap<String, DeletionVector>`, keyed by the normalized data-file path
   (`iceberg_rust_spec::util::strip_prefix`): DVs via
   `iceberg_rust::table::deletion_vector::load_deletion_vectors`, v2 position
   deletes via `iceberg_rust::table::position_delete::load_position_deletes`, merged
   per path (`:851–:886`).
3. When the map is non-empty, forces the Parquet scan to emit two internal columns —
   `__data_file_path` (a partition column) and a row-number virtual column (Arrow
   `RowNumber` extension type, `:904`) — and wraps each scan in `IcebergDvExec`
   (`:1303`, `:1337`).

`IcebergDvExec` (`table/dv_exec.rs`) looks up each batch's paths in the map
(`:322–:325`) and drops rows whose **absolute in-file position** is set. Because
positions come from the Parquet reader, not a running counter, filtering is correct
however the file is split across partitions or tasks. That property is what makes
shipping it safe.

Its state is: the child plan, the shared map, the path and row-number column
positions, and `strip_path_col`. Everything else (`strip_indices`, output schema,
properties) is derived in `IcebergDvExec::try_new`.

## Design

### Encoding

`IcebergPhysicalExtensionCodec::try_encode`: when the node is an `IcebergDvExec`,
write the payload below. Otherwise keep today's "not implemented" error naming the
node.

The payload is a small, explicit, big-endian binary layout, so no new dependency is
needed:

```text
tag                 b"datafusion_iceberg/dv-exec/v1"
u32 + utf8          path column name
u32 + utf8          row-number column name
u8                  strip_path_col (0/1)
u32                 entry count N
N × {
  u32 + utf8        normalized data-file path (the map key)
  u32 + bytes       DeletionVector::to_bytes()   // `deletion-vector-v1` blob
}
```

- **Column names, not indices.** `try_decode` re-resolves them against the decoded
  child's schema through `IcebergDvExec::try_new`, which already does exactly that.
  The names come from `input.schema().field(path_col_idx).name()` and likewise for the
  row-number column.
- **Bitmaps reuse the spec encoding.** `DeletionVector::to_bytes` /
  `DeletionVector::try_from(&[u8])` (`iceberg-rust-spec/src/spec/deletion_vector.rs`)
  write and parse the Iceberg `deletion-vector-v1` blob, CRC included. No new format
  for the bulk of the payload.

### Per-plan pruning (the important part)

Engines that distribute scans split one `FileScanConfig` into per-task scans over
subsets of files or byte ranges, and serialize each task's plan separately. At encode
time `IcebergDvExec`'s child is therefore a scan over **that task's** files. The
codec encodes only the map entries those files need:

1. Walk the child subtree for `DataSourceExec` → `FileScanConfig` and collect, for each
   `PartitionedFile`, the value of the `__data_file_path` partition column. That is the
   exact string `IcebergDvExec` sees at execution.
2. Normalize each value with `util::strip_prefix`, the same call `IcebergDvExec` makes
   at lookup (`dv_exec.rs:324`), and keep only matching map entries.
3. If the walk finds no `FileScanConfig` (an unexpected child shape), encode the full
   map. That's correct, just larger.

Without pruning every task would receive the whole table's deletes; with it, total
bytes shipped stay roughly equal to the deletes being applied. When two tasks split
one data file by byte range, both receive that file's bitmap, which is required for
correctness.

### Decoding

`try_decode(buf, inputs, ..)`: check the tag, parse the payload, rebuild the map
(`DeletionVector::try_from`), and call
`IcebergDvExec::try_new(inputs[0].clone(), Arc::new(map), path_name, row_number_name, strip_path_col)`.
Exactly one input is required; anything else is an error. An unknown tag is an
`internal_err!`, because `ComposedPhysicalExtensionCodec` only routes a payload back
to the codec that wrote it.

### Visibility

- `table/mod.rs`: `mod dv_exec;` → `pub(crate) mod dv_exec;`.
- `dv_exec.rs`: add `pub(crate)` accessors on `IcebergDvExec`: `input()`, `dvs()`,
  `path_column_name()`, `row_number_column_name()`, `strip_path_col()`. They are
  `#[cfg(feature = "proto")]`, because only the codec uses them; without the gate a
  build without the feature has dead code, and clippy `-D warnings` fails. Nothing
  becomes public API.

### Dependency: the row-number virtual column (needs a DataFusion fix)

The decoded child must still emit the row-number column. It is a `TableSchema`
*virtual column* (the Parquet `RowNumber` extension type). Reading the code (DataFusion
fork at `1bcd67a`) shows that **`datafusion-proto` does not serialize virtual columns**:

- `FileScanConfig::try_to_proto` writes file and partition columns only.
- `parse_table_schema_from_proto` never calls `with_virtual_columns`.
- The scan's projection still references the column, so a decoded child is broken.

The fix belongs in DataFusion and is specified in
`datafusion-andts/docs/superpowers/specs/2026-10-04-proto-virtual-columns-design.md`
(branch `feat/proto-virtual-columns`, stacked on the adapter branch). Test 1 below is
the gate that proves it works end to end through an Iceberg scan.

### Sequencing

Work that does **not** need the DataFusion fix lands first:

- the accessors and the codec's encode/decode with pruning;
- unit tests that call the codec directly on an `IcebergDvExec` and its in-memory
  child, without serializing the child;
- the equality-delete round trip (Test 6), which has no virtual columns.

The full round trips (Tests 1–4) wait for the DataFusion fix in the local
`datafusion-andts` checkout. This crate already builds against that checkout through
path dependencies.

## Alternatives considered

- **Ship delete-file *references* and load on executors.** Encode, per task, the
  delete files (DV puffin blob offsets, position-delete Parquet files) that apply to
  its data files, and have `IcebergDvExec` load them lazily on first use. Plans stay
  tiny and loading parallelizes across executors, instead of running serially in the
  planning process before anything is distributed. Costs a lazy-loading mode in
  `IcebergDvExec`, duplicate reads of v2 position-delete files that cover data files in
  several tasks, and the sequence-number filtering
  (`active_data_sequence_numbers` in `load_position_deletes`) moving to executors.
  **Deferred, not rejected:** worth doing once planning-time delete loading is measured
  to matter. This design keeps the change small and leaves that door open (a new
  payload tag).
- **Ship the full map to every task.** Simplest, but plan size grows with total
  deletes times task count.
- **Apply deletes only in the planning process.** Executors would stream undeleted
  rows back for filtering, which defeats distributing the scan.

## Out of scope

- **Equality deletes** are applied by an anti-join that the scan builds from standard
  DataFusion nodes (`HashJoinExec`, `UnionExec` over the delete files). Those serialize
  natively. This change adds a round-trip test for them (below) rather than new code;
  if it fails, that's a separate fix.
- Lazy reference-based loading (see Alternatives).

## Compatibility

Feature-gated (`proto`), additive. Plans over tables with row deletes go from
"serialization fails naming `IcebergDvExec`" to "serializes". No change to scans
without deletes.

## Test plan

1. **Virtual column survives (gate).** Plan a scan over a table with deletes,
   round-trip *only the child* `DataSourceExec` with `datafusion-proto`, and assert the
   decoded schema still has the row-number field with the `RowNumber` extension
   metadata and that executing it yields correct absolute positions.
2. **Round trip, v2 position deletes.** Turn
   `tests/position_delete.rs::plan_does_not_serialize` into a round trip: serialize with
   `IcebergPhysicalExtensionCodec` before executing, decode on a fresh session with the
   table's store registered under `object_store_url_for_location`, execute both, and
   assert equal results with the deleted rows absent.
3. **Round trip, plan level (stands in for v3 deletion vectors).** iceberg-rust can
   read but not write puffin deletion vectors, so a v3 table can't be built in a test.
   Once loaded, v2 and v3 deletes are the same `DeletionVector` map, so this test
   builds `IcebergDvExec` over a real Parquet scan configured exactly as `table_scan`
   does (row-number virtual column, path partition column, predicate pushdown). It
   reuses the fixture behind `row_number_virtual_column_drives_dv_filter_with_pushdown`,
   extracted into a shared `#[cfg(test)]` module. It round-trips through
   `datafusion-proto` and asserts deletes by absolute position.
4. **Split files across "tasks".** Repartition the scan's file groups into byte ranges
   (`FileGroupPartitioner` with `with_repartition_file_min_size(0)`) so one data file
   spans two scans. Wrap each in its own `IcebergDvExec` the way a distributed planner
   would, encode and decode each separately, execute both, and assert the union equals
   the single-process result. This proves position-based filtering under splits.
5. **Pruning.** For a two-task split over different files, decode each payload and
   assert its map holds only that task's files.
6. **Equality deletes.** Round trip over the `tests/equality_delete.rs` fixture; results
   equal.
7. **Codec unit tests.** Unknown tag errors; wrong input count errors;
   non-`IcebergDvExec` nodes still fail naming the node.

Encode **before executing**: an executed plan carries runtime dynamic-filter state
(TopK thresholds, populated join filters) that would skew round-trip results.

## Implementation checklist

- [ ] DataFusion: virtual columns serialized (separate handoff, see Dependency).
- [ ] Test 1 (gate, after the DataFusion fix); stop if it fails.
- [ ] `dv_exec.rs`: `pub(crate)` accessors; `table/mod.rs`: `pub(crate) mod dv_exec;`.
- [ ] `codec.rs`: encode (with pruning) / decode for `IcebergDvExec`; payload helpers.
- [ ] Tests 2–7; update `tests/position_delete.rs` (it currently pins the failure).
- [ ] README "shipping plans" section: drop the row-delete limitation; mention pruning.
- [ ] `make test-datafusion_iceberg`; `cargo clippy --all-targets --all-features -- -D warnings`.
