# `IcebergDvExec` Serialization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship physical plans over Iceberg tables with row-level deletes (v2 position deletes, v3 deletion vectors) through `datafusion-proto`, by encoding `IcebergDvExec` in `IcebergPhysicalExtensionCodec`. Each plan carries only the delete bitmaps of the files it scans.

**Architecture:** `IcebergPhysicalExtensionCodec::try_encode` and `try_decode` gain an `IcebergDvExec` payload: a versioned tag, the path and row-number column names, the strip flag, and `(normalized path, deletion-vector-v1 blob)` entries. The payload is pruned to the data files the child scan reads. Decoding rebuilds the node with `IcebergDvExec::try_new` over the decoded child. Full round trips depend on a DataFusion fix that serializes the Parquet row-number virtual column; tasks 4–6 wait for it.

**Tech Stack:** Rust, DataFusion 55 (andts fork, local path deps), `datafusion-proto`, iceberg-rust, tokio tests.

**Spec:** `docs/superpowers/specs/2026-10-04-dv-exec-serialization-design.md`. It depends on `~/Projects/datafusion-andts/docs/superpowers/specs/2026-10-04-proto-virtual-columns-design.md` (tasks 4–6) and builds on `docs/superpowers/specs/2026-10-04-physical-plan-codec-design.md`.

## Global Constraints

- **Run cargo through the capped wrapper**, never bare: `$CARGO` means `.superpowers/sdd/2026-10-04-physical-plan-codec/capcargo` (12G memory, 4 jobs, `target/claude`). Run `cargo fmt` as `~/.cargo/bin/cargo fmt --all`; cargo is not on PATH. Run one test binary at a time.
- **Branch:** `feat/dv-exec-serialization`, created from `feat/physical-plan-codec` (`651c155`). Workspace deps stay on the local `../../datafusion-andts` paths.
- **Payload tag** is exactly `b"datafusion_iceberg/dv-exec/v1"`. Layout, big-endian:
  1. the tag;
  2. `u32` length + UTF-8 path column name;
  3. `u32` length + UTF-8 row-number column name;
  4. `u8` strip flag (0/1);
  5. `u32` entry count;
  6. that many × (`u32` length + UTF-8 normalized path, then `u32` length + `DeletionVector::to_bytes()`).

  Entries are sorted by path.
- **Columns are encoded by name, not index.** Decode always goes through `IcebergDvExec::try_new`.
- **Errors:**
  - Unknown tag, truncated payload, trailing bytes, strip flag other than 0/1, or input count ≠ 1 → `internal_err!`.
  - Plan nodes other than `IcebergDvExec` → `not_impl_err!` naming the node, unchanged.
  - A failed encode writes nothing to `buf`.
- **Pruning:** keep the map entries whose key equals `util::strip_prefix(p)` for some `__data_file_path` partition value `p` of a file in the child's `FileScanConfig`s. Fall back to the full map when the child has no `FileScanConfig`, has one without the path partition column, or a file's value is not a non-null `Utf8`.
- **Visibility:**
  - `pub(crate) mod dv_exec;`.
  - The accessors on `IcebergDvExec` are `pub(crate)` and `#[cfg(feature = "proto")]`. Without the gate, a build without the feature has dead code and clippy fails.
  - Nothing new is public API.
- **Encode plans before executing them** in every test.
- **Done means** all of these are clean:
  - `$CARGO clippy --all-targets --all-features -- -D warnings`
  - `$CARGO clippy -p datafusion_iceberg --all-targets -- -D warnings`
  - `~/.cargo/bin/cargo fmt --all -- --check`
  - the non-container `datafusion_iceberg` tests with `--features proto`

## Review Focus

1. **One data file split across two tasks by byte range.** Both tasks must receive that file's bitmap. Deletes then apply by absolute position, so neither half drops the wrong rows or keeps a deleted one. Covered by Task 2 (pruning keeps the file in both halves) and Task 6 (execution of both shipped halves).
2. **A corrupted or foreign payload reaches `try_decode`** (truncated, trailing bytes, other version, empty). It must return an error, never panic or decode as v1. Covered by Task 1.
3. **A path stored with a scheme (`file:///…`, `s3://…`) while the map key is the stripped path.** Pruning must normalize the same way `IcebergDvExec` does at lookup, or every entry is pruned away and deletes silently stop applying. Covered by Task 2 (`pruning_normalizes_paths_like_the_lookup`), and end to end by Task 5.
4. **A user who opted in to `__data_file_path`** (`strip_path_col = false`). The decoded node must keep the column in its output. Covered by Task 1.
5. **Equality deletes in the same shipped plan.** They are standard anti-join nodes and must ship without Iceberg-specific encoding. Covered by Task 3.

---

### Task 1: Shared DV fixture; codec encodes and decodes `IcebergDvExec`

**Files:**
- Create: `datafusion_iceberg/src/table/dv_fixture.rs` (`#[cfg(test)]`)
- Modify: `datafusion_iceberg/src/table/mod.rs:5` (module declarations) and the test `row_number_virtual_column_drives_dv_filter_with_pushdown` (`:2033-2205`)
- Modify: `datafusion_iceberg/src/table/dv_exec.rs` (accessors)
- Modify: `datafusion_iceberg/src/codec.rs`
- Modify: `datafusion_iceberg/tests/position_delete.rs` (remove `plan_does_not_serialize`)

**Interfaces:**
- Produces (test-only, `crate::table::dv_fixture`):
  - `DvFixture::new(files: &[(&str, &[i64])]) -> DvFixture` (async), with fields `ctx: SessionContext` and `url: ObjectStoreUrl`;
  - methods `file(&self, path: &str) -> PartitionedFile`, `split_at_second_row_group(&self, path: &str) -> (PartitionedFile, PartitionedFile)`, and `scan(&self, files: Vec<PartitionedFile>, predicate: Option<Arc<dyn PhysicalExpr>>) -> Arc<dyn ExecutionPlan>` (async);
  - free functions `dv_exec(child, dvs: HashMap<String, DeletionVector>, strip_path_col: bool) -> Arc<dyn ExecutionPlan>`, `dv_entry(path: &str, positions: &[u64]) -> (String, DeletionVector)`, `v_at_least(n: i64) -> Arc<dyn PhysicalExpr>` and `int64_values(batches: &[RecordBatch], column: &str) -> Vec<i64>`.
- Produces (`IcebergDvExec`, `cfg(feature = "proto")`): `dvs(&self) -> &HashMap<String, DeletionVector>`, `path_column_name(&self) -> String`, `row_number_column_name(&self) -> String`, `strip_path_col(&self) -> bool`.
- Produces (`codec.rs`): `const DV_EXEC_V1`, `fn encode_dv_exec(exec: &IcebergDvExec, buf: &mut Vec<u8>) -> Result<()>`, `fn shipped_entries(exec: &IcebergDvExec) -> Vec<(&String, &DeletionVector)>` (full map, sorted; Task 2 prunes it), `fn decode_dv_exec(payload: &[u8], input: &Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>>`.

- [ ] **Step 1: Branch**

```bash
git switch feat/physical-plan-codec && git switch -c feat/dv-exec-serialization
```

- [ ] **Step 2: Extract the fixture (pure refactor)**

Create `datafusion_iceberg/src/table/dv_fixture.rs`:

```rust
//! Plan-level fixture for `IcebergDvExec`: Parquet data files in an in-memory
//! store, scanned the way `table_scan` scans tables with row-level deletes
//! (data-file path partition column plus row-number virtual column).

use std::{collections::HashMap, sync::Arc};

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::datasource::file_format::{parquet::ParquetFormat, FileFormat};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    parquet::source::ParquetSource, FileGroup, FileScanConfigBuilder,
};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Operator;
use datafusion::parquet::arrow::{ArrowWriter, RowNumber};
use datafusion::parquet::file::metadata::ParquetMetaDataReader;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::prelude::SessionContext;
use iceberg_rust::spec::{deletion_vector::DeletionVector, util};
use object_store::{memory::InMemory, path::Path as ObjPath, ObjectStoreExt, PutPayload};
use roaring::RoaringTreemap;

use super::{
    dv_exec::IcebergDvExec, object_store_url_for_location, DATA_FILE_PATH_COLUMN,
    ROW_NUMBER_COLUMN,
};

/// Data files are written in row groups of this many rows, so a longer file
/// has several: scans can split it by byte range and prune by statistics.
const ROWS_PER_ROW_GROUP: usize = 4;

pub(crate) struct DvFixture {
    pub(crate) ctx: SessionContext,
    pub(crate) url: ObjectStoreUrl,
    file_schema: SchemaRef,
    files: HashMap<String, Vec<u8>>,
}

impl DvFixture {
    /// Write one Parquet data file per `(path, values)`, each a single Int64
    /// column `v`, to an in-memory store registered on `self.ctx`.
    pub(crate) async fn new(files: &[(&str, &[i64])]) -> Self {
        let file_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let store = Arc::new(InMemory::new());
        let mut written = HashMap::new();
        for (path, values) in files {
            let batch = RecordBatch::try_new(
                file_schema.clone(),
                vec![Arc::new(Int64Array::from(values.to_vec()))],
            )
            .unwrap();
            let props = WriterProperties::builder()
                .set_max_row_group_row_count(Some(ROWS_PER_ROW_GROUP))
                .build();
            let mut buf = Vec::new();
            let mut writer =
                ArrowWriter::try_new(&mut buf, file_schema.clone(), Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            store
                .put(&ObjPath::from(*path), PutPayload::from(buf.clone()))
                .await
                .unwrap();
            written.insert(path.to_string(), buf);
        }
        let url = object_store_url_for_location("memory:///dv_fixture");
        let ctx = SessionContext::new();
        ctx.runtime_env()
            .register_object_store(url.as_ref(), store);
        Self {
            ctx,
            url,
            file_schema,
            files: written,
        }
    }

    /// The whole data file at `path`, carrying its path as the partition
    /// value, as `table_scan` does.
    pub(crate) fn file(&self, path: &str) -> PartitionedFile {
        let mut file = PartitionedFile::new(path.to_string(), self.files[path].len() as u64);
        file.partition_values = vec![ScalarValue::Utf8(Some(path.to_string()))];
        file
    }

    /// `path` split into two byte ranges at the start of its second row group:
    /// the first range reads row group 0, the second reads the rest, starting
    /// at absolute row `ROWS_PER_ROW_GROUP`.
    pub(crate) fn split_at_second_row_group(
        &self,
        path: &str,
    ) -> (PartitionedFile, PartitionedFile) {
        let bytes = bytes::Bytes::from(self.files[path].clone());
        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&bytes)
            .unwrap();
        // DataFusion assigns a row group to the range containing its first
        // column chunk's start offset.
        let column = metadata.row_group(1).column(0);
        let split = column
            .dictionary_page_offset()
            .unwrap_or_else(|| column.data_page_offset());
        let whole = self.file(path);
        let size = whole.object_meta.size as i64;
        (whole.clone().with_range(0, split), whole.with_range(split, size))
    }

    /// A Parquet scan of `files` in one file group, configured as `table_scan`
    /// configures scans of tables with row deletes: `v`, the data-file path
    /// partition column and the row-number virtual column are all projected,
    /// and `predicate` is pushed into the reader.
    pub(crate) async fn scan(
        &self,
        files: Vec<PartitionedFile>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
    ) -> Arc<dyn ExecutionPlan> {
        let table_schema = TableSchema::builder(self.file_schema.clone())
            .with_table_partition_cols(vec![Arc::new(Field::new(
                DATA_FILE_PATH_COLUMN,
                DataType::Utf8,
                false,
            ))])
            .with_virtual_columns(vec![Arc::new(
                Field::new(ROW_NUMBER_COLUMN, DataType::Int64, false)
                    .with_extension_type(RowNumber),
            )])
            .build();
        let mut source = ParquetSource::new(table_schema);
        if let Some(predicate) = predicate {
            source = source.with_predicate(predicate).with_pushdown_filters(true);
        }
        let config = FileScanConfigBuilder::new(self.url.clone(), Arc::new(source))
            .with_file_group(FileGroup::new(files))
            .with_projection_indices(Some(vec![0usize, 1, 2]))
            .unwrap()
            .build();
        ParquetFormat::default()
            .create_physical_plan(&self.ctx.state(), config)
            .await
            .unwrap()
    }
}

/// `IcebergDvExec` over `child`, configured as `table_scan` does. With
/// `strip_path_col` the path column is removed from the output, which is the
/// case unless the user opted in to it.
pub(crate) fn dv_exec(
    child: Arc<dyn ExecutionPlan>,
    dvs: HashMap<String, DeletionVector>,
    strip_path_col: bool,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        IcebergDvExec::try_new(
            child,
            Arc::new(dvs),
            DATA_FILE_PATH_COLUMN,
            ROW_NUMBER_COLUMN,
            strip_path_col,
        )
        .unwrap(),
    )
}

/// A deletion vector deleting `positions` of `path`, keyed as `table_scan`
/// keys them.
pub(crate) fn dv_entry(path: &str, positions: &[u64]) -> (String, DeletionVector) {
    (
        util::strip_prefix(path),
        DeletionVector::from(positions.iter().copied().collect::<RoaringTreemap>()),
    )
}

/// The predicate `v >= n`.
pub(crate) fn v_at_least(n: i64) -> Arc<dyn PhysicalExpr> {
    Arc::new(BinaryExpr::new(
        Arc::new(Column::new("v", 0)),
        Operator::GtEq,
        Arc::new(Literal::new(ScalarValue::Int64(Some(n)))),
    ))
}

/// Every value of the Int64 column `column`, in batch order.
pub(crate) fn int64_values(batches: &[RecordBatch], column: &str) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            let idx = batch.schema().index_of(column).unwrap();
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}
```

In `datafusion_iceberg/src/table/mod.rs`, replace `mod dv_exec;` (line 5) with:

```rust
pub(crate) mod dv_exec;
#[cfg(test)]
pub(crate) mod dv_fixture;
```

Replace the whole test `row_number_virtual_column_drives_dv_filter_with_pushdown`, from its doc comment through its closing brace, with:

```rust
    /// End-to-end at the physical-plan level: a real `ParquetSource` scan
    /// configured exactly as `table_scan` does for the DV path — with the
    /// row-number virtual column and predicate pushdown — feeding
    /// `IcebergDvExec`. Proves that (a) the Parquet reader materializes true
    /// absolute row numbers even after pushdown drops rows, and (b) the DV
    /// filter deletes by those true positions and strips the internal columns.
    #[tokio::test]
    async fn row_number_virtual_column_drives_dv_filter_with_pushdown() {
        use datafusion::physical_plan::collect;
        use std::collections::HashMap;

        use super::dv_fixture::{dv_entry, dv_exec, int64_values, v_at_least, DvFixture};
        use super::ROW_NUMBER_COLUMN;

        // One data file of 8 rows in two row groups of 4; row numbers 0..8.
        let data_path = "data/f1.parquet";
        let fixture = DvFixture::new(&[(data_path, &[0, 10, 20, 30, 40, 50, 60, 70])]).await;
        let task_ctx = fixture.ctx.task_ctx();

        // (a) `v >= 40` is pushed into the reader, so the first row group
        // (values 0..30) is pruned by statistics and only positions 4..8
        // survive, carrying their TRUE row numbers.
        let raw = collect(
            fixture
                .scan(vec![fixture.file(data_path)], Some(v_at_least(40)))
                .await,
            task_ctx.clone(),
        )
        .await
        .unwrap();
        assert_eq!(int64_values(&raw, "v"), vec![40, 50, 60, 70]);
        assert_eq!(
            int64_values(&raw, ROW_NUMBER_COLUMN),
            vec![4, 5, 6, 7],
            "row numbers must be the true file positions after the first row group is pruned"
        );

        // (b) The DV deletes absolute position 5 (value 50). The internal
        // columns are stripped, leaving just `v`.
        let dv_plan = dv_exec(
            fixture
                .scan(vec![fixture.file(data_path)], Some(v_at_least(40)))
                .await,
            HashMap::from([dv_entry(data_path, &[5])]),
            true,
        );
        let filtered = collect(dv_plan, task_ctx).await.unwrap();
        assert_eq!(filtered[0].num_columns(), 1, "internal columns stripped");
        assert_eq!(int64_values(&filtered, "v"), vec![40, 60, 70]);
    }
```

Run: `$CARGO test -p datafusion_iceberg --lib -- row_number_virtual_column_drives_dv_filter_with_pushdown`
Expected: PASS. If the compiler reports unused imports left in the `tests` module (they belonged to the old body), remove exactly those.

- [ ] **Step 3: Write the failing codec tests**

`plan_does_not_serialize` in `tests/position_delete.rs` asserts the behaviour this task removes. Delete the function and its call (the comment, the `#[cfg(feature = "proto")]` line and the `plan_does_not_serialize(...).await;` statement after `append_delete(...).commit()`). Tasks 4–5 add the replacement round trip.

In `codec.rs`'s `mod tests`, add these imports to the existing ones:

```rust
    use std::collections::HashMap;

    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::collect;

    use crate::table::dv_fixture::{dv_entry, dv_exec, int64_values, DvFixture};

    const F1: &str = "data/f1.parquet";

    /// Encode `plan` with the codec and decode it over its own children, the
    /// way the proto converter calls the codec (children are decoded first).
    fn reencode(
        plan: Arc<dyn ExecutionPlan>,
        ctx: &TaskContext,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let mut buf = Vec::new();
        IcebergPhysicalExtensionCodec.try_encode(
            plan.clone(),
            &mut buf,
            &DefaultPhysicalProtoConverter {},
        )?;
        let inputs: Vec<_> = plan.children().into_iter().cloned().collect();
        IcebergPhysicalExtensionCodec.try_decode(
            &buf,
            &inputs,
            ctx,
            &DefaultPhysicalProtoConverter {},
        )
    }
```

and these tests:

```rust
    #[tokio::test]
    async fn dv_exec_round_trips_and_deletes_by_position() {
        let fixture = DvFixture::new(&[(F1, &[0, 10, 20, 30, 40, 50, 60, 70])]).await;
        let plan = dv_exec(
            fixture.scan(vec![fixture.file(F1)], None).await,
            HashMap::from([dv_entry(F1, &[1, 5])]),
            true,
        );
        let task_ctx = fixture.ctx.task_ctx();
        let decoded = reencode(plan.clone(), &task_ctx).unwrap();
        assert_eq!(decoded.schema(), plan.schema());

        let here = int64_values(&collect(plan, task_ctx.clone()).await.unwrap(), "v");
        let shipped = int64_values(&collect(decoded, task_ctx).await.unwrap(), "v");
        assert_eq!(here, vec![0, 20, 30, 40, 60, 70]);
        assert_eq!(shipped, here);
    }

    /// A user who opted in to `__data_file_path` keeps it after decoding.
    #[tokio::test]
    async fn dv_exec_keeps_the_path_column_when_not_stripped() {
        let fixture = DvFixture::new(&[(F1, &[0, 10, 20, 30])]).await;
        let plan = dv_exec(
            fixture.scan(vec![fixture.file(F1)], None).await,
            HashMap::from([dv_entry(F1, &[0])]),
            false,
        );
        let decoded = reencode(plan.clone(), &fixture.ctx.task_ctx()).unwrap();
        assert_eq!(decoded.schema().fields().len(), 2, "v and the path column");
        assert_eq!(decoded.schema(), plan.schema());
    }

    #[tokio::test]
    async fn malformed_dv_exec_payloads_are_rejected() {
        let fixture = DvFixture::new(&[(F1, &[0, 10, 20, 30])]).await;
        let child = fixture.scan(vec![fixture.file(F1)], None).await;
        let plan = dv_exec(child.clone(), HashMap::from([dv_entry(F1, &[0])]), true);
        let mut buf = Vec::new();
        IcebergPhysicalExtensionCodec
            .try_encode(plan, &mut buf, &DefaultPhysicalProtoConverter {})
            .unwrap();

        let task_ctx = fixture.ctx.task_ctx();
        let decode = |payload: &[u8], inputs: &[Arc<dyn ExecutionPlan>]| {
            IcebergPhysicalExtensionCodec.try_decode(
                payload,
                inputs,
                &task_ctx,
                &DefaultPhysicalProtoConverter {},
            )
        };
        assert!(decode(&buf, &[child.clone()]).is_ok(), "the valid payload");
        assert!(decode(&buf, &[]).is_err(), "no input");
        assert!(decode(&buf, &[child.clone(), child.clone()]).is_err(), "two inputs");
        assert!(decode(&buf[..buf.len() - 1], &[child.clone()]).is_err(), "truncated");
        let mut trailing = buf.clone();
        trailing.push(0);
        assert!(decode(&trailing, &[child.clone()]).is_err(), "trailing bytes");
        assert!(
            decode(b"datafusion_iceberg/dv-exec/v2", &[child.clone()]).is_err(),
            "other version"
        );
        assert!(decode(b"", &[child]).is_err(), "empty");
    }
```

Leave the existing `plan_nodes_are_declined_by_name` (`EmptyExec`) as is.

- [ ] **Step 4: Run them and watch the round trips fail**

Run: `$CARGO test -p datafusion_iceberg --features proto --lib -- codec::tests`

Expected: it compiles, and:
- `dv_exec_round_trips_and_deletes_by_position` and `dv_exec_keeps_the_path_column_when_not_stripped` FAIL with `NotImplemented("IcebergPhysicalExtensionCodec does not encode plan node IcebergDvExec")`.
- `malformed_dv_exec_payloads_are_rejected` FAILS at its `.unwrap()` on encode.
- The 4 existing tests pass.

- [ ] **Step 5: Accessors**

In `datafusion_iceberg/src/table/dv_exec.rs`, after the `impl IcebergDvExec { ... }` block that holds `try_new`, add:

```rust
/// What `IcebergPhysicalExtensionCodec` needs to serialize this node; every
/// other field is derived again by `try_new` on decode.
#[cfg(feature = "proto")]
impl IcebergDvExec {
    pub(crate) fn dvs(&self) -> &HashMap<String, DeletionVector> {
        &self.dvs
    }

    pub(crate) fn path_column_name(&self) -> String {
        self.input.schema().field(self.path_col_idx).name().clone()
    }

    pub(crate) fn row_number_column_name(&self) -> String {
        self.input.schema().field(self.row_number_col_idx).name().clone()
    }

    pub(crate) fn strip_path_col(&self) -> bool {
        self.strip_path_col
    }
}
```

(`input()` comes in Task 2, with its only user.)

- [ ] **Step 6: Encode and decode `IcebergDvExec`**

In `codec.rs`, extend the imports:

```rust
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::{internal_datafusion_err, internal_err, not_impl_err, Result};
use iceberg_rust::spec::deletion_vector::DeletionVector;

use crate::table::dv_exec::IcebergDvExec;
```

(keep the existing `TaskContext`, `PhysicalExprAdapterFactory`, `ExecutionPlan`, codec-trait and `IcebergPhysicalExprAdapterFactory` imports). Add after `FIELD_ID_ADAPTER_V1`:

```rust
/// Payload tag for `IcebergDvExec`. Layout after the tag (big-endian):
/// path column name, row-number column name (each `u32` length + UTF-8),
/// strip flag (`u8`), entry count (`u32`), then per entry the normalized
/// data-file path and its `deletion-vector-v1` blob (each `u32` length +
/// bytes), sorted by path.
const DV_EXEC_V1: &[u8] = b"datafusion_iceberg/dv-exec/v1";
```

Replace the bodies of `try_decode` and `try_encode`:

```rust
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Unknown payloads are an internal error: `ComposedPhysicalExtensionCodec`
        // routes a payload back only to the codec that wrote it.
        let Some(payload) = buf.strip_prefix(DV_EXEC_V1) else {
            return internal_err!(
                "unknown datafusion_iceberg plan payload ({} bytes)",
                buf.len()
            );
        };
        let [input] = inputs else {
            return internal_err!("IcebergDvExec takes one input, got {}", inputs.len());
        };
        decode_dv_exec(payload, input)
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        match node.downcast_ref::<IcebergDvExec>() {
            Some(exec) => encode_dv_exec(exec, buf),
            None => not_impl_err!(
                "IcebergPhysicalExtensionCodec does not encode plan node {}",
                node.name()
            ),
        }
    }
```

Add these free functions after the `impl PhysicalExtensionCodec` block:

```rust
fn encode_dv_exec(exec: &IcebergDvExec, buf: &mut Vec<u8>) -> Result<()> {
    // Build the payload apart from `buf` so a failed encode writes nothing.
    let mut out = DV_EXEC_V1.to_vec();
    put_bytes(&mut out, exec.path_column_name().as_bytes())?;
    put_bytes(&mut out, exec.row_number_column_name().as_bytes())?;
    out.push(u8::from(exec.strip_path_col()));
    let entries = shipped_entries(exec);
    put_len(&mut out, entries.len())?;
    for (path, dv) in entries {
        put_bytes(&mut out, path.as_bytes())?;
        let blob = dv.to_bytes().map_err(|e| {
            internal_datafusion_err!("IcebergDvExec: encoding the deletion vector of {path}: {e}")
        })?;
        put_bytes(&mut out, &blob)?;
    }
    buf.extend_from_slice(&out);
    Ok(())
}

/// The deletion vectors `exec` ships, sorted by path.
fn shipped_entries(exec: &IcebergDvExec) -> Vec<(&String, &DeletionVector)> {
    let mut entries: Vec<_> = exec.dvs().iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    entries
}

fn decode_dv_exec(payload: &[u8], input: &Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let mut reader = PayloadReader(payload);
    let path_column = reader.string()?;
    let row_number_column = reader.string()?;
    let strip_path_col = match reader.u8()? {
        0 => false,
        1 => true,
        flag => return internal_err!("IcebergDvExec payload: invalid strip flag {flag}"),
    };
    let count = reader.len()?;
    let dvs = (0..count)
        .map(|_| {
            let path = reader.string()?;
            let dv = DeletionVector::try_from(reader.bytes()?).map_err(|e| {
                internal_datafusion_err!("IcebergDvExec payload: deletion vector of {path}: {e}")
            })?;
            Ok((path, dv))
        })
        .collect::<Result<HashMap<_, _>>>()?;
    if !reader.0.is_empty() {
        return internal_err!(
            "IcebergDvExec payload: {} trailing bytes",
            reader.0.len()
        );
    }
    Ok(Arc::new(IcebergDvExec::try_new(
        Arc::clone(input),
        Arc::new(dvs),
        &path_column,
        &row_number_column,
        strip_path_col,
    )?))
}

fn put_len(out: &mut Vec<u8>, len: usize) -> Result<()> {
    let len = u32::try_from(len)
        .map_err(|_| internal_datafusion_err!("IcebergDvExec payload field of {len} exceeds u32"))?;
    out.extend_from_slice(&len.to_be_bytes());
    Ok(())
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    put_len(out, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}

/// Reads the `IcebergDvExec` payload; every read fails, rather than panics,
/// on a truncated payload.
struct PayloadReader<'a>(&'a [u8]);

impl<'a> PayloadReader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.0.len() < n {
            return internal_err!("IcebergDvExec payload is truncated");
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn len(&mut self) -> Result<usize> {
        let bytes: [u8; 4] = self.take(4)?.try_into().expect("took 4 bytes");
        Ok(u32::from_be_bytes(bytes) as usize)
    }

    fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.len()?;
        self.take(n)
    }

    fn string(&mut self) -> Result<String> {
        String::from_utf8(self.bytes()?.to_vec())
            .map_err(|e| internal_datafusion_err!("IcebergDvExec payload: {e}"))
    }
}
```

If clippy flags `len` on `PayloadReader` (`len_without_is_empty`), rename it to `read_len` in all four places.

- [ ] **Step 7: Run the tests**

Run: `$CARGO test -p datafusion_iceberg --features proto --lib -- codec::tests`
Expected: 7 passed (4 existing + 3 new).

Run: `$CARGO test -p datafusion_iceberg --test position_delete`
Expected: PASS.

- [ ] **Step 8: Lint and commit**

Run:
```bash
$CARGO clippy -p datafusion_iceberg --all-targets --all-features -- -D warnings
$CARGO clippy -p datafusion_iceberg --all-targets -- -D warnings
~/.cargo/bin/cargo fmt --all -- --check
```
Expected: clean. The run without features proves the accessors' `cfg` gate works.

```bash
git add datafusion_iceberg/src/table/dv_fixture.rs datafusion_iceberg/src/table/mod.rs datafusion_iceberg/src/table/dv_exec.rs datafusion_iceberg/src/codec.rs datafusion_iceberg/tests/position_delete.rs
git commit -m "feat(datafusion): IcebergPhysicalExtensionCodec encodes IcebergDvExec"
```

---

### Task 2: Ship only the deletes of the scanned files

**Files:**
- Modify: `datafusion_iceberg/src/table/dv_exec.rs` (the `input()` accessor)
- Modify: `datafusion_iceberg/src/codec.rs` (`shipped_entries`, plus tests)

**Interfaces:**
- Consumes: Task 1's fixture, `reencode`, `F1`, and `IcebergDvExec::dvs()`.
- Produces: `IcebergDvExec::input(&self) -> &Arc<dyn ExecutionPlan>` (`cfg(feature = "proto")`), and `fn scanned_data_files(plan: &Arc<dyn ExecutionPlan>, path_column: &str) -> Option<HashSet<String>>` in `codec.rs`.

- [ ] **Step 1: Write the failing tests**

Add to `codec.rs` `mod tests`:

```rust
    use datafusion::common::ScalarValue;

    use crate::table::dv_exec::IcebergDvExec;

    const F2: &str = "data/f2.parquet";

    /// The data-file paths whose deletion vectors `plan` ships.
    fn shipped_dv_paths(plan: Arc<dyn ExecutionPlan>, ctx: &TaskContext) -> Vec<String> {
        let decoded = reencode(plan, ctx).unwrap();
        let mut paths: Vec<_> = decoded
            .downcast_ref::<IcebergDvExec>()
            .unwrap()
            .dvs()
            .keys()
            .cloned()
            .collect();
        paths.sort();
        paths
    }

    #[tokio::test]
    async fn each_task_ships_only_the_deletes_of_its_files() {
        let fixture = DvFixture::new(&[(F1, &[0, 10, 20, 30]), (F2, &[40, 50, 60, 70])]).await;
        let dvs = HashMap::from([dv_entry(F1, &[0]), dv_entry(F2, &[1])]);
        let ctx = fixture.ctx.task_ctx();

        let task1 = dv_exec(fixture.scan(vec![fixture.file(F1)], None).await, dvs.clone(), true);
        let task2 = dv_exec(fixture.scan(vec![fixture.file(F2)], None).await, dvs.clone(), true);
        let both = dv_exec(
            fixture.scan(vec![fixture.file(F1), fixture.file(F2)], None).await,
            dvs,
            true,
        );
        assert_eq!(shipped_dv_paths(task1, &ctx), vec![F1]);
        assert_eq!(shipped_dv_paths(task2, &ctx), vec![F2]);
        assert_eq!(shipped_dv_paths(both, &ctx), vec![F1, F2]);
    }

    /// Two tasks reading one file by byte range both need its bitmap.
    #[tokio::test]
    async fn both_halves_of_a_split_file_ship_its_deletes() {
        let fixture =
            DvFixture::new(&[(F1, &[0, 10, 20, 30, 40, 50, 60, 70]), (F2, &[80])]).await;
        let dvs = HashMap::from([dv_entry(F1, &[1, 5]), dv_entry(F2, &[0])]);
        let ctx = fixture.ctx.task_ctx();
        let (head, tail) = fixture.split_at_second_row_group(F1);
        for half in [head, tail] {
            let task = dv_exec(fixture.scan(vec![half], None).await, dvs.clone(), true);
            assert_eq!(shipped_dv_paths(task, &ctx), vec![F1]);
        }
    }

    /// When the files can't be determined, shipping everything is correct.
    #[tokio::test]
    async fn without_a_file_scan_every_delete_ships() {
        let fixture = DvFixture::new(&[(F1, &[0, 10, 20, 30])]).await;
        let schema = fixture.scan(vec![fixture.file(F1)], None).await.schema();
        let task = dv_exec(
            Arc::new(EmptyExec::new(schema)),
            HashMap::from([dv_entry(F1, &[0]), dv_entry(F2, &[0])]),
            true,
        );
        assert_eq!(shipped_dv_paths(task, &fixture.ctx.task_ctx()), vec![F1, F2]);
    }
```

`EmptyExec` is already imported in the module, for `plan_nodes_are_declined_by_name`.

Add one more test. It pins Review Focus 3, because it fails if pruning compares raw partition values with the normalized map keys:

```rust
    /// Map keys are normalized (`strip_prefix`); partition values carry the
    /// path as stored in the manifest, scheme included.
    #[tokio::test]
    async fn pruning_normalizes_paths_like_the_lookup() {
        let fixture = DvFixture::new(&[(F1, &[0, 10, 20, 30])]).await;
        let stored = format!("s3://bucket/{F1}");
        let mut file = fixture.file(F1);
        file.partition_values = vec![ScalarValue::Utf8(Some(stored.clone()))];
        let task = dv_exec(
            fixture.scan(vec![file], None).await,
            HashMap::from([dv_entry(&stored, &[0]), dv_entry(F2, &[0])]),
            true,
        );
        assert_eq!(
            shipped_dv_paths(task, &fixture.ctx.task_ctx()),
            vec![format!("/{F1}")]
        );
    }
```

- [ ] **Step 2: Run them and watch pruning fail**

Run: `$CARGO test -p datafusion_iceberg --features proto --lib -- codec::tests`
Expected:
- `each_task_ships_only_the_deletes_of_its_files`, `both_halves_of_a_split_file_ship_its_deletes` and `pruning_normalizes_paths_like_the_lookup` FAIL, because every entry ships (e.g. `left: ["data/f1.parquet", "data/f2.parquet"]`, `right: ["data/f1.parquet"]`).
- `without_a_file_scan_every_delete_ships` passes.
- The others pass.

- [ ] **Step 3: Implement pruning**

In `dv_exec.rs`, add to the `#[cfg(feature = "proto")] impl IcebergDvExec` block:

```rust
    pub(crate) fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
```

In `codec.rs`, add imports:

```rust
use std::collections::HashSet;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::ScalarValue;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use iceberg_rust::spec::util;
```

Replace `shipped_entries` with:

```rust
/// The deletion vectors `exec` ships, sorted by path: those of the data files
/// its child scans. An engine that splits a scan into tasks serializes each
/// task's plan separately, so this keeps the shipped bytes close to the deletes
/// actually applied. If the scanned files can't be determined, all are shipped.
fn shipped_entries(exec: &IcebergDvExec) -> Vec<(&String, &DeletionVector)> {
    let scanned = scanned_data_files(exec.input(), &exec.path_column_name());
    let mut entries: Vec<_> = exec
        .dvs()
        .iter()
        .filter(|(path, _)| scanned.as_ref().is_none_or(|files| files.contains(*path)))
        .collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    entries
}

/// Normalized paths of the data files `plan` scans, read from the
/// `path_column` partition value of each file and normalized exactly as
/// `IcebergDvExec` does at lookup. `None` when that can't be determined.
fn scanned_data_files(plan: &Arc<dyn ExecutionPlan>, path_column: &str) -> Option<HashSet<String>> {
    let mut files = HashSet::new();
    let mut found_scan = false;
    let mut complete = true;
    plan.apply(|node| {
        let Some(config) = node
            .downcast_ref::<DataSourceExec>()
            .and_then(|exec| exec.data_source().downcast_ref::<FileScanConfig>())
        else {
            return Ok(TreeNodeRecursion::Continue);
        };
        found_scan = true;
        let Some(idx) = config
            .table_partition_cols()
            .iter()
            .position(|field| field.name() == path_column)
        else {
            complete = false;
            return Ok(TreeNodeRecursion::Continue);
        };
        for file in config.file_groups.iter().flat_map(|group| group.iter()) {
            match file.partition_values.get(idx) {
                Some(ScalarValue::Utf8(Some(path))) => {
                    files.insert(util::strip_prefix(path));
                }
                _ => complete = false,
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .ok()?;
    (found_scan && complete).then_some(files)
}
```

- [ ] **Step 4: Run the tests**

Run: `$CARGO test -p datafusion_iceberg --features proto --lib -- codec::tests`
Expected: 11 passed.

To see the normalization test guard: temporarily change `files.insert(util::strip_prefix(path));` to `files.insert(path.clone());`. Rerun. Expected: `pruning_normalizes_paths_like_the_lookup` FAILS with an empty list. Restore the line.

- [ ] **Step 5: Lint and commit**

Run the same three lint commands as Task 1 Step 8. Expected: clean.

```bash
git add datafusion_iceberg/src/table/dv_exec.rs datafusion_iceberg/src/codec.rs
git commit -m "feat(datafusion): ship only the deletes of the files a plan scans"
```

---

### Task 3: Equality-delete plans ship

**Files:**
- Create: `datafusion_iceberg/tests/shipping/mod.rs`
- Modify: `datafusion_iceberg/tests/equality_delete.rs`

**Interfaces:**
- Produces (test helper module, `#![cfg(feature = "proto")]`): `pub fn executor(location: &str) -> SessionContext`, `pub fn ship(plan: Arc<dyn ExecutionPlan>, executor: &SessionContext) -> Arc<dyn ExecutionPlan>`, and `pub async fn execute_shipped(ctx: &SessionContext, query: &str, location: &str) -> Vec<RecordBatch>`.

- [ ] **Step 1: Shared shipping helper**

Create `datafusion_iceberg/tests/shipping/mod.rs`:

```rust
//! Shipping physical plans to another process the way a distributed executor
//! does: encode with datafusion-proto, then decode on a session that has only
//! the table's object store.
#![cfg(feature = "proto")]
// Each test binary uses a subset of these helpers.
#![allow(dead_code)]

use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion_iceberg::{object_store_url_for_location, IcebergPhysicalExtensionCodec};
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_extension_codec, physical_plan_to_bytes_with_extension_codec,
};
use object_store::local::LocalFileSystem;

/// An executor session with only the store of the table at `location`
/// registered. These fixtures keep tables on an unprefixed `LocalFileSystem`.
pub fn executor(location: &str) -> SessionContext {
    let ctx = SessionContext::new();
    ctx.runtime_env().register_object_store(
        object_store_url_for_location(location).as_ref(),
        Arc::new(LocalFileSystem::new()),
    );
    ctx
}

/// Encode `plan` and decode it on `executor`. Call this before executing
/// `plan`: an executed plan carries runtime dynamic-filter state.
pub fn ship(plan: Arc<dyn ExecutionPlan>, executor: &SessionContext) -> Arc<dyn ExecutionPlan> {
    let bytes = physical_plan_to_bytes_with_extension_codec(plan, &IcebergPhysicalExtensionCodec)
        .expect("encode the plan");
    physical_plan_from_bytes_with_extension_codec(
        &bytes,
        &executor.task_ctx(),
        &IcebergPhysicalExtensionCodec,
    )
    .expect("decode the plan on the executor")
}

/// Plan `query` on `ctx`, ship it to an executor for the table at `location`,
/// and execute it there.
pub async fn execute_shipped(ctx: &SessionContext, query: &str, location: &str) -> Vec<RecordBatch> {
    let plan = ctx
        .sql(query)
        .await
        .expect("plan the query")
        .create_physical_plan()
        .await
        .expect("create the physical plan");
    let executor = executor(location);
    collect(ship(plan, &executor), executor.task_ctx())
        .await
        .expect("execute the shipped plan")
}
```

- [ ] **Step 2: Write the round trip test**

In `tests/equality_delete.rs`, add after the `use` lines:

```rust
#[cfg(feature = "proto")]
mod shipping;
```

Insert directly before `let conn = Connection::open_in_memory().unwrap();` (line 223). At that point, `expected` holds the three rows that survive the equality deletes:

```rust
    // Equality deletes are applied by an anti-join of standard DataFusion
    // nodes, so the plan ships without any Iceberg-specific node encoding.
    #[cfg(feature = "proto")]
    assert_batches_eq!(
        expected,
        &shipping::execute_shipped(
            &ctx,
            "select * from warehouse.test.orders order by id",
            &table_dir,
        )
        .await
    );
```

- [ ] **Step 3: Run it**

Run: `$CARGO test -p datafusion_iceberg --features proto --test equality_delete`
Expected: PASS. If it fails, the failure is in DataFusion's native serialization of the join, not in this crate. **Stop** and record the error in the ledger. Per the spec this becomes a separate fix, and this task's test is then `#[ignore]`d with the error quoted.

To see the assertion is live: temporarily change one expected row (e.g. `| 2  | 2` → `| 2  | 9`) and rerun. Expected: FAIL in the shipped assertion. Restore it.

Run: `$CARGO test -p datafusion_iceberg --test equality_delete`
Expected: PASS (feature off: the module and assertion compile out).

- [ ] **Step 4: Lint and commit**

Run the three lint commands. Expected: clean.

```bash
git add datafusion_iceberg/tests/shipping/mod.rs datafusion_iceberg/tests/equality_delete.rs
git commit -m "test(datafusion): equality-delete plans round-trip through datafusion-proto"
```

---

### Task 4: Gate — the row-number column survives `datafusion-proto`

**Precondition: check it first.** The DataFusion virtual-column fix must be in the local checkout:

```bash
grep -n "virtual_columns = 17" ~/Projects/datafusion-andts/datafusion/proto-models/proto/datafusion.proto
git -C ~/Projects/datafusion-andts branch --show-current
```

The first command must print a match. If it doesn't, **stop**: tasks 4–6 wait for the go-ahead.

**Files:**
- Modify: `datafusion_iceberg/tests/position_delete.rs`

**Interfaces:**
- Consumes: `shipping::{executor, ship}` (Task 3).

- [ ] **Step 1: Write the gate test**

In `tests/position_delete.rs`, add after the `use` lines:

```rust
#[cfg(feature = "proto")]
mod shipping;
```

Add this helper before `fn write_position_delete_file`:

```rust
/// The scan under `IcebergDvExec` must still emit the Parquet row-number
/// column after a datafusion-proto round trip, with true file positions:
/// `IcebergDvExec` deletes by them.
#[cfg(feature = "proto")]
async fn row_number_column_survives_shipping(query: &str, ctx: &SessionContext, location: &str) {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::physical_plan::collect;

    let plan = ctx
        .sql(query)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let mut scan = None;
    plan.apply(|node| {
        if node.name() == "IcebergDvExec" {
            scan = Some(node.children()[0].clone());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    let scan = scan.expect("position deletes are applied by IcebergDvExec");
    // The row-number column is the scan's only field with an Arrow extension type.
    let row_number = scan
        .schema()
        .fields()
        .iter()
        .find(|field| field.metadata().contains_key("ARROW:extension:name"))
        .expect("the scan emits the row-number column")
        .clone();

    let executor = shipping::executor(location);
    let shipped = shipping::ship(scan, &executor);
    assert_eq!(
        shipped
            .schema()
            .field_with_name(row_number.name())
            .expect("the row-number column survives"),
        row_number.as_ref()
    );
    let batches = collect(shipped, executor.task_ctx()).await.unwrap();
    let mut positions: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name(row_number.name())
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    positions.sort_unstable();
    // The test's INSERT writes one data file of six rows.
    assert_eq!(positions, (0..6).collect::<Vec<i64>>());
}
```

Call it in `applies_v2_position_deletes`, directly after `.append_delete(delete_files).commit().await.unwrap();`:

```rust
    #[cfg(feature = "proto")]
    row_number_column_survives_shipping(
        "SELECT id, payload FROM warehouse.test.orders",
        &ctx,
        &table_dir,
    )
    .await;
```

- [ ] **Step 2: Run the gate**

Run: `$CARGO test -p datafusion_iceberg --features proto --test position_delete`
Expected: PASS.

If it fails, **stop**. Record in the ledger whether it failed at decode, at the schema assertion or at the positions; the DataFusion fix is incomplete. Then ask.

To see the gate is real: temporarily point the workspace's `datafusion-*` path deps back at the `feat/expr-adapter-factory-serialization` checkout, or `git stash` the DataFusion change there. Rerun and expect a FAIL, then restore. Skip this if the DataFusion checkout has uncommitted work you'd disturb, and record that in the ledger.

- [ ] **Step 3: Lint and commit**

Run the three lint commands. Expected: clean.

```bash
git add datafusion_iceberg/tests/position_delete.rs
git commit -m "test(datafusion): the Parquet row-number column survives datafusion-proto"
```

---

### Task 5: v2 position-delete plans ship and apply deletes on the executor

**Files:**
- Modify: `datafusion_iceberg/tests/position_delete.rs`

**Interfaces:**
- Consumes: `shipping::execute_shipped` (Task 3).

- [ ] **Step 1: Write the round trip**

In `applies_v2_position_deletes`, directly after the first `assert_batches_eq!` (the one expecting ids 1, 3, 4 from `SELECT id, payload ... ORDER BY id`), add:

```rust
    // Shipped to an executor, the plan applies the same deletes. The table's
    // paths are absolute (`/tmp/...`) and map keys are normalized, so this also
    // checks that pruning normalizes the way IcebergDvExec looks up.
    #[cfg(feature = "proto")]
    assert_batches_eq!(
        [
            "+----+---------+",
            "| id | payload |",
            "+----+---------+",
            "| 1  | one     |",
            "| 3  | three   |",
            "| 4  | four    |",
            "+----+---------+",
        ],
        &shipping::execute_shipped(
            &ctx,
            "SELECT id, payload FROM warehouse.test.orders ORDER BY id",
            &table_dir,
        )
        .await
    );
```

- [ ] **Step 2: Run it**

Run: `$CARGO test -p datafusion_iceberg --features proto --test position_delete`
Expected: PASS.

- [ ] **Step 3: Lint and commit**

Run the three lint commands. Expected: clean.

```bash
git add datafusion_iceberg/tests/position_delete.rs
git commit -m "test(datafusion): v2 position-delete plans apply deletes after shipping"
```

---

### Task 6: Plan-level round trips, including split files

**Files:**
- Modify: `datafusion_iceberg/src/table/dv_fixture.rs` (expose the store)
- Modify: `datafusion_iceberg/src/codec.rs` (tests)

**Interfaces:**
- Consumes: Task 1's fixture, `F1`, `dv_entry`, `dv_exec`, `int64_values` and `v_at_least`.
- Produces: `DvFixture.store: Arc<InMemory>` (`pub(crate)`).

- [ ] **Step 1: Expose the fixture's store**

In `dv_fixture.rs`, add the field `pub(crate) store: Arc<InMemory>,` to `DvFixture`. In `new`, change `.register_object_store(url.as_ref(), store);` to `.register_object_store(url.as_ref(), store.clone());`, and add `store,` to the returned struct.

- [ ] **Step 2: Write the tests**

Add to `codec.rs` `mod tests`:

```rust
    use datafusion::prelude::SessionContext;
    use datafusion_proto::bytes::{
        physical_plan_from_bytes_with_extension_codec,
        physical_plan_to_bytes_with_extension_codec,
    };

    use crate::table::dv_fixture::v_at_least;

    /// Ship `plan` through datafusion-proto to an executor session that has
    /// only the fixture's store, and execute it there.
    async fn execute_shipped(fixture: &DvFixture, plan: Arc<dyn ExecutionPlan>) -> Vec<i64> {
        let bytes =
            physical_plan_to_bytes_with_extension_codec(plan, &IcebergPhysicalExtensionCodec)
                .unwrap();
        let executor = SessionContext::new();
        executor
            .runtime_env()
            .register_object_store(fixture.url.as_ref(), fixture.store.clone());
        let decoded = physical_plan_from_bytes_with_extension_codec(
            &bytes,
            &executor.task_ctx(),
            &IcebergPhysicalExtensionCodec,
        )
        .unwrap();
        int64_values(&collect(decoded, executor.task_ctx()).await.unwrap(), "v")
    }

    /// Stands in for v3 deletion vectors: once loaded, they are the same map.
    #[tokio::test]
    async fn dv_exec_ships_with_predicate_pushdown() {
        let fixture = DvFixture::new(&[(F1, &[0, 10, 20, 30, 40, 50, 60, 70])]).await;
        let plan = dv_exec(
            fixture.scan(vec![fixture.file(F1)], Some(v_at_least(40))).await,
            HashMap::from([dv_entry(F1, &[5])]),
            true,
        );
        assert_eq!(execute_shipped(&fixture, plan).await, vec![40, 60, 70]);
    }

    /// One file split by byte range into two tasks, each shipped on its own:
    /// both apply deletes by absolute position, and together they return what
    /// one process does.
    #[tokio::test]
    async fn split_file_tasks_delete_by_absolute_position() {
        let fixture = DvFixture::new(&[(F1, &[0, 10, 20, 30, 40, 50, 60, 70])]).await;
        let dvs = HashMap::from([dv_entry(F1, &[1, 5])]);
        let (head, tail) = fixture.split_at_second_row_group(F1);

        let head_rows = execute_shipped(
            &fixture,
            dv_exec(fixture.scan(vec![head], None).await, dvs.clone(), true),
        )
        .await;
        let tail_rows = execute_shipped(
            &fixture,
            dv_exec(fixture.scan(vec![tail], None).await, dvs.clone(), true),
        )
        .await;
        assert_eq!(head_rows, vec![0, 20, 30], "row group 0, position 1 deleted");
        assert_eq!(tail_rows, vec![40, 60, 70], "row group 1, absolute position 5 deleted");

        let whole = dv_exec(fixture.scan(vec![fixture.file(F1)], None).await, dvs, true);
        let here = int64_values(&collect(whole, fixture.ctx.task_ctx()).await.unwrap(), "v");
        assert_eq!([head_rows, tail_rows].concat(), here);
    }
```

- [ ] **Step 3: Run them**

Run: `$CARGO test -p datafusion_iceberg --features proto --lib -- codec::tests`
Expected: 13 passed.

To see the split test guard positions: temporarily change `dv_entry(F1, &[1, 5])` in `split_file_tasks_delete_by_absolute_position` to `dv_entry(F1, &[1, 1])`. Rerun. Expected: FAIL on `tail_rows` (it keeps 50). Restore it.

- [ ] **Step 4: Lint and commit**

Run the three lint commands. Expected: clean.

```bash
git add datafusion_iceberg/src/table/dv_fixture.rs datafusion_iceberg/src/codec.rs
git commit -m "test(datafusion): shipped DV plans delete by absolute position across file splits"
```

---

### Task 7: Docs and full verification

**Files:**
- Modify: `datafusion_iceberg/README.md`
- Modify: `docs/superpowers/specs/2026-10-04-dv-exec-serialization-design.md`
- Modify: `docs/superpowers/specs/2026-10-04-physical-plan-codec-design.md` (out-of-scope note)

- [ ] **Step 1: README**

In `datafusion_iceberg/README.md`, replace the paragraph that starts `Plans that scan tables with position-delete files (v2) or deletion vectors (v3),` with:

```markdown
Plans over tables with row-level deletes (v2 position-delete files, v3 deletion
vectors) ship too. The deletes are loaded while planning, and each serialized plan
carries only the deletion bitmaps of the data files it scans, so splitting a scan
into many tasks doesn't multiply the payload. `INSERT`s and materialized-view
refreshes can't be serialized yet; serialization fails instead.
```

- [ ] **Step 2: Specs**

- In `2026-10-04-dv-exec-serialization-design.md`, set `**Status:** Implemented`, and tick every checklist box whose work is done. Leave the DataFusion box as it is if that change isn't pushed upstream; say so in the status line.
- In `2026-10-04-physical-plan-codec-design.md`'s "Out of scope", change the `IcebergDvExec` bullet's last sentence to: "Serialized since `2026-10-04-dv-exec-serialization-design.md`."

- [ ] **Step 3: Full verification**

Run one at a time:

```bash
$CARGO build
$CARGO clippy --all-targets --all-features -- -D warnings
$CARGO clippy -p datafusion_iceberg --all-targets -- -D warnings
~/.cargo/bin/cargo fmt --all -- --check
$CARGO test -p datafusion_iceberg --features proto -j 2 --lib <one --test NAME per tests/*.rs except empty_insert, integration_spark, integration_spark_transforms, integration_trino>
```

In zsh, build the `--test` list as an array (`${=ARGS}`); an unsplit string is passed as one argument. Expected: all clean, no failures. Report the container suites as not run unless podman is up.

- [ ] **Step 4: Commit**

```bash
git add datafusion_iceberg/README.md docs/superpowers/specs/2026-10-04-dv-exec-serialization-design.md docs/superpowers/specs/2026-10-04-physical-plan-codec-design.md
git commit -m "docs(datafusion): plans over tables with row deletes can be shipped"
```
