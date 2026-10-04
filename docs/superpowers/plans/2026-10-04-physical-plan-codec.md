# Physical Plan Codec Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let `datafusion_iceberg` physical plans be shipped with `datafusion-proto` and execute identically elsewhere: serialize the field-id expression adapter, and expose the object store URL that scans reference.

**Architecture:** A new `proto` feature adds `IcebergPhysicalExtensionCodec` (`src/codec.rs`). It implements only the `PhysicalExtensionCodec` adapter-factory hooks that the DataFusion fork gains, encoding the stateless `IcebergPhysicalExprAdapterFactory` as a versioned tag. Separately, the private `fake_object_store_url` becomes the public `object_store_url_for_location`, so executors can register a table's store under the URL that serialized scans name.

**Tech Stack:** Rust, DataFusion 55 (andts fork), `datafusion-proto`, iceberg-rust, tokio tests.

**Spec:** `docs/superpowers/specs/2026-10-04-physical-plan-codec-design.md`. It depends on `~/Projects/datafusion-andts/docs/superpowers/specs/2026-10-04-expr-adapter-factory-serialization-design.md`.

## Global Constraints

- **Run cargo under a cap, always.** Uncapped cargo has OOM'd this host and once caused a kernel panic. Every `$CARGO` below means:
  `systemd-run --user --scope -p MemoryMax=12G -p MemorySwapMax=0 -p CPUQuota=400% env CARGO_BUILD_JOBS=4 CARGO_TARGET_DIR=target/claude CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 RUST_TEST_THREADS=2 nice -n 10 cargo`
  Run one test binary at a time.
- The new feature is named `proto`: `proto = ["dep:datafusion-proto"]`. `datafusion-proto` is an optional dependency taken from the workspace.
- `IcebergPhysicalExprAdapterFactory` stays out of the public API. Its module becomes `pub(crate) mod expr_adapter;`.
- The payload tag is exactly `b"datafusion_iceberg/field-id-adapter/v1"`.
- Unknown adapter factories on encode → `not_impl_err!`, so `ComposedPhysicalExtensionCodec` can try the next codec. Unknown payload on decode → `internal_err!`.
- `try_encode` / `try_decode` of plan nodes → `not_impl_err!` naming the node (`node.name()`).
- `object_store_url_for_location` keeps the escaping of `fake_object_store_url` byte for byte. It is re-exported from the crate root next to `DataFusionTable`.
- The tests that need the feature are `#[cfg(feature = "proto")]`, and `make test-datafusion_iceberg` runs with `--features proto`.
- Encode plans **before** executing them in tests. An executed plan carries dynamic-filter state.
- Done means: `cargo clippy --all-targets --all-features -- -D warnings`, `cargo fmt --all -- --check`, and `make test-datafusion_iceberg` are clean.

## Review Focus

1. **The executor registers a store that can't resolve the files.** The fixture's store is a `LocalFileSystem` prefixed with the scratch dir. Registering anything else under `object_store_url_for_location` must fail loudly, not read the wrong files. Task 3's round trip registers the prefixed store and asserts exact values.
2. **`PARQUET:field_id` metadata lost on the wire.** The adapter would then decode but match nothing, and sanitized columns would read as NULL again. Task 3 asserts non-NULL values per column on the decoded plan.
3. **Engines register the codec inside a `ComposedPhysicalExtensionCodec`, not on its own.** Routing must still reach the Iceberg codec on both encode and decode. Task 3 round-trips through `ComposedPhysicalExtensionCodec::new(vec![DefaultPhysicalExtensionCodec, IcebergPhysicalExtensionCodec])`.
4. **Plans containing nodes nothing can serialize** (`IcebergDvExec`, `IcebergDataSink`, `PhysicalForkNode`) must fail with an error naming the node, not a generic message. A Task 2 unit test checks the node name in the `try_encode` error.
5. **A payload from a different version** (`…/v2`, trailing bytes, empty) must be rejected rather than decoded as v1. A Task 2 unit test covers all three.

---

### Task 1: Public `object_store_url_for_location`

This task has no dependency on the DataFusion change. Do it first, on the current `#Fork` revision.

**Files:**
- Modify: `datafusion_iceberg/src/table/mod.rs` (the function at `:481-496`, call sites at `:525`, `:1675`, `:1973`, `:2086`, and the test at `:3558-3580`)
- Modify: `datafusion_iceberg/src/lib.rs:11`
- Test: `datafusion_iceberg/tests/sanitized_parquet_names.rs`

**Interfaces:**
- Produces: `pub fn datafusion_iceberg::object_store_url_for_location(table_location_url: &str) -> ObjectStoreUrl`, also reachable as `datafusion_iceberg::table::object_store_url_for_location`.
- Produces (test helpers in `tests/sanitized_parquet_names.rs`, used by Task 3): `async fn physical_plan(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan>` and `fn scan_store_urls(plan: &Arc<dyn ExecutionPlan>) -> Vec<String>`.

- [ ] **Step 1: Write the failing test**

In `datafusion_iceberg/tests/sanitized_parquet_names.rs`, replace the line `use datafusion::common::tree_node::{TransformedResult, TreeNode};` with:

```rust
use datafusion::common::tree_node::{TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_iceberg::object_store_url_for_location;
```

Add these helpers after `async fn run(...)`:

```rust
/// Plan `sql` the way `run` does, without executing it.
async fn physical_plan(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
    let plan = ctx
        .state()
        .create_logical_plan(sql)
        .await
        .unwrap_or_else(|e| panic!("plan `{sql}`: {e}"));
    let plan = plan
        .transform(iceberg_transform)
        .data()
        .unwrap_or_else(|e| panic!("iceberg_transform `{sql}`: {e}"));
    ctx.state()
        .create_physical_plan(&plan)
        .await
        .unwrap_or_else(|e| panic!("physical plan `{sql}`: {e}"))
}

/// The object store URL of every file scan in `plan`.
fn scan_store_urls(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let mut urls = Vec::new();
    plan.apply(|node| {
        if let Some(scan) = node
            .downcast_ref::<DataSourceExec>()
            .and_then(|exec| exec.data_source().downcast_ref::<FileScanConfig>())
        {
            urls.push(scan.object_store_url.as_str().to_owned());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    urls
}
```

Add this test at the end of the file:

```rust
/// Executors that receive a serialized plan register the table's store under
/// `object_store_url_for_location(location)`; that only works if planned scans
/// reference exactly that URL.
#[tokio::test]
async fn scans_reference_object_store_url_for_location() {
    let dir = scratch("store-url");
    let ctx = boot(&dir).await;
    run(&ctx, "CREATE SCHEMA warehouse.ws").await;
    run(
        &ctx,
        r#"CREATE EXTERNAL TABLE warehouse.ws.t (id BIGINT NOT NULL)
           STORED AS ICEBERG LOCATION '/warehouse/ws/t'"#,
    )
    .await;
    run(&ctx, "INSERT INTO warehouse.ws.t VALUES (1), (2)").await;

    let plan = physical_plan(&ctx, "SELECT id FROM warehouse.ws.t").await;
    let urls = scan_store_urls(&plan);
    let _ = std::fs::remove_dir_all(&dir);

    assert!(!urls.is_empty(), "plan has no file scan");
    let expected = object_store_url_for_location("/warehouse/ws/t");
    assert!(
        urls.iter().all(|url| url == expected.as_str()),
        "every scan must reference {expected}, got {urls:?}"
    );
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `$CARGO test -p datafusion_iceberg --test sanitized_parquet_names scans_reference_object_store_url_for_location`
Expected: compile error `unresolved import datafusion_iceberg::object_store_url_for_location`.

- [ ] **Step 3: Rename the function and make it public**

```bash
sed -i 's/fake_object_store_url/object_store_url_for_location/g' datafusion_iceberg/src/table/mod.rs
```

This updates the definition, the four call sites, the test-module import and the unit test name (`test_object_store_url_for_location`). Then replace the comment and signature above the function body (`table/mod.rs:481-484`):

```rust
// Create a fake object store URL. Different table paths should produce fake URLs
// that differ in the host name, because DF's DefaultObjectStoreRegistry only takes
// hostname into account for object store keys
fn object_store_url_for_location(table_location_url: &str) -> ObjectStoreUrl {
```

with:

```rust
/// The object store URL `table_scan` registers a table's store under, and that
/// scans of that table reference. Executors that receive serialized plans must
/// register the table's `object_store()` under this URL before executing.
///
/// Different table locations map to different hosts, because DataFusion's
/// `DefaultObjectStoreRegistry` keys stores by scheme and host only.
///
/// # Panics
///
/// If the escaped location is not a valid URL authority (a location containing
/// `?` or `#`).
pub fn object_store_url_for_location(table_location_url: &str) -> ObjectStoreUrl {
```

Leave the body unchanged.

In `datafusion_iceberg/src/lib.rs`, replace `pub use crate::table::DataFusionTable;` with:

```rust
pub use crate::table::{object_store_url_for_location, DataFusionTable};
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `$CARGO test -p datafusion_iceberg --test sanitized_parquet_names`
Expected: both tests PASS.

Run: `$CARGO test -p datafusion_iceberg --lib -- test_object_store_url_for_location`
Expected: PASS.

Run: `grep -rn fake_object_store_url datafusion_iceberg/`
Expected: no output.

- [ ] **Step 5: Lint and commit**

Run: `$CARGO clippy -p datafusion_iceberg --all-targets --all-features -- -D warnings && cargo fmt --all -- --check`
Expected: clean.

```bash
git add datafusion_iceberg/src/table/mod.rs datafusion_iceberg/src/lib.rs datafusion_iceberg/tests/sanitized_parquet_names.rs
git commit -m "feat(datafusion): expose object_store_url_for_location for plan executors"
```

---

### Task 2: `proto` feature and `IcebergPhysicalExtensionCodec`

**Precondition, so check it first:** the DataFusion change is implemented in `~/Projects/datafusion-andts`. Run:

```bash
grep -n "fn try_encode_expr_adapter_factory" ~/Projects/datafusion-andts/datafusion/proto/src/physical_plan/mod.rs
grep -n "pub fn is<T: PhysicalExprAdapterFactory>" ~/Projects/datafusion-andts/datafusion/physical-expr-adapter/src/schema_rewriter.rs
```

Both must print a match. If either prints nothing, **stop**: this task can't compile yet.

**Files:**
- Modify: `Cargo.toml` (workspace, `#Fork` / `#Local Changes` sections, lines 36-52)
- Modify: `datafusion_iceberg/Cargo.toml`
- Modify: `Makefile:10`
- Modify: `datafusion_iceberg/src/table/mod.rs:6`
- Modify: `datafusion_iceberg/src/lib.rs`
- Create: `datafusion_iceberg/src/codec.rs`

**Interfaces:**
- Consumes (from the DataFusion fork): `PhysicalExtensionCodec::{try_encode_expr_adapter_factory(&self, &Arc<dyn PhysicalExprAdapterFactory>, &mut Vec<u8>) -> Result<()>, try_decode_expr_adapter_factory(&self, &[u8]) -> Result<Arc<dyn PhysicalExprAdapterFactory>>}`, and `impl dyn PhysicalExprAdapterFactory { fn is<T>() }`.
- Produces: `pub struct datafusion_iceberg::IcebergPhysicalExtensionCodec;` (also at `datafusion_iceberg::codec::IcebergPhysicalExtensionCodec`). It derives `Debug, Default, Clone, Copy` and implements `PhysicalExtensionCodec`.

- [ ] **Step 1: Point the workspace at the local DataFusion checkout and add the feature**

In the workspace `Cargo.toml`, comment out every line of the `#Fork` section (lines 37-43), and add a commented-out `datafusion-proto` line to that section:

```toml
#datafusion-proto = { version = "55", git = "https://github.com/andts/datafusion.git", rev = "fb722c227a3e5c22af3e140a418977c3df9c853d" }
```

Replace the `#Local Changes` section (lines 45-52) with these uncommented lines. The paths are corrected: the checkout is at `~/Projects/datafusion-andts`, two levels up from the workspace root.

```toml
#Local Changes (for local development against the datafusion-andts checkout)
datafusion = { version = "55", features = ["backtrace", ], path = "../../datafusion-andts/datafusion/core" }
datafusion-common = { version = "55", path = "../../datafusion-andts/datafusion/common"  }
datafusion-execution = { version = "55", path = "../../datafusion-andts/datafusion/execution" }
datafusion-expr = { version = "55", path = "../../datafusion-andts/datafusion/expr" }
datafusion-functions = { version = "55", features = ["crypto_expressions"], path = "../../datafusion-andts/datafusion/functions" }
datafusion-functions-aggregate = { version = "55", path = "../../datafusion-andts/datafusion/functions-aggregate" }
datafusion-proto = { version = "55", path = "../../datafusion-andts/datafusion/proto" }
datafusion-sql = { version = "55", path = "../../datafusion-andts/datafusion/sql" }
```

In `datafusion_iceberg/Cargo.toml`, add `datafusion-proto = { workspace = true, optional = true }` to `[dependencies]` directly after `datafusion-expr = { workspace = true }`. Add a features table after `repository = ...`:

```toml
[features]
# `IcebergPhysicalExtensionCodec`, for shipping physical plans with datafusion-proto.
proto = ["dep:datafusion-proto"]
```

In `Makefile`, change the `test-datafusion_iceberg` recipe to:

```make
	cargo test -p datafusion_iceberg --tests --features proto -j 2
```

In `datafusion_iceberg/src/table/mod.rs:6`, change `mod expr_adapter;` to `pub(crate) mod expr_adapter;`.

Run: `$CARGO build -p datafusion_iceberg && $CARGO build -p datafusion_iceberg --features proto`
Expected: both succeed. The fork's `Any` supertrait on `PhysicalExprAdapterFactory` needs no change here, because `IcebergPhysicalExprAdapterFactory` is `'static`.

- [ ] **Step 2: Write the codec with the plan-node methods only, plus all unit tests**

Create `datafusion_iceberg/src/codec.rs`:

```rust
//! `PhysicalExtensionCodec` for plans produced by this crate.

use std::sync::Arc;

use datafusion::common::{not_impl_err, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::{PhysicalExtensionCodec, PhysicalProtoConverterExtension};

/// Serializes the parts of `datafusion_iceberg` physical plans that
/// `datafusion-proto` cannot: currently the field-id expression adapter attached
/// to every Iceberg file scan. Register it with whatever serializes plans
/// (directly, or inside a `ComposedPhysicalExtensionCodec`).
#[derive(Debug, Default, Clone, Copy)]
pub struct IcebergPhysicalExtensionCodec;

impl PhysicalExtensionCodec for IcebergPhysicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("IcebergPhysicalExtensionCodec does not decode plan nodes")
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        // Name the node: this is the error a user sees when a plan contains
        // e.g. `IcebergDvExec`, which nothing can serialize yet.
        not_impl_err!(
            "IcebergPhysicalExtensionCodec does not encode plan node {}",
            node.name()
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::Schema;
    use datafusion::common::DataFusionError;
    use datafusion::physical_expr_adapter::{
        DefaultPhysicalExprAdapterFactory, PhysicalExprAdapterFactory,
    };
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion_proto::physical_plan::{DefaultPhysicalProtoConverter, PhysicalExtensionCodec};

    use super::IcebergPhysicalExtensionCodec;
    use crate::table::expr_adapter::IcebergPhysicalExprAdapterFactory;

    #[test]
    fn field_id_adapter_round_trips() {
        let codec = IcebergPhysicalExtensionCodec;
        let factory: Arc<dyn PhysicalExprAdapterFactory> =
            Arc::new(IcebergPhysicalExprAdapterFactory);
        let mut buf = Vec::new();
        codec
            .try_encode_expr_adapter_factory(&factory, &mut buf)
            .unwrap();
        let decoded = codec.try_decode_expr_adapter_factory(&buf).unwrap();
        assert!(decoded.is::<IcebergPhysicalExprAdapterFactory>());
    }

    /// Declining with `NotImplemented` is what lets
    /// `ComposedPhysicalExtensionCodec` try the next codec.
    #[test]
    fn other_adapters_are_declined_as_not_implemented() {
        let factory: Arc<dyn PhysicalExprAdapterFactory> =
            Arc::new(DefaultPhysicalExprAdapterFactory);
        let mut buf = Vec::new();
        let err = IcebergPhysicalExtensionCodec
            .try_encode_expr_adapter_factory(&factory, &mut buf)
            .unwrap_err();
        assert!(matches!(err, DataFusionError::NotImplemented(_)), "{err}");
        assert!(buf.is_empty(), "a declined encode must not write");
    }

    #[test]
    fn unknown_payloads_are_rejected() {
        for payload in [
            &b""[..],
            b"datafusion_iceberg/field-id-adapter/v2",
            b"datafusion_iceberg/field-id-adapter/v1\0",
        ] {
            assert!(
                IcebergPhysicalExtensionCodec
                    .try_decode_expr_adapter_factory(payload)
                    .is_err(),
                "payload {payload:?} must not decode"
            );
        }
    }

    #[test]
    fn plan_nodes_are_declined_by_name() {
        let node: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let err = IcebergPhysicalExtensionCodec
            .try_encode(node, &mut Vec::new(), &DefaultPhysicalProtoConverter {})
            .unwrap_err();
        assert!(matches!(err, DataFusionError::NotImplemented(_)), "{err}");
        assert!(err.to_string().contains("EmptyExec"), "{err}");
    }
}
```

In `datafusion_iceberg/src/lib.rs`, add after `pub mod catalog;`:

```rust
#[cfg(feature = "proto")]
pub mod codec;
```

and after the `pub use crate::table::{...};` line:

```rust
#[cfg(feature = "proto")]
pub use crate::codec::IcebergPhysicalExtensionCodec;
```

- [ ] **Step 3: Run the tests to verify the adapter round trip fails**

Run: `$CARGO test -p datafusion_iceberg --features proto --lib -- codec::tests`
Expected: `field_id_adapter_round_trips` FAILS with a `NotImplemented` error from the trait's default `try_encode_expr_adapter_factory`. The other three pass, because the defaults already decline everything.

- [ ] **Step 4: Implement the adapter hooks**

In `codec.rs`, change the imports to:

```rust
use std::sync::Arc;

use datafusion::common::{internal_err, not_impl_err, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::{PhysicalExtensionCodec, PhysicalProtoConverterExtension};

use crate::table::expr_adapter::IcebergPhysicalExprAdapterFactory;

/// Payload for `IcebergPhysicalExprAdapterFactory`. The factory is stateless, so
/// the versioned tag is the whole encoding; a future stateful version gets a new
/// tag and carries its state after it.
const FIELD_ID_ADAPTER_V1: &[u8] = b"datafusion_iceberg/field-id-adapter/v1";
```

Add these two methods to the `impl PhysicalExtensionCodec for IcebergPhysicalExtensionCodec` block, after `try_encode`:

```rust
    fn try_encode_expr_adapter_factory(
        &self,
        factory: &Arc<dyn PhysicalExprAdapterFactory>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        if factory.is::<IcebergPhysicalExprAdapterFactory>() {
            buf.extend_from_slice(FIELD_ID_ADAPTER_V1);
            Ok(())
        } else {
            not_impl_err!("IcebergPhysicalExtensionCodec does not encode {factory:?}")
        }
    }

    // Unknown payloads are an internal error, not "not implemented":
    // `ComposedPhysicalExtensionCodec` routes a payload back only to the codec
    // that produced it.
    fn try_decode_expr_adapter_factory(
        &self,
        buf: &[u8],
    ) -> Result<Arc<dyn PhysicalExprAdapterFactory>> {
        match buf {
            FIELD_ID_ADAPTER_V1 => Ok(Arc::new(IcebergPhysicalExprAdapterFactory)),
            _ => internal_err!(
                "unknown datafusion_iceberg adapter payload ({} bytes)",
                buf.len()
            ),
        }
    }
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `$CARGO test -p datafusion_iceberg --features proto --lib -- codec::tests`
Expected: 4 passed.

Run: `$CARGO test -p datafusion_iceberg --test sanitized_parquet_names`
Expected: PASS. This checks that the forked DataFusion still reads the fixture.

- [ ] **Step 6: Lint and commit**

Run: `$CARGO clippy -p datafusion_iceberg --all-targets --all-features -- -D warnings && $CARGO clippy -p datafusion_iceberg --all-targets -- -D warnings && cargo fmt --all -- --check`
Expected: clean both with and without the feature.

```bash
git add Cargo.toml Cargo.lock Makefile datafusion_iceberg/Cargo.toml datafusion_iceberg/src/codec.rs datafusion_iceberg/src/lib.rs datafusion_iceberg/src/table/mod.rs
git commit -m "feat(datafusion): IcebergPhysicalExtensionCodec serializes the field-id adapter"
```

This commit builds against the local DataFusion checkout through path dependencies. Task 4 pins the pushed revision before anything is pushed.

---

### Task 3: Serialized-plan round trip over sanitized Parquet names

**Files:**
- Modify: `datafusion_iceberg/tests/sanitized_parquet_names.rs`

**Interfaces:**
- Consumes: `physical_plan`, `scan_store_urls` (Task 1), `object_store_url_for_location` (Task 1), and `IcebergPhysicalExtensionCodec` (Task 2).
- Produces: test helpers `async fn sanitized_table(tag: &str) -> std::path::PathBuf` and `fn rows(batches: &[RecordBatch]) -> Vec<(i64, Option<i64>, Option<i64>)>`.

- [ ] **Step 1: Extract the fixture and the row extraction into helpers**

In `tests/sanitized_parquet_names.rs`, add these two helpers directly above `#[tokio::test] async fn sanitized_parquet_names_are_resolved_by_field_id`. Their bodies are lifted from that test:

```rust
/// Create `warehouse.ws.t` (`id`, `my col`, `filler_column_with_a_very_long_name`)
/// in a fresh scratch dir with rows (1, 10, 100) and (2, 20, 200). Then rewrite its
/// data files the way iceberg-java names columns, adding 1000 to every value.
/// Returns the scratch dir; open the table again with `boot`.
async fn sanitized_table(tag: &str) -> std::path::PathBuf {
    let dir = scratch(tag);
    let ctx = boot(&dir).await;
    run(&ctx, "CREATE SCHEMA warehouse.ws").await;
    run(
        &ctx,
        r#"CREATE EXTERNAL TABLE warehouse.ws.t
           (id BIGINT NOT NULL, "my col" BIGINT, filler_column_with_a_very_long_name BIGINT)
           STORED AS ICEBERG LOCATION '/warehouse/ws/t'"#,
    )
    .await;
    run(
        &ctx,
        "INSERT INTO warehouse.ws.t VALUES (1, 10, 100), (2, 20, 200)",
    )
    .await;

    let files = data_files(&dir);
    assert!(!files.is_empty(), "no data files under {dir:?}");
    for f in &files {
        rewrite_parquet(
            f,
            &[
                ("my col", "my_x20col"),
                // Shortened only so the rewritten file can be padded back to the
                // byte size the Iceberg manifest recorded for it.
                ("filler_column_with_a_very_long_name", "f"),
            ],
        );
    }
    dir
}

/// `(id, my col, filler_column_with_a_very_long_name)` for every row, each
/// value read from its own column by name.
fn rows(batches: &[RecordBatch]) -> Vec<(i64, Option<i64>, Option<i64>)> {
    let schema = batches[0].schema();
    let id_idx = schema.index_of("id").expect("id column");
    let my_col_idx = schema.index_of("my col").expect("`my col` column");
    let filler_idx = schema
        .index_of("filler_column_with_a_very_long_name")
        .expect("filler column");

    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(id_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        let my_cols = batch
            .column(my_col_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("`my col` is Int64");
        let fillers = batch
            .column(filler_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("filler column is Int64");
        for i in 0..batch.num_rows() {
            rows.push((
                ids.value(i),
                (!my_cols.is_null(i)).then(|| my_cols.value(i)),
                (!fillers.is_null(i)).then(|| fillers.value(i)),
            ));
        }
    }
    rows
}
```

Then rewrite the body of `sanitized_parquet_names_are_resolved_by_field_id` to use them. Keep the long explanatory comment block unchanged; it now sits above `let rows = rows(&out_batches);`:

```rust
#[tokio::test]
async fn sanitized_parquet_names_are_resolved_by_field_id() {
    let dir = sanitized_table("sanitized").await;

    // Read from a *fresh* session, as an executor pod always is.
    let cold = boot(&dir).await;
    let out_batches = run(&cold, "SELECT * FROM warehouse.ws.t ORDER BY id").await;
    let out = pretty_format_batches(&out_batches).unwrap().to_string();
    println!("{out}");
    let _ = std::fs::remove_dir_all(&dir);

    // The user-visible name must still be the Iceberg one, not the file's.
    assert!(
        out.contains("my col") && !out.contains("my_x20col"),
        "output schema should keep the Iceberg column name:\n{out}"
    );

    // <the existing comment block, "A substring search over the pretty-printed
    // table ..." through "... still reading back as NULL.", unchanged>
    let rows = rows(&out_batches);

    assert_eq!(
        rows,
        vec![
            (1001, Some(1010), Some(1100)),
            (1002, Some(1020), Some(1200)),
        ],
        "each row's `id`, `my col` and `filler_column_with_a_very_long_name` \
         values must land in their own column, unchanged and not cross-wired \
         with one another, proving the rewritten file -- not a cached copy --\
         was read:\n{out}"
    );
}
```

(The `<…>` line above marks where the existing comment text stays. Do not paste it literally.)

Run: `$CARGO test -p datafusion_iceberg --test sanitized_parquet_names`
Expected: both existing tests PASS. This step is a pure refactor.

- [ ] **Step 2: Write the round trip test**

Append to `tests/sanitized_parquet_names.rs`:

```rust
/// A plan serialized where it was planned must read sanitized columns by field
/// id where it is executed: the field-id adapter has to survive datafusion-proto.
#[cfg(feature = "proto")]
#[tokio::test]
async fn serialized_plan_reads_sanitized_columns_by_field_id() {
    use datafusion::physical_plan::collect;
    use datafusion_iceberg::IcebergPhysicalExtensionCodec;
    use datafusion_proto::bytes::{
        physical_plan_from_bytes_with_extension_codec,
        physical_plan_to_bytes_with_extension_codec,
    };
    use datafusion_proto::physical_plan::{
        ComposedPhysicalExtensionCodec, DefaultPhysicalExtensionCodec, PhysicalExtensionCodec,
    };
    use iceberg_rust::object_store::Bucket;

    let dir = sanitized_table("roundtrip").await;
    let planner = boot(&dir).await;
    let plan = physical_plan(
        &planner,
        r#"SELECT id, "my col", filler_column_with_a_very_long_name
           FROM warehouse.ws.t ORDER BY id"#,
    )
    .await;
    let location = object_store_url_for_location("/warehouse/ws/t");
    assert!(
        scan_store_urls(&plan).iter().all(|url| url == location.as_str()),
        "scans must reference {location}"
    );

    // Engines register codecs composed, so route through a composition.
    let codec = ComposedPhysicalExtensionCodec::new(vec![
        Arc::new(DefaultPhysicalExtensionCodec {}) as Arc<dyn PhysicalExtensionCodec>,
        Arc::new(IcebergPhysicalExtensionCodec),
    ]);

    // Encode before executing: an executed plan carries runtime dynamic-filter
    // state that would be shipped along with it.
    let bytes = physical_plan_to_bytes_with_extension_codec(plan.clone(), &codec)
        .expect("encode with the Iceberg codec");

    // Without the Iceberg codec, serialization must refuse, not drop the adapter.
    let err = physical_plan_to_bytes_with_extension_codec(
        plan.clone(),
        &DefaultPhysicalExtensionCodec {},
    )
    .expect_err("the default codec must not serialize the field-id adapter");
    assert!(
        err.to_string().contains("IcebergPhysicalExprAdapterFactory"),
        "{err}"
    );

    // An executor: no catalog, only the table's store under the scan's URL. The
    // store is the fixture's prefixed LocalFileSystem; table paths are
    // `/warehouse/...` relative to the scratch dir.
    let executor = SessionContext::new();
    executor.runtime_env().register_object_store(
        location.as_ref(),
        ObjectStoreBuilder::filesystem(&dir)
            .build(Bucket::Local)
            .expect("filesystem store"),
    );
    let decoded =
        physical_plan_from_bytes_with_extension_codec(&bytes, &executor.task_ctx(), &codec)
            .expect("decode on the executor");

    let expected = vec![
        (1001, Some(1010), Some(1100)),
        (1002, Some(1020), Some(1200)),
    ];
    let local = collect(plan, planner.task_ctx()).await.expect("execute locally");
    let remote = collect(decoded, executor.task_ctx())
        .await
        .expect("execute the decoded plan");
    let _ = std::fs::remove_dir_all(&dir);

    assert_eq!(rows(&local), expected, "planning process");
    assert_eq!(rows(&remote), expected, "executor, from the decoded plan");
}
```

- [ ] **Step 3: Run the test**

Run: `$CARGO test -p datafusion_iceberg --features proto --test sanitized_parquet_names serialized_plan_reads_sanitized_columns_by_field_id`
Expected: PASS.

To see it guard the behaviour, temporarily drop `Arc::new(IcebergPhysicalExtensionCodec),` from the composed codec and rerun. Expected: FAIL at `expect("encode with the Iceberg codec")`, with an error naming `IcebergPhysicalExprAdapterFactory`. Restore the line afterwards.

Run: `$CARGO test -p datafusion_iceberg --test sanitized_parquet_names`
Expected: PASS, with the round trip test compiled out because the feature is off.

- [ ] **Step 4: Lint and commit**

Run: `$CARGO clippy -p datafusion_iceberg --all-targets --all-features -- -D warnings && cargo fmt --all -- --check`
Expected: clean.

```bash
git add datafusion_iceberg/tests/sanitized_parquet_names.rs
git commit -m "test(datafusion): serialized plans read sanitized parquet columns by field id"
```

---

### Task 4: Docs, pin the DataFusion revision, full verification

**Precondition:** the DataFusion change is committed **and pushed** to `andts/datafusion`. Check:

```bash
REV=$(git -C ~/Projects/datafusion-andts rev-parse HEAD)
git -C ~/Projects/datafusion-andts fetch origin && git -C ~/Projects/datafusion-andts branch -r --contains "$REV"
```

The second command must list at least one `origin/...` branch. Otherwise **stop** and ask for the revision to use.

**Files:**
- Modify: `datafusion_iceberg/README.md`
- Modify: `Cargo.toml` (workspace)
- Modify: `docs/superpowers/specs/2026-10-04-physical-plan-codec-design.md` (status and checklist)

- [ ] **Step 1: README paragraph**

Append to `datafusion_iceberg/README.md`:

````markdown
## Shipping physical plans to other processes

Physical plans that scan Iceberg tables can be serialized with `datafusion-proto`
and executed elsewhere (Ballista, `datafusion-distributed`, a custom worker pool).
Enable the `proto` feature and register `IcebergPhysicalExtensionCodec` with your
plan serializer, directly or inside a `ComposedPhysicalExtensionCodec`. It carries
the field-id column mapping that scans need to read files written by iceberg-java
engines. Without it, serialization fails instead of silently reading those columns
as NULL. On the executing side, register each table's object store under
`object_store_url_for_location(table_location)` before executing:

```rust
use datafusion_iceberg::{object_store_url_for_location, IcebergPhysicalExtensionCodec};
use datafusion_proto::bytes::physical_plan_to_bytes_with_extension_codec;

let bytes = physical_plan_to_bytes_with_extension_codec(plan, &IcebergPhysicalExtensionCodec)?;
// on the executor:
ctx.runtime_env().register_object_store(
    object_store_url_for_location(table.metadata().location.as_str()).as_ref(),
    table.object_store(),
);
```

Plans that contain deletion-vector scans, `INSERT`s, or materialized-view refreshes
can't be serialized yet. Serialization fails, naming the node.
````

- [ ] **Step 2: Switch back to the fork at the pushed revision**

In the workspace `Cargo.toml`:
1. Comment out every line of the `#Local Changes` section again (keeping the corrected `../../datafusion-andts` paths and the `datafusion-proto` line).
2. Uncomment the `#Fork` section, including the `datafusion-proto` line.
3. Replace the revision on all Fork lines:

```bash
sed -i "s/fb722c227a3e5c22af3e140a418977c3df9c853d/$REV/g" Cargo.toml
grep -c "rev = \"$REV\"" Cargo.toml
```

Expected: `8` (seven existing crates plus `datafusion-proto`).

- [ ] **Step 3: Full verification**

Run these one at a time:

```bash
$CARGO build
$CARGO clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
$CARGO test -p datafusion_iceberg --tests --features proto -j 2
```

Expected: all clean, with the test run showing `serialized_plan_reads_sanitized_columns_by_field_id ... ok` and the 4 `codec::tests` passing. The container suites (Spark/Trino) need podman. Apply the SELinux/socket setup from memory and use `RUST_TEST_THREADS=1`, or report them as not run.

- [ ] **Step 4: Mark the spec implemented and commit**

In the spec, change `**Status:** Proposed (handoff — not implemented)` to `**Status:** Implemented`, and tick every box in its "Implementation checklist".

```bash
git add Cargo.toml Cargo.lock datafusion_iceberg/README.md docs/superpowers/specs/2026-10-04-physical-plan-codec-design.md
git commit -m "build: pin datafusion fork with adapter codec hooks; document plan shipping"
```
