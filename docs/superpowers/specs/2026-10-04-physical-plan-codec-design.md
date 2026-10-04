# `datafusion_iceberg`: Physical Plan Codec for Serialized Execution — Design / Handoff

**Date:** 2026-10-04
**Status:** Implemented; builds against the local `datafusion-andts` checkout until the fork revision is pinned
**Crate:** `datafusion_iceberg`
**Base:** this fork at `eeac8ab`
**Depends on:** the DataFusion change "Serialize `PhysicalExprAdapterFactory` in
`datafusion-proto`" (andts/datafusion,
`docs/superpowers/specs/2026-10-04-expr-adapter-factory-serialization-design.md`):
`PhysicalExtensionCodec::{try_encode,try_decode}_expr_adapter_factory` and the `Any`
supertrait on `PhysicalExprAdapterFactory`.
**Upstream intent:** applicable to JanKaul/iceberg-rust unchanged; nothing here is
specific to any downstream project.

## Summary

Physical plans that scan Iceberg tables cannot currently be shipped to another
process with `datafusion-proto` and execute identically there. This change gives
`datafusion_iceberg` what any plan-shipping executor (Ballista,
`datafusion-distributed`, or a custom worker pool) needs:

1. **`IcebergPhysicalExtensionCodec`** (behind a new `proto` feature) — a
   `PhysicalExtensionCodec` that serializes the field-id
   `IcebergPhysicalExprAdapterFactory` the scan attaches to every `FileScanConfig`,
   so the adapter survives the round trip.
2. **A public helper for the scan's object store URL**, so an executor can register
   a table's object store under the URL the serialized scan refers to.

## Motivation

### 1. The field-id adapter is dropped in serialization

`table_scan` attaches `IcebergPhysicalExprAdapterFactory` to every
`FileScanConfig` it builds (`datafusion_iceberg/src/table/mod.rs:924-927`). The
adapter resolves Parquet columns by Iceberg field id, because iceberg-java writers
(Spark, Flink, Trino) sanitize Parquet column names — `my col` is stored as
`my_x20col` (see the module docs in `table/expr_adapter.rs` and the test
`tests/sanitized_parquet_names.rs`).

`datafusion-proto` does not serialize `FileScanConfig::expr_adapter_factory`. After
a round trip the scan uses DataFusion's default, name-based adapter, which
substitutes NULL for any nullable column it cannot find by name. So a column the
planning process reads correctly is **read as all NULLs, without error,** by the
process that executes the deserialized plan.

The DataFusion change adds a codec hook for adapter factories and makes
unencodable custom adapters a serialization *error* rather than a silent drop. With
it, plans from this crate stop serializing at all until a codec handles the adapter —
correct, but unusable. This crate must provide that codec: only it can name
`IcebergPhysicalExprAdapterFactory`, which stays private.

### 2. Executors cannot find the scan's object store

`table_scan` registers each table's object store on the planning session under a
synthetic URL derived from the table location (`fake_object_store_url`,
`table/mod.rs:484`, private), and the serialized `FileScanConfig` refers to that URL.
A process that only receives the plan has no such registration and fails with "no
suitable object store". Executors today must re-derive the escaping rule by hand,
which silently breaks if the rule ever changes.

## Design

### 1. `IcebergPhysicalExtensionCodec`

New file `datafusion_iceberg/src/codec.rs`, compiled with the `proto` feature:

```toml
# Cargo.toml (workspace) — same source/rev as the other datafusion crates
[workspace.dependencies]
datafusion-proto = { version = "55", git = "https://github.com/andts/datafusion.git", rev = "<rev with the adapter codec hooks>" }

# datafusion_iceberg/Cargo.toml
[features]
proto = ["dep:datafusion-proto"]

[dependencies]
datafusion-proto = { workspace = true, optional = true }
```

```rust
//! `PhysicalExtensionCodec` for plans produced by this crate.

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
        not_impl_err!("IcebergPhysicalExtensionCodec does not encode plan node {}", node.name())
    }

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

    fn try_decode_expr_adapter_factory(
        &self,
        buf: &[u8],
    ) -> Result<Arc<dyn PhysicalExprAdapterFactory>> {
        match buf {
            FIELD_ID_ADAPTER_V1 => Ok(Arc::new(IcebergPhysicalExprAdapterFactory)),
            _ => internal_err!("unknown datafusion_iceberg adapter payload ({} bytes)", buf.len()),
        }
    }
}
```

`lib.rs`: `#[cfg(feature = "proto")] pub mod codec;` and
`#[cfg(feature = "proto")] pub use codec::IcebergPhysicalExtensionCodec;`.
In `table/mod.rs`, change `mod expr_adapter;` to `pub(crate) mod expr_adapter;` so
`codec.rs` can name the factory; it stays out of the public API.

Declining unknown factories with `not_impl_err!` lets
`ComposedPhysicalExtensionCodec` try the next codec. Decoding an unknown payload is an
`internal_err!`, because the composed codec only routes a payload back to the codec
that produced it.

### 2. Public object store URL helper

In `table/mod.rs`, replace the private function with a public one and keep the call
sites (`table_scan`, `insert_into`'s `FileSinkConfig`, and the in-module tests):

```rust
/// The object store URL `table_scan` registers a table's store under, and that
/// scans of that table reference. Executors that receive serialized plans must
/// register the table's `object_store()` under this URL before executing.
///
/// # Panics
///
/// If the escaped location is not a valid URL authority (a location containing
/// `?` or `#`). Unchanged from the private function this replaces.
pub fn object_store_url_for_location(table_location_url: &str) -> ObjectStoreUrl {
    ObjectStoreUrl::parse(format!(
        "iceberg-rust://{}",
        table_location_url
            .replace('-', "-2D")
            .replace('/', "-2F")
            .replace(':', "-3A")
            .replace(' ', "-20")
    ))
    .expect("Invalid object store url.")
}
```

Re-export it next to `DataFusionTable` in `lib.rs`. The escaping is unchanged; only
its visibility and name change (`fake_object_store_url` → a name that describes the
contract).

## Alternatives considered

- **Re-attach the adapter on the executor by convention** ("every `iceberg-rust://`
  scan gets the field-id adapter"), e.g. through an engine's worker-side plan rewrite
  hook. Works today because the factory is stateless and attached unconditionally,
  but it is invisible in the plan and silently diverges if the scan ever attaches
  adapters conditionally or gives them state. Rejected in favour of serializing what
  the plan actually contains.
- **Make `IcebergPhysicalExprAdapterFactory` public and let each engine write its own
  codec.** Spreads knowledge of this crate's internals into every consumer; the codec
  belongs next to the types it encodes.
- **Serialize the whole Iceberg scan as an extension node** (table identifier,
  snapshot, file list) and rebuild it on the executor. More robust against future
  `datafusion-proto` gaps, but a much larger change that duplicates what
  `datafusion-proto` already does correctly for `DataSourceExec`. Worth revisiting if
  further gaps appear.

## Out of scope (follow-ups)

- **`IcebergDvExec`** (`table/dv_exec.rs`) is a custom `ExecutionPlan` with no
  serialization. It applies both v3 deletion vectors and v2 position-delete files
  (`table_scan` routes both into `dv_index`), which includes common Spark/Flink
  merge-on-read v2 tables. Serialized since
  `2026-10-04-dv-exec-serialization-design.md`. The same codec is the natural home for it
  (`try_encode` / `try_decode`, payload = path-keyed deletion vectors plus column
  indices); left out to keep this change focused.
- **`IcebergDataSink`** (`INSERT` plans, `table/mod.rs`) and **`PhysicalForkNode`**
  (materialized-view refresh, `materialized_view/delta_queries/fork_node.rs`) are
  likewise custom and unserializable. With the codec registered, all three fail
  closed at serialization with "does not encode plan node <name>".

## Sequencing and build setup

- The DataFusion change is a prerequisite and is implemented separately (in the
  `datafusion-andts` checkout, branch `feat/expr-adapter-factory-serialization`).
- §2 (the URL helper) has no dependency on it and lands first.
- The codec work builds against the local checkout through the `#Local Changes`
  section of the workspace `Cargo.toml`. That section's paths are corrected to
  `../../datafusion-andts/...` (the checkout's real location) and gain a
  `datafusion-proto` line. A final step switches back to the `#Fork` section, pinned
  to the pushed revision.
- `make test-datafusion_iceberg` runs with `--features proto`, so CI runs the codec's
  unit tests and the round trip test. The tests are `#[cfg(feature = "proto")]`, and
  the workspace `cargo build` in CI still checks the build without the feature.

## Compatibility

- New optional feature; no change for users who don't enable `proto`.
- `fake_object_store_url` was private; the new public function has the same
  behaviour.
- Requires the DataFusion codec hooks named under "Depends on"; without them the
  codec does not compile.

## Test plan

1. **Unit (`codec.rs`)**
   - Encoding `IcebergPhysicalExprAdapterFactory` then decoding returns a factory for
     which `is::<IcebergPhysicalExprAdapterFactory>()` holds.
   - Encoding `DefaultPhysicalExprAdapterFactory` returns a not-implemented error
     (so composition works).
   - Decoding an unknown payload is an error.
2. **Round trip (`tests/sanitized_parquet_names.rs`, new test next to the existing
   one)** — reuse its fixture (Parquet written with sanitized names and field ids):
   - plan `SELECT` over the sanitized column;
   - serialize with
     `datafusion_proto::bytes::physical_plan_to_bytes_with_extension_codec(plan, &IcebergPhysicalExtensionCodec)`
     **before executing it**;
   - decode on a fresh `SessionContext` whose runtime env has only the table's
     store registered under `object_store_url_for_location(location)` (the
     fixture's store is a `LocalFileSystem` *prefixed* with the scratch dir —
     `ObjectStoreBuilder::filesystem(dir).build(Bucket::Local)`; an unprefixed one
     would resolve `/warehouse/...` against `/`);
   - execute both; results are equal and the column is non-NULL.
   - Negative control: serializing with `DefaultPhysicalExtensionCodec` fails (with
     the DataFusion change) instead of producing NULLs.
3. `make test-datafusion_iceberg`, `cargo clippy --all-targets --all-features -- -D warnings`.

Why "before executing": an executed plan carries runtime dynamic-filter state
(TopK thresholds, populated hash-join filters). Serializing it afterwards ships that
state and yields wrong rows or decode failures unrelated to this change.

## Implementation checklist

- [x] `table/mod.rs`: `object_store_url_for_location` (public), call sites updated, re-export. A test pins that planned scans reference it.
- [x] Workspace `Cargo.toml`: fix the `#Local Changes` paths, add `datafusion-proto` to both sections; `datafusion_iceberg/Cargo.toml`: `proto` feature, optional dependency; `Makefile`: `--features proto`.
- [x] `table/mod.rs`: `pub(crate) mod expr_adapter;`.
- [x] `src/codec.rs` + `lib.rs` exports.
- [x] Tests 1–2.
- [ ] Switch back to the `#Fork` section at the pushed revision with the adapter codec hooks.
- [x] README/docs: one paragraph on shipping plans — enable `proto`, register
      `IcebergPhysicalExtensionCodec`, register stores via `object_store_url_for_location`.
