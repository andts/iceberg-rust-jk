# Datafusion iceberg

Provides the functionality to use apache iceberg with datafusion including the `TableProvider`, `SchemaProvider` and `CatalogProvider` traits.
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

Plans over tables with row-level deletes (v2 position-delete files, v3 deletion
vectors) ship too. The deletes are loaded while planning, and each serialized plan
carries only the deletion bitmaps of the data files it scans, so splitting a scan
into many tasks doesn't multiply the payload. `INSERT`s and materialized-view
refreshes can't be serialized yet; serialization fails instead.
