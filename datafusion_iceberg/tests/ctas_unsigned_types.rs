//! Writing unsigned Arrow columns into an Iceberg table, the way CTAS does.
//!
//! Iceberg has no unsigned integers, so each unsigned Arrow type must map to
//! the narrowest Iceberg type that holds its whole range: UInt8/UInt16 ->
//! int, UInt32 -> long, UInt64 -> decimal(20, 0). Every column is seeded with
//! its type's maximum value, which overflows the next-narrower signed type,
//! so a lossy mapping or a missing data cast cannot pass.
//!
//! This mirrors `IcebergSchema::register_table` (the CTAS path): derive the
//! Iceberg schema from the query's Arrow schema, create the table, and hand
//! the query's batches, still unsigned, to `write_parquet_partitioned`. The
//! table gets an explicit location because the in-memory SQL catalog, unlike
//! a REST catalog, does not assign one.

use std::sync::Arc;

use datafusion::arrow::array::{UInt16Array, UInt32Array, UInt64Array, UInt8Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::prelude::SessionContext;
use futures::stream;

use datafusion_iceberg::DataFusionTable;
use iceberg_rust::arrow::write::write_parquet_partitioned;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_rust::spec::arrow::schema::new_fields_with_ids;
use iceberg_rust::spec::schema::Schema;
use iceberg_rust::spec::types::StructType;
use iceberg_rust::table::Table;
use iceberg_sql_catalog::SqlCatalog;

#[tokio::test]
async fn unsigned_columns_round_trip_losslessly() {
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("u8", DataType::UInt8, false),
        Field::new("u16", DataType::UInt16, false),
        Field::new("u32", DataType::UInt32, false),
        Field::new("u64", DataType::UInt64, false),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(UInt8Array::from(vec![u8::MAX])),
            Arc::new(UInt16Array::from(vec![u16::MAX])),
            Arc::new(UInt32Array::from(vec![u32::MAX])),
            Arc::new(UInt64Array::from(vec![u64::MAX])),
        ],
    )
    .unwrap();

    let iceberg_fields = StructType::try_from(&new_fields_with_ids(arrow_schema.fields(), &mut 0))
        .expect("map unsigned arrow types to iceberg");

    let catalog: Arc<dyn Catalog> = Arc::new(
        SqlCatalog::new("sqlite://", "warehouse", ObjectStoreBuilder::memory())
            .await
            .unwrap(),
    );
    let mut table = Table::builder()
        .with_name("maxes")
        .with_location("/test/maxes")
        .with_schema(Schema::from_struct_type(iceberg_fields, 0, None))
        .build(&["test".to_owned()], catalog)
        .await
        .unwrap();

    let files = write_parquet_partitioned(&table, stream::iter(vec![Ok(batch)]), None)
        .await
        .expect("write unsigned batch");
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await
        .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("maxes", Arc::new(DataFusionTable::from(table)))
        .unwrap();
    let batches = ctx
        .sql("SELECT u8, u16, u32, u64 FROM maxes")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

    let types: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect();
    assert_eq!(
        types,
        vec![
            DataType::Int32,
            DataType::Int32,
            DataType::Int64,
            DataType::Decimal128(20, 0),
        ]
    );

    let values: Vec<_> = (0..4)
        .map(|i| array_value_to_string(batch.column(i), 0).unwrap())
        .collect();
    assert_eq!(
        values,
        vec!["255", "65535", "4294967295", "18446744073709551615"]
    );
}
