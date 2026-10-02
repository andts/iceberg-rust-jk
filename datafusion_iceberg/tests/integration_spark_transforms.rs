//! Spark and DataFusion agree on partition transforms (requires Docker).

#[path = "spark_common/mod.rs"]
mod spark_common;

use std::sync::Arc;

use datafusion::{
    arrow::{array::AsArray, datatypes::Int64Type},
    execution::context::SessionContext,
};
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use iceberg_rust::spec::{
    partition::{PartitionField, PartitionSpec, Transform},
    schema::Schema,
    types::{PrimitiveType, StructField, Type},
};
use iceberg_rust::table::Table;
use spark_common::{boot_spark_stack, spark_sql, spark_sql_ok};

async fn count(ctx: &SessionContext, sql: &str) -> i64 {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    batches[0].column(0).as_primitive::<Int64Type>().value(0)
}

#[tokio::test]
async fn datafusion_reads_spark_partitioned_table() {
    let stack = boot_spark_stack().await;
    spark_sql_ok(&stack, "CREATE NAMESPACE IF NOT EXISTS demo.xeng").await;

    // Spark writes; DataFusion reads with filters on every partition source.
    spark_sql_ok(
        &stack,
        "SET spark.sql.session.timeZone=UTC; \
         CREATE TABLE demo.xeng.spark_t (id BIGINT, d DATE, ts TIMESTAMP_NTZ, s STRING) USING iceberg \
         PARTITIONED BY (months(ts), bucket(10, d), truncate(2, s)); \
         INSERT INTO demo.xeng.spark_t VALUES \
           (1, DATE '2017-11-16', TIMESTAMP_NTZ '2023-05-15 12:00:00', 'abcdef'), \
           (2, DATE '1969-12-31', TIMESTAMP_NTZ '1969-12-31 23:59:59', 'zz');",
    )
    .await;

    let ctx = SessionContext::new();
    ctx.register_catalog(
        "iceberg",
        Arc::new(
            IcebergCatalog::new(stack.catalog.clone(), None)
                .await
                .unwrap(),
        ),
    );
    for (filter, expected) in [
        (
            "ts >= TIMESTAMP '2023-05-01 00:00:00' AND ts < TIMESTAMP '2023-06-01 00:00:00'",
            1,
        ),
        ("ts > TIMESTAMP '2023-05-15 10:00:00'", 1),
        ("ts < TIMESTAMP '1970-01-01 00:00:00'", 1),
        ("d = DATE '2017-11-16'", 1),
        ("d = DATE '1969-12-31'", 1),
        ("s = 'abcdef'", 1),
    ] {
        assert_eq!(
            count(
                &ctx,
                &format!("SELECT count(*) FROM iceberg.xeng.spark_t WHERE {filter}")
            )
            .await,
            expected,
            "DataFusion reading Spark's table: {filter}"
        );
    }
}

#[tokio::test]
async fn spark_reads_datafusion_partitioned_table() {
    let stack = boot_spark_stack().await;
    spark_sql_ok(&stack, "CREATE NAMESPACE IF NOT EXISTS demo.xeng").await;
    let ctx = SessionContext::new();

    // DataFusion writes; Spark checks the partition values and prunes with them.
    let field = |id: i32, name: &str, ty: PrimitiveType| StructField {
        id,
        name: name.to_string(),
        required: false,
        field_type: Type::Primitive(ty),
        doc: None,
        initial_default: None,
        write_default: None,
    };
    Table::builder()
        .with_name("rust_t")
        .with_location("s3://warehouse/xeng/rust_t")
        .with_schema(
            Schema::builder()
                .with_struct_field(field(1, "id", PrimitiveType::Long))
                .with_struct_field(field(2, "d", PrimitiveType::Date))
                .with_struct_field(field(3, "ts", PrimitiveType::Timestamp))
                .build()
                .unwrap(),
        )
        .with_partition_spec(
            PartitionSpec::builder()
                .with_partition_field(PartitionField::new(3, 1000, "ts_month", Transform::Month))
                .with_partition_field(PartitionField::new(
                    2,
                    1001,
                    "d_bucket",
                    Transform::Bucket(10),
                ))
                .build()
                .unwrap(),
        )
        .build(&["xeng".to_owned()], stack.catalog.clone())
        .await
        .unwrap();
    ctx.register_catalog(
        "iceberg",
        Arc::new(
            IcebergCatalog::new(stack.catalog.clone(), None)
                .await
                .unwrap(),
        ),
    );
    ctx.sql(
        "INSERT INTO iceberg.xeng.rust_t VALUES (1, DATE '2017-11-16', TIMESTAMP '2023-05-15 12:00:00')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    let files = spark_sql(&stack, "SELECT _partition FROM demo.xeng.rust_t").await;
    assert!(files.is_success(), "{}", files.dump());
    // Spec: month(2023-05) = 640; bucket[10](date 2017-11-16) = 6.
    // Anchored on the delimiters so 6400 or 60 cannot match.
    assert!(
        files.stdout.contains("\"ts_month\":640,") && files.stdout.contains("\"d_bucket\":6}"),
        "unexpected partition values:\n{}",
        files.dump()
    );
    let read = spark_sql(
        &stack,
        "SELECT count(*) FROM demo.xeng.rust_t WHERE d = DATE '2017-11-16' \
         AND ts >= TIMESTAMP_NTZ '2023-05-01 00:00:00' AND ts < TIMESTAMP_NTZ '2023-06-01 00:00:00'",
    )
    .await;
    assert!(read.is_success(), "{}", read.dump());
    assert!(
        read.stdout.lines().any(|line| line.trim() == "1"),
        "{}",
        read.dump()
    );
}

/// Decimal truncate and binary identity partitions in both directions: the
/// decimal is encoded as Avro `fixed` and the binary as `bytes` in manifests.
#[tokio::test]
async fn decimal_and_binary_partitions_interoperate_with_spark() {
    let stack = boot_spark_stack().await;
    spark_sql_ok(&stack, "CREATE NAMESPACE IF NOT EXISTS demo.xeng").await;
    let ctx = SessionContext::new();
    ctx.register_catalog(
        "iceberg",
        Arc::new(
            IcebergCatalog::new(stack.catalog.clone(), None)
                .await
                .unwrap(),
        ),
    );

    // Spark writes; DataFusion reads with a filter on each partition source.
    spark_sql_ok(
        &stack,
        "CREATE TABLE demo.xeng.spark_db (id BIGINT, amount DECIMAL(9,2), b BINARY) USING iceberg \
         PARTITIONED BY (truncate(50, amount), b); \
         INSERT INTO demo.xeng.spark_db VALUES (1, 10.65, X'00010203');",
    )
    .await;
    for filter in ["amount = 10.65", "b = X'00010203'"] {
        assert_eq!(
            count(
                &ctx,
                &format!("SELECT count(*) FROM iceberg.xeng.spark_db WHERE {filter}")
            )
            .await,
            1,
            "DataFusion reading Spark's table: {filter}"
        );
    }

    // DataFusion writes; Spark checks the partition values and filters.
    let field = |id: i32, name: &str, ty: PrimitiveType| StructField {
        id,
        name: name.to_string(),
        required: false,
        field_type: Type::Primitive(ty),
        doc: None,
        initial_default: None,
        write_default: None,
    };
    Table::builder()
        .with_name("rust_db")
        .with_location("s3://warehouse/xeng/rust_db")
        .with_schema(
            Schema::builder()
                .with_struct_field(field(1, "id", PrimitiveType::Long))
                .with_struct_field(field(
                    2,
                    "amount",
                    PrimitiveType::Decimal {
                        precision: 9,
                        scale: 2,
                    },
                ))
                .with_struct_field(field(3, "b", PrimitiveType::Binary))
                .build()
                .unwrap(),
        )
        .with_partition_spec(
            PartitionSpec::builder()
                .with_partition_field(PartitionField::new(
                    2,
                    1000,
                    "amount_trunc",
                    Transform::Truncate(50),
                ))
                .with_partition_field(PartitionField::new(3, 1001, "b", Transform::Identity))
                .build()
                .unwrap(),
        )
        .build(&["xeng".to_owned()], stack.catalog.clone())
        .await
        .unwrap();
    ctx.sql(
        "INSERT INTO iceberg.xeng.rust_db VALUES (1, CAST(10.65 AS DECIMAL(9,2)), X'00010203')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Spark prints a binary as raw bytes, so the partition's binary value is
    // read through hex().
    let files = spark_sql(
        &stack,
        "SELECT _partition.amount_trunc, hex(_partition.b) FROM demo.xeng.rust_db",
    )
    .await;
    assert!(files.is_success(), "{}", files.dump());
    // Spec: truncate[50](10.65) = 10.50; identity keeps the bytes.
    assert!(
        files
            .stdout
            .lines()
            .any(|line| line.trim() == "10.50\t00010203"),
        "unexpected partition values:\n{}",
        files.dump()
    );
    for filter in ["amount = 10.65", "b = X'00010203'"] {
        let read = spark_sql(
            &stack,
            &format!("SELECT count(*) FROM demo.xeng.rust_db WHERE {filter}"),
        )
        .await;
        assert!(read.is_success(), "{}", read.dump());
        assert!(
            read.stdout.lines().any(|line| line.trim() == "1"),
            "Spark reading DataFusion's table: {filter}\n{}",
            read.dump()
        );
    }
}
