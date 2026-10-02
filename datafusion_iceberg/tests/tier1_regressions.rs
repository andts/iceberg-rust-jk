//! Regression tests for the Tier 1 correctness fixes
//! (docs/superpowers/specs/2026-10-02-tier1-correctness-fixes-design.md).

use std::sync::Arc;

use datafusion::{
    arrow::{array::AsArray, datatypes::Int64Type, error::ArrowError, record_batch::RecordBatch},
    prelude::SessionContext,
};
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use futures::stream;
use iceberg_rust::{
    arrow::write::write_equality_deletes_parquet_partitioned,
    catalog::{identifier::Identifier, namespace::Namespace, tabular::Tabular, Catalog},
    object_store::ObjectStoreBuilder,
    spec::{
        partition::Transform,
        partition::{PartitionField, PartitionSpec},
        schema::Schema,
        types::{PrimitiveType, StructField, Type},
    },
    table::Table,
};
use iceberg_sql_catalog::SqlCatalog;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// A SQLite-catalog warehouse with namespace `test`, registered as `warehouse`.
struct Fixture {
    dir: TempDir,
    catalog: Arc<dyn Catalog>,
    ctx: SessionContext,
}

impl Fixture {
    async fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let object_store = ObjectStoreBuilder::Filesystem(Arc::new(LocalFileSystem::new()));
        let catalog: Arc<dyn Catalog> = Arc::new(
            SqlCatalog::new("sqlite://", "warehouse", object_store)
                .await
                .unwrap(),
        );
        catalog
            .create_namespace(&Namespace::try_new(&["test".to_string()]).unwrap(), None)
            .await
            .unwrap();
        Fixture {
            dir,
            catalog,
            ctx: SessionContext::new(),
        }
    }

    /// Creates `warehouse.test.<name>`: `id long NOT NULL, n long, s string,
    /// d date, ts timestamp, amount decimal(9,2)` (field ids 1..=6).
    async fn create_table(&self, name: &str, partition_fields: Vec<PartitionField>) {
        let field = |id: i32, name: &str, required: bool, ty: PrimitiveType| StructField {
            id,
            name: name.to_string(),
            required,
            field_type: Type::Primitive(ty),
            doc: None,
            initial_default: None,
            write_default: None,
        };
        let schema = Schema::builder()
            .with_struct_field(field(1, "id", true, PrimitiveType::Long))
            .with_struct_field(field(2, "n", false, PrimitiveType::Long))
            .with_struct_field(field(3, "s", false, PrimitiveType::String))
            .with_struct_field(field(4, "d", false, PrimitiveType::Date))
            .with_struct_field(field(5, "ts", false, PrimitiveType::Timestamp))
            .with_struct_field(field(
                6,
                "amount",
                false,
                PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                },
            ))
            .build()
            .unwrap();
        Table::builder()
            .with_name(name)
            .with_location(format!("{}/test/{name}", self.dir.path().to_str().unwrap()))
            .with_schema(schema)
            .with_partition_spec(
                PartitionSpec::builder()
                    .with_fields(partition_fields)
                    .build()
                    .unwrap(),
            )
            .build(&["test".to_owned()], self.catalog.clone())
            .await
            .unwrap();
        // Re-register so DataFusion sees the new table.
        self.ctx.register_catalog(
            "warehouse",
            Arc::new(
                IcebergCatalog::new(self.catalog.clone(), None)
                    .await
                    .unwrap(),
            ),
        );
    }

    async fn sql(&self, sql: &str) -> Vec<RecordBatch> {
        self.ctx
            .sql(sql)
            .await
            .unwrap_or_else(|err| panic!("planning `{sql}`: {err}"))
            .collect()
            .await
            .unwrap_or_else(|err| panic!("executing `{sql}`: {err}"))
    }

    /// The first column (Int64) of every result row; errors as text.
    async fn try_ids(&self, sql: &str) -> Result<Vec<i64>, String> {
        let batches = self
            .ctx
            .sql(sql)
            .await
            .map_err(|err| err.to_string())?
            .collect()
            .await
            .map_err(|err| err.to_string())?;
        Ok(batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<Int64Type>()
                    .iter()
                    .flatten()
                    .collect::<Vec<_>>()
            })
            .collect())
    }

    async fn ids(&self, sql: &str) -> Vec<i64> {
        self.try_ids(sql)
            .await
            .unwrap_or_else(|err| panic!("`{sql}`: {err}"))
    }
}

#[tokio::test]
async fn insert_keeps_rows_with_null_partition_source() {
    let f = Fixture::new().await;
    f.create_table(
        "t",
        vec![PartitionField::new(2, 1000, "n", Transform::Identity)],
    )
    .await;
    f.sql("INSERT INTO warehouse.test.t (id, n) VALUES (1, 5), (2, NULL), (3, 0)")
        .await;
    f.sql("INSERT INTO warehouse.test.t (id, n) VALUES (4, NULL)")
        .await;
    assert_eq!(
        f.ids("SELECT id FROM warehouse.test.t ORDER BY id").await,
        vec![1, 2, 3, 4]
    );
    assert_eq!(
        f.ids("SELECT id FROM warehouse.test.t WHERE n IS NULL ORDER BY id")
            .await,
        vec![2, 4]
    );
    // Appending a non-null partition after null-only partitions (these merge into
    // one manifest; see `append_after_an_all_null_partition_manifest`).
    f.sql("INSERT INTO warehouse.test.t (id, n) VALUES (5, 7)")
        .await;
    assert_eq!(
        f.ids("SELECT id FROM warehouse.test.t ORDER BY id").await,
        vec![1, 2, 3, 4, 5]
    );
}

/// The first INSERT writes a manifest whose partition summary has no bounds
/// (every value is NULL). The second INSERT must append it unchanged or skip it
/// and write a fresh manifest, not fail.
#[tokio::test]
async fn append_after_an_all_null_partition_manifest() {
    let f = Fixture::new().await;
    f.create_table(
        "t",
        vec![PartitionField::new(2, 1000, "n", Transform::Identity)],
    )
    .await;
    f.sql("INSERT INTO warehouse.test.t (id, n) VALUES (1, NULL)")
        .await;
    f.sql("INSERT INTO warehouse.test.t (id, n) VALUES (5, 7)")
        .await;
    assert_eq!(
        f.ids("SELECT id FROM warehouse.test.t ORDER BY id").await,
        vec![1, 5]
    );
    assert_eq!(
        f.ids("SELECT id FROM warehouse.test.t WHERE n IS NULL ORDER BY id")
            .await,
        vec![1]
    );
    assert_eq!(
        f.ids("SELECT id FROM warehouse.test.t WHERE n = 7 ORDER BY id")
            .await,
        vec![5]
    );
}

#[tokio::test]
async fn insert_identity_partition_on_a_long_string() {
    let f = Fixture::new().await;
    f.create_table(
        "t",
        vec![PartitionField::new(3, 1000, "s", Transform::Identity)],
    )
    .await;
    let long = "x".repeat(100);
    f.sql(&format!(
        "INSERT INTO warehouse.test.t (id, s) VALUES (1, '{long}')"
    ))
    .await;
    assert_eq!(
        f.ids(&format!(
            "SELECT id FROM warehouse.test.t WHERE s = '{long}'"
        ))
        .await,
        vec![1]
    );
}

#[tokio::test]
async fn date_and_decimal_partitions_round_trip() {
    let f = Fixture::new().await;
    f.create_table(
        "t",
        vec![
            PartitionField::new(4, 1000, "d", Transform::Identity),
            PartitionField::new(6, 1001, "amount_trunc", Transform::Truncate(50)),
        ],
    )
    .await;
    f.sql("INSERT INTO warehouse.test.t (id, d, amount) VALUES (1, DATE '2023-05-15', 10.65), (2, NULL, NULL)")
        .await;
    assert_eq!(
        f.ids("SELECT id FROM warehouse.test.t ORDER BY id").await,
        vec![1, 2]
    );
    assert_eq!(
        f.ids("SELECT id FROM warehouse.test.t WHERE d = DATE '2023-05-15'")
            .await,
        vec![1]
    );
}

/// Writes the rows of `select` as an equality-delete file on `equality_ids`.
async fn delete_where(f: &Fixture, table: &str, select: &str, equality_ids: &[i32]) {
    let batches = f.sql(select).await;
    let Tabular::Table(mut table) = f
        .catalog
        .clone()
        .load_tabular(&Identifier::new(&["test".to_string()], table))
        .await
        .unwrap()
    else {
        panic!("{table} is not a table");
    };
    let files = write_equality_deletes_parquet_partitioned(
        &table,
        stream::iter(batches.into_iter().map(Ok::<_, ArrowError>)),
        None,
        equality_ids,
    )
    .await
    .unwrap();
    table
        .new_transaction(None)
        .append_delete(files)
        .commit()
        .await
        .unwrap();
}

#[tokio::test]
async fn limit_never_returns_equality_deleted_rows() {
    let f = Fixture::new().await;
    f.create_table("t", vec![]).await;
    f.sql("INSERT INTO warehouse.test.t (id) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")
        .await;
    // Physical order of the delete file: 10, 9, 1.
    delete_where(
        &f,
        "t",
        "SELECT id FROM warehouse.test.t WHERE id IN (1, 9, 10) ORDER BY id DESC",
        &[1],
    )
    .await;
    for limit in 1..=9 {
        let ids = f
            .ids(&format!("SELECT id FROM warehouse.test.t LIMIT {limit}"))
            .await;
        assert_eq!(ids.len(), limit.min(7), "LIMIT {limit} returned {ids:?}");
        assert!(
            ids.iter().all(|id| (2..=8).contains(id)),
            "LIMIT {limit} returned {ids:?}"
        );
    }
}

#[tokio::test]
async fn equality_deletes_match_null_keys() {
    let f = Fixture::new().await;
    f.create_table("t", vec![]).await;
    f.sql("INSERT INTO warehouse.test.t (id, n, s) VALUES (1, NULL, 'a'), (2, 2, 'b'), (3, NULL, 'c')")
        .await;
    // Two-column key (n, s) = (NULL, 'a') deletes only row 1.
    delete_where(
        &f,
        "t",
        "SELECT n, s FROM warehouse.test.t WHERE id = 1",
        &[2, 3],
    )
    .await;
    assert_eq!(
        f.ids("SELECT id FROM warehouse.test.t ORDER BY id").await,
        vec![2, 3]
    );
    // One-column key n = NULL deletes every remaining row with a NULL n.
    delete_where(&f, "t", "SELECT n FROM warehouse.test.t WHERE id = 3", &[2]).await;
    assert_eq!(
        f.ids("SELECT id FROM warehouse.test.t ORDER BY id").await,
        vec![2]
    );
}
