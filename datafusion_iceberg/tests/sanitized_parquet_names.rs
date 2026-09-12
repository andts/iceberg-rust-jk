//! Iceberg-java writers (Spark, Flink, Trino) sanitize Parquet column names:
//! `TypeToMessageType` runs every field name through
//! `AvroSchemaUtil.makeCompatibleName`, so a column the Iceberg schema calls
//! `my col` is stored in the file as `my_x20col`. The original name lives only
//! in the Iceberg schema; readers are expected to resolve columns by field id.
//!
//! This test builds exactly that situation and scans it from a cold session.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::tree_node::{TransformedResult, TreeNode};
use datafusion::execution::context::SessionContext;
use datafusion::execution::SessionStateBuilder;
use datafusion_iceberg::catalog::catalog_list::IcebergCatalogList;
use datafusion_iceberg::planner::{iceberg_transform, IcebergQueryPlanner};
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_sql_catalog::SqlCatalogList;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

fn scratch(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("dfi-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn boot(dir: &std::path::Path) -> SessionContext {
    let iceberg_catalog_list = Arc::new(
        SqlCatalogList::new(
            &format!("sqlite://{}/catalog.db?mode=rwc", dir.display()),
            ObjectStoreBuilder::filesystem(dir),
        )
        .await
        .expect("SqlCatalogList"),
    );
    let catalog_list = Arc::new(
        IcebergCatalogList::new(iceberg_catalog_list)
            .await
            .expect("IcebergCatalogList"),
    );
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_catalog_list(catalog_list)
        .with_query_planner(Arc::new(IcebergQueryPlanner::new()))
        .build();
    SessionContext::new_with_state(state)
}

async fn run(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    let plan = ctx
        .state()
        .create_logical_plan(sql)
        .await
        .unwrap_or_else(|e| panic!("plan `{sql}`: {e}"));
    let plan = plan
        .transform(iceberg_transform)
        .data()
        .unwrap_or_else(|e| panic!("iceberg_transform `{sql}`: {e}"));
    ctx.execute_logical_plan(plan)
        .await
        .unwrap_or_else(|e| panic!("execute `{sql}`: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("collect `{sql}`: {e}"))
}

fn data_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(p) = stack.pop() {
        for e in std::fs::read_dir(&p).unwrap() {
            let path = e.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|x| x == "parquet")
                && path.to_string_lossy().contains("/data/")
            {
                out.push(path);
            }
        }
    }
    out
}

/// Rewrite `path` applying `renames` to its column names and adding 1000 to
/// every (Int64) value, keeping each field's metadata -- including
/// `PARQUET:field_id`, which is what an Iceberg reader is supposed to key off.
///
/// The rewritten file must keep the *exact* byte size of the original: the
/// Iceberg manifest records `file_size_in_bytes` and `datafusion_iceberg` hands
/// that to the parquet reader. Padding is added as a key-value metadata entry
/// until the sizes match.
fn rewrite_parquet(path: &std::path::Path, renames: &[(&str, &str)]) {
    let target_len = std::fs::metadata(path).unwrap().len() as usize;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path).unwrap()).unwrap();
    let schema = builder.schema().clone();
    let batches: Vec<RecordBatch> = builder.build().unwrap().collect::<Result<_, _>>().unwrap();

    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            let name = renames
                .iter()
                .find(|(from, _)| from == f.name())
                .map(|(_, to)| *to)
                .unwrap_or_else(|| f.name().as_str());
            Field::new(name, f.data_type().clone(), f.is_nullable())
                .with_metadata(f.metadata().clone())
        })
        .collect();
    let new_schema = Arc::new(Schema::new(fields));

    let write = |pad: usize| -> Vec<u8> {
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .set_statistics_enabled(EnabledStatistics::None)
            .set_key_value_metadata(if pad == 0 {
                None
            } else {
                Some(vec![KeyValue::new("pad".to_string(), "0".repeat(pad))])
            })
            .build();
        let mut buf = Vec::new();
        {
            // Iceberg-Java writers do not embed an `ARROW:schema` key-value
            // either, so dropping it keeps the fixture realistic.
            let opts = ArrowWriterOptions::new()
                .with_properties(props)
                .with_skip_arrow_metadata(true);
            let mut w =
                ArrowWriter::try_new_with_options(&mut buf, new_schema.clone(), opts).unwrap();
            for b in &batches {
                let cols: Vec<ArrayRef> = b
                    .columns()
                    .iter()
                    .map(|c| {
                        let a = c.as_any().downcast_ref::<Int64Array>().unwrap();
                        Arc::new(Int64Array::from(
                            a.iter().map(|v| v.map(|v| v + 1000)).collect::<Vec<_>>(),
                        )) as ArrayRef
                    })
                    .collect();
                w.write(&RecordBatch::try_new(new_schema.clone(), cols).unwrap())
                    .unwrap();
            }
            w.close().unwrap();
        }
        buf
    };

    let mut pad: isize = 0;
    let mut buf = write(0);
    for _ in 0..64 {
        if buf.len() == target_len {
            break;
        }
        pad += target_len as isize - buf.len() as isize;
        assert!(
            pad >= 0,
            "cannot shrink the rewritten parquet to {target_len} bytes"
        );
        buf = write(pad as usize);
    }
    assert_eq!(
        buf.len(),
        target_len,
        "could not match the original file size"
    );
    std::fs::write(path, buf).unwrap();
}

#[tokio::test]
async fn sanitized_parquet_names_are_resolved_by_field_id() {
    let dir = scratch("sanitized");
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
    drop(ctx);

    // Read from a *fresh* session, as an executor pod always is.
    let cold = boot(&dir).await;
    let out = pretty_format_batches(&run(&cold, "SELECT * FROM warehouse.ws.t ORDER BY id").await)
        .unwrap()
        .to_string();
    println!("{out}");
    let _ = std::fs::remove_dir_all(&dir);

    // `rewrite_parquet` adds 1000 to every value, so a populated column also
    // proves the rewritten file -- not a cached copy -- is what was read.
    assert!(
        out.contains("1001"),
        "expected the rewritten file to be the one read:\n{out}"
    );
    assert!(
        out.contains("1010") && out.contains("1020"),
        "whitespace column read back as NULL:\n{out}"
    );
    // The user-visible name must still be the Iceberg one, not the file's.
    assert!(
        out.contains("my col") && !out.contains("my_x20col"),
        "output schema should keep the Iceberg column name:\n{out}"
    );
}
