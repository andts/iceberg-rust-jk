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
#[cfg(feature = "proto")]
use datafusion::parquet::file::metadata::ParquetMetaDataReader;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::prelude::SessionContext;
use iceberg_rust::spec::{deletion_vector::DeletionVector, util};
use object_store::{memory::InMemory, path::Path as ObjPath, ObjectStoreExt, PutPayload};
use roaring::RoaringTreemap;

use super::{
    dv_exec::IcebergDvExec, object_store_url_for_location, DATA_FILE_PATH_COLUMN, ROW_NUMBER_COLUMN,
};

/// Data files are written in row groups of this many rows, so a longer file
/// has several: scans can split it by byte range and prune by statistics.
const ROWS_PER_ROW_GROUP: usize = 4;

pub(crate) struct DvFixture {
    pub(crate) ctx: SessionContext,
    /// The store, for executor sessions in the codec tests.
    #[cfg(feature = "proto")]
    pub(crate) store: Arc<InMemory>,
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
            .register_object_store(url.as_ref(), store.clone());
        Self {
            ctx,
            #[cfg(feature = "proto")]
            store,
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
    /// at absolute row `ROWS_PER_ROW_GROUP`. Only the codec tests split files.
    #[cfg(feature = "proto")]
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
        (
            whole.clone().with_range(0, split),
            whole.with_range(split, size),
        )
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
