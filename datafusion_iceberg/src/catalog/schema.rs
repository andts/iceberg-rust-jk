use std::{ops::Deref, sync::Arc};

use crate::{catalog::mirror::Mirror, error::Error, DataFusionTable};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::arrow::error::ArrowError;
use datafusion::common::error::GenericError;
use datafusion::common::DataFusionError;
use datafusion::datasource::MemTable;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::{catalog::SchemaProvider, datasource::TableProvider, error::Result};
use futures::TryStreamExt;
use iceberg_rust::arrow::write::write_parquet_partitioned;
use iceberg_rust::catalog::{identifier::Identifier, namespace::Namespace};
use iceberg_rust::spec::arrow::schema::new_fields_with_ids;
use tokio::runtime::Handle;

/// Recursively remove any `PARQUET:field_id` metadata from the given fields so
/// that a subsequent `new_fields_with_ids` call assigns fresh, contiguous ids
/// instead of preserving (possibly colliding) ids inherited from source tables.
fn strip_field_ids(fields: &Fields) -> Fields {
    use iceberg_rust::spec::arrow::schema::PARQUET_FIELD_ID_META_KEY;

    fields
        .into_iter()
        .map(|field| {
            let mut metadata = field.metadata().clone();
            metadata.remove(PARQUET_FIELD_ID_META_KEY);

            let data_type = match field.data_type() {
                DataType::Struct(child) => DataType::Struct(strip_field_ids(child)),
                DataType::List(child) => {
                    DataType::List(Arc::new(strip_single_field_id(child)))
                }
                DataType::LargeList(child) => {
                    DataType::LargeList(Arc::new(strip_single_field_id(child)))
                }
                other => other.clone(),
            };

            Arc::new(
                Field::new(field.name(), data_type, field.is_nullable()).with_metadata(metadata),
            )
        })
        .collect()
}

fn strip_single_field_id(field: &Field) -> Field {
    use iceberg_rust::spec::arrow::schema::PARQUET_FIELD_ID_META_KEY;

    let mut metadata = field.metadata().clone();
    metadata.remove(PARQUET_FIELD_ID_META_KEY);

    let data_type = match field.data_type() {
        DataType::Struct(child) => DataType::Struct(strip_field_ids(child)),
        DataType::List(child) => DataType::List(Arc::new(strip_single_field_id(child))),
        DataType::LargeList(child) => DataType::LargeList(Arc::new(strip_single_field_id(child))),
        other => other.clone(),
    };

    Field::new(field.name(), data_type, field.is_nullable()).with_metadata(metadata)
}

#[derive(Debug)]
pub struct IcebergSchema {
    schema: Namespace,
    catalog: Arc<Mirror>,
}

impl IcebergSchema {
    pub(crate) fn new(schema: Namespace, catalog: Arc<Mirror>) -> Self {
        IcebergSchema { schema, catalog }
    }
}

#[async_trait::async_trait]
impl SchemaProvider for IcebergSchema {
    fn table_names(&self) -> Vec<String> {
        let tables = self.catalog.table_names(&self.schema);
        match tables {
            Err(_) => vec![],
            Ok(schemas) => schemas.into_iter().map(|x| x.name().to_owned()).collect(),
        }
    }
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.catalog
            .table(
                Identifier::try_new(&[self.schema.deref(), &[name.to_string()]].concat(), None)
                    .map_err(Error::from)?,
            )
            .await
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        use iceberg_rust::{
            spec::{schema::Schema, types::StructType},
            table::Table,
        };

        if self.table_exist(&name) {
            return Err(DataFusionError::External(GenericError::from(
                "Table already exists.",
            )));
        }

        // A CTAS result inherits `PARQUET:field_id` metadata from its source
        // table scans. Across a multi-table join those ids collide, and since
        // upstream `new_fields_with_ids` now preserves existing ids (rather than
        // always assigning fresh ones), the resulting iceberg schema would carry
        // duplicate field ids. Strip the inherited ids so the new table gets a
        // fresh, contiguous id assignment.
        let stripped_fields = strip_field_ids(table.schema().fields());
        let arrow_schema_fields = new_fields_with_ids(&stripped_fields, &mut 0);

        let iceberg_schema =
            StructType::try_from(&arrow_schema_fields).expect("convert arrow schema to iceberg");

        Ok(tokio::task::block_in_place(|| {
            Handle::current().block_on(async {
                let mut iceberg_table = Table::builder()
                    .with_name(name)
                    .with_schema(Schema::from_struct_type(iceberg_schema, 0, None))
                    .build(self.schema.as_ref(), self.catalog.catalog())
                    .await
                    .map_err(|e| DataFusionError::External(GenericError::from(e)))
                    .expect("create iceberg table");

                if let Some(mem_table) =
                    (table.as_ref() as &dyn std::any::Any).downcast_ref::<MemTable>()
                {
                    let mut all_batches = vec![];
                    for arc_inner_vec in mem_table.batches.iter() {
                        let inner_vec = arc_inner_vec.read().await;
                        all_batches.extend(
                            inner_vec
                                .iter()
                                .map(|x| Ok(x.clone())),
                        );
                    }

                    let stream = futures::stream::iter(all_batches);
                    let batch_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(table.schema(), stream));

                    let metadata_files =
                        write_parquet_partitioned(&iceberg_table, batch_stream.map_err(ArrowError::from), None)
                            .await
                            .expect("write parquet files");

                    iceberg_table
                        .new_transaction(None)
                        .append_data(metadata_files)
                        .commit()
                        .await
                        .map_err(|e| DataFusionError::External(GenericError::from(e)))
                        .expect("append data to table");
                }

                Some(
                    Arc::new(DataFusionTable::new_table(iceberg_table, None, None, None))
                        as Arc<dyn TableProvider>,
                )
            })
        }))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.catalog.table_exists(
            Identifier::try_new(&[self.schema.deref(), &[name.to_string()]].concat(), None)
                .unwrap(),
        )
    }
    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let identifier =
            Identifier::try_new(&[self.schema.deref(), &[name.to_string()]].concat(), None)
                .map_err(Error::from)?;
        self.catalog.deregister_table(identifier)?;
        Ok(None)
    }
}
