/*!
 * Functions to read arrow record batches from an iceberg table
*/

use std::{convert, sync::Arc};

use arrow::record_batch::RecordBatch;
use futures::{stream, Stream, StreamExt};
use object_store::ObjectStore;
use parquet::{
    arrow::{async_reader::ParquetObjectReader, ParquetRecordBatchStreamBuilder},
    errors::ParquetError,
};

use crate::error::Error;
use crate::object_store::store::path_from_location;

use iceberg_rust_spec::spec::manifest::{FileFormat, ManifestEntry};

/// Read a parquet file into a stream of arrow recordbatches. The record batches are read asynchronously and are unordered
pub async fn read(
    manifest_files: impl Iterator<Item = ManifestEntry>,
    object_store: Arc<dyn ObjectStore>,
) -> impl Stream<Item = Result<RecordBatch, ParquetError>> {
    stream::iter(manifest_files)
        .then(move |manifest| {
            let object_store = object_store.clone();
            async move {
                let data_file = manifest.data_file();
                match data_file.file_format() {
                    FileFormat::Parquet => {
                        let object_reader = ParquetObjectReader::new(
                            object_store,
                            path_from_location(data_file.file_path())
                                .map_err(|err| Error::External(Box::new(err)))?,
                        )
                        .with_file_size((*data_file.file_size_in_bytes()) as u64);
                        Ok::<_, Error>(
                            ParquetRecordBatchStreamBuilder::new(object_reader)
                                .await?
                                .build()?,
                        )
                    }
                    _ => Err(Error::NotSupported("fileformat".to_string())),
                }
            }
        })
        .map(|result| match result {
            Ok(batches) => batches.left_stream(),
            Err(err) => stream::iter(std::iter::once(Err(ParquetError::External(Box::new(err)))))
                .right_stream(),
        })
        .flat_map_unordered(None, convert::identity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg_rust_spec::spec::{
        manifest::{Content, DataFile, Status},
        table_metadata::FormatVersion,
        values::Struct,
    };
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn invalid_manifest_location_is_reported_instead_of_skipped() {
        let data_file = DataFile::builder()
            .with_content(Content::Data)
            .with_file_path("s3://bucket/data//broken.parquet".to_string())
            .with_file_format(FileFormat::Parquet)
            .with_partition(Struct {
                fields: vec![],
                lookup: Default::default(),
            })
            .with_record_count(0)
            .with_file_size_in_bytes(0)
            .with_column_sizes(None)
            .with_value_counts(None)
            .with_null_value_counts(None)
            .with_nan_value_counts(None)
            .with_distinct_counts(None)
            .with_lower_bounds(None)
            .with_upper_bounds(None)
            .build()
            .unwrap();
        let entry = ManifestEntry::builder()
            .with_format_version(FormatVersion::V2)
            .with_status(Status::Added)
            .with_data_file(data_file)
            .build()
            .unwrap();

        let stream = read(std::iter::once(entry), Arc::new(InMemory::new())).await;
        let items: Vec<_> = stream.collect().await;
        assert_eq!(items.len(), 1, "a bad file must not silently disappear");
        assert!(items[0].is_err());
    }
}
