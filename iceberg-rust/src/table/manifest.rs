//! Provides functionality for reading and writing Iceberg manifest files.
//!
//! This module implements the core manifest handling capabilities:
//! - Reading manifest files via [`ManifestReader`]
//! - Writing new manifest files via [`ManifestWriter`]
//! - Converting between manifest formats (V1/V2)
//! - Managing manifest entries and their metadata
//! - Tracking partition statistics and file counts
//!
//! Manifest files are a key part of Iceberg tables, containing:
//! - Data file locations and metadata
//! - Partition information
//! - File statistics and metrics
//! - Schema and partition spec references
//!
//! The module handles both V1 and V2 manifest formats transparently.

use std::{
    collections::HashSet,
    future::Future,
    io::Read,
    iter::{repeat, Map, Repeat, Zip},
    sync::Arc,
};

use apache_avro::{
    to_value, types::Value as AvroValue, Reader as AvroReader, Schema as AvroSchema,
    Writer as AvroWriter,
};
use futures::TryFutureExt;
use iceberg_rust_spec::{
    manifest::{Content, ManifestEntry, ManifestEntryV1, ManifestEntryV2, ManifestEntryV3, Status},
    manifest_list::{self, FieldSummary, ManifestListEntry},
    partition::{PartitionField, PartitionSpec},
    schema::{Schema, SchemaV1, SchemaV2},
    table_metadata::{FormatVersion, TableMetadata},
    util::strip_prefix,
    values::{Struct, Value},
};
use object_store::{ObjectStore, ObjectStoreExt};

use crate::error::Error;

type ReaderZip<'a, R> = Zip<AvroReader<'a, R>, Repeat<Arc<(Schema, PartitionSpec, FormatVersion)>>>;
type ReaderMap<'a, R> = Map<
    ReaderZip<'a, R>,
    fn(
        (
            Result<AvroValue, apache_avro::Error>,
            Arc<(Schema, PartitionSpec, FormatVersion)>,
        ),
    ) -> Result<ManifestEntry, Error>,
>;

/// A reader for Iceberg manifest files that provides an iterator over manifest entries.
///
/// The reader handles both V1 and V2 manifest formats and automatically converts entries
/// to the appropriate version based on the manifest metadata.
///
/// # Type Parameters
/// * `'a` - The lifetime of the underlying reader
/// * `R` - The type implementing `Read` that provides the manifest data
pub(crate) struct ManifestReader<'a, R: Read> {
    reader: ReaderMap<'a, R>,
}

impl<R: Read> Iterator for ManifestReader<'_, R> {
    type Item = Result<ManifestEntry, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        self.reader.next()
    }
}

impl<R: Read> ManifestReader<'_, R> {
    /// Creates a new ManifestReader from a reader implementing the Read trait.
    ///
    /// This method initializes a reader that can parse both V1 and V2 manifest formats.
    /// It extracts metadata from the Avro file including format version, schema, and partition spec information.
    ///
    /// # Arguments
    /// * `reader` - A type implementing the `Read` trait that provides access to the manifest file data
    ///
    /// # Returns
    /// * `Result<Self, Error>` - A new ManifestReader instance or an error if initialization fails
    ///
    /// # Errors
    /// Returns an error if:
    /// * The Avro reader cannot be created
    /// * Required metadata fields are missing
    /// * Format version is invalid
    /// * Schema or partition spec information cannot be parsed
    pub(crate) fn new(reader: R) -> Result<Self, Error> {
        let reader = AvroReader::new(reader)?;
        reject_uuid_partition_values(reader.writer_schema())?;
        let metadata = reader.user_metadata();

        let format_version: FormatVersion = match metadata
            .get("format-version")
            .map(|bytes| String::from_utf8(bytes.clone()))
            .transpose()?
            .unwrap_or("1".to_string())
            .as_str()
        {
            "1" => Ok(FormatVersion::V1),
            "2" => Ok(FormatVersion::V2),
            "3" => Ok(FormatVersion::V3),
            _ => Err(Error::InvalidFormat("format version".to_string())),
        }?;

        let schema: Schema = match format_version {
            FormatVersion::V1 => TryFrom::<SchemaV1>::try_from(serde_json::from_slice(
                metadata
                    .get("schema")
                    .ok_or(Error::InvalidFormat("manifest metadata".to_string()))?,
            )?)?,
            FormatVersion::V2 | FormatVersion::V3 => {
                TryFrom::<SchemaV2>::try_from(serde_json::from_slice(
                    metadata
                        .get("schema")
                        .ok_or(Error::InvalidFormat("manifest metadata".to_string()))?,
                )?)?
            }
        };

        let partition_fields: Vec<PartitionField> = serde_json::from_slice(
            metadata
                .get("partition-spec")
                .ok_or(Error::InvalidFormat("manifest metadata".to_string()))?,
        )?;
        let spec_id: i32 = metadata
            .get("partition-spec-id")
            .map(|x| String::from_utf8(x.clone()))
            .transpose()?
            .unwrap_or("0".to_string())
            .parse()?;
        let partition_spec = PartitionSpec::builder()
            .with_spec_id(spec_id)
            .with_fields(partition_fields)
            .build()?;
        Ok(Self {
            reader: reader
                .zip(repeat(Arc::new((schema, partition_spec, format_version))))
                .map(avro_value_to_manifest_entry),
        })
    }
}

/// Fails if the manifest's partition record has a uuid logical type.
///
/// Iceberg Java writes uuid partition values as `fixed[16]` with
/// `logicalType: uuid`. apache-avro 0.21 parses that as its `Schema::Uuid`,
/// which it decodes as a length-prefixed string, so the values would be
/// mis-decoded. The parsed schema no longer says whether the type was `fixed`
/// or `string`; neither Java nor this crate writes the string form, so any
/// uuid logical type is rejected. This crate writes uuid partition values as
/// plain `fixed[16]` (see `partition_value_schema`).
fn reject_uuid_partition_values(schema: &AvroSchema) -> Result<(), Error> {
    let field = |schema: &AvroSchema, name: &str| match schema {
        AvroSchema::Record(record) => record
            .lookup
            .get(name)
            .map(|index| record.fields[*index].schema.clone()),
        _ => None,
    };
    let Some(partition) =
        field(schema, "data_file").and_then(|data_file| field(&data_file, "partition"))
    else {
        return Ok(());
    };
    let AvroSchema::Record(partition) = partition else {
        return Ok(());
    };
    let is_uuid = |schema: &AvroSchema| match schema {
        AvroSchema::Union(union) => union.variants().contains(&AvroSchema::Uuid),
        schema => *schema == AvroSchema::Uuid,
    };
    match partition.fields.iter().find(|field| is_uuid(&field.schema)) {
        Some(field) => Err(Error::NotSupported(format!(
            "manifest partition field {}: uuid partition values written with logicalType uuid \
             cannot be read with apache-avro 0.21",
            field.name
        ))),
        None => Ok(()),
    }
}

/// A writer for Iceberg manifest files that handles creating and updating manifest entries.
///
/// ManifestWriter manages both creating new manifests and updating existing ones, handling
/// the complexities of manifest metadata, entry tracking, and partition summaries.
///
/// # Type Parameters
/// * `'schema` - The lifetime of the Avro schema used for writing entries
/// * `'metadata` - The lifetime of the table metadata reference
///
/// # Fields
/// * `table_metadata` - Reference to the table's metadata containing schema and partition information
/// * `manifest` - The manifest list entry being built or modified
/// * `writer` - The underlying Avro writer for serializing manifest entries
pub(crate) struct ManifestWriter<'schema, 'metadata> {
    table_metadata: &'metadata TableMetadata,
    manifest: ManifestListEntry,
    writer: AvroWriter<'schema, Vec<u8>>,
}

#[derive(Default, Debug, Clone, Copy)]
pub(crate) struct FilteredManifestStats {
    pub removed_data_files: i32,
    pub removed_records: i64,
    pub removed_file_size_bytes: i64,
}

impl FilteredManifestStats {
    pub(crate) fn append(&mut self, stats: FilteredManifestStats) {
        self.removed_file_size_bytes += stats.removed_file_size_bytes;
        self.removed_records += stats.removed_records;
        self.removed_data_files += stats.removed_data_files;
    }
}

impl<'schema, 'metadata> ManifestWriter<'schema, 'metadata> {
    /// Creates a new ManifestWriter for writing manifest entries to a new manifest file.
    ///
    /// # Arguments
    /// * `manifest_location` - The location where the manifest file will be written
    /// * `snapshot_id` - The ID of the snapshot this manifest belongs to
    /// * `schema` - The Avro schema used for serializing manifest entries
    /// * `table_metadata` - The table metadata containing schema and partition information
    /// * `branch` - Optional branch name to get the current schema from
    ///
    /// # Returns
    /// * `Result<Self, Error>` - A new ManifestWriter instance or an error if initialization fails
    ///
    /// # Errors
    /// Returns an error if:
    /// * The Avro writer cannot be created
    /// * Required metadata fields cannot be serialized
    /// * The partition spec ID is not found in table metadata
    pub(crate) fn new(
        manifest_location: &str,
        snapshot_id: i64,
        schema: &'schema AvroSchema,
        table_metadata: &'metadata TableMetadata,
        content: manifest_list::Content,
    ) -> Result<Self, Error> {
        let mut writer = AvroWriter::new(schema, Vec::new());

        writer.add_user_metadata(
            "format-version".to_string(),
            match table_metadata.format_version {
                FormatVersion::V1 => "1".as_bytes(),
                FormatVersion::V2 => "2".as_bytes(),
                FormatVersion::V3 => "3".as_bytes(),
            },
        )?;

        writer.add_user_metadata(
            "schema".to_string(),
            match table_metadata.format_version {
                FormatVersion::V1 => serde_json::to_string(&Into::<SchemaV1>::into(
                    table_metadata.current_schema()?.clone(),
                ))?,
                FormatVersion::V2 | FormatVersion::V3 => serde_json::to_string(
                    &Into::<SchemaV2>::into(table_metadata.current_schema()?.clone()),
                )?,
            },
        )?;

        writer.add_user_metadata(
            "schema-id".to_string(),
            serde_json::to_string(&table_metadata.current_schema()?.schema_id())?,
        )?;

        let spec_id = table_metadata.default_spec_id;

        writer.add_user_metadata(
            "partition-spec".to_string(),
            serde_json::to_string(
                &table_metadata
                    .partition_specs
                    .get(&spec_id)
                    .ok_or(Error::NotFound(format!("Partition spec with id {spec_id}")))?
                    .fields(),
            )?,
        )?;

        writer.add_user_metadata(
            "partition-spec-id".to_string(),
            serde_json::to_string(&spec_id)?,
        )?;

        writer.add_user_metadata(
            "content".to_string(),
            match content {
                manifest_list::Content::Data => "data",
                manifest_list::Content::Deletes => "deletes",
            },
        )?;

        let manifest = ManifestListEntry {
            format_version: table_metadata.format_version,
            manifest_path: manifest_location.to_owned(),
            manifest_length: 0,
            partition_spec_id: table_metadata.default_spec_id,
            content,
            sequence_number: table_metadata.last_sequence_number + 1,
            min_sequence_number: table_metadata.last_sequence_number + 1,
            added_snapshot_id: snapshot_id,
            added_files_count: Some(0),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(0),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: None,
            key_metadata: None,
            first_row_id: None,
        };

        Ok(ManifestWriter {
            manifest,
            writer,
            table_metadata,
        })
    }

    /// Creates a ManifestWriter from an existing manifest file, preserving its entries.
    ///
    /// This method reads an existing manifest file and creates a new writer that includes
    /// all the existing entries with their status updated to "Existing". It also updates
    /// sequence numbers and snapshot IDs as needed.
    ///
    /// # Arguments
    /// * `bytes` - The raw bytes of the existing manifest file
    /// * `manifest` - The manifest list entry describing the existing manifest
    /// * `schema` - The Avro schema used for serializing manifest entries
    /// * `table_metadata` - The table metadata containing schema and partition information
    /// * `branch` - Optional branch name to get the current schema from
    ///
    /// # Returns
    /// * `Result<Self, Error>` - A new ManifestWriter instance or an error if initialization fails
    ///
    /// # Errors
    /// Returns an error if:
    /// * The existing manifest cannot be read
    /// * The Avro writer cannot be created
    /// * Required metadata fields cannot be serialized
    /// * The partition spec ID is not found in table metadata
    pub(crate) fn from_existing(
        manifest_reader: impl Iterator<Item = Result<ManifestEntry, Error>>,
        mut manifest: ManifestListEntry,
        schema: &'schema AvroSchema,
        table_metadata: &'metadata TableMetadata,
    ) -> Result<Self, Error> {
        let mut writer = AvroWriter::new(schema, Vec::new());
        writer.add_user_metadata(
            "format-version".to_string(),
            match table_metadata.format_version {
                FormatVersion::V1 => "1".as_bytes(),
                FormatVersion::V2 => "2".as_bytes(),
                FormatVersion::V3 => "3".as_bytes(),
            },
        )?;

        writer.add_user_metadata(
            "schema".to_string(),
            match table_metadata.format_version {
                FormatVersion::V1 => serde_json::to_string(&Into::<SchemaV1>::into(
                    table_metadata.current_schema()?.clone(),
                ))?,
                FormatVersion::V2 | FormatVersion::V3 => serde_json::to_string(
                    &Into::<SchemaV2>::into(table_metadata.current_schema()?.clone()),
                )?,
            },
        )?;

        writer.add_user_metadata(
            "schema-id".to_string(),
            serde_json::to_string(&table_metadata.current_schema()?.schema_id())?,
        )?;

        let spec_id = table_metadata.default_spec_id;

        writer.add_user_metadata(
            "partition-spec".to_string(),
            serde_json::to_string(
                &table_metadata
                    .partition_specs
                    .get(&spec_id)
                    .ok_or(Error::NotFound(format!("Partition spec with id {spec_id}")))?
                    .fields(),
            )?,
        )?;

        writer.add_user_metadata(
            "partition-spec-id".to_string(),
            serde_json::to_string(&spec_id)?,
        )?;

        writer.add_user_metadata(
            "content".to_string(),
            match manifest.content {
                manifest_list::Content::Data => "data",
                manifest_list::Content::Deletes => "deletes",
            },
        )?;

        let partition_fields = table_metadata.current_partition_fields()?;
        // Collect first so a read or encode error fails the rewrite instead
        // of silently dropping the entry (and its data file) from the table.
        let entries = manifest_reader
            .map(|entry| {
                let mut entry = entry?;
                *entry.status_mut() = Status::Existing;
                if entry.sequence_number().is_none() {
                    *entry.sequence_number_mut() = Some(manifest.sequence_number);
                }
                if entry.snapshot_id().is_none() {
                    *entry.snapshot_id_mut() = Some(manifest.added_snapshot_id);
                }
                Ok(to_value(
                    entry.encode_partition_for_avro(&partition_fields)?,
                )?)
            })
            .collect::<Result<Vec<_>, Error>>()?;
        writer.extend(entries)?;

        manifest.sequence_number = table_metadata.last_sequence_number + 1;

        manifest.existing_files_count = Some(
            manifest.existing_files_count.unwrap_or(0) + manifest.added_files_count.unwrap_or(0),
        );
        manifest.existing_rows_count = Some(
            manifest.existing_rows_count.unwrap_or(0) + manifest.added_rows_count.unwrap_or(0),
        );

        // Zero, not absent: `added_files_count` / `added_rows_count` are
        // REQUIRED fields in the v2/v3 manifest-list schema, and
        // `ManifestListEntryV2::from` unwraps them. A manifest whose entries
        // were all rewritten as Existing adds nothing, but it must still say
        // so explicitly — leaving these `None` panics as soon as the entry is
        // serialized back into the manifest list without an intervening
        // `add_file` call to repopulate them.
        manifest.added_files_count = Some(0);
        manifest.added_rows_count = Some(0);

        Ok(ManifestWriter {
            manifest,
            writer,
            table_metadata,
        })
    }

    /// Creates a ManifestWriter from an existing manifest file with selective filtering of entries.
    ///
    /// This method reads an existing manifest file and creates a new writer that includes
    /// only the entries whose file paths are NOT in the provided filter set. Entries that
    /// pass the filter have their status updated to "Existing" and their sequence numbers
    /// and snapshot IDs updated as needed.
    ///
    /// This is particularly useful for overwrite operations where specific files need to be
    /// excluded from the new manifest while preserving other existing entries.
    ///
    /// # Arguments
    /// * `bytes` - The raw bytes of the existing manifest file
    /// * `manifest` - The manifest list entry describing the existing manifest
    /// * `filter` - A set of file paths to exclude from the new manifest
    /// * `schema` - The Avro schema used for serializing manifest entries
    /// * `table_metadata` - The table metadata containing schema and partition information
    /// * `branch` - Optional branch name to get the current schema from
    ///
    /// # Returns
    /// * `Result<Self, Error>` - A new ManifestWriter instance or an error if initialization fails
    ///
    /// # Errors
    /// Returns an error if:
    /// * The existing manifest cannot be read
    /// * The Avro writer cannot be created
    /// * Required metadata fields cannot be serialized
    /// * The partition spec ID is not found in table metadata
    ///
    /// # Behavior
    /// - Entries whose file paths are in the `filter` set are excluded from the new manifest
    /// - Remaining entries have their status set to `Status::Existing`
    /// - Sequence numbers are updated for entries that don't have them
    /// - Snapshot IDs are updated for entries that don't have them
    /// - The manifest's sequence number is incremented
    /// - File counts are updated to reflect the filtered entries
    pub(crate) fn from_existing_with_filter(
        bytes: &[u8],
        mut manifest: ManifestListEntry,
        filter: &HashSet<String>,
        schema: &'schema AvroSchema,
        table_metadata: &'metadata TableMetadata,
    ) -> Result<(Self, FilteredManifestStats), Error> {
        let manifest_reader = ManifestReader::new(bytes)?;

        let mut writer = AvroWriter::new(schema, Vec::new());
        let mut filtered_stats = FilteredManifestStats::default();

        writer.add_user_metadata(
            "format-version".to_string(),
            match table_metadata.format_version {
                FormatVersion::V1 => "1".as_bytes(),
                FormatVersion::V2 => "2".as_bytes(),
                FormatVersion::V3 => "3".as_bytes(),
            },
        )?;

        writer.add_user_metadata(
            "schema".to_string(),
            match table_metadata.format_version {
                FormatVersion::V1 => serde_json::to_string(&Into::<SchemaV1>::into(
                    table_metadata.current_schema()?.clone(),
                ))?,
                FormatVersion::V2 | FormatVersion::V3 => serde_json::to_string(
                    &Into::<SchemaV2>::into(table_metadata.current_schema()?.clone()),
                )?,
            },
        )?;

        writer.add_user_metadata(
            "schema-id".to_string(),
            serde_json::to_string(&table_metadata.current_schema()?.schema_id())?,
        )?;

        let spec_id = table_metadata.default_spec_id;

        writer.add_user_metadata(
            "partition-spec".to_string(),
            serde_json::to_string(
                &table_metadata
                    .partition_specs
                    .get(&spec_id)
                    .ok_or(Error::NotFound(format!("Partition spec with id {spec_id}")))?
                    .fields(),
            )?,
        )?;

        writer.add_user_metadata(
            "partition-spec-id".to_string(),
            serde_json::to_string(&spec_id)?,
        )?;

        writer.add_user_metadata(
            "content".to_string(),
            match manifest.content {
                manifest_list::Content::Data => "data",
                manifest_list::Content::Deletes => "deletes",
            },
        )?;

        let partition_fields = table_metadata.current_partition_fields()?;
        // Entries in `filter` are the only intended omissions; any read or
        // encode error fails the rewrite.
        let mut entries = Vec::new();
        for entry in manifest_reader {
            let mut entry = entry?;
            if !filter.contains(entry.data_file().file_path()) {
                *entry.status_mut() = Status::Existing;
                if entry.sequence_number().is_none() {
                    *entry.sequence_number_mut() = Some(manifest.sequence_number);
                }
                if entry.snapshot_id().is_none() {
                    *entry.snapshot_id_mut() = Some(manifest.added_snapshot_id);
                }
                entries.push(to_value(
                    entry.encode_partition_for_avro(&partition_fields)?,
                )?);
            } else {
                if *entry.data_file().content() == Content::Data {
                    filtered_stats.removed_records += entry.data_file().record_count();
                }
                filtered_stats.removed_file_size_bytes += entry.data_file().file_size_in_bytes();
                filtered_stats.removed_data_files += 1;
            }
        }
        writer.extend(entries)?;

        manifest.sequence_number = table_metadata.last_sequence_number + 1;

        manifest.existing_files_count = Some(
            manifest.existing_files_count.unwrap_or(0) + manifest.added_files_count.unwrap_or(0)
                - filtered_stats.removed_data_files,
        );
        manifest.existing_rows_count = Some(
            manifest.existing_rows_count.unwrap_or(0) + manifest.added_rows_count.unwrap_or(0)
                - filtered_stats.removed_records,
        );

        // Zero, not absent: `added_files_count` / `added_rows_count` are
        // REQUIRED fields in the v2/v3 manifest-list schema, and
        // `ManifestListEntryV2::from` unwraps them. A manifest whose entries
        // were all rewritten as Existing adds nothing, but it must still say
        // so explicitly — leaving these `None` panics as soon as the entry is
        // serialized back into the manifest list without an intervening
        // `add_file` call to repopulate them.
        manifest.added_files_count = Some(0);
        manifest.added_rows_count = Some(0);

        Ok((
            ManifestWriter {
                manifest,
                writer,
                table_metadata,
            },
            filtered_stats,
        ))
    }

    /// Appends a manifest entry to the manifest file and updates summary statistics.
    ///
    /// This method adds a new manifest entry while maintaining:
    /// - Partition statistics (null values, bounds)
    /// - File counts by status (added, existing, deleted)
    /// - Row counts (added, deleted)
    /// - Sequence number tracking
    ///
    /// # Arguments
    /// * `manifest_entry` - The manifest entry to append
    ///
    /// # Returns
    /// * `Result<(), Error>` - Ok if the entry was successfully appended, Error otherwise
    ///
    /// # Errors
    /// Returns an error if:
    /// * The entry cannot be serialized
    /// * Partition statistics cannot be updated
    /// * The default partition spec is not found
    pub(crate) fn append(&mut self, manifest_entry: ManifestEntry) -> Result<(), Error> {
        let mut added_rows_count = 0;
        let mut deleted_rows_count = 0;

        if self.manifest.partitions.is_none() {
            self.manifest.partitions = Some(
                self.table_metadata
                    .default_partition_spec()?
                    .fields()
                    .iter()
                    .map(|_| FieldSummary {
                        contains_null: false,
                        contains_nan: None,
                        lower_bound: None,
                        upper_bound: None,
                    })
                    .collect::<Vec<FieldSummary>>(),
            );
        }

        match manifest_entry.data_file().content() {
            Content::Data => {
                added_rows_count += manifest_entry.data_file().record_count();
            }
            Content::EqualityDeletes => {
                deleted_rows_count += manifest_entry.data_file().record_count();
            }
            _ => (),
        }
        let status = *manifest_entry.status();

        update_partitions(
            self.manifest.partitions.as_mut().unwrap(),
            manifest_entry.data_file().partition(),
            self.table_metadata.default_partition_spec()?.fields(),
        )?;

        if let Some(sequence_number) = manifest_entry.sequence_number() {
            if self.manifest.min_sequence_number > *sequence_number {
                self.manifest.min_sequence_number = *sequence_number;
            }
        };

        let partition_fields = self.table_metadata.current_partition_fields()?;
        self.writer
            .append_ser(manifest_entry.encode_partition_for_avro(&partition_fields)?)?;

        match status {
            Status::Added => {
                self.manifest.added_files_count = match self.manifest.added_files_count {
                    Some(count) => Some(count + 1),
                    None => Some(1),
                };
            }
            Status::Existing => {
                self.manifest.existing_files_count = match self.manifest.existing_files_count {
                    Some(count) => Some(count + 1),
                    None => Some(1),
                };
            }
            Status::Deleted => {
                self.manifest.deleted_files_count = match self.manifest.deleted_files_count {
                    Some(count) => Some(count + 1),
                    None => Some(1),
                };
            }
        }

        self.manifest.added_rows_count = match self.manifest.added_rows_count {
            Some(count) => Some(count + added_rows_count),
            None => Some(added_rows_count),
        };

        self.manifest.deleted_rows_count = match self.manifest.deleted_rows_count {
            Some(count) => Some(count + deleted_rows_count),
            None => Some(deleted_rows_count),
        };

        Ok(())
    }

    /// Finalizes the manifest writer and writes the manifest file to storage.
    ///
    /// This method:
    /// 1. Completes writing all entries
    /// 2. Updates the manifest length
    /// 3. Writes the manifest file to the object store
    ///
    /// # Arguments
    /// * `object_store` - The object store to write the manifest file to
    ///
    /// # Returns
    /// * `Result<ManifestListEntry, Error>` - The completed manifest list entry or an error
    ///
    /// # Errors
    /// Returns an error if:
    /// * The writer cannot be finalized
    /// * The manifest file cannot be written to storage
    pub(crate) async fn finish(
        mut self,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<ManifestListEntry, Error> {
        let manifest_bytes = self.writer.into_inner()?;

        let manifest_length: i64 = manifest_bytes.len() as i64;

        self.manifest.manifest_length += manifest_length;

        object_store
            .put(
                &strip_prefix(&self.manifest.manifest_path).as_str().into(),
                manifest_bytes.into(),
            )
            .await?;
        Ok(self.manifest)
    }

    /// Finishes writing the manifest file concurrently.
    ///
    /// This method completes the manifest writing process by finalizing the writer
    /// and returning both the manifest list entry and a future for the actual file upload.
    /// The upload operation can be awaited separately, allowing for concurrent processing
    /// of multiple manifest writes.
    ///
    /// # Arguments
    ///
    /// * `object_store` - The object store implementation used to persist the manifest file
    ///
    /// # Returns
    ///
    /// Returns a tuple containing:
    /// - `ManifestListEntry`: The completed manifest entry with updated metadata
    /// - `impl Future<Output = Result<PutResult, Error>>`: A future that performs the actual file upload
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The writer cannot be finalized
    /// - There are issues preparing the upload operation
    pub(crate) fn finish_concurrently(
        mut self,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<(ManifestListEntry, impl Future<Output = Result<(), Error>>), Error> {
        let manifest_bytes = self.writer.into_inner()?;

        let manifest_length: i64 = manifest_bytes.len() as i64;

        self.manifest.manifest_length += manifest_length;

        let path = strip_prefix(&self.manifest.manifest_path).as_str().into();
        let future = async move {
            object_store
                .put(&path, manifest_bytes.into())
                .map_ok(|_| ())
                .map_err(Error::from)
                .await
        };
        Ok((self.manifest, future))
    }

    pub(crate) fn apply_filtered_stats(&mut self, filtered_stats: &FilteredManifestStats) {
        let removed_files = filtered_stats.removed_data_files;
        if removed_files > 0 {
            self.manifest.deleted_files_count = match self.manifest.deleted_files_count {
                Some(count) => Some(count + removed_files),
                None => Some(removed_files),
            };
        }

        if filtered_stats.removed_records > 0 {
            self.manifest.deleted_rows_count = match self.manifest.deleted_rows_count {
                Some(count) => Some(count + filtered_stats.removed_records),
                None => Some(filtered_stats.removed_records),
            };
        }
    }
}

#[allow(clippy::type_complexity)]
/// Convert avro value to ManifestEntry based on the format version of the table.
fn avro_value_to_manifest_entry(
    value: (
        Result<AvroValue, apache_avro::Error>,
        Arc<(Schema, PartitionSpec, FormatVersion)>,
    ),
) -> Result<ManifestEntry, Error> {
    let entry = value.0?;
    let schema = &value.1 .0;
    let partition_spec = &value.1 .1;
    let format_version = &value.1 .2;
    match format_version {
        FormatVersion::V3 => ManifestEntry::try_from_v3(
            apache_avro::from_value::<ManifestEntryV3>(&entry)?,
            schema,
            partition_spec,
        )
        .map_err(Error::from),
        FormatVersion::V2 => ManifestEntry::try_from_v2(
            apache_avro::from_value::<ManifestEntryV2>(&entry)?,
            schema,
            partition_spec,
        )
        .map_err(Error::from),
        FormatVersion::V1 => ManifestEntry::try_from_v1(
            apache_avro::from_value::<ManifestEntryV1>(&entry)?,
            schema,
            partition_spec,
        )
        .map_err(Error::from),
    }
}

fn update_partitions(
    partitions: &mut [FieldSummary],
    partition_values: &Struct,
    partition_columns: &[PartitionField],
) -> Result<(), Error> {
    for (field, summary) in partition_columns.iter().zip(partitions.iter_mut()) {
        let value = partition_values.get(field.name()).and_then(|x| x.as_ref());
        if value.is_none() {
            summary.contains_null = true;
        }
        if let Some(value) = value {
            if summary.lower_bound.is_none() {
                summary.lower_bound = Some(value.clone());
            } else if let Some(lower_bound) = &mut summary.lower_bound {
                match (value, lower_bound) {
                    (Value::Boolean(val), Value::Boolean(current)) if *current & !*val => {
                        *current = *val
                    }
                    (Value::Int(val), Value::Int(current)) if *current > *val => *current = *val,
                    (Value::LongInt(val), Value::LongInt(current)) if *current > *val => {
                        *current = *val
                    }
                    (Value::Float(val), Value::Float(current)) if *current > *val => {
                        *current = *val
                    }
                    (Value::Double(val), Value::Double(current)) if *current > *val => {
                        *current = *val
                    }
                    (Value::Date(val), Value::Date(current)) if *current > *val => *current = *val,
                    (Value::Time(val), Value::Time(current)) if *current > *val => *current = *val,
                    (Value::Timestamp(val), Value::Timestamp(current)) if *current > *val => {
                        *current = *val
                    }
                    (Value::TimestampTZ(val), Value::TimestampTZ(current)) if *current > *val => {
                        *current = *val
                    }
                    (Value::String(val), Value::String(current)) if *current > *val => {
                        *current = val.clone()
                    }
                    (Value::UUID(val), Value::UUID(current)) if *current > *val => *current = *val,
                    (Value::Fixed(_, val), Value::Fixed(_, current)) if *current > *val => {
                        *current = val.clone()
                    }
                    (Value::Binary(val), Value::Binary(current)) if *current > *val => {
                        *current = val.clone()
                    }
                    (Value::Decimal(val), Value::Decimal(current)) if *current > *val => {
                        *current = *val
                    }
                    _ => {}
                }
            }
            if summary.upper_bound.is_none() {
                summary.upper_bound = Some(value.clone());
            } else if let Some(upper_bound) = &mut summary.upper_bound {
                match (value, upper_bound) {
                    (Value::Boolean(val), Value::Boolean(current)) if !*current & *val => {
                        *current = *val
                    }
                    (Value::Int(val), Value::Int(current)) if *current < *val => *current = *val,
                    (Value::LongInt(val), Value::LongInt(current)) if *current < *val => {
                        *current = *val
                    }
                    (Value::Float(val), Value::Float(current)) if *current < *val => {
                        *current = *val
                    }
                    (Value::Double(val), Value::Double(current)) if *current < *val => {
                        *current = *val
                    }
                    (Value::Date(val), Value::Date(current)) if *current < *val => *current = *val,
                    (Value::Time(val), Value::Time(current)) if *current < *val => *current = *val,
                    (Value::Timestamp(val), Value::Timestamp(current)) if *current < *val => {
                        *current = *val
                    }
                    (Value::TimestampTZ(val), Value::TimestampTZ(current)) if *current < *val => {
                        *current = *val
                    }
                    (Value::String(val), Value::String(current)) if *current < *val => {
                        *current = val.clone()
                    }
                    (Value::UUID(val), Value::UUID(current)) if *current < *val => *current = *val,
                    (Value::Fixed(_, val), Value::Fixed(_, current)) if *current < *val => {
                        *current = val.clone()
                    }
                    (Value::Binary(val), Value::Binary(current)) if *current < *val => {
                        *current = val.clone()
                    }
                    (Value::Decimal(val), Value::Decimal(current)) if *current < *val => {
                        *current = *val
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    // -- TestManifestReader (15) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[case(11)]
    #[case(12)]
    #[case(13)]
    #[case(14)]
    #[case(15)]
    #[ignore = "ManifestReader: V1/V2/V3 spec round-trip, filter without select, inheritable metadata, invalid usage"]
    fn test_manifest_reader_scenarios(#[case] _scenario: usize) {
        unimplemented!("ManifestReader");
    }

    // -- TestManifestReaderStats (10) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[ignore = "no ManifestReader filter / select / selectStats / project projection API"]
    fn test_manifest_reader_stats_scenarios(#[case] _scenario: usize) {
        unimplemented!("ManifestReaderStats");
    }

    // -- TestManifestWriter (9) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[ignore = "ManifestWriter direct-API scenarios (without-row-stats, fast-append entry status, partition summary, manifest cleanup)"]
    fn test_manifest_writer_scenarios(#[case] _scenario: usize) {
        unimplemented!("ManifestWriter");
    }

    // -- TestManifestWriterVersions (20) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[case(11)]
    #[case(12)]
    #[case(13)]
    #[case(14)]
    #[case(15)]
    #[case(16)]
    #[case(17)]
    #[case(18)]
    #[case(19)]
    #[case(20)]
    #[ignore = "no CreateTableBuilder::with_format_version setter; V1, V3, V4 + compression configuration paths"]
    fn test_manifest_writer_versions_scenarios(#[case] _scenario: usize) {
        unimplemented!("ManifestWriterVersions");
    }

    // -- TestManifestListVersions (10) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[ignore = "manifest list V1 / V3 round-trips unreachable without with_format_version setter; V3 first_row_id gap"]
    fn test_manifest_list_versions_scenarios(#[case] _scenario: usize) {
        unimplemented!("ManifestListVersions");
    }

    // -- TestManifestFileParser (3) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[ignore = "no ManifestFile JSON parser for REST scan-planning responses"]
    fn test_manifest_file_parser_scenarios(#[case] _scenario: usize) {
        unimplemented!("ManifestFileParser");
    }

    // -- TestManifestFileUtil (4) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[ignore = "no ManifestFileUtil helpers for filtering manifest entries by status / spec id"]
    fn test_manifest_file_util_scenarios(#[case] _scenario: usize) {
        unimplemented!("ManifestFileUtil");
    }

    // -- TestManifestInfoStruct (8) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[ignore = "no ManifestInfo struct projection of manifest entries"]
    fn test_manifest_info_struct_scenarios(#[case] _scenario: usize) {
        unimplemented!("ManifestInfoStruct");
    }

    // -- TestManifestListEncryption (2) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[ignore = "no encrypted manifest list read/write"]
    fn test_manifest_list_encryption_scenarios(#[case] _scenario: usize) {
        unimplemented!("ManifestListEncryption");
    }

    // -- TestManifestCaching (5) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[ignore = "no LRU cache for manifest reads"]
    fn test_manifest_caching_scenarios(#[case] _scenario: usize) {
        unimplemented!("ManifestCaching");
    }

    // -- Avro features suite (66 cases) covering: TestAvroDataWriter (1),
    //    TestAvroDeleteWriters (3), TestAvroEnums (1),
    //    TestAvroOptionsWithNonNullDefaults (2), TestAvroFileSplit (4),
    //    TestEncryptedAvroFileSplit (4), TestAvroIterable (1),
    //    TestAvroNameMapping (8), TestBuildAvroProjection (6),
    //    TestAvroReadProjection (1), TestAvroSchemaProjection (3),
    //    TestInternalData (2), TestSingleMessageEncoding (12),
    //    TestDecoderResolver (2), TestReadProjection (14),
    //    TestReadDefaultValues (2).
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[case(11)]
    #[case(12)]
    #[case(13)]
    #[case(14)]
    #[case(15)]
    #[case(16)]
    #[case(17)]
    #[case(18)]
    #[case(19)]
    #[case(20)]
    #[case(21)]
    #[case(22)]
    #[case(23)]
    #[case(24)]
    #[case(25)]
    #[case(26)]
    #[case(27)]
    #[case(28)]
    #[case(29)]
    #[case(30)]
    #[case(31)]
    #[case(32)]
    #[case(33)]
    #[case(34)]
    #[case(35)]
    #[case(36)]
    #[case(37)]
    #[case(38)]
    #[case(39)]
    #[case(40)]
    #[case(41)]
    #[case(42)]
    #[case(43)]
    #[case(44)]
    #[case(45)]
    #[case(46)]
    #[case(47)]
    #[case(48)]
    #[case(49)]
    #[case(50)]
    #[case(51)]
    #[case(52)]
    #[case(53)]
    #[case(54)]
    #[case(55)]
    #[case(56)]
    #[case(57)]
    #[case(58)]
    #[case(59)]
    #[case(60)]
    #[case(61)]
    #[case(62)]
    #[case(63)]
    #[case(64)]
    #[case(65)]
    #[case(66)]
    #[ignore = "Avro features beyond manifest read/write: data + delete writers, enums, options-with-non-null-defaults, file split, name mapping, build/read projection, schema projection, single-message encoding, decoder resolver, read projection, read default values"]
    fn test_avro_features_suite_scenarios(#[case] _scenario: usize) {
        unimplemented!("Avro features suite");
    }
}

#[cfg(test)]
mod partition_summary_tests {
    use iceberg_rust_spec::{
        manifest_list::FieldSummary,
        partition::{PartitionField, Transform},
        values::{Struct, Value},
    };

    use super::update_partitions;

    #[test]
    fn null_partition_value_sets_contains_null() {
        let fields = [PartitionField::new(1, 1000, "n", Transform::Identity)];
        let mut summaries = [FieldSummary {
            contains_null: false,
            contains_nan: None,
            lower_bound: None,
            upper_bound: None,
        }];

        update_partitions(
            &mut summaries,
            &Struct::from_iter([("n".to_owned(), Some(Value::LongInt(3)))]),
            &fields,
        )
        .unwrap();
        assert!(!summaries[0].contains_null);

        update_partitions(
            &mut summaries,
            &Struct::from_iter([("n".to_owned(), None)]),
            &fields,
        )
        .unwrap();
        assert!(summaries[0].contains_null);
        assert_eq!(summaries[0].lower_bound, Some(Value::LongInt(3)));
    }
}

#[cfg(test)]
mod avro_partition_tests {
    use std::collections::HashMap;

    use iceberg_rust_spec::{
        decimal::decimal_from_i128_with_scale,
        manifest::{partition_value_schema, Content, DataFile, FileFormat, ManifestEntry, Status},
        manifest_list::{self, ManifestListEntry},
        partition::{PartitionField, PartitionSpec, Transform},
        schema::Schema,
        table_metadata::{FormatVersion, TableMetadata, TableMetadataBuilder},
        types::{PrimitiveType, StructField, Type},
        values::{Struct, Value},
    };

    use super::{ManifestReader, ManifestWriter};
    use crate::error::Error;

    fn decimal_table() -> TableMetadata {
        TableMetadataBuilder::default()
            .location("/")
            .current_schema_id(0)
            .schemas(HashMap::from_iter([(
                0,
                Schema::builder()
                    .with_struct_field(StructField {
                        id: 1,
                        name: "amount".to_owned(),
                        required: false,
                        field_type: Type::Primitive(PrimitiveType::Decimal {
                            precision: 3,
                            scale: 2,
                        }),
                        doc: None,
                        initial_default: None,
                        write_default: None,
                    })
                    .build()
                    .unwrap(),
            )]))
            .default_spec_id(0)
            .partition_specs(HashMap::from_iter([(
                0,
                PartitionSpec::builder()
                    .with_partition_field(PartitionField::new(
                        1,
                        1000,
                        "amount",
                        Transform::Identity,
                    ))
                    .build()
                    .unwrap(),
            )]))
            .build()
            .unwrap()
    }

    fn entry(partition: Value) -> ManifestEntry {
        ManifestEntry::builder()
            .with_format_version(FormatVersion::V2)
            .with_status(Status::Added)
            .with_snapshot_id(1)
            .with_sequence_number(1)
            .with_data_file(
                DataFile::builder()
                    .with_content(Content::Data)
                    .with_file_path("/data.parquet".to_owned())
                    .with_file_format(FileFormat::Parquet)
                    .with_partition(Struct::from_iter([("amount".to_owned(), Some(partition))]))
                    .with_record_count(1)
                    .with_file_size_in_bytes(10)
                    .with_column_sizes(None)
                    .with_value_counts(None)
                    .with_null_value_counts(None)
                    .with_nan_value_counts(None)
                    .with_distinct_counts(None)
                    .with_lower_bounds(None)
                    .with_upper_bounds(None)
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap()
    }

    fn manifest() -> ManifestListEntry {
        ManifestListEntry {
            format_version: FormatVersion::V2,
            manifest_path: "/manifest.avro".to_owned(),
            manifest_length: 0,
            partition_spec_id: 0,
            content: manifest_list::Content::Data,
            sequence_number: 1,
            min_sequence_number: 1,
            added_snapshot_id: 1,
            added_files_count: Some(1),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(1),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: None,
            key_metadata: None,
            first_row_id: None,
        }
    }

    /// An entry whose partition value cannot be encoded for the current spec
    /// (e.g. read with an older spec) fails the rewrite instead of vanishing.
    #[test]
    fn from_existing_propagates_partition_encode_errors() {
        let table = decimal_table();
        let schema = ManifestEntry::schema(
            &partition_value_schema(&table.current_partition_fields().unwrap()).unwrap(),
            &FormatVersion::V2,
        )
        .unwrap();
        let fits = entry(Value::Decimal(
            decimal_from_i128_with_scale(123, 2).unwrap(),
        ));
        // 123.45 has five digits; the column is decimal(3, 2).
        let too_wide = entry(Value::Decimal(
            decimal_from_i128_with_scale(12345, 2).unwrap(),
        ));

        assert!(ManifestWriter::from_existing(
            [Ok(fits.clone())].into_iter(),
            manifest(),
            &schema,
            &table
        )
        .is_ok());
        let result = ManifestWriter::from_existing(
            [Ok(fits), Ok(too_wide)].into_iter(),
            manifest(),
            &schema,
            &table,
        );
        assert!(
            matches!(
                result,
                Err(Error::Iceberg(iceberg_rust_spec::error::Error::Conversion(
                    ..
                )))
            ),
            "{:?}",
            result.err()
        );
    }

    fn zigzag(value: i64, out: &mut Vec<u8>) {
        let mut n = ((value << 1) ^ (value >> 63)) as u64;
        while n >= 0x80 {
            out.push((n as u8) | 0x80);
            n >>= 7;
        }
        out.push(n as u8);
    }

    fn avro_bytes(bytes: &[u8], out: &mut Vec<u8>) {
        zigzag(bytes.len() as i64, out);
        out.extend_from_slice(bytes);
    }

    /// An Avro object container header (no data blocks) with the given raw
    /// schema JSON, as another writer (e.g. Iceberg Java) would produce it.
    fn container_header(schema_json: &str) -> Vec<u8> {
        let mut out = b"Obj\x01".to_vec();
        zigzag(2, &mut out);
        avro_bytes(b"avro.schema", &mut out);
        avro_bytes(schema_json.as_bytes(), &mut out);
        avro_bytes(b"avro.codec", &mut out);
        avro_bytes(b"null", &mut out);
        zigzag(0, &mut out);
        out.extend_from_slice(&[7; 16]);
        out
    }

    #[test]
    fn manifest_with_java_uuid_partition_is_rejected() {
        let partition = r#"{"type": "record", "name": "r102", "fields": [{
            "name": "u", "field-id": 1000, "default": null,
            "type": ["null", {"type": "fixed", "size": 16, "logicalType": "uuid", "name": "uuid_fixed"}]
        }]}"#;
        let schema = ManifestEntry::schema(partition, &FormatVersion::V2).unwrap();
        // Re-insert the partition record as Java writes it: apache-avro would
        // otherwise serialize its parsed form.
        let mut schema_json: serde_json::Value = serde_json::to_value(&schema).unwrap();
        let data_file = schema_json["fields"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|field| field["name"] == "data_file")
            .unwrap();
        let partition_field = data_file["type"]["fields"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|field| field["name"] == "partition")
            .unwrap();
        partition_field["type"] = serde_json::from_str(partition).unwrap();
        let header = container_header(&schema_json.to_string());

        let result = ManifestReader::new(&header[..]);
        match result {
            Err(Error::NotSupported(message)) => assert!(message.contains("uuid"), "{message}"),
            Err(other) => panic!("unexpected error {other:?}"),
            Ok(_) => panic!("manifest with a uuid logical type partition was accepted"),
        }
    }

    #[test]
    fn manifest_with_fixed_uuid_partition_is_accepted() {
        let partition = r#"{"type": "record", "name": "r102", "fields": [{
            "name": "u", "field-id": 1000, "default": null,
            "type": ["null", {"type": "fixed", "size": 16, "name": "uuid_fixed"}]
        }]}"#;
        let schema = ManifestEntry::schema(partition, &FormatVersion::V2).unwrap();
        let mut writer = apache_avro::Writer::new(&schema, Vec::new());
        writer
            .add_user_metadata(
                "schema".to_owned(),
                r#"{"type":"struct","schema-id":0,"fields":[]}"#,
            )
            .unwrap();
        writer
            .add_user_metadata("partition-spec".to_owned(), "[]")
            .unwrap();
        writer
            .add_user_metadata("format-version".to_owned(), "2")
            .unwrap();
        let bytes = writer.into_inner().unwrap();
        assert!(ManifestReader::new(&bytes[..]).is_ok());
    }
}
