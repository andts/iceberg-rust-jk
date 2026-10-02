/*!
 * Helpers for parquet files
*/

use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, HashMap, HashSet},
    ops::Sub,
};

use iceberg_rust_spec::{
    partition::BoundPartitionField,
    spec::{
        manifest::{AvroMap, Content, DataFile, FileFormat},
        schema::Schema,
        types::Type,
        values::{PhysicalTypeHint, Struct, Value},
    },
    table_metadata::WRITE_METADATA_METRICS_DISTINCT_COUNTS_ENABLED,
};
use parquet::file::{
    metadata::{ColumnChunkMetaData, ParquetMetaData, RowGroupMetaData},
    writer::TrackedWrite,
};
use thrift::protocol::{TCompactOutputProtocol, TSerializable};
use tracing::instrument;

use crate::error::Error;
use crate::file_format::metrics::{
    truncate_lower_bound, truncate_upper_bound, MetricsConfig, MetricsMode,
};

/// Parquet file-level KV metadata key that opts in to HLL-based `distinct_count`
/// estimation for `Int64` columns. A patched arrow-rs writer reads this entry
/// via `WriterProperties::key_value_metadata()` and, when the value is `"true"`,
/// runs an HLL sketch over each `Int64` column chunk and stores the estimate in
/// the chunk's `distinct_count` statistic. Setting it has no effect against
/// upstream parquet — the key is ignored.
pub const ICEBERG_ESTIMATE_INT64_DISTINCT_COUNT_META_KEY: &str =
    "iceberg.estimate-int64-distinct-count";

/// Parquet file-level KV metadata key recording the id of the table sort order
/// the file's rows honor. Written by the sorted write path together with the
/// row groups' `sorting_columns`, and lifted into the manifest entry's
/// `sort_order_id` by [`parquet_to_datafile`], so the attestation travels with
/// the file itself rather than being asserted at commit time.
pub const ICEBERG_SORT_ORDER_ID_META_KEY: &str = "iceberg.sort-order-id";

/// The sort order id a Parquet file attests in its footer, if any.
///
/// Returns `None` when the file carries no [`ICEBERG_SORT_ORDER_ID_META_KEY`]
/// entry or when the entry does not parse as an integer.
pub fn attested_sort_order_id(file_metadata: &ParquetMetaData) -> Option<i32> {
    file_metadata
        .file_metadata()
        .key_value_metadata()?
        .iter()
        .find(|kv| kv.key == ICEBERG_SORT_ORDER_ID_META_KEY)
        .and_then(|kv| kv.value.as_deref())
        .and_then(|value| value.parse::<i32>().ok())
}

/// Builds the manifest [`DataFile`] of a written Parquet file from its footer.
///
/// # Arguments
///
/// * `location` - the file's path, recorded as the data file path
/// * `file_size` - the file's size in bytes
/// * `file_metadata` - the file's Parquet footer; supplies the record count,
///   column sizes, value/null counts and lower/upper bounds
/// * `schema` - the table schema the file was written with; maps Parquet
///   column paths to field ids
/// * `partition_fields` - the table's partition fields bound to their source
///   columns, in partition spec order
/// * `partition_values` - the partition tuple the file was written for, one
///   entry per partition field (`None` entries are NULL partition values)
/// * `equality_ids` - `Some` for an equality delete file, with the equality
///   field ids; `None` for a data file
/// * `table_properties` - table properties selecting the metrics mode per
///   column (`write.metadata.metrics.*`) and distinct-count collection
///
/// With `Some` partition values, the tuple is recorded as given and its length
/// must match `partition_fields`. With `None`, every partition field's value is
/// derived from its source column's statistics, which only works when every
/// row group carries exact min/max statistics and a null count for that column
/// (a row group in which the column is entirely null needs no min/max), the
/// min and max transform to one value across the file, and nulls are not mixed
/// with values; a column that is null in every row yields a NULL value.
///
/// Bounds are merged across row groups and truncated per the metrics mode; a
/// column's bounds are dropped when a row group holding values has no min/max
/// statistics for it.
///
/// # Errors
///
/// `Error::InvalidFormat` when the number of partition values does not match
/// the partition fields, or when a partition value cannot be derived (missing
/// or truncated statistics, several partitions in one file); `Error::Schema`
/// when a Parquet column is not in `schema`; conversion errors when statistics
/// do not decode as the column's type.
#[allow(clippy::too_many_arguments)]
#[instrument(name = "iceberg_rust::file_format::parquet::parquet_to_datafile", level = "debug", skip(file_metadata, schema, partition_fields, partition_values, table_properties), fields(
    location = location,
    file_size = file_size,
    partition_field_count = partition_fields.len(),
    has_equality_ids = equality_ids.is_some()
))]
pub fn parquet_to_datafile(
    location: &str,
    file_size: u64,
    file_metadata: &ParquetMetaData,
    schema: &Schema,
    partition_fields: &[BoundPartitionField<'_>],
    partition_values: Option<&[Option<Value>]>,
    equality_ids: Option<&[i32]>,
    table_properties: &HashMap<String, String>,
) -> Result<DataFile, Error> {
    let write_distinct_counts = table_properties
        .get(WRITE_METADATA_METRICS_DISTINCT_COUNTS_ENABLED)
        .is_some_and(|x| x == "true");
    if let Some(values) = partition_values {
        if values.len() != partition_fields.len() {
            return Err(Error::InvalidFormat(format!(
                "{location}: {} partition values given for {} partition fields",
                values.len(),
                partition_fields.len()
            )));
        }
    }
    let partition = match partition_values {
        Some(values) => partition_fields
            .iter()
            .zip(values)
            .map(|(field, value)| (field.name().to_owned(), value.clone()))
            .collect::<Struct>(),
        None => partition_fields
            .iter()
            .map(|field| {
                derive_partition_value(location, file_metadata, schema, field)
                    .map(|value| (field.name().to_owned(), value))
            })
            .collect::<Result<Struct, Error>>()?,
    };

    let metrics_config = MetricsConfig::from_table_properties(table_properties);
    // `max-inferred-column-defaults` counts top-level columns, so a nested
    // column inherits the position of the field it hangs off.
    let top_level_indices: HashMap<&str, usize> = schema
        .iter()
        .enumerate()
        .map(|(index, field)| (field.name.as_str(), index))
        .collect();

    let mut column_sizes = AvroMap(HashMap::new());
    let mut value_counts = AvroMap(HashMap::new());
    let mut null_value_counts = AvroMap(HashMap::new());
    let mut distinct_counts = write_distinct_counts.then(|| AvroMap(HashMap::new()));
    let mut lower_bounds: HashMap<i32, Value> = HashMap::new();
    let mut upper_bounds: HashMap<i32, Value> = HashMap::new();
    // Which mode produced each column's bounds, so truncation can be applied
    // once at the end rather than per row group.
    let mut column_metrics_modes: HashMap<i32, MetricsMode> = HashMap::new();
    // Columns whose bounds must be dropped: their row groups produced bounds
    // of different value types, or a row group holding values has no min/max
    // (bounds that do not cover it would let pruning drop its rows).
    let mut conflicting_bounds: HashSet<i32> = HashSet::new();

    for row_group in file_metadata.row_groups() {
        for column in row_group.columns() {
            let column_name = column.column_descr().name();
            let column_path = column.column_path().parts().join(".");
            let id = schema
                .get_name(&column_path)
                .ok_or_else(|| Error::Schema(column_name.to_string(), "".to_string()))?
                .id;

            let top_level_index = column_path
                .split('.')
                .next()
                .and_then(|root| top_level_indices.get(root).copied());
            let metrics_mode = metrics_config.mode_for(&column_path, top_level_index);
            column_metrics_modes.insert(id, metrics_mode);

            // Column sizes describe the file's physical layout rather than its
            // values, so they are collected regardless of the metrics mode.
            column_sizes
                .entry(id)
                .and_modify(|x| *x += column.compressed_size())
                .or_insert(column.compressed_size());
            if metrics_mode.records_counts() {
                value_counts
                    .entry(id)
                    .and_modify(|x| *x += row_group.num_rows())
                    .or_insert(row_group.num_rows());
            }

            if metrics_mode.records_bounds() && !has_min_max_or_only_nulls(row_group, column) {
                conflicting_bounds.insert(id);
            }

            if let Some(statistics) = column.statistics() {
                if let Some(null_count) = statistics
                    .null_count_opt()
                    .filter(|_| metrics_mode.records_counts())
                {
                    null_value_counts
                        .entry(id)
                        .and_modify(|x| *x += null_count as i64)
                        .or_insert(null_count as i64);
                }

                let data_type = &schema
                    .fields()
                    .get(id as usize)
                    .ok_or_else(|| Error::Schema(column_name.to_string(), "".to_string()))?
                    .field_type;

                // Parquet's physical type can encode a logical type differently than
                // the Iceberg spec: INT32/INT64 stats are native little-endian
                // (Decimal's spec encoding is big-endian), and BYTE_ARRAY stats hold
                // Uuid's UTF-8 string form (spec: 16-byte big-endian integer).
                let physical_type_hint = physical_type_hint(column);

                if let Some(distinct_counts) = distinct_counts
                    .as_mut()
                    .filter(|_| metrics_mode.records_counts())
                {
                    if let (Some(distinct_count), Some(min_bytes), Some(max_bytes)) = (
                        statistics.distinct_count_opt(),
                        statistics.min_bytes_opt(),
                        statistics.max_bytes_opt(),
                    ) {
                        let min = Value::try_from_bytes_with_hint(
                            min_bytes,
                            data_type,
                            physical_type_hint,
                        )?;
                        let max = Value::try_from_bytes_with_hint(
                            max_bytes,
                            data_type,
                            physical_type_hint,
                        )?;
                        let current_min = lower_bounds.get(&id);
                        let current_max = upper_bounds.get(&id);
                        match (min, max, current_min, current_max) {
                            (
                                Value::Int(min),
                                Value::Int(max),
                                Some(Value::Int(current_min)),
                                Some(Value::Int(current_max)),
                            ) => {
                                distinct_counts
                                    .entry(id)
                                    .and_modify(|x| {
                                        *x += estimate_distinct_count(
                                            &[current_min, current_max],
                                            &[&min, &max],
                                            *x,
                                            distinct_count as i64,
                                        );
                                    })
                                    .or_insert(distinct_count as i64);
                            }
                            (
                                Value::LongInt(min),
                                Value::LongInt(max),
                                Some(Value::LongInt(current_min)),
                                Some(Value::LongInt(current_max)),
                            ) => {
                                distinct_counts
                                    .entry(id)
                                    .and_modify(|x| {
                                        *x += estimate_distinct_count(
                                            &[current_min, current_max],
                                            &[&min, &max],
                                            *x,
                                            distinct_count as i64,
                                        );
                                    })
                                    .or_insert(distinct_count as i64);
                            }
                            (_, _, None, None) => {
                                distinct_counts.entry(id).or_insert(distinct_count as i64);
                            }
                            _ => (),
                        }
                    }
                }

                // Parquet truncates long statistics (64 bytes by default): a
                // truncated min is a prefix, so still a lower bound, and a
                // truncated max is rounded up, so still an upper bound.
                if metrics_mode.records_bounds() && matches!(data_type, Type::Primitive(_)) {
                    if let Some(min_bytes) = statistics.min_bytes_opt() {
                        let min = Value::try_from_bytes_with_hint(
                            min_bytes,
                            data_type,
                            physical_type_hint,
                        )?;
                        merge_bound(
                            &mut lower_bounds,
                            &mut conflicting_bounds,
                            id,
                            min,
                            Ordering::Less,
                        );
                    }
                    if let Some(max_bytes) = statistics.max_bytes_opt() {
                        let max = Value::try_from_bytes_with_hint(
                            max_bytes,
                            data_type,
                            physical_type_hint,
                        )?;
                        merge_bound(
                            &mut upper_bounds,
                            &mut conflicting_bounds,
                            id,
                            max,
                            Ordering::Greater,
                        );
                    }
                }
            }
        }
    }
    lower_bounds.retain(|id, _| !conflicting_bounds.contains(id));
    upper_bounds.retain(|id, _| !conflicting_bounds.contains(id));
    // Truncate once, after the bounds have been merged across row groups.
    // Truncating per row group and merging afterwards would repeatedly widen an
    // already-widened upper bound.
    let lower_bounds = lower_bounds
        .into_iter()
        .map(|(id, value)| match column_metrics_modes.get(&id) {
            Some(MetricsMode::Truncate(length)) => (id, truncate_lower_bound(value, *length)),
            _ => (id, value),
        })
        .collect::<HashMap<i32, Value>>();
    let upper_bounds = upper_bounds
        .into_iter()
        .filter_map(|(id, value)| match column_metrics_modes.get(&id) {
            // A bound that cannot be raised is dropped: an understated upper
            // bound would prune files that still hold matching rows.
            Some(MetricsMode::Truncate(length)) => {
                truncate_upper_bound(value, *length).map(|value| (id, value))
            }
            _ => Some((id, value)),
        })
        .collect::<HashMap<i32, Value>>();

    let mut builder = DataFile::builder();
    builder
        .with_content(if equality_ids.is_none() {
            Content::Data
        } else {
            Content::EqualityDeletes
        })
        .with_file_path(location.to_string())
        .with_file_format(FileFormat::Parquet)
        .with_partition(partition)
        .with_record_count(file_metadata.file_metadata().num_rows())
        .with_file_size_in_bytes(file_size as i64)
        .with_column_sizes(Some(column_sizes))
        .with_value_counts(Some(value_counts))
        .with_null_value_counts(Some(null_value_counts))
        .with_nan_value_counts(None)
        .with_distinct_counts(distinct_counts)
        .with_lower_bounds(Some(lower_bounds))
        .with_upper_bounds(Some(upper_bounds));

    if let Some(equality_ids) = equality_ids {
        builder.with_equality_ids(Some(equality_ids.to_vec()));
    }

    // A file written under a declared sort order attests it in its footer;
    // carry that into the manifest so readers can trust the ordering without
    // opening the file.
    if let Some(sort_order_id) = attested_sort_order_id(file_metadata) {
        builder.with_sort_order_id(Some(sort_order_id));
    }

    let content = builder.build()?;
    Ok(content)
}

/// How a column chunk's statistics bytes encode the logical type.
///
/// Parquet's physical type can encode a logical type differently than the
/// Iceberg spec: INT32/INT64 stats are native little-endian (Decimal's spec
/// encoding is big-endian), and BYTE_ARRAY stats hold Uuid's UTF-8 string form
/// (spec: 16-byte big-endian integer).
fn physical_type_hint(column: &ColumnChunkMetaData) -> Option<PhysicalTypeHint> {
    match column.column_descr().physical_type() {
        parquet::basic::Type::INT32 | parquet::basic::Type::INT64 => {
            Some(PhysicalTypeHint::NativeLittleEndian)
        }
        parquet::basic::Type::BYTE_ARRAY => Some(PhysicalTypeHint::ByteArray),
        _ => None,
    }
}

/// Whether the chunk's statistics carry a min and a max, or prove that every
/// row of the row group is null in this column (so no bound is needed).
fn has_min_max_or_only_nulls(row_group: &RowGroupMetaData, column: &ColumnChunkMetaData) -> bool {
    column.statistics().is_some_and(|statistics| {
        (statistics.min_bytes_opt().is_some() && statistics.max_bytes_opt().is_some())
            || statistics
                .null_count_opt()
                .is_some_and(|nulls| nulls as i64 == row_group.num_rows())
    })
}

/// Derives one partition field's value from its source column's statistics.
///
/// Every row group must carry a min, a max and a null count for the source
/// column, except a row group in which the column is entirely null (null count
/// equal to its row count). The min and max of every row group must be exact
/// and transform to the same value, and nulls must not be mixed with values.
/// A column that is null in every row yields `None`. Any other case is an
/// `Error::InvalidFormat` naming the file and the column, because a value left
/// unset would read as NULL and be pruned wrongly.
fn derive_partition_value(
    location: &str,
    file_metadata: &ParquetMetaData,
    schema: &Schema,
    field: &BoundPartitionField<'_>,
) -> Result<Option<Value>, Error> {
    let source = field.source_name();
    let fail = |reason: String| {
        Err(Error::InvalidFormat(format!(
            "cannot derive the partition value of {location}: column {source} {reason}"
        )))
    };
    let column_index = file_metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|column| {
            schema
                .get_name(&column.path().parts().join("."))
                .is_some_and(|struct_field| struct_field.id == field.source_id())
        });
    let Some(column_index) = column_index else {
        return fail("has no statistics (missing statistics: it is not in the file)".to_owned());
    };

    let mut derived: Option<Value> = None;
    let mut null_count = 0i64;
    for (row_group_index, row_group) in file_metadata.row_groups().iter().enumerate() {
        let column = row_group.column(column_index);
        let statistics = column.statistics();
        let Some(nulls) = statistics.and_then(|statistics| statistics.null_count_opt()) else {
            return fail(format!(
                "has no statistics (missing statistics or null count in row group {row_group_index})"
            ));
        };
        null_count += nulls as i64;
        let min_max = statistics.and_then(|statistics| {
            statistics
                .min_bytes_opt()
                .zip(statistics.max_bytes_opt())
                .map(|min_max| (statistics, min_max))
        });
        let Some((statistics, (min_bytes, max_bytes))) = min_max else {
            if nulls as i64 == row_group.num_rows() {
                continue;
            }
            return fail(format!(
                "has no statistics (missing min/max in row group {row_group_index})"
            ));
        };
        if !(statistics.min_is_exact() && statistics.max_is_exact()) {
            return fail("statistics are truncated".to_owned());
        }
        let hint = physical_type_hint(column);
        let min = Value::try_from_bytes_with_hint(min_bytes, field.field_type(), hint)?
            .transform(field.transform())?;
        let max = Value::try_from_bytes_with_hint(max_bytes, field.field_type(), hint)?
            .transform(field.transform())?;
        if min != max || derived.as_ref().is_some_and(|current| *current != min) {
            return fail("spans several partitions".to_owned());
        }
        derived = Some(min);
    }
    if derived.is_some() && null_count > 0 {
        return fail("spans several partitions (it holds nulls and values)".to_owned());
    }
    Ok(derived)
}

/// Keeps the smaller (`keep == Ordering::Less`) or larger (`Ordering::Greater`)
/// of the stored bound and `new`. Values of different types cannot be
/// compared, so the column is marked and its bounds dropped later.
fn merge_bound(
    bounds: &mut HashMap<i32, Value>,
    conflicting: &mut HashSet<i32>,
    id: i32,
    new: Value,
    keep: Ordering,
) {
    match bounds.entry(id) {
        Entry::Vacant(entry) => {
            entry.insert(new);
        }
        Entry::Occupied(mut entry) => {
            if std::mem::discriminant(entry.get()) != std::mem::discriminant(&new) {
                conflicting.insert(id);
            } else if new.cmp(entry.get()) == keep {
                entry.insert(new);
            }
        }
    }
}

/// Get parquet metadata size
pub fn thrift_size<T: TSerializable>(metadata: &T) -> Result<usize, Error> {
    let mut buffer = TrackedWrite::new(Vec::<u8>::new());
    let mut protocol = TCompactOutputProtocol::new(&mut buffer);
    metadata.write_to_out_protocol(&mut protocol)?;
    Ok(buffer.bytes_written())
}

fn range_overlap<T: Ord + Sub + Copy>(
    old_range: &[&T; 2],
    new_range: &[&T; 2],
) -> <T as Sub>::Output {
    let overlap_start = (*old_range[0]).max(*new_range[0]);
    let overlap_end = (*old_range[1]).min(*new_range[1]);
    overlap_end - overlap_start
}

/// Helper trait to convert numeric types to f64 for statistical calculations.
///
/// This trait provides a uniform interface for converting integer types to f64,
/// which is necessary for the statistical estimation algorithms. The conversion
/// may be lossy for very large i64 values (beyond 2^53), but this is acceptable
/// for statistical approximations.
pub trait ToF64 {
    /// Converts the value to f64.
    ///
    /// # Note
    ///
    /// For i64 values larger than 2^53, precision may be lost in the conversion.
    /// This is acceptable for statistical calculations where exact precision is
    /// not required.
    fn to_f64(self) -> f64;
}

impl ToF64 for i32 {
    fn to_f64(self) -> f64 {
        self as f64
    }
}

impl ToF64 for i64 {
    fn to_f64(self) -> f64 {
        self as f64
    }
}

/// Estimates the number of new distinct values when merging two sets of statistics.
///
/// This function assumes uniform distribution of distinct values within their respective ranges
/// and uses an independence approximation to estimate overlap probability.
///
/// # Algorithm
///
/// The estimation is split into two parts:
/// 1. **Non-overlapping region**: All values in the new range that fall outside the old range
///    are guaranteed to be new.
/// 2. **Overlapping region**: Uses the independence approximation:
///    - P(specific value not covered) = ((R-1)/R)^k
///    - where R is the overlap size and k is the expected number of old values in the overlap
///    - Expected new values = n2_overlap × P(not covered)
///
/// # Parameters
///
/// * `old_range` - [min, max] of the existing value range
/// * `new_range` - [min, max] of the new value range
/// * `old_distinct_count` - Number of distinct values in the old range
/// * `new_distinct_count` - Number of distinct values in the new range
///
/// # Returns
///
/// Estimated number of new distinct values to add to the running total
///
/// # Example
///
/// ```ignore
/// // Old range [0, 1000] with 100 distinct values
/// // New range [500, 1500] with 50 distinct values
/// let new_count = estimate_distinct_count(&[&0, &1000], &[&500, &1500], 100, 50);
/// ```
pub fn estimate_distinct_count<T>(
    old_range: &[&T; 2],
    new_range: &[&T; 2],
    old_distinct_count: i64,
    new_distinct_count: i64,
) -> i64
where
    T: Ord + Sub<Output = T> + Copy + Default + ToF64,
{
    let new_range_size = (*new_range[1] - *new_range[0]).to_f64();
    let current_range_size = (*old_range[1] - *old_range[0]).to_f64();
    let overlap = range_overlap(old_range, new_range);
    let overlap_size: f64 = if overlap >= T::default() {
        overlap.to_f64()
    } else {
        0.0
    };
    let n2 = new_distinct_count as f64;
    let n1 = old_distinct_count as f64;

    // Values outside overlap are definitely new
    let outside_overlap = ((new_range_size - overlap_size) / new_range_size * n2).max(0.0);

    // For overlap region: estimate how many new values exist
    // using independence approximation: P(value not covered) = ((R-1)/R)^k
    // Expected new values in overlap = n2_overlap * ((R-1)/R)^(n1_overlap)
    let n2_overlap = (overlap_size / new_range_size * n2).max(0.0);
    let expected_n1_in_overlap = (overlap_size / current_range_size * n1).max(0.0);

    let new_in_overlap = if overlap_size > 0.0 {
        let prob_not_covered = ((overlap_size - 1.0) / overlap_size).powf(expected_n1_in_overlap);
        n2_overlap * prob_not_covered
    } else {
        0.0
    };

    (outside_overlap + new_in_overlap).round() as i64
}

#[cfg(test)]
mod metrics_mode_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use iceberg_rust_spec::partition::BoundPartitionField;
    use iceberg_rust_spec::spec::partition::{PartitionField, Transform};
    use iceberg_rust_spec::spec::schema::Schema;
    use iceberg_rust_spec::spec::types::{PrimitiveType, StructField, Type};
    use iceberg_rust_spec::spec::values::Value;
    use iceberg_rust_spec::table_metadata::{
        WRITE_METADATA_METRICS_COLUMN_PREFIX, WRITE_METADATA_METRICS_DEFAULT,
    };
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::ParquetMetaData;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    use super::parquet_to_datafile;

    /// A two-column table whose values are far longer than any sane bound.
    fn schema() -> Schema {
        Schema::builder()
            .with_struct_field(StructField {
                id: 0,
                name: "body".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::String),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .with_struct_field(StructField {
                id: 1,
                name: "trace_id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::String),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .build()
            .unwrap()
    }

    /// Writes one Parquet file holding `body`/`trace_id` and returns the
    /// `DataFile` built from it under the given table properties.
    fn datafile_with(
        properties: HashMap<String, String>,
    ) -> iceberg_rust_spec::spec::manifest::DataFile {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("body", DataType::Utf8, false),
            Field::new("trace_id", DataType::Utf8, false),
        ]));

        let bodies: ArrayRef = Arc::new(StringArray::from(vec![
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa- first log line",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz- last log line",
        ]));
        let trace_ids: ArrayRef = Arc::new(StringArray::from(vec![
            "00000000000000000000000000000001",
            "ffffffffffffffffffffffffffffffff",
        ]));
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![bodies, trace_ids]).unwrap();

        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, arrow_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let file_size = buffer.len() as u64;

        let reader = SerializedFileReader::new(bytes::Bytes::from(buffer)).unwrap();
        let parquet_metadata = reader.metadata().clone();

        parquet_to_datafile(
            "/t/data/1.parquet",
            file_size,
            &parquet_metadata,
            &schema(),
            &[],
            None,
            None,
            &properties,
        )
        .unwrap()
    }

    fn bound(bounds: &Option<HashMap<i32, Value>>, id: i32) -> Option<String> {
        match bounds.as_ref()?.get(&id)? {
            Value::String(string) => Some(string.clone()),
            other => panic!("expected a string bound, got {other:?}"),
        }
    }

    /// A table that says nothing must still not carry a full-length bound for
    /// every column: the spec's inferred default is `truncate(16)`.
    #[test]
    fn bounds_are_truncated_to_sixteen_by_default() {
        let datafile = datafile_with(HashMap::new());

        let lower = bound(datafile.lower_bounds(), 0).expect("a lower bound for body");
        let upper = bound(datafile.upper_bounds(), 0).expect("an upper bound for body");

        assert_eq!(lower.chars().count(), 16, "lower bound was not truncated");
        assert!(upper.chars().count() <= 16, "upper bound was not truncated");

        // Truncation must not break the bounds' meaning.
        assert!(lower.as_str() <= "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa- first log line");
        assert!(upper.as_str() >= "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz- last log line");
    }

    /// `none` on a column drops its bounds and counts, which is the point for a
    /// JSON blob or a log body that no query ever prunes on.
    #[test]
    fn a_column_set_to_none_carries_no_metrics() {
        let properties = HashMap::from([(
            format!("{WRITE_METADATA_METRICS_COLUMN_PREFIX}body"),
            "none".to_string(),
        )]);
        let datafile = datafile_with(properties);

        assert_eq!(bound(datafile.lower_bounds(), 0), None);
        assert_eq!(bound(datafile.upper_bounds(), 0), None);
        assert_eq!(datafile.value_counts().as_ref().unwrap().get(&0), None);

        // The other column is untouched.
        assert!(bound(datafile.lower_bounds(), 1).is_some());
        assert!(datafile.value_counts().as_ref().unwrap().get(&1).is_some());

        // Column sizes describe layout, not values, and are always present.
        assert!(datafile.column_sizes().as_ref().unwrap().get(&0).is_some());
    }

    /// `full` keeps the untruncated bound, for the columns worth it.
    #[test]
    fn a_column_set_to_full_keeps_untruncated_bounds() {
        let properties = HashMap::from([
            (
                WRITE_METADATA_METRICS_DEFAULT.to_string(),
                "counts".to_string(),
            ),
            (
                format!("{WRITE_METADATA_METRICS_COLUMN_PREFIX}trace_id"),
                "full".to_string(),
            ),
        ]);
        let datafile = datafile_with(properties);

        assert_eq!(
            bound(datafile.lower_bounds(), 1).as_deref(),
            Some("00000000000000000000000000000001")
        );
        // `counts` gives the other column counts but no bounds.
        assert_eq!(bound(datafile.lower_bounds(), 0), None);
        assert!(datafile.value_counts().as_ref().unwrap().get(&0).is_some());
    }

    /// One Utf8 column `s` (field id 1) holding `values`.
    fn string_file(values: Vec<Option<&str>>) -> (Schema, u64, ParquetMetaData) {
        let schema = Schema::builder()
            .with_struct_field(StructField::new(
                1,
                "s",
                false,
                Type::Primitive(PrimitiveType::String),
                None,
            ))
            .build()
            .unwrap();
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "s",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(StringArray::from(values))],
        )
        .unwrap();
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, arrow_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let size = buffer.len() as u64;
        let reader = SerializedFileReader::new(bytes::Bytes::from(buffer)).unwrap();
        (schema, size, reader.metadata().clone())
    }

    fn identity_on_s(schema: &Schema) -> (PartitionField, StructField) {
        let source = schema.fields().get(1).unwrap().clone();
        (
            PartitionField::new(1, 1000, "s", Transform::Identity),
            source,
        )
    }

    #[test]
    fn explicit_partition_values_are_used_as_given() {
        let long = "x".repeat(100);
        let (schema, size, metadata) = string_file(vec![Some(long.as_str())]);
        let (field, source) = identity_on_s(&schema);
        let bound = [BoundPartitionField::new(&field, &source)];
        let values = [Some(Value::String(long.clone()))];
        let datafile = parquet_to_datafile(
            "/t/data/1.parquet",
            size,
            &metadata,
            &schema,
            &bound,
            Some(&values),
            None,
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(
            datafile.partition().get("s"),
            Some(&Some(Value::String(long)))
        );
    }

    fn derive_err(schema: &Schema, size: u64, metadata: &ParquetMetaData) -> String {
        let (field, source) = identity_on_s(schema);
        let bound = [BoundPartitionField::new(&field, &source)];
        parquet_to_datafile(
            "/t/data/1.parquet",
            size,
            metadata,
            schema,
            &bound,
            None,
            None,
            &HashMap::new(),
        )
        .unwrap_err()
        .to_string()
    }

    /// Writes `groups` as separate row groups, optionally with statistics off.
    fn string_file_groups(
        groups: Vec<Vec<Option<&str>>>,
        stats: parquet::file::properties::EnabledStatistics,
    ) -> (Schema, u64, ParquetMetaData) {
        let (schema, _, _) = string_file(vec![Some("a")]);
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "s",
            DataType::Utf8,
            true,
        )]));
        let props = parquet::file::properties::WriterProperties::builder()
            .set_statistics_enabled(stats)
            .build();
        let mut buffer = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buffer, arrow_schema.clone(), Some(props)).unwrap();
        for group in groups {
            let batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![Arc::new(StringArray::from(group))],
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.flush().unwrap();
        }
        writer.close().unwrap();
        let size = buffer.len() as u64;
        let reader = SerializedFileReader::new(bytes::Bytes::from(buffer)).unwrap();
        (schema, size, reader.metadata().clone())
    }

    #[test]
    fn derived_partition_values_reject_different_row_groups() {
        use parquet::file::properties::EnabledStatistics;
        let (schema, size, metadata) = string_file_groups(
            vec![vec![Some("A")], vec![Some("B")]],
            EnabledStatistics::Chunk,
        );
        assert_eq!(metadata.num_row_groups(), 2);
        assert!(derive_err(&schema, size, &metadata).contains("spans several partitions"));
    }

    #[test]
    fn derived_partition_values_reject_a_partly_null_column() {
        let (schema, size, metadata) = string_file(vec![Some("x"), None]);
        assert!(derive_err(&schema, size, &metadata).contains("spans several partitions"));
    }

    #[test]
    fn derived_partition_values_reject_missing_statistics() {
        use parquet::file::properties::EnabledStatistics;
        let (schema, size, metadata) =
            string_file_groups(vec![vec![Some("x")]], EnabledStatistics::None);
        assert!(derive_err(&schema, size, &metadata).contains("has no statistics"));
    }

    #[test]
    fn explicit_partition_values_must_match_the_field_count() {
        let (schema, size, metadata) = string_file(vec![Some("x")]);
        let (field, source) = identity_on_s(&schema);
        let bound = [BoundPartitionField::new(&field, &source)];
        let err = parquet_to_datafile(
            "/t/data/1.parquet",
            size,
            &metadata,
            &schema,
            &bound,
            Some(&[]),
            None,
            &HashMap::new(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("partition values"));
    }

    #[test]
    fn derived_partition_values_reject_truncated_statistics() {
        let long = "x".repeat(100); // parquet truncates statistics to 64 bytes
        let (schema, size, metadata) = string_file(vec![Some(long.as_str())]);
        let (field, source) = identity_on_s(&schema);
        let bound = [BoundPartitionField::new(&field, &source)];
        let err = parquet_to_datafile(
            "/t/data/1.parquet",
            size,
            &metadata,
            &schema,
            &bound,
            None,
            None,
            &HashMap::new(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("truncated"), "{err}");
    }

    #[test]
    fn derived_partition_value_is_null_for_an_all_null_column() {
        let (schema, size, metadata) = string_file(vec![None, None]);
        let (field, source) = identity_on_s(&schema);
        let bound = [BoundPartitionField::new(&field, &source)];
        let datafile = parquet_to_datafile(
            "/t/data/1.parquet",
            size,
            &metadata,
            &schema,
            &bound,
            None,
            None,
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(datafile.partition().get("s"), Some(&None));
    }

    /// Writes each batch as its own row group; returns the data file.
    fn datafile_from_row_groups(
        schema: &Schema,
        batches: Vec<RecordBatch>,
    ) -> iceberg_rust_spec::spec::manifest::DataFile {
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, batches[0].schema(), None).unwrap();
        for batch in &batches {
            writer.write(batch).unwrap();
            writer.flush().unwrap();
        }
        writer.close().unwrap();
        let size = buffer.len() as u64;
        let reader = SerializedFileReader::new(bytes::Bytes::from(buffer)).unwrap();
        assert_eq!(reader.metadata().num_row_groups(), batches.len());
        parquet_to_datafile(
            "/t/data/1.parquet",
            size,
            reader.metadata(),
            schema,
            &[],
            None,
            None,
            &HashMap::new(),
        )
        .unwrap()
    }

    /// One nullable column `c` (field id 1) with one row group per array.
    fn one_column(
        ty: PrimitiveType,
        arrays: Vec<ArrayRef>,
    ) -> iceberg_rust_spec::spec::manifest::DataFile {
        let schema = Schema::builder()
            .with_struct_field(StructField::new(1, "c", false, Type::Primitive(ty), None))
            .build()
            .unwrap();
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "c",
            arrays[0].data_type().clone(),
            true,
        )]));
        let batches = arrays
            .into_iter()
            .map(|array| RecordBatch::try_new(arrow_schema.clone(), vec![array]).unwrap())
            .collect();
        datafile_from_row_groups(&schema, batches)
    }

    fn column_bounds(
        datafile: &iceberg_rust_spec::spec::manifest::DataFile,
    ) -> (Option<Value>, Option<Value>) {
        (
            datafile
                .lower_bounds()
                .as_ref()
                .and_then(|b| b.get(&1).cloned()),
            datafile
                .upper_bounds()
                .as_ref()
                .and_then(|b| b.get(&1).cloned()),
        )
    }

    #[test]
    fn bounds_cover_every_row_group_for_every_type() {
        use arrow::array::{BinaryArray, BooleanArray, Decimal128Array};
        use iceberg_rust_spec::spec::decimal::decimal_from_i128_with_scale;

        let strings = one_column(
            PrimitiveType::String,
            vec![
                Arc::new(StringArray::from(vec!["m", "z"])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        );
        assert_eq!(
            column_bounds(&strings),
            (
                Some(Value::String("a".into())),
                Some(Value::String("z".into()))
            )
        );

        let decimal = |v: Vec<i128>| -> ArrayRef {
            Arc::new(
                Decimal128Array::from(v)
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
            )
        };
        let decimals = one_column(
            PrimitiveType::Decimal {
                precision: 9,
                scale: 2,
            },
            vec![decimal(vec![500, 900]), decimal(vec![100, 200])],
        );
        assert_eq!(
            column_bounds(&decimals),
            (
                Some(Value::Decimal(
                    decimal_from_i128_with_scale(100, 2).unwrap()
                )),
                Some(Value::Decimal(
                    decimal_from_i128_with_scale(900, 2).unwrap()
                ))
            )
        );

        let binaries = one_column(
            PrimitiveType::Binary,
            vec![
                Arc::new(BinaryArray::from(vec![&[5u8][..], &[9u8][..]])),
                Arc::new(BinaryArray::from(vec![&[1u8][..], &[2u8][..]])),
            ],
        );
        assert_eq!(
            column_bounds(&binaries),
            (Some(Value::Binary(vec![1])), Some(Value::Binary(vec![9])))
        );

        let booleans = one_column(
            PrimitiveType::Boolean,
            vec![
                Arc::new(BooleanArray::from(vec![true, true])),
                Arc::new(BooleanArray::from(vec![false, false])),
            ],
        );
        assert_eq!(
            column_bounds(&booleans),
            (Some(Value::Boolean(false)), Some(Value::Boolean(true)))
        );
    }

    #[test]
    fn long_string_bounds_still_cover_every_row_group() {
        let high = format!("z{}", "y".repeat(100));
        let low = format!("a{}", "b".repeat(100));
        let datafile = one_column(
            PrimitiveType::String,
            vec![
                Arc::new(StringArray::from(vec![high.as_str()])),
                Arc::new(StringArray::from(vec![low.as_str()])),
            ],
        );
        let (Some(Value::String(lower)), Some(Value::String(upper))) = column_bounds(&datafile)
        else {
            panic!("expected string bounds");
        };
        assert!(
            lower.as_str() <= low.as_str(),
            "lower bound {lower} above {low}"
        );
        assert!(
            upper.as_str() >= high.as_str(),
            "upper bound {upper} below {high}"
        );
    }
    /// One Int64 column `c` (field id 1), one row group per entry of `groups`.
    fn long_file(groups: Vec<Vec<Option<i64>>>) -> (Schema, u64, ParquetMetaData) {
        use arrow::array::Int64Array;
        let schema = Schema::builder()
            .with_struct_field(StructField::new(
                1,
                "c",
                false,
                Type::Primitive(PrimitiveType::Long),
                None,
            ))
            .build()
            .unwrap();
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "c",
            DataType::Int64,
            true,
        )]));
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, arrow_schema.clone(), None).unwrap();
        for group in groups {
            let batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![Arc::new(Int64Array::from(group))],
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.flush().unwrap();
        }
        writer.close().unwrap();
        let size = buffer.len() as u64;
        let reader = SerializedFileReader::new(bytes::Bytes::from(buffer)).unwrap();
        (schema, size, reader.metadata().clone())
    }

    /// Derives the partition of a `long_file` under `transforms`, all on `c`.
    fn derive_on_c(
        transforms: &[(&str, Transform)],
        groups: Vec<Vec<Option<i64>>>,
    ) -> Result<Vec<Option<Value>>, crate::error::Error> {
        let (schema, size, metadata) = long_file(groups);
        let source = schema.fields().get(1).unwrap().clone();
        let fields = transforms
            .iter()
            .enumerate()
            .map(|(i, (name, transform))| {
                PartitionField::new(1, 1000 + i as i32, name, transform.clone())
            })
            .collect::<Vec<_>>();
        let bound = fields
            .iter()
            .map(|field| BoundPartitionField::new(field, &source))
            .collect::<Vec<_>>();
        let datafile = parquet_to_datafile(
            "/t/data/1.parquet",
            size,
            &metadata,
            &schema,
            &bound,
            None,
            None,
            &HashMap::new(),
        )?;
        Ok(transforms
            .iter()
            .map(|(name, _)| datafile.partition().get(name).cloned().flatten())
            .collect())
    }

    #[test]
    fn every_partition_field_on_a_shared_source_column_is_derived() {
        let values = derive_on_c(
            &[
                ("c", Transform::Identity),
                ("c_trunc", Transform::Truncate(10)),
            ],
            vec![vec![Some(42), Some(42)]],
        )
        .unwrap();
        assert_eq!(
            values,
            vec![Some(Value::LongInt(42)), Some(Value::LongInt(40))]
        );

        let values = derive_on_c(
            &[
                ("c_trunc", Transform::Truncate(10)),
                ("c", Transform::Identity),
            ],
            vec![vec![Some(42), Some(42)]],
        )
        .unwrap();
        assert_eq!(
            values,
            vec![Some(Value::LongInt(40)), Some(Value::LongInt(42))]
        );
    }

    #[test]
    fn a_shared_source_field_that_cannot_be_derived_is_an_error() {
        // truncate(10) agrees on 40, identity spans 41..47: in either order
        // the identity field cannot be derived.
        for transforms in [
            [
                ("c", Transform::Identity),
                ("c_trunc", Transform::Truncate(10)),
            ],
            [
                ("c_trunc", Transform::Truncate(10)),
                ("c", Transform::Identity),
            ],
        ] {
            let err = derive_on_c(&transforms, vec![vec![Some(41), Some(47)]]).unwrap_err();
            assert!(
                err.to_string().contains("spans several partitions"),
                "{err}"
            );
        }
    }

    /// Rewrites row group `index`'s only column chunk with `rewrite`.
    fn rewrite_column_chunk(
        metadata: ParquetMetaData,
        index: usize,
        rewrite: impl Fn(
            parquet::file::metadata::ColumnChunkMetaDataBuilder,
        ) -> parquet::file::metadata::ColumnChunkMetaDataBuilder,
    ) -> ParquetMetaData {
        let mut builder = metadata.into_builder();
        let row_groups = builder
            .take_row_groups()
            .into_iter()
            .enumerate()
            .map(|(i, row_group)| {
                if i != index {
                    return row_group;
                }
                let columns = row_group
                    .columns()
                    .iter()
                    .map(|column| rewrite(column.clone().into_builder()).build().unwrap())
                    .collect();
                row_group
                    .into_builder()
                    .set_column_metadata(columns)
                    .build()
                    .unwrap()
            })
            .collect();
        builder.set_row_groups(row_groups).build()
    }

    #[test]
    fn derived_partition_values_reject_a_row_group_without_statistics() {
        use parquet::file::properties::EnabledStatistics;
        let (schema, size, metadata) = string_file_groups(
            vec![vec![Some("x")], vec![Some("y")]],
            EnabledStatistics::Chunk,
        );
        let metadata = rewrite_column_chunk(metadata, 1, |column| column.clear_statistics());
        let err = derive_err(&schema, size, &metadata);
        assert!(err.contains("has no statistics"), "{err}");
    }

    #[test]
    fn derived_partition_values_reject_a_missing_null_count() {
        use parquet::data_type::ByteArray;
        use parquet::file::statistics::Statistics;
        let (schema, size, metadata) = string_file(vec![Some("x"), None]);
        let metadata = rewrite_column_chunk(metadata, 0, |column| {
            column.set_statistics(Statistics::byte_array(
                Some(ByteArray::from("x")),
                Some(ByteArray::from("x")),
                None,
                None,
                false,
            ))
        });
        let err = derive_err(&schema, size, &metadata);
        assert!(err.contains("has no statistics"), "{err}");
    }

    #[test]
    fn bounds_are_dropped_when_a_row_group_with_values_has_no_statistics() {
        let (schema, size, metadata) = long_file(vec![vec![Some(5), Some(9)], vec![Some(1)]]);
        let metadata = rewrite_column_chunk(metadata, 1, |column| column.clear_statistics());
        let datafile = parquet_to_datafile(
            "/t/data/1.parquet",
            size,
            &metadata,
            &schema,
            &[],
            None,
            None,
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(column_bounds(&datafile), (None, None));
    }

    #[test]
    fn bounds_survive_an_all_null_row_group() {
        let (schema, size, metadata) = long_file(vec![vec![Some(5), Some(9)], vec![None, None]]);
        let datafile = parquet_to_datafile(
            "/t/data/1.parquet",
            size,
            &metadata,
            &schema,
            &[],
            None,
            None,
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(
            column_bounds(&datafile),
            (Some(Value::LongInt(5)), Some(Value::LongInt(9)))
        );
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    // -- Parquet end-to-end + schema util + read projection + writers (8+10+5+7+3=33) --
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
    #[ignore = "TestParquet (8), TestParquetSchemaUtil (10), TestParquetReadProjection (5), TestParquetDataWriter (7), TestParquetDeleteWriters (3): direct-Parquet writer + reader + schema-bridge + delete-writer surface"]
    fn test_parquet_writer_reader_scenarios(#[case] _scenario: usize) {
        unimplemented!("Parquet writer/reader suite");
    }

    // -- Parquet page version + CDH parquet statistics (9+1=10) --
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
    #[ignore = "TestParquetPageVersion + TestCDHParquetStatistics: page-version and CDH-stats edge cases"]
    fn test_parquet_page_version_scenarios(#[case] _scenario: usize) {
        unimplemented!("Parquet page version + CDH stats");
    }

    // -- Row group filters: bloom (50) + dictionary (38) + metrics (41) + metrics-types (1) = 130 --
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
    #[case(67)]
    #[case(68)]
    #[case(69)]
    #[case(70)]
    #[case(71)]
    #[case(72)]
    #[case(73)]
    #[case(74)]
    #[case(75)]
    #[case(76)]
    #[case(77)]
    #[case(78)]
    #[case(79)]
    #[case(80)]
    #[case(81)]
    #[case(82)]
    #[case(83)]
    #[case(84)]
    #[case(85)]
    #[case(86)]
    #[case(87)]
    #[case(88)]
    #[case(89)]
    #[case(90)]
    #[case(91)]
    #[case(92)]
    #[case(93)]
    #[case(94)]
    #[case(95)]
    #[case(96)]
    #[case(97)]
    #[case(98)]
    #[case(99)]
    #[case(100)]
    #[case(101)]
    #[case(102)]
    #[case(103)]
    #[case(104)]
    #[case(105)]
    #[case(106)]
    #[case(107)]
    #[case(108)]
    #[case(109)]
    #[case(110)]
    #[case(111)]
    #[case(112)]
    #[case(113)]
    #[case(114)]
    #[case(115)]
    #[case(116)]
    #[case(117)]
    #[case(118)]
    #[case(119)]
    #[case(120)]
    #[case(121)]
    #[case(122)]
    #[case(123)]
    #[case(124)]
    #[case(125)]
    #[case(126)]
    #[case(127)]
    #[case(128)]
    #[case(129)]
    #[case(130)]
    #[ignore = "no row-group filter pushdown surface (BloomRowGroupFilter, DictionaryRowGroupFilter, MetricsRowGroupFilter + Types)"]
    fn test_parquet_row_group_filters_scenarios(#[case] _scenario: usize) {
        unimplemented!("Parquet row-group filters");
    }

    // -- TestParquetEncryption + WriteSupport + InputStreamAdapter (3+1+2=6) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[ignore = "no Parquet encryption + range-readable input stream adapter"]
    fn test_parquet_encryption_scenarios(#[case] _scenario: usize) {
        unimplemented!("Parquet encryption");
    }
}
