/*!
 * Implement pruning statistics for Datafusion table
 *
 * Pruning is done on two levels:
 *
 * 1. Prune manifests based on information in manifests lists
 * 2. Prune data files based on information in manifests
 *
 * For the first level the trait [`PruningStatistics`] is implemented for the DataFusionTable. It returns the pruning information for the manifest files
 * and not the final data files.
 *
 * For the second level the trait PruningStatistics is implemented for the Manifest
*/

use std::any::Any;

use datafusion::{
    arrow::{
        array::ArrayRef,
        datatypes::{DataType, Schema as ArrowSchema},
    },
    common::pruning::PruningStatistics,
    common::DataFusionError,
    prelude::Column,
    scalar::ScalarValue,
};
use iceberg_rust::{
    spec::{
        decimal::{decimal_mantissa, decimal_scale, Decimal},
        manifest::ManifestEntry,
        manifest_list::ManifestListEntry,
        partition::BoundPartitionField,
        schema::Schema,
        values::Value,
    },
    table::ManifestPath,
};

pub(crate) struct PruneManifests<'table, 'manifests> {
    partition_fields: &'table [BoundPartitionField<'table>],
    partition_spec_id: i32,
    files: &'manifests [ManifestListEntry],
}

impl<'table, 'manifests> PruneManifests<'table, 'manifests> {
    pub(crate) fn new(
        partition_fields: &'table [BoundPartitionField<'table>],
        partition_spec_id: i32,
        files: &'manifests [ManifestListEntry],
    ) -> Self {
        Self {
            partition_fields,
            partition_spec_id,
            files,
        }
    }
}

impl PruningStatistics for PruneManifests<'_, '_> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let (index, partition_field) = self
            .partition_fields
            .iter()
            .enumerate()
            .find(|(_, field)| field.name() == column.name())?;
        let data_type = partition_field
            .field_type()
            .tranform(partition_field.transform())
            .ok()?;
        let min_values = self.files.iter().map(|manifest| {
            (manifest.partition_spec_id == self.partition_spec_id)
                .then_some(manifest)
                .and_then(|manifest| manifest.partitions.as_ref())
                .and_then(|partitions| partitions.get(index))
                .and_then(|partition| partition.lower_bound.as_ref())
                .and_then(|min| min.clone().cast(&data_type).ok())
                .map(Value::into_any)
        });
        any_iter_to_array(min_values, &(&data_type).try_into().ok()?).ok()
    }
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let (index, partition_field) = self
            .partition_fields
            .iter()
            .enumerate()
            .find(|(_, field)| field.name() == column.name())?;
        let data_type = partition_field
            .field_type()
            .tranform(partition_field.transform())
            .ok()?;
        let max_values = self.files.iter().map(|manifest| {
            (manifest.partition_spec_id == self.partition_spec_id)
                .then_some(manifest)
                .and_then(|manifest| manifest.partitions.as_ref())
                .and_then(|partitions| partitions.get(index))
                .and_then(|partition| partition.upper_bound.as_ref())
                .and_then(|max| max.clone().cast(&data_type).ok())
                .map(Value::into_any)
        });
        any_iter_to_array(max_values, &(&data_type).try_into().ok()?).ok()
    }

    fn num_containers(&self) -> usize {
        self.files.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let (index, _) = self
            .partition_fields
            .iter()
            .enumerate()
            .find(|(_, field)| field.name() == column.name())?;
        let contains_null = self.files.iter().map(|manifest| {
            (manifest.partition_spec_id == self.partition_spec_id)
                .then_some(manifest)
                .and_then(|manifest| manifest.partitions.as_ref())
                .and_then(|partitions| partitions.get(index))
                .and_then(|partition| (!partition.contains_null).then_some(0))
        });
        ScalarValue::iter_to_array(contains_null.map(ScalarValue::Int32)).ok()
    }
    fn contained(
        &self,
        _column: &Column,
        _values: &std::collections::HashSet<ScalarValue>,
    ) -> Option<datafusion::arrow::array::BooleanArray> {
        None
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        let row_counts =
            self.files
                .iter()
                .map(|x| match (x.added_rows_count, x.existing_rows_count) {
                    (Some(a), Some(e)) => Some(a + e),
                    _ => None,
                });
        ScalarValue::iter_to_array(row_counts.map(ScalarValue::Int64)).ok()
    }
}

pub(crate) struct PruneDataFiles<'table, 'manifests> {
    schema: &'table Schema,
    arrow_schema: &'table ArrowSchema,
    files: &'manifests [(ManifestPath, ManifestEntry)],
}

impl<'table, 'manifests> PruneDataFiles<'table, 'manifests> {
    pub(crate) fn new(
        schema: &'table Schema,
        arrow_schema: &'table ArrowSchema,
        files: &'manifests [(ManifestPath, ManifestEntry)],
    ) -> Self {
        Self {
            schema,
            arrow_schema,
            files,
        }
    }
}

impl PruningStatistics for PruneDataFiles<'_, '_> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let field = self.schema.fields().get_name(&column.name)?;
        let column_id = field.id;
        let datatype = self
            .arrow_schema
            .field_with_name(&column.name)
            .ok()?
            .data_type();
        let min_values =
            self.files
                .iter()
                .map(|manifest| match &manifest.1.data_file().lower_bounds() {
                    Some(map) => map.get(&column_id).and_then(|value| {
                        value
                            .clone()
                            .cast(&field.field_type)
                            .ok()
                            .map(Value::into_any)
                    }),
                    None => None,
                });
        any_iter_to_array(min_values, datatype).ok()
    }
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let field = self.schema.fields().get_name(&column.name)?;
        let column_id = field.id;
        let datatype = self
            .arrow_schema
            .field_with_name(&column.name)
            .ok()?
            .data_type();
        let max_values =
            self.files
                .iter()
                .map(|manifest| match &manifest.1.data_file().upper_bounds() {
                    Some(map) => map.get(&column_id).and_then(|value| {
                        value
                            .clone()
                            .cast(&field.field_type)
                            .ok()
                            .map(Value::into_any)
                    }),
                    None => None,
                });
        any_iter_to_array(max_values, datatype).ok()
    }
    fn num_containers(&self) -> usize {
        self.files.len()
    }
    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.schema.fields().get_name(&column.name)?.id;
        let null_counts =
            self.files.iter().map(
                |manifest| match &manifest.1.data_file().null_value_counts() {
                    Some(map) => map.get(&{ column_id }).copied(),
                    None => None,
                },
            );
        ScalarValue::iter_to_array(null_counts.map(ScalarValue::Int64)).ok()
    }
    fn contained(
        &self,
        _column: &Column,
        _values: &std::collections::HashSet<ScalarValue>,
    ) -> Option<datafusion::arrow::array::BooleanArray> {
        None
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        let row_counts = self
            .files
            .iter()
            .map(|manifest| Some(*manifest.1.data_file().record_count()));
        ScalarValue::iter_to_array(row_counts.map(ScalarValue::Int64)).ok()
    }
}

fn any_iter_to_array(
    iter: impl Iterator<Item = Option<Box<dyn Any>>>,
    datatype: &DataType,
) -> Result<ArrayRef, DataFusionError> {
    match datatype {
        DataType::Boolean => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::Boolean(opt.and_then(|value| Some(*value.downcast::<bool>().ok()?)))
        })),
        DataType::Int32 => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::Int32(opt.and_then(|value| Some(*value.downcast::<i32>().ok()?)))
        })),
        DataType::Int64 => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::Int64(opt.and_then(|value| Some(*value.downcast::<i64>().ok()?)))
        })),
        DataType::Float32 => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::Float32(opt.and_then(|value| Some(*value.downcast::<f32>().ok()?)))
        })),
        DataType::Float64 => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::Float64(opt.and_then(|value| Some(*value.downcast::<f64>().ok()?)))
        })),
        DataType::Date32 => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::Date32(opt.and_then(|value| Some(*value.downcast::<i32>().ok()?)))
        })),
        DataType::Date64 => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::Date64(opt.and_then(|value| Some(*value.downcast::<i64>().ok()?)))
        })),
        DataType::Time64(_) => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::Time64Microsecond(
                opt.and_then(|value| Some(*value.downcast::<i64>().ok()?)),
            )
        })),
        DataType::Timestamp(_, tz) => {
            // Make sure to preserve the column's timezone for the sake of comparisons.
            ScalarValue::iter_to_array(iter.map(move |opt| {
                ScalarValue::TimestampMicrosecond(
                    opt.and_then(|value| Some(*value.downcast::<i64>().ok()?)),
                    tz.clone(),
                )
            }))
        }
        DataType::Utf8 => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::Utf8(opt.and_then(|value| Some(*value.downcast::<String>().ok()?)))
        })),
        DataType::Utf8View => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::Utf8View(opt.and_then(|value| Some(*value.downcast::<String>().ok()?)))
        })),
        DataType::FixedSizeBinary(_) => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::Binary(opt.and_then(|value| Some(*value.downcast::<Vec<u8>>().ok()?)))
        })),
        DataType::Binary => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::Binary(opt.and_then(|value| Some(*value.downcast::<Vec<u8>>().ok()?)))
        })),
        // Only prune when the stored scale matches the column's scale.
        DataType::Decimal128(precision, scale) => {
            let (precision, scale) = (*precision, *scale);
            ScalarValue::iter_to_array(iter.map(move |opt| {
                ScalarValue::Decimal128(
                    opt.and_then(|value| {
                        let d = *value.downcast::<Decimal>().ok()?;
                        if decimal_scale(&d) == scale as u32 {
                            decimal_mantissa(&d).ok()
                        } else {
                            None
                        }
                    }),
                    precision,
                    scale,
                )
            }))
        }
        _ => Err(DataFusionError::Internal(
            "Arrow datatype not supported for pruning.".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Array, Date32Array, Decimal128Array, Int64Array, TimestampMicrosecondArray,
    };
    use datafusion::arrow::datatypes::{Field, TimeUnit};
    use iceberg_rust::spec::decimal::decimal_from_i128_with_scale;
    use iceberg_rust::spec::partition::Transform;
    use iceberg_rust::spec::{
        manifest::{Content, DataFile, FileFormat, Status},
        manifest_list::{Content as ManifestContent, FieldSummary},
        partition::PartitionField,
        table_metadata::FormatVersion,
        types::{PrimitiveType, StructField, StructType, Type},
        values::Struct,
    };

    #[test]
    fn manifest_pruning_does_not_compare_different_partition_specs() {
        let source = StructField::new(2, "b", false, Type::Primitive(PrimitiveType::Long), None);
        let partition = PartitionField::new(2, 1000, "b", Transform::Identity);
        let fields = [BoundPartitionField::new(&partition, &source)];
        let entry = |spec_id, lower: Value| ManifestListEntry {
            format_version: FormatVersion::V2,
            manifest_path: format!("/{spec_id}.avro"),
            manifest_length: 1,
            partition_spec_id: spec_id,
            content: ManifestContent::Data,
            sequence_number: 1,
            min_sequence_number: 1,
            added_snapshot_id: 1,
            added_files_count: Some(1),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(1),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: Some(vec![FieldSummary {
                contains_null: false,
                contains_nan: None,
                lower_bound: Some(lower.clone()),
                upper_bound: Some(lower),
            }]),
            key_metadata: None,
            first_row_id: None,
        };
        let manifests = vec![
            entry(0, Value::Int(1000)),
            entry(1, Value::LongInt(3)),
            entry(1, Value::Int(-42)),
        ];
        let pruning = PruneManifests::new(&fields, 1, &manifests);
        let minimums = pruning.min_values(&Column::from_name("b")).unwrap();
        let minimums = minimums.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(minimums.len(), 3);
        assert!(minimums.is_null(0));
        assert_eq!(minimums.value(1), 3);
        assert_eq!(minimums.value(2), -42);
        let maximums = pruning.max_values(&Column::from_name("b")).unwrap();
        let maximums = maximums.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(maximums.is_null(0));
        assert_eq!(maximums.value(2), -42);
        let null_counts = pruning.null_counts(&Column::from_name("b")).unwrap();
        assert!(null_counts.is_null(0));
    }

    #[test]
    fn data_file_pruning_promotes_old_numeric_bounds_and_keeps_unknowns() {
        let schema = Schema::from_struct_type(
            StructType::new(vec![StructField::new(
                1,
                "id",
                false,
                Type::Primitive(PrimitiveType::Long),
                None,
            )]),
            1,
            None,
        );
        let arrow_schema = ArrowSchema::new(vec![Field::new("id", DataType::Int64, true)]);
        let entry = |lower: Value| {
            let file = DataFile::builder()
                .with_content(Content::Data)
                .with_file_path("/data.parquet".into())
                .with_file_format(FileFormat::Parquet)
                .with_partition(Struct::from_iter(Vec::<(String, Option<Value>)>::new()))
                .with_record_count(1)
                .with_file_size_in_bytes(1)
                .with_column_sizes(None)
                .with_value_counts(None)
                .with_null_value_counts(None)
                .with_nan_value_counts(None)
                .with_distinct_counts(None)
                .with_lower_bounds(Some(std::collections::HashMap::from([(1, lower)])))
                .with_upper_bounds(None)
                .build()
                .unwrap();
            ManifestEntry::builder()
                .with_format_version(FormatVersion::V2)
                .with_status(Status::Added)
                .with_data_file(file)
                .build()
                .unwrap()
        };
        let files = vec![
            ("old".into(), entry(Value::Int(-42))),
            ("unknown".into(), entry(Value::String("invalid".into()))),
        ];
        let pruning = PruneDataFiles::new(&schema, &arrow_schema, &files);
        let min_values = pruning.min_values(&Column::from_name("id")).unwrap();
        let min_values = min_values.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(min_values.len(), 2);
        assert_eq!(min_values.value(0), -42);
        assert!(min_values.is_null(1));
    }

    // 2024-03-15T10:30:00Z in microseconds since epoch
    const TS_MICROS: i64 = 1_710_498_600_000_000;

    #[test]
    fn any_iter_to_array_date32() {
        let iter = vec![Some(Value::Date(19797).into_any()), None].into_iter();
        let array = any_iter_to_array(iter, &DataType::Date32).unwrap();
        let dates = array.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(dates.value(0), 19797);
        assert!(dates.is_null(1));
    }

    #[test]
    fn any_iter_to_array_decimal128() {
        let iter = vec![
            Some(Value::Decimal(decimal_from_i128_with_scale(12345, 2).unwrap()).into_any()),
            None,
        ]
        .into_iter();
        let array = any_iter_to_array(iter, &DataType::Decimal128(10, 2)).unwrap();
        let dec = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(dec.value(0), 12345);
        assert!(dec.is_null(1));
        assert_eq!(dec.precision(), 10);
        assert_eq!(dec.scale(), 2);
    }

    #[test]
    fn any_iter_to_array_decimal128_scale_mismatch_is_null() {
        // Stored scale (2) != column scale (4): emit null rather than misread the mantissa.
        let iter = std::iter::once(Some(
            Value::Decimal(decimal_from_i128_with_scale(12345, 2).unwrap()).into_any(),
        ));
        let array = any_iter_to_array(iter, &DataType::Decimal128(10, 4)).unwrap();
        let dec = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert!(dec.is_null(0));
    }

    #[test]
    fn any_iter_to_array_preserves_timezone() {
        for val in [Value::Timestamp(TS_MICROS), Value::TimestampTZ(TS_MICROS)] {
            let iter = vec![Some(val.into_any()), None].into_iter();
            let dt = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));

            let array = any_iter_to_array(iter, &dt).unwrap();

            assert_eq!(array.data_type(), &dt);
            let ts = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            assert_eq!(ts.value(0), TS_MICROS);
            assert!(ts.is_null(1));
        }
    }

    /// Prunes one manifest whose partition summary is `[transform(source), transform(source)]`
    /// with `filter`, the way the scan does. Returns whether the manifest is kept.
    fn manifest_kept(
        source_type: PrimitiveType,
        transform: Transform,
        source: Value,
        filter: datafusion_expr::Expr,
    ) -> bool {
        use datafusion::{
            execution::context::ExecutionProps, physical_expr::create_physical_expr,
            physical_optimizer::pruning::PruningPredicateBuilder,
        };
        let source_field = StructField::new(2, "c", false, Type::Primitive(source_type), None);
        let partition = PartitionField::new(2, 1000, "c_part", transform.clone());
        let fields = [BoundPartitionField::new(&partition, &source_field)];
        let part_type = source_field.field_type.tranform(&transform).unwrap();
        let arrow_type: DataType = (&part_type).try_into().unwrap();
        let schema = std::sync::Arc::new(ArrowSchema::new(vec![Field::new(
            "c_part", arrow_type, true,
        )]));
        let bound = source.transform(&transform).unwrap();
        let manifests = vec![ManifestListEntry {
            format_version: FormatVersion::V2,
            manifest_path: "/m.avro".into(),
            manifest_length: 1,
            partition_spec_id: 0,
            content: ManifestContent::Data,
            sequence_number: 1,
            min_sequence_number: 1,
            added_snapshot_id: 1,
            added_files_count: Some(1),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(1),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: Some(vec![FieldSummary {
                contains_null: false,
                contains_nan: None,
                lower_bound: Some(bound.clone()),
                upper_bound: Some(bound),
            }]),
            key_metadata: None,
            first_row_id: None,
        }];
        let projected = crate::partition_projection::project(&filter, &fields, &schema)
            .unwrap_or_else(|| panic!("`{filter}` on {transform:?} was not projected"));
        let physical = create_physical_expr(
            &projected,
            &schema.as_ref().clone().try_into().unwrap(),
            &ExecutionProps::new(),
            &Default::default(),
        )
        .unwrap();
        let predicate = PruningPredicateBuilder::new()
            .with_file_schema(schema.clone())
            .try_build(physical)
            .unwrap();
        let keep = predicate
            .prune(&PruneManifests::new(&fields, 0, &manifests))
            .unwrap();
        assert_eq!(keep.len(), 1);
        keep[0]
    }

    fn check_prunes(
        source_type: PrimitiveType,
        transform: Transform,
        source: Value,
        matching: datafusion_expr::Expr,
        non_matching: datafusion_expr::Expr,
    ) {
        assert!(
            manifest_kept(
                source_type.clone(),
                transform.clone(),
                source.clone(),
                matching.clone()
            ),
            "{transform:?}: `{matching}` must keep the manifest"
        );
        assert!(
            !manifest_kept(source_type, transform.clone(), source, non_matching.clone()),
            "{transform:?}: `{non_matching}` must prune the manifest"
        );
    }

    #[test]
    fn manifests_are_pruned_by_projected_partition_filters() {
        use datafusion_expr::{col, lit};
        let ts = |micros| lit(ScalarValue::TimestampMicrosecond(Some(micros), None));
        let date = |days| lit(ScalarValue::Date32(Some(days)));
        // 2023-05-15 12:00:00 and 2023-05-16 00:00:00 in microseconds
        let ts_value = 1_684_152_000_000_000_i64;
        let next_day = 1_684_195_200_000_000_i64;
        // 2023-05-15 as days since epoch
        let day_value = 19_492;

        check_prunes(
            PrimitiveType::Long,
            Transform::Identity,
            Value::LongInt(15),
            col("c").eq(lit(15_i64)),
            col("c").eq(lit(16_i64)),
        );
        check_prunes(
            PrimitiveType::Timestamp,
            Transform::Day,
            Value::Timestamp(ts_value),
            col("c").gt(ts(ts_value - 7_200_000_000)),
            col("c").gt(ts(next_day)),
        );
        let bucket_of = |v: i64| Value::LongInt(v).transform(&Transform::Bucket(4)).unwrap();
        let other = (16..).find(|v| bucket_of(*v) != bucket_of(15)).unwrap();
        check_prunes(
            PrimitiveType::Long,
            Transform::Bucket(4),
            Value::LongInt(15),
            col("c").eq(lit(15_i64)),
            col("c").eq(lit(other)),
        );
        check_prunes(
            PrimitiveType::String,
            Transform::Truncate(2),
            Value::String("abcdef".into()),
            col("c").eq(lit("abcdef")),
            col("c").eq(lit("xyz")),
        );
        check_prunes(
            PrimitiveType::Date,
            Transform::Month,
            Value::Date(day_value),
            col("c").gt_eq(date(day_value - 14)),
            col("c").gt_eq(date(day_value + 17)),
        );
    }
}
