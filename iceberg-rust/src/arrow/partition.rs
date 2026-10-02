//! Splitting Arrow record batches by Iceberg partition tuple.
//!
//! Rows are grouped by their transformed partition columns using Arrow's
//! row format, so every type groups the same way and NULL is an ordinary
//! partition value.

use std::collections::{HashMap, HashSet};

use arrow::{
    array::{as_string_array, ArrayRef, UInt32Array},
    compute::{filter, kernels::cmp::distinct, take_record_batch},
    error::ArrowError,
    record_batch::RecordBatch,
    row::{RowConverter, SortField},
};
use iceberg_rust_spec::{partition::BoundPartitionField, spec::values::Value};

use super::{transform::transform_arrow, value::arrow_value};

/// A partition tuple (`None` for a null value) and the rows that belong to it.
type Partition = (Vec<Option<Value>>, RecordBatch);

/// Splits `record_batch` into one batch per distinct partition tuple.
///
/// Each item is the partition tuple (one entry per partition field, `None`
/// for a null value) and the rows that belong to it. Partitions appear in
/// order of their first row.
///
/// # Errors
/// A missing source column, an unsupported transform, or an Arrow failure.
pub fn partition_record_batch<'a>(
    record_batch: &'a RecordBatch,
    partition_fields: &[BoundPartitionField<'_>],
) -> Result<impl Iterator<Item = Result<Partition, ArrowError>> + 'a, ArrowError> {
    let columns: Vec<ArrayRef> = partition_fields
        .iter()
        .map(|field| {
            let source = record_batch
                .column_by_name(field.source_name())
                .ok_or_else(|| {
                    ArrowError::SchemaError(format!(
                        "partition source column {} is missing",
                        field.source_name()
                    ))
                })?;
            transform_arrow(source.clone(), field.transform(), field.field_type())
        })
        .collect::<Result<_, _>>()?;
    let result_types = partition_fields
        .iter()
        .map(|field| {
            field
                .field_type()
                .tranform(field.transform())
                .map_err(|err| ArrowError::ComputeError(err.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let converter = RowConverter::new(
        columns
            .iter()
            .map(|column| SortField::new(column.data_type().clone()))
            .collect(),
    )?;
    let rows = converter.convert_columns(&columns)?;

    let mut groups: Vec<Vec<u32>> = Vec::new();
    let mut group_by_key = HashMap::new();
    for (index, row) in rows.iter().enumerate() {
        let group = *group_by_key.entry(row).or_insert_with(|| {
            groups.push(Vec::new());
            groups.len() - 1
        });
        groups[group].push(index as u32);
    }

    let partitions = groups
        .into_iter()
        .map(|indices| {
            let first = indices[0] as usize;
            let values = columns
                .iter()
                .zip(&result_types)
                .map(|(column, ty)| arrow_value(column.as_ref(), first, ty))
                .collect::<Result<Vec<_>, _>>()?;
            Ok((values, UInt32Array::from(indices)))
        })
        .collect::<Result<Vec<_>, ArrowError>>()?;

    Ok(partitions
        .into_iter()
        .map(move |(values, indices)| Ok((values, take_record_batch(record_batch, &indices)?))))
}

/// Extracts distinct string values from an Arrow array into a HashSet
///
/// # Arguments
/// * `array` - The Arrow array to extract distinct values from
///
/// # Returns
/// A HashSet containing all unique string values from the array
pub fn distinct_values_string(array: ArrayRef) -> Result<HashSet<String>, ArrowError> {
    let slice_len = array.len() - 1;

    let array = as_string_array(&array);

    let first = array.value(0).to_owned();

    if slice_len == 0 {
        return Ok(HashSet::from_iter([first]));
    }

    let v1 = array.slice(0, slice_len);
    let v2 = array.slice(1, slice_len);

    // Which consecutive entries are different
    let mask = distinct(&v1, &v2)?;

    let unique = filter(&v2, &mask)?;

    let unique = as_string_array(&unique);

    let set = unique
        .iter()
        .fold(HashSet::from_iter([first]), |mut acc, x| {
            if let Some(x) = x {
                acc.insert(x.to_owned());
            }
            acc
        });
    Ok(set)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{AsArray, Date32Array, Int64Array, RecordBatch},
        datatypes::{DataType, Field, Int64Type, Schema},
    };
    use iceberg_rust_spec::spec::{
        partition::{BoundPartitionField, PartitionField, Transform},
        types::{PrimitiveType, StructField, Type},
        values::Value,
    };

    use super::*;

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("d", DataType::Date32, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(Date32Array::from(vec![
                    Some(19478),
                    None,
                    Some(19478),
                    Some(0),
                ])),
            ],
        )
        .unwrap()
    }

    /// (partition tuple, ids in that partition), ordered by first id.
    fn partitions(
        batch: &RecordBatch,
        transform: Transform,
    ) -> Vec<(Vec<Option<Value>>, Vec<i64>)> {
        let source = StructField::new(2, "d", false, Type::Primitive(PrimitiveType::Date), None);
        let field = PartitionField::new(2, 1000, "d_part", transform);
        let bound = [BoundPartitionField::new(&field, &source)];
        let mut result: Vec<_> = partition_record_batch(batch, &bound)
            .unwrap()
            .map(|partition| {
                let (values, rows) = partition.unwrap();
                (
                    values,
                    rows.column(0).as_primitive::<Int64Type>().values().to_vec(),
                )
            })
            .collect();
        result.sort_by_key(|(_, ids)| ids[0]);
        result
    }

    #[test]
    fn null_partition_values_form_their_own_partition() {
        assert_eq!(
            partitions(&batch(), Transform::Identity),
            vec![
                (vec![Some(Value::Date(19478))], vec![1, 3]),
                (vec![None], vec![2]),
                (vec![Some(Value::Date(0))], vec![4]),
            ]
        );
    }

    #[test]
    fn transformed_partition_values_use_the_result_type() {
        assert_eq!(
            partitions(&batch(), Transform::Month),
            vec![
                (vec![Some(Value::Int(640))], vec![1, 3]),
                (vec![None], vec![2]),
                (vec![Some(Value::Int(0))], vec![4]),
            ]
        );
    }

    #[test]
    fn empty_batch_has_no_partitions() {
        assert!(partitions(&batch().slice(0, 0), Transform::Identity).is_empty());
    }
}
