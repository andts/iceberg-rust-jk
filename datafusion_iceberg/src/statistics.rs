use datafusion::{
    common::stats::Precision,
    physical_plan::{ColumnStatistics, Statistics},
    scalar::ScalarValue,
};
use iceberg_rust::error::Error;
use iceberg_rust::file_format::parquet::estimate_distinct_count;
use iceberg_rust::spec::{
    manifest::{ManifestEntry, Status},
    schema::Schema,
    types::{PrimitiveType, Type},
    values::Value,
};

pub(crate) fn statistics_from_datafiles(
    schema: &Schema,
    datafiles: &[(String, ManifestEntry)],
) -> Statistics {
    datafiles
        .iter()
        .filter(|(_, manifest)| !matches!(manifest.status(), Status::Deleted))
        .map(|(_, manifest)| {
            let column_stats = column_statistics(schema, manifest);
            Statistics {
                num_rows: Precision::Exact(*manifest.data_file().record_count() as usize),
                total_byte_size: Precision::Exact(
                    *manifest.data_file().file_size_in_bytes() as usize
                ),
                column_statistics: column_stats
                    .into_iter()
                    .map(|x| ColumnStatistics {
                        null_count: x.null_count,
                        max_value: x.max_value,
                        min_value: x.min_value,
                        distinct_count: x.distinct_count,
                        sum_value: x.sum_value,
                        byte_size: x.byte_size,
                    })
                    .collect(),
            }
        })
        .reduce(|acc, x| Statistics {
            num_rows: acc.num_rows.add(&x.num_rows),
            total_byte_size: acc.total_byte_size.add(&x.total_byte_size),
            column_statistics: acc
                .column_statistics
                .into_iter()
                .zip(x.column_statistics)
                .map(|(acc, x)| {
                    let new_distinct_count = new_distinct_count(&acc, &x);

                    ColumnStatistics {
                        null_count: acc.null_count.add(&x.null_count),
                        max_value: acc.max_value.max(&x.max_value),
                        min_value: acc.min_value.min(&x.min_value),
                        distinct_count: new_distinct_count,
                        sum_value: acc.sum_value.add(&x.sum_value),
                        byte_size: acc.byte_size.add(&x.byte_size),
                    }
                })
                .collect(),
        })
        .unwrap_or_default()
}

fn column_statistics<'a>(
    schema: &'a Schema,
    manifest: &'a ManifestEntry,
) -> impl Iterator<Item = ColumnStatistics> + 'a {
    schema.fields().iter().map(|field| {
        let id = field.id;
        let field_type = &field.field_type;
        let data_file = &manifest.data_file();
        ColumnStatistics {
            null_count: data_file
                .null_value_counts()
                .as_ref()
                .and_then(|x| x.get(&id))
                .map(|x| Precision::Exact(*x as usize))
                .unwrap_or(Precision::Absent),
            max_value: data_file
                .upper_bounds()
                .as_ref()
                .and_then(|x| x.get(&id))
                .and_then(|x| {
                    Some(Precision::Exact(
                        convert_value_to_scalar_value(x.clone(), field_type).ok()?,
                    ))
                })
                .unwrap_or(Precision::Absent),
            min_value: data_file
                .lower_bounds()
                .as_ref()
                .and_then(|x| x.get(&id))
                .and_then(|x| {
                    Some(Precision::Exact(
                        convert_value_to_scalar_value(x.clone(), field_type).ok()?,
                    ))
                })
                .unwrap_or(Precision::Absent),
            distinct_count: Precision::Absent,
            sum_value: Precision::Absent,
            byte_size: Precision::Absent,
        }
    })
}

pub(crate) fn manifest_statistics(schema: &Schema, manifest: &ManifestEntry) -> Statistics {
    Statistics {
        num_rows: Precision::Exact(*manifest.data_file().record_count() as usize),
        total_byte_size: Precision::Exact(*manifest.data_file().file_size_in_bytes() as usize),
        column_statistics: column_statistics(schema, manifest).collect(),
    }
}

fn convert_value_to_scalar_value(value: Value, field_type: &Type) -> Result<ScalarValue, Error> {
    match value {
        Value::Boolean(b) => Ok(ScalarValue::Boolean(Some(b))),
        Value::Int(i) => Ok(ScalarValue::Int32(Some(i))),
        Value::LongInt(l) => Ok(ScalarValue::Int64(Some(l))),
        Value::Float(f) => Ok(ScalarValue::Float32(Some(f.0))),
        Value::Double(d) => Ok(ScalarValue::Float64(Some(d.0))),
        Value::Date(d) => Ok(ScalarValue::Date32(Some(d))),
        Value::Time(t) => Ok(ScalarValue::Time64Microsecond(Some(t))),
        Value::Timestamp(ts) => Ok(ScalarValue::TimestampMicrosecond(Some(ts), None)),
        Value::TimestampTZ(ts) => Ok(ScalarValue::TimestampMicrosecond(Some(ts), None)),
        Value::String(s) => Ok(ScalarValue::Utf8(Some(s))),
        Value::UUID(u) => Ok(ScalarValue::FixedSizeBinary(
            16,
            Some(u.into_bytes().into()),
        )),
        Value::Fixed(size, data) => Ok(ScalarValue::FixedSizeBinary(size as i32, Some(data))),
        Value::Binary(data) => Ok(ScalarValue::Binary(Some(data))),
        Value::Decimal(mut decimal) => {
            // Arrow requires a Decimal128 precision in [1, 38]; the declared
            // column type carries the precision/scale, so use those rather than
            // hardcoding (0, 0) (which makes the scalar impossible to
            // materialize into an array). The value's unscaled integer
            // representation (`mantissa`) must be read at the declared scale.
            let (precision, scale) = match field_type {
                Type::Primitive(PrimitiveType::Decimal { precision, scale }) => {
                    (*precision as u8, *scale as i8)
                }
                _ => {
                    return Err(Error::Conversion(
                        "Iceberg decimal value".to_string(),
                        format!("non-decimal field type {field_type:?}"),
                    ));
                }
            };
            decimal.rescale(scale as u32);
            Ok(ScalarValue::Decimal128(
                Some(decimal.mantissa()),
                precision,
                scale,
            ))
        }
        x => Err(Error::Conversion(
            "Iceberg value".to_string(),
            format!("{x:?}"),
        )),
    }
}

fn new_distinct_count(acc: &ColumnStatistics, x: &ColumnStatistics) -> Precision<usize> {
    match (
        &acc.distinct_count,
        &x.distinct_count,
        &acc.min_value,
        &acc.max_value,
        &x.min_value,
        &x.max_value,
    ) {
        (
            Precision::Exact(old_count),
            Precision::Exact(new_count),
            Precision::Exact(ScalarValue::Int32(Some(old_min))),
            Precision::Exact(ScalarValue::Int32(Some(old_max))),
            Precision::Exact(ScalarValue::Int32(Some(new_min))),
            Precision::Exact(ScalarValue::Int32(Some(new_max))),
        ) => {
            let estimated = estimate_distinct_count(
                &[old_min, old_max],
                &[new_min, new_max],
                *old_count as i64,
                *new_count as i64,
            );
            Precision::Inexact(*old_count + estimated as usize)
        }
        (
            Precision::Exact(old_count),
            Precision::Exact(new_count),
            Precision::Exact(ScalarValue::Int64(Some(old_min))),
            Precision::Exact(ScalarValue::Int64(Some(old_max))),
            Precision::Exact(ScalarValue::Int64(Some(new_min))),
            Precision::Exact(ScalarValue::Int64(Some(new_max))),
        ) => {
            let estimated = estimate_distinct_count(
                &[old_min, old_max],
                &[new_min, new_max],
                *old_count as i64,
                *new_count as i64,
            );
            Precision::Inexact(*old_count + estimated as usize)
        }
        (Precision::Absent, Precision::Exact(_), _, _, _, _) => x.distinct_count,
        _ => acc.distinct_count.add(&x.distinct_count),
    }
}

#[cfg(test)]
mod tests {
    use super::convert_value_to_scalar_value;
    use datafusion::scalar::ScalarValue;
    use iceberg_rust::spec::{
        types::{PrimitiveType, Type},
        values::Value,
    };

    #[test]
    fn decimal_stat_preserves_value_precision_and_scale() {
        let decimal_type = Type::Primitive(PrimitiveType::Decimal {
            precision: 15,
            scale: 2,
        });
        // 104899.50 as it appears in a manifest bound: the unscaled i128
        // (10_489_950) encoded big-endian.
        let bytes = 10_489_950_i128.to_be_bytes();
        let value = Value::try_from_bytes(&bytes, &decimal_type).unwrap();

        let scalar = convert_value_to_scalar_value(value, &decimal_type).unwrap();

        assert_eq!(scalar, ScalarValue::Decimal128(Some(10_489_950), 15, 2));
    }
}
