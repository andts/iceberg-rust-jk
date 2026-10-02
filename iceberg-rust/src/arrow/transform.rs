//! Arrow kernels for the Iceberg partition transforms.
//!
//! The arithmetic lives in [`iceberg_rust_spec::spec::transform`]; these
//! kernels only map it over Arrow arrays, so they always agree with
//! [`iceberg_rust_spec::spec::values::Value::transform`]. Nulls stay null.

use std::sync::Arc;

use arrow::{
    array::{
        new_null_array, Array, ArrayRef, AsArray, BinaryArray, BinaryViewArray, Int32Array,
        LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
    },
    compute::cast,
    datatypes::{
        DataType, Date32Type, Decimal128Type, Int32Type, Int64Type, Time64MicrosecondType,
        TimeUnit, TimestampMicrosecondType, TimestampNanosecondType,
    },
    error::ArrowError,
};
use iceberg_rust_spec::spec::{
    partition::Transform,
    transform as t,
    types::{PrimitiveType, Type},
};
use uuid::Uuid;

/// Applies `transform` to `array`, whose values have Iceberg type `source_type`.
///
/// Results: Int32 for bucket and the temporal transforms; the source type for
/// identity and truncate (Int8/Int16 are widened to Int32, Iceberg's `int`);
/// a null array of the source type for void.
///
/// # Errors
/// A transform/type pair the spec does not allow, a width of 0, or an
/// unparseable uuid string.
pub fn transform_arrow(
    array: ArrayRef,
    transform: &Transform,
    source_type: &Type,
) -> Result<ArrayRef, ArrowError> {
    if matches!(transform, Transform::Bucket(0) | Transform::Truncate(0)) {
        return Err(ArrowError::InvalidArgumentError(format!(
            "{transform} needs a width greater than zero"
        )));
    }
    if let Transform::Void = transform {
        return Ok(new_null_array(array.data_type(), array.len()));
    }
    let array = match array.data_type() {
        DataType::Int8 | DataType::Int16 => cast(&array, &DataType::Int32)?,
        _ => array,
    };
    let is_uuid = matches!(source_type, Type::Primitive(PrimitiveType::Uuid));
    let result = match transform {
        Transform::Identity => Some(array.clone()),
        Transform::Bucket(n) => bucket_hashes(&array, is_uuid)?
            .map(|hashes| Arc::new(hashes.unary::<_, Int32Type>(|h| t::bucket(h, *n))) as ArrayRef),
        Transform::Truncate(width) if !is_uuid => truncate(&array, *width)?,
        Transform::Year | Transform::Month | Transform::Day | Transform::Hour => {
            temporal(&array, transform)
        }
        Transform::Truncate(_) | Transform::Void => None,
    };
    result.ok_or_else(|| {
        ArrowError::ComputeError(format!(
            "{transform} transform is not supported for Iceberg {source_type} as Arrow {}",
            array.data_type()
        ))
    })
}

/// Murmur3 hashes per the spec, or `None` if the type cannot be bucketed.
fn bucket_hashes(array: &ArrayRef, is_uuid: bool) -> Result<Option<Int32Array>, ArrowError> {
    let hashes = match array.data_type() {
        DataType::Int32 => array.as_primitive::<Int32Type>().unary(t::hash_int),
        DataType::Int64 => array.as_primitive::<Int64Type>().unary(t::hash_long),
        DataType::Date32 => array.as_primitive::<Date32Type>().unary(t::hash_int),
        DataType::Time64(TimeUnit::Microsecond) => array
            .as_primitive::<Time64MicrosecondType>()
            .unary(t::hash_long),
        DataType::Timestamp(TimeUnit::Microsecond, _) => array
            .as_primitive::<TimestampMicrosecondType>()
            .unary(t::hash_long),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => array
            .as_primitive::<TimestampNanosecondType>()
            .unary(t::hash_timestamp_nanos),
        DataType::Decimal128(_, _) => array
            .as_primitive::<Decimal128Type>()
            .unary(t::hash_decimal),
        DataType::Utf8 if is_uuid => array
            .as_string::<i32>()
            .iter()
            .map(|value| {
                value
                    .map(|text| Uuid::parse_str(text).map(|uuid| t::hash_uuid(&uuid)))
                    .transpose()
            })
            .collect::<Result<Int32Array, _>>()
            .map_err(|err| ArrowError::ComputeError(format!("invalid uuid: {err}")))?,
        DataType::Utf8 => array
            .as_string::<i32>()
            .iter()
            .map(|v| v.map(t::hash_str))
            .collect(),
        DataType::LargeUtf8 => array
            .as_string::<i64>()
            .iter()
            .map(|v| v.map(t::hash_str))
            .collect(),
        DataType::Utf8View => array
            .as_string_view()
            .iter()
            .map(|v| v.map(t::hash_str))
            .collect(),
        DataType::Binary => array
            .as_binary::<i32>()
            .iter()
            .map(|v| v.map(t::hash_bytes))
            .collect(),
        DataType::LargeBinary => array
            .as_binary::<i64>()
            .iter()
            .map(|v| v.map(t::hash_bytes))
            .collect(),
        DataType::BinaryView => array
            .as_binary_view()
            .iter()
            .map(|v| v.map(t::hash_bytes))
            .collect(),
        DataType::FixedSizeBinary(_) => array
            .as_fixed_size_binary()
            .iter()
            .map(|v| v.map(t::hash_bytes))
            .collect(),
        _ => return Ok(None),
    };
    Ok(Some(hashes))
}

/// Truncation per the spec, or `None` if the type cannot be truncated.
fn truncate(array: &ArrayRef, width: u32) -> Result<Option<ArrayRef>, ArrowError> {
    let result: ArrayRef = match array.data_type() {
        DataType::Int32 => Arc::new(
            array
                .as_primitive::<Int32Type>()
                .unary::<_, Int32Type>(|v| t::truncate_int(v, width)),
        ),
        DataType::Int64 => Arc::new(
            array
                .as_primitive::<Int64Type>()
                .unary::<_, Int64Type>(|v| t::truncate_long(v, width)),
        ),
        DataType::Decimal128(precision, scale) => Arc::new(
            array
                .as_primitive::<Decimal128Type>()
                .unary::<_, Decimal128Type>(|v| t::truncate_decimal(v, width))
                .with_precision_and_scale(*precision, *scale)?,
        ),
        DataType::Utf8 => Arc::new(
            array
                .as_string::<i32>()
                .iter()
                .map(|v| v.map(|s| t::truncate_str(s, width)))
                .collect::<StringArray>(),
        ),
        DataType::LargeUtf8 => Arc::new(
            array
                .as_string::<i64>()
                .iter()
                .map(|v| v.map(|s| t::truncate_str(s, width)))
                .collect::<LargeStringArray>(),
        ),
        DataType::Utf8View => Arc::new(
            array
                .as_string_view()
                .iter()
                .map(|v| v.map(|s| t::truncate_str(s, width)))
                .collect::<StringViewArray>(),
        ),
        DataType::Binary => Arc::new(
            array
                .as_binary::<i32>()
                .iter()
                .map(|v| v.map(|b| t::truncate_bytes(b, width)))
                .collect::<BinaryArray>(),
        ),
        DataType::LargeBinary => Arc::new(
            array
                .as_binary::<i64>()
                .iter()
                .map(|v| v.map(|b| t::truncate_bytes(b, width)))
                .collect::<LargeBinaryArray>(),
        ),
        DataType::BinaryView => Arc::new(
            array
                .as_binary_view()
                .iter()
                .map(|v| v.map(|b| t::truncate_bytes(b, width)))
                .collect::<BinaryViewArray>(),
        ),
        _ => return Ok(None),
    };
    Ok(Some(result))
}

/// Year / month / day / hour per the spec, or `None` if not applicable.
fn temporal(array: &ArrayRef, transform: &Transform) -> Option<ArrayRef> {
    let dates = || array.as_primitive::<Date32Type>();
    let micros = || array.as_primitive::<TimestampMicrosecondType>();
    let nanos = || array.as_primitive::<TimestampNanosecondType>();
    let result: Int32Array = match (array.data_type(), transform) {
        (DataType::Date32, Transform::Year) => dates().unary(t::days_to_years),
        (DataType::Date32, Transform::Month) => dates().unary(t::days_to_months),
        (DataType::Date32, Transform::Day) => dates().unary(|days| days),
        (DataType::Timestamp(TimeUnit::Microsecond, _), Transform::Year) => {
            micros().unary(t::micros_to_years)
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), Transform::Month) => {
            micros().unary(t::micros_to_months)
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), Transform::Day) => {
            micros().unary(t::micros_to_days)
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), Transform::Hour) => {
            micros().unary(t::micros_to_hours)
        }
        (DataType::Timestamp(TimeUnit::Nanosecond, _), Transform::Year) => {
            nanos().unary(t::nanos_to_years)
        }
        (DataType::Timestamp(TimeUnit::Nanosecond, _), Transform::Month) => {
            nanos().unary(t::nanos_to_months)
        }
        (DataType::Timestamp(TimeUnit::Nanosecond, _), Transform::Day) => {
            nanos().unary(t::nanos_to_days)
        }
        (DataType::Timestamp(TimeUnit::Nanosecond, _), Transform::Hour) => {
            nanos().unary(t::nanos_to_hours)
        }
        _ => return None,
    };
    Some(Arc::new(result))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
        Int32Array, Int64Array, StringArray, StringViewArray, Time64MicrosecondArray,
        TimestampMicrosecondArray, TimestampNanosecondArray,
    };
    use iceberg_rust_spec::spec::{
        transform::{bucket, hash_timestamp_nanos},
        types::{PrimitiveType, Type},
    };

    use super::*;
    use crate::arrow::value::arrow_value;

    fn primitive(ty: PrimitiveType) -> Type {
        Type::Primitive(ty)
    }

    /// `transform_arrow` must agree with `Value::transform` on every row.
    fn assert_parity(array: ArrayRef, source_type: Type, transforms: &[Transform]) {
        for transform in transforms {
            let result = transform_arrow(array.clone(), transform, &source_type)
                .unwrap_or_else(|err| panic!("{transform} on {source_type}: {err}"));
            assert_eq!(result.len(), array.len());
            let result_type = source_type.tranform(transform).unwrap();
            for row in 0..array.len() {
                let expected = arrow_value(array.as_ref(), row, &source_type)
                    .unwrap()
                    .map(|value| value.transform(transform).unwrap());
                let actual = arrow_value(result.as_ref(), row, &result_type).unwrap();
                assert_eq!(actual, expected, "{transform} on {source_type}, row {row}");
            }
        }
    }

    #[test]
    fn arrow_and_value_transforms_agree() {
        let temporal = [
            Transform::Year,
            Transform::Month,
            Transform::Day,
            Transform::Hour,
        ];
        assert_parity(
            Arc::new(Int32Array::from(vec![
                Some(34),
                Some(-1),
                Some(i32::MIN),
                None,
            ])),
            primitive(PrimitiveType::Int),
            &[
                Transform::Identity,
                Transform::Bucket(10),
                Transform::Truncate(10),
            ],
        );
        assert_parity(
            Arc::new(Int64Array::from(vec![
                Some(34),
                Some(-1),
                Some(i64::MIN),
                None,
            ])),
            primitive(PrimitiveType::Long),
            &[
                Transform::Identity,
                Transform::Bucket(10),
                Transform::Truncate(10),
            ],
        );
        assert_parity(
            Arc::new(Date32Array::from(vec![
                Some(17486),
                Some(-1),
                Some(0),
                None,
            ])),
            primitive(PrimitiveType::Date),
            &[
                Transform::Identity,
                Transform::Bucket(10),
                Transform::Year,
                Transform::Month,
                Transform::Day,
            ],
        );
        assert_parity(
            Arc::new(Time64MicrosecondArray::from(vec![
                Some(81_068_000_000),
                None,
            ])),
            primitive(PrimitiveType::Time),
            &[Transform::Identity, Transform::Bucket(10)],
        );
        let micros = vec![Some(1_510_871_468_000_000), Some(-1), Some(0), None];
        assert_parity(
            Arc::new(TimestampMicrosecondArray::from(micros.clone())),
            primitive(PrimitiveType::Timestamp),
            &[
                &[Transform::Identity, Transform::Bucket(10)][..],
                &temporal[..],
            ]
            .concat(),
        );
        assert_parity(
            Arc::new(TimestampMicrosecondArray::from(micros).with_timezone_utc()),
            primitive(PrimitiveType::Timestamptz),
            &[
                &[Transform::Identity, Transform::Bucket(10)][..],
                &temporal[..],
            ]
            .concat(),
        );
        let strings = vec![Some("iceberg"), Some("éé"), None];
        let string_transforms = [
            Transform::Identity,
            Transform::Bucket(10),
            Transform::Truncate(1),
        ];
        assert_parity(
            Arc::new(StringArray::from(strings.clone())),
            primitive(PrimitiveType::String),
            &string_transforms,
        );
        assert_parity(
            Arc::new(StringViewArray::from(strings)),
            primitive(PrimitiveType::String),
            &string_transforms,
        );
        assert_parity(
            Arc::new(StringArray::from(vec![
                Some("f79c3e09-677c-4bbd-a479-3f349cb785e7"),
                None,
            ])),
            primitive(PrimitiveType::Uuid),
            &[Transform::Identity, Transform::Bucket(10)],
        );
        assert_parity(
            Arc::new(BinaryArray::from(vec![Some(&[0u8, 1, 2, 3][..]), None])),
            primitive(PrimitiveType::Binary),
            &[
                Transform::Identity,
                Transform::Bucket(10),
                Transform::Truncate(2),
            ],
        );
        assert_parity(
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    vec![Some(vec![0u8, 1, 2, 3]), None].into_iter(),
                    4,
                )
                .unwrap(),
            ),
            primitive(PrimitiveType::Fixed(4)),
            &[Transform::Identity, Transform::Bucket(10)],
        );
        assert_parity(
            Arc::new(
                Decimal128Array::from(vec![Some(1420), Some(-1), None])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
            ),
            primitive(PrimitiveType::Decimal {
                precision: 9,
                scale: 2,
            }),
            &[
                Transform::Identity,
                Transform::Bucket(10),
                Transform::Truncate(50),
            ],
        );
    }

    #[test]
    fn nanosecond_timestamps_hash_and_floor_as_micros() {
        let nanos: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            Some(1_510_871_468_000_001_001),
            Some(-1),
        ]));
        let ty = primitive(PrimitiveType::TimestampNs);
        let buckets = transform_arrow(nanos.clone(), &Transform::Bucket(10), &ty).unwrap();
        assert_eq!(
            buckets.as_primitive::<Int32Type>().value(0),
            bucket(hash_timestamp_nanos(1_510_871_468_000_001_001), 10)
        );
        let days = transform_arrow(nanos, &Transform::Day, &ty).unwrap();
        assert_eq!(days.as_primitive::<Int32Type>().value(1), -1);
    }

    #[test]
    fn string_bucket_keeps_nulls() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![None, Some("a")]));
        let result = transform_arrow(
            strings,
            &Transform::Bucket(4),
            &primitive(PrimitiveType::String),
        )
        .unwrap();
        assert!(result.is_null(0));
        assert!(result.is_valid(1));
    }

    #[test]
    fn uuid_is_bucketed_by_its_bytes_not_its_text() {
        let text = "f79c3e09-677c-4bbd-a479-3f349cb785e7";
        let array: ArrayRef = Arc::new(StringArray::from(vec![text]));
        let as_uuid = transform_arrow(
            array.clone(),
            &Transform::Bucket(1000),
            &primitive(PrimitiveType::Uuid),
        )
        .unwrap();
        let as_string = transform_arrow(
            array,
            &Transform::Bucket(1000),
            &primitive(PrimitiveType::String),
        )
        .unwrap();
        assert_eq!(
            as_uuid.as_primitive::<Int32Type>().value(0),
            bucket(1488055340, 1000)
        );
        assert_ne!(
            as_uuid.as_primitive::<Int32Type>().value(0),
            as_string.as_primitive::<Int32Type>().value(0)
        );
        let bad: ArrayRef = Arc::new(StringArray::from(vec!["not-a-uuid"]));
        assert!(
            transform_arrow(bad, &Transform::Bucket(4), &primitive(PrimitiveType::Uuid)).is_err()
        );
    }

    #[test]
    fn void_returns_nulls_of_the_source_type() {
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let result =
            transform_arrow(ints, &Transform::Void, &primitive(PrimitiveType::Long)).unwrap();
        assert_eq!(result.data_type(), &DataType::Int64);
        assert_eq!(result.null_count(), 2);
    }

    #[test]
    fn rejects_zero_width_and_unsupported_pairs() {
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        let int = primitive(PrimitiveType::Int);
        assert!(transform_arrow(ints.clone(), &Transform::Bucket(0), &int).is_err());
        assert!(transform_arrow(ints.clone(), &Transform::Truncate(0), &int).is_err());
        assert!(transform_arrow(ints, &Transform::Month, &int).is_err());
        let bools: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
        assert!(transform_arrow(
            bools,
            &Transform::Bucket(4),
            &primitive(PrimitiveType::Boolean)
        )
        .is_err());
    }
}
