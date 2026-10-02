//! Conversions between Iceberg values and DataFusion scalars of a given column type.

use datafusion::{arrow::datatypes::DataType, common::DataFusionError, scalar::ScalarValue};
use iceberg_rust::spec::{
    decimal::{decimal_mantissa, decimal_scale},
    values::Value,
};

/// `value` as a scalar of `data_type` (a typed null for `None`).
pub(crate) fn value_to_scalar(
    value: Option<&Value>,
    data_type: &DataType,
) -> Result<ScalarValue, DataFusionError> {
    let Some(value) = value else {
        return ScalarValue::try_from(data_type);
    };
    let scalar = match value {
        Value::Boolean(v) => ScalarValue::Boolean(Some(*v)),
        Value::Int(v) => ScalarValue::Int32(Some(*v)),
        Value::LongInt(v) => ScalarValue::Int64(Some(*v)),
        Value::Float(v) => ScalarValue::Float32(Some(v.into_inner())),
        Value::Double(v) => ScalarValue::Float64(Some(v.into_inner())),
        Value::Date(v) => ScalarValue::Date32(Some(*v)),
        Value::Time(v) => ScalarValue::Time64Microsecond(Some(*v)),
        Value::Timestamp(v) => ScalarValue::TimestampMicrosecond(Some(*v), None),
        Value::TimestampTZ(v) => ScalarValue::TimestampMicrosecond(Some(*v), Some("UTC".into())),
        Value::String(v) => ScalarValue::Utf8(Some(v.clone())),
        Value::UUID(v) => ScalarValue::Utf8(Some(v.to_string())),
        Value::Fixed(len, v) => ScalarValue::FixedSizeBinary(*len as i32, Some(v.clone())),
        Value::Binary(v) => ScalarValue::Binary(Some(v.clone())),
        Value::Decimal(v) => ScalarValue::Decimal128(
            Some(decimal_mantissa(v).map_err(|err| DataFusionError::External(Box::new(err)))?),
            38,
            decimal_scale(v) as i8,
        ),
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "partition value {other:?}"
            )))
        }
    };
    scalar.cast_to(data_type)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    use datafusion::scalar::ScalarValue;
    use iceberg_rust::spec::{decimal::decimal_from_i128_with_scale, values::Value};
    use uuid::Uuid;

    use super::*;

    #[test]
    fn converts_to_the_column_type() {
        let decimal = Value::Decimal(decimal_from_i128_with_scale(1050, 2).unwrap());
        assert_eq!(
            value_to_scalar(Some(&decimal), &DataType::Decimal128(9, 2)).unwrap(),
            ScalarValue::Decimal128(Some(1050), 9, 2)
        );
        let uuid = Uuid::parse_str("f79c3e09-677c-4bbd-a479-3f349cb785e7").unwrap();
        assert_eq!(
            value_to_scalar(Some(&Value::UUID(uuid)), &DataType::Utf8).unwrap(),
            ScalarValue::Utf8(Some(uuid.to_string()))
        );
        assert_eq!(
            value_to_scalar(Some(&Value::String("ab".into())), &DataType::Utf8View).unwrap(),
            ScalarValue::Utf8View(Some("ab".into()))
        );
        assert_eq!(
            value_to_scalar(None, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap(),
            ScalarValue::TimestampMicrosecond(None, None)
        );
    }
}
