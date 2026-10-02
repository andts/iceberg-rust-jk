//! Conversions between Iceberg values and DataFusion scalars of a given column type.

use datafusion::{arrow::datatypes::DataType, common::DataFusionError, scalar::ScalarValue};
use iceberg_rust::spec::{
    decimal::{decimal_from_i128_with_scale, decimal_mantissa, decimal_scale},
    types::{PrimitiveType, Type},
    values::Value,
};
use ordered_float::OrderedFloat;
use uuid::Uuid;

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

/// `scalar` as an Iceberg value of type `ty`; `None` if null or not convertible.
pub(crate) fn scalar_to_value(scalar: &ScalarValue, ty: &Type) -> Option<Value> {
    let Type::Primitive(primitive) = ty else {
        return None;
    };
    Some(match (scalar, primitive) {
        (ScalarValue::Boolean(Some(v)), PrimitiveType::Boolean) => Value::Boolean(*v),
        (ScalarValue::Int32(Some(v)), PrimitiveType::Int) => Value::Int(*v),
        (ScalarValue::Int64(Some(v)), PrimitiveType::Long) => Value::LongInt(*v),
        (ScalarValue::Float32(Some(v)), PrimitiveType::Float) => Value::Float(OrderedFloat(*v)),
        (ScalarValue::Float64(Some(v)), PrimitiveType::Double) => Value::Double(OrderedFloat(*v)),
        (ScalarValue::Date32(Some(v)), PrimitiveType::Date) => Value::Date(*v),
        (ScalarValue::Time64Microsecond(Some(v)), PrimitiveType::Time) => Value::Time(*v),
        (ScalarValue::TimestampMicrosecond(Some(v), _), PrimitiveType::Timestamp) => {
            Value::Timestamp(*v)
        }
        (ScalarValue::TimestampMicrosecond(Some(v), _), PrimitiveType::Timestamptz) => {
            Value::TimestampTZ(*v)
        }
        (
            ScalarValue::Utf8(Some(v))
            | ScalarValue::LargeUtf8(Some(v))
            | ScalarValue::Utf8View(Some(v)),
            PrimitiveType::String,
        ) => Value::String(v.clone()),
        (
            ScalarValue::Utf8(Some(v))
            | ScalarValue::LargeUtf8(Some(v))
            | ScalarValue::Utf8View(Some(v)),
            PrimitiveType::Uuid,
        ) => Value::UUID(Uuid::parse_str(v).ok()?),
        (
            ScalarValue::Binary(Some(v))
            | ScalarValue::LargeBinary(Some(v))
            | ScalarValue::BinaryView(Some(v)),
            PrimitiveType::Binary,
        ) => Value::Binary(v.clone()),
        (ScalarValue::FixedSizeBinary(_, Some(v)), PrimitiveType::Fixed(len)) => {
            Value::Fixed(*len as usize, v.clone())
        }
        (ScalarValue::Decimal128(Some(v), _, scale), PrimitiveType::Decimal { .. }) => {
            Value::Decimal(decimal_from_i128_with_scale(*v, u32::try_from(*scale).ok()?).ok()?)
        }
        _ => return None,
    })
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
