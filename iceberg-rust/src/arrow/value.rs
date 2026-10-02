//! Reading single Arrow cells as Iceberg [`Value`]s.

use arrow::{
    array::{Array, AsArray},
    datatypes::{
        DataType, Date32Type, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type,
        Int64Type, Int8Type, Time64MicrosecondType, TimeUnit, TimestampMicrosecondType,
    },
    error::ArrowError,
};
use iceberg_rust_spec::spec::{
    decimal::decimal_from_i128_with_scale,
    types::{PrimitiveType, Type},
    values::Value,
};
use ordered_float::OrderedFloat;
use uuid::Uuid;

/// The value at `row` of `array`, read as Iceberg type `ty`; `None` if null.
pub fn arrow_value(array: &dyn Array, row: usize, ty: &Type) -> Result<Option<Value>, ArrowError> {
    if array.is_null(row) {
        return Ok(None);
    }
    let unsupported = || {
        ArrowError::ComputeError(format!(
            "cannot read Iceberg {ty} from Arrow {}",
            array.data_type()
        ))
    };
    let Type::Primitive(primitive) = ty else {
        return Err(unsupported());
    };
    let value = match (primitive, array.data_type()) {
        (PrimitiveType::Boolean, DataType::Boolean) => {
            Value::Boolean(array.as_boolean().value(row))
        }
        (PrimitiveType::Int, DataType::Int8) => {
            Value::Int(array.as_primitive::<Int8Type>().value(row).into())
        }
        (PrimitiveType::Int, DataType::Int16) => {
            Value::Int(array.as_primitive::<Int16Type>().value(row).into())
        }
        (PrimitiveType::Int, DataType::Int32) => {
            Value::Int(array.as_primitive::<Int32Type>().value(row))
        }
        (PrimitiveType::Long, DataType::Int64) => {
            Value::LongInt(array.as_primitive::<Int64Type>().value(row))
        }
        (PrimitiveType::Float, DataType::Float32) => {
            Value::Float(OrderedFloat(array.as_primitive::<Float32Type>().value(row)))
        }
        (PrimitiveType::Double, DataType::Float64) => {
            Value::Double(OrderedFloat(array.as_primitive::<Float64Type>().value(row)))
        }
        (PrimitiveType::Date, DataType::Date32) => {
            Value::Date(array.as_primitive::<Date32Type>().value(row))
        }
        (PrimitiveType::Time, DataType::Time64(TimeUnit::Microsecond)) => {
            Value::Time(array.as_primitive::<Time64MicrosecondType>().value(row))
        }
        (PrimitiveType::Timestamp, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            Value::Timestamp(array.as_primitive::<TimestampMicrosecondType>().value(row))
        }
        (PrimitiveType::Timestamptz, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            Value::TimestampTZ(array.as_primitive::<TimestampMicrosecondType>().value(row))
        }
        (PrimitiveType::String, DataType::Utf8) => {
            Value::String(array.as_string::<i32>().value(row).to_owned())
        }
        (PrimitiveType::String, DataType::LargeUtf8) => {
            Value::String(array.as_string::<i64>().value(row).to_owned())
        }
        (PrimitiveType::String, DataType::Utf8View) => {
            Value::String(array.as_string_view().value(row).to_owned())
        }
        (PrimitiveType::Uuid, DataType::Utf8) => Value::UUID(
            Uuid::parse_str(array.as_string::<i32>().value(row))
                .map_err(|err| ArrowError::ComputeError(format!("invalid uuid: {err}")))?,
        ),
        (PrimitiveType::Uuid, DataType::FixedSizeBinary(16)) => Value::UUID(
            Uuid::from_slice(array.as_fixed_size_binary().value(row))
                .map_err(|err| ArrowError::ComputeError(format!("invalid uuid: {err}")))?,
        ),
        (PrimitiveType::Fixed(len), DataType::FixedSizeBinary(_)) => Value::Fixed(
            *len as usize,
            array.as_fixed_size_binary().value(row).to_vec(),
        ),
        (PrimitiveType::Binary, DataType::Binary) => {
            Value::Binary(array.as_binary::<i32>().value(row).to_vec())
        }
        (PrimitiveType::Binary, DataType::LargeBinary) => {
            Value::Binary(array.as_binary::<i64>().value(row).to_vec())
        }
        (PrimitiveType::Binary, DataType::BinaryView) => {
            Value::Binary(array.as_binary_view().value(row).to_vec())
        }
        (PrimitiveType::Decimal { scale, .. }, DataType::Decimal128(_, _)) => Value::Decimal(
            decimal_from_i128_with_scale(array.as_primitive::<Decimal128Type>().value(row), *scale)
                .map_err(|err| ArrowError::ComputeError(err.to_string()))?,
        ),
        _ => return Err(unsupported()),
    };
    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Date32Array, Decimal128Array, StringArray};

    use super::*;

    #[test]
    fn reads_typed_values_and_nulls() {
        let dates: ArrayRef = Arc::new(Date32Array::from(vec![Some(19478), None]));
        let date = Type::Primitive(PrimitiveType::Date);
        assert_eq!(
            arrow_value(&dates, 0, &date).unwrap(),
            Some(Value::Date(19478))
        );
        assert_eq!(arrow_value(&dates, 1, &date).unwrap(), None);

        let uuids: ArrayRef = Arc::new(StringArray::from(vec![
            "f79c3e09-677c-4bbd-a479-3f349cb785e7",
        ]));
        let uuid = Type::Primitive(PrimitiveType::Uuid);
        assert!(matches!(
            arrow_value(&uuids, 0, &uuid).unwrap(),
            Some(Value::UUID(_))
        ));

        let decimals: ArrayRef = Arc::new(
            Decimal128Array::from(vec![1065])
                .with_precision_and_scale(9, 2)
                .unwrap(),
        );
        let decimal = Type::Primitive(PrimitiveType::Decimal {
            precision: 9,
            scale: 2,
        });
        assert_eq!(
            arrow_value(&decimals, 0, &decimal).unwrap(),
            Some(Value::Decimal(
                decimal_from_i128_with_scale(1065, 2).unwrap()
            ))
        );
    }

    #[test]
    fn rejects_mismatched_types_and_bad_uuids() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec!["not-a-uuid"]));
        assert!(arrow_value(&strings, 0, &Type::Primitive(PrimitiveType::Uuid)).is_err());
        assert!(arrow_value(&strings, 0, &Type::Primitive(PrimitiveType::Long)).is_err());
    }
}
