//! Inclusive projection of query filters onto partition columns.
//!
//! [`project`] rewrites a filter on table columns into a filter on partition
//! columns that keeps every partition that can hold a matching row. It may
//! keep partitions without matches; it never drops partitions with matches.
//! Anything it cannot project safely becomes `None` ("don't prune").

use datafusion::{
    arrow::datatypes::{DataType, Schema as ArrowSchema},
    common::Column,
    scalar::ScalarValue,
};
use datafusion_expr::{binary_expr, expr::InList, BinaryExpr, Expr, Operator};
use iceberg_rust::spec::{
    partition::{BoundPartitionField, Transform},
    types::{PrimitiveType, Type},
};

use crate::partition_value::{scalar_to_value, value_to_scalar};

/// A predicate on one source column.
enum Leaf<'a> {
    Compare(Operator, &'a ScalarValue),
    In(Vec<&'a ScalarValue>),
    IsNull,
    IsNotNull,
}

/// Projects `expr` onto the partition columns described by `partition_fields`,
/// whose types are in `partition_schema`. Returns `None` if nothing can be pruned.
pub(crate) fn project(
    expr: &Expr,
    partition_fields: &[BoundPartitionField<'_>],
    partition_schema: &ArrowSchema,
) -> Option<Expr> {
    let recurse = |expr: &Expr| project(expr, partition_fields, partition_schema);
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => match (recurse(left), recurse(right)) {
            (Some(left), Some(right)) => Some(left.and(right)),
            (Some(one), None) | (None, Some(one)) => Some(one),
            (None, None) => None,
        },
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Or,
            right,
        }) => Some(recurse(left)?.or(recurse(right)?)),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            if !matches!(
                op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            ) {
                return None;
            }
            let (column, op, literal) = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(column), Expr::Literal(literal, _)) => (column, *op, literal),
                (Expr::Literal(literal, _), Expr::Column(column)) => (column, op.swap()?, literal),
                _ => return None,
            };
            project_leaf(
                column,
                &Leaf::Compare(op, literal),
                partition_fields,
                partition_schema,
            )
        }
        Expr::InList(InList {
            expr,
            list,
            negated: false,
        }) => {
            let Expr::Column(column) = expr.as_ref() else {
                return None;
            };
            let literals = list
                .iter()
                .map(|item| match item {
                    Expr::Literal(literal, _) => Some(literal),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            project_leaf(
                column,
                &Leaf::In(literals),
                partition_fields,
                partition_schema,
            )
        }
        Expr::IsNull(inner) => match inner.as_ref() {
            Expr::Column(column) => {
                project_leaf(column, &Leaf::IsNull, partition_fields, partition_schema)
            }
            _ => None,
        },
        Expr::IsNotNull(inner) => match inner.as_ref() {
            Expr::Column(column) => {
                project_leaf(column, &Leaf::IsNotNull, partition_fields, partition_schema)
            }
            _ => None,
        },
        _ => None,
    }
}

/// AND of the projections through every partition field built on `column`.
fn project_leaf(
    column: &Column,
    leaf: &Leaf<'_>,
    partition_fields: &[BoundPartitionField<'_>],
    partition_schema: &ArrowSchema,
) -> Option<Expr> {
    partition_fields
        .iter()
        .filter(|field| field.source_name() == column.name())
        .filter_map(|field| project_field(field, leaf, partition_schema))
        .reduce(Expr::and)
}

fn project_field(
    field: &BoundPartitionField<'_>,
    leaf: &Leaf<'_>,
    partition_schema: &ArrowSchema,
) -> Option<Expr> {
    let transform = field.transform();
    if let Transform::Void = transform {
        return None;
    }
    let partition_column = Expr::Column(Column::new_unqualified(field.name()));
    let partition_type = partition_schema
        .field_with_name(field.name())
        .ok()?
        .data_type();
    let partition_literal = |literal: &ScalarValue| -> Option<Expr> {
        let source_type: DataType = field.field_type().try_into().ok()?;
        let cast = literal.cast_to(&source_type).ok()?;
        // Reject casts that change the literal (truncation, rounding).
        if cast.cast_to(&literal.data_type()).ok()? != *literal {
            return None;
        }
        let value = scalar_to_value(&cast, field.field_type())?;
        // Reject non-canonical uuid text.
        if matches!(field.field_type(), Type::Primitive(PrimitiveType::Uuid))
            && value_to_scalar(Some(&value), &source_type).ok()? != cast
        {
            return None;
        }
        let transformed = value.transform(transform).ok()?;
        Some(Expr::Literal(
            value_to_scalar(Some(&transformed), partition_type).ok()?,
            None,
        ))
    };
    // Transforms with a <= b  =>  t(a) <= t(b).
    let order_preserving = match transform {
        Transform::Identity
        | Transform::Year
        | Transform::Month
        | Transform::Day
        | Transform::Hour => true,
        Transform::Truncate(_) => matches!(
            field.field_type(),
            Type::Primitive(
                PrimitiveType::Int | PrimitiveType::Long | PrimitiveType::Decimal { .. }
            )
        ),
        Transform::Bucket(_) | Transform::Void => false,
    };
    match leaf {
        Leaf::IsNull => Some(partition_column.is_null()),
        Leaf::IsNotNull => Some(partition_column.is_not_null()),
        Leaf::In(literals) => {
            let list = literals
                .iter()
                .map(|literal| partition_literal(literal))
                .collect::<Option<Vec<_>>>()?;
            Some(partition_column.in_list(list, false))
        }
        Leaf::Compare(op, literal) => {
            let op = match (op, transform) {
                (op, Transform::Identity) => *op,
                (Operator::Eq, _) => Operator::Eq,
                (Operator::Lt | Operator::LtEq, _) if order_preserving => Operator::LtEq,
                (Operator::Gt | Operator::GtEq, _) if order_preserving => Operator::GtEq,
                _ => return None,
            };
            Some(binary_expr(
                partition_column,
                op,
                partition_literal(literal)?,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema as ArrowSchema},
        scalar::ScalarValue,
    };
    use datafusion_expr::{col, lit, Expr};
    use iceberg_rust::spec::{
        partition::{BoundPartitionField, PartitionField, Transform},
        transform::{bucket, hash_long},
        types::{PrimitiveType, StructField, Type},
    };

    use super::*;

    const TS_2023_05_15_10H: i64 = 1_684_144_800_000_000;
    const DAY_2023_05_15: i32 = 19_492;

    fn ts(micros: i64) -> Expr {
        lit(ScalarValue::TimestampMicrosecond(Some(micros), None))
    }

    /// Projects `expr` with the given (source, partition field, partition column type)s.
    fn run(expr: Expr, fields: &[(StructField, PartitionField, DataType)]) -> Option<Expr> {
        let bound: Vec<_> = fields
            .iter()
            .map(|(source, field, _)| BoundPartitionField::new(field, source))
            .collect();
        let schema = ArrowSchema::new(
            fields
                .iter()
                .map(|(_, field, ty)| Field::new(field.name().clone(), ty.clone(), true))
                .collect::<Vec<_>>(),
        );
        project(&expr, &bound, &schema)
    }

    fn ts_source() -> StructField {
        StructField::new(
            5,
            "ts",
            false,
            Type::Primitive(PrimitiveType::Timestamp),
            None,
        )
    }
    fn n_source() -> StructField {
        StructField::new(2, "n", false, Type::Primitive(PrimitiveType::Long), None)
    }
    fn s_source() -> StructField {
        StructField::new(3, "s", false, Type::Primitive(PrimitiveType::String), None)
    }
    fn day() -> (StructField, PartitionField, DataType) {
        (
            ts_source(),
            PartitionField::new(5, 1000, "ts_day", Transform::Day),
            DataType::Int32,
        )
    }
    fn n_bucket() -> (StructField, PartitionField, DataType) {
        (
            n_source(),
            PartitionField::new(2, 1001, "n_bucket", Transform::Bucket(4)),
            DataType::Int32,
        )
    }
    fn n_trunc() -> (StructField, PartitionField, DataType) {
        (
            n_source(),
            PartitionField::new(2, 1002, "n_trunc", Transform::Truncate(10)),
            DataType::Int64,
        )
    }
    fn n_identity() -> (StructField, PartitionField, DataType) {
        (
            n_source(),
            PartitionField::new(2, 1003, "n", Transform::Identity),
            DataType::Int64,
        )
    }
    fn s_trunc() -> (StructField, PartitionField, DataType) {
        (
            s_source(),
            PartitionField::new(3, 1004, "s_trunc", Transform::Truncate(2)),
            DataType::Utf8,
        )
    }

    #[test]
    fn temporal_ranges_widen_to_the_boundary_partition() {
        let day_lit = lit(DAY_2023_05_15);
        assert_eq!(
            run(col("ts").gt(ts(TS_2023_05_15_10H)), &[day()]),
            Some(col("ts_day").gt_eq(day_lit.clone()))
        );
        assert_eq!(
            run(col("ts").gt_eq(ts(TS_2023_05_15_10H)), &[day()]),
            Some(col("ts_day").gt_eq(day_lit.clone()))
        );
        assert_eq!(
            run(col("ts").lt(ts(TS_2023_05_15_10H)), &[day()]),
            Some(col("ts_day").lt_eq(day_lit.clone()))
        );
        assert_eq!(
            run(col("ts").lt_eq(ts(TS_2023_05_15_10H)), &[day()]),
            Some(col("ts_day").lt_eq(day_lit.clone()))
        );
        assert_eq!(
            run(col("ts").eq(ts(TS_2023_05_15_10H)), &[day()]),
            Some(col("ts_day").eq(day_lit))
        );
        assert_eq!(run(col("ts").not_eq(ts(TS_2023_05_15_10H)), &[day()]), None);
    }

    #[test]
    fn literal_on_the_left_flips_the_operator() {
        assert_eq!(
            run(lit(15i64).gt(col("n")), &[n_trunc()]),
            Some(col("n_trunc").lt_eq(lit(10i64)))
        );
    }

    #[test]
    fn bucket_projects_only_equality_and_in() {
        let b34 = bucket(hash_long(34), 4);
        let b35 = bucket(hash_long(35), 4);
        assert_eq!(
            run(col("n").eq(lit(34i64)), &[n_bucket()]),
            Some(col("n_bucket").eq(lit(b34)))
        );
        assert_eq!(
            run(
                col("n").in_list(vec![lit(34i64), lit(35i64)], false),
                &[n_bucket()]
            ),
            Some(col("n_bucket").in_list(vec![lit(b34), lit(b35)], false))
        );
        assert_eq!(run(col("n").lt(lit(34i64)), &[n_bucket()]), None);
        assert_eq!(
            run(col("n").in_list(vec![lit(34i64)], true), &[n_bucket()]),
            None
        );
    }

    #[test]
    fn string_truncate_projects_only_equality() {
        assert_eq!(
            run(col("s").eq(lit("abcdef")), &[s_trunc()]),
            Some(col("s_trunc").eq(lit("ab")))
        );
        assert_eq!(run(col("s").lt(lit("b")), &[s_trunc()]), None);
    }

    #[test]
    fn identity_keeps_every_comparison() {
        assert_eq!(
            run(col("n").not_eq(lit(5i64)), &[n_identity()]),
            Some(col("n").not_eq(lit(5i64)))
        );
        // The literal is cast to the source column's type first.
        assert_eq!(
            run(col("n").eq(lit(5i32)), &[n_identity()]),
            Some(col("n").eq(lit(5i64)))
        );
    }

    #[test]
    fn nulls_project_for_every_non_void_transform() {
        assert_eq!(
            run(col("n").is_null(), &[n_bucket()]),
            Some(col("n_bucket").is_null())
        );
        assert_eq!(
            run(col("n").is_not_null(), &[n_trunc()]),
            Some(col("n_trunc").is_not_null())
        );
        let void = (
            n_source(),
            PartitionField::new(2, 1005, "n_void", Transform::Void),
            DataType::Int64,
        );
        assert_eq!(run(col("n").is_null(), std::slice::from_ref(&void)), None);
        assert_eq!(run(col("n").eq(lit(1i64)), &[void]), None);
    }

    #[test]
    fn a_column_feeding_two_partition_fields_projects_to_both() {
        let ts_bucket = (
            ts_source(),
            PartitionField::new(5, 1006, "ts_bucket", Transform::Bucket(4)),
            DataType::Int32,
        );
        let b = bucket(hash_long(TS_2023_05_15_10H), 4);
        assert_eq!(
            run(col("ts").eq(ts(TS_2023_05_15_10H)), &[day(), ts_bucket]),
            Some(
                col("ts_day")
                    .eq(lit(DAY_2023_05_15))
                    .and(col("ts_bucket").eq(lit(b)))
            )
        );
    }

    #[test]
    fn and_keeps_what_projects_or_needs_every_branch() {
        let n_lt = col("n").lt(lit(15i64));
        let other = col("id").eq(lit(1i64));
        assert_eq!(
            run(n_lt.clone().and(other.clone()), &[n_trunc()]),
            Some(col("n_trunc").lt_eq(lit(10i64)))
        );
        assert_eq!(run(n_lt.clone().or(other), &[n_trunc()]), None);
        assert_eq!(
            run(n_lt.clone().or(col("n").gt(lit(30i64))), &[n_trunc()]),
            Some(
                col("n_trunc")
                    .lt_eq(lit(10i64))
                    .or(col("n_trunc").gt_eq(lit(30i64)))
            )
        );
        assert_eq!(run(Expr::Not(Box::new(n_lt)), &[n_trunc()]), None);
    }

    #[test]
    fn unprojectable_shapes_return_none() {
        assert_eq!(
            run((col("n") + lit(1i64)).eq(lit(15i64)), &[n_trunc()]),
            None
        );
        assert_eq!(
            run(col("n").eq(lit(ScalarValue::Int64(None))), &[n_trunc()]),
            None
        );
        assert_eq!(run(col("n").eq(col("id")), &[n_trunc()]), None);
    }

    #[test]
    fn lossy_literal_casts_are_not_projected() {
        assert_eq!(run(col("n").lt(lit(3.7f64)), &[n_identity()]), None);
        assert_eq!(run(col("n").not_eq(lit(3.7f64)), &[n_identity()]), None);
        let amount = (
            StructField::new(
                7,
                "d",
                false,
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 9,
                    scale: 1,
                }),
                None,
            ),
            PartitionField::new(7, 1010, "d_id", Transform::Identity),
            DataType::Decimal128(9, 1),
        );
        assert_eq!(
            run(
                col("d").gt(lit(ScalarValue::Decimal128(Some(375), 9, 2))),
                &[amount]
            ),
            None
        );
    }

    #[test]
    fn timestamptz_literal_with_offset_projects_through_day() {
        let source = StructField::new(
            6,
            "tz",
            false,
            Type::Primitive(PrimitiveType::Timestamptz),
            None,
        );
        let field = (
            source,
            PartitionField::new(6, 1011, "tz_day", Transform::Day),
            DataType::Int32,
        );
        let literal = lit(ScalarValue::TimestampMicrosecond(
            Some(TS_2023_05_15_10H),
            Some("+02:00".into()),
        ));
        assert_eq!(
            run(col("tz").gt(literal), &[field]),
            Some(col("tz_day").gt_eq(lit(DAY_2023_05_15)))
        );
    }

    #[test]
    fn decimal_truncate_range() {
        let source = StructField::new(
            8,
            "amount",
            false,
            Type::Primitive(PrimitiveType::Decimal {
                precision: 9,
                scale: 2,
            }),
            None,
        );
        let field = (
            source,
            PartitionField::new(8, 1012, "amount_trunc", Transform::Truncate(50)),
            DataType::Decimal128(9, 2),
        );
        assert_eq!(
            run(
                col("amount").lt(lit(ScalarValue::Decimal128(Some(1070), 9, 2))),
                &[field]
            ),
            Some(col("amount_trunc").lt_eq(lit(ScalarValue::Decimal128(Some(1050), 9, 2))))
        );
    }

    #[test]
    fn null_checks_on_identity_and_temporal() {
        assert_eq!(
            run(col("n").is_null(), &[n_identity()]),
            Some(col("n").is_null())
        );
        assert_eq!(
            run(col("n").is_not_null(), &[n_identity()]),
            Some(col("n").is_not_null())
        );
        assert_eq!(
            run(col("ts").is_null(), &[day()]),
            Some(col("ts_day").is_null())
        );
        assert_eq!(
            run(col("ts").is_not_null(), &[day()]),
            Some(col("ts_day").is_not_null())
        );
    }

    #[test]
    fn literal_on_the_left_of_equality() {
        assert_eq!(
            run(lit(15i64).eq(col("n")), &[n_trunc()]),
            Some(col("n_trunc").eq(lit(10i64)))
        );
    }

    #[test]
    fn or_across_two_partitioned_columns() {
        assert_eq!(
            run(
                col("n")
                    .lt(lit(15i64))
                    .or(col("ts").gt(ts(TS_2023_05_15_10H))),
                &[n_trunc(), day()]
            ),
            Some(
                col("n_trunc")
                    .lt_eq(lit(10i64))
                    .or(col("ts_day").gt_eq(lit(DAY_2023_05_15)))
            )
        );
    }
}
