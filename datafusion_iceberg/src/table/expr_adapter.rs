/*!
 * Resolving Parquet file columns by Iceberg field id.
 *
 * Writers built on iceberg-java (Spark, Flink, Trino) sanitize Parquet column
 * names: `TypeToMessageType` runs each name through
 * `AvroSchemaUtil.makeCompatibleName`, which replaces every character outside
 * `[A-Za-z0-9_]` with `_x<HEX>`. A column the Iceberg schema calls `my col` is
 * therefore stored as `my_x20col`, and the original name survives only in the
 * Iceberg schema. Readers are expected to key off the Iceberg field id, which
 * is stamped on every Parquet column.
 *
 * DataFusion's `DefaultPhysicalExprAdapter` reconciles a file against the table
 * schema *by name*, and substitutes a NULL literal for any nullable column it
 * cannot find -- so such a column reads back as all NULLs with no error.
 * [`IcebergPhysicalExprAdapterFactory`] closes that gap: for each file it pairs
 * logical fields with physical ones by `PARQUET:field_id`, renames the logical
 * schema to the names the file actually uses, rewrites column references to
 * match, and then hands off to the default adapter so all of its casting,
 * null-filling and index-fixing behaviour is preserved.
 *
 * Files whose names already agree -- everything this stack writes itself, since
 * iceberg-rust stores names verbatim -- take an identity fast path.
 *
 * # Scope: top-level columns only
 *
 * The id-based pairing this module does covers only top-level fields of the
 * table schema. Nested struct subfields are still matched by DataFusion *by
 * name*, exactly as before this module existed: a renamed nullable subfield
 * silently reads back as NULL, a renamed required subfield errors, and a
 * struct where *no* subfield name survives sanitization errors with "no field
 * name overlap". A future change that wants id-based resolution inside nested
 * types needs to walk the struct/list/map trees explicitly; nothing here does
 * that.
 *
 * # Known gap: absent ids still fall back to name matching
 *
 * A logical field id that appears nowhere in a file that *does* carry field
 * ids should, per Iceberg's column-projection rule, resolve to NULL for that
 * file. Today it instead falls back to matching that field by name against
 * the file, and can return whatever data a like-named physical column holds.
 * This is reachable by dropping a column and re-adding one under the same
 * name (Iceberg never reuses field ids, so the new column gets a fresh id
 * that the old files don't carry, but the old *name* may still be present in
 * those files under the old id). This is pre-existing behaviour -- it
 * predates this module and is not introduced by it -- and is deliberately
 * left unfixed here; it is out of scope for this change.
*/

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::Result;
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::PhysicalExpr;
use iceberg_rust::spec::arrow::schema::PARQUET_FIELD_ID_META_KEY;

/// A [`PhysicalExprAdapterFactory`] that matches file columns to table columns
/// by Iceberg field id, falling back to DataFusion's name matching whenever the
/// ids cannot settle it.
///
/// Note: DataFusion's own parquet type coercions (`apply_file_schema_type_coercions`,
/// `Int96Coercer`) run *before* this adapter is consulted, and they too match
/// by name -- so a column this module ends up renaming does not benefit from
/// them at that stage. Results still come out correct, because the delegated
/// `DefaultPhysicalExprAdapter` inserts the equivalent cast itself once the
/// schema has been renamed; the only loss is that a renamed string column
/// does not get the view-type (`StringViewArray`) optimization those
/// coercions would otherwise have applied.
#[derive(Debug, Clone, Default)]
pub struct IcebergPhysicalExprAdapterFactory;

impl PhysicalExprAdapterFactory for IcebergPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        let renames = renames_by_field_id(&logical_file_schema, &physical_file_schema);

        // Nothing to remap: identical to plain DataFusion behaviour.
        if renames.is_empty() {
            return DefaultPhysicalExprAdapterFactory
                .create(logical_file_schema, physical_file_schema);
        }

        let remapped = Arc::new(rename_schema(&logical_file_schema, &renames));
        let inner = DefaultPhysicalExprAdapterFactory.create(remapped, physical_file_schema)?;
        Ok(Arc::new(IcebergPhysicalExprAdapter { inner, renames }))
    }
}

#[derive(Debug)]
struct IcebergPhysicalExprAdapter {
    inner: Arc<dyn PhysicalExprAdapter>,
    renames: HashMap<String, String>,
}

impl PhysicalExprAdapter for IcebergPhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        self.inner.rewrite(rename_columns(expr, &self.renames)?)
    }
}

fn field_id(field: &Field) -> Option<i32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)?
        .parse()
        .ok()
}

/// Logical field name -> the name the file uses for the same field id, for every
/// top-level field where the two disagree.
///
/// Returns an empty map -- meaning "leave everything to the default adapter" --
/// when no name differs, or when applying the renames would make two logical
/// fields share a name. Abandoning the rename in that case is not a guarantee
/// of correctness: it simply falls back to DataFusion's pre-existing
/// name-matching behaviour, which can itself resolve to the wrong column (for
/// example when a stale, like-named physical column lingers in the file). It
/// is better than guessing which of two colliding fields is meant, but it is
/// not a safety net. Each path that abandons remapping logs a `tracing::warn!`
/// so a malformed or ambiguous file is not silently indistinguishable from a
/// well-formed one.
fn renames_by_field_id(logical: &Schema, physical: &Schema) -> HashMap<String, String> {
    let mut by_id: HashMap<i32, &str> = HashMap::new();
    for f in physical.fields() {
        if let Some(id) = field_id(f) {
            // A duplicate id is a malformed file; do not guess which one is meant.
            if by_id.insert(id, f.name().as_str()).is_some() {
                tracing::warn!(
                    "parquet file has field id {id} stamped on more than one \
                     column; falling back to name matching for this file"
                );
                return HashMap::new();
            }
        }
    }
    // No ids in the file at all: no logical field can acquire a rename below,
    // so this is purely a fast path, not a correctness guard -- removing it
    // would not change the result, only skip straight to the loop that would
    // return the same empty map anyway.
    if by_id.is_empty() {
        return HashMap::new();
    }

    let mut renames = HashMap::new();
    let mut resulting_names = HashSet::new();
    for f in logical.fields() {
        let physical_name = field_id(f)
            .and_then(|id| by_id.get(&id).copied())
            .unwrap_or_else(|| f.name().as_str());
        if !resulting_names.insert(physical_name.to_string()) {
            tracing::warn!(
                "resolving by Iceberg field id would alias two logical columns \
                 onto the file column {physical_name:?}; falling back to name \
                 matching for this file"
            );
            return HashMap::new();
        }
        if physical_name != f.name() {
            // Iceberg forbids duplicate field names in a schema, so this
            // insert should never overwrite an existing entry -- but check
            // that structurally rather than relying on the invariant holding.
            if renames
                .insert(f.name().clone(), physical_name.to_string())
                .is_some()
            {
                let name = f.name();
                tracing::warn!(
                    "two logical fields share the name {name:?}; falling back \
                     to name matching for this file"
                );
                return HashMap::new();
            }
        }
    }
    renames
}

/// `logical` with each renamed field's name swapped for the file's, keeping the
/// logical data type, nullability and metadata so the default adapter still
/// inserts the casts it would otherwise have inserted.
fn rename_schema(logical: &Schema, renames: &HashMap<String, String>) -> Schema {
    let fields = logical
        .fields()
        .iter()
        .map(|f| match renames.get(f.name()) {
            Some(new_name) => Arc::new(f.as_ref().clone().with_name(new_name)),
            None => Arc::clone(f),
        })
        .collect::<Vec<_>>();
    Schema::new_with_metadata(fields, logical.metadata().clone())
}

/// Point every renamed [`Column`] reference at the name the file uses. The index
/// is left alone; the default adapter re-resolves it against the file schema.
fn rename_columns(
    expr: Arc<dyn PhysicalExpr>,
    renames: &HashMap<String, String>,
) -> Result<Arc<dyn PhysicalExpr>> {
    expr.transform(|e| {
        let Some(column) = e.downcast_ref::<Column>() else {
            return Ok(Transformed::no(e));
        };
        match renames.get(column.name()) {
            Some(new_name) => Ok(Transformed::yes(
                Arc::new(Column::new(new_name, column.index())) as Arc<dyn PhysicalExpr>,
            )),
            None => Ok(Transformed::no(e)),
        }
    })
    .data()
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::datatypes::DataType;
    use datafusion::physical_plan::expressions::{CastExpr, Literal};
    use datafusion::scalar::ScalarValue;

    fn field(name: &str, id: Option<i32>) -> Field {
        let f = Field::new(name, DataType::Int64, true);
        match id {
            Some(id) => f.with_metadata(HashMap::from_iter([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )])),
            None => f,
        }
    }

    fn adapter(logical: Schema, physical: Schema) -> Arc<dyn PhysicalExprAdapter> {
        IcebergPhysicalExprAdapterFactory
            .create(Arc::new(logical), Arc::new(physical))
            .unwrap()
    }

    /// The bug: the file calls the column `my_x20col`, the Iceberg schema calls
    /// it `my col`, and only the field id ties them together.
    #[test]
    fn resolves_a_renamed_column_by_field_id() {
        let logical = Schema::new(vec![field("id", Some(1)), field("my col", Some(2))]);
        let physical = Schema::new(vec![field("id", Some(1)), field("my_x20col", Some(2))]);

        let rewritten = adapter(logical, physical)
            .rewrite(Arc::new(Column::new("my col", 1)))
            .unwrap();

        let col = rewritten
            .downcast_ref::<Column>()
            .unwrap_or_else(|| panic!("expected a Column, got {rewritten:?}"));
        assert_eq!(col.name(), "my_x20col");
        assert_eq!(col.index(), 1);
    }

    /// Reordering is resolved by id too, not by the index the planner handed us.
    #[test]
    fn resolves_a_reordered_column_by_field_id() {
        let logical = Schema::new(vec![field("id", Some(1)), field("my col", Some(2))]);
        let physical = Schema::new(vec![field("my_x20col", Some(2)), field("id", Some(1))]);

        let rewritten = adapter(logical, physical)
            .rewrite(Arc::new(Column::new("my col", 1)))
            .unwrap();

        let col = rewritten.downcast_ref::<Column>().unwrap();
        assert_eq!(col.name(), "my_x20col");
        assert_eq!(col.index(), 0);
    }

    /// A file with no ids at all (pre-Iceberg parquet, or a writer that drops
    /// them) must keep the old name-matching behaviour exactly.
    #[test]
    fn falls_back_to_name_matching_when_the_file_has_no_ids() {
        let logical = Schema::new(vec![field("id", Some(1)), field("my col", Some(2))]);
        let physical = Schema::new(vec![field("id", None), field("my col", None)]);

        let rewritten = adapter(logical, physical)
            .rewrite(Arc::new(Column::new("my col", 1)))
            .unwrap();

        // The logical (Iceberg) schema always carries `PARQUET:field_id`
        // metadata that a no-ids physical file lacks, so the delegated
        // `DefaultPhysicalExprAdapter` wraps the resolved column in a
        // metadata-fixing `CastExpr` -- this is its real, unmodified
        // behaviour for these inputs, not something this adapter adds.
        let col = rewritten
            .downcast_ref::<Column>()
            .or_else(|| {
                rewritten
                    .downcast_ref::<CastExpr>()
                    .and_then(|cast| cast.expr().downcast_ref::<Column>())
            })
            .unwrap_or_else(|| {
                panic!("expected a Column (optionally cast-wrapped), got {rewritten:?}")
            });
        assert_eq!(col.name(), "my col");
        assert_eq!(col.index(), 1);
    }

    /// Schema evolution still works: a column added after this file was written
    /// has an id the file does not carry, and must read back as NULL.
    #[test]
    fn a_column_absent_from_the_file_is_still_nulled() {
        let logical = Schema::new(vec![field("id", Some(1)), field("added later", Some(9))]);
        let physical = Schema::new(vec![field("id", Some(1))]);

        let rewritten = adapter(logical, physical)
            .rewrite(Arc::new(Column::new("added later", 1)))
            .unwrap();

        let literal = rewritten
            .downcast_ref::<Literal>()
            .unwrap_or_else(|| panic!("expected a NULL literal, got {rewritten:?}"));
        assert_eq!(literal.value(), &ScalarValue::Int64(None));
    }

    /// If remapping would make two logical fields share a name we would silently
    /// read the wrong column, so remapping is abandoned wholesale instead.
    #[test]
    fn remapping_is_abandoned_when_it_would_collide() {
        let logical = Schema::new(vec![field("a", Some(1)), field("b", Some(2))]);
        // Both ids map onto the same file name -- a malformed file.
        let physical = Schema::new(vec![field("b", Some(1)), field("b", Some(2))]);

        let rewritten = adapter(logical, physical)
            .rewrite(Arc::new(Column::new("a", 0)))
            .unwrap();

        // No rename applied; `a` is simply missing from the file, so it nulls.
        assert!(
            rewritten.downcast_ref::<Literal>().is_some(),
            "expected remapping to be skipped, got {rewritten:?}"
        );
    }

    /// A file where the same field id is stamped on two different physical
    /// columns is malformed; guessing which one is meant would risk silently
    /// aliasing onto the wrong data, so a file that repeats a field id must
    /// abandon remapping entirely rather than pick an arbitrary winner.
    ///
    /// Critically, the logical schema has a field whose id (5) matches the
    /// *duplicated* physical id, not some other id absent from the file. If
    /// the duplicate-id guard were removed, `by_id` would just keep the last
    /// physical field seen for id 5 (`"y"`), `data` would get renamed to `"y"`,
    /// and the column would silently resolve to whichever duplicate happened
    /// to win -- the exact wrong-column read this guard exists to prevent.
    /// With the guard, that never happens: remapping is abandoned wholesale,
    /// so `data` isn't found by name in the file and nulls out instead.
    ///
    /// The two duplicate-id physical fields deliberately have different names
    /// (unlike `remapping_is_abandoned_when_it_would_collide`, where the
    /// collision is in the *resulting* names) so this exercises the
    /// duplicate-id guard specifically, not the resulting-name-collision guard.
    #[test]
    fn remapping_is_abandoned_when_the_file_repeats_a_field_id() {
        let logical = Schema::new(vec![field("id", Some(1)), field("data", Some(5))]);
        // Two different physical columns both claim field id 5 -- a malformed file.
        let physical = Schema::new(vec![field("x", Some(5)), field("y", Some(5))]);

        let rewritten = adapter(logical, physical)
            .rewrite(Arc::new(Column::new("data", 1)))
            .unwrap();

        // No rename applied; `data` is simply missing from the file, so it nulls.
        let literal = rewritten
            .downcast_ref::<Literal>()
            .unwrap_or_else(|| panic!("expected a NULL literal, got {rewritten:?}"));
        assert_eq!(literal.value(), &ScalarValue::Int64(None));
    }
}
