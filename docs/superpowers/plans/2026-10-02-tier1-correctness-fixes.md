# Tier 1 Correctness Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make partition transforms, partitioned writes, partition pruning, equality-delete scans and file bounds match the Iceberg spec and the Java implementation.

**Architecture:** One set of pure scalar transform functions in `iceberg-rust-spec` is the single source of truth; `Value::transform` and the Arrow kernels both call it. Partitioned writes group rows with Arrow's `RowConverter` and carry the computed partition tuple into the data file. Partition pruning uses a new inclusive projection over DataFusion `Expr`. Equality-delete scans stop pushing `LIMIT` below the anti-join and treat NULL keys as equal. File bounds merge across row groups with `Value: Ord`.

**Tech Stack:** Rust, Arrow / Parquet 59, DataFusion 55 (fork), `murmur3`, `uuid`, `fastnum` decimals, tokio tests, testcontainers (Spark).

**Spec:** `docs/superpowers/specs/2026-10-02-tier1-correctness-fixes-design.md`

## Global Constraints

- No production data exists: no migration or repair tooling.
- Tier 2 and Tier 3 audit items are out of scope (except the two one-line manifest-pruning fixes in Task 10, which the new `IS NULL` projection depends on).
- `cargo clippy --all-targets --all-features -- -D warnings` and `cargo fmt --all -- --check` must pass after every task.
- `make test` must pass at the end of each PR (Tasks 7, 8, 10, 11).
- Keep the DataFusion fork pin in `Cargo.toml` unchanged.
- The only new dependency is `ordered-float = "5.3.0"` in `iceberg-rust/Cargo.toml` (same version `iceberg-rust-spec` already uses).
- Every commit message ends with:
  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01DV558cLAwzTYg4nezLXrWG
  ```
- Work on branch `fix/tier1-correctness`. PR boundaries: PR 1 = Tasks 1–7, PR 2 = Task 8, PR 3 = Tasks 9–10, PR 4 = Task 11.

## Review Focus

1. `bucket[0]` / `truncate[0]` → a returned error, never a divide-by-zero panic. Pinned in Tasks 2 and 3.
2. An empty record batch passed to `partition_record_batch` → yields no partitions, no panic (the old code computed `len() - 1`). Pinned in Task 4.
3. A uuid column (Arrow `Utf8`) → bucketed by its 16 bytes, not by its text; an unparseable uuid string → error, not panic. Pinned in Task 3.
4. A filter on a column that feeds two partition fields (e.g. `day(ts)` and `bucket(ts)`) → both projections ANDed, nothing dropped. Pinned in Task 9.
5. Timestamps and dates at the extremes (`i64::MIN` micros, `i32::MIN` days) → a (wrapped, Java-compatible) value, never a panic. Pinned in Task 1.

## File Map

| File | Change | Responsibility |
|---|---|---|
| `iceberg-rust-spec/src/spec/transform.rs` | Create | Scalar bucket / temporal / truncate functions |
| `iceberg-rust-spec/src/spec/mod.rs` | Modify | Register `transform` module |
| `iceberg-rust-spec/src/spec/values.rs` | Modify | `Value::transform` delegates to `transform` module |
| `iceberg-rust-spec/src/spec/types.rs` | Modify | `Type::tranform(Void)` returns the source type |
| `iceberg-rust/src/arrow/value.rs` | Create | Read one Arrow cell as an Iceberg `Value` |
| `iceberg-rust/src/arrow/transform.rs` | Rewrite | Arrow kernels for every transform × type |
| `iceberg-rust/src/arrow/partition.rs` | Rewrite | Row grouping by partition tuple |
| `iceberg-rust/src/arrow/write.rs` | Modify | `Option<Value>` partition tuples; pass them to `parquet_to_datafile` |
| `iceberg-rust/src/file_format/parquet.rs` | Modify | Accept partition tuple; strict stats fallback; generic bounds merge |
| `datafusion_iceberg/src/partition_value.rs` | Create | `Value` ⇄ `ScalarValue` conversions typed by target column |
| `datafusion_iceberg/src/partition_projection.rs` | Create | Inclusive projection of filters onto partition columns |
| `datafusion_iceberg/src/pruning_statistics.rs` | Modify | Remove old predicate rewrite; fix manifest null/row counts |
| `datafusion_iceberg/src/table/mod.rs` | Modify | Demuxer partition tuples; typed partition values on read; projection; equality-delete limit + NULL |
| `datafusion_iceberg/tests/tier1_regressions.rs` | Create | End-to-end regressions and differential pruning test |
| `datafusion_iceberg/tests/integration_spark_transforms.rs` | Create | Spark ⇄ DataFusion transform compatibility |

---

## PR 1 — Transforms and partitioned writes

### Task 1: Scalar transform functions

**Files:**
- Create: `iceberg-rust-spec/src/spec/transform.rs`
- Modify: `iceberg-rust-spec/src/spec/mod.rs`

**Interfaces:**
- Consumes: `iceberg_rust_spec::spec::decimal::i128_to_be_bytes_min(i128) -> Vec<u8>`
- Produces (all `pub`, in `iceberg_rust_spec::spec::transform`):
  - `hash_bytes(&[u8]) -> i32`, `hash_int(i32) -> i32`, `hash_long(i64) -> i32`, `hash_timestamp_nanos(i64) -> i32`, `hash_decimal(i128) -> i32`, `hash_str(&str) -> i32`, `hash_uuid(&Uuid) -> i32`
  - `bucket(hash: i32, n: u32) -> i32` (caller guarantees `n > 0`)
  - `days_to_years(i32) -> i32`, `days_to_months(i32) -> i32`
  - `micros_to_years(i64) -> i32`, `micros_to_months(i64) -> i32`, `micros_to_days(i64) -> i32`, `micros_to_hours(i64) -> i32`
  - `nanos_to_years(i64) -> i32`, `nanos_to_months(i64) -> i32`, `nanos_to_days(i64) -> i32`, `nanos_to_hours(i64) -> i32`
  - `truncate_int(i32, u32) -> i32`, `truncate_long(i64, u32) -> i64`, `truncate_decimal(i128, u32) -> i128` (caller guarantees width `> 0`)
  - `truncate_str(&str, u32) -> &str`, `truncate_bytes(&[u8], u32) -> &[u8]`

- [ ] **Step 1: Register the module and write the failing tests**

In `iceberg-rust-spec/src/spec/mod.rs`, add after `pub mod tabular;`:

```rust
pub mod transform;
```

Create `iceberg-rust-spec/src/spec/transform.rs` with only the tests:

```rust
//! Scalar implementations of the Iceberg partition transforms.

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    // Iceberg spec, Appendix B: 32-bit hash requirements.
    #[test]
    fn hashes_match_spec_appendix_b() {
        assert_eq!(hash_int(34), 2017239379);
        assert_eq!(hash_long(34), 2017239379);
        assert_eq!(hash_decimal(1420), -500754589); // decimal(9,2) 14.20
        assert_eq!(hash_int(17486), -653330422); // date 2017-11-16
        assert_eq!(hash_long(81_068_000_000), -662762989); // time 22:31:08
        assert_eq!(hash_long(1_510_871_468_000_000), -2047944441); // 2017-11-16T22:31:08
        assert_eq!(hash_long(1_510_871_468_000_001), -1207196810); // ...08.000001
        assert_eq!(hash_timestamp_nanos(1_510_871_468_000_001_001), -1207196810);
        assert_eq!(hash_str("iceberg"), 1210000089);
        let uuid = Uuid::parse_str("f79c3e09-677c-4bbd-a479-3f349cb785e7").unwrap();
        assert_eq!(hash_uuid(&uuid), 1488055340);
        assert_eq!(hash_bytes(&[0, 1, 2, 3]), -188683207);
    }

    #[test]
    fn bucket_masks_the_sign_bit_before_modulo() {
        assert_eq!(bucket(2017239379, 10), 9);
        // (-653330422 & i32::MAX) % 10 = 1494153226 % 10
        assert_eq!(bucket(-653330422, 10), 6);
        assert_eq!(bucket(i32::MIN, 7), 0);
    }

    #[test]
    fn temporal_transforms_floor_before_the_epoch() {
        assert_eq!(days_to_years(0), 0);
        assert_eq!(days_to_years(-1), -1); // 1969-12-31
        assert_eq!(days_to_years(19478), 53); // 2023-05-01
        assert_eq!(days_to_months(19478), 640);
        assert_eq!(days_to_months(-1), -1);
        assert_eq!(days_to_months(-365), -12); // 1969-01-01
        assert_eq!(micros_to_years(-1), -1);
        assert_eq!(micros_to_months(-1), -1);
        assert_eq!(micros_to_months(1_682_937_000_000_000), 640); // 2023-05-01T10:30Z
        assert_eq!(micros_to_days(-1), -1);
        assert_eq!(micros_to_days(0), 0);
        assert_eq!(micros_to_hours(-1), -1);
        assert_eq!(micros_to_hours(3_600_000_000), 1);
        assert_eq!(nanos_to_days(-1), -1);
        assert_eq!(nanos_to_hours(1), 0);
        assert_eq!(nanos_to_years(-1), -1);
        assert_eq!(nanos_to_months(1_682_937_000_000_000_000), 640);
    }

    #[test]
    fn temporal_transforms_do_not_panic_at_the_extremes() {
        for micros in [i64::MIN, i64::MAX] {
            micros_to_years(micros);
            micros_to_months(micros);
            micros_to_days(micros);
            micros_to_hours(micros);
            nanos_to_years(micros);
            nanos_to_months(micros);
        }
        for days in [i32::MIN, i32::MAX] {
            days_to_years(days);
            days_to_months(days);
        }
    }

    #[test]
    fn truncate_matches_spec_examples() {
        assert_eq!(truncate_int(1, 10), 0);
        assert_eq!(truncate_int(-1, 10), -10);
        assert_eq!(truncate_long(1, 10), 0);
        assert_eq!(truncate_long(-1, 10), -10);
        assert_eq!(truncate_decimal(1065, 50), 1050); // 10.65 -> 10.50
        assert_eq!(truncate_decimal(-1, 50), -50);
        assert_eq!(truncate_str("iceberg", 3), "ice");
        assert_eq!(truncate_str("éé", 1), "é"); // code points, not bytes
        assert_eq!(truncate_str("ab", 5), "ab");
        assert_eq!(truncate_bytes(&[1, 2, 3, 4, 5], 3), &[1, 2, 3]);
        assert_eq!(truncate_bytes(&[1], 3), &[1]);
    }

    #[test]
    fn truncate_wraps_like_java_at_the_minimum() {
        // Java int arithmetic wraps; so must we, without a debug-mode panic.
        truncate_int(i32::MIN, 10);
        truncate_long(i64::MIN, 10);
        truncate_decimal(i128::MIN, 10);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p iceberg-rust-spec --lib spec::transform`
Expected: compile errors `cannot find function hash_int in this scope` (and the other functions).

- [ ] **Step 3: Implement the functions**

Insert above the `#[cfg(test)]` block in `iceberg-rust-spec/src/spec/transform.rs`:

```rust
//!
//! Every function follows the Iceberg spec and the Java reference
//! implementation (`BucketUtil`, `DateTimeUtil`, `TruncateUtil`,
//! `UnicodeUtil`). [`crate::spec::values::Value::transform`] and the Arrow
//! kernels in `iceberg-rust` both call these, so the two paths cannot disagree.

use uuid::Uuid;

use super::decimal::i128_to_be_bytes_min;

const MICROS_PER_HOUR: i64 = 3_600_000_000;
const MICROS_PER_DAY: i64 = 86_400_000_000;
const NANOS_PER_MICRO: i64 = 1_000;
const EPOCH_YEAR: i64 = 1970;

/// Murmur3 x86 32-bit hash (seed 0) of `bytes`, as Java's signed `int`.
pub fn hash_bytes(bytes: &[u8]) -> i32 {
    murmur3::murmur3_32(&mut &bytes[..], 0).expect("reading from a byte slice cannot fail") as i32
}

/// Hash of a long, time, timestamp or timestamptz: 8 bytes little-endian.
pub fn hash_long(value: i64) -> i32 {
    hash_bytes(&value.to_le_bytes())
}

/// Hash of an int or date: hashed as a long.
pub fn hash_int(value: i32) -> i32 {
    hash_long(i64::from(value))
}

/// Hash of a timestamp_ns / timestamptz_ns: hashed as microseconds.
pub fn hash_timestamp_nanos(nanos: i64) -> i32 {
    hash_long(nanos.div_euclid(NANOS_PER_MICRO))
}

/// Hash of a decimal's unscaled value: minimal big-endian two's complement.
pub fn hash_decimal(unscaled: i128) -> i32 {
    hash_bytes(&i128_to_be_bytes_min(unscaled))
}

/// Hash of a string: its UTF-8 bytes.
pub fn hash_str(value: &str) -> i32 {
    hash_bytes(value.as_bytes())
}

/// Hash of a uuid: its 16 bytes, big-endian.
pub fn hash_uuid(value: &Uuid) -> i32 {
    hash_bytes(value.as_bytes())
}

/// Bucket number of `hash` among `n` buckets: `(hash & i32::MAX) % n`.
///
/// The caller must ensure `n > 0`.
pub fn bucket(hash: i32, n: u32) -> i32 {
    ((i64::from(hash & i32::MAX)) % i64::from(n)) as i32
}

/// Proleptic Gregorian (year, month 1..=12) of the day `days` after 1970-01-01.
/// Howard Hinnant's `civil_from_days`; exact for every `i64` input.
fn civil_from_days(days: i64) -> (i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month)
}

// Results outside `i32` wrap, like Java's `(int)` cast.
fn years_from_days(days: i64) -> i32 {
    (civil_from_days(days).0 - EPOCH_YEAR) as i32
}

fn months_from_days(days: i64) -> i32 {
    let (year, month) = civil_from_days(days);
    ((year - EPOCH_YEAR) * 12 + month - 1) as i32
}

/// Years from 1970 of a date.
pub fn days_to_years(days: i32) -> i32 {
    years_from_days(i64::from(days))
}

/// Months from 1970-01 of a date.
pub fn days_to_months(days: i32) -> i32 {
    months_from_days(i64::from(days))
}

/// Years from 1970 of a timestamp in microseconds.
pub fn micros_to_years(micros: i64) -> i32 {
    years_from_days(micros.div_euclid(MICROS_PER_DAY))
}

/// Months from 1970-01 of a timestamp in microseconds.
pub fn micros_to_months(micros: i64) -> i32 {
    months_from_days(micros.div_euclid(MICROS_PER_DAY))
}

/// Days from 1970-01-01 of a timestamp in microseconds.
pub fn micros_to_days(micros: i64) -> i32 {
    micros.div_euclid(MICROS_PER_DAY) as i32
}

/// Hours from 1970-01-01T00:00 of a timestamp in microseconds.
pub fn micros_to_hours(micros: i64) -> i32 {
    micros.div_euclid(MICROS_PER_HOUR) as i32
}

/// Years from 1970 of a timestamp in nanoseconds.
pub fn nanos_to_years(nanos: i64) -> i32 {
    micros_to_years(nanos.div_euclid(NANOS_PER_MICRO))
}

/// Months from 1970-01 of a timestamp in nanoseconds.
pub fn nanos_to_months(nanos: i64) -> i32 {
    micros_to_months(nanos.div_euclid(NANOS_PER_MICRO))
}

/// Days from 1970-01-01 of a timestamp in nanoseconds.
pub fn nanos_to_days(nanos: i64) -> i32 {
    micros_to_days(nanos.div_euclid(NANOS_PER_MICRO))
}

/// Hours from 1970-01-01T00:00 of a timestamp in nanoseconds.
pub fn nanos_to_hours(nanos: i64) -> i32 {
    micros_to_hours(nanos.div_euclid(NANOS_PER_MICRO))
}

/// `value - (value mod width)`, wrapping like Java. The caller ensures `width > 0`.
pub fn truncate_int(value: i32, width: u32) -> i32 {
    let width = i64::from(width);
    (i64::from(value) - i64::from(value).rem_euclid(width)) as i32
}

/// `value - (value mod width)`, wrapping like Java. The caller ensures `width > 0`.
pub fn truncate_long(value: i64, width: u32) -> i64 {
    value.wrapping_sub(value.rem_euclid(i64::from(width)))
}

/// Truncates a decimal's unscaled value; the scale is unchanged.
/// The caller ensures `width > 0`.
pub fn truncate_decimal(unscaled: i128, width: u32) -> i128 {
    unscaled.wrapping_sub(unscaled.rem_euclid(i128::from(width)))
}

/// The first `width` Unicode code points of `value`.
pub fn truncate_str(value: &str, width: u32) -> &str {
    match value.char_indices().nth(width as usize) {
        Some((end, _)) => &value[..end],
        None => value,
    }
}

/// The first `width` bytes of `value`.
pub fn truncate_bytes(value: &[u8], width: u32) -> &[u8] {
    &value[..value.len().min(width as usize)]
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p iceberg-rust-spec --lib spec::transform`
Expected: 7 tests pass.

- [ ] **Step 5: Lint and commit**

```bash
cargo clippy -p iceberg-rust-spec --all-targets --all-features -- -D warnings
cargo fmt --all
git add iceberg-rust-spec/src/spec/transform.rs iceberg-rust-spec/src/spec/mod.rs
git commit -m "feat(spec): add spec-compliant scalar partition transforms

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DV558cLAwzTYg4nezLXrWG"
```

---

### Task 2: `Value::transform` on the scalar functions

**Files:**
- Modify: `iceberg-rust-spec/src/spec/values.rs` (`Value::transform` at ~line 379; `mod datetime` at ~line 911; imports at ~line 27; tests at ~lines 1519–1690)
- Modify: `iceberg-rust-spec/src/spec/types.rs:661`

**Interfaces:**
- Consumes: everything Task 1 produces.
- Produces: `Value::transform(&self, &Transform) -> Result<Value, Error>` — supported pairs return the spec result type (`Value::Int` for bucket and temporal; source variant for identity and truncate). Unsupported pairs → `Error::NotSupported`; width 0 → `Error::InvalidFormat`; `Transform::Void` → `Error::NotSupported`. A `Value::Decimal` is bucketed and truncated at its own scale: callers must cast it to the column's scale first.
- Produces: `Type::tranform(&Transform::Void)` returns `Ok(self.clone())`.

- [ ] **Step 1: Write the failing tests**

In `iceberg-rust-spec/src/spec/values.rs` tests module, change the three month tests' expectations (they assert the off-by-one values):

```rust
    #[test]
    fn test_transform_month_date() {
        let value = Value::Date(19478);
        let result = value.transform(&Transform::Month).unwrap();
        assert_eq!(result, Value::Int(640)); // 2023-05

        let value = Value::Date(19523);
        let result = value.transform(&Transform::Month).unwrap();
        assert_eq!(result, Value::Int(641)); // 2023-06

        let value = Value::Date(19723);
        let result = value.transform(&Transform::Month).unwrap();
        assert_eq!(result, Value::Int(648)); // 2024-01
    }

    #[test]
    fn test_transform_month_timestamp() {
        let value = Value::Timestamp(1682937000000000);
        let result = value.transform(&Transform::Month).unwrap();
        assert_eq!(result, Value::Int(640));

        let value = Value::Timestamp(1686840330000000);
        let result = value.transform(&Transform::Month).unwrap();
        assert_eq!(result, Value::Int(641));

        let value = Value::Timestamp(1704067200000000);
        let result = value.transform(&Transform::Month).unwrap();
        assert_eq!(result, Value::Int(648));
    }
```

Add these tests to the same module:

```rust
    #[test]
    fn transform_bucket_matches_spec_for_every_type() {
        use crate::spec::transform::{bucket, hash_long};
        let cases = [
            (Value::Int(34), 2017239379),
            (Value::LongInt(34), 2017239379),
            (Value::Date(17486), -653330422),
            (Value::Time(81_068_000_000), -662762989),
            (Value::Timestamp(1_510_871_468_000_000), -2047944441),
            (Value::TimestampTZ(1_510_871_468_000_000), -2047944441),
            (Value::String("iceberg".into()), 1210000089),
            (
                Value::UUID(Uuid::parse_str("f79c3e09-677c-4bbd-a479-3f349cb785e7").unwrap()),
                1488055340,
            ),
            (Value::Fixed(4, vec![0, 1, 2, 3]), -188683207),
            (Value::Binary(vec![0, 1, 2, 3]), -188683207),
            (
                Value::Decimal(decimal_from_i128_with_scale(1420, 2).unwrap()),
                -500754589,
            ),
        ];
        for (value, hash) in cases {
            assert_eq!(
                value.transform(&Transform::Bucket(10)).unwrap(),
                Value::Int(bucket(hash, 10)),
                "{value:?}"
            );
        }
        assert_eq!(hash_long(34), 2017239379);
    }

    #[test]
    fn transform_temporal_before_the_epoch() {
        assert_eq!(Value::Date(-1).transform(&Transform::Year).unwrap(), Value::Int(-1));
        assert_eq!(Value::Date(-1).transform(&Transform::Month).unwrap(), Value::Int(-1));
        assert_eq!(Value::Timestamp(-1).transform(&Transform::Day).unwrap(), Value::Int(-1));
        assert_eq!(Value::Timestamp(-1).transform(&Transform::Hour).unwrap(), Value::Int(-1));
        assert_eq!(Value::TimestampTZ(-1).transform(&Transform::Year).unwrap(), Value::Int(-1));
    }

    #[test]
    fn transform_truncate_all_supported_types() {
        assert_eq!(
            Value::String("éé".into()).transform(&Transform::Truncate(1)).unwrap(),
            Value::String("é".into())
        );
        assert_eq!(
            Value::Binary(vec![1, 2, 3]).transform(&Transform::Truncate(2)).unwrap(),
            Value::Binary(vec![1, 2])
        );
        assert_eq!(
            Value::Decimal(decimal_from_i128_with_scale(1065, 2).unwrap())
                .transform(&Transform::Truncate(50))
                .unwrap(),
            Value::Decimal(decimal_from_i128_with_scale(1050, 2).unwrap())
        );
        assert_eq!(Value::Int(-1).transform(&Transform::Truncate(10)).unwrap(), Value::Int(-10));
        assert_eq!(
            Value::LongInt(-1).transform(&Transform::Truncate(10)).unwrap(),
            Value::LongInt(-10)
        );
    }

    #[test]
    fn transform_rejects_zero_width_and_void() {
        assert!(matches!(
            Value::Int(1).transform(&Transform::Bucket(0)),
            Err(Error::InvalidFormat(_))
        ));
        assert!(matches!(
            Value::Int(1).transform(&Transform::Truncate(0)),
            Err(Error::InvalidFormat(_))
        ));
        assert!(matches!(
            Value::Int(1).transform(&Transform::Void),
            Err(Error::NotSupported(_))
        ));
        assert!(matches!(
            Value::Boolean(true).transform(&Transform::Bucket(4)),
            Err(Error::NotSupported(_))
        ));
    }
```

In `iceberg-rust-spec/src/spec/types.rs` tests module add:

```rust
    #[test]
    fn void_transform_keeps_the_source_type() {
        let source = Type::Primitive(PrimitiveType::Date);
        assert_eq!(source.tranform(&Transform::Void).unwrap(), source);
    }
```

(If `Transform` is not imported in that test module, add `use crate::spec::partition::Transform;`.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p iceberg-rust-spec --lib -- transform`
Expected: FAIL — month tests report `left: Int(641) right: Int(640)`; `transform_temporal_before_the_epoch` panics in `years_since(...).unwrap()`; `transform_truncate_all_supported_types` panics with `is_char_boundary`; bucket cases mismatch; `void_transform_keeps_the_source_type` fails with `Err(NotSupported("void transform"))`.

- [ ] **Step 3: Replace `Value::transform`**

Replace the whole body of `pub fn transform(&self, transform: &Transform) -> Result<Value, Error>` in `values.rs` with:

```rust
    pub fn transform(&self, transform: &Transform) -> Result<Value, Error> {
        use super::transform as t;

        let unsupported = || Error::NotSupported(format!("{transform} transform of {self:?}"));
        match (transform, self) {
            (Transform::Bucket(0) | Transform::Truncate(0), _) => Err(Error::InvalidFormat(
                format!("{transform} needs a width greater than zero"),
            )),
            (Transform::Identity, value) => Ok(value.clone()),
            (Transform::Bucket(n), value) => {
                let hash = match value {
                    Value::Int(v) | Value::Date(v) => t::hash_int(*v),
                    Value::LongInt(v)
                    | Value::Time(v)
                    | Value::Timestamp(v)
                    | Value::TimestampTZ(v) => t::hash_long(*v),
                    Value::Decimal(v) => t::hash_decimal(decimal_mantissa(v)?),
                    Value::String(v) => t::hash_str(v),
                    Value::UUID(v) => t::hash_uuid(v),
                    Value::Fixed(_, v) | Value::Binary(v) => t::hash_bytes(v),
                    _ => return Err(unsupported()),
                };
                Ok(Value::Int(t::bucket(hash, *n)))
            }
            (Transform::Truncate(w), Value::Int(v)) => Ok(Value::Int(t::truncate_int(*v, *w))),
            (Transform::Truncate(w), Value::LongInt(v)) => {
                Ok(Value::LongInt(t::truncate_long(*v, *w)))
            }
            (Transform::Truncate(w), Value::Decimal(v)) => {
                Ok(Value::Decimal(decimal_from_i128_with_scale(
                    t::truncate_decimal(decimal_mantissa(v)?, *w),
                    decimal_scale(v),
                )?))
            }
            (Transform::Truncate(w), Value::String(v)) => {
                Ok(Value::String(t::truncate_str(v, *w).to_owned()))
            }
            (Transform::Truncate(w), Value::Binary(v)) => {
                Ok(Value::Binary(t::truncate_bytes(v, *w).to_vec()))
            }
            (Transform::Year, Value::Date(v)) => Ok(Value::Int(t::days_to_years(*v))),
            (Transform::Month, Value::Date(v)) => Ok(Value::Int(t::days_to_months(*v))),
            (Transform::Day, Value::Date(v)) => Ok(Value::Int(*v)),
            (Transform::Year, Value::Timestamp(v) | Value::TimestampTZ(v)) => {
                Ok(Value::Int(t::micros_to_years(*v)))
            }
            (Transform::Month, Value::Timestamp(v) | Value::TimestampTZ(v)) => {
                Ok(Value::Int(t::micros_to_months(*v)))
            }
            (Transform::Day, Value::Timestamp(v) | Value::TimestampTZ(v)) => {
                Ok(Value::Int(t::micros_to_days(*v)))
            }
            (Transform::Hour, Value::Timestamp(v) | Value::TimestampTZ(v)) => {
                Ok(Value::Int(t::micros_to_hours(*v)))
            }
            _ => Err(unsupported()),
        }
    }
```

Update the doc comment above it: replace the "Supported transforms include" list with:

```rust
    /// Follows the Iceberg spec (see [`super::transform`]). A decimal is
    /// bucketed and truncated at its own scale, so cast it to the column's
    /// scale first. `Transform::Void` has no value and returns
    /// `Error::NotSupported`; width 0 returns `Error::InvalidFormat`.
```

Remove the now-unused helpers: delete `date_to_years`, `date_to_months`, `datetime_to_months`, `datetime_to_days` and `datetime_to_hours` from `mod datetime`, and remove them from the `use datetime::{...}` import at the top of the file. Run `cargo build -p iceberg-rust-spec`; if the compiler reports another remaining use of one of them, keep that one.

In `iceberg-rust-spec/src/spec/types.rs`, change the `Transform::Void` arm of `tranform`:

```rust
            Transform::Void => Ok(self.clone()),
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p iceberg-rust-spec --lib`
Expected: all tests pass.

- [ ] **Step 5: Lint and commit**

```bash
cargo clippy -p iceberg-rust-spec --all-targets --all-features -- -D warnings
cargo fmt --all
git add iceberg-rust-spec/src/spec/values.rs iceberg-rust-spec/src/spec/types.rs
git commit -m "fix(spec): make Value::transform spec-compliant

Bucket hashes ints/dates as longs and masks the sign bit; month is
0-based; pre-1970 dates and timestamps floor; truncate counts code
points and supports decimal and binary.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DV558cLAwzTYg4nezLXrWG"
```

---

### Task 3: Arrow transform kernels

**Files:**
- Create: `iceberg-rust/src/arrow/value.rs`
- Rewrite: `iceberg-rust/src/arrow/transform.rs`
- Modify: `iceberg-rust/src/arrow/mod.rs` (add `pub mod value;`)
- Modify: `iceberg-rust/Cargo.toml` (add `ordered-float = "5.3.0"` under `[dependencies]`)
- Modify: `iceberg-rust/src/arrow/partition.rs:61` (call-site signature)
- Modify: `datafusion_iceberg/src/pruning_statistics.rs:461` (call-site signature)

**Interfaces:**
- Consumes: Task 1 functions; `Value`, `Type`, `PrimitiveType`, `decimal_from_i128_with_scale`.
- Produces:
  - `iceberg_rust::arrow::value::arrow_value(array: &dyn Array, row: usize, ty: &Type) -> Result<Option<Value>, ArrowError>` — `None` for a null cell.
  - `iceberg_rust::arrow::transform::transform_arrow(array: ArrayRef, transform: &Transform, source_type: &Type) -> Result<ArrayRef, ArrowError>` — result Int32 for bucket/temporal, source type for identity/truncate (Int8/Int16 normalised to Int32), null array of the source type for void.

- [ ] **Step 1: Write `value.rs` with its test, and the new kernel tests**

Create `iceberg-rust/src/arrow/value.rs`:

```rust
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
        (PrimitiveType::Boolean, DataType::Boolean) => Value::Boolean(array.as_boolean().value(row)),
        (PrimitiveType::Int, DataType::Int8) => {
            Value::Int(array.as_primitive::<Int8Type>().value(row).into())
        }
        (PrimitiveType::Int, DataType::Int16) => {
            Value::Int(array.as_primitive::<Int16Type>().value(row).into())
        }
        (PrimitiveType::Int, DataType::Int32) => Value::Int(array.as_primitive::<Int32Type>().value(row)),
        (PrimitiveType::Long, DataType::Int64) => {
            Value::LongInt(array.as_primitive::<Int64Type>().value(row))
        }
        (PrimitiveType::Float, DataType::Float32) => {
            Value::Float(OrderedFloat(array.as_primitive::<Float32Type>().value(row)))
        }
        (PrimitiveType::Double, DataType::Float64) => {
            Value::Double(OrderedFloat(array.as_primitive::<Float64Type>().value(row)))
        }
        (PrimitiveType::Date, DataType::Date32) => Value::Date(array.as_primitive::<Date32Type>().value(row)),
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
        (PrimitiveType::Fixed(len), DataType::FixedSizeBinary(_)) => {
            Value::Fixed(*len as usize, array.as_fixed_size_binary().value(row).to_vec())
        }
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
        assert_eq!(arrow_value(&dates, 0, &date).unwrap(), Some(Value::Date(19478)));
        assert_eq!(arrow_value(&dates, 1, &date).unwrap(), None);

        let uuids: ArrayRef = Arc::new(StringArray::from(vec!["f79c3e09-677c-4bbd-a479-3f349cb785e7"]));
        let uuid = Type::Primitive(PrimitiveType::Uuid);
        assert!(matches!(arrow_value(&uuids, 0, &uuid).unwrap(), Some(Value::UUID(_))));

        let decimals: ArrayRef =
            Arc::new(Decimal128Array::from(vec![1065]).with_precision_and_scale(9, 2).unwrap());
        let decimal = Type::Primitive(PrimitiveType::Decimal { precision: 9, scale: 2 });
        assert_eq!(
            arrow_value(&decimals, 0, &decimal).unwrap(),
            Some(Value::Decimal(decimal_from_i128_with_scale(1065, 2).unwrap()))
        );
    }

    #[test]
    fn rejects_mismatched_types_and_bad_uuids() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec!["not-a-uuid"]));
        assert!(arrow_value(&strings, 0, &Type::Primitive(PrimitiveType::Uuid)).is_err());
        assert!(arrow_value(&strings, 0, &Type::Primitive(PrimitiveType::Long)).is_err());
    }
}
```

In `iceberg-rust/src/arrow/mod.rs`, add `pub mod value;` next to `pub mod transform;`. In `iceberg-rust/Cargo.toml` `[dependencies]`, add `ordered-float = "5.3.0"`.

In `iceberg-rust/src/arrow/transform.rs`, delete the whole existing `#[cfg(test)] mod tests { ... }` block (its month expectations are the old off-by-one values and its calls use the old signature) and add this new test module at the end of the file:

```rust
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
        let temporal = [Transform::Year, Transform::Month, Transform::Day, Transform::Hour];
        assert_parity(
            Arc::new(Int32Array::from(vec![Some(34), Some(-1), Some(i32::MIN), None])),
            primitive(PrimitiveType::Int),
            &[Transform::Identity, Transform::Bucket(10), Transform::Truncate(10)],
        );
        assert_parity(
            Arc::new(Int64Array::from(vec![Some(34), Some(-1), Some(i64::MIN), None])),
            primitive(PrimitiveType::Long),
            &[Transform::Identity, Transform::Bucket(10), Transform::Truncate(10)],
        );
        assert_parity(
            Arc::new(Date32Array::from(vec![Some(17486), Some(-1), Some(0), None])),
            primitive(PrimitiveType::Date),
            &[Transform::Identity, Transform::Bucket(10), Transform::Year, Transform::Month, Transform::Day],
        );
        assert_parity(
            Arc::new(Time64MicrosecondArray::from(vec![Some(81_068_000_000), None])),
            primitive(PrimitiveType::Time),
            &[Transform::Identity, Transform::Bucket(10)],
        );
        let micros = vec![Some(1_510_871_468_000_000), Some(-1), Some(0), None];
        assert_parity(
            Arc::new(TimestampMicrosecondArray::from(micros.clone())),
            primitive(PrimitiveType::Timestamp),
            &[&[Transform::Identity, Transform::Bucket(10)][..], &temporal[..]].concat(),
        );
        assert_parity(
            Arc::new(TimestampMicrosecondArray::from(micros).with_timezone_utc()),
            primitive(PrimitiveType::Timestamptz),
            &[&[Transform::Identity, Transform::Bucket(10)][..], &temporal[..]].concat(),
        );
        let strings = vec![Some("iceberg"), Some("éé"), None];
        let string_transforms = [Transform::Identity, Transform::Bucket(10), Transform::Truncate(1)];
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
            Arc::new(StringArray::from(vec![Some("f79c3e09-677c-4bbd-a479-3f349cb785e7"), None])),
            primitive(PrimitiveType::Uuid),
            &[Transform::Identity, Transform::Bucket(10)],
        );
        assert_parity(
            Arc::new(BinaryArray::from(vec![Some(&[0u8, 1, 2, 3][..]), None])),
            primitive(PrimitiveType::Binary),
            &[Transform::Identity, Transform::Bucket(10), Transform::Truncate(2)],
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
            primitive(PrimitiveType::Decimal { precision: 9, scale: 2 }),
            &[Transform::Identity, Transform::Bucket(10), Transform::Truncate(50)],
        );
    }

    #[test]
    fn nanosecond_timestamps_hash_and_floor_as_micros() {
        let nanos: ArrayRef =
            Arc::new(TimestampNanosecondArray::from(vec![Some(1_510_871_468_000_001_001), Some(-1)]));
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
        let result =
            transform_arrow(strings, &Transform::Bucket(4), &primitive(PrimitiveType::String)).unwrap();
        assert!(result.is_null(0));
        assert!(result.is_valid(1));
    }

    #[test]
    fn uuid_is_bucketed_by_its_bytes_not_its_text() {
        let text = "f79c3e09-677c-4bbd-a479-3f349cb785e7";
        let array: ArrayRef = Arc::new(StringArray::from(vec![text]));
        let as_uuid =
            transform_arrow(array.clone(), &Transform::Bucket(1000), &primitive(PrimitiveType::Uuid)).unwrap();
        let as_string =
            transform_arrow(array, &Transform::Bucket(1000), &primitive(PrimitiveType::String)).unwrap();
        assert_eq!(as_uuid.as_primitive::<Int32Type>().value(0), bucket(1488055340, 1000));
        assert_ne!(
            as_uuid.as_primitive::<Int32Type>().value(0),
            as_string.as_primitive::<Int32Type>().value(0)
        );
        let bad: ArrayRef = Arc::new(StringArray::from(vec!["not-a-uuid"]));
        assert!(transform_arrow(bad, &Transform::Bucket(4), &primitive(PrimitiveType::Uuid)).is_err());
    }

    #[test]
    fn void_returns_nulls_of_the_source_type() {
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let result = transform_arrow(ints, &Transform::Void, &primitive(PrimitiveType::Long)).unwrap();
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
        assert!(transform_arrow(bools, &Transform::Bucket(4), &primitive(PrimitiveType::Boolean)).is_err());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p iceberg-rust --lib arrow::`
Expected: compile error — `transform_arrow` takes 2 arguments but 3 were supplied.

- [ ] **Step 3: Rewrite the kernels**

Replace everything in `iceberg-rust/src/arrow/transform.rs` above the test module with:

```rust
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
        DataType::Time64(TimeUnit::Microsecond) => {
            array.as_primitive::<Time64MicrosecondType>().unary(t::hash_long)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            array.as_primitive::<TimestampMicrosecondType>().unary(t::hash_long)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => array
            .as_primitive::<TimestampNanosecondType>()
            .unary(t::hash_timestamp_nanos),
        DataType::Decimal128(_, _) => array.as_primitive::<Decimal128Type>().unary(t::hash_decimal),
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
        DataType::Utf8 => array.as_string::<i32>().iter().map(|v| v.map(t::hash_str)).collect(),
        DataType::LargeUtf8 => array.as_string::<i64>().iter().map(|v| v.map(t::hash_str)).collect(),
        DataType::Utf8View => array.as_string_view().iter().map(|v| v.map(t::hash_str)).collect(),
        DataType::Binary => array.as_binary::<i32>().iter().map(|v| v.map(t::hash_bytes)).collect(),
        DataType::LargeBinary => {
            array.as_binary::<i64>().iter().map(|v| v.map(t::hash_bytes)).collect()
        }
        DataType::BinaryView => array.as_binary_view().iter().map(|v| v.map(t::hash_bytes)).collect(),
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
        (DataType::Timestamp(TimeUnit::Microsecond, _), Transform::Year) => micros().unary(t::micros_to_years),
        (DataType::Timestamp(TimeUnit::Microsecond, _), Transform::Month) => micros().unary(t::micros_to_months),
        (DataType::Timestamp(TimeUnit::Microsecond, _), Transform::Day) => micros().unary(t::micros_to_days),
        (DataType::Timestamp(TimeUnit::Microsecond, _), Transform::Hour) => micros().unary(t::micros_to_hours),
        (DataType::Timestamp(TimeUnit::Nanosecond, _), Transform::Year) => nanos().unary(t::nanos_to_years),
        (DataType::Timestamp(TimeUnit::Nanosecond, _), Transform::Month) => nanos().unary(t::nanos_to_months),
        (DataType::Timestamp(TimeUnit::Nanosecond, _), Transform::Day) => nanos().unary(t::nanos_to_days),
        (DataType::Timestamp(TimeUnit::Nanosecond, _), Transform::Hour) => nanos().unary(t::nanos_to_hours),
        _ => return None,
    };
    Some(Arc::new(result))
}
```

If the compiler cannot infer the output type of a `.unary(...)` call inside `bucket_hashes` or `temporal`, add the turbofish `.unary::<_, Int32Type>(...)`.

Update the two callers:

`iceberg-rust/src/arrow/partition.rs` (inside `partition_record_batch`):

```rust
            let transformed =
                transform_arrow(array.clone(), field.transform(), field.field_type())?;
```

`datafusion_iceberg/src/pruning_statistics.rs`, in `DateTransform::invoke_with_args`, the `ColumnarValue::Array` arm (this UDF is deleted in Task 10):

```rust
            ColumnarValue::Array(array) => {
                let source_type = iceberg_rust::spec::types::Type::try_from(array.data_type())
                    .map_err(|err| DataFusionError::External(Box::new(err)))?;
                Ok(ColumnarValue::Array(transform_arrow(
                    array.clone(),
                    &transform,
                    &source_type,
                )?))
            }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p iceberg-rust --lib arrow::`
Expected: all `arrow::transform` and `arrow::value` tests pass.
Run: `cargo build -p datafusion_iceberg`
Expected: builds.

- [ ] **Step 5: Lint and commit**

```bash
cargo clippy -p iceberg-rust -p datafusion_iceberg --all-targets --all-features -- -D warnings
cargo fmt --all
git add iceberg-rust/Cargo.toml Cargo.lock iceberg-rust/src/arrow datafusion_iceberg/src/pruning_statistics.rs
git commit -m "fix(arrow): spec-compliant Arrow transform kernels for every type

transform_arrow now delegates to the spec crate's scalar functions,
takes the source Iceberg type (uuid columns are Arrow Utf8), keeps
nulls, and supports bucket/truncate/temporal on every mapped type.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DV558cLAwzTYg4nezLXrWG"
```

---

### Task 4: Partition rows by `RowConverter`, keeping NULL partitions

**Files:**
- Rewrite: `iceberg-rust/src/arrow/partition.rs`
- Modify: `iceberg-rust/src/arrow/write.rs` (`LruCache<Vec<Value>, …>` at ~line 336; `generate_partition_path` at ~line 596; its test at ~line 1496)
- Modify: `datafusion_iceberg/src/table/mod.rs` (`partitions_demuxer`, ~line 1820)
- Create: `datafusion_iceberg/tests/tier1_regressions.rs`

**Interfaces:**
- Consumes: `transform_arrow(array, transform, source_type)`, `arrow_value(array, row, ty)` (Task 3); `Type::tranform` (Task 2).
- Produces:
  - `partition_record_batch(&RecordBatch, &[BoundPartitionField<'_>]) -> Result<impl Iterator<Item = Result<(Vec<Option<Value>>, RecordBatch), ArrowError>> + '_, ArrowError>`
  - `generate_partition_path(&[BoundPartitionField<'_>], &[Option<Value>]) -> Result<String, ArrowError>` — `None` renders as `null`.
  - Test helper `Fixture` in `datafusion_iceberg/tests/tier1_regressions.rs` with `new()`, `create_table(name, Vec<PartitionField>)`, `sql(&str) -> Vec<RecordBatch>`, `ids(&str) -> Vec<i64>`, `try_ids(&str) -> Result<Vec<i64>, String>`. Table columns: `id long NOT NULL (1), n long (2), s string (3), d date (4), ts timestamp (5), amount decimal(9,2) (6)`.

- [ ] **Step 1: Write the failing tests**

Replace the `partition.rs` module with this skeleton holding only the tests (the implementation comes in Step 3):

```rust
//! Splitting Arrow record batches by Iceberg partition tuple.

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
                Arc::new(Date32Array::from(vec![Some(19478), None, Some(19478), Some(0)])),
            ],
        )
        .unwrap()
    }

    /// (partition tuple, ids in that partition), ordered by first id.
    fn partitions(batch: &RecordBatch, transform: Transform) -> Vec<(Vec<Option<Value>>, Vec<i64>)> {
        let source = StructField::new(2, "d", false, Type::Primitive(PrimitiveType::Date), None);
        let field = PartitionField::new(2, 1000, "d_part", transform);
        let bound = [BoundPartitionField::new(&field, &source)];
        let mut result: Vec<_> = partition_record_batch(batch, &bound)
            .unwrap()
            .map(|partition| {
                let (values, rows) = partition.unwrap();
                (values, rows.column(0).as_primitive::<Int64Type>().values().to_vec())
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
```

In `iceberg-rust/src/arrow/write.rs` tests, change the `generate_partition_path` test input at ~line 1496 from `vec![Value::Int(10)]` to `vec![Some(Value::Int(10))]`, and add:

```rust
    #[test]
    fn partition_path_renders_null_values() {
        let source = StructField::new(1, "n", false, Type::Primitive(PrimitiveType::Long), None);
        let field = PartitionField::new(1, 1000, "n", Transform::Identity);
        let fields = [BoundPartitionField::new(&field, &source)];
        assert_eq!(super::generate_partition_path(&fields, &[None]).unwrap(), "n=null/");
    }
```

(Import `StructField`, `PartitionField`, `BoundPartitionField`, `Transform`, `Type`, `PrimitiveType` in that test module if they are not already imported.)

Create `datafusion_iceberg/tests/tier1_regressions.rs`:

```rust
//! Regression tests for the Tier 1 correctness fixes
//! (docs/superpowers/specs/2026-10-02-tier1-correctness-fixes-design.md).

use std::sync::Arc;

use datafusion::{
    arrow::{array::AsArray, datatypes::Int64Type, record_batch::RecordBatch},
    prelude::SessionContext,
};
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use iceberg_rust::{
    catalog::Catalog,
    object_store::ObjectStoreBuilder,
    spec::{
        namespace::Namespace,
        partition::{PartitionField, PartitionSpec, Transform},
        schema::Schema,
        types::{PrimitiveType, StructField, Type},
    },
    table::Table,
};
use iceberg_sql_catalog::SqlCatalog;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// A SQLite-catalog warehouse with namespace `test`, registered as `warehouse`.
struct Fixture {
    dir: TempDir,
    catalog: Arc<dyn Catalog>,
    ctx: SessionContext,
}

impl Fixture {
    async fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let object_store = ObjectStoreBuilder::Filesystem(Arc::new(LocalFileSystem::new()));
        let catalog: Arc<dyn Catalog> =
            Arc::new(SqlCatalog::new("sqlite://", "warehouse", object_store).await.unwrap());
        catalog
            .create_namespace(&Namespace::try_new(&["test".to_string()]).unwrap(), None)
            .await
            .unwrap();
        Fixture {
            dir,
            catalog,
            ctx: SessionContext::new(),
        }
    }

    /// Creates `warehouse.test.<name>`: `id long NOT NULL, n long, s string,
    /// d date, ts timestamp, amount decimal(9,2)` (field ids 1..=6).
    async fn create_table(&self, name: &str, partition_fields: Vec<PartitionField>) {
        let field = |id: i32, name: &str, required: bool, ty: PrimitiveType| StructField {
            id,
            name: name.to_string(),
            required,
            field_type: Type::Primitive(ty),
            doc: None,
            initial_default: None,
            write_default: None,
        };
        let schema = Schema::builder()
            .with_struct_field(field(1, "id", true, PrimitiveType::Long))
            .with_struct_field(field(2, "n", false, PrimitiveType::Long))
            .with_struct_field(field(3, "s", false, PrimitiveType::String))
            .with_struct_field(field(4, "d", false, PrimitiveType::Date))
            .with_struct_field(field(5, "ts", false, PrimitiveType::Timestamp))
            .with_struct_field(field(
                6,
                "amount",
                false,
                PrimitiveType::Decimal { precision: 9, scale: 2 },
            ))
            .build()
            .unwrap();
        Table::builder()
            .with_name(name)
            .with_location(&format!("{}/test/{name}", self.dir.path().to_str().unwrap()))
            .with_schema(schema)
            .with_partition_spec(PartitionSpec::builder().with_fields(partition_fields).build().unwrap())
            .build(&["test".to_owned()], self.catalog.clone())
            .await
            .unwrap();
        // Re-register so DataFusion sees the new table.
        self.ctx.register_catalog(
            "warehouse",
            Arc::new(IcebergCatalog::new(self.catalog.clone(), None).await.unwrap()),
        );
    }

    async fn sql(&self, sql: &str) -> Vec<RecordBatch> {
        self.ctx
            .sql(sql)
            .await
            .unwrap_or_else(|err| panic!("planning `{sql}`: {err}"))
            .collect()
            .await
            .unwrap_or_else(|err| panic!("executing `{sql}`: {err}"))
    }

    /// The first column (Int64) of every result row; errors as text.
    async fn try_ids(&self, sql: &str) -> Result<Vec<i64>, String> {
        let batches = self
            .ctx
            .sql(sql)
            .await
            .map_err(|err| err.to_string())?
            .collect()
            .await
            .map_err(|err| err.to_string())?;
        Ok(batches
            .iter()
            .flat_map(|batch| {
                batch.column(0).as_primitive::<Int64Type>().iter().flatten().collect::<Vec<_>>()
            })
            .collect())
    }

    async fn ids(&self, sql: &str) -> Vec<i64> {
        self.try_ids(sql).await.unwrap_or_else(|err| panic!("`{sql}`: {err}"))
    }
}

#[tokio::test]
async fn insert_keeps_rows_with_null_partition_source() {
    let f = Fixture::new().await;
    f.create_table("t", vec![PartitionField::new(2, 1000, "n", Transform::Identity)])
        .await;
    f.sql("INSERT INTO warehouse.test.t (id, n) VALUES (1, 5), (2, NULL), (3, 0)")
        .await;
    f.sql("INSERT INTO warehouse.test.t (id, n) VALUES (4, NULL)").await;
    assert_eq!(f.ids("SELECT id FROM warehouse.test.t ORDER BY id").await, vec![1, 2, 3, 4]);
    assert_eq!(
        f.ids("SELECT id FROM warehouse.test.t WHERE n IS NULL ORDER BY id").await,
        vec![2, 4]
    );
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p iceberg-rust --lib arrow::partition`
Expected: compile error — `partition_record_batch` not found (skeleton has no implementation).
Run: `cargo test -p datafusion_iceberg --test tier1_regressions insert_keeps_rows_with_null_partition_source`
Expected: FAIL (after restoring a compiling `partition.rs` this test fails with `left: [1, 3] right: [1, 2, 3, 4]`; until Step 3 the crate does not compile, which also counts as failing).

- [ ] **Step 3: Implement**

Put the implementation above the test module in `iceberg-rust/src/arrow/partition.rs`:

```rust
//! Splitting Arrow record batches by Iceberg partition tuple.
//!
//! Rows are grouped by their transformed partition columns using Arrow's
//! row format, so every type groups the same way and NULL is an ordinary
//! partition value.

use std::collections::HashMap;

use arrow::{
    array::{ArrayRef, UInt32Array},
    compute::take_record_batch,
    error::ArrowError,
    record_batch::RecordBatch,
    row::{RowConverter, SortField},
};
use iceberg_rust_spec::{partition::BoundPartitionField, spec::values::Value};

use super::{transform::transform_arrow, value::arrow_value};

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
) -> Result<
    impl Iterator<Item = Result<(Vec<Option<Value>>, RecordBatch), ArrowError>> + 'a,
    ArrowError,
> {
    let columns: Vec<ArrayRef> = partition_fields
        .iter()
        .map(|field| {
            let source = record_batch.column_by_name(field.source_name()).ok_or_else(|| {
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
```

In `iceberg-rust/src/arrow/write.rs`:

- Change `generate_partition_path`:

```rust
pub fn generate_partition_path(
    partition_fields: &[BoundPartitionField<'_>],
    partition_values: &[Option<Value>],
) -> Result<String, ArrowError> {
    partition_fields
        .iter()
        .zip(partition_values.iter())
        .map(|(field, value)| {
            let value = value.as_ref().map_or_else(|| "null".to_owned(), ToString::to_string);
            Ok(field.name().to_owned() + "=" + &value + "/")
        })
        .collect::<Result<String, ArrowError>>()
}
```

- Change the sender cache type to `LruCache<Vec<Option<Value>>, Sender<Result<RecordBatch, ArrowError>>>`.

In `datafusion_iceberg/src/table/mod.rs`, `partitions_demuxer`: change `LruCache<Vec<Value>, mpsc::Sender<RecordBatch>>` to `LruCache<Vec<Option<Value>>, mpsc::Sender<RecordBatch>>`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p iceberg-rust --lib arrow::`
Expected: PASS.
Run: `cargo test -p datafusion_iceberg --test tier1_regressions`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
cargo clippy -p iceberg-rust -p datafusion_iceberg --all-targets --all-features -- -D warnings
cargo fmt --all
git add iceberg-rust/src/arrow/partition.rs iceberg-rust/src/arrow/write.rs datafusion_iceberg/src/table/mod.rs datafusion_iceberg/tests/tier1_regressions.rs
git commit -m "fix(write): keep rows with NULL partition values

partition_record_batch dropped rows whose partition source was NULL and
only supported int/long/string partition values. Group rows with Arrow's
row format instead and return Option<Value> partition tuples.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DV558cLAwzTYg4nezLXrWG"
```

---

### Task 5: Carry the partition tuple into the data file

**Files:**
- Modify: `iceberg-rust/src/file_format/parquet.rs` (`parquet_to_datafile` at line 67; partition derivation at ~lines 345–378; tests at ~line 647)
- Modify: `iceberg-rust/src/arrow/write.rs` (`write_parquet_files` at ~line 450 and its two callers; `parquet_to_datafile` call at ~line 564)
- Modify: `datafusion_iceberg/src/table/mod.rs` (`write_parquet_files` ~line 1679, `start_demuxer_task` ~line 1778, `partitions_demuxer`)
- Modify: `datafusion_iceberg/tests/tier1_regressions.rs`

**Interfaces:**
- Consumes: `partition_record_batch` tuples (Task 4).
- Produces: `parquet_to_datafile(location: &str, file_size: u64, file_metadata: &ParquetMetaData, schema: &Schema, partition_fields: &[BoundPartitionField<'_>], partition_values: Option<&[Option<Value>]>, equality_ids: Option<&[i32]>, table_properties: &HashMap<String, String>) -> Result<DataFile, Error>`. With `Some`, the tuple is used as-is. With `None`, values are derived from exact statistics only; inexact statistics or min/max transforming to different values → `Error::InvalidFormat`; a column with no min/max is `None` only if every row is null, otherwise `Error::InvalidFormat`.
- Produces: `start_demuxer_task(...) -> Result<(SpawnedTask<…>, DemuxedStreamReceiver, PartitionValuesByPath), DataFusionError>` where `type PartitionValuesByPath = Arc<std::sync::Mutex<HashMap<object_store::path::Path, Vec<Option<Value>>>>>`.

- [ ] **Step 1: Write the failing tests**

Add to the `#[cfg(test)]` module of `iceberg-rust/src/file_format/parquet.rs`:

```rust
    /// One Utf8 column `s` (field id 1) holding `values`.
    fn string_file(values: Vec<Option<&str>>) -> (Schema, u64, ParquetMetaData) {
        use std::sync::Arc;

        use arrow::{
            array::StringArray,
            datatypes::{DataType, Field, Schema as ArrowSchema},
            record_batch::RecordBatch,
        };
        use iceberg_rust_spec::spec::types::{PrimitiveType, StructField};
        use parquet::{
            arrow::ArrowWriter,
            file::reader::{FileReader, SerializedFileReader},
        };

        let schema = Schema::builder()
            .with_struct_field(StructField::new(1, "s", false, Type::Primitive(PrimitiveType::String), None))
            .build()
            .unwrap();
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let batch =
            RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(StringArray::from(values))]).unwrap();
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, arrow_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let size = buffer.len() as u64;
        let reader = SerializedFileReader::new(bytes::Bytes::from(buffer)).unwrap();
        (schema, size, reader.metadata().clone())
    }

    fn identity_on_s(schema: &Schema) -> (PartitionField, iceberg_rust_spec::spec::types::StructField) {
        let source = schema.fields().get(1).unwrap().clone();
        (
            PartitionField::new(1, 1000, "s", iceberg_rust_spec::spec::partition::Transform::Identity),
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
            "/t/data/1.parquet", size, &metadata, &schema, &bound, Some(&values), None, &HashMap::new(),
        )
        .unwrap();
        assert_eq!(datafile.partition().get("s"), Some(&Some(Value::String(long))));
    }

    #[test]
    fn derived_partition_values_reject_truncated_statistics() {
        let long = "x".repeat(100); // parquet truncates statistics to 64 bytes
        let (schema, size, metadata) = string_file(vec![Some(long.as_str())]);
        let (field, source) = identity_on_s(&schema);
        let bound = [BoundPartitionField::new(&field, &source)];
        assert!(parquet_to_datafile(
            "/t/data/1.parquet", size, &metadata, &schema, &bound, None, None, &HashMap::new(),
        )
        .is_err());
    }

    #[test]
    fn derived_partition_value_is_null_for_an_all_null_column() {
        let (schema, size, metadata) = string_file(vec![None, None]);
        let (field, source) = identity_on_s(&schema);
        let bound = [BoundPartitionField::new(&field, &source)];
        let datafile = parquet_to_datafile(
            "/t/data/1.parquet", size, &metadata, &schema, &bound, None, None, &HashMap::new(),
        )
        .unwrap();
        assert_eq!(datafile.partition().get("s"), Some(&None));
    }
```

Update the existing test call at ~line 647 to the new signature (insert `None,` after `&[],`).

Add to `datafusion_iceberg/tests/tier1_regressions.rs`:

```rust
#[tokio::test]
async fn insert_identity_partition_on_a_long_string() {
    let f = Fixture::new().await;
    f.create_table("t", vec![PartitionField::new(3, 1000, "s", Transform::Identity)])
        .await;
    let long = "x".repeat(100);
    f.sql(&format!("INSERT INTO warehouse.test.t (id, s) VALUES (1, '{long}')"))
        .await;
    assert_eq!(
        f.ids(&format!("SELECT id FROM warehouse.test.t WHERE s = '{long}'")).await,
        vec![1]
    );
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p iceberg-rust --lib file_format::parquet`
Expected: compile error — `parquet_to_datafile` takes 7 arguments but 8 were supplied.
Run: `cargo test -p datafusion_iceberg --test tier1_regressions insert_identity_partition_on_a_long_string`
Expected: FAIL — panics `executing ... Partition value of data file` (min and max statistics differ after truncation).

- [ ] **Step 3: Implement**

In `iceberg-rust/src/file_format/parquet.rs`:

1. Add the parameter `partition_values: Option<&[Option<Value>]>` after `partition_fields` in `parquet_to_datafile`.
2. Replace the initial `let mut partition = ...` with:

```rust
    let mut partition = match partition_values {
        Some(values) => partition_fields
            .iter()
            .zip(values)
            .map(|(field, value)| (field.name().to_owned(), value.clone()))
            .collect::<Struct>(),
        None => partition_fields
            .iter()
            .map(|field| (field.name().to_owned(), None))
            .collect::<Struct>(),
    };
    let derive_partition = partition_values.is_none();
    // Null counts of partition source columns, for the derivation check below.
    let mut partition_source_null_counts: HashMap<String, i64> = HashMap::new();
```

3. Replace the block that starts `if let Some(partition_field) = partition_fields.get(column_name) {` (inside the per-column loop) with:

```rust
                if let Some(partition_field) =
                    partition_fields.get(column_name).filter(|_| derive_partition)
                {
                    *partition_source_null_counts
                        .entry(column_name.to_owned())
                        .or_default() += statistics.null_count_opt().unwrap_or(0) as i64;
                    let unset = matches!(partition.get(partition_field.name()), Some(None));
                    if let (true, Some(min_bytes), Some(max_bytes)) =
                        (unset, statistics.min_bytes_opt(), statistics.max_bytes_opt())
                    {
                        if !(statistics.min_is_exact() && statistics.max_is_exact()) {
                            return Err(Error::InvalidFormat(format!(
                                "cannot derive the partition value of {location}: statistics of column {column_name} are truncated"
                            )));
                        }
                        let transform = partition_field.transform();
                        let min = Value::try_from_bytes_with_hint(min_bytes, data_type, physical_type_hint)?
                            .transform(transform)?;
                        let max = Value::try_from_bytes_with_hint(max_bytes, data_type, physical_type_hint)?
                            .transform(transform)?;
                        if min != max {
                            return Err(Error::InvalidFormat(format!(
                                "cannot derive the partition value of {location}: column {column_name} spans several partitions"
                            )));
                        }
                        if let Some(value) = partition.get_mut(partition_field.name()) {
                            *value = Some(min);
                        }
                    }
                }
```

4. After the row-group loop (before the bounds truncation), add:

```rust
    if derive_partition {
        let num_rows = file_metadata.file_metadata().num_rows();
        for (source_name, partition_field) in &partition_fields {
            let unset = matches!(partition.get(partition_field.name()), Some(None));
            let all_null = partition_source_null_counts.get(source_name) == Some(&num_rows);
            if unset && !all_null {
                return Err(Error::InvalidFormat(format!(
                    "cannot derive the partition value of {location}: column {source_name} has no statistics"
                )));
            }
        }
    }
```

In `iceberg-rust/src/arrow/write.rs`:

- Add `partition_values: &[Option<Value>],` after `partition_path: Option<String>,` in `write_parquet_files`, and pass `Some(partition_values),` after `partition_fields,` in its `parquet_to_datafile(...)` call.
- Unpartitioned caller (`if partition_fields.is_empty()`): pass `&[],` after `partition_path,`.
- Partitioned caller: before `async move {`, add `let partition_values = partition_values.clone();`, and pass `&partition_values,` after `partition_path,`.

In `datafusion_iceberg/src/table/mod.rs`:

- Add near `start_demuxer_task`:

```rust
/// Partition tuple of each file the demuxer opened, keyed by its path.
pub(crate) type PartitionValuesByPath =
    Arc<std::sync::Mutex<HashMap<object_store::path::Path, Vec<Option<Value>>>>>;
```

- `start_demuxer_task` returns `(task, rx, values_by_path)`: create `let values_by_path = PartitionValuesByPath::default();` at the top, pass `values_by_path.clone()` to `partitions_demuxer`, and return it as the third element.
- `partitions_demuxer` takes `values_by_path: PartitionValuesByPath` and, where it builds `path`, records the tuple:

```rust
                let path: object_store::path::Path = path.into();
                values_by_path
                    .lock()
                    .unwrap()
                    .insert(path.clone(), partition_values.clone());
                partition_sender
                    .send((path, reciever))
                    .map_err(DataFusionIcebergError::from)?;
```

- In `write_parquet_files`, destructure `let (demux_task, file_receiver, values_by_path) = start_demuxer_task(...)?;` and in the loop over `sink.written()`:

```rust
        let partition_values = values_by_path.lock().unwrap().get(&path).cloned();
        datafiles.push(
            parquet_to_datafile(
                &(bucket.to_string() + "/" + path.as_ref()),
                size,
                &file,
                schema,
                &partition_fields,
                partition_values.as_deref(),
                equality_ids,
                &metadata.properties,
            )
            .map_err(DataFusionIcebergError::from)?,
        );
```

(Unpartitioned tables have no entry, so `None` is passed and there are no partition fields to derive.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p iceberg-rust --lib file_format::parquet`
Expected: PASS.
Run: `cargo test -p datafusion_iceberg --test tier1_regressions`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
cargo clippy -p iceberg-rust -p datafusion_iceberg --all-targets --all-features -- -D warnings
cargo fmt --all
git add iceberg-rust/src/file_format/parquet.rs iceberg-rust/src/arrow/write.rs datafusion_iceberg/src/table/mod.rs datafusion_iceberg/tests/tier1_regressions.rs
git commit -m "fix(write): record the computed partition tuple in each data file

Partition values were re-derived from Parquet min/max statistics, which
parquet truncates to 64 bytes. The writers now pass the tuple they
partitioned by; derivation from statistics remains only as a strict
fallback.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DV558cLAwzTYg4nezLXrWG"
```

---

### Task 6: Typed partition values on read

**Files:**
- Create: `datafusion_iceberg/src/partition_value.rs`
- Modify: `datafusion_iceberg/src/lib.rs` (add `mod partition_value;`)
- Modify: `datafusion_iceberg/src/table/mod.rs` (`generate_partitioned_file` ~line 1528 and its 4 callers; delete `value_to_scalarvalue` ~line 1585)
- Modify: `datafusion_iceberg/tests/tier1_regressions.rs`

**Interfaces:**
- Consumes: `Value`, `decimal_mantissa`, `decimal_scale`.
- Produces: `crate::partition_value::value_to_scalar(value: Option<&Value>, data_type: &DataType) -> Result<ScalarValue, DataFusionError>` — builds the natural scalar for the value (uuid as its `Utf8` text, decimal as `Decimal128(38, scale)`) then casts to `data_type`; `None` gives a typed null.
- Produces: `generate_partitioned_file(schema, manifest, partition_types: &[DataType], last_updated_ms, enable_data_file_path, manifest_file_path)`.

- [ ] **Step 1: Write the failing tests**

Create `datafusion_iceberg/src/partition_value.rs` with only tests:

```rust
//! Conversions between Iceberg values and DataFusion scalars of a given column type.

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
```

Register it in `datafusion_iceberg/src/lib.rs`: `mod partition_value;`.

Add to `datafusion_iceberg/tests/tier1_regressions.rs`:

```rust
#[tokio::test]
async fn date_and_decimal_partitions_round_trip() {
    let f = Fixture::new().await;
    f.create_table(
        "t",
        vec![
            PartitionField::new(4, 1000, "d", Transform::Identity),
            PartitionField::new(6, 1001, "amount_trunc", Transform::Truncate(50)),
        ],
    )
    .await;
    f.sql("INSERT INTO warehouse.test.t (id, d, amount) VALUES (1, DATE '2023-05-15', 10.65), (2, NULL, NULL)")
        .await;
    assert_eq!(f.ids("SELECT id FROM warehouse.test.t ORDER BY id").await, vec![1, 2]);
    assert_eq!(
        f.ids("SELECT id FROM warehouse.test.t WHERE d = DATE '2023-05-15'").await,
        vec![1]
    );
    assert_eq!(f.ids("SELECT id FROM warehouse.test.t WHERE amount = 10.65").await, vec![1]);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p datafusion_iceberg --lib partition_value`
Expected: compile error — `value_to_scalar` not found.
Run: `cargo test -p datafusion_iceberg --test tier1_regressions date_and_decimal_partitions_round_trip`
Expected: FAIL — `Conversion from Value 10.50 to ScalarValue` not supported.

- [ ] **Step 3: Implement**

Add above the tests in `partition_value.rs`:

```rust
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
```

In `datafusion_iceberg/src/table/mod.rs`:

- Change `generate_partitioned_file` to take `partition_types: &[DataType]` as its third parameter and build the values with:

```rust
    let mut partition_values = manifest
        .data_file()
        .partition()
        .iter()
        .zip(partition_types)
        .map(|(value, data_type)| value_to_scalar(value.as_ref(), data_type))
        .collect::<Result<Vec<ScalarValue>, _>>()?;
```

- Delete the old `fn value_to_scalarvalue(value: &Value)` and import `crate::partition_value::value_to_scalar`.
- In the scan function, right after `let mut table_partition_cols = datafusion_partition_columns(partition_fields)?;`, add:

```rust
    let partition_types: Vec<DataType> = table_partition_cols
        .iter()
        .map(|field| field.data_type().clone())
        .collect();
```

- Pass `&partition_types` as the third argument at each of the four `generate_partitioned_file(` call sites (~lines 1015, 1070, 1167, 1232). Inside the `async move` closures that call it, clone it first like the other captured values: `let partition_types = partition_types.clone();` next to `let statistics = statistics.clone();`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p datafusion_iceberg --lib partition_value`
Expected: PASS.
Run: `cargo test -p datafusion_iceberg --test tier1_regressions`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
cargo clippy -p datafusion_iceberg --all-targets --all-features -- -D warnings
cargo fmt --all
git add datafusion_iceberg/src/partition_value.rs datafusion_iceberg/src/lib.rs datafusion_iceberg/src/table/mod.rs datafusion_iceberg/tests/tier1_regressions.rs
git commit -m "fix(scan): type partition values by their column

Decimal partition values failed to convert and uuid values were emitted
as FixedSizeBinary for a Utf8 column. Convert every value, then cast it
to the partition column's type; nulls become typed nulls.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DV558cLAwzTYg4nezLXrWG"
```

---

### Task 7: Spark ⇄ DataFusion transform compatibility, then close PR 1

**Files:**
- Create: `datafusion_iceberg/tests/integration_spark_transforms.rs`

**Interfaces:**
- Consumes: `spark_common::{boot_spark_stack, spark_sql, spark_sql_ok, SparkStack}` (existing; `SparkStack.catalog: Arc<RestCatalog>`, Spark catalog name `demo`).

- [ ] **Step 1: Write the test**

```rust
//! Spark and DataFusion agree on partition transforms (requires Docker).

#[path = "spark_common/mod.rs"]
mod spark_common;

use std::sync::Arc;

use datafusion::{
    arrow::{array::AsArray, datatypes::Int64Type},
    execution::context::SessionContext,
};
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use iceberg_rust::spec::{
    partition::{PartitionField, PartitionSpec, Transform},
    schema::Schema,
    types::{PrimitiveType, StructField, Type},
};
use iceberg_rust::table::Table;
use spark_common::{boot_spark_stack, spark_sql, spark_sql_ok};

async fn count(ctx: &SessionContext, sql: &str) -> i64 {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    batches[0].column(0).as_primitive::<Int64Type>().value(0)
}

#[tokio::test]
async fn spark_and_datafusion_agree_on_partition_transforms() {
    let stack = boot_spark_stack().await;
    spark_sql_ok(&stack, "CREATE NAMESPACE IF NOT EXISTS demo.xeng").await;

    // Spark writes; DataFusion reads with filters on every partition source.
    spark_sql_ok(
        &stack,
        "SET spark.sql.session.timeZone=UTC; \
         CREATE TABLE demo.xeng.spark_t (id BIGINT, d DATE, ts TIMESTAMP_NTZ, s STRING) USING iceberg \
         PARTITIONED BY (months(ts), bucket(10, d), truncate(2, s)); \
         INSERT INTO demo.xeng.spark_t VALUES \
           (1, DATE '2017-11-16', TIMESTAMP_NTZ '2023-05-15 12:00:00', 'abcdef'), \
           (2, DATE '1969-12-31', TIMESTAMP_NTZ '1969-12-31 23:59:59', 'zz');",
    )
    .await;

    let ctx = SessionContext::new();
    ctx.register_catalog(
        "iceberg",
        Arc::new(IcebergCatalog::new(stack.catalog.clone(), None).await.unwrap()),
    );
    for (filter, expected) in [
        ("ts >= TIMESTAMP '2023-05-01 00:00:00' AND ts < TIMESTAMP '2023-06-01 00:00:00'", 1),
        ("ts > TIMESTAMP '2023-05-15 10:00:00'", 1),
        ("ts < TIMESTAMP '1970-01-01 00:00:00'", 1),
        ("d = DATE '2017-11-16'", 1),
        ("d = DATE '1969-12-31'", 1),
        ("s = 'abcdef'", 1),
    ] {
        assert_eq!(
            count(&ctx, &format!("SELECT count(*) FROM iceberg.xeng.spark_t WHERE {filter}")).await,
            expected,
            "DataFusion reading Spark's table: {filter}"
        );
    }

    // DataFusion writes; Spark checks the partition values and prunes with them.
    let field = |id: i32, name: &str, ty: PrimitiveType| StructField {
        id,
        name: name.to_string(),
        required: false,
        field_type: Type::Primitive(ty),
        doc: None,
        initial_default: None,
        write_default: None,
    };
    Table::builder()
        .with_name("rust_t")
        .with_location("s3://warehouse/xeng/rust_t")
        .with_schema(
            Schema::builder()
                .with_struct_field(field(1, "id", PrimitiveType::Long))
                .with_struct_field(field(2, "d", PrimitiveType::Date))
                .with_struct_field(field(3, "ts", PrimitiveType::Timestamp))
                .build()
                .unwrap(),
        )
        .with_partition_spec(
            PartitionSpec::builder()
                .with_partition_field(PartitionField::new(3, 1000, "ts_month", Transform::Month))
                .with_partition_field(PartitionField::new(2, 1001, "d_bucket", Transform::Bucket(10)))
                .build()
                .unwrap(),
        )
        .build(&["xeng".to_owned()], stack.catalog.clone())
        .await
        .unwrap();
    ctx.register_catalog(
        "iceberg",
        Arc::new(IcebergCatalog::new(stack.catalog.clone(), None).await.unwrap()),
    );
    ctx.sql(
        "INSERT INTO iceberg.xeng.rust_t VALUES (1, DATE '2017-11-16', TIMESTAMP '2023-05-15 12:00:00')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    let files = spark_sql(&stack, "SELECT partition FROM demo.xeng.rust_t.files").await;
    assert!(files.is_success(), "{}", files.dump());
    // Spec: month(2023-05) = 640; bucket[10](date 2017-11-16) = 6.
    assert!(
        files.stdout.contains("\"ts_month\":640") && files.stdout.contains("\"d_bucket\":6"),
        "unexpected partition values:\n{}",
        files.dump()
    );
    let read = spark_sql(
        &stack,
        "SELECT count(*) FROM demo.xeng.rust_t WHERE d = DATE '2017-11-16' \
         AND ts >= TIMESTAMP_NTZ '2023-05-01 00:00:00' AND ts < TIMESTAMP_NTZ '2023-06-01 00:00:00'",
    )
    .await;
    assert!(read.is_success(), "{}", read.dump());
    assert!(read.stdout.lines().any(|line| line.trim() == "1"), "{}", read.dump());
}
```

- [ ] **Step 2: Run it**

Run: `cargo test -p datafusion_iceberg --test integration_spark_transforms -- --nocapture`
Expected: PASS (requires Docker; it would have failed before Tasks 1–6 on the month and bucket values). If Spark's `.files` output formats the struct differently, adjust only the two `contains` strings to Spark's actual rendering of the same numbers (640 and 6); the numbers themselves must not change.

- [ ] **Step 3: Close PR 1**

Run: `make test`
Expected: all suites pass (Docker needed for the Spark/Trino suites).
Run: `cargo clippy --all-targets --all-features -- -D warnings && cargo fmt --all -- --check`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add datafusion_iceberg/tests/integration_spark_transforms.rs
git commit -m "test: Spark and DataFusion agree on partition transforms

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DV558cLAwzTYg4nezLXrWG"
```

---

## PR 2 — Equality-delete scan

### Task 8: No `LIMIT` below the anti-join; NULL keys match

**Files:**
- Modify: `datafusion_iceberg/src/table/mod.rs` (equality-delete plan: `.with_limit(limit)` at ~lines 1093, 1106, 1184; `NullEquality::NullEqualsNothing` at ~line 1147)
- Modify: `datafusion_iceberg/tests/tier1_regressions.rs`

**Interfaces:**
- Consumes: `Fixture` (Task 4).

- [ ] **Step 1: Write the failing tests**

Add to `datafusion_iceberg/tests/tier1_regressions.rs` (and add these imports at the top: `use datafusion::arrow::error::ArrowError; use futures::stream; use iceberg_rust::{arrow::write::write_equality_deletes_parquet_partitioned, catalog::{identifier::Identifier, tabular::Tabular}};`):

```rust
/// Writes the rows of `select` as an equality-delete file on `equality_ids`.
async fn delete_where(f: &Fixture, table: &str, select: &str, equality_ids: &[i32]) {
    let batches = f.sql(select).await;
    let Tabular::Table(mut table) = f
        .catalog
        .clone()
        .load_tabular(&Identifier::new(&["test".to_string()], table))
        .await
        .unwrap()
    else {
        panic!("{table} is not a table");
    };
    let files = write_equality_deletes_parquet_partitioned(
        &table,
        stream::iter(batches.into_iter().map(Ok::<_, ArrowError>)),
        None,
        equality_ids,
    )
    .await
    .unwrap();
    table.new_transaction(None).append_delete(files).commit().await.unwrap();
}

#[tokio::test]
async fn limit_never_returns_equality_deleted_rows() {
    let f = Fixture::new().await;
    f.create_table("t", vec![]).await;
    f.sql("INSERT INTO warehouse.test.t (id) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")
        .await;
    // Physical order of the delete file: 10, 9, 1.
    delete_where(&f, "t", "SELECT id FROM warehouse.test.t WHERE id IN (1, 9, 10) ORDER BY id DESC", &[1])
        .await;
    for limit in 1..=9 {
        let ids = f.ids(&format!("SELECT id FROM warehouse.test.t LIMIT {limit}")).await;
        assert_eq!(ids.len(), limit.min(7), "LIMIT {limit} returned {ids:?}");
        assert!(ids.iter().all(|id| (2..=8).contains(id)), "LIMIT {limit} returned {ids:?}");
    }
}

#[tokio::test]
async fn equality_deletes_match_null_keys() {
    let f = Fixture::new().await;
    f.create_table("t", vec![]).await;
    f.sql("INSERT INTO warehouse.test.t (id, n, s) VALUES (1, NULL, 'a'), (2, 2, 'b'), (3, NULL, 'c')")
        .await;
    // Two-column key (n, s) = (NULL, 'a') deletes only row 1.
    delete_where(&f, "t", "SELECT n, s FROM warehouse.test.t WHERE id = 1", &[2, 3]).await;
    assert_eq!(f.ids("SELECT id FROM warehouse.test.t ORDER BY id").await, vec![2, 3]);
    // One-column key n = NULL deletes every remaining row with a NULL n.
    delete_where(&f, "t", "SELECT n FROM warehouse.test.t WHERE id = 3", &[2]).await;
    assert_eq!(f.ids("SELECT id FROM warehouse.test.t ORDER BY id").await, vec![2]);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p datafusion_iceberg --test tier1_regressions -- limit_never equality_deletes_match_null_keys`
Expected: FAIL — `LIMIT 2 returned [1, 2]` (id 1 was deleted) and `left: [1, 2, 3] right: [2, 3]`.

- [ ] **Step 3: Implement**

In the equality-delete plan of `datafusion_iceberg/src/table/mod.rs` (the `stream::iter(delete_file_groups.into_iter())` block):

- Change the three `.with_limit(limit)` calls on the delete-file scan config, the data-file scan config inside the fold, and the `additional_data_files` scan config to `.with_limit(None)`. Add above the first one:

```rust
                            // Never limit below the anti-join: a limited delete scan
                            // misses deletes, a limited data scan under-returns. The
                            // query's LIMIT still applies above this plan.
```

- Change `NullEquality::NullEqualsNothing` to `NullEquality::NullEqualsNull` and add above the `HashJoinExec::try_new(`:

```rust
                            // Spec: a NULL in an equality-delete key matches a NULL
                            // in the row, column by column.
```

Do not change the `.with_limit(if dvs_present { None } else { limit })` calls in the scans for partitions without equality deletes.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p datafusion_iceberg --test tier1_regressions`
Expected: PASS.
Run: `cargo test -p datafusion_iceberg --test equality_delete --test position_delete`
Expected: PASS.

- [ ] **Step 5: Close PR 2 and commit**

Run: `make test` and `cargo clippy --all-targets --all-features -- -D warnings`
Expected: clean.

```bash
cargo fmt --all
git add datafusion_iceberg/src/table/mod.rs datafusion_iceberg/tests/tier1_regressions.rs
git commit -m "fix(scan): apply equality deletes before LIMIT and match NULL keys

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DV558cLAwzTYg4nezLXrWG"
```

---

## PR 3 — Partition filter projection

### Task 9: Inclusive projection module

**Files:**
- Create: `datafusion_iceberg/src/partition_projection.rs`
- Modify: `datafusion_iceberg/src/lib.rs` (add `mod partition_projection;`)
- Modify: `datafusion_iceberg/src/partition_value.rs` (add `scalar_to_value`)

**Interfaces:**
- Consumes: `value_to_scalar(Option<&Value>, &DataType)` (Task 6); `Value::transform` (Task 2).
- Produces:
  - `crate::partition_projection::project(expr: &Expr, partition_fields: &[BoundPartitionField<'_>], partition_schema: &ArrowSchema) -> Option<Expr>` — an inclusive filter over partition columns (unqualified `Column`s named by partition field name), or `None`.
  - `crate::partition_value::scalar_to_value(scalar: &ScalarValue, ty: &Type) -> Option<Value>` — `None` for nulls and unsupported pairs.

- [ ] **Step 1: Write the failing tests**

Create `datafusion_iceberg/src/partition_projection.rs` with only tests:

```rust
//! Inclusive projection of query filters onto partition columns.

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
        StructField::new(5, "ts", false, Type::Primitive(PrimitiveType::Timestamp), None)
    }
    fn n_source() -> StructField {
        StructField::new(2, "n", false, Type::Primitive(PrimitiveType::Long), None)
    }
    fn s_source() -> StructField {
        StructField::new(3, "s", false, Type::Primitive(PrimitiveType::String), None)
    }
    fn day() -> (StructField, PartitionField, DataType) {
        (ts_source(), PartitionField::new(5, 1000, "ts_day", Transform::Day), DataType::Int32)
    }
    fn n_bucket() -> (StructField, PartitionField, DataType) {
        (n_source(), PartitionField::new(2, 1001, "n_bucket", Transform::Bucket(4)), DataType::Int32)
    }
    fn n_trunc() -> (StructField, PartitionField, DataType) {
        (n_source(), PartitionField::new(2, 1002, "n_trunc", Transform::Truncate(10)), DataType::Int64)
    }
    fn n_identity() -> (StructField, PartitionField, DataType) {
        (n_source(), PartitionField::new(2, 1003, "n", Transform::Identity), DataType::Int64)
    }
    fn s_trunc() -> (StructField, PartitionField, DataType) {
        (s_source(), PartitionField::new(3, 1004, "s_trunc", Transform::Truncate(2)), DataType::Utf8)
    }

    #[test]
    fn temporal_ranges_widen_to_the_boundary_partition() {
        let day_lit = lit(DAY_2023_05_15);
        assert_eq!(run(col("ts").gt(ts(TS_2023_05_15_10H)), &[day()]), Some(col("ts_day").gt_eq(day_lit.clone())));
        assert_eq!(run(col("ts").gt_eq(ts(TS_2023_05_15_10H)), &[day()]), Some(col("ts_day").gt_eq(day_lit.clone())));
        assert_eq!(run(col("ts").lt(ts(TS_2023_05_15_10H)), &[day()]), Some(col("ts_day").lt_eq(day_lit.clone())));
        assert_eq!(run(col("ts").lt_eq(ts(TS_2023_05_15_10H)), &[day()]), Some(col("ts_day").lt_eq(day_lit.clone())));
        assert_eq!(run(col("ts").eq(ts(TS_2023_05_15_10H)), &[day()]), Some(col("ts_day").eq(day_lit)));
        assert_eq!(run(col("ts").not_eq(ts(TS_2023_05_15_10H)), &[day()]), None);
    }

    #[test]
    fn literal_on_the_left_flips_the_operator() {
        assert_eq!(run(lit(15i64).gt(col("n")), &[n_trunc()]), Some(col("n_trunc").lt_eq(lit(10i64))));
    }

    #[test]
    fn bucket_projects_only_equality_and_in() {
        let b34 = bucket(hash_long(34), 4);
        let b35 = bucket(hash_long(35), 4);
        assert_eq!(run(col("n").eq(lit(34i64)), &[n_bucket()]), Some(col("n_bucket").eq(lit(b34))));
        assert_eq!(
            run(col("n").in_list(vec![lit(34i64), lit(35i64)], false), &[n_bucket()]),
            Some(col("n_bucket").in_list(vec![lit(b34), lit(b35)], false))
        );
        assert_eq!(run(col("n").lt(lit(34i64)), &[n_bucket()]), None);
        assert_eq!(run(col("n").in_list(vec![lit(34i64)], true), &[n_bucket()]), None);
    }

    #[test]
    fn string_truncate_projects_only_equality() {
        assert_eq!(run(col("s").eq(lit("abcdef")), &[s_trunc()]), Some(col("s_trunc").eq(lit("ab"))));
        assert_eq!(run(col("s").lt(lit("b")), &[s_trunc()]), None);
    }

    #[test]
    fn identity_keeps_every_comparison() {
        assert_eq!(run(col("n").not_eq(lit(5i64)), &[n_identity()]), Some(col("n").not_eq(lit(5i64))));
        // The literal is cast to the source column's type first.
        assert_eq!(run(col("n").eq(lit(5i32)), &[n_identity()]), Some(col("n").eq(lit(5i64))));
    }

    #[test]
    fn nulls_project_for_every_non_void_transform() {
        assert_eq!(run(col("n").is_null(), &[n_bucket()]), Some(col("n_bucket").is_null()));
        assert_eq!(run(col("n").is_not_null(), &[n_trunc()]), Some(col("n_trunc").is_not_null()));
        let void = (n_source(), PartitionField::new(2, 1005, "n_void", Transform::Void), DataType::Int64);
        assert_eq!(run(col("n").is_null(), &[void.clone()]), None);
        assert_eq!(run(col("n").eq(lit(1i64)), &[void]), None);
    }

    #[test]
    fn a_column_feeding_two_partition_fields_projects_to_both() {
        let ts_bucket = (ts_source(), PartitionField::new(5, 1006, "ts_bucket", Transform::Bucket(4)), DataType::Int32);
        let b = bucket(hash_long(TS_2023_05_15_10H), 4);
        assert_eq!(
            run(col("ts").eq(ts(TS_2023_05_15_10H)), &[day(), ts_bucket]),
            Some(col("ts_day").eq(lit(DAY_2023_05_15)).and(col("ts_bucket").eq(lit(b))))
        );
    }

    #[test]
    fn and_keeps_what_projects_or_needs_every_branch() {
        let n_lt = col("n").lt(lit(15i64));
        let other = col("id").eq(lit(1i64));
        assert_eq!(run(n_lt.clone().and(other.clone()), &[n_trunc()]), Some(col("n_trunc").lt_eq(lit(10i64))));
        assert_eq!(run(n_lt.clone().or(other), &[n_trunc()]), None);
        assert_eq!(
            run(n_lt.clone().or(col("n").gt(lit(30i64))), &[n_trunc()]),
            Some(col("n_trunc").lt_eq(lit(10i64)).or(col("n_trunc").gt_eq(lit(30i64))))
        );
        assert_eq!(run(Expr::Not(Box::new(n_lt)), &[n_trunc()]), None);
    }

    #[test]
    fn unprojectable_shapes_return_none() {
        assert_eq!(run((col("n") + lit(1i64)).eq(lit(15i64)), &[n_trunc()]), None);
        assert_eq!(run(col("n").eq(lit(ScalarValue::Int64(None))), &[n_trunc()]), None);
        assert_eq!(run(col("n").eq(col("id")), &[n_trunc()]), None);
    }
}
```

Register it in `datafusion_iceberg/src/lib.rs`: `mod partition_projection;`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p datafusion_iceberg --lib partition_projection`
Expected: compile error — `project` not found.

- [ ] **Step 3: Implement**

Add to `datafusion_iceberg/src/partition_value.rs` (above its tests):

```rust
use datafusion::arrow::datatypes::TimeUnit;
use iceberg_rust::spec::{
    decimal::decimal_from_i128_with_scale,
    types::{PrimitiveType, Type},
};
use ordered_float::OrderedFloat;
use uuid::Uuid;

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
        (ScalarValue::TimestampMicrosecond(Some(v), _), PrimitiveType::Timestamp) => Value::Timestamp(*v),
        (ScalarValue::TimestampMicrosecond(Some(v), _), PrimitiveType::Timestamptz) => {
            Value::TimestampTZ(*v)
        }
        (
            ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) | ScalarValue::Utf8View(Some(v)),
            PrimitiveType::String,
        ) => Value::String(v.clone()),
        (ScalarValue::Utf8(Some(v)), PrimitiveType::Uuid) => Value::UUID(Uuid::parse_str(v).ok()?),
        (
            ScalarValue::Binary(Some(v)) | ScalarValue::LargeBinary(Some(v)) | ScalarValue::BinaryView(Some(v)),
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
```

(`ordered-float` is not a direct dependency of `datafusion_iceberg`: add `ordered-float = "5.3.0"` to `datafusion_iceberg/Cargo.toml` `[dependencies]`. Remove the unused `TimeUnit` import if clippy flags it.)

Add above the tests in `datafusion_iceberg/src/partition_projection.rs`:

```rust
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
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::And, right }) => {
            match (recurse(left), recurse(right)) {
                (Some(left), Some(right)) => Some(left.and(right)),
                (Some(one), None) | (None, Some(one)) => Some(one),
                (None, None) => None,
            }
        }
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::Or, right }) => {
            Some(recurse(left)?.or(recurse(right)?))
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            if !matches!(
                op,
                Operator::Eq | Operator::NotEq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
            ) {
                return None;
            }
            let (column, op, literal) = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(column), Expr::Literal(literal, _)) => (column, *op, literal),
                (Expr::Literal(literal, _), Expr::Column(column)) => (column, op.swap()?, literal),
                _ => return None,
            };
            project_leaf(column, &Leaf::Compare(op, literal), partition_fields, partition_schema)
        }
        Expr::InList(InList { expr, list, negated: false }) => {
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
            project_leaf(column, &Leaf::In(literals), partition_fields, partition_schema)
        }
        Expr::IsNull(inner) => match inner.as_ref() {
            Expr::Column(column) => project_leaf(column, &Leaf::IsNull, partition_fields, partition_schema),
            _ => None,
        },
        Expr::IsNotNull(inner) => match inner.as_ref() {
            Expr::Column(column) => project_leaf(column, &Leaf::IsNotNull, partition_fields, partition_schema),
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
    let partition_type = partition_schema.field_with_name(field.name()).ok()?.data_type();
    let partition_literal = |literal: &ScalarValue| -> Option<Expr> {
        let source_type: DataType = field.field_type().try_into().ok()?;
        let literal = literal.cast_to(&source_type).ok()?;
        let value = scalar_to_value(&literal, field.field_type())?;
        let transformed = value.transform(transform).ok()?;
        Some(Expr::Literal(value_to_scalar(Some(&transformed), partition_type).ok()?, None))
    };
    // Transforms with a <= b  =>  t(a) <= t(b).
    let order_preserving = match transform {
        Transform::Identity | Transform::Year | Transform::Month | Transform::Day | Transform::Hour => true,
        Transform::Truncate(_) => matches!(
            field.field_type(),
            Type::Primitive(PrimitiveType::Int | PrimitiveType::Long | PrimitiveType::Decimal { .. })
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
            Some(binary_expr(partition_column, op, partition_literal(literal)?))
        }
    }
}
```

If `Type -> DataType` is implemented only for `&Type`, the `try_into()` above already takes `field.field_type()` (a `&Type`); if the compiler asks for an explicit conversion, use `DataType::try_from(field.field_type())`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p datafusion_iceberg --lib partition_projection partition_value`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
cargo clippy -p datafusion_iceberg --all-targets --all-features -- -D warnings
cargo fmt --all
git add datafusion_iceberg/Cargo.toml Cargo.lock datafusion_iceberg/src/lib.rs datafusion_iceberg/src/partition_projection.rs datafusion_iceberg/src/partition_value.rs
git commit -m "feat(scan): inclusive projection of filters onto partition columns

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DV558cLAwzTYg4nezLXrWG"
```

(`project` and `scalar_to_value` are only used by tests until Task 10. If clippy reports them as dead code, add `#[cfg_attr(not(test), allow(dead_code))]` to each and remove the attribute in Task 10.)

---

### Task 10: Use the projection; remove the old rewrite

**Files:**
- Modify: `datafusion_iceberg/src/table/mod.rs` (~lines 620–680; import at ~line 49)
- Modify: `datafusion_iceberg/src/pruning_statistics.rs` (delete `transform_predicate`, `transform_literal`, `DateTransform`, `scalarvalue_to_value`, `value_to_scalarvalue`, test `scalar_value_roundtrip_preserves_timezone`; fix `null_counts` and `row_counts` in `PruneManifests`)
- Modify: `datafusion_iceberg/tests/tier1_regressions.rs`

**Interfaces:**
- Consumes: `project(expr, partition_fields, partition_schema)` (Task 9).

- [ ] **Step 1: Write the failing differential test**

Add to `datafusion_iceberg/tests/tier1_regressions.rs`:

```rust
/// One row per table. The third row is all-null apart from `id`.
const ROWS: [&str; 3] = [
    "(1, 15, 'abcdef', DATE '2023-05-15', TIMESTAMP '2023-05-15 12:00:00', 10.65)",
    "(1, -1, 'éa', DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59', -0.01)",
    "(1, NULL, NULL, NULL, NULL, NULL)",
];

const FILTERS: [&str; 46] = [
    "n = 15", "n = 14", "n < 15", "n <= 15", "n > 15", "n >= 15", "n < 20", "n > 10",
    "n < -1", "n <= -1", "n > -5", "n != 15", "n IN (14, 15)", "n IS NULL", "n IS NOT NULL",
    "20 > n",
    "s = 'abcdef'", "s = 'abz'", "s < 'abd'", "s > 'ab'", "s IN ('abcdef', 'x')", "s = 'éa'",
    "d = DATE '2023-05-15'", "d < DATE '2023-05-16'", "d > DATE '2023-05-14'",
    "d >= DATE '2023-05-15'", "d != DATE '2023-05-01'", "d < DATE '1970-01-01'",
    "d = DATE '1969-12-31'",
    "ts > TIMESTAMP '2023-05-15 10:00:00'", "ts < TIMESTAMP '2023-05-15 14:00:00'",
    "ts >= TIMESTAMP '2023-05-15 12:00:00'", "ts <= TIMESTAMP '2023-05-15 12:00:00'",
    "ts = TIMESTAMP '2023-05-15 12:00:00'", "ts != TIMESTAMP '2023-05-15 13:00:00'",
    "ts < TIMESTAMP '1970-01-01 00:00:00'", "ts > TIMESTAMP '1969-12-31 23:00:00'",
    "amount = 10.65", "amount < 10.70", "amount > 10.60", "amount = -0.01", "amount < 0",
    "n = 99 OR ts > TIMESTAMP '2023-05-15 10:00:00'", "NOT (n < 10)", "n + 0 = 15",
    "n = 15 AND s = 'abcdef'",
];

fn partition_fields_under_test() -> Vec<PartitionField> {
    vec![
        PartitionField::new(2, 1000, "n_identity", Transform::Identity),
        PartitionField::new(2, 1000, "n_bucket", Transform::Bucket(4)),
        PartitionField::new(2, 1000, "n_trunc", Transform::Truncate(10)),
        PartitionField::new(3, 1000, "s_trunc", Transform::Truncate(2)),
        PartitionField::new(3, 1000, "s_bucket", Transform::Bucket(4)),
        PartitionField::new(4, 1000, "d_year", Transform::Year),
        PartitionField::new(4, 1000, "d_month", Transform::Month),
        PartitionField::new(4, 1000, "d_day", Transform::Day),
        PartitionField::new(4, 1000, "d_bucket", Transform::Bucket(4)),
        PartitionField::new(5, 1000, "ts_year", Transform::Year),
        PartitionField::new(5, 1000, "ts_month", Transform::Month),
        PartitionField::new(5, 1000, "ts_day", Transform::Day),
        PartitionField::new(5, 1000, "ts_hour", Transform::Hour),
        PartitionField::new(5, 1000, "ts_bucket", Transform::Bucket(4)),
        PartitionField::new(6, 1000, "amount_trunc", Transform::Truncate(50)),
        PartitionField::new(6, 1000, "amount_bucket", Transform::Bucket(4)),
    ]
}

/// Each table holds one row in one partition, so a wrong projection prunes
/// its only manifest. An unpartitioned copy gives the expected answer.
#[tokio::test]
async fn partition_pruning_never_changes_results() {
    let f = Fixture::new().await;
    let insert = |table: &str, row: &str| {
        format!("INSERT INTO warehouse.test.{table} (id, n, s, d, ts, amount) VALUES {row}")
    };
    let mut failures = Vec::new();
    for (row_index, row) in ROWS.iter().enumerate() {
        let unpartitioned = format!("u{row_index}");
        f.create_table(&unpartitioned, vec![]).await;
        f.sql(&insert(&unpartitioned, row)).await;
        for (field_index, field) in partition_fields_under_test().into_iter().enumerate() {
            let table = format!("p{row_index}_{field_index}");
            let label = format!("{field:?}");
            f.create_table(&table, vec![field]).await;
            f.sql(&insert(&table, row)).await;
            for filter in FILTERS {
                let expected = f
                    .try_ids(&format!("SELECT id FROM warehouse.test.{unpartitioned} WHERE {filter}"))
                    .await;
                let actual = f.try_ids(&format!("SELECT id FROM warehouse.test.{table} WHERE {filter}")).await;
                if expected != actual {
                    failures.push(format!("{label}, row {row}, `{filter}`: expected {expected:?}, got {actual:?}"));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{} mismatches:\n{}", failures.len(), failures.join("\n"));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p datafusion_iceberg --test tier1_regressions partition_pruning_never_changes_results`
Expected: FAIL — mismatches including `ts_day ... ts > TIMESTAMP '2023-05-15 10:00:00': expected Ok([1]), got Ok([])` and errors on bucket filters (`Invalid comparison operation`).

- [ ] **Step 3: Implement**

In `datafusion_iceberg/src/table/mod.rs`, replace the block from `let partition_column_names = partition_fields` through the end of `let partition_predicates = conjunction(...);` with:

```rust
        let partition_predicates = conjunction(filters.iter().filter_map(|expr| {
            crate::partition_projection::project(expr, partition_fields, &partition_schema)
        }));
```

Remove `transform_predicate` from the `pruning_statistics::{...}` import, and remove any import that becomes unused (`HashSet` only if nothing else uses it).

In `datafusion_iceberg/src/pruning_statistics.rs`:

- Delete `transform_predicate`, `transform_literal`, the `DateTransform` struct and its `impl`s, `scalarvalue_to_value`, `value_to_scalarvalue`, and the test `scalar_value_roundtrip_preserves_timezone`. Remove imports clippy reports as unused.
- In `PruneManifests::null_counts`, look the field up by partition column name (the pruning predicate references partition columns, not source columns):

```rust
            .find(|(_, field)| field.name() == column.name())?;
```

- In `PruneManifests::row_counts`, count live rows only (deleted entries are not in the snapshot):

```rust
        let row_counts = self.files.iter().map(|x| {
            match (x.added_rows_count, x.existing_rows_count) {
                (Some(a), Some(e)) => Some(a + e),
                _ => None,
            }
        });
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p datafusion_iceberg --test tier1_regressions`
Expected: PASS (the differential test runs ~2,200 queries; allow a few minutes in debug builds).
Run: `cargo test -p datafusion_iceberg --lib` and `cargo test -p datafusion_iceberg --test integration_df_filters`
Expected: PASS.

- [ ] **Step 5: Close PR 3 and commit**

Run: `make test` and `cargo clippy --all-targets --all-features -- -D warnings`
Expected: clean.

```bash
cargo fmt --all
git add datafusion_iceberg/src/table/mod.rs datafusion_iceberg/src/pruning_statistics.rs datafusion_iceberg/tests/tier1_regressions.rs
git commit -m "fix(scan): prune partitions with an inclusive projection

The old rewrite applied the query operator to the transformed literal,
dropping matching partitions for range filters on temporal transforms
and comparing untransformed literals for bucket and truncate.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DV558cLAwzTYg4nezLXrWG"
```

---

## PR 4 — Bounds merge

### Task 11: Merge bounds of every type across row groups

**Files:**
- Modify: `iceberg-rust/src/file_format/parquet.rs` (bounds blocks at ~lines 237–330; truncation at ~line 402; tests module)

**Interfaces:**
- Consumes: `Value: Ord` (derived), `parquet_to_datafile` 8-argument signature (Task 5).

- [ ] **Step 1: Write the failing tests**

Add to the tests module of `iceberg-rust/src/file_format/parquet.rs`:

```rust
    /// Writes each batch as its own row group; returns the data file.
    fn datafile_from_row_groups(schema: &Schema, batches: Vec<arrow::record_batch::RecordBatch>) -> DataFile {
        use parquet::{
            arrow::ArrowWriter,
            file::{
                properties::WriterProperties,
                reader::{FileReader, SerializedFileReader},
            },
        };
        let mut buffer = Vec::new();
        let props = WriterProperties::builder().set_max_row_group_row_count(Some(1024)).build();
        let mut writer = ArrowWriter::try_new(&mut buffer, batches[0].schema(), Some(props)).unwrap();
        for batch in &batches {
            writer.write(batch).unwrap();
            writer.flush().unwrap();
        }
        writer.close().unwrap();
        let size = buffer.len() as u64;
        let reader = SerializedFileReader::new(bytes::Bytes::from(buffer)).unwrap();
        assert_eq!(reader.metadata().num_row_groups(), batches.len());
        parquet_to_datafile("/t/data/1.parquet", size, reader.metadata(), schema, &[], None, None, &HashMap::new())
            .unwrap()
    }

    fn one_column(ty: iceberg_rust_spec::spec::types::PrimitiveType, arrays: Vec<arrow::array::ArrayRef>) -> DataFile {
        use std::sync::Arc;

        use arrow::{
            datatypes::{Field, Schema as ArrowSchema},
            record_batch::RecordBatch,
        };
        use iceberg_rust_spec::spec::types::StructField;

        let schema = Schema::builder()
            .with_struct_field(StructField::new(1, "c", false, Type::Primitive(ty), None))
            .build()
            .unwrap();
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new("c", arrays[0].data_type().clone(), true)]));
        let batches = arrays
            .into_iter()
            .map(|array| RecordBatch::try_new(arrow_schema.clone(), vec![array]).unwrap())
            .collect();
        datafile_from_row_groups(&schema, batches)
    }

    fn bounds(datafile: &DataFile) -> (Option<Value>, Option<Value>) {
        (
            datafile.lower_bounds().as_ref().and_then(|b| b.get(&1).cloned()),
            datafile.upper_bounds().as_ref().and_then(|b| b.get(&1).cloned()),
        )
    }

    #[test]
    fn bounds_cover_every_row_group_for_every_type() {
        use std::sync::Arc;

        use arrow::array::{BinaryArray, BooleanArray, Decimal128Array, StringArray};
        use iceberg_rust_spec::spec::{decimal::decimal_from_i128_with_scale, types::PrimitiveType};

        let strings = one_column(
            PrimitiveType::String,
            vec![Arc::new(StringArray::from(vec!["m", "z"])), Arc::new(StringArray::from(vec!["a", "b"]))],
        );
        assert_eq!(bounds(&strings), (Some(Value::String("a".into())), Some(Value::String("z".into()))));

        let decimal = |v: Vec<i128>| Arc::new(Decimal128Array::from(v).with_precision_and_scale(9, 2).unwrap());
        let decimals = one_column(
            PrimitiveType::Decimal { precision: 9, scale: 2 },
            vec![decimal(vec![500, 900]), decimal(vec![100, 200])],
        );
        assert_eq!(
            bounds(&decimals),
            (
                Some(Value::Decimal(decimal_from_i128_with_scale(100, 2).unwrap())),
                Some(Value::Decimal(decimal_from_i128_with_scale(900, 2).unwrap()))
            )
        );

        let binaries = one_column(
            PrimitiveType::Binary,
            vec![
                Arc::new(BinaryArray::from(vec![&[5u8][..], &[9u8][..]])),
                Arc::new(BinaryArray::from(vec![&[1u8][..], &[2u8][..]])),
            ],
        );
        assert_eq!(bounds(&binaries), (Some(Value::Binary(vec![1])), Some(Value::Binary(vec![9]))));

        let booleans = one_column(
            PrimitiveType::Boolean,
            vec![Arc::new(BooleanArray::from(vec![true, true])), Arc::new(BooleanArray::from(vec![false, false]))],
        );
        assert_eq!(bounds(&booleans), (Some(Value::Boolean(false)), Some(Value::Boolean(true))));
    }

    #[test]
    fn long_string_bounds_still_cover_every_row_group() {
        use std::sync::Arc;

        use arrow::array::StringArray;
        use iceberg_rust_spec::spec::types::PrimitiveType;

        let high = format!("z{}", "y".repeat(100));
        let low = format!("a{}", "b".repeat(100));
        let datafile = one_column(
            PrimitiveType::String,
            vec![
                Arc::new(StringArray::from(vec![high.as_str()])),
                Arc::new(StringArray::from(vec![low.as_str()])),
            ],
        );
        let (Some(Value::String(lower)), Some(Value::String(upper))) = bounds(&datafile) else {
            panic!("expected string bounds");
        };
        assert!(lower.as_str() <= low.as_str(), "lower bound {lower} above {low}");
        assert!(upper.as_str() >= high.as_str(), "upper bound {upper} below {high}");
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p iceberg-rust --lib file_format::parquet::tests::bounds_cover file_format::parquet::tests::long_string`
Expected: FAIL — `left: (Some(String("m")), Some(String("z")))`.

- [ ] **Step 3: Implement**

In `parquet_to_datafile`, declare next to `lower_bounds`/`upper_bounds`:

```rust
    // Columns whose row groups produced bounds of different value types.
    let mut conflicting_bounds: std::collections::HashSet<i32> = std::collections::HashSet::new();
```

Replace both `if let Some(min_bytes) = statistics.min_bytes_opt().filter(...) { ... }` and `if let Some(max_bytes) = ...max_bytes_opt()... { ... }` blocks with:

```rust
                // Parquet truncates long statistics (64 bytes by default): a
                // truncated min is a prefix, so still a lower bound, and a
                // truncated max is rounded up, so still an upper bound.
                if metrics_mode.records_bounds() && matches!(data_type, Type::Primitive(_)) {
                    if let Some(min_bytes) = statistics.min_bytes_opt() {
                        let min = Value::try_from_bytes_with_hint(min_bytes, data_type, physical_type_hint)?;
                        merge_bound(&mut lower_bounds, &mut conflicting_bounds, id, min, Ordering::Less);
                    }
                    if let Some(max_bytes) = statistics.max_bytes_opt() {
                        let max = Value::try_from_bytes_with_hint(max_bytes, data_type, physical_type_hint)?;
                        merge_bound(&mut upper_bounds, &mut conflicting_bounds, id, max, Ordering::Greater);
                    }
                }
```

Right after the row-group loop (before the truncation that builds the final `lower_bounds`/`upper_bounds`), add:

```rust
    lower_bounds.retain(|id, _| !conflicting_bounds.contains(id));
    upper_bounds.retain(|id, _| !conflicting_bounds.contains(id));
```

Add this function to the module (outside `parquet_to_datafile`), and `use std::cmp::Ordering;` at the top:

```rust
/// Keeps the smaller (`keep == Ordering::Less`) or larger (`Ordering::Greater`)
/// of the stored bound and `new`. Values of different types cannot be
/// compared, so the column is marked and its bounds dropped later.
fn merge_bound(
    bounds: &mut HashMap<i32, Value>,
    conflicting: &mut std::collections::HashSet<i32>,
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p iceberg-rust --lib file_format::parquet`
Expected: PASS (including the existing truncation tests).

- [ ] **Step 5: Close PR 4 and commit**

Run: `make test` and `cargo clippy --all-targets --all-features -- -D warnings`
Expected: clean.

```bash
cargo fmt --all
git add iceberg-rust/src/file_format/parquet.rs
git commit -m "fix(stats): merge lower/upper bounds of every type across row groups

Strings, decimals, binary, uuid, fixed and boolean kept the first row
group's min/max, so files with several row groups had bounds that
excluded real values and were pruned wrongly.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DV558cLAwzTYg4nezLXrWG"
```

---

## Final verification

### Task 12: Whole-branch check

- [ ] **Step 1:** `cargo fmt --all -- --check` — clean.
- [ ] **Step 2:** `cargo clippy --all-targets --all-features -- -D warnings` — clean.
- [ ] **Step 3:** `make test` — all suites pass; record the summary line of each suite.
- [ ] **Step 4:** `cargo test -p datafusion_iceberg --test integration_spark_transforms --test integration_spark --test integration_trino` — pass (Docker).
- [ ] **Step 5:** Re-run the audit reproductions now in `tier1_regressions.rs` and confirm each named audit item (1–8) has a passing test:
  1 → `partition_pruning_never_changes_results`; 2 → `limit_never_returns_equality_deleted_rows`; 3 → `equality_deletes_match_null_keys`; 4 → `bounds_cover_every_row_group_for_every_type`; 5, 6, 7 → `hashes_match_spec_appendix_b`, `temporal_transforms_floor_before_the_epoch`, `truncate_matches_spec_examples`, `arrow_and_value_transforms_agree`, `spark_and_datafusion_agree_on_partition_transforms`; 8 → `insert_keeps_rows_with_null_partition_source`.
