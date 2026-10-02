//! Scalar implementations of the Iceberg partition transforms.

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
