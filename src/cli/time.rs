//! Timestamp parsing and formatting shared by the CLI.
//!
//! Every instant is shown in UTC. The output formats match what s3m printed
//! before it moved from `chrono` to `jiff`, so scripts that read them keep
//! working.

use jiff::{Timestamp, civil::Date, fmt::rfc2822::DateTimeParser, tz::Offset};
use std::fmt::Write as _;

static RFC2822: DateTimeParser = DateTimeParser::new();

/// Parses an RFC 3339 timestamp with an explicit offset, such as S3's
/// `LastModified` (`2026-03-14T12:00:00.000Z`).
///
/// # Errors
/// Will return `Err` if `value` is not a timestamp with an offset
pub fn parse_rfc3339(value: &str) -> Result<Timestamp, jiff::Error> {
    value.parse()
}

/// Parses an HTTP date header: RFC 2822 (`Fri, 24 May 2013 00:00:00 GMT`)
/// first, then RFC 3339.
///
/// # Errors
/// Will return `Err` if `value` is neither form
pub fn parse_http_date(value: &str) -> Result<Timestamp, jiff::Error> {
    RFC2822
        .parse_timestamp(value)
        .or_else(|_| parse_rfc3339(value))
}

/// Human-readable UTC form used by listings: `2026-03-14 12:00:00 UTC`.
#[must_use]
pub fn display(timestamp: Timestamp) -> String {
    timestamp.strftime("%F %T UTC").to_string()
}

/// RFC 3339 in UTC with a `+00:00` offset and fractional seconds only when
/// present, in groups of 3, 6 or 9 digits: `2026-03-14T12:00:00+00:00`,
/// `2026-03-14T12:00:00.250+00:00`.
#[must_use]
pub fn rfc3339(timestamp: Timestamp) -> String {
    let mut out = timestamp.strftime("%Y-%m-%dT%H:%M:%S").to_string();
    let nanos = timestamp.subsec_nanosecond().unsigned_abs();
    if nanos != 0 {
        let _ = if nanos.is_multiple_of(1_000_000) {
            write!(out, ".{:03}", nanos / 1_000_000)
        } else if nanos.is_multiple_of(1_000) {
            write!(out, ".{:06}", nanos / 1_000)
        } else {
            write!(out, ".{nanos:09}")
        };
    }
    out.push_str("+00:00");
    out
}

/// RFC 3339 in UTC with whole seconds and a `Z` suffix, the form S3 expects
/// for Object Lock dates: `2027-01-01T00:00:00Z`.
#[must_use]
pub fn rfc3339_seconds(timestamp: Timestamp) -> String {
    timestamp.strftime("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// The calendar day of `timestamp` in UTC.
#[must_use]
pub fn utc_date(timestamp: Timestamp) -> Date {
    Offset::UTC.to_datetime(timestamp).date()
}

/// The instant `seconds` after the Unix epoch, if it is representable.
#[must_use]
pub fn from_unix_seconds(seconds: u64) -> Option<Timestamp> {
    i64::try_from(seconds)
        .ok()
        .and_then(|seconds| Timestamp::from_second(seconds).ok())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn ts(value: &str) -> Timestamp {
        parse_rfc3339(value).unwrap()
    }

    #[test]
    fn parses_s3_and_offset_timestamps_as_instants() {
        assert_eq!(
            ts("2026-03-16T13:00:00+02:00"),
            ts("2026-03-16T11:00:00.000Z")
        );
        for invalid in ["invalid", "2026-03-16", "2026-03-16T11:00:00"] {
            assert!(parse_rfc3339(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn parses_http_dates_in_both_forms() {
        let expected = ts("2013-05-24T00:00:00Z");
        assert_eq!(
            parse_http_date("Fri, 24 May 2013 00:00:00 GMT").unwrap(),
            expected
        );
        assert_eq!(parse_http_date("2013-05-24T00:00:00Z").unwrap(), expected);
        assert!(parse_http_date("yesterday").is_err());
    }

    #[test]
    fn formats_match_the_previous_output() {
        let instant = ts("2026-03-14T14:00:00+02:00");
        assert_eq!(display(instant), "2026-03-14 12:00:00 UTC");
        assert_eq!(rfc3339(instant), "2026-03-14T12:00:00+00:00");
        assert_eq!(rfc3339_seconds(instant), "2026-03-14T12:00:00Z");
        assert_eq!(
            utc_date(ts("2026-03-14T00:30:00+02:00")).to_string(),
            "2026-03-13"
        );
    }

    #[test]
    fn rfc3339_keeps_fractional_seconds_in_groups_of_three() {
        for (input, expected) in [
            ("2026-03-14T12:00:00.25Z", "2026-03-14T12:00:00.250+00:00"),
            (
                "2026-03-14T12:00:00.000250Z",
                "2026-03-14T12:00:00.000250+00:00",
            ),
            (
                "2026-03-14T12:00:00.000000250Z",
                "2026-03-14T12:00:00.000000250+00:00",
            ),
        ] {
            assert_eq!(rfc3339(ts(input)), expected);
        }
        // Object Lock dates drop fractions instead of rounding.
        assert_eq!(
            rfc3339_seconds(ts("2027-01-01T00:00:00.999Z")),
            "2027-01-01T00:00:00Z"
        );
    }

    #[test]
    fn unix_seconds_outside_the_supported_range_are_none() {
        assert_eq!(
            from_unix_seconds(0).map(display).unwrap(),
            "1970-01-01 00:00:00 UTC"
        );
        assert_eq!(from_unix_seconds(u64::MAX), None);
    }
}
