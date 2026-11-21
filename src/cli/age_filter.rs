use crate::s3::responses::Object;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Duration, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgeFilter {
    duration: Duration,
}

impl AgeFilter {
    #[must_use]
    pub const fn new(duration: Duration) -> Self {
        Self { duration }
    }

    #[must_use]
    pub const fn duration(self) -> Duration {
        self.duration
    }

    /// # Errors
    /// Will return `Err` if the object's `LastModified` timestamp can not be parsed
    pub fn matches(self, object: &Object, now: DateTime<Utc>) -> Result<bool> {
        let last_modified = parse_last_modified(object)?;
        // A cutoff before the earliest representable date selects nothing.
        Ok(now
            .checked_sub_signed(self.duration)
            .is_some_and(|cutoff| last_modified < cutoff))
    }
}

/// # Errors
/// Will return `Err` if the duration is not in `Nd`, `Nh`, or `Nm` form
pub fn parse_age_filter(input: &str) -> Result<AgeFilter> {
    let (value, unit) = input.strip_suffix(['d', 'h', 'm']).map_or_else(
        || {
            Err(anyhow!(
                "Invalid duration '{input}'. Expected Nd, Nh, or Nm"
            ))
        },
        |value| {
            let unit = input
                .chars()
                .last()
                .ok_or_else(|| anyhow!("Invalid duration '{input}'"))?;
            Ok((value, unit))
        },
    )?;

    let amount: i64 = value.parse().map_err(|_| {
        anyhow!("Invalid duration '{input}'. Expected a number followed by d, h, or m")
    })?;

    if amount <= 0 {
        return Err(anyhow!(
            "Invalid duration '{input}'. Duration must be greater than zero"
        ));
    }

    let duration = match unit {
        'd' => Duration::try_days(amount),
        'h' => Duration::try_hours(amount),
        'm' => Duration::try_minutes(amount),
        _ => {
            return Err(anyhow!(
                "Invalid duration '{input}'. Expected Nd, Nh, or Nm"
            ));
        }
    }
    .ok_or_else(|| anyhow!("Invalid duration '{input}'. Duration is too large"))?;

    Ok(AgeFilter::new(duration))
}

/// # Errors
/// Will return `Err` if the object's `LastModified` timestamp can not be parsed
pub fn parse_last_modified(object: &Object) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(&object.last_modified)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|error| {
            anyhow!(
                "Failed to parse LastModified for object '{}': {} ({error})",
                object.key,
                object.last_modified
            )
        })
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::unnecessary_wraps
)]
mod tests {
    use super::*;
    use crate::s3::responses::Object;
    use chrono::TimeZone;

    fn object(last_modified: &str) -> Object {
        Object {
            last_modified: last_modified.to_string(),
            e_tag: "\"etag\"".to_string(),
            storage_class: "STANDARD".to_string(),
            key: "key".to_string(),
            owner: None,
            size: 1,
        }
    }

    #[test]
    fn test_parse_age_filter_days() {
        assert_eq!(
            parse_age_filter("30d").unwrap().duration(),
            Duration::days(30)
        );
    }

    #[test]
    fn test_parse_age_filter_hours() {
        assert_eq!(
            parse_age_filter("12h").unwrap().duration(),
            Duration::hours(12)
        );
    }

    #[test]
    fn test_parse_age_filter_minutes() {
        assert_eq!(
            parse_age_filter("45m").unwrap().duration(),
            Duration::minutes(45)
        );
    }

    #[test]
    fn test_parse_age_filter_invalid() {
        let err = parse_age_filter("30x").unwrap_err().to_string();
        assert!(err.contains("Invalid duration"));
    }

    #[test]
    fn test_parse_age_filter_rejects_zero() {
        let err = parse_age_filter("0d").unwrap_err().to_string();
        assert!(err.contains("greater than zero"));
    }

    #[test]
    fn test_parse_age_filter_rejects_negative() {
        let err = parse_age_filter("-1d").unwrap_err().to_string();
        assert!(err.contains("greater than zero"));
    }

    #[test]
    fn test_matches_strictly_older_than() {
        let filter = parse_age_filter("30d").unwrap();
        let now = Utc.with_ymd_and_hms(2026, 3, 14, 12, 0, 0).unwrap();

        assert!(
            filter
                .matches(&object("2026-02-12T11:59:59Z"), now)
                .unwrap()
        );
        assert!(
            !filter
                .matches(&object("2026-02-12T12:00:00Z"), now)
                .unwrap()
        );
    }

    #[test]
    fn test_parse_age_filter_rejects_out_of_range_durations() {
        for input in [
            "999999999999999d",
            "9223372036854775807h",
            "9223372036854775807m",
        ] {
            let err = parse_age_filter(input).unwrap_err().to_string();
            assert_eq!(
                err,
                format!("Invalid duration '{input}'. Duration is too large")
            );
        }
    }

    #[test]
    fn test_matches_nothing_when_cutoff_precedes_supported_dates() {
        // Representable as a duration, but `now - duration` is before the
        // earliest supported date: nothing can be older than that.
        let filter = parse_age_filter("100000000d").unwrap();
        let now = Utc.with_ymd_and_hms(2026, 3, 14, 12, 0, 0).unwrap();

        assert!(
            !filter
                .matches(&object("1970-01-01T00:00:00Z"), now)
                .unwrap()
        );
    }

    #[test]
    fn test_parse_last_modified_invalid() {
        let err = parse_last_modified(&object("invalid"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("Failed to parse LastModified for object 'key'"));
    }
}
