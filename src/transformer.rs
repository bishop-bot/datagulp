//! Date/time transformation utilities for QuestDB ingestion

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use std::str::FromStr;

/// Date parsing formats
const DATE_FORMATS: &[&str] = &[
    "%m/%d/%Y",
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%Y/%m/%d",
];

/// Time parsing formats
const TIME_FORMATS: &[&str] = &[
    "%H:%M:%S%.f",
    "%H:%M:%S",
    "%H:%M:%S%.3f",
];

pub struct Transformer;

impl Transformer {
    /// Parse a date string with multiple format attempts
    pub fn parse_date(s: &str) -> Option<NaiveDate> {
        for fmt in DATE_FORMATS {
            if let Ok(date) = NaiveDate::parse_from_str(s, fmt) {
                return Some(date);
            }
        }
        None
    }

    /// Parse a time string with multiple format attempts
    pub fn parse_time(s: &str) -> Option<NaiveTime> {
        for fmt in TIME_FORMATS {
            if let Ok(time) = NaiveTime::parse_from_str(s, fmt) {
                return Some(time);
            }
        }
        None
    }

    /// Combine date and time strings into NaiveDateTime
    pub fn parse_datetime(date: &str, time: &str) -> Option<NaiveDateTime> {
        let date = Self::parse_date(date)?;
        let time = Self::parse_time(time)?;
        Some(date.and_time(time))
    }

    /// Format timestamp for various output formats
    pub fn format_timestamp(dt: &NaiveDateTime, format: &str) -> String {
        match format {
            "iso8601" => dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string(),
            "unix_epoch" => {
                let utc_dt = Utc.from_utc_datetime(dt);
                utc_dt.timestamp().to_string() + "." + &dt.format("%6f").to_string()
            }
            _ => {
                // QuestDB format: yyyy-MM-dd HH:mm:ss.SSS
                dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
            }
        }
    }

    /// Parse a numeric value with flexible decimal handling
    #[allow(dead_code)]
    pub fn parse_number<T: FromStr>(s: &str) -> Option<T> {
        let cleaned = s.trim().replace(",", "").replace("$", "");
        cleaned.parse().ok()
    }

    /// Clean a string value (remove quotes, whitespace)
    #[allow(dead_code)]
    pub fn clean_string(s: &str) -> String {
        s.trim().trim_matches('"').to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, Timelike};

    #[test]
    fn test_parse_date_mdy() {
        assert_eq!(Transformer::parse_date("1/1/2026").unwrap().month(), 1);
    }

    #[test]
    fn test_parse_date_ymd() {
        let d = Transformer::parse_date("2026-01-01").unwrap();
        assert_eq!(d.year(), 2026);
        assert_eq!(d.month(), 1);
        assert_eq!(d.day(), 1);
    }

    #[test]
    fn test_parse_time() {
        assert_eq!(Transformer::parse_time("17:00:00.000").unwrap().hour(), 17);
    }

    #[test]
    fn test_parse_time_no_millis() {
        let t = Transformer::parse_time("17:00:00").unwrap();
        assert_eq!(t.hour(), 17);
        assert_eq!(t.minute(), 0);
    }

    #[test]
    fn test_parse_datetime() {
        let dt = Transformer::parse_datetime("1/1/2026", "17:00:00.000").unwrap();
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 1);
        assert_eq!(dt.hour(), 17);
    }

    #[test]
    fn test_format_questdb() {
        let dt = NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_milli_opt(17, 0, 0, 0)
            .unwrap();
        let result = Transformer::format_timestamp(&dt, "questdb");
        assert!(result.starts_with("2026-01-01 17:00:00"));
    }

    #[test]
    fn test_format_iso8601() {
        let dt = NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_micro_opt(17, 0, 0, 123456)
            .unwrap();
        let result = Transformer::format_timestamp(&dt, "iso8601");
        assert!(result.starts_with("2026-01-01T17:00:00"));
    }

    #[test]
    fn test_clean_string() {
        assert_eq!(Transformer::clean_string("  \"hello\"  "), "hello");
        assert_eq!(Transformer::clean_string("world"), "world");
    }

    #[test]
    fn test_parse_number() {
        assert_eq!(Transformer::parse_number::<f64>("$1,234.56").unwrap(), 1234.56);
        assert_eq!(Transformer::parse_number::<i32>("42").unwrap(), 42);
    }
}