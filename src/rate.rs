use byte_unit::Byte;
use lazy_static::lazy_static;
use regex::Regex;

#[derive(PartialEq, Debug)]
pub enum RateSpecification {
    TimerBased(u32),
    ThroughputBased(u32),
}

lazy_static! {
    static ref HZ_RE: Regex = Regex::new(r"(\d+)\s*(hz|HZ)").unwrap();
}
pub fn parse_rate(rate: &str) -> Option<RateSpecification> {
    if let Some(hz_captures) = HZ_RE.captures(rate) {
        if let Some(hz_value) = hz_captures.get(1) {
            if let Ok(hz_u32) = hz_value.as_str().parse::<u32>() {
                return Some(RateSpecification::TimerBased(hz_u32));
            }
            return None;
        }
    }
    if let Ok(bytes) = Byte::from_str(rate) {
        let bytes_per_second = bytes.get_bytes() as u32;
        return Some(RateSpecification::ThroughputBased(bytes_per_second));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nonsense_string() {
        assert_eq!(parse_rate("abcde"), None);
    }

    #[test]
    fn hz_string() {
        assert_eq!(parse_rate("1hz"), Some(RateSpecification::TimerBased(1)));
        assert_eq!(parse_rate("1 hz"), Some(RateSpecification::TimerBased(1)));
        assert_eq!(parse_rate("2 hz"), Some(RateSpecification::TimerBased(2)));
        assert_eq!(
            parse_rate("22222 hz"),
            Some(RateSpecification::TimerBased(22222))
        );
        assert_eq!(
            parse_rate("22222hz"),
            Some(RateSpecification::TimerBased(22222))
        );
        assert_eq!(parse_rate("1HZ"), Some(RateSpecification::TimerBased(1)));
        assert_eq!(parse_rate("10HZ"), Some(RateSpecification::TimerBased(10)));
    }

    #[test]
    fn throughput_string() {
        assert_eq!(
            parse_rate("1b"),
            Some(RateSpecification::ThroughputBased(1))
        );
        assert_eq!(
            parse_rate("1kb"),
            Some(RateSpecification::ThroughputBased(1000))
        );
        assert_eq!(
            parse_rate("1 kb"),
            Some(RateSpecification::ThroughputBased(1000))
        );
        assert_eq!(
            parse_rate("100 kb"),
            Some(RateSpecification::ThroughputBased(1000 * 100))
        );
        assert_eq!(
            parse_rate("100 Mb"),
            Some(RateSpecification::ThroughputBased(100 * 1_000_000))
        );
        assert_eq!(
            parse_rate("100 MB"),
            Some(RateSpecification::ThroughputBased(100 * 1_000_000))
        );
    }
}
