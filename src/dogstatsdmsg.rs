use std::fmt::Debug;

use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum DogStatsDMsgError {
    #[error("Metric parsing error: {0}")]
    InvalidMetric(&'static str),
    #[error("Event parsing error: {0}")]
    InvalidEvent(&'static str),
}

#[derive(Debug)]
pub enum DogStatsDStr<'a> {
    Metric(DogStatsDMetricStr<'a>),
    Event(DogStatsDEventStr<'a>),
    ServiceCheck(DogStatsDServiceCheckStr<'a>),
}

// _e{<TITLE_UTF8_LENGTH>,<TEXT_UTF8_LENGTH>}:<TITLE>|<TEXT>|d:<TIMESTAMP>|h:<HOSTNAME>|p:<PRIORITY>|t:<ALERT_TYPE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>

#[derive(Debug)]
pub struct DogStatsDEventStr<'a> {
    pub title: &'a str,
    pub text: &'a str,
    pub timestamp: Option<&'a str>,
    pub hostname: Option<&'a str>,
    pub priority: Option<&'a str>, // Set to normal or low. Default normal.
    pub alert_type: Option<&'a str>, // Set to error, warning, info, or success. Default info.
    pub tags: Vec<&'a str>,
    pub raw_msg: &'a str,
}

// _sc|<NAME>|<STATUS>|d:<TIMESTAMP>|h:<HOSTNAME>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|m:<SERVICE_CHECK_MESSAGE>
#[derive(Debug)]
pub struct DogStatsDServiceCheckStr<'a> {
    pub name: &'a str,
    pub status: &'a str,
    pub raw_msg: &'a str,
}

#[derive(Debug)]
pub struct DogStatsDMetricStr<'a> {
    pub name: &'a str,
    pub values: &'a str,
    pub sample_rate: Option<&'a str>,
    pub timestamp: Option<&'a str>,
    pub container_id: Option<&'a str>,
    pub metric_type: &'a str,
    pub tags: Vec<&'a str>,
    pub raw_msg: &'a str,
}

impl<'a> DogStatsDStr<'a> {
    // _e{<TITLE_UTF8_LENGTH>,<TEXT_UTF8_LENGTH>}:<TITLE>|<TEXT>|d:<TIMESTAMP>|h:<HOSTNAME>|p:<PRIORITY>|t:<ALERT_TYPE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
    fn parse_event(str_msg: &'a str) -> Result<Self, DogStatsDMsgError> {
        let orig_msg = str_msg;
        let str_msg = str_msg.trim_end();
        let start_lengths_idx = str_msg
            .find('{')
            .ok_or(DogStatsDMsgError::InvalidEvent("No opening brace found"))?;
        let end_lengths_idx = str_msg
            .find('}')
            .ok_or(DogStatsDMsgError::InvalidEvent("No closing brace found"))?;

        let lengths = &str_msg[start_lengths_idx + 1..end_lengths_idx]
            .split(",")
            .collect::<Vec<&str>>();
        let title_length: usize = lengths[0]
            .parse()
            .map_err(|_e| DogStatsDMsgError::InvalidEvent("Invalid title length specified"))?;

        let text_length: usize = lengths[1]
            .parse()
            .map_err(|_e| DogStatsDMsgError::InvalidEvent("Invalid text length specified"))?;

        let title_start_idx = end_lengths_idx + 2;
        let title_end_idx = title_start_idx + title_length;
        let text_start_idx = title_end_idx + 1;
        let text_end_idx = text_start_idx + text_length;

        let title =
            str_msg
                .get(title_start_idx..title_end_idx)
                .ok_or(DogStatsDMsgError::InvalidEvent(
                    "Title length specified is longer than msg length",
                ))?;

        let text =
            str_msg
                .get(text_start_idx..text_end_idx)
                .ok_or(DogStatsDMsgError::InvalidEvent(
                    "Text length specified is longer than msg length",
                ))?;

        // Initialize optional fields
        let mut timestamp = None;
        let mut hostname = None;
        let mut priority = None;
        let mut alert_type = None;
        let mut tags = Vec::new();

        let post_text_idx = end_lengths_idx + 2 + title_length + text_length + 1;
        if post_text_idx < str_msg.len() {
            let post_text_msg = &str_msg[post_text_idx..];
            if !post_text_msg.starts_with('|') {
                return Err(DogStatsDMsgError::InvalidEvent(
                    "data present after title and text, but did not start with a pipe",
                ));
            }
            for part in post_text_msg[1..].split('|') {
                match part.chars().next() {
                    Some('d') => timestamp = Some(&part[2..]),
                    Some('h') => hostname = Some(&part[2..]),
                    Some('p') => priority = Some(&part[2..]),
                    Some('t') => alert_type = Some(&part[2..]),
                    Some('#') => tags.extend(part[1..].split(',')),
                    _ => {
                        return Err(DogStatsDMsgError::InvalidEvent("Unknown field value found"));
                    }
                }
            }
        }

        Ok(DogStatsDStr::Event(DogStatsDEventStr {
            title,
            text,
            timestamp,
            hostname,
            priority,
            alert_type,
            tags,
            raw_msg: orig_msg,
        }))
    }

    pub fn new(str_msg: &'a str) -> Result<Self, DogStatsDMsgError> {
        if str_msg.starts_with("_e") {
            return Self::parse_event(str_msg);
        }
        if str_msg.starts_with("_sc") {
            return Ok(DogStatsDStr::ServiceCheck(DogStatsDServiceCheckStr {
                name: "placeholder",
                status: "placeholder_status",
                raw_msg: str_msg,
            }));
        }
        let parts: Vec<&str> = str_msg.trim_end().split('|').collect();
        match parts.first() {
            Some(prepipe) => {
                let prepipe_deref = *prepipe;
                let name_and_values = match prepipe_deref.split_once(':') {
                    Some(n_and_v) => n_and_v,
                    None => return Err(DogStatsDMsgError::InvalidMetric("Name or value missing")),
                };
                let name = name_and_values.0;
                let values = name_and_values.1;

                let metric_type: &str = match parts.get(1) {
                    Some(s) => *s,
                    None => return Err(DogStatsDMsgError::InvalidMetric("No metric type found")),
                };

                let tags = match parts.iter().find(|part| part.starts_with('#')) {
                    Some(tags) => tags[1..].split(',').collect(),
                    None => vec![],
                };

                let timestamp = parts
                    .iter()
                    .find(|part| part.starts_with('T'))
                    .map(|p| p.get(1..).unwrap());
                let sample_rate = parts
                    .iter()
                    .find(|part| part.starts_with('@'))
                    .map(|p| p.get(1..).unwrap());
                let container_id = parts
                    .iter()
                    .find(|part| part.starts_with("c:"))
                    .map(|p| p.get(2..).unwrap());

                Ok(DogStatsDStr::Metric(DogStatsDMetricStr {
                    raw_msg: str_msg,
                    name,
                    values,
                    container_id,
                    timestamp,
                    sample_rate,
                    tags,
                    metric_type,
                }))
            }
            None => Err(DogStatsDMsgError::InvalidMetric("Unknown error")),
        }
    }
}

// TODO implement debug once I figure out the syntax using lifetimes
/*
impl Debug for DogStatsDMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Full dogstatsd msg: {}", self)
    }
} */

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! metric_test {
        ($name:ident, $input:expr, $expected_name:expr, $expected_values:expr, $expected_type:expr, $expected_tags:expr, $expected_sample_rate:expr, $expected_timestamp:expr, $expected_container_id:expr, $expected_error:expr) => {
            #[test]
            fn $name() {
                let msg = match DogStatsDStr::new($input) {
                    Ok(DogStatsDStr::Metric(m)) => m,
                    Ok(DogStatsDStr::ServiceCheck(_)) => {
                        panic!("Got service check, expected metric")
                    }
                    Ok(DogStatsDStr::Event(_)) => panic!("Got event, expected metric"),
                    Err(e) => match $expected_error {
                        Some(_expected_error) => {
                            // TODO check if the expected_error is the "same" as 'e'
                            // expected_error is ideally 'DogStatsDMsgError::InvalidMetric'
                            // and that should match 'e' if 'e' is _any_ DogStatsDMsgError::InvalidMetric
                            // ie, should match DogStatsDMsgError::InvalidMetric("foo")
                            //
                            // The strings in this error are meant to be human-readable descriptions of the
                            // specific "invalidation", so I don't want to match the exact same
                            // phrasing in the test code.
                            return;
                        }
                        None => panic!("Unexpected error: {}", e),
                    },
                };
                assert!($expected_error.is_none());
                assert_eq!(msg.raw_msg, $input);
                assert_eq!(msg.name, $expected_name);
                assert_eq!(msg.values, $expected_values);
                assert_eq!(msg.metric_type, $expected_type);
                assert_eq!(msg.tags, $expected_tags);
                assert_eq!(msg.sample_rate, $expected_sample_rate);
                assert_eq!(msg.timestamp, $expected_timestamp);
                assert_eq!(msg.container_id, $expected_container_id);
            }
        };
    }

    macro_rules! event_test {
        ($name:ident, $input:expr, $expected_title:expr, $expected_text:expr, $expected_timestamp:expr, $expected_hostname:expr, $expected_priority:expr, $expected_alert_type:expr, $expected_tags:expr, $expected_error:expr) => {
            #[test]
            fn $name() {
                let msg = match DogStatsDStr::new($input) {
                    Ok(DogStatsDStr::Event(e)) => e,
                    Ok(DogStatsDStr::ServiceCheck(_)) => {
                        panic!("Got service check, expected metric")
                    }
                    Ok(DogStatsDStr::Metric(_)) => panic!("Got metric, expected event"),
                    Err(e) => match $expected_error {
                        Some(_expected_error) => {
                            // TODO check if the expected_error is the "same" as 'e'
                            // expected_error is ideally 'DogStatsDMsgError::InvalidEvent'
                            // and that should match 'e' if 'e' is _any_
                            // DogStatsDMsgError::InvalidEvent
                            // ie, should match DogStatsDMsgError::InvalidEvent("foo")
                            //
                            // The strings in this error are meant to be human-readable descriptions of the
                            // specific "invalidation", so I don't want to match the exact same
                            // phrasing in the test code.
                            return;
                        }
                        None => panic!("Unexpected error: {}", e),
                    },
                };
                assert!($expected_error.is_none());
                assert_eq!(msg.raw_msg, $input);
                assert_eq!(msg.title, $expected_title);
                assert_eq!(msg.text, $expected_text);
                assert_eq!(msg.timestamp, $expected_timestamp);
                assert_eq!(msg.hostname, $expected_hostname);
                assert_eq!(msg.priority, $expected_priority);
                assert_eq!(msg.alert_type, $expected_alert_type);
                assert_eq!(msg.tags, $expected_tags);
            }
        };
    }

    metric_test!(
        basic_metric,
        "metric.name:1|c",
        "metric.name",
        "1",
        "c",
        Vec::<&str>::new(),
        None,
        None,
        None,
        None::<DogStatsDMsgError>
    );

    metric_test!(
        basic_gauge,
        "metric.name:1|g",
        "metric.name",
        "1",
        "g",
        Vec::<&str>::new(),
        None,
        None,
        None,
        None::<DogStatsDMsgError>
    );

    metric_test!(
        basic_histogram,
        "metric.name:1|h",
        "metric.name",
        "1",
        "h",
        Vec::<&str>::new(),
        None,
        None,
        None,
        None::<DogStatsDMsgError>
    );

    metric_test!(
        basic_timer,
        "metric.name:1|t",
        "metric.name",
        "1",
        "t",
        Vec::<&str>::new(),
        None,
        None,
        None,
        None::<DogStatsDMsgError>
    );

    metric_test!(
        basic_set,
        "metric.name:1|s",
        "metric.name",
        "1",
        "s",
        Vec::<&str>::new(),
        None,
        None,
        None,
        None::<DogStatsDMsgError>
    );

    metric_test!(
        basic_gauge_floating_value,
        "metric.name:1.321|g",
        "metric.name",
        "1.321",
        "g",
        Vec::<&str>::new(),
        None,
        None,
        None,
        None::<DogStatsDMsgError>
    );

    metric_test!(
        basic_dist_floating_value,
        "metric.name:1.321|d",
        "metric.name",
        "1.321",
        "d",
        Vec::<&str>::new(),
        None,
        None,
        None,
        None::<DogStatsDMsgError>
    );

    metric_test!(
        basic_dist_multi_floating_value,
        "metric.name:1.321:1.11111|d",
        "metric.name",
        "1.321:1.11111",
        "d",
        Vec::<&str>::new(),
        None,
        None,
        None,
        None::<DogStatsDMsgError>
    );

    metric_test!(
        metric_with_container_id,
        "metric.name:1|c|c:container123",
        "metric.name",
        "1",
        "c",
        Vec::<&str>::new(),
        None,
        None,
        Some("container123"),
        None::<DogStatsDMsgError>
    );

    metric_test!(
        metric_with_everything,
        "metric.name:1|c|@0.5|T1234567890|c:container123|#tag1:value1,tag2",
        "metric.name",
        "1",
        "c",
        vec!["tag1:value1", "tag2"],
        Some("0.5"),
        Some("1234567890"),
        Some("container123"),
        None::<DogStatsDMsgError>
    );

    metric_test!(
        metric_with_mixed_order,
        "metric.name:1|c|#tag1:value1,tag2|@0.5|c:container123|T1234567890",
        "metric.name",
        "1",
        "c",
        vec!["tag1:value1", "tag2"],
        Some("0.5"),
        Some("1234567890"),
        Some("container123"),
        None::<DogStatsDMsgError>
    );

    metric_test!(
        metric_with_multiple_tags,
        "metric.name:1|c|#tag1:value1,tag2,tag3:another",
        "metric.name",
        "1",
        "c",
        vec!["tag1:value1", "tag2", "tag3:another"],
        None,
        None,
        None,
        None::<DogStatsDMsgError>
    );

    metric_test!(
        metric_with_no_optional_fields,
        "metric.name:1|c",
        "metric.name",
        "1",
        "c",
        Vec::<&str>::new(),
        None,
        None,
        None,
        None::<DogStatsDMsgError>
    );

    metric_test!(
        metric_with_unrecognized_field,
        "metric.name:1|c|x:unknown",
        "metric.name",
        "1",
        "c",
        Vec::<&str>::new(),
        None,
        None,
        None,
        None::<DogStatsDMsgError>
    );

    metric_test!(
        malformed_metric_missing_value,
        "metric.name:|c",
        "metric.name",
        "",
        "c",
        Vec::<&str>::new(),
        None,
        None,
        None,
        None::<DogStatsDMsgError>
    );

    metric_test!(
        malformed_metric_invalid_format,
        "metric.name|1|c",
        "metric.name",
        "1",
        "c",
        Vec::<&str>::new(),
        None,
        None,
        None,
        Some(DogStatsDMsgError::InvalidMetric("Name or value missing"))
    );

    event_test!(
        basic_event,
        "_e{5,4}:title|text",
        "title",
        "text",
        None,
        None,
        None,
        None,
        Vec::<&str>::new(),
        None::<DogStatsDMsgError>
    );

    event_test!(
        basic_event_short_title_text,
        "_e{1,1}:t|t",
        "t",
        "t",
        None,
        None,
        None,
        None,
        Vec::<&str>::new(),
        None::<DogStatsDMsgError>
    );

    event_test!(
        event_with_no_text,
        "_e{1,0}:t|",
        "t",
        "",
        None,
        None,
        None,
        None,
        Vec::<&str>::new(),
        None::<DogStatsDMsgError> // This is arguably invalid, but don't care at the moment
    );

    event_test!(
        event_with_basic_fields,
        "_e{2,4}:ab|cdef|d:160|h:myhost|p:high|t:severe|#env:prod,onfire:true\n",
        "ab",
        "cdef",
        Some("160"),
        Some("myhost"),
        Some("high"),
        Some("severe"),
        vec!["env:prod", "onfire:true"],
        None::<DogStatsDMsgError>
    );

    event_test!(
        invalid_event_text_length,
        "_e{100,0}:t|",
        "t",
        "",
        None,
        None,
        None,
        None,
        Vec::<&str>::new(),
        Some(DogStatsDMsgError::InvalidEvent)
    );

    #[test]
    fn basic_events() {
        // _e{<TITLE_UTF8_LENGTH>,<TEXT_UTF8_LENGTH>}:<TITLE>|<TEXT>|d:<TIMESTAMP>|h:<HOSTNAME>|p:<PRIORITY>|t:<ALERT_TYPE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        let raw_msg = "_e{2,4}:ab|cdef|d:160|h:myhost|p:high|t:severe|#env:prod,onfire:true\n";
        match DogStatsDStr::new(raw_msg) {
            Ok(DogStatsDStr::Event(m)) => m,
            Err(e) => panic!("Unexpected error: {}", e),
            Ok(_) => panic!("Wrong type"),
        };
        // Not implemented yet
        // assert_eq!(msg.title, "ab");
        // assert_eq!(msg.text, "cdef");
    }

    #[test]
    fn basic_service_checks() {
        // _sc|<NAME>|<STATUS>|d:<TIMESTAMP>|h:<HOSTNAME>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|m:<SERVICE_CHECK_MESSAGE>
        let raw_msg = "_sc:ab|error|d:160|h:myhost|#env:prod,onfire:true|m:mymessage\n";
        match DogStatsDStr::new(raw_msg) {
            Ok(DogStatsDStr::ServiceCheck(m)) => m,
            _ => panic!("Wrong type"),
        };
        // No other fields implemented
    }

    #[test]
    fn invalid_statsd_msg() {
        assert_eq!(
            DogStatsDMsgError::InvalidMetric("Name or value missing"),
            DogStatsDStr::new("abcdefghiq").unwrap_err()
        );
    }
}
