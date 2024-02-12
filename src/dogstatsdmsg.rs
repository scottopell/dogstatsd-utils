use std::fmt::Display;

use smallvec::{smallvec, SmallVec};
use thiserror::Error;

use lading_payload::dogstatsd::event::Alert as LadingAlert;

const MAX_TAGS: usize = 50;

#[derive(Error, Debug, PartialEq)]
pub enum DogStatsDMsgError {
    #[error("Parsing Error for {kind}: '{reason}' Full msg: '{raw_msg}'")]
    ParseError {
        kind: DogStatsDMsgKind,
        reason: &'static str,
        raw_msg: String,
    },
}

impl DogStatsDMsgError {
    fn new_parse_error(kind: DogStatsDMsgKind, reason: &'static str, raw_msg: String) -> Self {
        Self::ParseError {
            kind,
            reason,
            raw_msg,
        }
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum DogStatsDMsg<'a> {
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
    pub alert_type: EventAlert,
    pub aggregation_key: Option<&'a str>,
    pub source_type_name: Option<&'a str>,
    pub tags: SmallVec<&'a str, MAX_TAGS>,
    pub raw_msg: &'a str,
}

// Status: An integer corresponding to the check status (OK = 0, WARNING = 1, CRITICAL = 2, UNKNOWN = 3).
#[derive(Debug, PartialEq)]
pub enum ServiceCheckStatus {
    Ok = 0,
    Warning = 1,
    Critical = 2,
    Unknown = 3,
}

#[derive(Debug, PartialEq)]
pub enum EventAlert {
    Error,
    Warning,
    Info,
    Success,
}

impl TryFrom<&str> for EventAlert {
    type Error = ();

    fn try_from(s: &str) -> Result<Self, ()> {
        match s {
            "error" => Ok(EventAlert::Error),
            "warning" => Ok(EventAlert::Warning),
            "info" => Ok(EventAlert::Info),
            "success" => Ok(EventAlert::Success),
            _ => Err(()),
        }
    }
}

impl From<LadingAlert> for EventAlert {
    fn from(a: LadingAlert) -> Self {
        match a {
            LadingAlert::Error => EventAlert::Error,
            LadingAlert::Warning => EventAlert::Warning,
            LadingAlert::Info => EventAlert::Info,
            LadingAlert::Success => EventAlert::Success,
        }
    }
}

impl TryFrom<&str> for ServiceCheckStatus {
    type Error = ();

    fn try_from(s: &str) -> Result<Self, ()> {
        match s {
            "0" => Ok(ServiceCheckStatus::Ok),
            "1" => Ok(ServiceCheckStatus::Warning),
            "2" => Ok(ServiceCheckStatus::Critical),
            "3" => Ok(ServiceCheckStatus::Unknown),
            _ => Err(()),
        }
    }
}

// _sc|<NAME>|<STATUS>|d:<TIMESTAMP>|h:<HOSTNAME>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|m:<SERVICE_CHECK_MESSAGE>
#[derive(Debug)]
pub struct DogStatsDServiceCheckStr<'a> {
    pub name: &'a str,
    pub status: ServiceCheckStatus,
    pub timestamp: Option<&'a str>,
    pub hostname: Option<&'a str>,
    pub message: Option<&'a str>,
    pub tags: SmallVec<&'a str, MAX_TAGS>,
    pub raw_msg: &'a str,
}

#[derive(Debug)]
pub struct DogStatsDMetricStr<'a> {
    pub name: &'a str,
    pub values: SmallVec<f64, MAX_TAGS>,
    pub sample_rate: Option<&'a str>,
    pub timestamp: Option<&'a str>,
    pub container_id: Option<&'a str>,
    pub metric_type: DogStatsDMetricType,
    pub tags: SmallVec<&'a str, MAX_TAGS>,
    pub raw_msg: &'a str,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum DogStatsDMetricType {
    Count,
    Gauge,
    Histogram,
    Timer,
    Set,
    Distribution,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum DogStatsDMsgKind {
    Metric,
    ServiceCheck,
    Event,
}

impl Display for DogStatsDMsgKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DogStatsDMsgKind::Metric => write!(f, "Metric"),
            DogStatsDMsgKind::ServiceCheck => write!(f, "ServiceCheck"),
            DogStatsDMsgKind::Event => write!(f, "Event"),
        }
    }
}

impl DogStatsDMetricType {
    fn from_str(s: &str) -> Result<Self, ()> {
        match s {
            "c" => Ok(DogStatsDMetricType::Count),
            "g" => Ok(DogStatsDMetricType::Gauge),
            "h" => Ok(DogStatsDMetricType::Histogram),
            "ms" => Ok(DogStatsDMetricType::Timer),
            "s" => Ok(DogStatsDMetricType::Set),
            "d" => Ok(DogStatsDMetricType::Distribution),
            _ => Err(()),
        }
    }
}

impl Display for DogStatsDMetricType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DogStatsDMetricType::Count => write!(f, "Count"),
            DogStatsDMetricType::Gauge => write!(f, "Gauge"),
            DogStatsDMetricType::Histogram => write!(f, "Histogram"),
            DogStatsDMetricType::Timer => write!(f, "Timer"),
            DogStatsDMetricType::Set => write!(f, "Set"),
            DogStatsDMetricType::Distribution => write!(f, "Distribution"),
        }
    }
}

impl<'a> DogStatsDMsg<'a> {
    pub fn kind(self) -> DogStatsDMsgKind {
        match self {
            DogStatsDMsg::Event(_) => DogStatsDMsgKind::Event,
            DogStatsDMsg::ServiceCheck(_) => DogStatsDMsgKind::ServiceCheck,
            DogStatsDMsg::Metric(_) => DogStatsDMsgKind::Metric,
        }
    }
    // _e{<TITLE_UTF8_LENGTH>,<TEXT_UTF8_LENGTH>}:<TITLE>|<TEXT>|d:<TIMESTAMP>|h:<HOSTNAME>|p:<PRIORITY>|t:<ALERT_TYPE>|k:<AGGREGATION_KEY>|s:<SOURCE_TYPE_NAME>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
    fn parse_event(str_msg: &'a str) -> Result<Self, DogStatsDMsgError> {
        let orig_msg = str_msg;
        let str_msg = str_msg.trim_end();
        let start_lengths_idx = str_msg.find('{').ok_or(DogStatsDMsgError::new_parse_error(
            DogStatsDMsgKind::Event,
            "No opening brace found",
            str_msg.to_owned(),
        ))?;
        let end_lengths_idx = str_msg.find('}').ok_or(DogStatsDMsgError::new_parse_error(
            DogStatsDMsgKind::Event,
            "No closing brace found",
            str_msg.to_owned(),
        ))?;

        let lengths = &str_msg[start_lengths_idx + 1..end_lengths_idx]
            .split(',')
            .collect::<Vec<&str>>();
        let title_length: usize = lengths[0].parse().map_err(|_e| {
            DogStatsDMsgError::new_parse_error(
                DogStatsDMsgKind::Event,
                "Invalid title length specified",
                str_msg.to_owned(),
            )
        })?;

        let text_length: usize = lengths[1].parse().map_err(|_e| {
            DogStatsDMsgError::new_parse_error(
                DogStatsDMsgKind::Event,
                "Invalid text length specified",
                str_msg.to_owned(),
            )
        })?;

        let title_start_idx = end_lengths_idx + 2;
        let title_end_idx = title_start_idx + title_length;
        let text_start_idx = title_end_idx + 1;
        let text_end_idx = text_start_idx + text_length;

        let title = str_msg.get(title_start_idx..title_end_idx).ok_or(
            DogStatsDMsgError::new_parse_error(
                DogStatsDMsgKind::Event,
                "Title length specified is longer than msg length",
                str_msg.to_owned(),
            ),
        )?;

        let text =
            str_msg
                .get(text_start_idx..text_end_idx)
                .ok_or(DogStatsDMsgError::new_parse_error(
                    DogStatsDMsgKind::Event,
                    "Text length specified is longer than msg length",
                    str_msg.to_owned(),
                ))?;

        // Initialize optional fields
        let mut timestamp = None;
        let mut hostname = None;
        let mut priority = None;
        let mut alert_type = EventAlert::Info;
        let mut aggregation_key = None;
        let mut source_type_name = None;
        let mut tags = smallvec![];

        let post_text_idx = end_lengths_idx + 2 + title_length + text_length + 1;
        if post_text_idx < str_msg.len() {
            let post_text_msg = &str_msg[post_text_idx..];
            if !post_text_msg.starts_with('|') {
                return Err(DogStatsDMsgError::new_parse_error(
                    DogStatsDMsgKind::Event,
                    "data present after title and text, but did not start with a pipe",
                    str_msg.to_owned(),
                ));
            }
            for part in post_text_msg[1..].split('|') {
                match part.chars().next() {
                    Some('d') => timestamp = Some(&part[2..]),
                    Some('h') => hostname = Some(&part[2..]),
                    Some('p') => priority = Some(&part[2..]),
                    Some('t') => {
                        alert_type = match EventAlert::try_from(&part[2..]) {
                            Ok(parsed_alert_type) => parsed_alert_type,
                            // consider logging a trace/info level saying "defaulting to alert type"?
                            Err(_) => EventAlert::Info,
                        }
                    }
                    Some('k') => aggregation_key = Some(&part[2..]),
                    Some('s') => source_type_name = Some(&part[2..]),
                    Some('#') => tags.extend(part[1..].split(',')),
                    _ => {
                        return Err(DogStatsDMsgError::new_parse_error(
                            DogStatsDMsgKind::Event,
                            "Unknown event field value found",
                            str_msg.to_owned(),
                        ));
                    }
                }
            }
        }

        Ok(DogStatsDMsg::Event(DogStatsDEventStr {
            title,
            text,
            timestamp,
            hostname,
            priority,
            source_type_name,
            aggregation_key,
            alert_type,
            tags,
            raw_msg: orig_msg,
        }))
    }

    fn parse_metric(str_msg: &'a str) -> Result<Self, DogStatsDMsgError> {
        let str_msg = str_msg.trim_end();
        let parts: Vec<&str> = str_msg.split('|').collect();
        match parts.first() {
            Some(prepipe) => {
                let prepipe_deref = *prepipe;
                let name_and_values = match prepipe_deref.split_once(':') {
                    Some(n_and_v) => n_and_v,
                    None => {
                        return Err(DogStatsDMsgError::new_parse_error(
                            DogStatsDMsgKind::Metric,
                            "Name or value missing",
                            str_msg.to_owned(),
                        ))
                    }
                };
                let name = name_and_values.0;
                let str_values = name_and_values.1;
                let mut values = smallvec![];
                for part in str_values.split(':') {
                    match part.parse::<f64>() {
                        Ok(v) => {values.push(v);}
                        Err(_) => {
                            return Err(DogStatsDMsgError::new_parse_error(
                                DogStatsDMsgKind::Metric,
                                "Invalid or no value found",
                                str_msg.to_owned(),
                            ))
                        }
                    }
                }

                let metric_type: DogStatsDMetricType = match parts.get(1) {
                    Some(s) => {
                        if s.len() > 2 {
                            return Err(DogStatsDMsgError::new_parse_error(
                                DogStatsDMsgKind::Metric,
                                "Too many chars for metric type",
                                str_msg.to_owned(),
                            ));
                        }
                        match DogStatsDMetricType::from_str(s) {
                            Ok(t) => t,
                            Err(_) => {
                                return Err(DogStatsDMsgError::new_parse_error(
                                    DogStatsDMsgKind::Metric,
                                    "Invalid metric type found.",
                                    str_msg.to_owned(),
                                ))
                            }
                        }
                    }
                    None => {
                        return Err(DogStatsDMsgError::new_parse_error(
                            DogStatsDMsgKind::Metric,
                            "No metric type found",
                            str_msg.to_owned(),
                        ))
                    }
                };

                let tags: SmallVec<&'a str, MAX_TAGS> =
                    match parts.iter().find(|part| part.starts_with('#')) {
                        Some(tags) => tags[1..].split(',').collect(),
                        None => smallvec![],
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

                Ok(DogStatsDMsg::Metric(DogStatsDMetricStr {
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
            None => Err(DogStatsDMsgError::new_parse_error(
                DogStatsDMsgKind::Metric,
                "Unknown error",
                str_msg.to_owned(),
            )),
        }
    }

    // _sc|<NAME>|<STATUS>|d:<TIMESTAMP>|h:<HOSTNAME>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|m:<SERVICE_CHECK_MESSAGE>
    // Status: An integer corresponding to the check status (OK = 0, WARNING = 1, CRITICAL = 2, UNKNOWN = 3).
    fn parse_servicecheck(str_msg: &'a str) -> Result<Self, DogStatsDMsgError> {
        let raw_msg = str_msg;
        let str_msg = str_msg.trim_end();
        let mut fields = str_msg.split('|');
        // consume prefix
        match fields.next() {
            Some(pre) => {
                if pre != "_sc" {
                    return Err(DogStatsDMsgError::ParseError {
                        kind: DogStatsDMsgKind::ServiceCheck,
                        reason: "Unexpected prefix found for service check",
                        raw_msg: raw_msg.to_owned(),
                    });
                }
            }
            None => {
                return Err(DogStatsDMsgError::ParseError {
                    kind: DogStatsDMsgKind::ServiceCheck,
                    reason: "Not enough fields in msg",
                    raw_msg: raw_msg.to_owned(),
                })
            }
        }
        let name = match fields.next() {
            Some(name) => name,
            None => {
                return Err(DogStatsDMsgError::new_parse_error(
                    DogStatsDMsgKind::ServiceCheck,
                    "Not enough fields, couldn't find name",
                    raw_msg.to_owned(),
                ))
            }
        };

        let status = match fields.next() {
            Some(status) => match ServiceCheckStatus::try_from(status) {
                Ok(s) => s,
                Err(_) => {
                    return Err(DogStatsDMsgError::new_parse_error(
                        DogStatsDMsgKind::ServiceCheck,
                        "Invalid status found.",
                        raw_msg.to_owned(),
                    ))
                }
            },
            None => {
                return Err(DogStatsDMsgError::new_parse_error(
                    DogStatsDMsgKind::ServiceCheck,
                    "Not enough fields, couldn't find status",
                    raw_msg.to_owned(),
                ))
            }
        };

        let mut timestamp = None;
        let mut hostname = None;
        let mut message = None;
        let mut tags = smallvec![];
        for field in fields {
            match field.chars().next() {
                Some('d') => timestamp = Some(&field[2..]),
                Some('h') => hostname = Some(&field[2..]),
                Some('m') => message = Some(&field[2..]),
                Some('#') => tags.extend(field[1..].split(',')),
                _ => {
                    return Err(DogStatsDMsgError::new_parse_error(
                        DogStatsDMsgKind::ServiceCheck,
                        "Unknown servicecheck field value found",
                        raw_msg.to_owned(),
                    ));
                }
            }
        }

        Ok(DogStatsDMsg::ServiceCheck(DogStatsDServiceCheckStr {
            raw_msg,
            name,
            tags,
            status,
            timestamp,
            hostname,
            message,
        }))
    }

    pub fn new(str_msg: &'a str) -> Result<Self, DogStatsDMsgError> {
        if str_msg.starts_with("_e") {
            return Self::parse_event(str_msg);
        }
        if str_msg.starts_with("_sc") {
            return Self::parse_servicecheck(str_msg);
        }
        Self::parse_metric(str_msg)
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
    use lading_payload::dogstatsd::{self};
    use rand::{rngs::SmallRng, SeedableRng};

    use super::*;

    macro_rules! metric_test {
        ($name:ident, $input:expr, $expected_name:expr, $expected_values:expr, $expected_type:expr, $expected_tags:expr, $expected_sample_rate:expr, $expected_timestamp:expr, $expected_container_id:expr, $expected_error:expr) => {
            #[test]
            fn $name() {
                let msg = match DogStatsDMsg::new($input) {
                    Ok(DogStatsDMsg::Metric(m)) => m,
                    Ok(DogStatsDMsg::ServiceCheck(_)) => {
                        panic!("Got service check, expected metric")
                    }
                    Ok(DogStatsDMsg::Event(_)) => panic!("Got event, expected metric"),
                    Err(e) => {
                        let Some((expected_error_kind, expected_error_message)) = $expected_error else {
                            panic!("Got an error but did not expect one {}", e);
                        };
                        let expected_error = DogStatsDMsgError::new_parse_error(expected_error_kind, expected_error_message, $input.to_owned());
                        assert_eq!(e, expected_error);
                        return;
                    },
                };

                assert!($expected_error.is_none());

                assert_eq!(msg.raw_msg, $input);
                assert_eq!(msg.name, $expected_name);
                let expected_values: SmallVec<f64, MAX_TAGS> = $expected_values;
                assert_eq!(msg.values, expected_values);
                assert_eq!(msg.metric_type, $expected_type);
                let expected_tags: SmallVec<&str, MAX_TAGS> = $expected_tags;
                assert_eq!(msg.tags, expected_tags);
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
                let msg = match DogStatsDMsg::new($input) {
                    Ok(DogStatsDMsg::Event(e)) => e,
                    Ok(DogStatsDMsg::ServiceCheck(_)) => {
                        panic!("Got service check, expected metric")
                    }
                    Ok(DogStatsDMsg::Metric(_)) => panic!("Got metric, expected event"),
                    Err(e) => match $expected_error {
                        Some(_expected_error) => {
                            let Some((expected_error_kind, expected_error_message)) = $expected_error else {
                                panic!("Got an error but did not expect one {}", e);
                            };
                            let expected_error = DogStatsDMsgError::new_parse_error(expected_error_kind, expected_error_message, $input.to_owned());
                            assert_eq!(e, expected_error);
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
                let expected_tags: SmallVec<&str, MAX_TAGS> = $expected_tags;
                assert_eq!(msg.tags, expected_tags);
            }
        };
    }

    const NO_ERR: Option<(DogStatsDMsgKind, &str)> = None::<(DogStatsDMsgKind, &str)>;

    metric_test!(
        basic_metric,
        "metric.name:1|c",
        "metric.name",
        smallvec![1.0],
        DogStatsDMetricType::Count,
        smallvec![],
        None,
        None,
        None,
        NO_ERR
    );

    metric_test!(
        basic_gauge,
        "metric.name:1|g",
        "metric.name",
        smallvec![1.0],
        DogStatsDMetricType::Gauge,
        smallvec![],
        None,
        None,
        None,
        NO_ERR
    );

    metric_test!(
        basic_histogram,
        "metric.name:1|h",
        "metric.name",
        smallvec![1.0],
        DogStatsDMetricType::Histogram,
        smallvec![],
        None,
        None,
        None,
        NO_ERR
    );

    metric_test!(
        basic_timer,
        "metric.name:1|ms",
        "metric.name",
        smallvec![1.0],
        DogStatsDMetricType::Timer,
        smallvec![],
        None,
        None,
        None,
        NO_ERR
    );

    metric_test!(
        basic_set,
        "metric.name:1|s",
        "metric.name",
        smallvec![1.0],
        DogStatsDMetricType::Set,
        smallvec![],
        None,
        None,
        None,
        NO_ERR
    );

    metric_test!(
        basic_gauge_floating_value,
        "metric.name:1.321|g",
        "metric.name",
        smallvec![1.321],
        DogStatsDMetricType::Gauge,
        smallvec![],
        None,
        None,
        None,
        NO_ERR
    );

    metric_test!(
        basic_dist_floating_value,
        "metric.name:1.321|d",
        "metric.name",
        smallvec![1.321],
        DogStatsDMetricType::Distribution,
        smallvec![],
        None,
        None,
        None,
        NO_ERR
    );

    metric_test!(
        basic_dist_multi_floating_value,
        "metric.name:1.321:1.11111|d",
        "metric.name",
        smallvec![1.321, 1.11111],
        DogStatsDMetricType::Distribution,
        smallvec![],
        None,
        None,
        None,
        NO_ERR
    );

    metric_test!(
        metric_with_container_id,
        "metric.name:1|c|c:container123",
        "metric.name",
        smallvec![1.0],
        DogStatsDMetricType::Count,
        smallvec![],
        None,
        None,
        Some("container123"),
        NO_ERR
    );

    metric_test!(
        metric_with_everything,
        "metric.name:1|c|@0.5|T1234567890|c:container123|#tag1:value1,tag2",
        "metric.name",
        smallvec![1.0],
        DogStatsDMetricType::Count,
        smallvec!["tag1:value1", "tag2"],
        Some("0.5"),
        Some("1234567890"),
        Some("container123"),
        NO_ERR
    );

    metric_test!(
        metric_with_mixed_order,
        "metric.name:1|c|#tag1:value1,tag2|@0.5|c:container123|T1234567890",
        "metric.name",
        smallvec![1.0],
        DogStatsDMetricType::Count,
        smallvec!["tag1:value1", "tag2"],
        Some("0.5"),
        Some("1234567890"),
        Some("container123"),
        NO_ERR
    );

    metric_test!(
        metric_with_multiple_tags,
        "metric.name:1|c|#tag1:value1,tag2,tag3:another",
        "metric.name",
        smallvec![1.0],
        DogStatsDMetricType::Count,
        smallvec!["tag1:value1", "tag2", "tag3:another"],
        None,
        None,
        None,
        NO_ERR
    );

    metric_test!(
        metric_with_no_optional_fields,
        "metric.name:1|c",
        "metric.name",
        smallvec![1.0],
        DogStatsDMetricType::Count,
        smallvec![],
        None,
        None,
        None,
        NO_ERR
    );

    metric_test!(
        metric_with_unrecognized_field,
        "metric.name:1|c|x:unknown",
        "metric.name",
        smallvec![1.0],
        DogStatsDMetricType::Count,
        smallvec![],
        None,
        None,
        None,
        NO_ERR
    );

    metric_test!(
        malformed_metric_missing_value,
        "metric.name:|c",
        "metric.name",
        smallvec![],
        DogStatsDMetricType::Count,
        smallvec![],
        None,
        None,
        None,
        Some((DogStatsDMsgKind::Metric, "Invalid or no value found"))
    );

    metric_test!(
        malformed_metric_invalid_format,
        "metric.name|1|c",
        "metric.name",
        smallvec![1.0],
        DogStatsDMetricType::Count,
        smallvec![],
        None,
        None,
        None,
        Some((DogStatsDMsgKind::Metric, "Name or value missing"))
    );

    metric_test!(
        security_msg,
        "datadog.security_agent.compliance.inputs.duration_ms:19.489043|ms|#dd.internal.entity_id:484d54a7-8851-490f-9efa-9fd7f870cdb8,env:staging,service:datadog-agent,rule_id:xccdf_org.ssgproject.content_rule_file_permissions_cron_monthly,rule_input_type:xccdf,agent_version:7.48.0-rc.0+git.217.1425a0f",
        "datadog.security_agent.compliance.inputs.duration_ms",
        smallvec![19.489043],
        DogStatsDMetricType::Timer,
        smallvec!["dd.internal.entity_id:484d54a7-8851-490f-9efa-9fd7f870cdb8", "env:staging", "service:datadog-agent", "rule_id:xccdf_org.ssgproject.content_rule_file_permissions_cron_monthly", "rule_input_type:xccdf", "agent_version:7.48.0-rc.0+git.217.1425a0f"],
        None,
        None,
        None,
        NO_ERR
    );

    event_test!(
        basic_event,
        "_e{5,4}:title|text",
        "title",
        "text",
        None,
        None,
        None,
        EventAlert::Info,
        smallvec![],
        NO_ERR
    );

    event_test!(
        basic_event_short_title_text,
        "_e{1,1}:t|t",
        "t",
        "t",
        None,
        None,
        None,
        EventAlert::Info,
        smallvec![],
        NO_ERR
    );

    event_test!(
        event_with_no_text,
        "_e{1,0}:t|",
        "t",
        "",
        None,
        None,
        None,
        EventAlert::Info,
        smallvec![],
        NO_ERR // This is arguably invalid, but don't care at the moment
    );

    event_test!(
        event_with_basic_fields,
        "_e{2,4}:ab|cdef|d:160|h:myhost|p:high|t:error|#env:prod,onfire:true\n",
        "ab",
        "cdef",
        Some("160"),
        Some("myhost"),
        Some("high"),
        EventAlert::Error,
        smallvec!["env:prod", "onfire:true"],
        NO_ERR
    );

    event_test!(
        invalid_event_text_length,
        "_e{100,0}:t|",
        "t",
        "",
        None,
        None,
        None,
        EventAlert::Info,
        smallvec![],
        Some((
            DogStatsDMsgKind::Event,
            "Title length specified is longer than msg length"
        ))
    );

    #[test]
    fn basic_events() {
        // _e{<TITLE_UTF8_LENGTH>,<TEXT_UTF8_LENGTH>}:<TITLE>|<TEXT>|d:<TIMESTAMP>|h:<HOSTNAME>|p:<PRIORITY>|t:<ALERT_TYPE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        let raw_msg = "_e{2,4}:ab|cdef|d:160|h:myhost|p:high|t:severe|#env:prod,onfire:true\n";
        let msg = match DogStatsDMsg::new(raw_msg) {
            Ok(DogStatsDMsg::Event(m)) => m,
            Err(e) => panic!("Unexpected error: {}", e),
            Ok(_) => panic!("Wrong type"),
        };
        assert_eq!(msg.title, "ab");
        assert_eq!(msg.text, "cdef");
    }

    #[test]
    fn lading_test() {
        let mut rng = SmallRng::seed_from_u64(34512423); // todo use random seed
        let config = dogstatsd::Config::default();
        let dd = dogstatsd::DogStatsD::new(config, &mut rng)
        .expect("Failed to create dogstatsd generator");

        for _ in 0..500_000 {
            let lading_msg = dd.generate(&mut rng).unwrap();
            let str_lading_msg = format!("{}", lading_msg);
            let msg = DogStatsDMsg::new(str_lading_msg.as_str()).unwrap();
            match lading_msg {
                dogstatsd::Member::Event(ld_event) => match msg {
                    DogStatsDMsg::Metric(_) => panic!("Wrong type"),
                    DogStatsDMsg::Event(e_parsed) => {
                        assert_eq!(e_parsed.title, ld_event.title);
                        assert_eq!(e_parsed.aggregation_key, ld_event.aggregation_key);
                        assert_eq!(e_parsed.hostname, ld_event.hostname);
                        assert_eq!(e_parsed.text, ld_event.text);
                        assert_eq!(e_parsed.source_type_name, ld_event.source_type_name);

                        // todo: Implement to/from
                        // assert_eq!(e_parsed.priority, ld_event.priority);

                        // todo: Represent timestamp as Option<u32>
                        // assert_eq!(e_parsed.timestamp, ld_event.timestamp);
                        if let Some(ld_alert_type) = ld_event.alert_type {
                            let ld_alert_as_alert: EventAlert = ld_alert_type.into();
                            assert_eq!(ld_alert_as_alert, e_parsed.alert_type);
                        } else {
                            assert_eq!(EventAlert::Info, e_parsed.alert_type);
                        }
                    }
                    DogStatsDMsg::ServiceCheck(_) => panic!("Wrong type"),
                },
                dogstatsd::Member::ServiceCheck(ld_sc) => {
                    match msg {
                        DogStatsDMsg::Metric(_) => panic!("Wrong type"),
                        DogStatsDMsg::ServiceCheck(sc_parsed) => {
                            assert_eq!(sc_parsed.name, ld_sc.name);
                            assert_eq!(sc_parsed.hostname, ld_sc.hostname);
                            assert_eq!(sc_parsed.message, ld_sc.message);
                            // todo: Represent our timestamp as option<u32>
                            // assert_eq!(sc_parsed.timestamp, ld_sc.timestamp_second);

                            // todo: implement into/from
                            // assert_eq!(sc_parsed.status, sc.status);
                            if let Some(_ld_sc_tags) = ld_sc.tags {
                                // todo: implement into/from
                                // assert_eq!(sc_parsed.tags, ld_sc_tags);
                            } else {
                                assert_eq!(sc_parsed.tags.len(), 0);
                            }
                        }
                        DogStatsDMsg::Event(_) => panic!("Wrong type"),
                    }
                }
                dogstatsd::Member::Metric(m) => match m {
                    lading_payload::dogstatsd::metric::Metric::Count(c) => match msg {
                        DogStatsDMsg::ServiceCheck(_) => panic!("Wrong type"),
                        DogStatsDMsg::Metric(m_parsed) => {
                            assert_eq!(m_parsed.name, c.name);
                            assert_eq!(m_parsed.metric_type, DogStatsDMetricType::Count);
                            assert_eq!(m_parsed.container_id, c.container_id);
                            for t in m_parsed.tags {
                                assert!(c.tags.contains(&t.to_owned()));
                            }
                        }
                        DogStatsDMsg::Event(_) => panic!("Wrong type"),
                    },
                    lading_payload::dogstatsd::metric::Metric::Gauge(g) => match msg {
                        DogStatsDMsg::ServiceCheck(_) => panic!("Wrong type"),
                        DogStatsDMsg::Metric(m_parsed) => {
                            assert_eq!(m_parsed.name, g.name);
                            assert_eq!(m_parsed.metric_type, DogStatsDMetricType::Gauge);
                        }
                        DogStatsDMsg::Event(_) => panic!("Wrong type"),
                    },
                    lading_payload::dogstatsd::metric::Metric::Histogram(h) => match msg {
                        DogStatsDMsg::ServiceCheck(_) => panic!("Wrong type"),
                        DogStatsDMsg::Metric(m_parsed) => {
                            assert_eq!(m_parsed.name, h.name);
                            assert_eq!(m_parsed.metric_type, DogStatsDMetricType::Histogram);
                        }
                        DogStatsDMsg::Event(_) => panic!("Wrong type"),
                    },
                    lading_payload::dogstatsd::metric::Metric::Timer(t) => match msg {
                        DogStatsDMsg::ServiceCheck(_) => panic!("Wrong type"),
                        DogStatsDMsg::Metric(m_parsed) => {
                            assert_eq!(m_parsed.name, t.name);
                            assert_eq!(m_parsed.metric_type, DogStatsDMetricType::Timer);
                        }
                        DogStatsDMsg::Event(_) => panic!("Wrong type"),
                    },
                    lading_payload::dogstatsd::metric::Metric::Distribution(d) => match msg {
                        DogStatsDMsg::ServiceCheck(_) => panic!("Wrong type"),
                        DogStatsDMsg::Metric(m_parsed) => {
                            assert_eq!(m_parsed.name, d.name);
                            assert_eq!(m_parsed.metric_type, DogStatsDMetricType::Distribution);
                        }
                        DogStatsDMsg::Event(_) => panic!("Wrong type"),
                    },
                    lading_payload::dogstatsd::metric::Metric::Set(s) => match msg {
                        DogStatsDMsg::ServiceCheck(_) => panic!("Wrong type"),
                        DogStatsDMsg::Metric(m_parsed) => {
                            assert_eq!(m_parsed.name, s.name);
                            assert_eq!(m_parsed.metric_type, DogStatsDMetricType::Set);
                        }
                        DogStatsDMsg::Event(_) => panic!("Wrong type"),
                    },
                },
            }
        }
    }

    #[test]
    fn basic_service_checks() {
        // _sc|<NAME>|<STATUS>|d:<TIMESTAMP>|h:<HOSTNAME>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|m:<SERVICE_CHECK_MESSAGE>
        let raw_msg = "_sc|ab|2|d:160|h:myhost|#env:prod,onfire:true|m:mymessage\n";
        let msg = match DogStatsDMsg::new(raw_msg) {
            Ok(DogStatsDMsg::ServiceCheck(m)) => m,
            Ok(_) => panic!("Wrong type"),
            Err(e) => panic!("Unexpected error {}", e),
        };
        assert_eq!(msg.hostname, Some("myhost"));
        assert_eq!(msg.timestamp, Some("160"));
        assert_eq!(msg.message, Some("mymessage"));
        assert_eq!(msg.name, "ab");
        assert_eq!(msg.status, ServiceCheckStatus::Critical);
    }

    #[test]
    fn invalid_statsd_msg() {
        let mut found_expected_error = false;
        if let Err(DogStatsDMsgError::ParseError { kind, .. } ) = DogStatsDMsg::new("abcdefghiq") {
            found_expected_error = kind == DogStatsDMsgKind::Metric
        }
        assert!(found_expected_error);
    }
}
