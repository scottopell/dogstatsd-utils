use std::fmt::Debug;

use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum DogStatsDMsgError {
    #[error("Invalid format for msg")]
    InvalidFormat,
}

#[derive(Debug)]
pub enum DogStatsDStr<'a> {
    Metric(DogStatsDMetricStr<'a>),
    Event(DogStatsDEventStr<'a>),
    ServiceCheck(DogStatsDServiceCheckStr<'a>),
}

#[derive(Debug)]
pub struct DogStatsDEventStr<'a> {
    pub title: &'a str,
    pub text: &'a str,
    pub raw_msg: &'a str,
}

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
    pub metric_type: &'a str,
    pub raw_msg: &'a str,
}

impl<'a> DogStatsDStr<'a> {
    pub fn new(str_msg: &'a str) -> Result<Self, DogStatsDMsgError> {
        if str_msg.starts_with("_e") {
            return Ok(DogStatsDStr::Event(DogStatsDEventStr {
                title: "placeholder",
                text: "placeholder_raw_full_msg",
                raw_msg: str_msg,
            }));
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
                    None => return Err(DogStatsDMsgError::InvalidFormat),
                };
                let name = name_and_values.0;
                let values = name_and_values.1;

                let metric_type_and_rest: Vec<&str> = match parts.get(1) {
                    Some(s) => (*s).split('#').collect::<Vec<&str>>(),
                    None => return Err(DogStatsDMsgError::InvalidFormat),
                };
                let metric_type = match metric_type_and_rest.first() {
                    Some(s) => *s,
                    None => return Err(DogStatsDMsgError::InvalidFormat),
                };

                Ok(DogStatsDStr::Metric(DogStatsDMetricStr {
                    raw_msg: str_msg,
                    name,
                    values,
                    metric_type,
                }))
            }
            None => Err(DogStatsDMsgError::InvalidFormat),
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

    #[test]
    fn basic_statsd_msg() {
        let msg = match DogStatsDStr::new("my.metric:1|g\n") {
            Ok(DogStatsDStr::Metric(m)) => m,
            _ => panic!("Wrong type"),
        };
        assert_eq!(msg.raw_msg, "my.metric:1|g\n");
        assert_eq!(msg.name, "my.metric");
        assert_eq!(msg.values, "1");
        assert_eq!(msg.metric_type, "g");

        let raw_msg = "my.metric:2|c\n";
        let msg = match DogStatsDStr::new(raw_msg) {
            Ok(DogStatsDStr::Metric(m)) => m,
            _ => panic!("Wrong type"),
        };
        assert_eq!(msg.name, "my.metric");
        assert_eq!(msg.values, "2");
        assert_eq!(msg.metric_type, "c");

        let raw_msg = "my.metric:2.45|d\n";
        let msg = match DogStatsDStr::new(raw_msg) {
            Ok(DogStatsDStr::Metric(m)) => m,
            _ => panic!("Wrong type"),
        };
        assert_eq!(msg.name, "my.metric");
        assert_eq!(msg.values, "2.45");
        assert_eq!(msg.metric_type, "d");

        let raw_msg = "my.metric:2.45:3.45|d\n";
        let msg = match DogStatsDStr::new(raw_msg) {
            Ok(DogStatsDStr::Metric(m)) => m,
            _ => panic!("Wrong type"),
        };
        assert_eq!(msg.name, "my.metric");
        assert_eq!(msg.values, "2.45:3.45");
        assert_eq!(msg.metric_type, "d");

        // more formats to cover
        // <METRIC_NAME>:<VALUE>|h|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|h|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
    }

    #[test]
    fn basic_events() {
        // _e{<TITLE_UTF8_LENGTH>,<TEXT_UTF8_LENGTH>}:<TITLE>|<TEXT>|d:<TIMESTAMP>|h:<HOSTNAME>|p:<PRIORITY>|t:<ALERT_TYPE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        let raw_msg = "_e{2,4}:ab|cdef|d:160|h:myhost|p:high|t:severe|#env:prod,onfire:true\n";
        let msg = match DogStatsDStr::new(raw_msg) {
            Ok(DogStatsDStr::Event(m)) => m,
            _ => panic!("Wrong type"),
        };
        // Not implemented yet
        // assert_eq!(msg.title, "ab");
        // assert_eq!(msg.text, "cdef");
    }

    #[test]
    fn basic_service_checks() {
        // _sc|<NAME>|<STATUS>|d:<TIMESTAMP>|h:<HOSTNAME>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|m:<SERVICE_CHECK_MESSAGE>
        let raw_msg = "_sc:ab|error|d:160|h:myhost|#env:prod,onfire:true|m:mymessage\n";
        let msg = match DogStatsDStr::new(raw_msg) {
            Ok(DogStatsDStr::ServiceCheck(m)) => m,
            _ => panic!("Wrong type"),
        };
        // No other fields implemented
    }

    #[test]
    fn invalid_statsd_msg() {
        assert_eq!(
            DogStatsDMsgError::InvalidFormat,
            DogStatsDStr::new("abcdefghiq").unwrap_err()
        );
    }
}
