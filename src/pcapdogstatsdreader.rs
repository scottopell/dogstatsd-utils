use std::{collections::VecDeque, str::Utf8Error};
use thiserror::Error;

use bytes::Bytes;
use tracing::{warn, error, debug};

use crate::pcapreader::{PcapReader, PcapReaderError};


#[derive(Error, Debug)]
pub enum PcapDogStatsDReaderError {
    #[error("Error from pcap reader")]
    PcapReader(PcapReaderError),
    #[error("Invalid UTF-8 sequence found in packet")]
    InvalidUtf8Sequence(Utf8Error),
}

pub struct PcapDogStatsDReader {
    pcap_reader: PcapReader,
    current_messages: VecDeque<String>,
}


impl PcapDogStatsDReader {
    pub fn new(buf: Bytes) -> Result<Self, PcapDogStatsDReaderError> {
        match PcapReader::new(buf) {
            Ok(reader) => Ok(PcapDogStatsDReader {
                pcap_reader: reader,
                current_messages: VecDeque::new(),
            }),
            Err(e) => Err(PcapDogStatsDReaderError::PcapReader(e)),
        }
    }
    pub fn read_msg(&mut self, s: &mut String) -> Result<usize, PcapDogStatsDReaderError> {
        if let Some(line) = self.current_messages.pop_front() {
            s.insert_str(0, &line);
            return Ok(1);
        }
        let header = self.pcap_reader.header;

        match self.pcap_reader.read_packet() {
            Ok(Some(packet)) => {
                match PcapReader::get_udp_payload_from_packet(packet, header) {
                    Ok(Some(udp_payload)) => {
                        debug!("Got a UDP Payload of length {}", udp_payload.len());
                        match std::str::from_utf8(&udp_payload) {
                            Ok(v) => {
                                if v.is_empty() {
                                    // Read operation was successful, read 0 msgs
                                    return Ok(0);
                                }

                                for line in v.lines() {
                                    self.current_messages.push_back(String::from(line));
                                }

                                self.read_msg(s)
                            }
                            Err(e) => Err(PcapDogStatsDReaderError::InvalidUtf8Sequence(e)),
                        }
                    },
                    Ok(None) => {
                        debug!("Skipping non-udp packet");
                        self.read_msg(s)
                    },
                    Err(e) => {
                        error!("Error while trying to read a packet: {e}");
                        Err(PcapDogStatsDReaderError::PcapReader(e))
                    }
                }
            }
            Ok(None) => Ok(0), // Read was validly issued, just nothing to be read.
            Err(e) => {
                warn!("Error while trying to read a packet: {e}");
                Err(PcapDogStatsDReaderError::PcapReader(e))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const PCAP_SINGLE_MESSAGE: &[u8] = &[
        0xd4, 0xc3, 0xb2, 0xa1, 0x02, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x14, 0x01, 0x00, 0x00,
        0xef, 0xc0, 0x9d, 0x65, 0xb2, 0xbc, 0x0a, 0x00, 0x4f, 0x00, 0x00, 0x00,
        0x4f, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        0x03, 0x04, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x45, 0x00, 0x00, 0x3b, 0x30, 0xf0, 0x40, 0x00, 0x40, 0x11, 0x0b, 0xc0,
        0x7f, 0x00, 0x00, 0x01, 0x7f, 0x00, 0x00, 0x01, 0x8d, 0x81, 0x1f, 0xbd,
        0x00, 0x27, 0xfe, 0x3a, 0x61, 0x62, 0x63, 0x2e, 0x6d, 0x79, 0x2e, 0x66,
        0x61, 0x76, 0x2e, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x3a, 0x31, 0x7c,
        0x63, 0x7c, 0x23, 0x68, 0x6f, 0x73, 0x74, 0x3a, 0x66, 0x6f, 0x6f
    ];

    #[test]
    fn can_read_single_message_packet() {
        let mut reader = PcapDogStatsDReader::new(Bytes::from_static(PCAP_SINGLE_MESSAGE)).unwrap();

        let mut s = String::new();

        let res = reader.read_msg(&mut s).unwrap();
        assert_eq!(res, 1);
        assert_eq!("abc.my.fav.metric:1|c|#host:foo", s);
        s.clear();

        let res = reader.read_msg(&mut s).unwrap();
        assert_eq!(res, 0);
    }
}