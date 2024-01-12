use std::{collections::VecDeque, str::Utf8Error};
use etherparse::SlicedPacket;
use thiserror::Error;

use bytes::Bytes;
use tracing::{warn, info, error};

use crate::pcapreader::{PcapReader, PcapReaderError};


#[derive(Error, Debug)]
pub enum PcapDogStatsDReaderError {
    #[error("Error from pcap reader")]
    PcapReader(PcapReaderError),
    #[error("Invalid UTF-8 sequence found in packet")]
    InvalidUtf8Sequence(Utf8Error),
    #[error("Ethernet frame parsing error")]
    Ethernet(#[from] etherparse::ReadError),
}

pub struct PcapDogStatsDReader {
    pcap_reader: PcapReader,
    current_messages: VecDeque<String>,
}

fn payload_from_pcap(packet: SlicedPacket) -> Bytes {
    if let Some(ethertype) = packet.payload_ether_type() {
        match etherparse::SlicedPacket::from_ether_type(ethertype, packet.payload) {
            Ok(value) => {
                info!("Found nested packet with ethertype: {ethertype}. Recursing into it.");
                return payload_from_pcap(value);
            }
            Err(e) => {
                error!("Failed to parse payload from ethertype ({ethertype}): {e:?}");
            }
        }
    } else {
        info!("Packet does not contain a nested packet, testing below for relevant fields");
    }
    if let Some(link) = packet.link {
        info!("Link: {:?}", link);
    }
    if let Some(vlan) = packet.vlan {
        info!("vlan: {:?}", vlan)
    }
    if let Some(ip) = packet.ip {
        info!("ip: {:?}", ip)
    }
    if let Some(transport) = packet.transport {
        // could be Some(Udp(_))
        info!("transport: {:?}", transport);

        return Bytes::copy_from_slice(packet.payload);
    }

    Bytes::copy_from_slice(packet.payload)
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

        match self.pcap_reader.read_packet() {
            Ok(Some(packet)) => {
                // packet.data contains the full ethernet frame
                // so lets try to find the udp packets within

                info!("Got raw PCAP packet of length: {}", packet.data.len());
                let data: Bytes = match etherparse::SlicedPacket::from_ethernet(&packet.data) {
                    Ok(value) => {
                        payload_from_pcap(value)
                    }
                    Err(e) => {
                        warn!("Couldn't parse packet from pcap as IP: {e}");
                        return Err(PcapDogStatsDReaderError::Ethernet(e));
                    }
                };

                info!("Parsed out what I hope is a payload: {data:?}");
                match std::str::from_utf8(&data) {
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
    use crate::init_logging;

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
        init_logging();

        let mut reader = PcapDogStatsDReader::new(Bytes::from_static(PCAP_SINGLE_MESSAGE)).unwrap();

        let mut s = String::new();

        let res = reader.read_msg(&mut s).unwrap();
        assert_eq!(res, 1);
        assert_eq!("statsd.example.time.micros:2.39283|d|@1.000000|#environment:dev|c:2a25f7fc8fbf573d62053d7263dd2d440c07b6ab4d2b107e50b0d4df1f2ee15f", s);
        s.clear();
    }
}