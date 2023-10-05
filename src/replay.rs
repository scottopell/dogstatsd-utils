use bytes::{Buf, Bytes};
use prost::Message;

use crate::dogstatsdreplayreader::dogstatsd::unix::UnixDogstatsdMsg;

const DATADOG_HEADER: &[u8] = &[0xD4, 0x74, 0xD0, 0x60];
use thiserror::Error;

pub mod dogstatsd {
    pub mod unix {
        include!(concat!(env!("OUT_DIR"), "/dogstatsd.unix.rs"));
    }
}

// TODO currently missing ability to read tagger state from replay file
// If this is desired, the length can be found as the last 4 bytes of the replay file
// Only present in version 2 or greater
#[derive(Debug)]
pub struct ReplayReader {
    buf: Bytes,
    read_all_unixdogstatsdmsg: bool,
}

#[derive(Error, Debug, PartialEq)]
pub enum ReplayReaderError {
    #[error("No dogstatsd replay marker found")]
    NotAReplayFile,
    #[error("Unsupported replay version")]
    UnsupportedReplayVersion(u8),
}

impl ReplayReader {
    pub fn supported_versions() -> &'static [u8] {
        &[4]
    }
    /// read_msg will return the next UnixDogstatsdMsg if it exists
    pub fn read_msg(&mut self) -> Option<UnixDogstatsdMsg> {
        if self.buf.remaining() < 4 || self.read_all_unixdogstatsdmsg {
            return None;
        }

        // Read the little endian uint32 that gives the length of the next protobuf message
        let message_length = self.buf.get_u32_le() as usize;

        if message_length == 0 {
            // This indicates a record separator between UnixDogStatsdMsg list
            // and the tagger state. Next bytes are all for tagger state.
            self.read_all_unixdogstatsdmsg = true;
            return None;
        }

        if self.buf.remaining() < message_length {
            // end of stream
            return None;
        }

        // Read the protobuf message
        let msg_buf = self.buf.copy_to_bytes(message_length);

        // Decode the protobuf message using the provided .proto file
        match UnixDogstatsdMsg::decode(msg_buf) {
            Ok(msg) => Some(msg),
            Err(e) => {
                println!(
                    "Unexpected error decoding msg buf: {} do you have a valid dsd capture file?",
                    e
                );
                None
            }
        }
    }

    pub fn new(mut buf: Bytes) -> Result<Self, ReplayReaderError> {
        let header = buf.copy_to_bytes(4);
        if header != DATADOG_HEADER {
            return Err(ReplayReaderError::NotAReplayFile);
        }
        // Next byte describes the replay version
        // f0 is bitwise or'd with the file version, so to get the file version, do a bitwise xor
        let version = buf.get_u8() ^ 0xF0;

        if version != 3 {
            return Err(ReplayReaderError::UnsupportedReplayVersion(version));
        }
        // Consume the next 3 bytes, the rest of the file header
        buf.advance(3);

        Ok(Self {
            buf,
            read_all_unixdogstatsdmsg: false,
        })
    }
}

/*
pub struct ReplayAssembler {
    buf: Bytes,
}

impl ReplayAssembler {
    pub fn new() {

        Self {
            buf: Bytes::new(),
        }
    }
    pub fn add_msg(msg: UnixDogstatsdMsg) {}

    pub fn finalize() -> Bytes {}
} */

#[cfg(test)]
mod tests {

    use super::*;

    // TODO can't decide if I like this representation or not.
    const TWO_MSGS_ONE_LINE_EACH: &[u8] = &[
        0xD4, b't', 0xD0, b'`', 0xF3, 0xFF, 0x00, 0x00, 0x93, 0x00, 0x00, 0x00, 0x08, 0x84, 0xE2,
        0x88, 0x8A, 0xE0, 0xB6, 0x87, 0xBF, 0x17, 0x10, 0x83, 0x01, 0x1A, 0x83, 0x01, b's', b't',
        b'a', b't', b's', b'd', b'.', b'e', b'x', b'a', b'm', b'p', b'l', b'e', b'.', b't', b'i',
        b'm', b'e', b'.', b'm', b'i', b'c', b'r', b'o', b's', b':', b'2', b'.', b'3', b'9', b'2',
        b'8', b'3', b'|', b'd', b'|', b'@', b'1', b'.', b'0', b'0', b'0', b'0', b'0', b'0', b'|',
        b'#', b'e', b'n', b'v', b'i', b'r', b'o', b'n', b'm', b'e', b'n', b't', b':', b'd', b'e',
        b'v', b'|', b'c', b':', b'2', b'a', b'2', b'5', b'f', b'7', b'f', b'c', b'8', b'f', b'b',
        b'f', b'5', b'7', b'3', b'd', b'6', b'2', b'0', b'5', b'3', b'd', b'7', b'2', b'6', b'3',
        b'd', b'd', b'2', b'd', b'4', b'4', b'0', b'c', b'0', b'7', b'b', b'6', b'a', b'b', b'4',
        b'd', b'2', b'b', b'1', b'0', b'7', b'e', b'5', b'0', b'b', b'0', b'd', b'4', b'd', b'f',
        b'1', b'f', b'2', b'e', b'e', b'1', b'5', b'f', 0x0A, 0x93, 0x00, 0x00, 0x00, 0x08, 0x9F,
        0xE9, 0xBD, 0x83, 0xE3, 0xB6, 0x87, 0xBF, 0x17, 0x10, 0x83, 0x01, 0x1A, 0x83, 0x01, b's',
        b't', b'a', b't', b's', b'd', b'.', b'e', b'x', b'a', b'm', b'p', b'l', b'e', b'.', b't',
        b'i', b'm', b'e', b'.', b'm', b'i', b'c', b'r', b'o', b's', b':', b'2', b'.', b'3', b'9',
        b'2', b'8', b'3', b'|', b'd', b'|', b'@', b'1', b'.', b'0', b'0', b'0', b'0', b'0', b'0',
        b'|', b'#', b'e', b'n', b'v', b'i', b'r', b'o', b'n', b'm', b'e', b'n', b't', b':', b'd',
        b'e', b'v', b'|', b'c', b':', b'2', b'a', b'2', b'5', b'f', b'7', b'f', b'c', b'8', b'f',
        b'b', b'f', b'5', b'7', b'3', b'd', b'6', b'2', b'0', b'5', b'3', b'd', b'7', b'2', b'6',
        b'3', b'd', b'd', b'2', b'd', b'4', b'4', b'0', b'c', b'0', b'7', b'b', b'6', b'a', b'b',
        b'4', b'd', b'2', b'b', b'1', b'0', b'7', b'e', b'5', b'0', b'b', b'0', b'd', b'4', b'd',
        b'f', b'1', b'f', b'2', b'e', b'e', b'1', b'5', b'f', 0x0A, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00,
    ];

    fn hex_to_rust_literal(hex_codes: &[u8]) -> String {
        let mut result = String::from("[");

        for (i, &code) in hex_codes.iter().enumerate() {
            // Check if the code is in printable ASCII range
            if code >= 0x20 && code <= 0x7E {
                result.push_str(&format!("b'{}'", code as char));
            } else {
                result.push_str(&format!("0x{:02X}", code));
            }

            // If not the last element, add a comma and space
            if i != hex_codes.len() - 1 {
                result.push_str(", ");
            }
        }

        result.push(']');
        result
    }

    #[test]
    fn two_msg_two_lines() {
        println!(
            "TWO_MSGS_ONE_LINE_EACH follows: {}",
            hex_to_rust_literal(TWO_MSGS_ONE_LINE_EACH)
        );
        let mut replay = ReplayReader::new(Bytes::from(TWO_MSGS_ONE_LINE_EACH)).unwrap();
        let msg = replay.read_msg().unwrap();
        let mut expected_msg = UnixDogstatsdMsg::default();
        let expected_payload: &[u8] = &[
            b's', b't', b'a', b't', b's', b'd', b'.', b'e', b'x', b'a', b'm', b'p', b'l', b'e',
            b'.', b't', b'i', b'm', b'e', b'.', b'm', b'i', b'c', b'r', b'o', b's', b':', b'2',
            b'.', b'3', b'9', b'2', b'8', b'3', b'|', b'd', b'|', b'@', b'1', b'.', b'0', b'0',
            b'0', b'0', b'0', b'0', b'|', b'#', b'e', b'n', b'v', b'i', b'r', b'o', b'n', b'm',
            b'e', b'n', b't', b':', b'd', b'e', b'v', b'|', b'c', b':', b'2', b'a', b'2', b'5',
            b'f', b'7', b'f', b'c', b'8', b'f', b'b', b'f', b'5', b'7', b'3', b'd', b'6', b'2',
            b'0', b'5', b'3', b'd', b'7', b'2', b'6', b'3', b'd', b'd', b'2', b'd', b'4', b'4',
            b'0', b'c', b'0', b'7', b'b', b'6', b'a', b'b', b'4', b'd', b'2', b'b', b'1', b'0',
            b'7', b'e', b'5', b'0', b'b', b'0', b'd', b'4', b'd', b'f', b'1', b'f', b'2', b'e',
            b'e', b'1', b'5', b'f', 0x0A,
        ];
        //println!("{:#02X?}", expected_payload);

        let expected_ancillary_data: &[u8] = &[];
        expected_msg.payload = expected_payload.to_vec();

        expected_msg.payload_size = 131;
        expected_msg.pid = 0;
        expected_msg.timestamp = 1692823177480253700;
        expected_msg.ancillary = expected_ancillary_data.to_vec();
        expected_msg.ancillary_size = 0;
        assert_eq!(expected_msg, msg);

        let msg = replay.read_msg().unwrap();
        let mut expected_msg = UnixDogstatsdMsg::default();
        let expected_payload: &[u8] = &[
            115, 116, 97, 116, 115, 100, 46, 101, 120, 97, 109, 112, 108, 101, 46, 116, 105, 109,
            101, 46, 109, 105, 99, 114, 111, 115, 58, 50, 46, 51, 57, 50, 56, 51, 124, 100, 124,
            64, 49, 46, 48, 48, 48, 48, 48, 48, 124, 35, 101, 110, 118, 105, 114, 111, 110, 109,
            101, 110, 116, 58, 100, 101, 118, 124, 99, 58, 50, 97, 50, 53, 102, 55, 102, 99, 56,
            102, 98, 102, 53, 55, 51, 100, 54, 50, 48, 53, 51, 100, 55, 50, 54, 51, 100, 100, 50,
            100, 52, 52, 48, 99, 48, 55, 98, 54, 97, 98, 52, 100, 50, 98, 49, 48, 55, 101, 53, 48,
            98, 48, 100, 52, 100, 102, 49, 102, 50, 101, 101, 49, 53, 102, 10,
        ];
        let expected_ancillary_data: &[u8] = &[];
        expected_msg.payload = expected_payload.to_vec();

        expected_msg.payload_size = 131;
        expected_msg.pid = 0;
        expected_msg.timestamp = 1692823178271749279;
        expected_msg.ancillary = expected_ancillary_data.to_vec();
        expected_msg.ancillary_size = 0;
        assert_eq!(expected_msg, msg);

        assert_eq!(None, replay.read_msg())
    }

    #[test]
    fn invalid_replay_bytes() {
        let replay = ReplayReader::new(Bytes::from_static(b"my.metric:1|g\n"));
        assert_eq!(replay.unwrap_err(), ReplayReaderError::NotAReplayFile);

        let replay = ReplayReader::new(Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz"));
        assert_eq!(replay.unwrap_err(), ReplayReaderError::NotAReplayFile);

        let replay = ReplayReader::new(Bytes::from_static(b"\n\n\n\n\n\n\n\n\n\n\n\t\t\t\n\t\n"));
        assert_eq!(replay.unwrap_err(), ReplayReaderError::NotAReplayFile);
    }
}
