use bytes::{Buf, Bytes};
use prost::Message;

use crate::dogstatsdreplayreader::dogstatsd::unix::UnixDogstatsdMsg;

const DATADOG_HEADER: [u8; 8] = [0xD4, 0x74, 0xD0, 0x60, 0xF0, 0xFF, 0x00, 0x00];

pub mod dogstatsd {
    pub mod unix {
        include!(concat!(env!("OUT_DIR"), "/dogstatsd.unix.rs"));
    }
}
pub struct ReplayReader {
    buf: Bytes,
}

impl ReplayReader {
    pub fn read_msg(&mut self) -> Option<UnixDogstatsdMsg> {
        if self.buf.remaining() < 4 {
            return None;
        }

        // Read the little endian uint32 that gives the length of the next protobuf message
        let message_length = self.buf.get_u32_le() as usize;

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

    pub fn new(mut buf: Bytes) -> Self {
        // TODO test if its a replay file and return Err if it is.

        buf.advance(8); // eat the header

        Self { buf }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    const TWO_MSGS_ONE_LINE_EACH: &[u8] = &[
        0xd4, 0x74, 0xd0, 0x60, 0xf3, 0xff, 0x00, 0x00, 0x93, 0x00, 0x00, 0x00, 0x08, 0x84, 0xe2,
        0x88, 0x8a, 0xe0, 0xb6, 0x87, 0xbf, 0x17, 0x10, 0x83, 0x01, 0x1a, 0x83, 0x01, 0x73, 0x74,
        0x61, 0x74, 0x73, 0x64, 0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2e, 0x74, 0x69,
        0x6d, 0x65, 0x2e, 0x6d, 0x69, 0x63, 0x72, 0x6f, 0x73, 0x3a, 0x32, 0x2e, 0x33, 0x39, 0x32,
        0x38, 0x33, 0x7c, 0x64, 0x7c, 0x40, 0x31, 0x2e, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x7c,
        0x23, 0x65, 0x6e, 0x76, 0x69, 0x72, 0x6f, 0x6e, 0x6d, 0x65, 0x6e, 0x74, 0x3a, 0x64, 0x65,
        0x76, 0x7c, 0x63, 0x3a, 0x32, 0x61, 0x32, 0x35, 0x66, 0x37, 0x66, 0x63, 0x38, 0x66, 0x62,
        0x66, 0x35, 0x37, 0x33, 0x64, 0x36, 0x32, 0x30, 0x35, 0x33, 0x64, 0x37, 0x32, 0x36, 0x33,
        0x64, 0x64, 0x32, 0x64, 0x34, 0x34, 0x30, 0x63, 0x30, 0x37, 0x62, 0x36, 0x61, 0x62, 0x34,
        0x64, 0x32, 0x62, 0x31, 0x30, 0x37, 0x65, 0x35, 0x30, 0x62, 0x30, 0x64, 0x34, 0x64, 0x66,
        0x31, 0x66, 0x32, 0x65, 0x65, 0x31, 0x35, 0x66, 0x0a, 0x93, 0x00, 0x00, 0x00, 0x08, 0x9f,
        0xe9, 0xbd, 0x83, 0xe3, 0xb6, 0x87, 0xbf, 0x17, 0x10, 0x83, 0x01, 0x1a, 0x83, 0x01, 0x73,
        0x74, 0x61, 0x74, 0x73, 0x64, 0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2e, 0x74,
        0x69, 0x6d, 0x65, 0x2e, 0x6d, 0x69, 0x63, 0x72, 0x6f, 0x73, 0x3a, 0x32, 0x2e, 0x33, 0x39,
        0x32, 0x38, 0x33, 0x7c, 0x64, 0x7c, 0x40, 0x31, 0x2e, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30,
        0x7c, 0x23, 0x65, 0x6e, 0x76, 0x69, 0x72, 0x6f, 0x6e, 0x6d, 0x65, 0x6e, 0x74, 0x3a, 0x64,
        0x65, 0x76, 0x7c, 0x63, 0x3a, 0x32, 0x61, 0x32, 0x35, 0x66, 0x37, 0x66, 0x63, 0x38, 0x66,
        0x62, 0x66, 0x35, 0x37, 0x33, 0x64, 0x36, 0x32, 0x30, 0x35, 0x33, 0x64, 0x37, 0x32, 0x36,
        0x33, 0x64, 0x64, 0x32, 0x64, 0x34, 0x34, 0x30, 0x63, 0x30, 0x37, 0x62, 0x36, 0x61, 0x62,
        0x34, 0x64, 0x32, 0x62, 0x31, 0x30, 0x37, 0x65, 0x35, 0x30, 0x62, 0x30, 0x64, 0x34, 0x64,
        0x66, 0x31, 0x66, 0x32, 0x65, 0x65, 0x31, 0x35, 0x66, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00,
    ];

    #[test]
    fn two_msg_two_lines() {
        let mut replay = ReplayReader::new(Bytes::from(TWO_MSGS_ONE_LINE_EACH));
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

        // TODO this is a BUG
        // I believe we're incorrectly _not_ detecting the end of the replay file
        // There is tagger state at the end of a capture file
        // I just haven't cared about it yet, but its worth parsing out
        assert_eq!(Some(UnixDogstatsdMsg::default()), replay.read_msg());
        assert_eq!(Some(UnixDogstatsdMsg::default()), replay.read_msg());
        assert_eq!(None, replay.read_msg())
    }
}
