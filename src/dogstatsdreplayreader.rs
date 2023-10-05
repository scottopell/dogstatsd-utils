use std::collections::VecDeque;

use bytes::{Buf, Bytes};

use prost::Message;

use crate::dogstatsdreplayreader::dogstatsd::unix::UnixDogstatsdMsg;

const DATADOG_HEADER: [u8; 8] = [0xD4, 0x74, 0xD0, 0x60, 0xF0, 0xFF, 0x00, 0x00];

pub mod dogstatsd {
    pub mod unix {
        include!(concat!(env!("OUT_DIR"), "/dogstatsd.unix.rs"));
    }
}

pub struct DogStatsDReplayReader {
    buf: Bytes,
    current_messages: VecDeque<String>,
}

impl DogStatsDReplayReader {
    // TODO this currently returns an entire dogstatsd replay payload, which is not a single dogstatsd message.
    pub fn read_msg(&mut self, s: &mut String) -> std::io::Result<usize> {
        if let Some(line) = self.current_messages.pop_front() {
            s.insert_str(0, &line);
            return Ok(1);
        }

        if self.buf.remaining() < 4 {
            return Ok(0); // end of stream
        }

        // Read the little endian uint32 that gives the length of the next protobuf message
        let message_length = self.buf.get_u32_le() as usize;

        if self.buf.remaining() < message_length {
            return Ok(0); // end of stream
        }

        // Read the protobuf message
        let msg_buf = self.buf.copy_to_bytes(message_length);

        // Decode the protobuf message using the provided .proto file
        let message = UnixDogstatsdMsg::decode(msg_buf)?;
        match std::str::from_utf8(&message.payload) {
            Ok(v) => {
                if v.len() == 0 {
                    return Ok(0); // end of stream
                }

                // should already be empty
                self.current_messages.clear();
                for line in v.lines() {
                    self.current_messages.push_back(String::from(line));
                }

                let line = self
                    .current_messages
                    .pop_front()
                    .expect("Found no next line, why not?? ");

                s.insert_str(0, &line);
                Ok(1)
            }
            Err(e) => panic!("Invalid utf-8 sequence: {}", e),
        }
    }

    pub fn new(mut buf: Bytes) -> Self {
        buf.advance(8); // eat the header

        DogStatsDReplayReader {
            buf,
            current_messages: VecDeque::new(),
        }
    }
}

pub fn is_replay_header(header: &[u8]) -> std::io::Result<()> {
    if header.len() <= 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Not enough bytes to determine if its a replay file",
        ));
    }

    // f0 is bitwise or'd with the file version, so to get the file version, lets do a bitwise xor
    let version = header[4] ^ 0xF0;

    if version != 3 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Unexpected version, wanted 3 but found {}", version),
        ));
    }

    if header[0..4] != DATADOG_HEADER[0..4] {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Did not find replay header. Found: {:X?}", header),
        ));
    }

    return Ok(());
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

    const ONE_MSG_TWO_LINES: &[u8] = &[
        0xd4, 0x74, 0xd0, 0x60, 0xf3, 0xff, 0x00, 0x00, 0xe6, 0x00, 0x00, 0x00, 0x08, 0xf7, 0xc3,
        0xb4, 0xdc, 0xfa, 0x85, 0x88, 0xbf, 0x17, 0x10, 0xd6, 0x01, 0x1a, 0xd6, 0x01, 0x73, 0x74,
        0x61, 0x74, 0x73, 0x64, 0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2e, 0x74, 0x69,
        0x6d, 0x65, 0x2e, 0x6d, 0x69, 0x63, 0x72, 0x6f, 0x73, 0x3a, 0x32, 0x2e, 0x33, 0x39, 0x32,
        0x38, 0x33, 0x7c, 0x64, 0x7c, 0x40, 0x31, 0x2e, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x7c,
        0x23, 0x65, 0x6e, 0x76, 0x69, 0x72, 0x6f, 0x6e, 0x6d, 0x65, 0x6e, 0x74, 0x3a, 0x64, 0x65,
        0x76, 0x2c, 0x6e, 0x6f, 0x77, 0x3a, 0x32, 0x30, 0x32, 0x33, 0x2d, 0x30, 0x38, 0x2d, 0x32,
        0x33, 0x54, 0x32, 0x31, 0x3a, 0x32, 0x34, 0x3a, 0x35, 0x39, 0x2b, 0x30, 0x30, 0x3a, 0x30,
        0x30, 0x7c, 0x63, 0x3a, 0x32, 0x61, 0x32, 0x35, 0x66, 0x37, 0x66, 0x63, 0x38, 0x66, 0x62,
        0x66, 0x35, 0x37, 0x33, 0x64, 0x36, 0x32, 0x30, 0x35, 0x33, 0x64, 0x37, 0x32, 0x36, 0x33,
        0x64, 0x64, 0x32, 0x64, 0x34, 0x34, 0x30, 0x63, 0x30, 0x37, 0x62, 0x36, 0x61, 0x62, 0x34,
        0x64, 0x32, 0x62, 0x31, 0x30, 0x37, 0x65, 0x35, 0x30, 0x62, 0x30, 0x64, 0x34, 0x64, 0x66,
        0x31, 0x66, 0x32, 0x65, 0x65, 0x31, 0x35, 0x66, 0x0a, 0x73, 0x74, 0x61, 0x74, 0x73, 0x64,
        0x2e, 0x6f, 0x74, 0x68, 0x65, 0x72, 0x2e, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x3a, 0x38,
        0x2e, 0x37, 0x7c, 0x67, 0x7c, 0x40, 0x31, 0x2e, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x7c,
        0x23, 0x65, 0x6e, 0x76, 0x69, 0x72, 0x6f, 0x6e, 0x6d, 0x65, 0x6e, 0x74, 0x3a, 0x64, 0x65,
        0x76, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    const ONE_MSG_THREE_LINES: &[u8] = &[
        0xd4, 0x74, 0xd0, 0x60, 0xf3, 0xff, 0x00, 0x00, 0xa9, 0x00, 0x00, 0x00, 0x08, 0xa7, 0xe3,
        0x97, 0xff, 0xaf, 0xbb, 0x88, 0xbf, 0x17, 0x10, 0x99, 0x01, 0x1a, 0x99, 0x01, 0x73, 0x74,
        0x61, 0x74, 0x73, 0x64, 0x2e, 0x6f, 0x74, 0x68, 0x65, 0x72, 0x2e, 0x6d, 0x65, 0x74, 0x72,
        0x69, 0x63, 0x3a, 0x33, 0x7c, 0x63, 0x7c, 0x40, 0x31, 0x2e, 0x30, 0x30, 0x30, 0x30, 0x30,
        0x30, 0x7c, 0x23, 0x65, 0x6e, 0x76, 0x69, 0x72, 0x6f, 0x6e, 0x6d, 0x65, 0x6e, 0x74, 0x3a,
        0x64, 0x65, 0x76, 0x0a, 0x73, 0x74, 0x61, 0x74, 0x73, 0x64, 0x2e, 0x6f, 0x74, 0x68, 0x65,
        0x72, 0x2e, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x3a, 0x38, 0x7c, 0x63, 0x7c, 0x40, 0x31,
        0x2e, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x7c, 0x23, 0x65, 0x6e, 0x76, 0x69, 0x72, 0x6f,
        0x6e, 0x6d, 0x65, 0x6e, 0x74, 0x3a, 0x64, 0x65, 0x76, 0x0a, 0x73, 0x74, 0x61, 0x74, 0x73,
        0x64, 0x2e, 0x6f, 0x74, 0x68, 0x65, 0x72, 0x2e, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x3a,
        0x37, 0x7c, 0x63, 0x7c, 0x40, 0x31, 0x2e, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x7c, 0x23,
        0x65, 0x6e, 0x76, 0x69, 0x72, 0x6f, 0x6e, 0x6d, 0x65, 0x6e, 0x74, 0x3a, 0x64, 0x65, 0x76,
        0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    #[test]
    fn two_msg_two_lines() {
        let mut replay = DogStatsDReplayReader::new(Bytes::from(TWO_MSGS_ONE_LINE_EACH));
        let mut s = String::new();
        let res = replay.read_msg(&mut s).unwrap();
        assert_eq!(res, 1);
        assert_eq!("statsd.example.time.micros:2.39283|d|@1.000000|#environment:dev|c:2a25f7fc8fbf573d62053d7263dd2d440c07b6ab4d2b107e50b0d4df1f2ee15f", s);
        s.clear();
        let res = replay.read_msg(&mut s).unwrap();
        assert_eq!(res, 1);
        assert_eq!("statsd.example.time.micros:2.39283|d|@1.000000|#environment:dev|c:2a25f7fc8fbf573d62053d7263dd2d440c07b6ab4d2b107e50b0d4df1f2ee15f", s);
        let res = replay.read_msg(&mut s).unwrap();
        assert_eq!(res, 0);
    }

    #[test]
    fn one_msg_two_lines() {
        let mut replay = DogStatsDReplayReader::new(Bytes::from(ONE_MSG_TWO_LINES));
        let mut s = String::new();
        let res = replay.read_msg(&mut s).unwrap();
        assert_eq!(res, 1);
        assert_eq!("statsd.example.time.micros:2.39283|d|@1.000000|#environment:dev,now:2023-08-23T21:24:59+00:00|c:2a25f7fc8fbf573d62053d7263dd2d440c07b6ab4d2b107e50b0d4df1f2ee15f", s);
        s.clear();
        let res = replay.read_msg(&mut s).unwrap();
        assert_eq!(res, 1);
        assert_eq!("statsd.other.metric:8.7|g|@1.000000|#environment:dev", s);
        let res = replay.read_msg(&mut s).unwrap();
        assert_eq!(res, 0);
    }

    #[test]
    fn one_msg_three_lines() {
        let mut replay = DogStatsDReplayReader::new(Bytes::from(ONE_MSG_THREE_LINES));
        let mut s = String::new();

        let res = replay.read_msg(&mut s).unwrap();
        assert_eq!(res, 1);
        assert_eq!("statsd.other.metric:3|c|@1.000000|#environment:dev", s);
        s.clear();

        let res = replay.read_msg(&mut s).unwrap();
        assert_eq!(res, 1);
        assert_eq!("statsd.other.metric:8|c|@1.000000|#environment:dev", s);
        s.clear();

        let res = replay.read_msg(&mut s).unwrap();
        assert_eq!(res, 1);
        assert_eq!("statsd.other.metric:7|c|@1.000000|#environment:dev", s);
        s.clear();

        let res = replay.read_msg(&mut s).unwrap();
        assert_eq!(res, 0);
    }
}