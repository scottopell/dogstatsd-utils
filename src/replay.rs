use std::{io::{self, Read, BufReader, BufRead}, ops::Deref};
use byteorder::{ByteOrder, LittleEndian};

use bytes::{Buf, Bytes, BytesMut};
use prost::{Message, DecodeError};
use tracing::warn;

use crate::dogstatsdreplayreader::dogstatsd::unix::UnixDogstatsdMsg;

const DATADOG_HEADER: &[u8] = &[0xD4, 0x74, 0xD0, 0x60];
const MAX_MSG_SIZE: usize = 8192; // TODO what is the real max size?
use thiserror::Error;

pub mod dogstatsd {
    pub mod unix {
        include!(concat!(env!("OUT_DIR"), "/dogstatsd.unix.rs"));
    }
}

// TODO currently missing ability to read tagger state from replay file
// If this is desired, the length can be found as the last 4 bytes of the replay file
// Only present in version 2 or greater
pub struct ReplayReader<'a> {
    reader: Box<dyn std::io::BufRead + 'a>,
    read_all_unixdogstatsdmsg: bool,
    buf: BytesMut,
}

impl<'a> std::fmt::Debug for ReplayReader<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayReader")
            .field("read_all_unixdogstatsdmsg", &self.read_all_unixdogstatsdmsg)
            .finish()
    }
}

#[derive(Error, Debug)]
pub enum ReplayReaderError {
    #[error("No dogstatsd replay marker found")]
    NotAReplayFile,
    #[error("Unsupported replay version")]
    UnsupportedReplayVersion(u8),
    #[error("IO Error")]
    Io(#[from] io::Error),
    #[error("Protobuf Decode error")]
    ProtoDecode(#[from] DecodeError),
}

/// header must point to at least 8 bytes
/// first four should be the replay header magic bytes
/// next u8 should be the dogstatsd version
/// next 3 bytes are unused
///
/// 8 bytes are always consumed.
pub fn is_replay(mut header: Bytes) -> Result<(), ReplayReaderError> {
    assert!(header.len() >= 8);

    // todo is there a better way to grab first 4 into slice?
    // - slice + advance
    // - clone + take(4) + into_inner
    let first_four = header.slice(0..4);
    header.advance(4);
    if first_four != DATADOG_HEADER {
        header.advance(4); // consume next 4 bytes for a total of 8
        return Err(ReplayReaderError::NotAReplayFile);
    }
    // Next byte describes the replay version
    // f0 is bitwise or'd with the file version, so to get the file version, do a bitwise xor
    let version = header.get_u8() ^ 0xF0;

    if version != 3 {
        header.advance(3); // consume next 3 bytes per contract
        return Err(ReplayReaderError::UnsupportedReplayVersion(version));
    }
    header.advance(3); // consume next 3 bytes per contract
    Ok(())
}


impl<'a> ReplayReader<'a> {
    pub fn supported_versions() -> &'static [u8] {
        &[3]
    }
    /// read_msg will return the next UnixDogstatsdMsg if it exists
    /// TODO, may be useful to explicitly return an EOF error
    /// rather than None
    pub fn read_msg(&mut self) -> Result<Option<UnixDogstatsdMsg>, ReplayReaderError> {
        if self.read_all_unixdogstatsdmsg {
            return Ok(None);
        }

        // Read the little endian uint32 that gives the length of the next protobuf message

        let mut msg_length_buf = [0; 4];
        self.reader.read_exact(&mut msg_length_buf)?;

        let message_length = LittleEndian::read_u32(&msg_length_buf) as usize;

        if message_length == 0 {
            // This indicates a record separator between UnixDogStatsdMsg list
            // and the tagger state. Next bytes are all for tagger state.
            self.read_all_unixdogstatsdmsg = true;
            return Ok(None);
        }

        // Read the protobuf message
        // todo avoid this allocation by using the BytesMut stored in self
        let mut msg_buf = vec![0; message_length];
        self.reader.read_exact(&mut msg_buf)?;

        let msg_buf = Bytes::from(msg_buf);

        // Decode the protobuf message using the provided .proto file
        match UnixDogstatsdMsg::decode(msg_buf) {
            Ok(msg) => Ok(Some(msg)),
            Err(e) => {
                warn!(
                    "Unexpected error decoding msg buf: {} do you have a valid dsd capture file?",
                    e
                );
                Err(e.into())
            }
        }
    }

    // consumes 8 bytes during construction, even if construction fails
    pub fn new(mut byte_reader: impl BufRead + 'a) -> Result<Self, ReplayReaderError> {
        let mut byte_reader: Box<dyn std::io::BufRead + 'a> = Box::new(byte_reader);
        let mut header_buf = [0; 8];
        byte_reader.read_exact(&mut header_buf)?;
        is_replay(Bytes::copy_from_slice(&header_buf))?;

        Ok(Self {
            reader: byte_reader,
            read_all_unixdogstatsdmsg: false,
            buf: BytesMut::with_capacity(MAX_MSG_SIZE).into(),
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

    use std::mem::discriminant;

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
            if (0x20..=0x7E).contains(&code) {
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
        let mut replay = ReplayReader::new(&TWO_MSGS_ONE_LINE_EACH[..]).unwrap();
        let msg = replay.read_msg().unwrap().unwrap();
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

        let msg = replay.read_msg().unwrap().unwrap();
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

        assert_eq!(None, replay.read_msg().unwrap())
    }

    #[test]
    fn invalid_replay_bytes() {
        let replay = ReplayReader::new(&b"my.metric:1|g\n"[..]);
        assert_eq!(discriminant(&replay.unwrap_err()), discriminant(&ReplayReaderError::NotAReplayFile));

        let replay = ReplayReader::new(&b"abcdefghijklmnopqrstuvwxyz"[..]);
        assert_eq!(discriminant(&replay.unwrap_err()), discriminant(&ReplayReaderError::NotAReplayFile));

        let replay = ReplayReader::new(&b"\n\n\n\n\n\n\n\n\n\n\n\t\t\t\n\t\n"[..]);
        assert_eq!(discriminant(&replay.unwrap_err()), discriminant(&ReplayReaderError::NotAReplayFile));
    }
}
