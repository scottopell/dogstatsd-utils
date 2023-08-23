use std::collections::VecDeque;

use bytes::{Buf, Bytes};

extern crate zstd;

use prost::Message;

use crate::dogstatsdreplay::dogstatsd::unix::UnixDogstatsdMsg;

const DATADOG_HEADER: [u8; 8] = [0xD4, 0x74, 0xD0, 0x60, 0xF0, 0xFF, 0x00, 0x00];

pub mod dogstatsd {
    pub mod unix {
        include!(concat!(env!("OUT_DIR"), "/dogstatsd.unix.rs"));
    }
}

pub struct DogStatsDReplay {
    buf: Bytes,
    current_messages: VecDeque<String>,
}

impl DogStatsDReplay {
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

        DogStatsDReplay {
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
