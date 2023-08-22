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

// TODO - what would a more efficient implementation of this look like?
// I first tried storing the single String and an idx, but this proved
// to be a bit nastier than I expected.
// next_line could just return a str, doesn't have to be a String
// but then everything gets lifetime annotations
struct CurrentMessage {
    lines: VecDeque<String>,
}

impl CurrentMessage {
    fn next_line(&mut self) -> Option<String> {
        self.lines.pop_front()
    }
}

pub struct DogStatsDReplay {
    buf: Bytes,
    current_message: Option<CurrentMessage>,
}

impl DogStatsDReplay {
    // TODO this currently returns an entire dogstatsd replay payload, which is not a single dogstatsd message.
    pub fn read_msg(&mut self, s: &mut String) -> std::io::Result<usize> {
        if let Some(ref mut current) = self.current_message {
            if let Some(line) = current.next_line() {
                s.insert_str(0, &line);
                return Ok(1);
            } else {
                self.current_message = None;
            }
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

                let lines: Vec<String> = v.lines().map(|l| l.to_owned()).collect();
                let mut msg = CurrentMessage {
                    lines: VecDeque::from(lines),
                };
                let line = msg.next_line().expect("Found no next line, why not?? ");

                s.insert_str(0, &line);
                self.current_message = Some(msg);

                Ok(1)
            }
            Err(e) => panic!("Invalid utf-8 sequence: {}", e),
        }
    }

    pub fn new(mut buf: Bytes) -> Self {
        buf.advance(8); // eat the header
        DogStatsDReplay {
            buf,
            current_message: None,
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
