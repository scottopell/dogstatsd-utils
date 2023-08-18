use std::{fs::File, io};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{Cursor, Error, Read, Write};
extern crate zstd;

use prost::Message;

use crate::dogstatsdreplay::dogstatsd::unix::UnixDogstatsdMsg;

pub mod dogstatsd {
    pub mod unix {
        include!(concat!(env!("OUT_DIR"), "/dogstatsd.unix.rs"));
    }
}

pub struct DogStatsDReplay {
    file: File,

}
impl DogStatsDReplay {

    pub fn write_to(&mut self, out_path: &str) -> Result<(), io::Error> {
        let mut output_file = File::create(out_path.to_owned())?;

        // TODO detect zstd -- I'm hardcoding it to true
        // 0xFD2FB528 are the magic bytes
        // https://github.com/facebook/zstd/blob/3298a08076081dbfa8eba5b08c2167b06020c5ff/doc/zstd_compression_format.md#zstandard-frames
        let is_zstd = true;

        let mut buffer = Vec::new();
        let f = &mut self.file;
        f.read_to_end(&mut buffer)?;

        // Decompress the buffer if it's Zstd
        let decompressed_buffer = if is_zstd {
            zstd::decode_all(Cursor::new(&buffer))?
        } else {
            buffer
        };

        // Read the 8-byte header
        let header = &decompressed_buffer[0..8];

        let datadog_header  = [0xD4, 0x74, 0xD0, 0x60, 0xF0, 0xFF, 0x00, 0x00];

        // f0 is bitwise or'd with the file version, so to get the file version, lets do a bitwise xor
        let version = header[4] ^ 0xF0;

        assert_eq!(version, 3);
        assert_eq!(header[0..4], datadog_header[0..4], "Encountered unexpected header");

        // Iterate through the protobuf messages
        let mut cursor = Cursor::new(&decompressed_buffer[8..]);
        let msg_len = decompressed_buffer.len() as u64 - 8;
        while cursor.position() < msg_len - 4 {
            // Read the little endian uint32 that gives the length of the next protobuf message
            let message_length = match cursor.read_u32::<LittleEndian>() {
                Ok(i) => i,
                Err(error) => match error.kind() {
                    std::io::ErrorKind::UnexpectedEof => break,
                    _ => panic!("Unexpected error reading msg length: {}", error)
                }
            };

            // Read the protobuf message
            let mut message_buffer = vec![0; message_length as usize];
            match cursor.read_exact(&mut message_buffer) {
                Ok(()) => {}
                Err(error) => match error.kind() {
                    std::io::ErrorKind::UnexpectedEof => break,
                    _ => panic!("Unexpected error reading msg of length {} from offset {}: {}", message_length, cursor.position(), error)
                }
            }

            // Decode the protobuf message using the provided .proto file
            let message = UnixDogstatsdMsg::decode(bytes::Bytes::from(message_buffer))?;
            let str_payload = match std::str::from_utf8(&message.payload) {
                Ok(v) => v,
                Err(e) => panic!("Invalid utf-8 sequence: {}", e),
            };
            output_file.write_all(str_payload.as_bytes())?;
        }

        Ok(())

    }

}

impl TryFrom<&str> for DogStatsDReplay {
    type Error = io::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match File::open(value.to_owned()) {
            Ok(file) => Ok(DogStatsDReplay{ file }),
            Err(e) => Err(e),
        }
    }
}