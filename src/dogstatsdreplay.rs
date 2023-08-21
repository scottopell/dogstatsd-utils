use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{Cursor, Error, Read, Write};
use std::sync::Arc;
use std::{
    fs::File,
    io::{self, BufRead},
};
extern crate zstd;

use prost::Message;

use crate::{
    dogstatsdreader::DogStatsDReader, dogstatsdreplay::dogstatsd::unix::UnixDogstatsdMsg,
    zstd::is_zstd,
};

pub mod dogstatsd {
    pub mod unix {
        include!(concat!(env!("OUT_DIR"), "/dogstatsd.unix.rs"));
    }
}

pub struct DogStatsDReplay {
    cursor: Cursor<Vec<u8>>,
    len: usize,
}

impl DogStatsDReader for DogStatsDReplay {
    fn read_msg(&mut self, _s: &mut String) -> std::io::Result<usize> {
        // TODO -- last step before it works!
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Not Implemented yettt",
        ))
    }
}

pub fn check_replay_header(header: &[u8]) -> std::io::Result<()> {
    if header.len() <= 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Not enough bytes to determine if its a replay file",
        ));
    }
    let datadog_header = [0xD4, 0x74, 0xD0, 0x60, 0xF0, 0xFF, 0x00, 0x00];

    // f0 is bitwise or'd with the file version, so to get the file version, lets do a bitwise xor
    let version = header[4] ^ 0xF0;

    if version != 3 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Unexpected version, wanted 3 but found {}", version),
        ));
    }

    if header[0..4] != datadog_header[0..4] {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Did not find replay header. Found: {:X?}", header),
        ));
    }

    return Ok(());
}

impl DogStatsDReplay {
    pub fn new(mut bytes: Vec<u8>) -> Self {
        // bytes contains the replay data minus the 8 byte header.
        // 8 byte header should have already been checked before constructing this
        bytes.drain(0..8);
        let len = bytes.len();
        let cursor = Cursor::new(bytes);

        DogStatsDReplay { cursor, len }
    }

    pub fn read_msgs(&mut self) -> Result<Vec<String>, io::Error> {
        let mut msgs = Vec::new();

        while self.cursor.position() < self.len as u64 - 4 {
            // Read the little endian uint32 that gives the length of the next protobuf message
            let message_length = match self.cursor.read_u32::<LittleEndian>() {
                Ok(i) => i,
                Err(error) => match error.kind() {
                    std::io::ErrorKind::UnexpectedEof => break,
                    _ => panic!("Unexpected error reading msg length: {}", error),
                },
            };

            // Read the protobuf message
            let mut message_buffer = vec![0; message_length as usize];
            match self.cursor.read_exact(&mut message_buffer) {
                Ok(()) => {}
                Err(error) => match error.kind() {
                    std::io::ErrorKind::UnexpectedEof => break,
                    _ => panic!(
                        "Unexpected error reading msg of length {} from offset {}: {}",
                        message_length,
                        self.cursor.position(),
                        error
                    ),
                },
            }

            // Decode the protobuf message using the provided .proto file
            let message = UnixDogstatsdMsg::decode(bytes::Bytes::from(message_buffer))?;
            let str_payload = match std::str::from_utf8(&message.payload) {
                Ok(v) => v,
                Err(e) => panic!("Invalid utf-8 sequence: {}", e),
            };
            msgs.push(str_payload.to_owned());
        }

        Ok(msgs)
    }

    pub fn write_to(&mut self, out_path: &str) -> Result<(), io::Error> {
        let mut output_file = File::create(out_path.to_owned())?;

        let msgs = self.read_msgs()?;

        for msg in msgs {
            output_file.write_all(msg.as_bytes())?;
        }

        Ok(())
    }
}

impl TryFrom<&mut File> for DogStatsDReplay {
    type Error = io::Error;

    fn try_from(f: &mut File) -> Result<Self, Self::Error> {
        let mut buffer = Vec::new();
        f.read_to_end(&mut buffer)?;

        // Decompress if we find zstd data
        if is_zstd(&buffer[0..4]) {
            buffer = zstd::decode_all(Cursor::new(buffer))?;
        }
        // Are the bytes likely to be a dogstatsd replay file?
        match check_replay_header(&buffer[0..8]) {
            Ok(_) => Ok(DogStatsDReplay::new(buffer)),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    return Err(e);
                } else {
                    panic!("Unexpected error: {}", e);
                }
            }
        }
    }
}
