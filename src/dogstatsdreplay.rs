use byteorder::{LittleEndian, ReadBytesExt};
use bytes::{Buf, Bytes};
use std::fmt::write;
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
    buf: Bytes,
}

impl DogStatsDReader for DogStatsDReplay {
    fn read_msg(&mut self, s: &mut String) -> std::io::Result<usize> {
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
                s.insert_str(0, v);
                Ok(1)
            }
            Err(e) => panic!("Invalid utf-8 sequence: {}", e),
        }
    }
}

pub fn check_replay_header(header: &[u8]) -> std::io::Result<()> {
    if header.len() <= 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Not enough bytes to determine if its a replay file",
        ));
    }
    // todo constify this
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
    pub fn new(mut buf: Bytes) -> Self {
        buf.advance(8); // eat the header
        DogStatsDReplay { buf }
    }

    pub fn read_msgs(&mut self) -> Result<Vec<String>, io::Error> {
        let mut msgs = Vec::new();

        let mut s = String::new();
        while let Ok(num_read) = self.read_msg(&mut s) {
            if num_read <= 0 {
                break;
            }
            msgs.push(s.clone());
            s.clear();
        }

        Ok(msgs)
    }

    pub fn print_msgs(&mut self) {
        let mut s = String::new();
        loop {
            match self.read_msg(&mut s) {
                Ok(num_read) => {
                    if num_read <= 0 {
                        break;
                    }
                    println!("{}", s);
                    s.clear();
                }
                Err(e) => eprintln!("Error while reading a message!, {}", e),
            }
        }
    }

    pub fn write_to(&mut self, out_path: &str) -> Result<(), io::Error> {
        let mut output_file = File::create(out_path.to_owned())?;

        let mut s = String::new();
        while let Ok(num_read) = self.read_msg(&mut s) {
            if num_read <= 0 {
                break;
            }
            output_file.write_all(s.as_bytes())?;
        }

        Ok(())
    }
}

// thiserror - crate is useful for defining custom errors easily

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
            Ok(_) => Ok(DogStatsDReplay::new(Bytes::from(buffer))),
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
