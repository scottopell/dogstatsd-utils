use bytes::{Buf, Bytes};

use std::io::Write;

use std::{
    fs::File,
    io::{self},
};
extern crate zstd;

use prost::Message;

use crate::dogstatsdreplay::dogstatsd::unix::UnixDogstatsdMsg;

pub mod dogstatsd {
    pub mod unix {
        include!(concat!(env!("OUT_DIR"), "/dogstatsd.unix.rs"));
    }
}

pub struct DogStatsDReplay {
    buf: Bytes,
    current_message: Option<UnixDogstatsdMsg>,
}

impl DogStatsDReplay {
    // TODO this currently returns an entire dogstatsd replay payload, which is not a single dogstatsd message.
    pub fn read_msg(&mut self, s: &mut String) -> std::io::Result<usize> {
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
        DogStatsDReplay {
            buf,
            current_message: None,
        }
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
