use std::io::BufRead;

use bytes::{buf::Reader, Buf, Bytes};
use thiserror::Error;

use crate::{
    dogstatsdreplay::{check_replay_header, DogStatsDReplay},
    zstd::is_zstd,
};

pub trait StringDogStatsDReader {
    fn read_msg(s: &mut String) -> std::io::Result<usize>;
}

#[derive(Error, Debug)]
pub enum DogStatsDReaderError {
    #[error("No dogstatsd replay found")]
    NotAReplayFile,
}

pub struct DogStatsDReader {
    // todo this should probably be an enum?
    replay_reader: Option<DogStatsDReplay>,
    simple_reader: Option<SimpleDogStatsDReader>,
}

impl DogStatsDReader {
    pub fn new(mut buf: Bytes) -> DogStatsDReader {
        let zstd_header = buf.slice(0..4);
        if is_zstd(&zstd_header) {
            buf = Bytes::from(zstd::decode_all(buf.reader()).unwrap());
        }

        if let Ok(()) = check_replay_header(&buf.slice(0..8)) {
            DogStatsDReader {
                replay_reader: Some(DogStatsDReplay::new(buf)),
                simple_reader: None,
            }
        } else {
            DogStatsDReader {
                replay_reader: None,
                simple_reader: Some(SimpleDogStatsDReader::new(buf)),
            }
        }
    }

    /// read_msg populates the given String with a dogstastd message
    pub fn read_msg(&mut self, s: &mut String) -> std::io::Result<usize> {
        if let Some(ref mut replay) = self.replay_reader {
            replay.read_msg(s)
        } else if let Some(ref mut simpl) = self.simple_reader {
            simpl.read_msg(s)
        } else {
            panic!("IMPOSSIBLE!");
        }
    }
}

pub struct SimpleDogStatsDReader {
    reader: Reader<Bytes>,
}

impl SimpleDogStatsDReader {
    pub fn new(buf: Bytes) -> Self {
        let reader = buf.reader();
        SimpleDogStatsDReader { reader }
    }

    pub fn read_msg(&mut self, s: &mut String) -> std::io::Result<usize> {
        self.reader.read_line(s)
    }
}
