use bytes::{Buf, Bytes};
use thiserror::Error;
use tracing::{error, info, debug};

use crate::{
    dogstatsdreplayreader::{DogStatsDReplayReader, DogStatsDReplayReaderError},
    replay::{ReplayReader, ReplayReaderError},
    utf8dogstatsdreader::Utf8DogStatsDReader,
    zstd::is_zstd, pcapdogstatsdreader::{PcapDogStatsDReader, PcapDogStatsDReaderError}, pcapreader::PcapReader,
};

#[derive(Error, Debug)]
pub enum DogStatsDReaderError {
    #[error("DSD Replay")]
    Replay(#[from] DogStatsDReplayReaderError),
    #[error("PCAP")]
    Pcap(#[from] PcapDogStatsDReaderError),
    #[error("IO Error")]
    Io(#[from] std::io::Error),
}

pub enum DogStatsDReader {
    Replay(DogStatsDReplayReader),
    Utf8(Utf8DogStatsDReader),
    Pcap(PcapDogStatsDReader),
}

enum InputType {
    Replay,
    Pcap,
    Utf8,
}

/// Does not consume from header
fn input_type_of(header: Bytes) -> InputType {
    assert!(header.len() >= 8);

    // todo improve printout to print 8 hex tuples
    debug!("8 byte header: {:#x?}", header.slice(0..8));

    // is_replay will consume the first 8 bytes, so pass a clone
    match ReplayReader::is_replay(header.clone()) {
        Ok(()) => return InputType::Replay,
        Err(e) => {
            match e {
                ReplayReaderError::NotAReplayFile => debug!("Not a replay file."),
                ReplayReaderError::UnsupportedReplayVersion(v) => debug!("Replay header detected, but unsupported version found: {v:x}."),
            }
        }
    }

    match PcapReader::is_pcap(header.clone()) {
        Ok(()) => return InputType::Pcap,
        Err(r) => {
            debug!("Not a pcap file: {r:?}");
        }
    }

    // fallback to text, its probably utf8

    InputType::Utf8
}

impl DogStatsDReader {
    /// 'buf' should point either to the beginning of a utf-8 encoded stream of
    /// DogStatsD messages, or to the beginning of a DogStatsD Replay/Capture file
    /// Either sequence can be optionally zstd encoded, it will be automatically
    /// decoded if needed.
    pub fn new(mut buf: Bytes) -> Self {
        let mut header_bytes = buf.slice(0..8);
        // pass a cheap clone to check if zstd
        if is_zstd(&header_bytes.slice(0..4)) {
            info!("Detected zstd compression.");
            // consume original buffer to completion
            buf = Bytes::from(zstd::decode_all(buf.reader()).unwrap());
            // buf now points to a new buffer, so grab a new 8 byte slice of
            // possibly-header from the new decompressed buffer
            header_bytes = buf.slice(0..8);
        }
        match input_type_of(header_bytes) {
            InputType::Pcap => {
                info!("Treating input as pcap");
                match PcapDogStatsDReader::new(buf) {
                    Ok(reader) => Self::Pcap(reader),
                    Err(e) => {
                        panic!("Pcap Reader couldn't be created: {e:?}");
                    }
                }
            }
            InputType::Replay => {
                info!("Treating input as dogstatsd-replay");
                match DogStatsDReplayReader::new(buf) {
                    Ok(reader) => Self::Replay(reader),
                    Err(e) => {
                        panic!("Replay reader couldn't be created: {e:?}");
                    },
                }
            }
            InputType::Utf8 => {
                info!("Treating input as utf8");
                Self::Utf8(Utf8DogStatsDReader::new(buf))
            }
        }
    }

    /// read_msg populates the given String with a dogstatsd message
    /// and returns the number of messages read (currently always 1)
    pub fn read_msg(&mut self, s: &mut String) -> Result<usize, DogStatsDReaderError> {
        match self {
            Self::Utf8(r) => Ok(r.read_msg(s)?),
            Self::Replay(r) => Ok(r.read_msg(s)?),
            Self::Pcap(r) => Ok(r.read_msg(s)?),
        }
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

    const PCAP_SINGLE_MESSAGE: &[u8] = &[
        0xd4, 0xc3, 0xb2, 0xa1, 0x02, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x14, 0x01, 0x00, 0x00,
        0xef, 0xc0, 0x9d, 0x65, 0xb2, 0xbc, 0x0a, 0x00, 0x4f, 0x00, 0x00, 0x00,
        0x4f, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        0x03, 0x04, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x45, 0x00, 0x00, 0x3b, 0x30, 0xf0, 0x40, 0x00, 0x40, 0x11, 0x0b, 0xc0,
        0x7f, 0x00, 0x00, 0x01, 0x7f, 0x00, 0x00, 0x01, 0x8d, 0x81, 0x1f, 0xbd,
        0x00, 0x27, 0xfe, 0x3a, 0x61, 0x62, 0x63, 0x2e, 0x6d, 0x79, 0x2e, 0x66,
        0x61, 0x76, 0x2e, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x3a, 0x31, 0x7c,
        0x63, 0x7c, 0x23, 0x68, 0x6f, 0x73, 0x74, 0x3a, 0x66, 0x6f, 0x6f
    ];

    #[test]
    fn utf8_single_msg() {
        // Given 1 msg
        let payload = b"my.metric:1|g";
        let mut reader = DogStatsDReader::new(Bytes::from_static(payload));
        let mut s = String::new();

        // When reader is read
        let num_read = reader.read_msg(&mut s).unwrap();
        // Expect one msg
        assert_eq!(s.as_str(), "my.metric:1|g");
        assert_eq!(num_read, 1);
        s.clear();

        // then no more
        assert_eq!(reader.read_msg(&mut s).unwrap(), 0);
    }

    #[test]
    fn utf8_single_msg_trailing_newline() {
        // Given one msg with newline
        let payload = b"my.metric:1|g\n";
        let mut reader = DogStatsDReader::new(Bytes::from_static(payload));
        let mut s = String::new();

        // When read
        let num_read = reader.read_msg(&mut s).unwrap();
        // Expect one msg
        assert_eq!(s.as_str(), "my.metric:1|g");
        assert_eq!(num_read, 1);
        s.clear();

        // then no more
        assert_eq!(reader.read_msg(&mut s).unwrap(), 0);
    }

    #[test]
    fn utf8_multi_msg() {
        // Given 2 msgs
        let payload = b"my.metric:1|g\nmy.metric:2|g";
        let mut reader = DogStatsDReader::new(Bytes::from_static(payload));
        let mut s = String::new();

        // When read, expect msg 1
        let num_read = reader.read_msg(&mut s).unwrap();
        assert_eq!(s.as_str(), "my.metric:1|g");
        assert_eq!(num_read, 1);
        s.clear();

        // and msg 2
        reader.read_msg(&mut s).unwrap();
        assert_eq!(s.as_str(), "my.metric:2|g");
        s.clear();

        // then no more
        assert_eq!(reader.read_msg(&mut s).unwrap(), 0);
    }

    #[test]
    fn utf8_multi_msg_msg_trailing_newline() {
        // Given 2 msgs with a trailing newline
        let payload = b"my.metric:1|g\nmy.metric:2|g\n";
        let mut reader = DogStatsDReader::new(Bytes::from_static(payload));
        let mut s = String::new();

        // When read, expect msg 1
        let num_read = reader.read_msg(&mut s).unwrap();
        assert_eq!(s.as_str(), "my.metric:1|g");
        assert_eq!(num_read, 1);
        s.clear();

        // and msg 2
        reader.read_msg(&mut s).unwrap();
        assert_eq!(s.as_str(), "my.metric:2|g");
        s.clear();

        // then no more
        assert_eq!(reader.read_msg(&mut s).unwrap(), 0);
    }

    #[test]
    fn utf8_example() {
        // Given 2 msgs with a trailing newline
        let payload = b"my.metric:1|g\nmy.metric:2|g\nother.metric:20|d|#env:staging\nother.thing:10|d|#datacenter:prod\n";
        let mut reader = DogStatsDReader::new(Bytes::from_static(payload));
        let mut s = String::new();

        let mut iters = 0;
        loop {
            match reader.read_msg(&mut s) {
                Ok(num_read) => {
                    if num_read == 0 {
                        break;
                    }
                    iters += 1;
                    println!("{}", s);
                    assert!(s.len() < 40); // sanity check, longest msg is 40
                    s.clear();
                }
                Err(e) => {
                    panic!("unexpected err {}", e)
                }
            }
        }
        assert_eq!(iters, 4);
    }

    #[test]
    fn zstd_utf8_reader_single_msg() {
        // Given 1 msg without newline that is zstd compressed
        let payload = &[
            0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x58, 0x69, 0x00, 0x00, 0x6d, 0x79, 0x2e, 0x6d, 0x65,
            0x74, 0x72, 0x69, 0x63, 0x3a, 0x31, 0x7c, 0x67, 0x1e, 0xc8, 0x48, 0xb4,
        ];
        let mut reader = DogStatsDReader::new(Bytes::from_static(payload));
        let mut s = String::new();

        // When reader is read
        let num_read = reader.read_msg(&mut s).unwrap();
        // Expect one msg
        assert_eq!(s.as_str(), "my.metric:1|g");
        assert_eq!(num_read, 1);

        // then no more
        assert_eq!(reader.read_msg(&mut s).unwrap(), 0);
    }

    #[test]
    fn zstd_utf8_single_msg_trailing_newline() {
        // Given 1 msg with newline that is zstd compressed
        let payload = &[
            0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x58, 0x71, 0x00, 0x00, 0x6d, 0x79, 0x2e, 0x6d, 0x65,
            0x74, 0x72, 0x69, 0x63, 0x3a, 0x31, 0x7c, 0x67, 0x0a, 0x00, 0x72, 0x2c, 0x42,
        ];
        let mut reader = DogStatsDReader::new(Bytes::from_static(payload));
        let mut s = String::new();

        // When reader is read
        let num_read = reader.read_msg(&mut s).unwrap();
        // Expect one msg
        assert_eq!(s.as_str(), "my.metric:1|g");
        assert_eq!(num_read, 1);

        // then no more
        assert_eq!(reader.read_msg(&mut s).unwrap(), 0);
    }

    #[test]
    fn zstd_utf8_four_msg_trailing_newline() {
        // Given 4 msgs with newline that is zstd compressed
        let payload = &[
            0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x58, 0x6d, 0x02, 0x00, 0xe4, 0x03, 0x6d, 0x79, 0x2e,
            0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x3a, 0x31, 0x7c, 0x67, 0x0a, 0x32, 0x7c, 0x67,
            0x0a, 0x6f, 0x74, 0x68, 0x65, 0x72, 0x30, 0x7c, 0x64, 0x7c, 0x23, 0x65, 0x6e, 0x76,
            0x3a, 0x73, 0x74, 0x61, 0x67, 0x69, 0x6e, 0x74, 0x68, 0x69, 0x6e, 0x67, 0x3a, 0x31,
            0x64, 0x61, 0x74, 0x61, 0x63, 0x65, 0x6e, 0x74, 0x65, 0x72, 0x3a, 0x70, 0x72, 0x6f,
            0x64, 0x0a, 0x0a, 0x04, 0x00, 0x41, 0x09, 0x43, 0x28, 0x52, 0x69, 0x16, 0x39, 0xb6,
            0xa9, 0x04, 0xb6, 0x9f, 0x86, 0x7f,
        ];
        let mut reader = DogStatsDReader::new(Bytes::from_static(payload));
        let mut s = String::new();

        // When reader is read
        let num_read = reader.read_msg(&mut s).unwrap();

        // Expect one msg
        assert_eq!(s.as_str(), "my.metric:1|g");
        assert_eq!(num_read, 1);
        s.clear();

        let num_read = reader.read_msg(&mut s).unwrap();
        assert_eq!(s.as_str(), "my.metric:2|g");
        assert_eq!(num_read, 1);
        s.clear();

        let num_read = reader.read_msg(&mut s).unwrap();
        assert_eq!(s.as_str(), "other.metric:20|d|#env:staging");
        assert_eq!(num_read, 1);
        s.clear();

        let num_read = reader.read_msg(&mut s).unwrap();
        assert_eq!(s.as_str(), "other.thing:10|d|#datacenter:prod");
        assert_eq!(num_read, 1);
        s.clear();

        // then no more
        assert_eq!(reader.read_msg(&mut s).unwrap(), 0);
    }

    #[test]
    fn dsdreplay_two_msg_two_lines() {
        let mut replay = DogStatsDReader::new(Bytes::from(TWO_MSGS_ONE_LINE_EACH));
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
    fn pcap_single_message() {
        let mut reader = DogStatsDReader::new(Bytes::from(PCAP_SINGLE_MESSAGE));
        let mut s = String::new();
        let res = reader.read_msg(&mut s).unwrap();
        assert_eq!(res, 1);
        assert_eq!("abc.my.fav.metric:1|c|#host:foo", s);
        s.clear();
        let res = reader.read_msg(&mut s).unwrap();
        assert_eq!(res, 0);
    }
}
