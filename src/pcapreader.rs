
use thiserror::Error;
use bytes::{Buf, Bytes};
use pcap_file::PcapError as PcapError;
use pcap_file::pcap::PcapPacket;
use bytes::buf::Reader;

// The writing application writes 0xa1b2c3d4 with it's native byte
// ordering format into this field.
// The reading application will read either
// -  0xa1b2c3d4 (identical)
// -  0xd4c3b2a1 (swapped)
// If the reading application reads the swapped 0xd4c3b2a1 value,
// it knows that all the following fields will have to be swapped too.
// https://wiki.wireshark.org/Development/LibpcapFileFormat
const PCAP_HEADER: &[u8] = &[0xa1, 0xb2, 0xc3, 0xd4];
const PCAP_HEADER_SWAPPED: &[u8] = &[0xd4, 0xc3, 0xb2, 0xa1,];

#[derive(Debug)]
pub struct PcapReader {
    reader: pcap_file::pcap::PcapReader<Reader<Bytes>>,
}

#[derive(Error, Debug)]
pub enum PcapReaderError {
    #[error("Unrecognized Header")]
    BadHeader(String),
    #[error("PCAP Error: {0}")]
    Pcap(#[from] PcapError),
}


impl PcapReader {

    // Advances header 4 bytes
    pub fn is_pcap(mut header: Bytes) -> Result<(), PcapReaderError> {
        let first_four = header.slice(0..4);
        header.advance(4);
        // todo pcap_file has a more comprehensive check
        if first_four != PCAP_HEADER && first_four != PCAP_HEADER_SWAPPED {
            return Err(PcapReaderError::BadHeader(format!("first four: {first_four:#?}")));
        }
        Ok(())
    }

    pub fn read_packet(&mut self) -> Result<Option<PcapPacket>, PcapReaderError> {
        match self.reader.next_packet() {
            Some(Ok(p)) => Ok(Some(p)),
            Some(Err(e)) => Err(PcapReaderError::Pcap(e)),
            None => Ok(None),
        }
    }

    pub fn new(buf: Bytes) -> Result<Self, PcapReaderError> {
        let reader = pcap_file::pcap::PcapReader::new(buf.reader())?;

        Ok(Self {
            reader
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

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

    const DSD_RECAP_PARTIAL: &[u8] = & [
        0xd4, 0x74, 0xd0, 0x60, 0xf3, 0xff, 0x00, 0x00, 0x93, 0x00, 0x00, 0x00, 0x08,
    ];

    #[test]
    fn can_detect_pcap() {
        PcapReader::is_pcap(Bytes::from_static(PCAP_SINGLE_MESSAGE)).unwrap();
    }

    #[test]
    fn can_reject_utf8() {
        let err = PcapReader::is_pcap(Bytes::from_static(b"abcdefg")).unwrap_err();
        match err { PcapReaderError::BadHeader(_) => {}, _ => panic!("Unexpected error reason")}
    }

    #[test]
    fn can_reject_dsdreplay() {
        let err = PcapReader::is_pcap(Bytes::from_static(DSD_RECAP_PARTIAL)).unwrap_err();
        match err { PcapReaderError::BadHeader(_) => {}, _ => panic!("Unexpected error reason")}
    }
}