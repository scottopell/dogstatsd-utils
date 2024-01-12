
use etherparse::SlicedPacket;
use thiserror::Error;
use bytes::{Buf, Bytes};
use pcap_file::PcapError as PcapError;
use pcap_file::pcap::PcapPacket;
use bytes::buf::Reader;
use tracing::{info, error};

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

fn _payload_from_pcap(packet: SlicedPacket) -> Bytes {
    if let Some(ethertype) = packet.payload_ether_type() {
        match etherparse::SlicedPacket::from_ether_type(ethertype, packet.payload) {
            Ok(value) => {
                info!("Found nested packet with ethertype: {ethertype}. Recursing into it.");
                return _payload_from_pcap(value);
            }
            Err(e) => {
                error!("Failed to parse payload from ethertype ({ethertype}): {e:?}");
            }
        }
    } else {
        info!("Packet does not contain a nested packet, testing below for relevant fields");
    }
    if let Some(link) = packet.link {
        info!("Link: {:?}", link);
    }
    if let Some(vlan) = packet.vlan {
        info!("vlan: {:?}", vlan)
    }
    if let Some(ip) = packet.ip {
        info!("ip: {:?}", ip)
    }
    if let Some(transport) = packet.transport {
        // could be Some(Udp(_))
        info!("transport: {:?}", transport);

        return Bytes::copy_from_slice(packet.payload);
    }

    Bytes::copy_from_slice(packet.payload)
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

    /// This function takes a pcap packet and attempts to unwrap it into a UDP packet
    /// If this is possible, it will return the byte payload of the udp packet.
    /// otherwise this will return None.
    pub fn get_udp_payload_from_packet(packet: PcapPacket) -> Result<Option<Bytes>, PcapReaderError> {
        let data = packet.data;
        // packet.data contains a frame
        // we need to find the udp packet within that frame
        // from pcap files captured on the 'any' interface,
        // we get SLL frames, "Linux Cooked Mode v2"
        // etherparse doesn't appear to support this.
        // I found this crate which appears to, but it is fairly low-level
        // https://docs.rs/pnet/latest/pnet/packet/sll2/struct.SLL2Packet.html

        info!("Attempting to read UDP packet out of raw PCAP packet (len: {})", data.len());
        // todo is this an ethernet packet or SLL?
        // I suspect those will be the two main cases
        // Ideally I can find some lib that generically finds the right packet given the bytes




        Ok(Some(Bytes::copy_from_slice(&data)))
    }

    /// Returns a pcap packet from the pcap file if one is available.
    /// If no more packets can be read, then this will return Ok(None)
    ///
    /// # Errors
    /// - This function will return an error if the pcap data is malformed
    pub fn read_packet(&mut self) -> Result<Option<PcapPacket>, PcapReaderError> {
        match self.reader.next_packet() {
            Some(Ok(packet)) => Ok(Some(packet)),
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

    // all of my current pcap files were created using this tcpdump invocation
    // sudo tcpdump -i any "udp port 8125" -w output.pcap
    // this 'any' interface triggers tcpdump to write the pcap with
    // a special "linux cooked mode" that I've never run across before
    // https://posts.oztamir.com/linux-cooked-packets-and-where-to-find-them/

    const PCAP_SLLV2_SINGLE_UDP_PACKET: &[u8] = &[
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
        PcapReader::is_pcap(Bytes::from_static(PCAP_SLLV2_SINGLE_UDP_PACKET)).unwrap();
    }

    #[test]
    fn can_read_single_packet() {
        let mut reader = PcapReader::new(Bytes::from_static(PCAP_SLLV2_SINGLE_UDP_PACKET)).unwrap();
        let packet = reader.read_packet().unwrap().unwrap();
        assert_eq!(packet.data.len(), 79);
    }

    #[test]
    fn can_read_udp_from_sll2_packet() {
        let mut reader = PcapReader::new(Bytes::from_static(PCAP_SLLV2_SINGLE_UDP_PACKET)).unwrap();
        let packet = reader.read_packet().unwrap().unwrap();
        let udp_payload = PcapReader::get_udp_payload_from_packet(packet).unwrap().unwrap();

        let expected_udp_payload: &[u8] = &[
            0x61, 0x62, 0x63, 0x2e, 0x6d, 0x79, 0x2e, 0x66,
            0x61, 0x76, 0x2e, 0x6d, 0x65, 0x74, 0x72, 0x69,
            0x63, 0x3a, 0x31, 0x7c, 0x63, 0x7c, 0x23, 0x68,
            0x6f, 0x73, 0x74, 0x3a, 0x66, 0x6f, 0x6f
        ];

        assert_eq!(udp_payload, expected_udp_payload);
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