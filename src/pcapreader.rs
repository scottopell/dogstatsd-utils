


use bytes::{Buf, Bytes};
use pcap_file::pcap::PcapPacket;
use pcap_file::{pcap::PcapHeader, PcapError};
use pnet::packet::Packet;
use thiserror::Error;
use tracing::{debug, error, info};

// The writing application writes 0xa1b2c3d4 with it's native byte
// ordering format into this field.
// The reading application will read either
// -  0xa1b2c3d4 (identical)
// -  0xd4c3b2a1 (swapped)
// If the reading application reads the swapped 0xd4c3b2a1 value,
// it knows that all the following fields will have to be swapped too.
// https://wiki.wireshark.org/Development/LibpcapFileFormat
const PCAP_HEADER: &[u8] = &[0xa1, 0xb2, 0xc3, 0xd4];
const PCAP_HEADER_SWAPPED: &[u8] = &[0xd4, 0xc3, 0xb2, 0xa1];

pub struct PcapReader<'a>
{
    reader: pcap_file::pcap::PcapReader<Box<dyn std::io::BufRead + 'a>>,
    pub header: pcap_file::pcap::PcapHeader,
}

#[derive(Error, Debug)]
pub enum PcapReaderError {
    #[error("Unrecognized Header")]
    BadHeader(String),
    #[error("PCAP Error: {0}")]
    Pcap(#[from] PcapError),
    #[error("Unsupported datalink type: {0:?}")]
    UnsupportedDatalinkType(pcap_file::DataLink),
}

// Advances header 4 bytes
pub fn is_pcap(mut header: Bytes) -> Result<(), PcapReaderError> {
    assert!(header.len() >= 4);

    let first_four = header.slice(0..4);
    header.advance(4);
    // pcap_file has a more comprehensive check, but requires at least 24 bytes
    if first_four != PCAP_HEADER && first_four != PCAP_HEADER_SWAPPED {
        return Err(PcapReaderError::BadHeader(format!(
            "first four: {first_four:#?}"
        )));
    }
    Ok(())
}

/// This function takes a pcap packet and attempts to unwrap it into a UDP packet
/// If this is possible, it will return the byte payload of the udp packet.
/// otherwise this will return None.
pub fn get_udp_payload_from_packet(
    packet: PcapPacket,
    header: PcapHeader,
) -> Result<Option<Bytes>, PcapReaderError> {
    let data = packet.data;
    // data will be interpreted according to the datalink type
    // specified in the pcap header

    debug!(
        "Attempting to read UDP packet out of raw PCAP packet (len: {})",
        data.len()
    );

    match header.datalink {
        pcap_file::DataLink::ETHERNET => {
            let ethernet_packet = pnet::packet::ethernet::EthernetPacket::new(&data)
                .expect("pcap header claimed ethernet packet, but parsing failed");
            debug!("Ethernet packet: {:?}", ethernet_packet);
            match ethernet_packet.get_ethertype() {
                pnet::packet::ethernet::EtherTypes::Ipv4 => {
                    let ipv4_packet =
                        pnet::packet::ipv4::Ipv4Packet::new(ethernet_packet.payload())
                            .expect("Header said ipv4, but parsing failed");
                    debug!("IPv4 packet: {:?}", ipv4_packet);
                    return get_udp_payload_from_ipv4(ipv4_packet);
                }
                _ => {
                    // todo - ipv6
                    error!(
                        "Unsupported protocol found in ethernet packet: {}",
                        ethernet_packet.get_ethertype()
                    );
                }
            }
            todo!()
        }
        pcap_file::DataLink::LINUX_SLL2 => {
            let sllv2_packet = pnet::packet::sll2::SLL2Packet::new(&data)
                .expect("Pcap header claimed sll2 packets, but parsing failed.");
            debug!(
                "SLLv2 packet: {:?} with protocol type: {}",
                sllv2_packet,
                sllv2_packet.get_protocol_type()
            );
            match sllv2_packet.get_protocol_type() {
                pnet::packet::ethernet::EtherTypes::Ipv4 => {
                    let ipv4_packet = pnet::packet::ipv4::Ipv4Packet::new(sllv2_packet.payload())
                        .expect("Header said ipv4, but parsing failed");
                    debug!("IPv4 packet: {:?}", ipv4_packet);
                    return get_udp_payload_from_ipv4(ipv4_packet);
                }
                _ => {
                    // todo - ipv6
                    error!(
                        "Unsupported protocol found in SLLv2 packet: {}",
                        sllv2_packet.get_protocol_type()
                    );
                }
            }
        }
        _ => {
            unreachable!("Unsupported datalink type found, this should have been caught during construction.");
        }
    }

    Ok(Some(Bytes::copy_from_slice(&data)))
}

fn get_udp_payload_from_ipv4(
    ipv4: pnet::packet::ipv4::Ipv4Packet,
) -> Result<Option<Bytes>, PcapReaderError> {
    match ipv4.get_next_level_protocol() {
        pnet::packet::ip::IpNextHeaderProtocols::Udp => {
            let udp_packet = pnet::packet::udp::UdpPacket::new(ipv4.payload());
            debug!("UDP packet: {:?}", udp_packet);
            match udp_packet {
                Some(udp_packet) => {
                    return Ok(Some(Bytes::copy_from_slice(udp_packet.payload())));
                }
                None => {
                    error!("Failed to parse UDP packet from IPv4 packet");
                }
            }
        }
        _ => {
            error!(
                "Unsupported protocol found in IPv4 packet: {:?}",
                ipv4.get_next_level_protocol()
            );
        }
    }
    Ok(None)
}


impl<'a> PcapReader<'a>
{
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

    pub fn new(byte_reader: impl std::io::BufRead + 'a) -> Result<Self, PcapReaderError> {
        let byte_reader: Box<dyn std::io::BufRead + 'a> = Box::new(byte_reader);
        let reader = pcap_file::pcap::PcapReader::new(byte_reader)?;
        let header = reader.header();
        match header.datalink {
            pcap_file::DataLink::ETHERNET => {
                info!("Datalink: Ethernet");
            }
            pcap_file::DataLink::LINUX_SLL2 => {
                info!("Datalink: Linux Cooked Mode v2");
            }
            _ => {
                error!(
                    "Unsupported datalink type in pcap file: {:?}",
                    header.datalink
                );
                return Err(PcapReaderError::UnsupportedDatalinkType(header.datalink));
            }
        }

        Ok(Self { reader, header })
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
        0xd4, 0xc3, 0xb2, 0xa1, 0x02, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x04, 0x00, 0x14, 0x01, 0x00, 0x00, 0xef, 0xc0, 0x9d, 0x65, 0xb2, 0xbc,
        0x0a, 0x00, 0x4f, 0x00, 0x00, 0x00, 0x4f, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x01, 0x03, 0x04, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x45, 0x00, 0x00, 0x3b, 0x30, 0xf0, 0x40, 0x00, 0x40, 0x11, 0x0b, 0xc0, 0x7f, 0x00, 0x00,
        0x01, 0x7f, 0x00, 0x00, 0x01, 0x8d, 0x81, 0x1f, 0xbd, 0x00, 0x27, 0xfe, 0x3a, 0x61, 0x62,
        0x63, 0x2e, 0x6d, 0x79, 0x2e, 0x66, 0x61, 0x76, 0x2e, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63,
        0x3a, 0x31, 0x7c, 0x63, 0x7c, 0x23, 0x68, 0x6f, 0x73, 0x74, 0x3a, 0x66, 0x6f, 0x6f,
    ];

    const PCAP_ETH1_SINGLE_UDP_PACKET: &[u8] = &[
        0xd4, 0xc3, 0xb2, 0xa1, 0x02, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x04, 0x00, 0x01, 0x00, 0x00, 0x00, 0x11, 0xbe, 0xa1, 0x65, 0x07, 0x14,
        0x0c, 0x00, 0x49, 0x00, 0x00, 0x00, 0x49, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x45, 0x00, 0x00, 0x3b, 0xf7, 0x5a,
        0x40, 0x00, 0x40, 0x11, 0x45, 0x55, 0x7f, 0x00, 0x00, 0x01, 0x7f, 0x00, 0x00, 0x01, 0x9c,
        0x60, 0x1f, 0xbd, 0x00, 0x27, 0xfe, 0x3a, 0x61, 0x62, 0x63, 0x2e, 0x6d, 0x79, 0x2e, 0x66,
        0x61, 0x76, 0x2e, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x3a, 0x31, 0x7c, 0x63, 0x7c, 0x23,
        0x68, 0x6f, 0x73, 0x74, 0x3a, 0x66, 0x6f, 0x6f,
    ];

    const DSD_RECAP_PARTIAL: &[u8] = &[
        0xd4, 0x74, 0xd0, 0x60, 0xf3, 0xff, 0x00, 0x00, 0x93, 0x00, 0x00, 0x00, 0x08,
    ];

    #[test]
    fn can_detect_pcap() {
        is_pcap(Bytes::from_static(PCAP_SLLV2_SINGLE_UDP_PACKET)).unwrap();
        is_pcap(Bytes::from_static(PCAP_ETH1_SINGLE_UDP_PACKET)).unwrap();
    }

    #[test]
    fn can_read_single_packet() {
        let mut reader = PcapReader::new(&PCAP_SLLV2_SINGLE_UDP_PACKET[..]).unwrap();
        let packet = reader.read_packet().unwrap().unwrap();
        assert_eq!(packet.data.len(), 79);
    }

    #[test]
    fn can_read_udp_from_sll2_packet() {
        let mut reader = PcapReader::new(&PCAP_SLLV2_SINGLE_UDP_PACKET[..]).unwrap();
        let header = reader.header;
        let packet = reader.read_packet().unwrap().unwrap();
        let udp_payload = get_udp_payload_from_packet(packet, header)
            .unwrap()
            .unwrap();

        let expected_udp_payload: &[u8] = &[
            0x61, 0x62, 0x63, 0x2e, 0x6d, 0x79, 0x2e, 0x66, 0x61, 0x76, 0x2e, 0x6d, 0x65, 0x74,
            0x72, 0x69, 0x63, 0x3a, 0x31, 0x7c, 0x63, 0x7c, 0x23, 0x68, 0x6f, 0x73, 0x74, 0x3a,
            0x66, 0x6f, 0x6f,
        ];

        assert_eq!(udp_payload, expected_udp_payload);
    }

    #[test]
    fn can_read_udp_from_eth1_packet() {
        let mut reader = PcapReader::new(&PCAP_ETH1_SINGLE_UDP_PACKET[..]).unwrap();
        let header = reader.header;
        let packet = reader.read_packet().unwrap().unwrap();
        let udp_payload = get_udp_payload_from_packet(packet, header)
            .unwrap()
            .unwrap();

        let expected_udp_payload: &[u8] = &[
            0x61, 0x62, 0x63, 0x2e, 0x6d, 0x79, 0x2e, 0x66, 0x61, 0x76, 0x2e, 0x6d, 0x65, 0x74,
            0x72, 0x69, 0x63, 0x3a, 0x31, 0x7c, 0x63, 0x7c, 0x23, 0x68, 0x6f, 0x73, 0x74, 0x3a,
            0x66, 0x6f, 0x6f,
        ];

        assert_eq!(udp_payload, expected_udp_payload);
    }

    #[test]
    fn can_reject_utf8() {
        let err = is_pcap(Bytes::from_static(b"abcdefg")).unwrap_err();
        match err {
            PcapReaderError::BadHeader(_) => {}
            _ => panic!("Unexpected error reason"),
        }
    }

    #[test]
    fn can_reject_dsdreplay() {
        let err = is_pcap(Bytes::from_static(DSD_RECAP_PARTIAL)).unwrap_err();
        match err {
            PcapReaderError::BadHeader(_) => {}
            _ => panic!("Unexpected error reason"),
        }
    }
}
