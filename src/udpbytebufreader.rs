use bytes::{BytesMut, Buf};
use anyhow::Result;
use tracing::info;
use std::{io::{BufReader, Read, BufRead}, net::UdpSocket, fs::copy};

pub struct UdpByteBufReader {
    buf: BytesMut,
    socket: UdpSocket,
}

/// The goal of this struct is to provide a conceptual "stream" of udp bytes
/// Even though UDP is datagram based, the dogstatsd message format is line based
/// and doesn't particularly care about "packets" or the underlying transport
/// It does not work yet, but the goal is to have it implement BufRead
/// for drop-in to the existing `DogStatsDReader` struct leveraging the UTF8Reader
impl UdpByteBufReader {
    pub fn new(interface: &str, port: &str) -> Result<Self> {
        let addr = format!("{}:{}", interface, port);
        info!("Binding to addr '{}'", addr);
        let socket = UdpSocket::bind(addr)?;
        info!("Bound!");
        Ok(Self {
            buf: BytesMut::with_capacity(65536),
            socket,
        })
    }
}

impl Read for UdpByteBufReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.buf.is_empty() {
            self.fill_buf()?;
        }

        for i in 0..buf.len() {
            if let Some(b) = self.buf.get(i) {
                buf[i] = *b;
            } else {
                break
            }
        }

        // todo this hsould be i?
        Ok(buf.len())
    }
}

impl BufRead for UdpByteBufReader {
    /// fill up the buffer with a new packet
    /// only if the buffer is empty
    fn fill_buf(&mut self) -> Result<&[u8], std::io::Error> {
        if self.buf.is_empty() {
            let mut local_buf = vec![0; 65536];
            let local_buf_ref = &mut local_buf[..];
            let selfbuf_ref = self.buf.as_mut();
            // todo why are local_buf_ref and selfbuf_ref not interchangeable
            let refref: &mut [u8] = selfbuf_ref;
            let (num_read, _) = self.socket.recv_from(refref)?;
            if num_read == 0 {
                return Ok(&[]);
            }
        }
        Ok(&self.buf)
    }

    fn consume(&mut self, amt: usize) {
        self.buf.advance(amt);
    }
}