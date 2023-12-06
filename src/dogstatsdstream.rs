use bytes::Bytes;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio_stream::Stream;

use crate::dogstatsdmsg::DogStatsDStr;

pub struct Utf8DogStatsDReader {
    bytes: Bytes,
}

impl Utf8DogStatsDReader {
    pub fn new(bytes: Bytes) -> Self {
        Self { bytes }
    }
}

// Inspired by https://stackoverflow.com/a/59519429
impl Stream for Utf8DogStatsDReader {
    type Item = DogStatsDStr<'s> where Self: 's;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll(Option<Item<'s>>) {
        bytes.find_byte(b'\n').map(|pos| {
            let line = bytes.split_to(pos);
            bytes.advance();
            Poll::Ready(Some(DogStatsDStr::new(line)))
        })
    }
}
