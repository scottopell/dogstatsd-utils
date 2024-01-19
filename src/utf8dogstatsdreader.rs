use bytes::{buf::Reader, Buf, Bytes};
use std::io::{BufRead, BufReader, Read};
pub struct Utf8DogStatsDReader<'a>
{
    reader: Box<dyn std::io::BufRead + 'a>,
}

impl<'a> Utf8DogStatsDReader<'a>
{
    pub fn new(reader: impl BufRead + 'a) -> Self {
        Utf8DogStatsDReader {
            reader: Box::new(reader),
        }
    }

    pub fn read_msg(&mut self, s: &mut String) -> std::io::Result<usize> {
        self.reader.read_line(s).map(|num_read| {
            if num_read == 0 {
                return num_read;
            }

            let new_len = s.trim_end().len();
            s.truncate(new_len);
            if new_len == 0 {
                return 0;
            }

            1
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf8_reader_single_msg() {
        // Given 1 msg
        let payload = b"my.metric:1|g";
        let mut reader = Utf8DogStatsDReader::new(&payload[..]);
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
    fn utf8_reader_single_msg_multiple_newlines() {
        // Given 1 msg
        let payload = b"my.metric:1|g\n\n";
        let mut reader = Utf8DogStatsDReader::new(&payload[..]);
        let mut s = String::new();

        // When reader is read
        let num_read = reader.read_msg(&mut s).unwrap();
        // Expect one msg
        assert_eq!(s.as_str(), "my.metric:1|g");
        assert_eq!(num_read, 1);
        s.clear();

        let num_read = reader.read_msg(&mut s).unwrap();
        // then no more
        assert_eq!(num_read, 0);
    }

    #[test]
    fn utf8_reader_single_msg_trailing_newline() {
        // Given one msg with newline
        let payload = b"my.metric:1|g\n";
        let mut reader = Utf8DogStatsDReader::new(&payload[..]);
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
    fn utf8_reader_multi_msg_msg() {
        // Given 2 msgs
        let payload = b"my.metric:1|g\nmy.metric:2|g";
        let mut reader = Utf8DogStatsDReader::new(&payload[..]);
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
    fn utf8_reader_multi_msg_msg_trailing_newline() {
        // Given 2 msgs with a trailing newline
        let payload = b"my.metric:1|g\nmy.metric:2|g\n";
        let mut reader = Utf8DogStatsDReader::new(&payload[..]);
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
    fn utf8_reader_example() {
        // Given 2 msgs with a trailing newline
        let payload = b"my.metric:1|g\nmy.metric:2|g\nother.metric:20|d|#env:staging\nother.thing:10|d|#datacenter:prod\n";
        let mut reader = Utf8DogStatsDReader::new(&payload[..]);
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
}
