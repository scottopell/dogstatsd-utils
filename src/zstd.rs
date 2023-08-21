use std::io::Cursor;

use byteorder::{BigEndian, LittleEndian, NativeEndian, ReadBytesExt};

// https://github.com/facebook/zstd/blob/3298a08076081dbfa8eba5b08c2167b06020c5ff/doc/zstd_compression_format.md#zstandard-frames
// 0xFD2FB528 as a little endian u32
const ZSTD_MAGIC_BYTES: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

pub fn is_zstd(header: &[u8]) -> bool {
    header[0] == ZSTD_MAGIC_BYTES[0]
        && header[1] == ZSTD_MAGIC_BYTES[1]
        && header[2] == ZSTD_MAGIC_BYTES[2]
        && header[3] == ZSTD_MAGIC_BYTES[3]
}

#[cfg(test)]
mod tests {
    use super::*;
    // export WORD=hello; echo -n "$WORD" | zstd | xxd -i | awk -v input=$(echo $WORD | tr '[:lower:]' '[:upper:]') 'BEGIN { print("const "  input  "_ZSTD_BYTES: &[u8] = &[") } { print $0 } END { print("];") }'
    const HELLO_ZSTD_BYTES: &[u8] = &[
        0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x58, 0x29, 0x00, 0x00, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0xa3,
        0x6d, 0x9f, 0x88,
    ];

    // export WORD=hello; echo -n "$WORD" |  xxd -i | awk -v input=$(echo $WORD | tr '[:lower:]' '[:upper:]') 'BEGIN { print("const "  input  "_ZSTD_BYTES: &[u8] = &[") } { print $0 } END { print("];") }'const HELLO_ZSTD_BYTES: &[u8] = &[
    const HELLO_BYTES: &[u8] = &[0x68, 0x65, 0x6c, 0x6c, 0x6f];

    #[test]
    fn is_zstd_compressed_data_is_detected() {
        assert!(is_zstd(HELLO_ZSTD_BYTES));
    }

    #[test]
    fn is_zstd_ascii_data_is_not_detected() {
        assert!(!is_zstd(HELLO_BYTES));
    }
}
