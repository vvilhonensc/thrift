// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use std::io::{self, Cursor, Read};
use thrift::protocol::{
    TBinaryInputProtocol, TBinaryOutputProtocol, TFieldIdentifier, TInputProtocol, TListIdentifier,
    TMapIdentifier, TOutputProtocol, TStructIdentifier, TType,
};
use thrift::transport::{TBufferedReadTransport, TFramedReadTransport, TReadTransport};

fn fixture() -> Vec<u8> {
    let mut data = Vec::new();
    let mut p = TBinaryOutputProtocol::new(&mut data, true);
    p.write_struct_begin(&TStructIdentifier::new("Test"))
        .unwrap();
    p.write_field_begin(&TFieldIdentifier::new("text", TType::String, 1))
        .unwrap();
    p.write_string(&"hello".repeat(20_000)).unwrap();
    p.write_field_end().unwrap();
    p.write_field_begin(&TFieldIdentifier::new("numbers", TType::List, 2))
        .unwrap();
    p.write_list_begin(&TListIdentifier::new(TType::I64, 100))
        .unwrap();
    for i in 0..100 {
        p.write_i64(i).unwrap();
    }
    p.write_list_end().unwrap();
    p.write_field_end().unwrap();
    p.write_field_begin(&TFieldIdentifier::new("nested", TType::Struct, 3))
        .unwrap();
    p.write_struct_begin(&TStructIdentifier::new("Nested"))
        .unwrap();
    p.write_field_begin(&TFieldIdentifier::new("value", TType::Double, 1))
        .unwrap();
    p.write_double(1.25).unwrap();
    p.write_field_end().unwrap();
    p.write_field_stop().unwrap();
    p.write_struct_end().unwrap();
    p.write_field_end().unwrap();
    p.write_field_begin(&TFieldIdentifier::new("map", TType::Map, 4))
        .unwrap();
    p.write_map_begin(&TMapIdentifier::new(TType::String, TType::I32, 2))
        .unwrap();
    for i in 0..2 {
        p.write_string("key").unwrap();
        p.write_i32(i).unwrap();
    }
    p.write_map_end().unwrap();
    p.write_field_end().unwrap();
    p.write_field_stop().unwrap();
    p.write_struct_end().unwrap();
    p.write_i32(0x12345678).unwrap();
    data
}

#[test]
fn tiny_buffer_round_trip_and_skip_have_same_position() {
    let data = fixture();
    let mut p = TBinaryInputProtocol::new(
        TBufferedReadTransport::with_capacity(8, Cursor::new(&data)),
        true,
    );
    p.read_struct_begin().unwrap();
    assert_eq!(p.read_field_begin().unwrap().id, Some(1));
    assert_eq!(p.read_string().unwrap(), "hello".repeat(20_000));
    p.read_field_end().unwrap();
    assert_eq!(p.read_field_begin().unwrap().id, Some(2));
    let list = p.read_list_begin().unwrap();
    assert_eq!(list.element_type, TType::I64);
    assert_eq!(list.size, 100);
    for i in 0..100 {
        assert_eq!(p.read_i64().unwrap(), i);
    }
    p.read_list_end().unwrap();
    p.read_field_end().unwrap();
    assert_eq!(p.read_field_begin().unwrap().id, Some(3));
    p.read_struct_begin().unwrap();
    assert_eq!(p.read_field_begin().unwrap().id, Some(1));
    assert_eq!(p.read_double().unwrap(), 1.25);
    p.read_field_end().unwrap();
    assert_eq!(p.read_field_begin().unwrap().field_type, TType::Stop);
    p.read_struct_end().unwrap();
    p.read_field_end().unwrap();
    assert_eq!(p.read_field_begin().unwrap().id, Some(4));
    assert_eq!(p.read_map_begin().unwrap().size, 2);
    for i in 0..2 {
        assert_eq!(p.read_string().unwrap(), "key");
        assert_eq!(p.read_i32().unwrap(), i);
    }
    p.read_map_end().unwrap();
    p.read_field_end().unwrap();
    assert_eq!(p.read_field_begin().unwrap().field_type, TType::Stop);
    p.read_struct_end().unwrap();
    assert_eq!(p.read_i32().unwrap(), 0x12345678);

    let mut p = TBinaryInputProtocol::new(
        TBufferedReadTransport::with_capacity(8, Cursor::new(&data)),
        true,
    );
    p.skip(TType::Struct).unwrap();
    assert_eq!(p.read_i32().unwrap(), 0x12345678);
}

#[test]
fn skip_preserves_depth_and_size_checks() {
    use thrift::{Error, ProtocolErrorKind};
    for (data, ty, depth, kind) in [
        (
            vec![10, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0],
            TType::List,
            1,
            ProtocolErrorKind::DepthLimit,
        ),
        (
            vec![255; 4],
            TType::String,
            8,
            ProtocolErrorKind::NegativeSize,
        ),
        (
            vec![10, 255, 255, 255, 255],
            TType::List,
            8,
            ProtocolErrorKind::NegativeSize,
        ),
    ] {
        let mut p = TBinaryInputProtocol::new(
            TBufferedReadTransport::with_capacity(8, Cursor::new(data)),
            true,
        );
        assert!(matches!(p.skip_till_depth(ty, depth), Err(Error::Protocol(e)) if e.kind == kind));
    }
    let mut p = TBinaryInputProtocol::new(
        TBufferedReadTransport::with_capacity(8, Cursor::new([10, 0, 0, 0, 0])),
        true,
    );
    p.skip_till_depth(TType::List, 1).unwrap();
}

fn expect_bytes(t: &mut dyn TReadTransport, expected: &[u8]) {
    let mut calls = 0;
    t.with_bytes(expected.len(), &mut |bytes| {
        calls += 1;
        assert_eq!(bytes, expected);
    })
    .unwrap();
    assert_eq!(calls, 1);
}

struct BorrowOnly<'a> {
    bytes: &'a [u8],
    skips: Vec<usize>,
}

impl Read for BorrowOnly<'_> {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        panic!("binary input should use borrowed reads");
    }
}

impl TReadTransport for BorrowOnly<'_> {
    fn with_bytes(&mut self, n: usize, f: &mut dyn FnMut(&[u8])) -> io::Result<()> {
        if n > self.bytes.len() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        f(&self.bytes[..n]);
        self.bytes = &self.bytes[n..];
        Ok(())
    }
    fn skip_bytes(&mut self, n: usize) -> io::Result<()> {
        self.skips.push(n);
        self.with_bytes(n, &mut |_| {})
    }
}

#[test]
fn binary_uses_borrowed_reads_and_bulk_skips() {
    let data = fixture();
    let mut p = TBinaryInputProtocol::new(
        BorrowOnly {
            bytes: &data,
            skips: Vec::new(),
        },
        true,
    );
    p.skip(TType::Struct).unwrap();
    assert!(p.transport.skips.contains(&100_000));
    assert!(p.transport.skips.contains(&800));
    assert_eq!(p.read_i32().unwrap(), 0x12345678);

    let mut data = Vec::new();
    let mut out = TBinaryOutputProtocol::new(&mut data, true);
    out.write_bool(true).unwrap();
    out.write_i8(-12).unwrap();
    out.write_i16(-234).unwrap();
    out.write_i32(-123456).unwrap();
    out.write_i64(i64::MIN).unwrap();
    out.write_double(-1.25).unwrap();
    out.write_uuid(&uuid::Uuid::nil()).unwrap();
    out.write_string("hello").unwrap();
    out.write_bytes(&[0, 255, 1]).unwrap();
    out.write_field_stop().unwrap();
    let mut p = TBinaryInputProtocol::new(
        BorrowOnly {
            bytes: &data,
            skips: Vec::new(),
        },
        true,
    );
    assert!(p.read_bool().unwrap());
    assert_eq!(p.read_i8().unwrap(), -12);
    assert_eq!(p.read_i16().unwrap(), -234);
    assert_eq!(p.read_i32().unwrap(), -123456);
    assert_eq!(p.read_i64().unwrap(), i64::MIN);
    assert_eq!(p.read_double().unwrap(), -1.25);
    assert_eq!(p.read_uuid().unwrap(), uuid::Uuid::nil());
    assert_eq!(p.read_string().unwrap(), "hello");
    assert_eq!(p.read_bytes().unwrap(), [0, 255, 1]);
    assert_eq!(p.read_field_begin().unwrap().field_type, TType::Stop);
}

#[test]
fn primitive_lists_and_sets_use_one_bulk_skip() {
    for ty in [TType::List, TType::Set] {
        for (element, width) in [(2, 1), (3, 1), (6, 2), (8, 4), (10, 8), (4, 8), (16, 16)] {
            let mut data = vec![element, 0, 0, 0, 3];
            data.resize(5 + 3 * width, 0);
            data.push(42);
            let mut p = TBinaryInputProtocol::new(
                BorrowOnly {
                    bytes: &data,
                    skips: Vec::new(),
                },
                true,
            );
            p.skip_till_depth(ty, 2).unwrap();
            assert_eq!(p.transport.skips, [3 * width]);
            assert_eq!(p.read_i8().unwrap(), 42);
        }
    }
}

#[test]
fn oversized_borrow_preserves_buffer_after_short_reads() {
    let mut t = TBufferedReadTransport::with_capacity(
        8,
        ShortReads {
            data: Cursor::new((0..40).collect()),
            interrupted: false,
        },
    );
    expect_bytes(&mut t, &[0]);
    expect_bytes(&mut t, &(1..25).collect::<Vec<_>>());
    expect_bytes(&mut t, &[25, 26, 27]);
    t.skip_bytes(11).unwrap();
    expect_bytes(&mut t, &[39]);
}

#[test]
fn buffered_borrows_refills_and_large_requests() {
    let data: Vec<u8> = (0..40).collect();
    let mut t = TBufferedReadTransport::with_capacity(8, Cursor::new(data.clone()));
    expect_bytes(&mut t, &data[..3]);
    expect_bytes(&mut t, &data[3..5]);
    expect_bytes(&mut t, &data[5..11]);
    expect_bytes(&mut t, &data[11..30]);
    expect_bytes(&mut t, &data[30..]);
    expect_bytes(&mut t, &[]);
}

#[test]
fn buffered_skip_across_refills() {
    let data: Vec<u8> = (0..40).collect();
    let mut t = TBufferedReadTransport::with_capacity(8, Cursor::new(data));
    expect_bytes(&mut t, &[0]);
    t.skip_bytes(3).unwrap();
    expect_bytes(&mut t, &[4]);
    t.skip_bytes(30).unwrap();
    expect_bytes(&mut t, &[35, 36, 37, 38, 39]);
    t.skip_bytes(0).unwrap();
}

#[test]
fn buffered_eof_does_not_call_callback() {
    for n in [4, 20] {
        let mut t = TBufferedReadTransport::with_capacity(8, Cursor::new([1, 2, 3]));
        let err = t
            .with_bytes(n, &mut |_| panic!("incomplete read"))
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        let mut t = TBufferedReadTransport::with_capacity(8, Cursor::new([1, 2, 3]));
        assert_eq!(
            t.skip_bytes(n).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
    }
}

struct ShortReads {
    data: Cursor<Vec<u8>>,
    interrupted: bool,
}

impl Read for ShortReads {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.interrupted = !self.interrupted;
        if self.interrupted {
            return Err(io::ErrorKind::Interrupted.into());
        }
        let n = out.len().min(2);
        self.data.read(&mut out[..n])
    }
}

#[test]
fn borrowed_reads_retry_short_and_interrupted_reads() {
    let mut t = TBufferedReadTransport::with_capacity(
        8,
        ShortReads {
            data: Cursor::new((0..40).collect()),
            interrupted: false,
        },
    );
    expect_bytes(&mut t, &[0, 1, 2, 3, 4]);
    t.skip_bytes(30).unwrap();
    expect_bytes(&mut t, &[35, 36, 37, 38, 39]);
}

#[test]
fn framed_requests_stay_in_one_frame() {
    let data = [0, 0, 0, 3, 1, 2, 3, 0, 0, 0, 2, 4, 5];
    for skip in [false, true] {
        let mut t = TFramedReadTransport::new(Cursor::new(data));
        expect_bytes(&mut t, &[]);
        expect_bytes(&mut t, &[1]);
        let err = if skip {
            t.skip_bytes(3)
        } else {
            t.with_bytes(3, &mut |_| panic!("crossed frame"))
        }
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }
    let mut t: Box<dyn TReadTransport> = Box::new(TFramedReadTransport::new(Cursor::new(data)));
    expect_bytes(&mut t, &[1]);
    t.skip_bytes(2).unwrap();
    expect_bytes(&mut t, &[]);
    expect_bytes(&mut t, &[4, 5]);
}

#[test]
fn default_methods_support_custom_readers_and_eof() {
    struct Custom(Cursor<Vec<u8>>);
    impl Read for Custom {
        fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
            self.0.read(b)
        }
    }
    impl TReadTransport for Custom {}
    let mut t = Custom(Cursor::new((0..40).collect()));
    expect_bytes(&mut t, &[0, 1]);
    expect_bytes(&mut t, &(2..22).collect::<Vec<_>>());
    t.skip_bytes(17).unwrap();
    expect_bytes(&mut t, &[39]);
    expect_bytes(&mut t, &[]);
    assert_eq!(
        t.with_bytes(1, &mut |_| panic!("EOF")).unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert_eq!(
        t.skip_bytes(1).unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
}

#[test]
fn binary_borrowed_message_headers_and_utf8_errors() {
    use thrift::protocol::{TMessageIdentifier, TMessageType};
    for strict in [true, false] {
        let message = TMessageIdentifier::new("message", TMessageType::Call, 12);
        let mut data = Vec::new();
        TBinaryOutputProtocol::new(&mut data, strict)
            .write_message_begin(&message)
            .unwrap();
        let t: Box<dyn TReadTransport> = Box::new(BorrowOnly {
            bytes: &data,
            skips: Vec::new(),
        });
        let mut p = TBinaryInputProtocol::new(t, strict);
        assert_eq!(p.read_message_begin().unwrap(), message);
    }
    let mut p = TBinaryInputProtocol::new(
        BorrowOnly {
            bytes: &[0, 0, 0, 1, 255],
            skips: Vec::new(),
        },
        true,
    );
    assert!(
        matches!(p.read_string(), Err(thrift::Error::Protocol(e)) if e.kind == thrift::ProtocolErrorKind::InvalidData)
    );
}

#[test]
fn skip_enforces_configured_limits() {
    use thrift::{Error, ProtocolErrorKind, TConfiguration};
    let cases = [
        (
            vec![0, 0, 0, 2],
            TType::String,
            TConfiguration::builder()
                .max_string_size(Some(1))
                .build()
                .unwrap(),
            ProtocolErrorKind::SizeLimit,
        ),
        (
            vec![10, 0, 0, 0, 2],
            TType::List,
            TConfiguration::builder()
                .max_container_size(Some(1))
                .build()
                .unwrap(),
            ProtocolErrorKind::SizeLimit,
        ),
        (
            vec![10, 0, 0, 0, 2],
            TType::Set,
            TConfiguration::builder()
                .max_message_size(Some(8))
                .max_frame_size(Some(8))
                .build()
                .unwrap(),
            ProtocolErrorKind::SizeLimit,
        ),
        (
            vec![12, 0, 1, 0, 0],
            TType::Struct,
            TConfiguration::builder()
                .max_recursion_depth(Some(1))
                .build()
                .unwrap(),
            ProtocolErrorKind::DepthLimit,
        ),
    ];
    for (data, ty, config, kind) in cases {
        let mut p = TBinaryInputProtocol::with_config(
            BorrowOnly {
                bytes: &data,
                skips: Vec::new(),
            },
            true,
            config,
        );
        assert!(matches!(p.skip(ty), Err(Error::Protocol(e)) if e.kind == kind));
    }
}

#[test]
fn skip_nested_containers_at_depth_boundary() {
    // Alternate list and map nesting, including more than 16 open containers.
    for nesting in [1usize, 15, 16, 17, 40] {
        let mut data = vec![0, 0, 0, 1, b'x'];
        let mut ty = TType::String;
        for level in 0..nesting {
            let tag = match ty {
                TType::String => 11,
                TType::List => 15,
                TType::Map => 13,
                _ => unreachable!(),
            };
            let mut outer = if level % 2 == 0 {
                vec![tag, 0, 0, 0, 1]
            } else {
                vec![8, tag, 0, 0, 0, 1, 0, 0, 0, 7]
            };
            outer.extend_from_slice(&data);
            data = outer;
            ty = if level % 2 == 0 {
                TType::List
            } else {
                TType::Map
            };
        }
        data.push(42);
        let mut p = TBinaryInputProtocol::new(data.as_slice(), true);
        p.skip_till_depth(ty, (nesting + 1) as i8).unwrap();
        assert_eq!(p.read_i8().unwrap(), 42);
        let mut p = TBinaryInputProtocol::new(data.as_slice(), true);
        assert!(matches!(
            p.skip_till_depth(ty, nesting as i8),
            Err(thrift::Error::Protocol(e)) if e.kind == thrift::ProtocolErrorKind::DepthLimit
        ));
    }
}

#[test]
fn skip_rejects_nonpositive_depth_before_consuming_input() {
    for depth in [0, -1, i8::MIN] {
        let mut p = TBinaryInputProtocol::new([42u8].as_slice(), true);
        assert!(matches!(
            p.skip_till_depth(TType::I08, depth),
            Err(thrift::Error::Protocol(e)) if e.kind == thrift::ProtocolErrorKind::DepthLimit
        ));
        assert_eq!(p.read_i8().unwrap(), 42);
    }
}

#[test]
fn skip_resumes_map_after_container_keys_and_values() {
    let mut data = Vec::new();
    let mut out = TBinaryOutputProtocol::new(&mut data, true);
    out.write_map_begin(&TMapIdentifier::new(TType::List, TType::Struct, 2))
        .unwrap();
    for i in 0..2 {
        out.write_list_begin(&TListIdentifier::new(TType::I32, 2))
            .unwrap();
        out.write_i32(i).unwrap();
        out.write_i32(i + 1).unwrap();
        out.write_list_end().unwrap();
        out.write_struct_begin(&TStructIdentifier::new("Value"))
            .unwrap();
        out.write_field_begin(&TFieldIdentifier::new("text", TType::String, 1))
            .unwrap();
        out.write_string("value").unwrap();
        out.write_field_end().unwrap();
        out.write_field_stop().unwrap();
        out.write_struct_end().unwrap();
    }
    out.write_map_end().unwrap();
    out.write_i8(42).unwrap();
    let mut p = TBinaryInputProtocol::new(data.as_slice(), true);
    p.skip_till_depth(TType::Map, 3).unwrap();
    assert_eq!(p.read_i8().unwrap(), 42);
}

#[test]
fn skip_deep_structs_restores_configured_depth() {
    let mut record = [12, 0, 1].repeat(19);
    record.extend_from_slice(&[0; 20]);
    let mut data = record.repeat(2);
    data.push(42);
    let config = thrift::TConfiguration::builder()
        .max_recursion_depth(Some(20))
        .build()
        .unwrap();
    let mut p = TBinaryInputProtocol::with_config(data.as_slice(), true, config);
    p.skip_till_depth(TType::Struct, 20).unwrap();
    p.skip_till_depth(TType::Struct, 20).unwrap();
    assert_eq!(p.read_i8().unwrap(), 42);
}
