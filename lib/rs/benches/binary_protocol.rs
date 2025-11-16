use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use thrift::protocol::{
    TBinaryInputProtocol, TBinaryOutputProtocol, TInputProtocol, TMessageIdentifier, TMessageType,
    TOutputProtocol,
};
use thrift::transport::TBufferChannel;

// Helper function to create a channel with serialized data
fn create_channel_with_data(capacity: usize) -> TBufferChannel {
    TBufferChannel::with_capacity(capacity, capacity)
}

// Helper to serialize and return the bytes
fn serialize_to_bytes<F>(f: F) -> Vec<u8>
where
    F: FnOnce(&mut TBinaryOutputProtocol<TBufferChannel>) -> thrift::Result<()>,
{
    let channel = create_channel_with_data(1024 * 1024); // 1MB buffer
    let mut protocol = TBinaryOutputProtocol::new(channel.clone(), true);
    f(&mut protocol).unwrap();
    protocol.flush().unwrap();
    channel.write_bytes()
}

/// Real-world scenario: Deserialize a typical message with mixed types
/// This shows the combined impact of all optimizations
fn bench_realistic_message(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimization_realistic");

    // A typical struct with various field types
    let message_data = serialize_to_bytes(|p| {
        use thrift::protocol::{TFieldIdentifier, TStructIdentifier, TType};

        p.write_message_begin(&TMessageIdentifier::new(
            "getUserProfile",
            TMessageType::Call,
            12345,
        ))?;

        p.write_struct_begin(&TStructIdentifier::new("UserProfile"))?;

        // Field 1: user_id (i64)
        p.write_field_begin(&TFieldIdentifier::new("user_id", TType::I64, 1))?;
        p.write_i64(9876543210)?;
        p.write_field_end()?;

        // Field 2: username (string, 20 chars)
        p.write_field_begin(&TFieldIdentifier::new("username", TType::String, 2))?;
        p.write_string("john_doe_12345")?;
        p.write_field_end()?;

        // Field 3: email (string, 30 chars)
        p.write_field_begin(&TFieldIdentifier::new("email", TType::String, 3))?;
        p.write_string("john.doe@example.com")?;
        p.write_field_end()?;

        // Field 4: age (i32)
        p.write_field_begin(&TFieldIdentifier::new("age", TType::I32, 4))?;
        p.write_i32(30)?;
        p.write_field_end()?;

        // Field 5: balance (double)
        p.write_field_begin(&TFieldIdentifier::new("balance", TType::Double, 5))?;
        p.write_double(12345.67)?;
        p.write_field_end()?;

        // Field 6: avatar (bytes, 100 bytes)
        p.write_field_begin(&TFieldIdentifier::new("avatar", TType::String, 6))?;
        p.write_bytes(&vec![0xFF; 100])?;
        p.write_field_end()?;

        // Field 7: bio (string, 200 chars)
        p.write_field_begin(&TFieldIdentifier::new("bio", TType::String, 7))?;
        p.write_string(&"x".repeat(200))?;
        p.write_field_end()?;

        p.write_field_stop()?;
        p.write_struct_end()?;
        p.write_message_end()
    });

    group.bench_function("read_user_profile_message", |b| {
        b.iter(|| {
            let mut channel = create_channel_with_data(message_data.len());
            channel.set_readable_bytes(&message_data);
            let mut protocol = TBinaryInputProtocol::new(channel, true);

            use thrift::protocol::TType;

            let _msg = protocol.read_message_begin().unwrap();
            protocol.read_struct_begin().unwrap();

            let mut user_id = 0i64;
            let mut username = String::new();
            let mut email = String::new();
            let mut age = 0i32;
            let mut balance = 0.0f64;
            let mut avatar = Vec::new();
            let mut bio = String::new();

            loop {
                let field = protocol.read_field_begin().unwrap();
                if field.field_type == TType::Stop {
                    break;
                }

                match field.id {
                    Some(1) => user_id = protocol.read_i64().unwrap(),
                    Some(2) => username = protocol.read_string().unwrap(),
                    Some(3) => email = protocol.read_string().unwrap(),
                    Some(4) => age = protocol.read_i32().unwrap(),
                    Some(5) => balance = protocol.read_double().unwrap(),
                    Some(6) => avatar = protocol.read_bytes().unwrap(),
                    Some(7) => bio = protocol.read_string().unwrap(),
                    _ => protocol.skip(field.field_type).unwrap(),
                }
                protocol.read_field_end().unwrap();
            }

            protocol.read_struct_end().unwrap();
            protocol.read_message_end().unwrap();

            black_box((user_id, username, email, age, balance, avatar, bio))
        })
    });

    group.finish();
}

/// Complex nested structure benchmark: list of events with nested collections
/// This tests deep nesting with maps, lists of structs, and multiple levels
/// Exercises 4 levels of nesting with maps and lists at each level
fn bench_complex_nested_message(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimization_complex_nested");

    // Serialize a list of Event objects with deep nesting
    let message_data = serialize_to_bytes(|p| {
        use thrift::protocol::{
            TFieldIdentifier, TListIdentifier, TMapIdentifier, TStructIdentifier, TType,
        };

        p.write_message_begin(&TMessageIdentifier::new(
            "getComplexData",
            TMessageType::Call,
            1,
        ))?;

        // List of 3 Event structs
        p.write_list_begin(&TListIdentifier::new(TType::Struct, 3))?;

        // Write 3 events
        for event_idx in 0..3 {
            p.write_struct_begin(&TStructIdentifier::new("Event"))?;

            // Field 1: Metadata struct
            p.write_field_begin(&TFieldIdentifier::new("metadata", TType::Struct, 1))?;
            p.write_struct_begin(&TStructIdentifier::new("Metadata"))?;

            // Field 1: id (string)
            p.write_field_begin(&TFieldIdentifier::new("id", TType::String, 1))?;
            p.write_string(&format!("meta_{}", event_idx))?;
            p.write_field_end()?;

            // Field 2: TimeRange struct
            p.write_field_begin(&TFieldIdentifier::new("timeRange", TType::Struct, 2))?;
            p.write_struct_begin(&TStructIdentifier::new("TimeRange"))?;

            p.write_field_begin(&TFieldIdentifier::new("start", TType::String, 1))?;
            p.write_string("20231101T120000.000Z")?;
            p.write_field_end()?;

            p.write_field_begin(&TFieldIdentifier::new("end", TType::String, 2))?;
            p.write_string("20231130T235959.999Z")?;
            p.write_field_end()?;

            p.write_field_stop()?;
            p.write_struct_end()?;
            p.write_field_end()?;

            // Field 3: secondaryTimeRange struct
            p.write_field_begin(&TFieldIdentifier::new(
                "secondaryTimeRange",
                TType::Struct,
                3,
            ))?;
            p.write_struct_begin(&TStructIdentifier::new("TimeRange"))?;

            p.write_field_begin(&TFieldIdentifier::new("start", TType::String, 1))?;
            p.write_string("20231101T100000.000Z")?;
            p.write_field_end()?;

            p.write_field_begin(&TFieldIdentifier::new("end", TType::String, 2))?;
            p.write_string("20231130T235959.999Z")?;
            p.write_field_end()?;

            p.write_field_stop()?;
            p.write_struct_end()?;
            p.write_field_end()?;

            // Field 4: Config struct
            p.write_field_begin(&TFieldIdentifier::new("config", TType::Struct, 4))?;
            p.write_struct_begin(&TStructIdentifier::new("Config"))?;

            p.write_field_begin(&TFieldIdentifier::new("type", TType::I32, 1))?;
            p.write_i32(1)?; // Type 1
            p.write_field_end()?;

            p.write_field_begin(&TFieldIdentifier::new("globalFlag", TType::Bool, 2))?;
            p.write_bool(event_idx == 0)?;
            p.write_field_end()?;

            p.write_field_stop()?;
            p.write_struct_end()?;
            p.write_field_end()?;

            // Field 5: active (bool)
            p.write_field_begin(&TFieldIdentifier::new("active", TType::Bool, 5))?;
            p.write_bool(true)?;
            p.write_field_end()?;

            // Field 6: labels (map<string, string> - multilingual labels)
            p.write_field_begin(&TFieldIdentifier::new("labels", TType::Map, 6))?;
            p.write_map_begin(&TMapIdentifier::new(TType::String, TType::String, 3))?;
            p.write_string("en")?;
            p.write_string("Primary Label")?;
            p.write_string("es")?;
            p.write_string("Etiqueta Principal")?;
            p.write_string("fi")?;
            p.write_string("Ensisijainen Merkintä")?;
            p.write_map_end()?;
            p.write_field_end()?;

            // Field 7: notes (map<string, string> - multilingual notes)
            p.write_field_begin(&TFieldIdentifier::new("notes", TType::Map, 7))?;
            p.write_map_begin(&TMapIdentifier::new(TType::String, TType::String, 3))?;
            p.write_string("en")?;
            p.write_string("Additional information about this data structure")?;
            p.write_string("es")?;
            p.write_string("Información adicional sobre esta estructura de datos")?;
            p.write_string("fi")?;
            p.write_string("Lisätietoja tästä tietorakenteesta")?;
            p.write_map_end()?;
            p.write_field_end()?;

            // Field 8: reference (string)
            p.write_field_begin(&TFieldIdentifier::new("reference", TType::String, 8))?;
            p.write_string("https://example.com/reference.txt")?;
            p.write_field_end()?;

            p.write_field_stop()?;
            p.write_struct_end()?;
            p.write_field_end()?;

            // Field 2: packages (list<Package>)
            p.write_field_begin(&TFieldIdentifier::new("packages", TType::List, 2))?;
            p.write_list_begin(&TListIdentifier::new(TType::Struct, 2))?;

            // Write 2 Package structs
            for pkg_idx in 0..2 {
                p.write_struct_begin(&TStructIdentifier::new("Package"))?;

                // Field 1: category (enum as i32)
                p.write_field_begin(&TFieldIdentifier::new("category", TType::I32, 1))?;
                p.write_i32(pkg_idx)?; // Category type
                p.write_field_end()?;

                // Field 2: id (string)
                p.write_field_begin(&TFieldIdentifier::new("id", TType::String, 2))?;
                p.write_string(&format!("com.example.app.package_{}", pkg_idx))?;
                p.write_field_end()?;

                // Field 3: sequenceId (optional i32)
                if pkg_idx == 1 {
                    p.write_field_begin(&TFieldIdentifier::new("sequenceId", TType::I32, 3))?;
                    p.write_i32(42)?;
                    p.write_field_end()?;
                }

                // Field 4: variantId (optional i32)
                if pkg_idx == 1 {
                    p.write_field_begin(&TFieldIdentifier::new("variantId", TType::I32, 4))?;
                    p.write_i32(0)?;
                    p.write_field_end()?;
                }

                // Field 5: identifier (string)
                p.write_field_begin(&TFieldIdentifier::new("identifier", TType::String, 5))?;
                p.write_string(&format!("com.example.app.level{}", pkg_idx + 1))?;
                p.write_field_end()?;

                // Field 6: titleMap (map)
                p.write_field_begin(&TFieldIdentifier::new("titleMap", TType::Map, 6))?;
                p.write_map_begin(&TMapIdentifier::new(TType::String, TType::String, 3))?;
                p.write_string("en")?;
                p.write_string(&format!("Package Set {}", pkg_idx + 1))?;
                p.write_string("es")?;
                p.write_string(&format!("Conjunto {}", pkg_idx + 1))?;
                p.write_string("fi")?;
                p.write_string(&format!("Paketti {}", pkg_idx + 1))?;
                p.write_map_end()?;
                p.write_field_end()?;

                // Field 7: descriptionMap (map)
                p.write_field_begin(&TFieldIdentifier::new("descriptionMap", TType::Map, 7))?;
                p.write_map_begin(&TMapIdentifier::new(TType::String, TType::String, 3))?;
                p.write_string("en")?;
                p.write_string("Comprehensive data package")?;
                p.write_string("es")?;
                p.write_string("Paquete de datos completo")?;
                p.write_string("fi")?;
                p.write_string("Kattava datapaketti")?;
                p.write_map_end()?;
                p.write_field_end()?;

                // Field 8: attributes (list<string>)
                p.write_field_begin(&TFieldIdentifier::new("attributes", TType::List, 8))?;
                p.write_list_begin(&TListIdentifier::new(TType::String, 4))?;
                p.write_string("priority")?;
                p.write_string("time_limited")?;
                p.write_string("recommended")?;
                p.write_string("highlighted")?;
                p.write_list_end()?;
                p.write_field_end()?;

                // Field 9: entries (list<Entry>)
                p.write_field_begin(&TFieldIdentifier::new("entries", TType::List, 9))?;
                p.write_list_begin(&TListIdentifier::new(TType::Struct, 3))?;

                // Write 3 Entry structs
                for entry_idx in 0..3 {
                    p.write_struct_begin(&TStructIdentifier::new("Entry"))?;

                    // Field 1: id (string)
                    p.write_field_begin(&TFieldIdentifier::new("id", TType::String, 1))?;
                    p.write_string(&format!("entry_{}", entry_idx))?;
                    p.write_field_end()?;

                    // Field 2: kind (enum as i32)
                    p.write_field_begin(&TFieldIdentifier::new("kind", TType::I32, 2))?;
                    p.write_i32(entry_idx)?; // Entry kind type
                    p.write_field_end()?;

                    // Field 3: nameMap (map)
                    p.write_field_begin(&TFieldIdentifier::new("nameMap", TType::Map, 3))?;
                    p.write_map_begin(&TMapIdentifier::new(TType::String, TType::String, 3))?;
                    p.write_string("en")?;
                    p.write_string(&format!("Entry {}", entry_idx + 1))?;
                    p.write_string("es")?;
                    p.write_string(&format!("Entrada {}", entry_idx + 1))?;
                    p.write_string("fi")?;
                    p.write_string(&format!("Merkintä {}", entry_idx + 1))?;
                    p.write_map_end()?;
                    p.write_field_end()?;

                    // Field 4: detailsMap (map)
                    p.write_field_begin(&TFieldIdentifier::new("detailsMap", TType::Map, 4))?;
                    p.write_map_begin(&TMapIdentifier::new(TType::String, TType::String, 3))?;
                    p.write_string("en")?;
                    p.write_string("Core data element")?;
                    p.write_string("es")?;
                    p.write_string("Elemento de datos principal")?;
                    p.write_string("fi")?;
                    p.write_string("Ydindata-elementti")?;
                    p.write_map_end()?;
                    p.write_field_end()?;

                    // Field 5: quantity (i32)
                    p.write_field_begin(&TFieldIdentifier::new("quantity", TType::I32, 5))?;
                    p.write_i32((entry_idx + 1) * 100)?;
                    p.write_field_end()?;

                    p.write_field_stop()?;
                    p.write_struct_end()?;
                }

                p.write_list_end()?;
                p.write_field_end()?;

                // Field 13: weight (optional i32)
                p.write_field_begin(&TFieldIdentifier::new("weight", TType::I32, 13))?;
                p.write_i32(100 - (pkg_idx * 10))?;
                p.write_field_end()?;

                p.write_field_stop()?;
                p.write_struct_end()?;
            }

            p.write_list_end()?;
            p.write_field_end()?;

            p.write_field_stop()?;
            p.write_struct_end()?;
        }

        p.write_list_end()?;
        p.write_message_end()
    });

    group.bench_function("read_complex_nested_data", |b| {
        b.iter(|| {
            let mut channel = create_channel_with_data(message_data.len());
            channel.set_readable_bytes(&message_data);
            let mut protocol = TBinaryInputProtocol::new(channel, true);

            use thrift::protocol::TType;

            // Read message header
            let _msg = protocol.read_message_begin().unwrap();

            // Read list of events
            let list_ident = protocol.read_list_begin().unwrap();
            let mut events = Vec::with_capacity(list_ident.size as usize);

            for _ in 0..list_ident.size {
                // Read Event struct
                protocol.read_struct_begin().unwrap();

                let mut meta_id = String::new();
                let mut packages = Vec::new();

                loop {
                    let field = protocol.read_field_begin().unwrap();
                    if field.field_type == TType::Stop {
                        break;
                    }

                    match field.id {
                        Some(1) => {
                            // Metadata struct
                            protocol.read_struct_begin().unwrap();
                            loop {
                                let meta_field = protocol.read_field_begin().unwrap();
                                if meta_field.field_type == TType::Stop {
                                    break;
                                }

                                match meta_field.id {
                                    Some(1) => meta_id = protocol.read_string().unwrap(),
                                    Some(2) | Some(3) => {
                                        // TimeRange struct
                                        protocol.read_struct_begin().unwrap();
                                        loop {
                                            let time_field = protocol.read_field_begin().unwrap();
                                            if time_field.field_type == TType::Stop {
                                                break;
                                            }
                                            protocol.skip(time_field.field_type).unwrap();
                                            protocol.read_field_end().unwrap();
                                        }
                                        protocol.read_struct_end().unwrap();
                                    }
                                    Some(4) => {
                                        // Config struct
                                        protocol.read_struct_begin().unwrap();
                                        loop {
                                            let config_field = protocol.read_field_begin().unwrap();
                                            if config_field.field_type == TType::Stop {
                                                break;
                                            }
                                            protocol.skip(config_field.field_type).unwrap();
                                            protocol.read_field_end().unwrap();
                                        }
                                        protocol.read_struct_end().unwrap();
                                    }
                                    Some(6) | Some(7) => {
                                        // Multilingual maps
                                        let map_ident = protocol.read_map_begin().unwrap();
                                        for _ in 0..map_ident.size {
                                            protocol.read_string().unwrap();
                                            protocol.read_string().unwrap();
                                        }
                                        protocol.read_map_end().unwrap();
                                    }
                                    _ => protocol.skip(meta_field.field_type).unwrap(),
                                }
                                protocol.read_field_end().unwrap();
                            }
                            protocol.read_struct_end().unwrap();
                        }
                        Some(2) => {
                            // List of Package structs
                            let pkg_list = protocol.read_list_begin().unwrap();
                            for _ in 0..pkg_list.size {
                                protocol.read_struct_begin().unwrap();

                                let mut pkg_id = String::new();
                                let mut pkg_attrs = Vec::new();
                                let mut pkg_entries = Vec::new();

                                loop {
                                    let pkg_field = protocol.read_field_begin().unwrap();
                                    if pkg_field.field_type == TType::Stop {
                                        break;
                                    }

                                    match pkg_field.id {
                                        Some(2) => pkg_id = protocol.read_string().unwrap(),
                                        Some(6) | Some(7) => {
                                            // Multilingual maps
                                            let map_ident = protocol.read_map_begin().unwrap();
                                            for _ in 0..map_ident.size {
                                                protocol.read_string().unwrap();
                                                protocol.read_string().unwrap();
                                            }
                                            protocol.read_map_end().unwrap();
                                        }
                                        Some(8) => {
                                            // Attributes list
                                            let attrs_list = protocol.read_list_begin().unwrap();
                                            for _ in 0..attrs_list.size {
                                                pkg_attrs.push(protocol.read_string().unwrap());
                                            }
                                            protocol.read_list_end().unwrap();
                                        }
                                        Some(9) => {
                                            // List of Entry structs
                                            let entries_list = protocol.read_list_begin().unwrap();
                                            for _ in 0..entries_list.size {
                                                protocol.read_struct_begin().unwrap();

                                                let mut entry_id = String::new();
                                                let mut entry_qty = 0i32;

                                                loop {
                                                    let entry_field =
                                                        protocol.read_field_begin().unwrap();
                                                    if entry_field.field_type == TType::Stop {
                                                        break;
                                                    }

                                                    match entry_field.id {
                                                        Some(1) => {
                                                            entry_id =
                                                                protocol.read_string().unwrap()
                                                        }
                                                        Some(3) | Some(4) => {
                                                            // Multilingual maps
                                                            let map_ident =
                                                                protocol.read_map_begin().unwrap();
                                                            for _ in 0..map_ident.size {
                                                                protocol.read_string().unwrap();
                                                                protocol.read_string().unwrap();
                                                            }
                                                            protocol.read_map_end().unwrap();
                                                        }
                                                        Some(5) => {
                                                            entry_qty = protocol.read_i32().unwrap()
                                                        }
                                                        _ => protocol
                                                            .skip(entry_field.field_type)
                                                            .unwrap(),
                                                    }
                                                    protocol.read_field_end().unwrap();
                                                }

                                                protocol.read_struct_end().unwrap();
                                                pkg_entries.push((entry_id, entry_qty));
                                            }
                                            protocol.read_list_end().unwrap();
                                        }
                                        _ => protocol.skip(pkg_field.field_type).unwrap(),
                                    }
                                    protocol.read_field_end().unwrap();
                                }

                                protocol.read_struct_end().unwrap();
                                packages.push((pkg_id, pkg_attrs, pkg_entries));
                            }
                            protocol.read_list_end().unwrap();
                        }
                        _ => protocol.skip(field.field_type).unwrap(),
                    }
                    protocol.read_field_end().unwrap();
                }

                protocol.read_struct_end().unwrap();
                events.push((meta_id, packages));
            }

            protocol.read_list_end().unwrap();
            protocol.read_message_end().unwrap();

            black_box(events)
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_realistic_message,
    bench_complex_nested_message,
);
criterion_main!(benches);
