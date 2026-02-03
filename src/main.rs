use std::env;
use tikv_client::{RawClient, TransactionClient};

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().skip(1).collect();

    // Parse optional --table-id and --limit flags
    let mut pd_endpoints = Vec::new();
    let mut table_id_filter: Option<i64> = None;
    let mut limit: u32 = 20;
    let mut use_txn = false;
    let mut record_type: Option<String> = None; // "record", "index", or None (all)
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--table-id" => {
                i += 1;
                table_id_filter = Some(args[i].parse().expect("--table-id must be a number"));
            }
            "--limit" => {
                i += 1;
                limit = args[i].parse().expect("--limit must be a number");
            }
            "--txn" => {
                use_txn = true;
            }
            "--type" => {
                i += 1;
                record_type = Some(args[i].clone());
            }
            _ => pd_endpoints.push(args[i].clone()),
        }
        i += 1;
    }

    if pd_endpoints.is_empty() {
        eprintln!("Usage: test_tikv_client <pd_addr> [--table-id <ID>] [--limit <N>] [--txn] [--type record|index]");
        eprintln!("Example: test_tikv_client 10.0.12.184:2379 --table-id 11875 --limit 5 --txn --type record");
        std::process::exit(1);
    }

    let mode = if use_txn { "TransactionClient" } else { "RawClient" };
    println!("=== TiKV Scan Test ({}) ===", mode);
    println!("PD endpoints: {:?}", pd_endpoints);
    if let Some(tid) = table_id_filter {
        println!("Filter: table_id={}", tid);
    }
    if let Some(ref t) = record_type {
        println!("Type filter: {}", t);
    }
    println!("Limit: {}\n", limit);

    // Build scan range
    // For --table-id, use the logical key directly (no memcomparable encoding).
    // TiDB TransactionClient handles encoding internally.
    // For RawClient, keys in storage have memcomparable encoding.
    let (start_key, end_key, range_desc) = if let Some(tid) = table_id_filter {
        if use_txn {
            // TransactionClient: use logical key (no memcomparable)
            let mut start = vec![b't'];
            start.extend_from_slice(&encode_i64(tid));

            // Apply --type filter on key range
            if let Some(ref t) = record_type {
                match t.as_str() {
                    "record" => start.extend_from_slice(b"_r"),
                    "index" => start.extend_from_slice(b"_i"),
                    _ => {}
                }
            }

            let mut end = vec![b't'];
            end.extend_from_slice(&encode_i64(tid + 1));
            (start, end, format!("table_id={}", tid))
        } else {
            // RawClient: use memcomparable-encoded key
            let mut start_logical = vec![b't'];
            start_logical.extend_from_slice(&encode_i64(tid));

            if let Some(ref t) = record_type {
                match t.as_str() {
                    "record" => start_logical.extend_from_slice(b"_r"),
                    "index" => start_logical.extend_from_slice(b"_i"),
                    _ => {}
                }
            }

            (encode_memcomparable_bytes(&start_logical), encode_table_prefix(tid + 1), format!("table_id={}", tid))
        }
    } else {
        (vec![b't'], vec![b'u'], "all tables".to_string())
    };

    println!("start_key (hex): {}", bytes_to_hex(&start_key));
    println!("end_key   (hex): {}", bytes_to_hex(&end_key));
    println!();

    if use_txn {
        println!("Connecting (TransactionClient)...");
        let txn_client = TransactionClient::new(pd_endpoints)
            .await
            .expect("failed to connect");
        println!("Connected!\n");

        let mut txn = txn_client.begin_optimistic().await.expect("failed to begin txn");
        println!("Scanning {} keys for [{}]...", limit, range_desc);

        match txn.scan(start_key..end_key, limit).await {
            Ok(pairs) => {
                let pairs: Vec<_> = pairs.collect();
                println!("Found {} key-value pairs:\n", pairs.len());
                for (i, kv) in pairs.iter().enumerate() {
                    let key_bytes: Vec<u8> = kv.0.clone().into();
                    let val_bytes: &[u8] = &kv.1;
                    print_kv(i, &key_bytes, val_bytes, false);
                }
            }
            Err(e) => {
                eprintln!("Scan failed: {}", e);
                std::process::exit(1);
            }
        }
        let _ = txn.commit().await;
    } else {
        println!("Connecting (RawClient)...");
        let client = RawClient::new(pd_endpoints)
            .await
            .expect("failed to connect");
        println!("Connected!\n");

        println!("Scanning {} keys for [{}]...", limit, range_desc);

        match client.scan(start_key..end_key, limit).await {
            Ok(pairs) => {
                println!("Found {} key-value pairs:\n", pairs.len());
                for (i, kv) in pairs.iter().enumerate() {
                    let key_bytes: Vec<u8> = kv.0.clone().into();
                    let val_bytes: &[u8] = &kv.1;
                    print_kv(i, &key_bytes, val_bytes, true);
                }
            }
            Err(e) => {
                eprintln!("Scan failed: {}", e);
                std::process::exit(1);
            }
        }
    }

    println!("=== Done ===");
}

fn print_kv(i: usize, key_bytes: &[u8], val_bytes: &[u8], raw_mode: bool) {
    println!("--- [{}] ---", i);
    println!("  key (hex):  {}", bytes_to_hex(key_bytes));

    let decoded_key = if raw_mode {
        decode_tidb_key_detailed(key_bytes)
    } else {
        decode_logical_key_detailed(key_bytes)
    };

    println!("  {}", decoded_key);

    println!("  val (hex):  {}", bytes_to_hex(val_bytes));
    println!("  val (len):  {} bytes", val_bytes.len());

    // Decode value if it's an index entry
    if !key_bytes.is_empty() && is_index_key(key_bytes, raw_mode) {
        decode_index_value(val_bytes);
    }

    let readable = extract_ascii_strings(val_bytes, 4);
    if !readable.is_empty() {
        println!("  val (strings): {}", readable.join(" | "));
    }
    println!();
}

/// Determine if key is an index key (_i) vs record key (_r).
fn is_index_key(key: &[u8], raw_mode: bool) -> bool {
    if raw_mode {
        let (decoded, _) = decode_memcomparable_bytes(key);
        decoded.len() >= 11 && decoded[0] == b't' && &decoded[9..11] == b"_i"
    } else {
        key.len() >= 11 && key[0] == b't' && &key[9..11] == b"_i"
    }
}

/// Decode a logical TiDB key with detailed field parsing.
fn decode_logical_key_detailed(key: &[u8]) -> String {
    if key.is_empty() || key[0] != b't' {
        return format!("key (tidb): not a table key");
    }
    if key.len() < 9 {
        return format!("key (tidb): table key too short ({} bytes)", key.len());
    }

    let table_id = decode_i64(&key[1..9]);

    if key.len() < 11 {
        return format!("key (tidb): table_id={}", table_id);
    }

    let tag = &key[9..11];
    match tag {
        b"_r" => {
            if key.len() >= 19 {
                let row_id = decode_i64(&key[11..19]);
                format!("key (tidb): table_id={}, record, row_id={}", table_id, row_id)
            } else {
                format!("key (tidb): table_id={}, record (row_id truncated)", table_id)
            }
        }
        b"_i" => {
            if key.len() >= 19 {
                let index_id = decode_i64(&key[11..19]);
                let mut result = format!("key (tidb): table_id={}, index_id={}", table_id, index_id);

                // Try to decode index column values
                if key.len() > 19 {
                    let index_data = &key[19..];
                    result.push_str("\n  key (index): ");
                    result.push_str(&decode_index_columns(index_data));
                }
                result
            } else {
                format!("key (tidb): table_id={}, index (index_id truncated)", table_id)
            }
        }
        _ => format!("key (tidb): table_id={}, unknown tag {:02x}{:02x}", table_id, tag[0], tag[1]),
    }
}

/// Decode a logical TiDB key (no memcomparable encoding, as returned by TransactionClient).
fn decode_logical_key(key: &[u8]) -> String {
    if key.is_empty() || key[0] != b't' {
        return format!("not a table key (hex: {})", bytes_to_hex(key));
    }
    if key.len() < 9 {
        return format!("table key too short ({} bytes)", key.len());
    }

    let table_id = decode_i64(&key[1..9]);

    if key.len() < 11 {
        return format!("table_id={}", table_id);
    }

    let tag = &key[9..11];
    match tag {
        b"_r" => {
            if key.len() >= 19 {
                let row_id = decode_i64(&key[11..19]);
                format!("table_id={}, record row_id={}", table_id, row_id)
            } else {
                format!("table_id={}, record (row_id truncated)", table_id)
            }
        }
        b"_i" => {
            if key.len() >= 19 {
                let index_id = decode_i64(&key[11..19]);
                format!("table_id={}, index_id={}", table_id, index_id)
            } else {
                format!("table_id={}, index (index_id truncated)", table_id)
            }
        }
        _ => format!("table_id={}, unknown tag {:02x}{:02x}", table_id, tag[0], tag[1]),
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Decode memcomparable bytes encoding used by TiKV.
/// Every 8 data bytes are followed by 1 marker byte:
///   0xff = all 8 bytes valid, more groups follow
///   0xff - N = last group, only (8-N) bytes are real data
fn decode_memcomparable_bytes(encoded: &[u8]) -> (Vec<u8>, usize) {
    let mut decoded = Vec::new();
    let mut pos = 0;
    loop {
        if pos + 9 > encoded.len() {
            break;
        }
        let group = &encoded[pos..pos + 8];
        let marker = encoded[pos + 8];
        pos += 9;

        if marker == 0xff {
            decoded.extend_from_slice(group);
        } else {
            let pad_count = (0xff - marker) as usize;
            if pad_count <= 8 {
                decoded.extend_from_slice(&group[..8 - pad_count]);
            }
            break;
        }
    }
    (decoded, pos)
}

/// Decode TiDB key with detailed parsing (includes memcomparable layer).
fn decode_tidb_key_detailed(raw_key: &[u8]) -> String {
    let (key, consumed) = decode_memcomparable_bytes(raw_key);
    let remaining = raw_key.len() - consumed;

    if key.is_empty() || key[0] != b't' {
        return "key (tidb): not a table key".to_string();
    }
    if key.len() < 9 {
        return format!("key (tidb): table key too short ({} decoded bytes)", key.len());
    }

    let table_id = decode_i64(&key[1..9]);

    if key.len() < 11 {
        return format!("key (tidb): table_id={}", table_id);
    }

    let tag = &key[9..11];
    let suffix = if remaining > 0 {
        format!(" (+ {} bytes mvcc)", remaining)
    } else {
        String::new()
    };

    match tag {
        b"_r" => {
            if key.len() >= 19 {
                let row_id = decode_i64(&key[11..19]);
                format!("key (tidb): table_id={}, record, row_id={}{}", table_id, row_id, suffix)
            } else {
                format!("key (tidb): table_id={}, record (row_id truncated){}", table_id, suffix)
            }
        }
        b"_i" => {
            if key.len() >= 19 {
                let index_id = decode_i64(&key[11..19]);
                let mut result = format!("key (tidb): table_id={}, index_id={}{}", table_id, index_id, suffix);

                if key.len() > 19 {
                    let index_data = &key[19..];
                    result.push_str("\n  key (index): ");
                    result.push_str(&decode_index_columns(index_data));
                }
                result
            } else {
                format!("key (tidb): table_id={}, index (index_id truncated){}", table_id, suffix)
            }
        }
        _ => format!("key (tidb): table_id={}, unknown tag {:02x}{:02x}{}", table_id, tag[0], tag[1], suffix),
    }
}

/// Decode TiDB key encoding (with memcomparable layer):
///   table record: 't' + table_id(8B) + '_r' + row_id(8B)
///   table index:  't' + table_id(8B) + '_i' + index_id(8B) + ...
/// Integers are big-endian with sign bit flipped (XOR 0x80 on first byte).
fn decode_tidb_key(raw_key: &[u8]) -> String {
    // Step 1: strip memcomparable encoding
    let (key, consumed) = decode_memcomparable_bytes(raw_key);
    let remaining = raw_key.len() - consumed;

    if key.is_empty() || key[0] != b't' {
        return "not a table key".to_string();
    }
    if key.len() < 9 {
        return format!("table key too short ({} decoded bytes)", key.len());
    }

    let table_id = decode_i64(&key[1..9]);

    if key.len() < 11 {
        return format!("table_id={}", table_id);
    }

    let tag = &key[9..11];
    let suffix = if remaining > 0 {
        format!(" (+ {} bytes mvcc ver)", remaining)
    } else {
        String::new()
    };

    match tag {
        b"_r" => {
            if key.len() >= 19 {
                let row_id = decode_i64(&key[11..19]);
                format!("table_id={}, record row_id={}{}", table_id, row_id, suffix)
            } else {
                format!("table_id={}, record (row_id truncated){}", table_id, suffix)
            }
        }
        b"_i" => {
            if key.len() >= 19 {
                let index_id = decode_i64(&key[11..19]);
                format!("table_id={}, index_id={}{}", table_id, index_id, suffix)
            } else {
                format!("table_id={}, index (index_id truncated){}", table_id, suffix)
            }
        }
        _ => format!("table_id={}, unknown tag {:02x}{:02x}{}", table_id, tag[0], tag[1], suffix),
    }
}

/// Encode a table prefix key: 't' + table_id, with memcomparable bytes encoding.
/// This produces the start key for scanning a specific table.
fn encode_table_prefix(table_id: i64) -> Vec<u8> {
    // Logical key: 't' (1 byte) + encoded table_id (8 bytes) = 9 bytes
    let mut logical = vec![b't'];
    logical.extend_from_slice(&encode_i64(table_id));

    // Memcomparable encode: 9 bytes → group1 (8 data + 0xff) + group2 (1 data + 7 padding + 0xf8)
    encode_memcomparable_bytes(&logical)
}

fn encode_i64(val: i64) -> [u8; 8] {
    let mut buf = val.to_be_bytes();
    buf[0] ^= 0x80;
    buf
}

fn encode_memcomparable_bytes(data: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    let mut pos = 0;
    loop {
        let remaining = data.len() - pos;
        if remaining >= 8 {
            encoded.extend_from_slice(&data[pos..pos + 8]);
            encoded.push(0xff);
            pos += 8;
        } else {
            let mut group = [0u8; 8];
            group[..remaining].copy_from_slice(&data[pos..]);
            encoded.extend_from_slice(&group);
            encoded.push(0xff - (8 - remaining) as u8);
            break;
        }
    }
    encoded
}

fn decode_i64(bytes: &[u8]) -> i64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    buf[0] ^= 0x80; // flip sign bit
    i64::from_be_bytes(buf)
}

/// Decode index columns from the key data after table_id + _i + index_id.
/// This is a best-effort decode showing field types and raw values.
fn decode_index_columns(data: &[u8]) -> String {
    let mut result = Vec::new();
    let mut pos = 0;

    while pos < data.len() {
        if pos + 1 > data.len() {
            break;
        }

        let type_flag = data[pos];
        pos += 1;

        match type_flag {
            // Int (positive): 0x03 + 8 bytes
            0x03 => {
                if pos + 8 <= data.len() {
                    let val = decode_i64(&data[pos..pos + 8]);
                    result.push(format!("int={}", val));
                    pos += 8;
                } else {
                    result.push("int=<truncated>".to_string());
                    break;
                }
            }
            // Bytes/String: 0x01 + data + 0xff markers
            0x01 => {
                let _start = pos;
                let mut decoded = Vec::new();
                loop {
                    if pos + 9 > data.len() {
                        break;
                    }
                    let group = &data[pos..pos + 8];
                    let marker = data[pos + 8];
                    pos += 9;

                    if marker == 0xff {
                        decoded.extend_from_slice(group);
                    } else {
                        let pad = (0xff - marker) as usize;
                        if pad <= 8 {
                            decoded.extend_from_slice(&group[..8 - pad]);
                        }
                        break;
                    }
                }
                if let Ok(s) = String::from_utf8(decoded.clone()) {
                    result.push(format!("str=\"{}\"", s));
                } else {
                    result.push(format!("bytes=<{} bytes>", decoded.len()));
                }
            }
            _ => {
                result.push(format!("unknown_type=0x{:02x}", type_flag));
                break;
            }
        }
    }

    if result.is_empty() {
        format!("<{} bytes>", data.len())
    } else {
        result.join(", ")
    }
}

/// Decode index value to extract handle (_tidb_rowid) and restore data.
/// Index value format (simplified):
///   - For unique index: [version_info] + handle + [restore_data]
fn decode_index_value(val: &[u8]) {
    if val.is_empty() {
        return;
    }

    // Try to extract handle from the end (common handle is usually last 8 bytes for int handle)
    if val.len() >= 8 {
        // The handle is often encoded after some prefix bytes.
        // In TiDB's new collation format, there's usually a version byte, then restore data, then handle.
        // Let's try to find int64-like patterns (8 consecutive bytes that decode to reasonable values).

        // Common pattern: first byte is length/version, then comes data
        let _first_byte = val[0];

        // Try to decode handle from different positions
        let mut handle_candidates = Vec::new();

        // Try position after first byte
        if val.len() >= 9 {
            let h = decode_i64(&val[1..9]);
            if h > 0 && h < 1_000_000_000 {
                handle_candidates.push((1, h));
            }
        }

        // Try last 8 bytes
        if val.len() >= 8 {
            let offset = val.len() - 8;
            let h = decode_i64(&val[offset..]);
            if h > 0 && h < 1_000_000_000 {
                handle_candidates.push((offset, h));
            }
        }

        // Try position at offset 9 (common in newer format)
        if val.len() >= 17 {
            let h = decode_i64(&val[9..17]);
            if h > 0 && h < 1_000_000_000 {
                handle_candidates.push((9, h));
            }
        }

        if !handle_candidates.is_empty() {
            println!("  val (parsed): handle_candidates={:?}", handle_candidates);
            if let Some((offset, h)) = handle_candidates.first() {
                println!("  val (handle): _tidb_rowid={} (at offset {})", h, offset);
            }
        }
    }
}

/// Extract printable ASCII strings (min_len or longer) from binary data.
/// Skips 0xff bytes (memcomparable markers) between printable chars so
/// encoded strings are reconstructed intact.
fn extract_ascii_strings(data: &[u8], min_len: usize) -> Vec<String> {
    let mut strings = Vec::new();
    let mut current = String::new();
    let mut i = 0;
    while i < data.len() {
        let b = data[i];
        if b >= 0x20 && b < 0x7f {
            current.push(b as char);
        } else if b == 0xff && !current.is_empty() {
            // memcomparable marker between printable chars - skip it
        } else {
            if current.len() >= min_len {
                strings.push(current.clone());
            }
            current.clear();
        }
        i += 1;
    }
    if current.len() >= min_len {
        strings.push(current);
    }
    strings
}


// timeout 10 ./target/debug/test_tikv_client 10.0.12.184:2379
