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
            _ => pd_endpoints.push(args[i].clone()),
        }
        i += 1;
    }

    if pd_endpoints.is_empty() {
        eprintln!("Usage: test_tikv_client <pd_addr> [--table-id <ID>] [--limit <N>] [--txn]");
        eprintln!("Example: test_tikv_client 10.0.12.184:2379 --table-id 11875 --limit 5 --txn");
        std::process::exit(1);
    }

    let mode = if use_txn { "TransactionClient" } else { "RawClient" };
    println!("=== TiKV Scan Test ({}) ===", mode);
    println!("PD endpoints: {:?}", pd_endpoints);
    if let Some(tid) = table_id_filter {
        println!("Filter: table_id={}", tid);
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
            let mut end = vec![b't'];
            end.extend_from_slice(&encode_i64(tid + 1));
            (start, end, format!("table_id={}", tid))
        } else {
            // RawClient: use memcomparable-encoded key
            (encode_table_prefix(tid), encode_table_prefix(tid + 1), format!("table_id={}", tid))
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
    if raw_mode {
        // RawClient returns memcomparable-encoded keys with MVCC suffix
        println!("  key (tidb): {}", decode_tidb_key(key_bytes));
    } else {
        // TransactionClient returns logical keys directly
        println!("  key (tidb): {}", decode_logical_key(key_bytes));
    }
    println!("  val (hex):  {}", bytes_to_hex(val_bytes));
    println!("  val (len):  {} bytes", val_bytes.len());

    let readable = extract_ascii_strings(val_bytes, 4);
    if !readable.is_empty() {
        println!("  val (strings): {}", readable.join(" | "));
    }
    println!();
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
