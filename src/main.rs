use std::env;
use tikv_client::RawClient;

#[tokio::main]
async fn main() {
    let pd_endpoints: Vec<String> = env::args().skip(1).collect();
    if pd_endpoints.is_empty() {
        eprintln!("Usage: test_tikv_client <pd_addr1> [pd_addr2 ...]");
        eprintln!("Example: test_tikv_client 127.0.0.1:2379");
        std::process::exit(1);
    }

    println!("=== TiKV Scan Test (RawClient) ===");
    println!("PD endpoints: {:?}\n", pd_endpoints);

    println!("Connecting to TiKV cluster...");
    let client = RawClient::new(pd_endpoints)
        .await
        .expect("failed to connect");

    println!("Connected successfully!\n");

    // Scan first 20 keys starting from 't' prefix (table data)
    let start_key = vec![b't'];
    let end_key = vec![b'u']; // just after 't'
    println!("Scanning 20 keys from range [t..u)...");

    match client.scan(start_key..end_key, 20).await {
        Ok(pairs) => {
            println!("Found {} key-value pairs:\n", pairs.len());

            for (i, kv) in pairs.iter().enumerate() {
                let key_bytes: Vec<u8> = kv.0.clone().into();
                let val_bytes: &[u8] = &kv.1;

                println!("--- [{}] ---", i);
                println!("  key (hex):  {}", bytes_to_hex(&key_bytes));
                println!("  key (raw):  {:?}", key_bytes);
                println!("  key (tidb): {}", decode_tidb_key(&key_bytes));
                println!("  val (hex):  {}", bytes_to_hex(val_bytes));
                println!("  val (len):  {} bytes", val_bytes.len());

                // Show readable ASCII strings found in value
                let readable = extract_ascii_strings(val_bytes, 4);
                if !readable.is_empty() {
                    println!("  val (strings): {}", readable.join(" | "));
                }
                println!();
            }
        }
        Err(e) => {
            eprintln!("Scan failed: {}", e);
            std::process::exit(1);
        }
    }

    println!("=== Done ===");
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

fn decode_i64(bytes: &[u8]) -> i64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    buf[0] ^= 0x80; // flip sign bit
    i64::from_be_bytes(buf)
}

/// Extract printable ASCII strings (min_len or longer) from binary data.
fn extract_ascii_strings(data: &[u8], min_len: usize) -> Vec<String> {
    let mut strings = Vec::new();
    let mut current = String::new();
    for &b in data {
        if b >= 0x20 && b < 0x7f {
            current.push(b as char);
        } else {
            if current.len() >= min_len {
                strings.push(current.clone());
            }
            current.clear();
        }
    }
    if current.len() >= min_len {
        strings.push(current);
    }
    strings
}


// timeout 10 ./target/debug/test_tikv_client 10.0.12.184:2379
