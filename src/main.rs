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

                // Try to show value as UTF-8 if possible (truncate if too long)
                if let Ok(s) = std::str::from_utf8(val_bytes) {
                    let display = if s.len() > 100 {
                        format!("{}... (truncated)", &s[..100])
                    } else {
                        s.to_string()
                    };
                    println!("  val (utf8): {}", display);
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

/// Decode TiDB key encoding:
///   table record: 't' + table_id(8B) + '_r' + row_id(8B)
///   table index:  't' + table_id(8B) + '_i' + index_id(8B) + ...
/// Integers are big-endian with sign bit flipped (XOR 0x80 on first byte).
fn decode_tidb_key(key: &[u8]) -> String {
    if key.is_empty() || key[0] != b't' {
        return "not a table key".to_string();
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

fn decode_i64(bytes: &[u8]) -> i64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    buf[0] ^= 0x80; // flip sign bit
    i64::from_be_bytes(buf)
}


// timeout 10 ./target/debug/test_tikv_client 10.0.12.184:2379
