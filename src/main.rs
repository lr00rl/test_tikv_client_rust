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


// timeout 10 ./target/debug/test_tikv_client 10.0.12.184:2379
