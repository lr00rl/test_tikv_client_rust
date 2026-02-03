use std::env;
use tikv_client::TransactionClient;

#[tokio::main]
async fn main() {
    let pd_endpoints: Vec<String> = env::args().skip(1).collect();
    if pd_endpoints.is_empty() {
        eprintln!("Usage: test_tikv_client <pd_addr1> [pd_addr2 ...]");
        eprintln!("Example: test_tikv_client 127.0.0.1:2379");
        std::process::exit(1);
    }

    println!("=== TiKV Scan Test ===");
    println!("PD endpoints: {:?}\n", pd_endpoints);

    let txn_client = TransactionClient::new(pd_endpoints)
        .await
        .expect("failed to connect to PD");

    let mut txn = txn_client
        .begin_optimistic()
        .await
        .expect("failed to begin txn");

    // Scan the first 20 keys starting from the beginning
    // TiDB table data keys start with 't' (0x74), meta keys start with 'm' (0x6d)
    let start: Vec<u8> = vec![0x74]; // 't' prefix - table data region
    let end: Vec<u8> = vec![0x75];   // just past 't'

    match txn.scan(start..end, 20).await {
        Ok(pairs) => {
            let pairs: Vec<_> = pairs.collect();
            println!("Found {} key-value pairs:\n", pairs.len());
            for (i, pair) in pairs.iter().enumerate() {
                let key_bytes: Vec<u8> = pair.0.clone().into();
                let val_bytes: &[u8] = &pair.1;

                println!("--- [{i}] ---");
                println!("  key hex:   {}", hex(&key_bytes));
                println!("  key bytes: {:?}", key_bytes);
                println!("  val hex:   {}", hex(val_bytes));
                // Try to show as UTF-8 if possible
                if let Ok(s) = std::str::from_utf8(val_bytes) {
                    println!("  val utf8:  {s}");
                }
                println!();
            }
        }
        Err(e) => eprintln!("scan error: {e}"),
    }

    let _ = txn.commit().await;
    println!("=== Done ===");
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect::<Vec<_>>().join(" ")
}
