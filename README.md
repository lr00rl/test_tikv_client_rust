# test_tikv_client

使用 Rust TiKV Client 直接读取 TiKV 底层 KV 数据，并解析 TiDB 的 Key 编码，反查出 table_id、row_id 等信息。

## 背景

TiDB 是建立在 TiKV 之上的 SQL 层。TiKV 本身是纯 KV 存储，没有表、列、schema 的概念。TiDB 通过自定义的 Key/Value 编码规则，将 SQL 表的行和索引映射到 TiKV 的 KV 对中。

本项目通过 TiKV Rust Client 直接扫描 TiKV 中的原始 KV 数据，并解码出 TiDB 层面的 table_id、row_id / index_id 等信息，再通过 TiDB SQL 接口反向验证。

## 依赖

```toml
[dependencies]
tikv-client = "0.3"
tokio = { version = "1", features = ["full"] }
```

- Rust >= 1.56.1
- 需要能访问 PD (Placement Driver) 节点以及 TiKV 节点的网络

## 编译与运行

```bash
cargo build
./target/debug/test_tikv_client <PD_ADDR>

# 示例
./target/debug/test_tikv_client 10.0.12.184:2379

# 如果连接超时可加 timeout
timeout 10 ./target/debug/test_tikv_client 10.0.12.184:2379
```

PD 地址通过命令行参数传入，支持多个：

```bash
./target/debug/test_tikv_client 10.0.12.184:2379 10.0.10.43:2379
```

### 网络注意事项

TiKV Client 连接 PD 后，PD 会返回 TiKV 节点的地址。如果 PD 返回的是内网 IP（如 `10.0.x.x`），而你从外部访问，需要做地址映射。可以用 iptables DNAT：

```bash
sudo iptables -t nat -A OUTPUT -d 10.0.12.184 -j DNAT --to-destination <公网IP1>
sudo iptables -t nat -A OUTPUT -d 10.0.10.43  -j DNAT --to-destination <公网IP2>
```

## TiDB 的 Key 编码详解

### 整体架构

```
TiDB (SQL 层)
  ↓ 编码
TiKV (KV 存储层)
  Key:   bytes
  Value: bytes
```

TiKV 中的每一个 KV 对，在 TiDB 看来就是一行记录或一条索引。

### Key 的逻辑格式

TiDB 有两种 Key 类型：

| 类型 | 格式 | 说明 |
|------|------|------|
| 行记录 (Record) | `t{table_id}_r{row_id}` | 一行数据 |
| 索引 (Index)    | `t{table_id}_i{index_id}{index_value}` | 一条索引项 |

具体字节布局（逻辑层）：

```
行记录 Key (19 bytes):
┌──────┬──────────────────┬──────┬──────────────────┐
│ 't'  │ table_id (8B)    │ '_r' │ row_id (8B)      │
│ 0x74 │ big-endian i64   │ 5f72 │ big-endian i64   │
└──────┴──────────────────┴──────┴──────────────────┘

索引 Key (19+ bytes):
┌──────┬──────────────────┬──────┬──────────────────┬─────────────┐
│ 't'  │ table_id (8B)    │ '_i' │ index_id (8B)    │ index_value │
│ 0x74 │ big-endian i64   │ 5f69 │ big-endian i64   │ ...         │
└──────┴──────────────────┴──────┴──────────────────┴─────────────┘
```

### 整数编码方式 (Comparable Encoding)

table_id、row_id 等 i64 值使用 **符号位翻转 + big-endian** 编码，使得编码后的字节序与数值大小一致（可直接按字节比较排序）：

```
编码: 将 i64 转为 big-endian 8 字节，然后将第一个字节 XOR 0x80
解码: 将第一个字节 XOR 0x80，然后按 big-endian 读取 i64
```

示例：

| 值 | big-endian hex | 编码后 hex |
|----|---------------|-----------|
| 0  | `00 00 00 00 00 00 00 00` | `80 00 00 00 00 00 00 00` |
| 1  | `00 00 00 00 00 00 00 01` | `80 00 00 00 00 00 00 01` |
| 24 | `00 00 00 00 00 00 00 18` | `80 00 00 00 00 00 00 18` |
| -1 | `ff ff ff ff ff ff ff ff` | `7f ff ff ff ff ff ff ff` |

对应代码 (`decode_i64`)：

```rust
fn decode_i64(bytes: &[u8]) -> i64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    buf[0] ^= 0x80; // flip sign bit
    i64::from_be_bytes(buf)
}
```

### Memcomparable Bytes 编码

TiKV 在逻辑 Key 之上还包了一层 **memcomparable bytes** 编码，保证编码后的字节序仍然等价于原始 Key 的排序。

规则：

- 每 8 字节数据后跟 1 字节标记（marker），共 9 字节一组
- 标记 `0xff`：本组 8 字节全部有效，后面还有更多组
- 标记 `0xff - N`：本组最后 N 字节是填充（`0x00`），只有前 `8 - N` 字节有效，这是最后一组

```
原始字节:     [b0 b1 b2 b3 b4 b5 b6 b7] [b8 b9 bA ...]
              ↓
编码后:       [b0 b1 b2 b3 b4 b5 b6 b7 ff] [b8 b9 bA 00 00 00 00 00 fc]
              ──────── 8字节 ──── marker    ──── 8字节(含填充) ── marker
                                 (全有效)                        (0xff-0xfc=3字节有效+5填充)
```

对应代码 (`decode_memcomparable_bytes`)：

```rust
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
```

### MVCC 版本号

在 memcomparable 编码的 Key 之后，TiKV 还会追加 8 字节的 MVCC 版本号（时间戳）。在用 RawClient scan 时这部分会出现在 key 末尾。

### 完整解码流程

```
TiKV 中的原始 Key bytes
  │
  ├─ Step 1: Memcomparable 解码 → 逻辑 Key + 剩余 MVCC 版本号
  │
  ├─ Step 2: 解析逻辑 Key
  │   ├─ key[0] = 't' (0x74)
  │   ├─ key[1..9] → decode_i64 → table_id
  │   ├─ key[9..11] = "_r" 或 "_i"
  │   └─ key[11..19] → decode_i64 → row_id 或 index_id
  │
  └─ Step 3: 通过 table_id 在 TiDB 中反查表名
```

对应代码 (`decode_tidb_key`)：

```rust
fn decode_tidb_key(raw_key: &[u8]) -> String {
    let (key, consumed) = decode_memcomparable_bytes(raw_key);
    let remaining = raw_key.len() - consumed;
    // key[0] == 't', key[1..9] → table_id, key[9..11] → tag, key[11..19] → row_id/index_id
    ...
}
```

## Value 中的字符串提取

TiDB 的 Value 使用自定义行格式（Row Format v2），其中字符串/blob 类型的列值也使用了 memcomparable bytes 编码。因此 value 的二进制数据中，ASCII 字符串会被 `0xff` 标记字节打断。

`extract_ascii_strings` 函数在提取可读字符串时会跳过夹在 ASCII 字符之间的 `0xff` 字节，从而还原完整的字符串。

## 案例演示

### 1. 扫描 TiKV 中的原始 KV 数据

```bash
$ ./target/debug/test_tikv_client 10.0.12.184:2379
```

输出：

```
=== TiKV Scan Test (RawClient) ===
PD endpoints: ["10.0.12.184:2379"]

Connecting to TiKV cluster...
Connected successfully!

Scanning 20 keys from range [t..u)...
Found 20 key-value pairs:

--- [0] ---
  key (hex):  74 80 00 00 00 00 00 00 ff 18 5f 72 80 00 00 00 00 ff 04 56 4d 00 00 00 00 00 fa f9 9a 79 61 35 cf ff fc
  key (raw):  [116, 128, 0, 0, 0, ...]
  key (tidb): table_id=24, record row_id=284237 (+ 8 bytes mvcc ver)
  val (hex):  08 02 08 a0 97 01 ...
  val (len):  307 bytes
  val (strings): 202509_raw_data_first_insert | s3://openjobs-jobdata-import/multisource/member/202509_parquet/partition_by_column=united_states/part-00001-fec457e0-e4b1-42ab-94b6-8fd0b2ff47bf.c001.gz.parquet
```

### 2. Key 解码过程详解

以上面 key 的 hex 为例，手动走一遍解码：

```
原始 key hex (35 bytes):
74 80 00 00 00 00 00 00 ff | 18 5f 72 80 00 00 00 00 ff | 04 56 4d 00 00 00 00 00 fa | f9 9a 79 61 35 cf ff fc
───────── group 1 ─────────  ───────── group 2 ─────────  ───────── group 3 ─────────  ────── mvcc (8B) ──────

Group 1: marker=0xff → 全部 8 字节有效 → 74 80 00 00 00 00 00 00
Group 2: marker=0xff → 全部 8 字节有效 → 18 5f 72 80 00 00 00 00
Group 3: marker=0xfa → 填充 5 字节 → 前 3 字节有效 → 04 56 4d

逻辑 Key (19 bytes): 74 80 00 00 00 00 00 00 18 5f 72 80 00 00 00 00 04 56 4d
                      │  └─── table_id ────┘ └tag┘ └──── row_id ─────┘
                      t

table_id: 80 00 00 00 00 00 00 18 → XOR 0x80 → 00 00 00 00 00 00 00 18 → 24
row_id:   80 00 00 00 00 04 56 4d → XOR 0x80 → 00 00 00 00 00 04 56 4d → 284237
```

### 3. 通过 TiDB SQL 反查表名

```sql
SELECT TABLE_SCHEMA, TABLE_NAME, TIDB_TABLE_ID
FROM information_schema.tables
WHERE TIDB_TABLE_ID = 24;
```

结果：

```
+--------------+------------------+---------------+
| TABLE_SCHEMA | TABLE_NAME       | TIDB_TABLE_ID |
+--------------+------------------+---------------+
| mysql        | stats_histograms |            24 |
+--------------+------------------+---------------+
```

### 4. 通过 _tidb_rowid 伪列验证具体行

对于 `NONCLUSTERED` 主键的表，TiDB 使用隐式的 `_tidb_rowid` 作为 row_id：

```sql
SELECT *, _tidb_rowid
FROM mysql.stats_histograms
WHERE _tidb_rowid = 284237;
```

结果：

```
+----------+----------+---------+----------------+------------+--------------+--------------+--------------------+-----------+-----------+------+-------------+------------------+-------------+
| table_id | is_index | hist_id | distinct_count | null_count | tot_col_size | modify_count | version            | cm_sketch | stats_ver | flag | correlation | last_analyze_pos | _tidb_rowid |
+----------+----------+---------+----------------+------------+--------------+--------------+--------------------+-----------+-----------+------+-------------+------------------+-------------+
|     9680 |        1 |       2 |          11425 |          0 |      2273575 |            0 | 460922553430441987 | NULL      |         2 |    1 |           0 | (blob data)      |      284237 |
+----------+----------+---------+----------------+------------+--------------+--------------+--------------------+-----------+-----------+------+-------------+------------------+-------------+
```

验证通过：TiKV 中的 raw key 解码出 `table_id=24, row_id=284237`，在 TiDB SQL 层能精确查到对应的行。

## 函数索引

| 函数 | 作用 |
|------|------|
| `decode_memcomparable_bytes` | 解码 memcomparable bytes 编码，每 9 字节一组（8 数据 + 1 标记） |
| `decode_tidb_key`            | 组合解码：先 memcomparable，再解析 table_id + tag + row_id/index_id |
| `decode_i64`                 | 解码符号位翻转的 big-endian i64 |
| `extract_ascii_strings`      | 从二进制 value 中提取可读 ASCII 字符串（跳过 0xff 标记） |
| `bytes_to_hex`               | 字节转 hex 字符串显示 |

## 参考

- [TiDB Key-Value 映射](https://docs.pingcap.com/tidb/stable/tidb-computing#mapping-table-data-to-key-value)
- [TiKV Rust Client (GitHub)](https://github.com/tikv/client-rust)
- [tikv-client crate (crates.io)](https://crates.io/crates/tikv-client)
