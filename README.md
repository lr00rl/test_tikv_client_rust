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
./target/debug/test_tikv_client <PD_ADDR> [OPTIONS]

# 基础示例
./target/debug/test_tikv_client 10.0.12.184:2379

# 指定 table_id 过滤
./target/debug/test_tikv_client 10.0.12.184:2379 --table-id 11875

# 限制扫描条数
./target/debug/test_tikv_client 10.0.12.184:2379 --table-id 11875 --limit 5

# 使用 TransactionClient (推荐，能读到 TiDB 写入的数据)
./target/debug/test_tikv_client 10.0.12.184:2379 --table-id 11875 --limit 5 --txn

# 只扫描行记录 (跳过索引)
./target/debug/test_tikv_client 10.0.12.184:2379 --table-id 11875 --txn --type record

# 只扫描索引
./target/debug/test_tikv_client 10.0.12.184:2379 --table-id 11875 --txn --type index

# 如果连接超时可加 timeout
timeout 10 ./target/debug/test_tikv_client 10.0.12.184:2379
```

### 命令行参数

| 参数 | 说明 | 示例 |
|------|------|------|
| `<PD_ADDR>` | PD 节点地址，支持多个 | `10.0.12.184:2379 10.0.10.43:2379` |
| `--table-id <ID>` | 过滤指定 table_id 的数据 | `--table-id 11875` |
| `--limit <N>` | 限制扫描的 KV 对数量 (默认 20) | `--limit 5` |
| `--txn` | 使用 TransactionClient 而非 RawClient | `--txn` |
| `--type <record\|index>` | 过滤 key 类型：只扫行记录或只扫索引 | `--type record` |

### RawClient vs TransactionClient

TiKV 提供两种客户端模式：

| 模式 | 说明 | 适用场景 |
|------|------|---------|
| **RawClient** | 直接读取 RawKV 层，不走 MVCC | 适用于原生使用 RawKV API 的数据 |
| **TransactionClient** | 通过事务层读取，走 MVCC | **适用于 TiDB 写入的数据（推荐）** |

**关键区别**：

- TiDB 写入的数据是通过 **Transaction API** 存储的，存在于 TiKV 的 MVCC 层
- RawClient 只能访问 RawKV 层，**看不到** TiDB 写入的数据
- TransactionClient 能正确读取 TiDB 数据，并且不会在 key 末尾带 MVCC 版本号（已在内部处理）

**实测案例**：

```bash
# RawClient 读不到数据
$ ./target/debug/test_tikv_client 10.0.12.184:2379 --table-id 11875 --limit 5
Found 0 key-value pairs

# TransactionClient 正常读取
$ ./target/debug/test_tikv_client 10.0.12.184:2379 --table-id 11875 --limit 5 --txn
Found 5 key-value pairs
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

**符号位翻转的作用**：

- 对于无符号数，字节序天然有序（`0x00 < 0x01 < ... < 0xff`）
- 对于有符号数，负数的最高位是 1，会大于正数的最高位 0，导致 `负数 > 正数`
- XOR 0x80 将符号位翻转后：
  - 负数的 `1xxx xxxx` → `0xxx xxxx`（变小）
  - 正数的 `0xxx xxxx` → `1xxx xxxx`（变大）
  - 结果：`负数 < 0 < 正数`，字节序与数值序一致

对应代码 (`decode_i64`)：

```rust
fn decode_i64(bytes: &[u8]) -> i64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    buf[0] ^= 0x80; // flip sign bit
    i64::from_be_bytes(buf)
}
```

### 索引 Key 的编码

索引 Key 在 `table_id + _i + index_id` 之后，还会追加索引列的值。每列值按类型编码：

**列类型编码格式**：

| 类型 | 前缀字节 | 后续编码 | 说明 |
|------|---------|---------|------|
| Int (正数) | `0x03` | 8 字节 i64 (符号位翻转) | 整数类型 |
| Int (负数) | `0x04` | 8 字节 i64 (符号位翻转) | 整数类型 |
| Bytes/String | `0x01` | memcomparable bytes 编码 | 变长字符串 |

**示例：解析 UNIQUE KEY `idx_profileid_tag` (`profile_id`, `tag`)**

```
Table: updatelog_esdoc_tagsinfo
CREATE TABLE `updatelog_esdoc_tagsinfo` (
  `profile_id` bigint NOT NULL,
  `tag` varchar(64) NOT NULL,
  UNIQUE KEY `idx_profileid_tag` (`profile_id`,`tag`)
) PARTITION BY HASH (`profile_id`) PARTITIONS 32
```

索引 Key 格式：`t{table_id}_i{index_id} + {profile_id} + {tag}`

实际 hex：

```
74 80 00 00 00 00 00 2e 63 5f 69 80 00 00 00 00 00 00 01 03 80 00 00 00 00 00 10 80 01 32 30 32 35 30 39 5f 32 ff 30 32 35 31 31 5f 75 70 ff 64 61 74 65 00 00 00 00 fb
│  └─── table_id=11875 ──┘ └tag┘ └─ index_id=1 ────┘ │  └─ profile_id=4224 ──┘ │  └──────── tag="202509_202511_update" ──────────┘
t                                                      0x03 (int)                0x01 (bytes)
```

解码结果：

```
key (tidb): table_id=11875, index_id=1
key (index): int=4224, str="202509_202511_update"
```

其中 `profile_id=4224` 实际上是 `_tidb_rowid`（因为该表无显式主键，使用隐式 rowid）。

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

### 索引 Value 的编码

对于 **唯一索引（UNIQUE INDEX）**，TiDB 将 handle (row_id) 存储在索引的 **Value** 中，用于回表查询。

**Value 格式（简化）**：

```
[version_info] + handle (_tidb_rowid) + [restore_data]
```

- **handle**：用于回表，即 `t{table_id}_r{handle}` 的 key
- **restore_data**：存储索引列的原始值（用于 restore 非 binary collation 的字符串）

**实测案例**：

```
val (hex):  08 80 00 02 00 00 00 01 02 02 00 16 00 80 10 32 30 32 35 30 39 5f 32 30 32 35 31 31 5f 75 70 64 61 74 65 00 00 00 00 03 68 7f 8e
val (len):  43 bytes
val (parsed): handle_candidates=[(1, 2199023255554), (9, 566935683088), (35, 230686350)]
val (handle): _tidb_rowid=4224 (at offset 1)
```

解码出的 `_tidb_rowid=4224`，可在 TiDB 中验证：

```sql
mysql> SELECT * FROM updatelog_esdoc_tagsinfo WHERE _tidb_rowid = 4224;
+------------+---------------------------------------+---------------------+---------------------+
| profile_id | tag                                   | gmt_create          | gmt_modified        |
+------------+---------------------------------------+---------------------+---------------------+
|   88851681 | es_main, es_exp: update null location | 2025-11-22 02:42:22 | 2025-11-22 02:42:22 |
+------------+---------------------------------------+---------------------+---------------------+
```

**为什么需要 restore_data？**

对于 `utf8mb4_general_ci` 等非 binary collation，索引 key 中的字符串是按 collation 排序规则编码的（lossy），无法直接还原原始字符串。因此 TiDB 在 value 中额外存储原始字符串用于 `SELECT` 时返回正确值。

对于 `utf8mb4_bin` (binary collation)，key 和 value 中的字符串相同，但 TiDB 仍可能存储以保持格式一致性。

### Record vs Index：Key 的字节序排序

在 TiKV 中，同一 `table_id` 下，**索引 key 会排在行记录 key 之前**，原因是：

```
'_i' (0x69) < '_r' (0x72)
```

因此在按字节序扫描时，会先遇到索引条目，再遇到行记录。

**实测**：

```bash
$ ./target/debug/test_tikv_client 10.0.12.184:2379 --table-id 11875 --limit 5 --txn
Scanning 5 keys for [table_id=11875]...
Found 5 key-value pairs:

--- [0] ---
  key (tidb): table_id=11875, index_id=1  # <- 索引
--- [1] ---
  key (tidb): table_id=11875, index_id=1  # <- 索引
--- [2] ---
  key (tidb): table_id=11875, index_id=1  # <- 索引
...
```

如果只想扫描行记录，使用 `--type record`：

```bash
$ ./target/debug/test_tikv_client 10.0.12.184:2379 --table-id 11875 --txn --type record
```

此时 scan 的起始 key 为 `t{table_id}_r`，跳过了所有索引。

### MVCC 版本号

在 memcomparable 编码的 Key 之后，TiKV 还会追加 8 字节的 MVCC 版本号（时间戳）。

- **RawClient** scan 时这部分会出现在 key 末尾（需要手动解析）
- **TransactionClient** 在内部已处理 MVCC，返回的 key 不带版本号

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

## Value 的行格式

TiDB 的 Value 使用自定义行格式（Row Format v2），将一行的所有列值紧凑编码成字节流。

**编码特点**：

- 每列按类型编码（int、string、datetime 等各有格式）
- 字符串/blob 类型的列值使用 memcomparable bytes 编码
- Value 的二进制数据中，ASCII 字符串会被 `0xff` 标记字节打断

`extract_ascii_strings` 函数在提取可读字符串时会跳过夹在 ASCII 字符之间的 `0xff` 字节，从而还原完整的字符串。

**示例**：

```
val (hex):  ... 32 30 32 35 30 39 5f 32 30 32 35 31 31 5f 75 70 64 61 74 65 ...
            └──── "202509_202511_update" ────┘
```

## 案例演示

### 1. 扫描索引条目并解析字段

**场景**：扫描 TiDB 表 `updatelog_esdoc_tagsinfo` (table_id=11875) 的索引 `idx_profileid_tag`。

**表结构**：

```sql
mysql> SHOW CREATE TABLE updatelog_esdoc_tagsinfo\G
CREATE TABLE `updatelog_esdoc_tagsinfo` (
  `profile_id` bigint NOT NULL,
  `tag` varchar(64) NOT NULL,
  `gmt_create` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `gmt_modified` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `idx_profileid_tag` (`profile_id`,`tag`),
  KEY `idx_tag` (`tag`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY HASH (`profile_id`) PARTITIONS 32
```

**执行**：

```bash
$ ./target/debug/test_tikv_client 10.0.12.184:2379 --table-id 11875 --limit 3 --txn
```

**输出**：

```
=== TiKV Scan Test (TransactionClient) ===
PD endpoints: ["10.0.12.184:2379"]
Filter: table_id=11875
Limit: 3

start_key (hex): 74 80 00 00 00 00 00 2e 63
end_key   (hex): 74 80 00 00 00 00 00 2e 64

Connecting (TransactionClient)...
Connected!

Scanning 3 keys for [table_id=11875]...
Found 3 key-value pairs:

--- [0] ---
  key (hex):  74 80 00 00 00 00 00 2e 63 5f 69 80 00 00 00 00 00 00 01 03 80 00 00 00 00 00 10 80 01 32 30 32 35 30 39 5f 32 ff 30 32 35 31 31 5f 75 70 ff 64 61 74 65 00 00 00 00 fb
  key (tidb): table_id=11875, index_id=1
  key (index): int=4224, str="202509_202511_update"
  val (hex):  08 80 00 02 00 00 00 01 02 02 00 16 00 80 10 32 30 32 35 30 39 5f 32 30 32 35 31 31 5f 75 70 64 61 74 65 00 00 00 00 03 68 7f 8e
  val (len):  43 bytes
  val (parsed): handle_candidates=[(1, 2199023255554), (9, 566935683088), (35, 230686350)]
  val (handle): _tidb_rowid=4224 (at offset 1)
  val (strings): 202509_202511_update

--- [1] ---
  key (hex):  74 80 00 00 00 00 00 2e 63 5f 69 80 00 00 00 00 00 00 01 03 80 00 00 00 00 00 11 40 01 32 30 32 35 30 39 5f 32 ff 30 32 35 31 31 5f 75 70 ff 64 61 74 65 00 00 00 00 fb
  key (tidb): table_id=11875, index_id=1
  key (index): int=4416, str="202509_202511_update"
  val (hex):  08 80 00 02 00 00 00 01 02 02 00 16 00 40 11 32 30 32 35 30 39 5f 32 30 32 35 31 31 5f 75 70 64 61 74 65 00 00 00 00 03 68 77 e6
  val (len):  43 bytes
  val (parsed): handle_candidates=[(1, 2199023255554), (9, 288230376185864960), (35, 230675430)]
  val (handle): _tidb_rowid=4416 (at offset 1)
  val (strings): 202509_202511_update

--- [2] ---
  key (hex):  74 80 00 00 00 00 00 2e 63 5f 69 80 00 00 00 00 00 00 01 03 80 00 00 00 00 00 1e c0 01 32 30 32 35 30 39 5f 32 ff 30 32 35 31 31 5f 75 70 ff 64 61 74 65 00 00 00 00 fb
  key (tidb): table_id=11875, index_id=1
  key (index): int=7872, str="202509_202511_update"
  val (hex):  08 80 00 02 00 00 00 01 02 02 00 16 00 c0 1e 32 30 32 35 30 39 5f 32 30 32 35 31 31 5f 75 70 64 61 74 65 00 00 00 00 03 68 79 31
  val (len):  43 bytes
  val (parsed): handle_candidates=[(1, 2199023255554), (9, 566935683088), (35, 230698289)]
  val (handle): _tidb_rowid=7872 (at offset 1)
  val (strings): 202509_202511_update

=== Done ===
```

**解析结果**：

- `table_id=11875`：表 `updatelog_esdoc_tagsinfo`
- `index_id=1`：第一个索引 `idx_profileid_tag`
- `key (index): int=4224, str="202509_202511_update"`：
  - `int=4224` 实际上是 `_tidb_rowid`（该表无显式主键）
  - `str="202509_202511_update"` 是索引列 `tag` 的值
- `val (handle): _tidb_rowid=4224`：从 value 中解析出的 handle，用于回表

**TiDB 验证**：

```sql
mysql> SELECT * FROM updatelog_esdoc_tagsinfo WHERE _tidb_rowid = 4224;
+------------+---------------------------------------+---------------------+---------------------+
| profile_id | tag                                   | gmt_create          | gmt_modified        |
+------------+---------------------------------------+---------------------+---------------------+
|   88851681 | es_main, es_exp: update null location | 2025-11-22 02:42:22 | 2025-11-22 02:42:22 |
+------------+---------------------------------------+---------------------+---------------------+

mysql> SELECT * FROM updatelog_esdoc_tagsinfo WHERE _tidb_rowid = 4416;
+------------+---------------------------------------+---------------------+---------------------+
| profile_id | tag                                   | gmt_create          | gmt_modified        |
+------------+---------------------------------------+---------------------+---------------------+
|  476918689 | es_main, es_exp: update null location | 2025-11-22 02:42:22 | 2025-11-22 02:42:22 |
+------------+---------------------------------------+---------------------+---------------------+

mysql> SELECT * FROM updatelog_esdoc_tagsinfo WHERE _tidb_rowid = 7872;
+------------+---------------------------------------+---------------------+---------------------+
| profile_id | tag                                   | gmt_create          | gmt_modified        |
+------------+---------------------------------------+---------------------+---------------------+
|  175970688 | es_main, es_exp: update null location | 2025-11-22 02:42:28 | 2025-11-22 02:42:28 |
+------------+---------------------------------------+---------------------+---------------------+
```

完全匹配！从 TiKV 底层解析出的 `_tidb_rowid`，能在 TiDB 中精确查到对应的行。

### 2. 只扫描行记录 (跳过索引)

索引条目和行记录在同一 `table_id` 下，但按字节序排序时，索引 `_i` (0x69) 排在行记录 `_r` (0x72) 之前。

使用 `--type record` 只扫描行记录：

```bash
$ ./target/debug/test_tikv_client 10.0.12.184:2379 --table-id 11875 --limit 3 --txn --type record
```

此时 scan 起始 key 为 `t{table_id}_r`，直接跳过所有索引条目。

### 3. 扫描 TiKV 中的原始 KV 数据 (RawClient)

```bash
$ ./target/debug/test_tikv_client 10.0.12.184:2379
```

输出：

```
=== TiKV Scan Test (RawClient) ===
PD endpoints: ["10.0.12.184:2379"]

Connecting (RawClient)...
Connected!

Scanning 20 keys for [all tables]...
Found 20 key-value pairs:

--- [0] ---
  key (hex):  74 80 00 00 00 00 00 00 ff 18 5f 72 80 00 00 00 00 ff 04 56 4d 00 00 00 00 00 fa f9 9a 79 61 35 cf ff fc
  key (tidb): table_id=24, record, row_id=284237 (+ 8 bytes mvcc)
  val (hex):  08 02 08 a0 97 01 ...
  val (len):  307 bytes
  val (strings): 202509_raw_data_first_insert | s3://openjobs-jobdata-import/multisource/member/202509_parquet/partition_by_column=united_states/part-00001-fec457e0-e4b1-42ab-94b6-8fd0b2ff47bf.c001.gz.parquet
```

注意：RawClient 返回的 key 带 memcomparable 编码和 MVCC 版本号后缀。

### 4. Key 解码过程详解

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

### 5. 通过 TiDB SQL 反查表名

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

### 6. 通过 _tidb_rowid 伪列验证具体行

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

## 底层原理总结

### Key 的排序与扫描顺序

TiKV 中的 key 按字节序排序，因此：

1. **同一 table_id 下，索引排在行记录前**：`_i` (0x69) < `_r` (0x72)
2. **同一索引下，按索引列值排序**：索引列值按 memcomparable 编码后字节序排列
3. **跨 table_id 扫描时，按 table_id 升序**

### 唯一索引的回表流程

1. **查询**：`SELECT * FROM t WHERE unique_col = 'value'`
2. **TiDB 构造索引 key**：`t{table_id}_i{index_id}{value}`
3. **TiKV 返回索引 value**：包含 `handle (_tidb_rowid)`
4. **TiDB 构造行记录 key**：`t{table_id}_r{handle}`
5. **TiKV 返回行记录 value**：完整行数据

### 分区表的 table_id

对于 `PARTITION BY HASH` 的表，每个分区有独立的 `table_id`。

查询分区的 table_id：

```sql
SELECT PARTITION_NAME, TIDB_PARTITION_ID
FROM information_schema.PARTITIONS
WHERE TABLE_NAME = 'updatelog_esdoc_tagsinfo';
```

在 TiKV 中，不同分区的数据分散在不同的 `table_id` 下。

## 函数索引

| 函数 | 作用 |
|------|------|
| `decode_memcomparable_bytes` | 解码 memcomparable bytes 编码，每 9 字节一组（8 数据 + 1 标记） |
| `decode_tidb_key_detailed`   | 详细解码：memcomparable + table_id + tag + row_id/index_id + 索引列 |
| `decode_logical_key_detailed`| 解码逻辑 key（TransactionClient 返回的 key） |
| `decode_index_columns`       | 解码索引 key 中的列值（int、string 等类型） |
| `decode_index_value`         | 解码索引 value 中的 handle (_tidb_rowid) |
| `decode_i64`                 | 解码符号位翻转的 big-endian i64 |
| `extract_ascii_strings`      | 从二进制 value 中提取可读 ASCII 字符串（跳过 0xff 标记） |
| `bytes_to_hex`               | 字节转 hex 字符串显示 |
| `is_index_key`               | 判断 key 是否为索引 key (_i) |

## 参考

- [TiDB Key-Value 映射](https://docs.pingcap.com/tidb/stable/tidb-computing#mapping-table-data-to-key-value)
- [TiKV Rust Client (GitHub)](https://github.com/tikv/client-rust)
- [tikv-client crate (crates.io)](https://crates.io/crates/tikv-client)
