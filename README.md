# NativeDumper

Library for read and write Native format between ClickHouse and file

## Features

- Read/write data between ClickHouse and Native format files
- Transfer data directly between different ClickHouse servers
- Stream processing with minimal memory footprint
- Support for BINARY and CSV formats
- Multiple compression methods (ZSTD, GZIP, LZ4)
- Pandas and Polars integration
- Debug mode with query execution plans
- Read-only mode detection

## Installation

### From pip

```bash
pip install native-dumper -U --index-url https://dns-technologies.github.io/dbhose-dev-pip/simple/
```

### From local directory

```bash
pip install . --extra-index-url https://dns-technologies.github.io/dbhose-dev-pip/simple/
```

### From git

```bash
pip install git+https://github.com/dns-technologies/native_dumper --extra-index-url https://dns-technologies.github.io/dbhose-dev-pip/simple/
```

## Quick Start

### Initialization

```python
from native_dumper import (
    CompressionMethod,
    CHConnector,
    NativeDumper,
)

connector = CHConnector(
    host="localhost",
    dbname="default",
    user="default",
    password="",
    port=8123,
)

dumper = NativeDumper(
    connector=connector,
    compression_method=CompressionMethod.ZSTD,
    compression_level=3,
    dump_format=DumpFormat.BINARY,
    mode=DumperMode.PROD,
)
```

### Read dump from ClickHouse into file

```python
file_name = "dump.native"
# use either query or table_name
query = "SELECT * FROM users WHERE age > 21"
table_name = "default.users"

with open(file_name, "wb") as fileobj:
    dumper.read_dump(fileobj, query=query, table_name=table_name)
```

### Write dump from file into ClickHouse

```python
file_name = "dump.native"
table_name = "default.users_copy"

with open(file_name, "rb") as fileobj:
    dumper.write_dump(fileobj, table_name)
```

### Transfer data between ClickHouse servers

**Same server:**

```python
dumper.write_between(
    table_dest="default.users_backup",
    table_src="default.users",
)
```

**Different servers:**

```python
connector_src = CHConnector(
    host="source-host",
    dbname="default",
    user="default",
    password="",
    port=8123,
)

dumper_src = NativeDumper(connector=connector_src)

dumper.write_between(
    table_dest="default.users_copy",
    table_src="default.users",
    dumper_src=dumper_src,
)
```

### Stream reader

```python
reader = dumper.to_reader(table_name="default.users")

# Get as Python rows generator
for row in reader.to_rows():
    print(row)

# Get as pandas DataFrame
df = reader.to_pandas()

# Get as polars DataFrame
pl_df = reader.to_polars()
```

### Write from Python objects

```python
# From rows (tuples or lists)
rows = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
dumper.from_rows(rows, "default.users")

# From pandas DataFrame
import pandas as pd
df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
dumper.from_pandas(df, "default.users")

# From polars DataFrame
import polars as pl
pl_df = pl.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
dumper.from_polars(pl_df, "default.users")
```

### Get table metadata

```python
# Get metadata as DBMetadata object
metadata = dumper.metadata(table_name="default.users")
print(metadata.columns)  # OrderedDict with column names and types
print(metadata.version)  # ClickHouse version

# Get raw metadata (for internal use)
raw_metadata = dumper.metadata(table_name="default.users", reader_meta=True)
```

## Configuration

### Compression Methods

| Method | Description |
|--------|-------------|
| `CompressionMethod.NONE` | No compression |
| `CompressionMethod.GZIP` | GZIP compression |
| `CompressionMethod.LZ4` | LZ4 compression (fast) |
| `CompressionMethod.ZSTD` | Zstandard compression (default, best ratio) |

*Note: Snappy compression is not supported by ClickHouse.*

### Dump Formats

| Format | Description |
|--------|-------------|
| `DumpFormat.BINARY` | ClickHouse Native binary format (default) |
| `DumpFormat.CSV` | CSV format with type preservation |

### Dumper Modes

| Mode | Description |
|------|-------------|
| `DumperMode.PROD` | Production mode - normal operation |
| `DumperMode.DEBUG` | Debug mode - shows query execution plans |
| `DumperMode.TEST` | Test mode - validates without writing data |

## Class Reference

### NativeDumper

Main class for database operations.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `connector` | `CHConnector` | required | ClickHouse connection parameters |
| `compression_method` | `CompressionMethod` | `ZSTD` | Compression algorithm |
| `compression_level` | `int` | `3` | Compression level (1-9 for ZSTD/GZIP/LZ4) |
| `logger` | `Logger` | `None` | Custom logger instance |
| `timeout` | `int` | `300` | Request timeout in seconds |
| `isolation` | `IsolationLevel` | `committed` | Not used (ClickHouse has no transactions) |
| `mode` | `DumperMode` | `PROD` | Dumper operation mode |
| `dump_format` | `DumpFormat` | `BINARY` | Output format |
| `s3_file` | `bool` | `False` | S3 streaming mode (not yet implemented) |

**Properties:**

| Property | Description |
|----------|-------------|
| `timeout` | Get/set request timeout |
| `compression_level` | Get/set compression level |
| `dump_format` | Get/set dump format |
| `is_readonly` | Check if connected to read-only server |

**Methods:**

| Method | Description |
|--------|-------------|
| `read_dump(fileobj, query, table_name)` | Read data to file |
| `write_dump(fileobj, table_name)` | Write data from file |
| `write_between(table_dest, table_src, query_src, dumper_src)` | Transfer between servers |
| `to_reader(query, table_name)` | Get stream reader |
| `from_rows(rows, table_name)` | Write from Python rows |
| `from_pandas(df, table_name)` | Write from pandas DataFrame |
| `from_polars(df, table_name)` | Write from polars DataFrame |
| `from_bytes(bytes_data, table_name)` | Write from bytes chunks |
| `metadata(query, table_name, reader_meta)` | Get table metadata |
| `refresh()` | Refresh session ID |
| `close()` | Close connection |

### CHConnector

Connection parameters container.

```python
CHConnector(
    host="localhost",
    port=8123,
    user="default",
    password="",
    dbname="default",
)
```

## Debug Mode Features

When `mode=DumperMode.DEBUG`, the dumper provides:

- **Query execution plans** - Detailed query performance statistics
- **Response headers info** - ClickHouse server response metadata

## Dependencies

- Python >= 3.10
- `base_dumper` >= 0.2.0.dev5
- `nativelib` >= 0.2.5.dev1
- `csvpack` >= 0.1.0.dev5
- `light-compressor` >= 0.1.1.dev2

## License

MIT
