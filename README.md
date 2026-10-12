# RedisLite Engine

A single-file, console-based Java key-value store with a background TTL worker and an LRU capacity limit of five entries.

## Requirements

- Java 8 or newer

## Run

```sh
javac RedisLiteEngine.java
java RedisLiteEngine
```

## Commands

```text
SET [key] [value]
SETEX [key] [value] [ttl_in_seconds]
GET [key]
STATUS
EXIT
```

Values are single whitespace-free tokens. `STATUS` reports active keys, approximate UTF-8 payload bytes, and LRU order from least to most recently used. The TTL worker scans once per second and prints a message for each expired key. When a new key exceeds capacity, the least recently used key is evicted and reported.
