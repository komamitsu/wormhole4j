# Wormhole4j

**Wormhole4j** is a high-performance ordered in-memory index for Java, based on the research paper [*“Wormhole: A Fast Ordered Index for In-memory Data Management”*](https://dl.acm.org/doi/10.1145/3302424.3303955).
It is designed for workloads that require extremely fast point lookups and efficient ordered scans, while also supporting fast inserts and deletes.

## Features

* **Optimized for point lookups and scans**
  * Ultra-fast `get()` for single-key lookups
  * High-performance `scan`, `scanWithExclusiveEndKey()`, and `scanWithInclusiveEndKey()` for full, range, and prefix scans
* **Efficient updates**
  * Fast `put()` (inserts/updates) and `delete()` operations
* **Ordered index for String keys**
  * Currently supports only `String` as the key type

## Quick Start

```java
// Create an index with the default settings
Wormhole<String> wormhole = new Wormhole<>();

// Insert a record
wormhole.put("James", "semaj");

// Get a record
String value = wormhole.get("James"); // returns "semaj"

// Prefix scan
List<KeyValue<String>> prefixResults = wormhole.scanWithCount("Jam", 3);

// Range scan (exclusive end)
List<KeyValue<String>> rangeResults = new ArrayList<>();
wormhole.scanWithExclusiveEndKey("James", "John", rangeResults::add);

// Range scan (inclusive end)
wormhole.scanWithInclusiveEndKey("James", "John", rangeResults::add);

// Full scan
List<KeyValue<String>> fullScanResults = new ArrayList<>();
wormhole.scan(fullScanResults::add);

// Delete a record
wormhole.delete("James");
````

## Future Plans

* **Persistence support** – Add an optional persistent variant of Wormhole.
* **Thread safety** – Provide a thread-safe version for concurrent access.

## License

Apache License 2.0.
