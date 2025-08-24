# Wormhole4j

**Wormhole4j** is a high-performance ordered in-memory index for Java, based on the research paper [*“Wormhole: A Fast Ordered Index for In-memory Data Management”*](https://dl.acm.org/doi/10.1145/3302424.3303955).
It is designed for workloads that require extremely fast point lookups and efficient ordered scans, while also supporting fast inserts and deletes.

## Features

* **Extremely fast `scan()` API** for full scans, prefix scans, and range scans (inclusive/exclusive)
* **Ultra-fast `get()` API** for point lookups
* Fast `put()` and `delete()` operations

## Current limitations

* Supports only `String` keys
* Not thread-safe

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

// Delete a record
wormhole.delete("James");
```

## Future Plans

* **Persistence support** – Add an optional persistent variant of Wormhole.
* **Thread safety** – Provide a thread-safe version for concurrent access.

## License

Apache License 2.0.
