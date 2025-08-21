# Wormhole4j

**Wormhole4j** is a high-performance ordered in-memory index for Java, based on the research paper *“Wormhole: A Fast Ordered Index for In-memory Data Management.”*
It is designed for workloads requiring extremely fast point lookups, efficient inserts, and ordered scans.

## Features

* **Ordered index for String keys** (currently only `String` is supported as the key type)
* High-performance `put`, `get`, and `delete`
* Efficient **prefix scans** and **range scans** for ordered traversal
* In-memory data structure (persistent variants may come in the future)

## Quick Start

```java
// Create an index with a leaf node size of 128
Wormhole<String> wormhole = new Wormhole<>(128, true);

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

## License

Apache License 2.0.
