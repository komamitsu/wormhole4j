# Wormhole4j

**Wormhole4j** is a high-performance sorted map for Java 8 or later, based on the research paper [*"Wormhole: A Fast Ordered Index for In-memory Data Management"*](https://dl.acm.org/doi/10.1145/3302424.3303955).
It provides fast range scans and prefix searches, fast point lookups, and competitive update performance, with support for thread-safe concurrent access.

## Features

* Supports `put()`, `get()`, `scan()`, and `delete()` operations for Integer, Long, and String keys
* **[Significantly faster](#benchmark-result) `scan()` API** for full scans, prefix scans, and range scans (inclusive/exclusive) - up to 4x faster than tree-based alternatives
* **[Excellent performance](#benchmark-result) for String keys** - 30-41% faster get/put operations than tree-based structures
* **Faster `get()` for numeric keys** - 26% faster than tree-based structures; competitive `put()` performance
* **[Thread-safe concurrent access](#multi-thread)** via `setConcurrent(true)` in the builder - outperforms `ConcurrentSkipListMap` for Put+Get and String key scans; trade-off on concurrent numeric key scans

## Installation

### Maven

Add the following to your `pom.xml`:

```xml
<dependency>
    <groupId>org.komamitsu</groupId>
    <artifactId>wormhole4j</artifactId>
    <version>0.2.0</version>
</dependency>
```

### Gradle (Groovy DSL)

Add the following to your `build.gradle`:

```groovy
implementation 'org.komamitsu:wormhole4j:0.2.0'
```

### Gradle (Kotlin DSL)

Add the following to your `build.gradle.kts`:

```kotlin
implementation("org.komamitsu:wormhole4j:0.2.0")
```

## Quick Start

```java
// Example with String keys
Wormhole<String, String> wormholeStr = new WormholeBuilder.ForStringKey<String>().build();
wormholeStr.put("James", "semaj");
wormholeStr.put("Joseph", "hpesoj");
wormholeStr.put("John", "nhoj");
wormholeStr.put("Jacob", "bocaj");
wormholeStr.put("Jason", "nosaj");
String value = wormholeStr.get("James"); // returns "semaj"

// Prefix scan
List<KeyValue<String, String>> prefixScanResult = wormholeStr.scanWithCount("Ja", 3);

// Range scan (exclusive end)
List<KeyValue<String, String>> rangeScanResult = new ArrayList<>();
wormholeStr.scan("Ja", "Joseph", true, (k, v) -> {
  rangeScanResult.add(new KeyValue<>(k, v));
  /*
  if (k.equals("unexpected_key")) {
    // If you want to stop the scan, return false.
    return false;
  }
  */
  return true;
});

// Delete a record
wormholeStr.delete("James");

// Example with Integer keys
Wormhole<Integer, String> wormholeInt = new WormholeBuilder.ForIntKey<String>().build();
wormholeInt.put(100, "hundred");

// Example with Long keys
Wormhole<Long, String> wormholeLong = new WormholeBuilder.ForLongKey<String>().build();
wormholeLong.put(9000000000L, "nine billion");
```

## Concurrent Usage

When using Wormhole from multiple threads, enable concurrent mode via the builder.
Each thread must call `registerThread()` before accessing the Wormhole and `unregisterThread()` when done.

```java
Wormhole<String, String> wormhole = new WormholeBuilder.ForStringKey<String>()
    .setConcurrent(true)
    .build();

ExecutorService executor = Executors.newFixedThreadPool(2);
try {
    // Thread 1
    executor.submit(() -> {
        wormhole.registerThread();
        try {
            wormhole.put("James", "semaj");
            wormhole.put("Joseph", "hpesoj");
            wormhole.put("John", "nhoj");
        } finally {
            wormhole.unregisterThread();
        }
    });

    // Thread 2
    executor.submit(() -> {
        wormhole.registerThread();
        try {
            String value = wormhole.get("James"); // returns "semaj"
        } finally {
            wormhole.unregisterThread();
        }
    });
} finally {
    executor.shutdown();
    executor.awaitTermination(60, TimeUnit.SECONDS);
}
```

## Benchmark Result

### Single-thread

The performance of Wormhole4j was evaluated against well-known sorted map implementations.

![Java 21 - Get](./data/benchmark/2026/05/01/bench-java21-get.png)
![Java 21 - Put](./data/benchmark/2026/05/01/bench-java21-put.png)
![Java 21 - Scan](./data/benchmark/2026/05/01/bench-java21-scan.png)
  - GET: 100,000 operations (random lookups from a populated map)
  - PUT: 100,000 operations (random updates with a populated map)
  - SCAN: 10,000 operations (range scans with scan size of 512 records)
- **Key types tested:** Integer, Long, and String
  - String keys: length range 32-256 characters
- **JVM version:** Java 21

#### Comparison Targets

- `java.util.TreeMap` - Red-Black tree from Java standard library
- `it.unimi.dsi.fastutil.objects.Object2ObjectAVLTreeMap` - AVL tree from `it.unimi.dsi:fastutil:8.5.16`

#### Key Findings

**Scan** is Wormhole's strongest suit: up to 4x faster than the AVL tree and 2x faster than `TreeMap` for numeric keys, and 40% faster than `TreeMap` for string keys. This reflects the core advantage of the Wormhole index structure for range traversal.

**String key get/put** is 30-41% faster than both tree implementations, likely due to Wormhole's ability to exploit common key prefixes and avoid redundant character comparisons.

**Numeric key get** shows a clear advantage (+26% over both trees). **Numeric key put** is roughly on par with `TreeMap` and marginally ahead of the AVL tree — within the noise for small datasets.

### Multi-thread

The concurrent performance of Wormhole4j was evaluated with simultaneous read and write operations across varying thread counts.

![Java 21 - Put and Get (IntKey)](./data/benchmark/2026/05/01/bench-java21-mt-putandget-intkey.png)
![Java 21 - Put and Get (LongKey)](./data/benchmark/2026/05/01/bench-java21-mt-putandget-longkey.png)
![Java 21 - Put and Get (StringKey)](./data/benchmark/2026/05/01/bench-java21-mt-putandget-stringkey.png)
![Java 21 - Put and Scan (IntKey)](./data/benchmark/2026/05/01/bench-java21-mt-putandscan-intkey.png)
![Java 21 - Put and Scan (LongKey)](./data/benchmark/2026/05/01/bench-java21-mt-putandscan-longkey.png)
![Java 21 - Put and Scan (StringKey)](./data/benchmark/2026/05/01/bench-java21-mt-putandscan-stringkey.png)

#### Benchmark Configuration

- **Record count:** 100,000 records
- **Scenarios:** N PUT threads and N GET (or SCAN) threads running concurrently, where N = 1, 2, 4, 8
- **Key types tested:** Integer, Long, and String
  - String keys: length range 32-256 characters
- **JVM version:** Java 21

#### Comparison Target

- `java.util.concurrent.ConcurrentSkipListMap` - Concurrent sorted map from Java standard library

#### Key Findings

**Put + Get:** Wormhole consistently outperforms `ConcurrentSkipListMap` across all thread counts and key types. The advantage is largest for String keys (up to 2x faster at 1+1 threads, 56% faster at 8+8) and solid for numeric keys (15-48% faster depending on thread count).

**Put + Scan (PUT throughput):** Similarly strong gains, especially for String keys (up to 2.3x faster PUT at 1+1 threads, 84% faster combined at 8+8).

**Put + Scan (SCAN throughput):** Results are mixed. For String keys, Wormhole is faster across all thread counts (30-89%). For Integer and Long keys, `ConcurrentSkipListMap` has a SCAN advantage (38-48% faster) under concurrent write pressure — a trade-off to be aware of for scan-heavy workloads with numeric keys.

**Scalability:** Both implementations scale roughly linearly from 1+1 to 4+4 threads (around 3x throughput gain) and flatten similarly beyond that. Wormhole's throughput advantage is maintained at all tested thread counts.

## Future Plans

* **Persistence support** – Add an optional persistent variant of Wormhole.
* **Further optimization**

## License

Apache License 2.0.
