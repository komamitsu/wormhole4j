package org.komamitsu.wormhole;

import it.unimi.dsi.fastutil.objects.Object2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import org.junit.jupiter.api.Test;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.komamitsu.wormhole.TestHelpers.genRandomKey;

class Benchmark {
  private static final String PROP_PREFIX = "wormhole4j.benchmark.";
  private static final String PROP_MAX_KEY_LENGTH = PROP_PREFIX + "max_key_length";
  private static final String PROP_RECORD_COUNT = PROP_PREFIX + "record_count";
  private static final String PROP_WARMUP_COUNT = PROP_PREFIX + "warmup_count";
  private static final String PROP_ATTEMPT_COUNT = PROP_PREFIX + "attempt_count";
  private static final String PROP_MAX_SCAN_SIZE = PROP_PREFIX + "max_scan_size";
  private static final String DEFAULT_MAX_KEY_LENGTH = "64";
  private static final String DEFAULT_RECORD_COUNT = "100000";
  private static final String DEFAULT_WARMUP_COUNT = "4";
  private static final String DEFAULT_ATTEMPT_COUNT = "4";
  private static final String DEFAULT_MAX_SCAN_SIZE = "1024";
  private final int maxKeyLength;
  private final int recordCount;
  private final int warmupCount;
  private final int attemptCount;
  private final int maxScanSize;

  public Benchmark() {
    this.maxKeyLength = Integer.parseInt(System.getProperty(PROP_MAX_KEY_LENGTH, DEFAULT_MAX_KEY_LENGTH));
    this.recordCount = Integer.parseInt(System.getProperty(PROP_RECORD_COUNT, DEFAULT_RECORD_COUNT));
    this.warmupCount = Integer.parseInt(System.getProperty(PROP_WARMUP_COUNT, DEFAULT_WARMUP_COUNT));
    this.attemptCount = Integer.parseInt(System.getProperty(PROP_ATTEMPT_COUNT, DEFAULT_ATTEMPT_COUNT));
    this.maxScanSize = Integer.parseInt(System.getProperty(PROP_MAX_SCAN_SIZE, DEFAULT_MAX_SCAN_SIZE));
  }

  @FunctionalInterface
  private interface ThrowableRunnable<E extends Throwable> {
    void run() throws E, IOException;
  }

  private <E extends Throwable> long measure(ThrowableRunnable<E> task) throws E, IOException {
    long startMillis = System.currentTimeMillis();
    task.run();
    long durationMillis = System.currentTimeMillis() - startMillis;
    System.gc();
    try {
      TimeUnit.MILLISECONDS.sleep(500);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    return durationMillis;
  }

  private interface TestCase<T, E extends Throwable> {
    String label();

    int count();

    T init() throws E, IOException;

    ThrowableRunnable<E> createTask(T resource);

    default void release(T resource) {}
  }

  <T, E extends Throwable> void execute(TestCase<T, E> testCase) throws Throwable {
    T resource = testCase.init();

    try {
      System.out.println("----------------------------------------------------------------");
      System.out.printf("Starting: %s%n", testCase.label());

      // Warmups
      for (int i = 0; i < warmupCount; i++) {
        long durationMillis = measure(testCase.createTask(resource));
        long throughput = testCase.count() * 1000L / durationMillis;
        System.out.printf("Warmup #%d: %d per second%n", i, throughput);
      }

      // Attempts
      List<Long> throughputs = new ArrayList<>();
      for (int i = 0; i < attemptCount; i++) {
        long durationMillis = measure(testCase.createTask(resource));
        long throughput = testCase.count() * 1000L / durationMillis;
        throughputs.add(throughput);
        System.out.printf("Attempt #%d: %d per second%n", i, throughput);
      }
      long averageThroughput = throughputs.stream().reduce(0L, Long::sum) / throughputs.size();
      double stdDev = Math.sqrt(
          throughputs.stream()
              .map(throughput -> (long) Math.pow(throughput - averageThroughput, 2.0))
              .reduce(0L, Long::sum));
      System.out.printf("Average throughput: %d per second%n", averageThroughput);
      System.out.printf("StdDev: %f per second%n", stdDev);
    }
    finally {
      testCase.release(resource);
    }
  }

  @Test
  void insertToWormhole() throws Throwable {
    execute(
        new TestCase<List<String>, RuntimeException>() {
          @Override
          public String label() {
            return "Insert to Wormhole";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public List<String> init() {
            List<String> keys = new ArrayList<>(recordCount);
            for (int i = 0; i < recordCount; i++) {
              keys.add(genRandomKey(maxKeyLength));
            }
            return keys;
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(List<String> keys) {
            return () -> {
              Wormhole<Integer> wormhole = new Wormhole<>();
              for (int i = 0; i < recordCount; i++) {
                wormhole.put(keys.get(i), i);
              }
            };
          }
        }
    );
  }

  @Test
  void insertToRedBlackTreeMap() throws Throwable {
    execute(
        new TestCase<List<String>, RuntimeException>() {
          @Override
          public String label() {
            return "Insert to Red-Black tree (TreeMap)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public List<String> init() {
            List<String> keys = new ArrayList<>(recordCount);
            for (int i = 0; i < recordCount; i++) {
              keys.add(genRandomKey(maxKeyLength));
            }
            return keys;
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(List<String> keys) {
            return () -> {
              TreeMap<String, Integer> map = new TreeMap<>();
              for (int i = 0; i < recordCount; i++) {
                map.put(keys.get(i), i);
              }
            };
          }
        }
    );
  }

  @Test
  void insertToAVLTreeMap() throws Throwable {
    execute(
        new TestCase<List<String>, RuntimeException>() {
          @Override
          public String label() {
            return "Insert to AVL tree map";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public List<String> init() {
            List<String> keys = new ArrayList<>(recordCount);
            for (int i = 0; i < recordCount; i++) {
              keys.add(genRandomKey(maxKeyLength));
            }
            return keys;
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(List<String> keys) {
            return () -> {
              Object2ObjectSortedMap<String, Integer> map = new Object2ObjectAVLTreeMap<>();
              for (int i = 0; i < recordCount; i++) {
                map.put(keys.get(i), i);
              }
            };
          }
        }
    );
  }

  @Test
  void insertToBPlusTree() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<DB>, RuntimeException>() {
          @Override
          public String label() {
            return "Insert to in-memory B+Tree (MapDB)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public ResourceAndKeys<DB> init() {
            List<String> keys = new ArrayList<>(recordCount);
            DB db = DBMaker.memoryDB().make();
            for (int i = 0; i < recordCount; i++) {
              keys.add(genRandomKey(maxKeyLength));
            }
            return new ResourceAndKeys<>(db, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(ResourceAndKeys<DB> resourceAndKeys) {
            return () -> {
              BTreeMap<String, Integer> map = resourceAndKeys.resource.treeMap("map")
                  .keySerializer(Serializer.STRING)
                  .valueSerializer(Serializer.INTEGER)
                  .createOrOpen();

              for (int i = 0; i < recordCount; i++) {
                map.put(resourceAndKeys.keys.get(i), i);
              }
            };
          }

          @Override
          public void release(ResourceAndKeys<DB> resource) {
            resource.resource.close();
          }
        }
    );
  }

  private static class ResourceAndKeys<T> {
    private final T resource;
    private final List<String> keys;

    public ResourceAndKeys(T resource, List<String> keys) {
      this.resource = resource;
      this.keys = keys;
    }
  }

  @Test
  void getFromWormhole() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<Wormhole<Integer>>, RuntimeException>() {
          @Override
          public String label() {
            return "Get from Wormhole";
          }

          @Override
          public int count() {
            return recordCount * 2;
          }

          @Override
          public ResourceAndKeys<Wormhole<Integer>> init() {
            List<String> keys = new ArrayList<>(recordCount);
            Wormhole<Integer> wormhole = new Wormhole<>();
            for (int i = 0; i < recordCount; i++) {
              String key = genRandomKey(maxKeyLength);
              keys.add(key);
              wormhole.put(key, i);
            }
            return new ResourceAndKeys<>(wormhole, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(ResourceAndKeys<Wormhole<Integer>> resourceAndKeys) {
            return () -> {
              Wormhole<Integer> wormhole = resourceAndKeys.resource;
              List<String> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex = ThreadLocalRandom.current().nextInt(recordCount);
                wormhole.get(keys.get(keyIndex));
              }
            };
          }
        }
    );
  }

  @Test
  void getFromRedBlackTreeMap() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<TreeMap<String, Integer>>, RuntimeException>() {
          @Override
          public String label() {
            return "Get from Red-Black tree (TreeMap)";
          }

          @Override
          public int count() {
            return recordCount * 2;
          }

          @Override
          public ResourceAndKeys<TreeMap<String, Integer>> init() {
            List<String> keys = new ArrayList<>(recordCount);
            TreeMap<String, Integer> map = new TreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              String key = genRandomKey(maxKeyLength);
              keys.add(key);
              map.put(key, i);
            }
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(ResourceAndKeys<TreeMap<String, Integer>> resourceAndKeys) {
            return () -> {
              TreeMap<String, Integer> map = resourceAndKeys.resource;
              List<String> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex = ThreadLocalRandom.current().nextInt(recordCount);
                map.get(keys.get(keyIndex));
              }
            };
          }
        }
    );
  }

  @Test
  void getFromAVLTreeMap() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<Object2ObjectSortedMap<String, Integer>>, RuntimeException>() {
          @Override
          public String label() {
            return "Get from AVL tree map";
          }

          @Override
          public int count() {
            return recordCount * 2;
          }

          @Override
          public ResourceAndKeys<Object2ObjectSortedMap<String, Integer>> init() {
            List<String> keys = new ArrayList<>(recordCount);
            Object2ObjectSortedMap<String, Integer> map = new Object2ObjectAVLTreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              String key = genRandomKey(maxKeyLength);
              keys.add(key);
              map.put(key, i);
            }
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(ResourceAndKeys<Object2ObjectSortedMap<String, Integer>> resourceAndKeys) {
            return () -> {
              Object2ObjectSortedMap<String, Integer> map = resourceAndKeys.resource;
              List<String> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex = ThreadLocalRandom.current().nextInt(recordCount);
                map.get(keys.get(keyIndex));
              }
            };
          }
        }
    );
  }

  @Test
  void getFromBPlusTree() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<Tuple<DB, BTreeMap<String, Integer>>>, RuntimeException>() {
          @Override
          public String label() {
            return "Get from in-memory B+Tree (MapDB)";
          }

          @Override
          public int count() {
            return recordCount * 2;
          }

          @Override
          public ResourceAndKeys<Tuple<DB, BTreeMap<String, Integer>>> init() {
            List<String> keys = new ArrayList<>(recordCount);

            DB db = DBMaker.memoryDB().make();
            BTreeMap<String, Integer> map = db.treeMap("map")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.INTEGER)
                .createOrOpen();

            for (int i = 0; i < recordCount; i++) {
              String key = genRandomKey(maxKeyLength);
              keys.add(key);
              map.put(key, i);
            }
            return new ResourceAndKeys<>(new Tuple<>(db, map), keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(ResourceAndKeys<Tuple<DB, BTreeMap<String, Integer>>> resourceAndKeys) {
            return () -> {
              BTreeMap<String, Integer> map = resourceAndKeys.resource.second;
              List<String> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex = ThreadLocalRandom.current().nextInt(recordCount);
                map.get(keys.get(keyIndex));
              }
            };
          }

          @Override
          public void release(ResourceAndKeys<Tuple<DB, BTreeMap<String, Integer>>> resource) {
            resource.resource.first.close();
          }
        }
    );
  }

  @Test
  void scanFromWormhole() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<Wormhole<Integer>>, RuntimeException>() {
          @Override
          public String label() {
            return "Scan from Wormhole";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public ResourceAndKeys<Wormhole<Integer>> init() {
            List<String> keys = new ArrayList<>(recordCount);
            Wormhole<Integer> wormhole = new Wormhole<>();
            for (int i = 0; i < recordCount; i++) {
              String key = genRandomKey(maxKeyLength);
              keys.add(key);
              wormhole.put(key, i);
            }
            Collections.sort(keys);
            return new ResourceAndKeys<>(wormhole, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(ResourceAndKeys<Wormhole<Integer>> resourceAndKeys) {
            return () -> {
              Wormhole<Integer> wormhole = resourceAndKeys.resource;
              List<String> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex1 = ThreadLocalRandom.current().nextInt(recordCount);
                int keyIndex2 = Math.min(keys.size() -1, keyIndex1 + ThreadLocalRandom.current().nextInt(maxScanSize));
                String key1 = keys.get(keyIndex1);
                String key2 = keys.get(keyIndex2);
                wormhole.scanWithExclusiveEndKey(key1, key2, kv -> true);
              }
            };
          }
        }
    );
  }

  @Test
  void scanFromRedBlackTreeMap() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<TreeMap<String, Integer>>, RuntimeException>() {
          @Override
          public String label() {
            return "Scan from Red-Black tree (TreeMap)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public ResourceAndKeys<TreeMap<String, Integer>> init() {
            List<String> keys = new ArrayList<>(recordCount);
            TreeMap<String, Integer> map = new TreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              String key = genRandomKey(maxKeyLength);
              keys.add(key);
              map.put(key, i);
            }
            Collections.sort(keys);
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(ResourceAndKeys<TreeMap<String, Integer>> resourceAndKeys) {
            return () -> {
              TreeMap<String, Integer> map = resourceAndKeys.resource;
              List<String> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex1 = ThreadLocalRandom.current().nextInt(recordCount);
                int keyIndex2 = Math.min(keys.size() -1, keyIndex1 + ThreadLocalRandom.current().nextInt(maxScanSize));
                String key1 = keys.get(keyIndex1);
                String key2 = keys.get(keyIndex2);
                for (Map.Entry<String, Integer> ignored: map.subMap(key1, key2).entrySet()) {
                  // Nothing to do.
                }
              }
            };
          }
        }
    );
  }

  @Test
  void scanFromAVLTreeMap() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<Object2ObjectSortedMap<String, Integer>>, RuntimeException>() {
          @Override
          public String label() {
            return "Scan from AVL tree map";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public ResourceAndKeys<Object2ObjectSortedMap<String, Integer>> init() {
            List<String> keys = new ArrayList<>(recordCount);
            Object2ObjectSortedMap<String, Integer> map = new Object2ObjectAVLTreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              String key = genRandomKey(maxKeyLength);
              keys.add(key);
              map.put(key, i);
            }
            Collections.sort(keys);
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(ResourceAndKeys<Object2ObjectSortedMap<String, Integer>> resourceAndKeys) {
            return () -> {
              Object2ObjectSortedMap<String, Integer> map = resourceAndKeys.resource;
              List<String> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex1 = ThreadLocalRandom.current().nextInt(recordCount);
                int keyIndex2 = Math.min(keys.size() -1, keyIndex1 + ThreadLocalRandom.current().nextInt(maxScanSize));
                String key1 = keys.get(keyIndex1);
                String key2 = keys.get(keyIndex2);
                for (Map.Entry<String, Integer> ignored: map.subMap(key1, key2).entrySet()) {
                  // Nothing to do.
                }
              }
            };
          }
        }
    );
  }

  @Test
  void scanFromBPlusTree() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<Tuple<DB, BTreeMap<String, Integer>>>, RuntimeException>() {
          @Override
          public String label() {
            return "Scan from in-memory B+Tree (MapDB)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public ResourceAndKeys<Tuple<DB, BTreeMap<String, Integer>>> init() {
            List<String> keys = new ArrayList<>(recordCount);
            DB db = DBMaker.memoryDB().make();
            BTreeMap<String, Integer> map = db.treeMap("map")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.INTEGER)
                .createOrOpen();

            for (int i = 0; i < recordCount; i++) {
              String key = genRandomKey(maxKeyLength);
              keys.add(key);
              map.put(key, i);
            }
            Collections.sort(keys);
            return new ResourceAndKeys<>(new Tuple<>(db, map), keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(ResourceAndKeys<Tuple<DB, BTreeMap<String, Integer>>> resourceAndKeys) {
            return () -> {
              BTreeMap<String, Integer> map = resourceAndKeys.resource.second;
              List<String> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex1 = ThreadLocalRandom.current().nextInt(recordCount);
                int keyIndex2 = Math.min(keys.size() -1, keyIndex1 + ThreadLocalRandom.current().nextInt(maxScanSize));
                String key1 = keys.get(keyIndex1);
                String key2 = keys.get(keyIndex2);
                for (Map.Entry<String, Integer> ignored: map.subMap(key1, key2).entrySet()) {
                  // Nothing to do.
                }
              }
            };
          }

          @Override
          public void release(ResourceAndKeys<Tuple<DB, BTreeMap<String, Integer>>> resource) {
            resource.resource.first.close();
          }
        }
    );
  }
}
