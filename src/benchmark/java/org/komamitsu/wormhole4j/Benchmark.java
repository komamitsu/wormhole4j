/*
 * Copyright 2025 Mitsunori Komatsu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.komamitsu.wormhole4j;

import it.unimi.dsi.fastutil.objects.Object2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class Benchmark {
  private static final String PROP_PREFIX = "wormhole4j.benchmark.";
  private static final String PROP_MIN_KEY_LENGTH = PROP_PREFIX + "min_key_length";
  private static final String PROP_MAX_KEY_LENGTH = PROP_PREFIX + "max_key_length";
  private static final String PROP_RECORD_COUNT = PROP_PREFIX + "record_count";
  private static final String PROP_WARMUP_COUNT = PROP_PREFIX + "warmup_count";
  private static final String PROP_ATTEMPT_COUNT = PROP_PREFIX + "attempt_count";
  private static final String PROP_MAX_SCAN_SIZE = PROP_PREFIX + "max_scan_size";
  private static final String DEFAULT_MIN_KEY_LENGTH = "8";
  private static final String DEFAULT_MAX_KEY_LENGTH = "128";
  private static final String DEFAULT_RECORD_COUNT = "100000";
  private static final String DEFAULT_WARMUP_COUNT = "5";
  private static final String DEFAULT_ATTEMPT_COUNT = "5";
  private static final String DEFAULT_MAX_SCAN_SIZE = "512";
  private final int minKeyLength;
  private final int maxKeyLength;
  private final int recordCount;
  private final int warmupCount;
  private final int attemptCount;
  private final int maxScanSize;

  public Benchmark() {
    this.minKeyLength =
        Integer.parseInt(System.getProperty(PROP_MIN_KEY_LENGTH, DEFAULT_MIN_KEY_LENGTH));
    this.maxKeyLength =
        Integer.parseInt(System.getProperty(PROP_MAX_KEY_LENGTH, DEFAULT_MAX_KEY_LENGTH));
    this.recordCount =
        Integer.parseInt(System.getProperty(PROP_RECORD_COUNT, DEFAULT_RECORD_COUNT));
    this.warmupCount =
        Integer.parseInt(System.getProperty(PROP_WARMUP_COUNT, DEFAULT_WARMUP_COUNT));
    this.attemptCount =
        Integer.parseInt(System.getProperty(PROP_ATTEMPT_COUNT, DEFAULT_ATTEMPT_COUNT));
    this.maxScanSize =
        Integer.parseInt(System.getProperty(PROP_MAX_SCAN_SIZE, DEFAULT_MAX_SCAN_SIZE));
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

    default boolean initForEachAttempt() {
      return false;
    }

    T init() throws E, IOException;

    ThrowableRunnable<E> createTask(T resource);
  }

  <T, E extends Throwable> void execute(TestCase<T, E> testCase) throws Throwable {
    T resource = null;
    if (!testCase.initForEachAttempt()) {
      resource = testCase.init();
    }

    System.out.println("----------------------------------------------------------------");
    System.out.printf("Starting: %s%n", testCase.label());

    // Warmups
    for (int i = 0; i < warmupCount; i++) {
      if (testCase.initForEachAttempt()) {
        resource = testCase.init();
      }
      long durationMillis = measure(testCase.createTask(resource));
      long throughput = testCase.count() * 1000L / durationMillis;
      System.out.printf("Warmup #%d: %d per second%n", i, throughput);
    }

    // Attempts
    List<Long> throughputs = new ArrayList<>();
    for (int i = 0; i < attemptCount; i++) {
      if (testCase.initForEachAttempt()) {
        resource = testCase.init();
      }
      long durationMillis = measure(testCase.createTask(resource));
      long throughput = testCase.count() * 1000L / durationMillis;
      throughputs.add(throughput);
      System.out.printf("Attempt #%d: %d per second%n", i, throughput);
    }
    long averageThroughput = throughputs.stream().reduce(0L, Long::sum) / throughputs.size();
    double stdDev =
        Math.sqrt(
            (double)
                    throughputs.stream()
                        .map(throughput -> (long) Math.pow(throughput - averageThroughput, 2.0))
                        .reduce(0L, Long::sum)
                / throughputs.size());
    System.out.printf("Average throughput: %d per second%n", averageThroughput);
    System.out.printf("StdDev: %f per second%n", stdDev);
  }

  @Test
  void insertToWormhole() throws Throwable {
    execute(
        new TestCase<List<String>, RuntimeException>() {
          @Override
          public String label() {
            return "Insert to Wormhole (Wormhole4j)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public List<String> init() {
            List<String> keys = new ArrayList<>(recordCount);
            for (int i = 0; i < recordCount; i++) {
              keys.add(TestHelpers.genRandomKey(minKeyLength, maxKeyLength));
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
        });
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
              keys.add(TestHelpers.genRandomKey(minKeyLength, maxKeyLength));
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
        });
  }

  @Test
  void insertToAVLTreeMap() throws Throwable {
    execute(
        new TestCase<List<String>, RuntimeException>() {
          @Override
          public String label() {
            return "Insert to AVL tree map (Fastutil)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public List<String> init() {
            List<String> keys = new ArrayList<>(recordCount);
            for (int i = 0; i < recordCount; i++) {
              keys.add(TestHelpers.genRandomKey(minKeyLength, maxKeyLength));
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
        });
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
            return "Get from Wormhole (Wormhole4j)";
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
              String key = TestHelpers.genRandomKey(minKeyLength, maxKeyLength);
              keys.add(key);
              wormhole.put(key, i);
            }
            return new ResourceAndKeys<>(wormhole, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<Wormhole<Integer>> resourceAndKeys) {
            return () -> {
              Wormhole<Integer> wormhole = resourceAndKeys.resource;
              List<String> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex = ThreadLocalRandom.current().nextInt(recordCount);
                wormhole.get(keys.get(keyIndex));
              }
            };
          }
        });
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
              String key = TestHelpers.genRandomKey(minKeyLength, maxKeyLength);
              keys.add(key);
              map.put(key, i);
            }
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<TreeMap<String, Integer>> resourceAndKeys) {
            return () -> {
              TreeMap<String, Integer> map = resourceAndKeys.resource;
              List<String> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex = ThreadLocalRandom.current().nextInt(recordCount);
                map.get(keys.get(keyIndex));
              }
            };
          }
        });
  }

  @Test
  void getFromAVLTreeMap() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<Object2ObjectSortedMap<String, Integer>>, RuntimeException>() {
          @Override
          public String label() {
            return "Get from AVL tree map (Fastutil)";
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
              String key = TestHelpers.genRandomKey(minKeyLength, maxKeyLength);
              keys.add(key);
              map.put(key, i);
            }
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<Object2ObjectSortedMap<String, Integer>> resourceAndKeys) {
            return () -> {
              Object2ObjectSortedMap<String, Integer> map = resourceAndKeys.resource;
              List<String> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex = ThreadLocalRandom.current().nextInt(recordCount);
                map.get(keys.get(keyIndex));
              }
            };
          }
        });
  }

  @Test
  void updateWormhole() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<Wormhole<Integer>>, RuntimeException>() {
          @Override
          public String label() {
            return "Update Wormhole (Wormhole4j)";
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
              String key = TestHelpers.genRandomKey(minKeyLength, maxKeyLength);
              keys.add(key);
              wormhole.put(key, i);
            }
            return new ResourceAndKeys<>(wormhole, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<Wormhole<Integer>> resourceAndKeys) {
            return () -> {
              Wormhole<Integer> wormhole = resourceAndKeys.resource;
              List<String> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex = ThreadLocalRandom.current().nextInt(recordCount);
                wormhole.put(keys.get(keyIndex), i);
              }
            };
          }
        });
  }

  @Test
  void updateRedBlackTreeMap() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<TreeMap<String, Integer>>, RuntimeException>() {
          @Override
          public String label() {
            return "Update Red-Black tree (TreeMap)";
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
              String key = TestHelpers.genRandomKey(minKeyLength, maxKeyLength);
              keys.add(key);
              map.put(key, i);
            }
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<TreeMap<String, Integer>> resourceAndKeys) {
            return () -> {
              TreeMap<String, Integer> map = resourceAndKeys.resource;
              List<String> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex = ThreadLocalRandom.current().nextInt(recordCount);
                map.put(keys.get(keyIndex), i);
              }
            };
          }
        });
  }

  @Test
  void updateAVLTreeMap() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<Object2ObjectSortedMap<String, Integer>>, RuntimeException>() {
          @Override
          public String label() {
            return "Update AVL tree map (Fastutil)";
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
              String key = TestHelpers.genRandomKey(minKeyLength, maxKeyLength);
              keys.add(key);
              map.put(key, i);
            }
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<Object2ObjectSortedMap<String, Integer>> resourceAndKeys) {
            return () -> {
              Object2ObjectSortedMap<String, Integer> map = resourceAndKeys.resource;
              List<String> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex = ThreadLocalRandom.current().nextInt(recordCount);
                map.put(keys.get(keyIndex), i);
              }
            };
          }
        });
  }

  @Test
  void deleteFromWormhole() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<Wormhole<Integer>>, RuntimeException>() {
          @Override
          public String label() {
            return "Delete from Wormhole (Wormhole4j)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public boolean initForEachAttempt() {
            return true;
          }

          @Override
          public ResourceAndKeys<Wormhole<Integer>> init() {
            List<String> keys = new ArrayList<>(recordCount);
            Wormhole<Integer> wormhole = new Wormhole<>();
            for (int i = 0; i < recordCount; i++) {
              String key = TestHelpers.genRandomKey(minKeyLength, maxKeyLength);
              keys.add(key);
              wormhole.put(key, i);
            }
            return new ResourceAndKeys<>(wormhole, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<Wormhole<Integer>> resourceAndKeys) {
            return () -> {
              Wormhole<Integer> wormhole = resourceAndKeys.resource;
              List<String> keys = resourceAndKeys.keys;
              while (!keys.isEmpty()) {
                int keyIndex = ThreadLocalRandom.current().nextInt(keys.size());
                String key = keys.remove(keyIndex);
                assert key != null;
                assert wormhole.delete(key);
              }
            };
          }
        });
  }

  @Test
  void deleteFromRedBlackTreeMap() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<TreeMap<String, Integer>>, RuntimeException>() {
          @Override
          public String label() {
            return "Delete from Red-Black tree (TreeMap)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public boolean initForEachAttempt() {
            return true;
          }

          @Override
          public ResourceAndKeys<TreeMap<String, Integer>> init() {
            List<String> keys = new ArrayList<>(recordCount);
            TreeMap<String, Integer> map = new TreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              String key = TestHelpers.genRandomKey(minKeyLength, maxKeyLength);
              keys.add(key);
              map.put(key, i);
            }
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<TreeMap<String, Integer>> resourceAndKeys) {
            return () -> {
              TreeMap<String, Integer> map = resourceAndKeys.resource;
              List<String> keys = resourceAndKeys.keys;
              while (!keys.isEmpty()) {
                int keyIndex = ThreadLocalRandom.current().nextInt(keys.size());
                String key = keys.remove(keyIndex);
                assert key != null;
                assert map.remove(key) != null;
              }
            };
          }
        });
  }

  @Test
  void deleteFromAVLTreeMap() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<Object2ObjectSortedMap<String, Integer>>, RuntimeException>() {
          @Override
          public String label() {
            return "Delete AVL tree map (Fastutil)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public boolean initForEachAttempt() {
            return true;
          }

          @Override
          public ResourceAndKeys<Object2ObjectSortedMap<String, Integer>> init() {
            List<String> keys = new ArrayList<>(recordCount);
            Object2ObjectSortedMap<String, Integer> map = new Object2ObjectAVLTreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              String key = TestHelpers.genRandomKey(minKeyLength, maxKeyLength);
              keys.add(key);
              map.put(key, i);
            }
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<Object2ObjectSortedMap<String, Integer>> resourceAndKeys) {
            return () -> {
              Object2ObjectSortedMap<String, Integer> map = resourceAndKeys.resource;
              List<String> keys = resourceAndKeys.keys;
              while (!keys.isEmpty()) {
                int keyIndex = ThreadLocalRandom.current().nextInt(keys.size());
                String key = keys.remove(keyIndex);
                assert key != null;
                assert map.remove(key) != null;
              }
            };
          }
        });
  }

  @Test
  void scanFromWormhole() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<Wormhole<Integer>>, RuntimeException>() {
          @Override
          public String label() {
            return "Scan from Wormhole (Wormhole4j)";
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
              String key = TestHelpers.genRandomKey(minKeyLength, maxKeyLength);
              keys.add(key);
              wormhole.put(key, i);
            }
            Collections.sort(keys);
            return new ResourceAndKeys<>(wormhole, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<Wormhole<Integer>> resourceAndKeys) {
            return () -> {
              Wormhole<Integer> wormhole = resourceAndKeys.resource;
              List<String> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex1 = ThreadLocalRandom.current().nextInt(recordCount);
                int keyIndex2 =
                    Math.min(
                        keys.size() - 1,
                        keyIndex1 + ThreadLocalRandom.current().nextInt(maxScanSize));
                String key1 = keys.get(keyIndex1);
                String key2 = keys.get(keyIndex2);
                wormhole.scanWithExclusiveEndKey(key1, key2, kv -> true);
              }
            };
          }
        });
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
              String key = TestHelpers.genRandomKey(minKeyLength, maxKeyLength);
              keys.add(key);
              map.put(key, i);
            }
            Collections.sort(keys);
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<TreeMap<String, Integer>> resourceAndKeys) {
            return () -> {
              TreeMap<String, Integer> map = resourceAndKeys.resource;
              List<String> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex1 = ThreadLocalRandom.current().nextInt(recordCount);
                int keyIndex2 =
                    Math.min(
                        keys.size() - 1,
                        keyIndex1 + ThreadLocalRandom.current().nextInt(maxScanSize));
                String key1 = keys.get(keyIndex1);
                String key2 = keys.get(keyIndex2);
                for (Map.Entry<String, Integer> ignored : map.subMap(key1, key2).entrySet()) {
                  // Nothing to do.
                }
              }
            };
          }
        });
  }

  @Test
  void scanFromAVLTreeMap() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<Object2ObjectSortedMap<String, Integer>>, RuntimeException>() {
          @Override
          public String label() {
            return "Scan from AVL tree map (Fastutil)";
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
              String key = TestHelpers.genRandomKey(minKeyLength, maxKeyLength);
              keys.add(key);
              map.put(key, i);
            }
            Collections.sort(keys);
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<Object2ObjectSortedMap<String, Integer>> resourceAndKeys) {
            return () -> {
              Object2ObjectSortedMap<String, Integer> map = resourceAndKeys.resource;
              List<String> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex1 = ThreadLocalRandom.current().nextInt(recordCount);
                int keyIndex2 =
                    Math.min(
                        keys.size() - 1,
                        keyIndex1 + ThreadLocalRandom.current().nextInt(maxScanSize));
                String key1 = keys.get(keyIndex1);
                String key2 = keys.get(keyIndex2);
                for (Map.Entry<String, Integer> ignored : map.subMap(key1, key2).entrySet()) {
                  // Nothing to do.
                }
              }
            };
          }
        });
  }
}
