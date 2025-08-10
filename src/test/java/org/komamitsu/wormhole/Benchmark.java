package org.komamitsu.wormhole;

import btree4j.BTree;
import btree4j.BTreeException;
import btree4j.Value;
import btree4j.utils.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.komamitsu.wormhole.TestHelpers.genRandomKey;

class Benchmark {
  private static final String PROP_PREFIX = "wormhole4j.benchmark.";
  private static final String PROP_MAX_KEY_LENGTH = PROP_PREFIX + "max_key_length";
  private static final String PROP_RECORD_COUNT = PROP_PREFIX + "record_count";
  private static final String PROP_WARMUP_COUNT = PROP_PREFIX + "warmup_count";
  private static final String PROP_ATTEMPT_COUNT = PROP_PREFIX + "attempt_count";
  private static final String DEFAULT_MAX_KEY_LENGTH = "64";
  private static final String DEFAULT_RECORD_COUNT = "100000";
  private static final String DEFAULT_WARMUP_COUNT = "4";
  private static final String DEFAULT_ATTEMPT_COUNT = "4";
  private final int maxKeyLength;
  private final int recordCount;
  private final int warmupCount;
  private final int attemptCount;

  public Benchmark() {
    this.maxKeyLength = Integer.parseInt(System.getProperty(PROP_MAX_KEY_LENGTH, DEFAULT_MAX_KEY_LENGTH));
    this.recordCount = Integer.parseInt(System.getProperty(PROP_RECORD_COUNT, DEFAULT_RECORD_COUNT));
    this.warmupCount = Integer.parseInt(System.getProperty(PROP_WARMUP_COUNT, DEFAULT_WARMUP_COUNT));
    this.attemptCount = Integer.parseInt(System.getProperty(PROP_ATTEMPT_COUNT, DEFAULT_ATTEMPT_COUNT));
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
  }

  <T, E extends Throwable> void execute(TestCase<T, E> testCase) throws Throwable {
    T resource = testCase.init();

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
            .map(throughput -> (long)Math.pow(throughput - averageThroughput, 2.0))
            .reduce(0L, Long::sum));
    System.out.printf("Average throughput: %d per second%n", averageThroughput);
    System.out.printf("StdDev: %f per second%n", stdDev);
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
  void insertToTreeMap() throws Throwable {
    execute(
        new TestCase<List<String>, RuntimeException>() {
          @Override
          public String label() {
            return "Insert to TreeMap";
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
              TreeMap<String, Integer> treeMap = new TreeMap<>();
              for (int i = 0; i < recordCount; i++) {
                treeMap.put(keys.get(i), i);
              }
            };
          }
        }
    );
  }

  @Test
  void insertToBTreePlus() throws Throwable {
    execute(
        new TestCase<List<String>, BTreeException>() {
          @Override
          public String label() {
            return "Insert to BTree+";
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
          public ThrowableRunnable<BTreeException> createTask(List<String> keys) {
            return () -> {
              File tmpDir = FileUtils.getTempDir();
              File tmpFile = new File(tmpDir, "btree-" + System.nanoTime() + ".idx");
              tmpFile.deleteOnExit();
              BTree btree = new BTree(tmpFile);
              btree.init(false);
              for (int i = 0; i < recordCount; i++) {
                btree.addValue(new Value(keys.get(i)), i);
              }
            };
          }
        }
    );
  }

  private static class MapAndKeys<T> {
    private final T map;
    private final List<String> keys;

    public MapAndKeys(T map, List<String> keys) {
      this.map = map;
      this.keys = keys;
    }
  }

  @Test
  void getFromWormhole() throws Throwable {
    execute(
        new TestCase<MapAndKeys<Wormhole<Integer>>, RuntimeException>() {
          @Override
          public String label() {
            return "Get from Wormhole";
          }

          @Override
          public int count() {
            return recordCount * 2;
          }

          @Override
          public MapAndKeys<Wormhole<Integer>> init() {
            List<String> keys = new ArrayList<>(recordCount);
            Wormhole<Integer> wormhole = new Wormhole<>();
            for (int i = 0; i < recordCount; i++) {
              String key = genRandomKey(maxKeyLength);
              keys.add(key);
              wormhole.put(key, i);
            }
            return new MapAndKeys<>(wormhole, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(MapAndKeys<Wormhole<Integer>> mapAndKeys) {
            return () -> {
              Wormhole<Integer> wormhole = mapAndKeys.map;
              List<String> keys = mapAndKeys.keys;
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
  void getFromTreeMap() throws Throwable {
    execute(
        new TestCase<MapAndKeys<TreeMap<String, Integer>>, RuntimeException>() {
          @Override
          public String label() {
            return "Get from TreeMap";
          }

          @Override
          public int count() {
            return recordCount * 2;
          }

          @Override
          public MapAndKeys<TreeMap<String, Integer>> init() {
            List<String> keys = new ArrayList<>(recordCount);
            TreeMap<String, Integer> treeMap = new TreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              String key = genRandomKey(maxKeyLength);
              keys.add(key);
              treeMap.put(key, i);
            }
            return new MapAndKeys<>(treeMap, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(MapAndKeys<TreeMap<String, Integer>> mapAndKeys) {
            return () -> {
              TreeMap<String, Integer> treeMap = mapAndKeys.map;
              List<String> keys = mapAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex = ThreadLocalRandom.current().nextInt(recordCount);
                treeMap.get(keys.get(keyIndex));
              }
            };
          }
        }
    );
  }

  @Test
  void getFromBTreePlus() throws Throwable {
    execute(
        new TestCase<MapAndKeys<BTree>, BTreeException>() {
          @Override
          public String label() {
            return "Get from BTree+";
          }

          @Override
          public int count() {
            return recordCount * 2;
          }

          @Override
          public MapAndKeys<BTree> init() throws BTreeException, IOException {
            List<String> keys = new ArrayList<>(recordCount);
            File tmpDir = FileUtils.getTempDir();
            File tmpFile = new File(tmpDir, "btree-" + System.nanoTime() + ".idx");
            tmpFile.deleteOnExit();
            BTree btree = new BTree(tmpFile);
            btree.init(false);
            for (int i = 0; i < recordCount; i++) {
              String key = genRandomKey(maxKeyLength);
              keys.add(key);
              btree.addValue(new Value(key), i);
            }
            return new MapAndKeys<>(btree, keys);
          }

          @Override
          public ThrowableRunnable<BTreeException> createTask(MapAndKeys<BTree> mapAndKeys) {
            return () -> {
              BTree bTree = mapAndKeys.map;
              List<String> keys = mapAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex = ThreadLocalRandom.current().nextInt(recordCount);
                bTree.findValue(new Value(keys.get(keyIndex)));
              }
            };
          }
        }
    );
  }
}
