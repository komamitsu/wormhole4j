package org.komamitsu.wormhole;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
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

  private long measure(Runnable task) {
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

  private interface TestCase<T> {
    String label();

    T init();

    Runnable createTask(T resource);
  }

  <T> void execute(TestCase<T> testCase) {
    T resource = testCase.init();

    System.out.printf("Starting: %s%n", testCase.label());

    // Warmups
    for (int i = 0; i < warmupCount; i++) {
      long durationMillis = measure(testCase.createTask(resource));
      System.out.printf("Warmup #%d: %d ms%n", i, durationMillis);
    }

    // Attempts
    List<Long> durationsMillis = new ArrayList<>();
    for (int i = 0; i < attemptCount; i++) {
      long durationMillis = measure(testCase.createTask(resource));
      durationsMillis.add(durationMillis);
      System.out.printf("Attempt #%d: %d ms%n", i, durationMillis);
    }
    long averageMillis = durationsMillis.stream().reduce(0L, Long::sum) / durationsMillis.size();
    double stdDev = Math.sqrt(
        durationsMillis.stream()
            .map(durationMillis -> (long)Math.pow(durationMillis - averageMillis, 2.0))
            .reduce(0L, Long::sum));
    System.out.printf("Average: %d ms%n", averageMillis);
    System.out.printf("StdDev: %f ms%n", stdDev);
  }

  @Test
  void insertToWormhole() {
    execute(
        new TestCase<List<String>>() {
          @Override
          public String label() {
            return "Insert to Wormhole";
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
          public Runnable createTask(List<String> keys) {
            return () -> {
              Wormhole<Integer> wormhole = new Wormhole<>(256);
              for (int i = 0; i < recordCount; i++) {
                wormhole.put(keys.get(i), i);
              }
            };
          }
        }
    );
  }

  @Test
  void insertToTreeMap() {
    execute(
        new TestCase<List<String>>() {
          @Override
          public String label() {
            return "Insert to TreeMap";
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
          public Runnable createTask(List<String> keys) {
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
}
