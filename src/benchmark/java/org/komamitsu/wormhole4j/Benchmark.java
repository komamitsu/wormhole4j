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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

abstract class Benchmark {
  static final String PROP_PREFIX = "wormhole4j.benchmark.";
  static final String PROP_MIN_KEY_LENGTH = PROP_PREFIX + "min_key_length";
  static final String PROP_MAX_KEY_LENGTH = PROP_PREFIX + "max_key_length";
  static final String PROP_RECORD_COUNT = PROP_PREFIX + "record_count";
  static final String PROP_WARMUP_COUNT = PROP_PREFIX + "warmup_count";
  static final String PROP_ATTEMPT_COUNT = PROP_PREFIX + "attempt_count";
  static final String PROP_MAX_SCAN_SIZE = PROP_PREFIX + "max_scan_size";
  static final String DEFAULT_MIN_KEY_LENGTH = "8";
  static final String DEFAULT_MAX_KEY_LENGTH = "128";
  static final String DEFAULT_RECORD_COUNT = "100000";
  static final String DEFAULT_WARMUP_COUNT = "5";
  static final String DEFAULT_ATTEMPT_COUNT = "5";
  static final String DEFAULT_MAX_SCAN_SIZE = "512";
  final int minKeyLength;
  final int maxKeyLength;
  final int recordCount;
  final int warmupCount;
  final int attemptCount;
  final int maxScanSize;

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
  interface ThrowableRunnable<E extends Throwable> {
    void run() throws E, IOException;
  }

  <E extends Throwable> long measure(ThrowableRunnable<E> task) throws E, IOException {
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

  interface TestCase<T, E extends Throwable> {
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

  static class ResourceAndKeys<R, K> {
    final R resource;
    final List<K> keys;

    ResourceAndKeys(R resource, List<K> keys) {
      this.resource = resource;
      this.keys = keys;
    }
  }
}
