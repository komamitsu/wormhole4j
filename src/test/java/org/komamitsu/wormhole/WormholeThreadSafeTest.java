package org.komamitsu.wormhole;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@ParameterizedClass
@ValueSource(ints = {3, 128})
class WormholeThreadSafeTest {
  private static final int CONCURRENCY = Runtime.getRuntime().availableProcessors();
  private static final Duration DURATION = Duration.ofSeconds(10);
  @Parameter int leafNodeSize;
  private ExecutorService executorService;

  @BeforeEach
  void beforeEach() {
    executorService = Executors.newFixedThreadPool(CONCURRENCY);
  }

  @AfterEach
  void afterEach() {
    if (executorService != null) {
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        System.err.println("Interrupted: " + e.getMessage());
      }
    }
  }

  @Test
  void insert_ShouldStoreProperRecords() throws InterruptedException {
    // Arrange
    Wormhole<Integer> wormhole = new WormholeThreadSafe<>(leafNodeSize, false);
    CountDownLatch startWait = new CountDownLatch(1);
    AtomicBoolean shouldStop = new AtomicBoolean();
    List<AtomicInteger> counters = new ArrayList<>(CONCURRENCY);

    // Act
    for (int i = 0; i < CONCURRENCY; i++) {
      final int threadIndex = i;
      counters.add(new AtomicInteger());
      executorService.submit(() -> {
        try {
          startWait.await();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }

        while (!shouldStop.get()) {
          int counter = counters.get(threadIndex).getAndIncrement();
          String key = String.format("%020d-%02d", counter, threadIndex);
          try {
            wormhole.put(key, counter);
          }
          catch (Exception e) {
            e.printStackTrace();
            throw e;
          }
        }
      });
    }

    startWait.countDown();
    TimeUnit.MILLISECONDS.sleep(DURATION.toMillis());
    shouldStop.set(true);

    // Assert
    int maxCount = counters.stream().map(AtomicInteger::get).reduce(0, Integer::max).intValue();
    for (int counter = 0; counter < maxCount; counter++) {
      for (int threadIndex = 0; threadIndex < CONCURRENCY; threadIndex++) {
        if (counters.get(threadIndex).get() >= counter) {
          String key = String.format("%020d-%02d", counter, threadIndex);
          assertThat(wormhole.get(key)).isEqualTo(counter);
        }
      }
    }
  }
}
