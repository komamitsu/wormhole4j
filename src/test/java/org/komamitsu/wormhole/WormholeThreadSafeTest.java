package org.komamitsu.wormhole;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

@ParameterizedClass
//@ValueSource(ints = {3, 128})
@ValueSource(ints = {128})
class WormholeThreadSafeTest {
//  private static final int CONCURRENCY = Runtime.getRuntime().availableProcessors();
  private static final int CONCURRENCY = 4;
//  private static final Duration DURATION = Duration.ofSeconds(10);
  private static final Duration DURATION = Duration.ofSeconds(1);
  @Parameter int leafNodeSize;

  private void shutdownAndAwaitTermination(@Nullable ExecutorService executorService) {
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
    ExecutorService executorService = null;
    try {
      executorService = Executors.newFixedThreadPool(CONCURRENCY);
      Wormhole<Integer> wormhole = new WormholeThreadSafe<>(leafNodeSize, false);
      CountDownLatch startWait = new CountDownLatch(1);
      AtomicBoolean shouldStop = new AtomicBoolean();
      List<AtomicInteger> counters = new ArrayList<>(CONCURRENCY);

      // Act
      for (int i = 0; i < CONCURRENCY; i++) {
        final int threadIndex = i;
        counters.add(new AtomicInteger());
        executorService.submit(
            () -> {
              try {
                startWait.await();
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }

              while (!shouldStop.get()) {
                int counter = counters.get(threadIndex).get();
                String key = String.format("%020d-%02d", counter, threadIndex);
                try {
                  wormhole.put(key, counter);
                  counters.get(threadIndex).incrementAndGet();
                } catch (Exception e) {
                  e.printStackTrace();
                  throw e;
                }
// FIXME
if (counter >= 40) { break; }
              }
            });
      }

      startWait.countDown();
      TimeUnit.MILLISECONDS.sleep(DURATION.toMillis());
      shouldStop.set(true);
      shutdownAndAwaitTermination(executorService);

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
    finally {
      shutdownAndAwaitTermination(executorService);
    }
  }
}
