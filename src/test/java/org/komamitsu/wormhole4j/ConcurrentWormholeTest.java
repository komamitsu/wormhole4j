/*
 * Copyright 2026 Mitsunori Komatsu
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

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

class ConcurrentWormholeTest {
  private Wormhole<Integer, Integer> wormhole;

  private interface ThrowableSupplier<R, E extends Exception> {
    R get() throws E;
  }

  private <R> R withRegisteredWormhole(ThrowableSupplier<R, Exception> task) throws Exception {
    wormhole.register();
    try {
      return task.get();
    } finally {
      wormhole.unregister();
    }
  }

  @BeforeEach
  void setUp() {
    wormhole =
        new WormholeBuilder.ForIntKey<Integer>().setConcurrent(true).setLeafNodeSize(4).build();
  }

  @Test
  void putNewKeysAfterSplit_ShouldReturnNull() throws Exception {
    withRegisteredWormhole(
        () -> {
          // Act Assert
          assertThat(wormhole.put(10, 100)).isNull();
          assertThat(wormhole.put(11, 110)).isNull();
          assertThat(wormhole.put(9, 90)).isNull();
          assertThat(wormhole.put(12, 120)).isNull();
          assertThat(wormhole.put(8, 80)).isNull();

          return null;
        });
  }

  @RepeatedTest(10000)
  void conflict2Puts_ShouldReturnNullAndExistingValue() throws Exception {
    withRegisteredWormhole(
        () -> {
          // Arrange
          ExecutorService executorService = Executors.newFixedThreadPool(2);
          List<Future<Integer>> futures = new ArrayList<>();

          try {
            // Act
            futures.add(
                executorService.submit(() -> withRegisteredWormhole(() -> wormhole.put(1, 10))));
            futures.add(
                executorService.submit(() -> withRegisteredWormhole(() -> wormhole.put(1, 20))));

            // Assert
            List<Integer> resultValues = new ArrayList<>();
            for (Future<Integer> future : futures) {
              Integer resultValue = future.get();
              if (resultValue != null) {
                resultValues.add(resultValue);
              }
            }
            assertThat(resultValues).hasSize(1);
            assertThat(resultValues.get(0)).isIn(10, 20);

            assertThat(wormhole.get(1)).isIn(10, 20);

            return null;
          } finally {
            executorService.shutdown();
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
              executorService.shutdownNow();
            }
          }
        });
  }

  @RepeatedTest(10000)
  void concurrent2PutsAfterSplit_ShouldReturnProperValues_1() throws Exception {
    withRegisteredWormhole(
        () -> {
          // Arrange
          assertThat(wormhole.put(8, 80)).isNull();
          assertThat(wormhole.put(9, 90)).isNull();
          assertThat(wormhole.put(10, 100)).isNull();
          assertThat(wormhole.put(11, 110)).isNull();

          ExecutorService executorService = Executors.newFixedThreadPool(2);
          List<Future<Integer>> futures = new ArrayList<>();
          CyclicBarrier barrier = new CyclicBarrier(2);

          try {
            // Act
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier.await();
                              return wormhole.put(9, 99);
                            })));
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier.await();
                              return wormhole.put(12, 120);
                            })));

            // Assert
            List<Integer> resultValues = new ArrayList<>();
            for (Future<Integer> future : futures) {
              Integer resultValue = future.get();
              if (resultValue != null) {
                resultValues.add(resultValue);
              }
            }
            assertThat(resultValues).hasSize(1);
            assertThat(resultValues.get(0)).isEqualTo(90);

            assertThat(wormhole.get(8)).isEqualTo(80);
            assertThat(wormhole.get(9)).isEqualTo(99);
            assertThat(wormhole.get(10)).isEqualTo(100);
            assertThat(wormhole.get(11)).isEqualTo(110);
            assertThat(wormhole.get(12)).isEqualTo(120);

            return null;
          } finally {
            executorService.shutdown();
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
              executorService.shutdownNow();
            }
          }
        });
  }

  @RepeatedTest(10000)
  void concurrent2PutsAfterSplit_ShouldReturnProperValues_2() throws Exception {
    withRegisteredWormhole(
        () -> {
          // Arrange
          assertThat(wormhole.put(10, 100)).isNull();
          assertThat(wormhole.put(11, 110)).isNull();
          assertThat(wormhole.put(12, 120)).isNull();
          assertThat(wormhole.put(9, 90)).isNull();

          ExecutorService executorService = Executors.newFixedThreadPool(2);
          List<Future<Integer>> futures = new ArrayList<>();
          CyclicBarrier barrier = new CyclicBarrier(2);

          try {
            // Act
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier.await();
                              return wormhole.put(8, 80);
                            })));
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier.await();
                              return wormhole.put(11, 111);
                            })));

            // Assert
            List<Integer> resultValues = new ArrayList<>();
            for (Future<Integer> future : futures) {
              Integer resultValue = future.get();
              if (resultValue != null) {
                resultValues.add(resultValue);
              }
            }
            assertThat(resultValues).hasSize(1);
            assertThat(resultValues.get(0)).isEqualTo(110);

            assertThat(wormhole.get(8)).isEqualTo(80);
            assertThat(wormhole.get(9)).isEqualTo(90);
            assertThat(wormhole.get(10)).isEqualTo(100);
            assertThat(wormhole.get(11)).isEqualTo(111);
            assertThat(wormhole.get(12)).isEqualTo(120);

            return null;
          } finally {
            executorService.shutdown();
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
              executorService.shutdownNow();
            }
          }
        });
  }

  @RepeatedTest(10000)
  void concurrent3PutsAfterSplit_ShouldReturnProperValues_simplified() throws Exception {
    withRegisteredWormhole(
        () -> {
          // Arrange
          assertThat(wormhole.put(10, 100)).isNull();
          assertThat(wormhole.put(11, 110)).isNull();
          assertThat(wormhole.put(9, 90)).isNull();
          assertThat(wormhole.put(7, 70)).isNull();
          assertThat(wormhole.put(14, 140)).isNull();

          ExecutorService executorService = Executors.newFixedThreadPool(3);
          List<Future<List<Integer>>> futures = new ArrayList<>();
          CyclicBarrier barrier = new CyclicBarrier(3);

          try {
            // Act
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier.await();
                              List<Integer> existingValues = new ArrayList<>();
                              existingValues.add(wormhole.put(12, 120));
                              return existingValues;
                            })));
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier.await();
                              List<Integer> existingValues = new ArrayList<>();
                              existingValues.add(wormhole.put(13, 130));
                              return existingValues;
                            })));
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier.await();
                              List<Integer> existingValues = new ArrayList<>();
                              existingValues.add(wormhole.put(15, 150));
                              return existingValues;
                            })));

            // Assert
            List<Integer> resultValues = new ArrayList<>();
            for (Future<List<Integer>> future : futures) {
              List<Integer> result =
                  future.get().stream().filter(Objects::nonNull).collect(toList());
              resultValues.addAll(result);
            }
            assertThat(resultValues).hasSize(0);

            assertThat(wormhole.get(7)).isEqualTo(70);
            assertThat(wormhole.get(9)).isEqualTo(90);
            assertThat(wormhole.get(10)).isEqualTo(100);
            assertThat(wormhole.get(11)).isEqualTo(110);
            assertThat(wormhole.get(12)).isEqualTo(120);
            assertThat(wormhole.get(13)).isEqualTo(130);
            assertThat(wormhole.get(14)).isEqualTo(140);
            assertThat(wormhole.get(15)).isEqualTo(150);

            return null;
          } finally {
            executorService.shutdown();
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
              executorService.shutdownNow();
            }
          }
        });
  }

  @RepeatedTest(10000)
  void concurrent3PutsAfterSplit_ShouldReturnProperValues_1() throws Exception {
    withRegisteredWormhole(
        () -> {
          // Arrange
          assertThat(wormhole.put(10, 100)).isNull();
          assertThat(wormhole.put(11, 110)).isNull();
          assertThat(wormhole.put(9, 90)).isNull();

          ExecutorService executorService = Executors.newFixedThreadPool(3);
          List<Future<List<Integer>>> futures = new ArrayList<>();
          CyclicBarrier barrier = new CyclicBarrier(3);

          try {
            // Act
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier.await();
                              List<Integer> existingValues = new ArrayList<>();
                              existingValues.add(wormhole.put(7, 70));
                              existingValues.add(wormhole.put(12, 120));
                              return existingValues;
                            })));
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier.await();
                              List<Integer> existingValues = new ArrayList<>();
                              existingValues.add(wormhole.put(13, 130));
                              return existingValues;
                            })));
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier.await();
                              List<Integer> existingValues = new ArrayList<>();
                              existingValues.add(wormhole.put(14, 140));
                              existingValues.add(wormhole.put(15, 150));
                              return existingValues;
                            })));

            // Assert
            List<Integer> resultValues = new ArrayList<>();
            for (Future<List<Integer>> future : futures) {
              List<Integer> result =
                  future.get().stream().filter(Objects::nonNull).collect(toList());
              resultValues.addAll(result);
            }
            assertThat(resultValues).hasSize(0);

            assertThat(wormhole.get(7)).isEqualTo(70);
            assertThat(wormhole.get(9)).isEqualTo(90);
            assertThat(wormhole.get(10)).isEqualTo(100);
            assertThat(wormhole.get(11)).isEqualTo(110);
            assertThat(wormhole.get(12)).isEqualTo(120);
            assertThat(wormhole.get(13)).isEqualTo(130);
            assertThat(wormhole.get(14)).isEqualTo(140);
            assertThat(wormhole.get(15)).isEqualTo(150);

            return null;
          } finally {
            executorService.shutdown();
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
              executorService.shutdownNow();
            }
          }
        });
  }

  @RepeatedTest(10000)
  void concurrent3PutsAfterSplit_ShouldReturnProperValues_2() throws Exception {
    withRegisteredWormhole(
        () -> {
          // Arrange
          assertThat(wormhole.put(11, 110)).isNull();
          assertThat(wormhole.put(9, 90)).isNull();
          assertThat(wormhole.put(12, 120)).isNull();
          assertThat(wormhole.put(8, 80)).isNull();

          ExecutorService executorService = Executors.newFixedThreadPool(3);
          List<Future<List<Integer>>> futures = new ArrayList<>();
          CyclicBarrier barrier = new CyclicBarrier(3);

          try {
            // Act
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier.await();
                              List<Integer> existingValues = new ArrayList<>();
                              existingValues.add(wormhole.put(10, 100));
                              return existingValues;
                            })));
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier.await();
                              List<Integer> existingValues = new ArrayList<>();
                              existingValues.add(wormhole.put(13, 130));
                              return existingValues;
                            })));
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier.await();
                              List<Integer> existingValues = new ArrayList<>();
                              existingValues.add(wormhole.put(15, 150));
                              return existingValues;
                            })));

            // Assert
            List<Integer> resultValues = new ArrayList<>();
            for (Future<List<Integer>> future : futures) {
              List<Integer> result =
                  future.get().stream().filter(Objects::nonNull).collect(toList());
              resultValues.addAll(result);
            }
            assertThat(resultValues).hasSize(0);

            assertThat(wormhole.get(8)).isEqualTo(80);
            assertThat(wormhole.get(9)).isEqualTo(90);
            assertThat(wormhole.get(10)).isEqualTo(100);
            assertThat(wormhole.get(11)).isEqualTo(110);
            assertThat(wormhole.get(12)).isEqualTo(120);
            assertThat(wormhole.get(13)).isEqualTo(130);
            assertThat(wormhole.get(15)).isEqualTo(150);

            return null;
          } finally {
            executorService.shutdown();
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
              executorService.shutdownNow();
            }
          }
        });
  }

  @RepeatedTest(10000)
  void concurrent3PutsAfterSplit_ShouldReturnProperValues_3() throws Exception {
    withRegisteredWormhole(
        () -> {
          // Arrange
          assertThat(wormhole.put(11, 110)).isNull();
          assertThat(wormhole.put(10, 100)).isNull();
          assertThat(wormhole.put(8, 80)).isNull();
          assertThat(wormhole.put(6, 60)).isNull();
          assertThat(wormhole.put(9, 90)).isNull();
          assertThat(wormhole.put(15, 150)).isNull();
          assertThat(wormhole.put(16, 160)).isNull();

          assertThat(wormhole.put(7, 70)).isNull();

          ExecutorService executorService = Executors.newFixedThreadPool(3);
          List<Future<List<Integer>>> futures = new ArrayList<>();
          CyclicBarrier barrier = new CyclicBarrier(3);

          try {
            // Act
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier.await();
                              List<Integer> existingValues = new ArrayList<>();
                              existingValues.add(wormhole.put(5, 50));
                              return existingValues;
                            })));
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier.await();
                              List<Integer> existingValues = new ArrayList<>();
                              Thread.sleep(0, 50);
                              wormhole.get(4);
                              return existingValues;
                            })));
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier.await();
                              List<Integer> existingValues = new ArrayList<>();
                              existingValues.add(wormhole.put(17, 170));
                              return existingValues;
                            })));

            // Assert
            List<Integer> resultValues = new ArrayList<>();
            for (Future<List<Integer>> future : futures) {
              List<Integer> result =
                  future.get(10, TimeUnit.SECONDS).stream()
                      .filter(Objects::nonNull)
                      .collect(toList());
              resultValues.addAll(result);
            }
            assertThat(resultValues).hasSize(0);

            assertThat(wormhole.get(5)).isEqualTo(50);
            assertThat(wormhole.get(6)).isEqualTo(60);
            assertThat(wormhole.get(7)).isEqualTo(70);
            assertThat(wormhole.get(8)).isEqualTo(80);
            assertThat(wormhole.get(9)).isEqualTo(90);
            assertThat(wormhole.get(10)).isEqualTo(100);
            assertThat(wormhole.get(11)).isEqualTo(110);
            assertThat(wormhole.get(15)).isEqualTo(150);
            assertThat(wormhole.get(16)).isEqualTo(160);
            assertThat(wormhole.get(17)).isEqualTo(170);

            return null;
          } finally {
            executorService.shutdown();
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
              executorService.shutdownNow();
            }
          }
        });
  }
}
