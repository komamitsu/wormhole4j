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
    wormhole.registerThread();
    try {
      return task.get();
    } finally {
      wormhole.unregisterThread();
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

  @RepeatedTest(1000)
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

  @RepeatedTest(1000)
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

  @RepeatedTest(1000)
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

  @RepeatedTest(1000)
  void concurrent2PutsAfterSplit_ShouldReturnProperValues_3() throws Exception {
    withRegisteredWormhole(
        () -> {
          // Arrange
          assertThat(wormhole.put(10, 100)).isNull();
          assertThat(wormhole.put(11, 110)).isNull();
          assertThat(wormhole.put(13, 130)).isNull();
          assertThat(wormhole.put(14, 140)).isNull();
          assertThat(wormhole.put(7, 70)).isNull();
          assertThat(wormhole.put(8, 80)).isNull();

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
                              return wormhole.put(6, 60);
                            })));
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier.await();
                              return wormhole.put(7, 71);
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
            assertThat(resultValues.get(0)).isEqualTo(70);

            assertThat(wormhole.get(7)).isEqualTo(71);
            assertThat(wormhole.get(8)).isEqualTo(80);
            assertThat(wormhole.get(10)).isEqualTo(100);
            assertThat(wormhole.get(11)).isEqualTo(110);
            assertThat(wormhole.get(13)).isEqualTo(130);
            assertThat(wormhole.get(14)).isEqualTo(140);

            return null;
          } finally {
            executorService.shutdown();
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
              executorService.shutdownNow();
            }
          }
        });
  }

  @RepeatedTest(1000)
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

  @RepeatedTest(1000)
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

  @RepeatedTest(1000)
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

  @RepeatedTest(1000)
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

  @RepeatedTest(1000)
  void concurrent3PutsAfterSplit_ShouldReturnProperValues_4() throws Exception {
    withRegisteredWormhole(
        () -> {
          // Arrange
          assertThat(wormhole.put(10, 100)).isNull();
          assertThat(wormhole.put(13, 130)).isNull();

          ExecutorService executorService = Executors.newFixedThreadPool(3);
          List<Future<List<Integer>>> futures = new ArrayList<>();
          CyclicBarrier barrier1 = new CyclicBarrier(3);
          CyclicBarrier barrier2 = new CyclicBarrier(2);

          try {
            // Act
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              List<Integer> existingValues = new ArrayList<>();
                              barrier1.await();
                              existingValues.add(wormhole.put(11, 110));
                              barrier2.await();
                              existingValues.add(wormhole.put(8, 80));
                              return existingValues;
                            })));
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier1.await();
                              List<Integer> existingValues = new ArrayList<>();
                              existingValues.add(wormhole.put(12, 120));
                              barrier2.await();
                              existingValues.add(wormhole.put(7, 70));
                              return existingValues;
                            })));
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier1.await();
                              List<Integer> existingValues = new ArrayList<>();
                              existingValues.add(wormhole.put(6, 60));
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

            assertThat(wormhole.get(6)).isEqualTo(60);
            assertThat(wormhole.get(7)).isEqualTo(70);
            assertThat(wormhole.get(8)).isEqualTo(80);
            assertThat(wormhole.get(10)).isEqualTo(100);
            assertThat(wormhole.get(11)).isEqualTo(110);
            assertThat(wormhole.get(12)).isEqualTo(120);
            assertThat(wormhole.get(13)).isEqualTo(130);

            return null;
          } finally {
            executorService.shutdown();
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
              executorService.shutdownNow();
            }
          }
        });
  }

  @RepeatedTest(1000)
  void concurrent2Deletes_ShouldReturnProperValues() throws Exception {
    withRegisteredWormhole(
        () -> {
          // Arrange
          assertThat(wormhole.put(9, 90)).isNull();
          assertThat(wormhole.put(10, 100)).isNull();

          ExecutorService executorService = Executors.newFixedThreadPool(3);
          List<Future<List<Boolean>>> futures = new ArrayList<>();
          CyclicBarrier barrier = new CyclicBarrier(2);

          try {
            // Act
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier.await();
                              List<Boolean> existingValues = new ArrayList<>();
                              existingValues.add(wormhole.delete(9));
                              return existingValues;
                            })));
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier.await();
                              List<Boolean> existingValues = new ArrayList<>();
                              existingValues.add(wormhole.delete(10));
                              return existingValues;
                            })));

            // Assert
            List<Boolean> resultValues = new ArrayList<>();
            for (Future<List<Boolean>> future : futures) {
              List<Boolean> result =
                  future.get().stream().filter(Objects::nonNull).collect(toList());
              resultValues.addAll(result);
            }
            assertThat(resultValues).hasSize(2);
            assertThat(resultValues.get(0)).isTrue();
            assertThat(resultValues.get(1)).isTrue();

            assertThat(wormhole.get(9)).isNull();
            assertThat(wormhole.get(10)).isNull();

            return null;
          } finally {
            executorService.shutdown();
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
              executorService.shutdownNow();
            }
          }
        });
  }

  @RepeatedTest(1000)
  void concurrentPutAndDelete_ShouldReturnProperValues() throws Exception {
    withRegisteredWormhole(
        () -> {
          // Arrange
          assertThat(wormhole.put(8, 80)).isNull();
          assertThat(wormhole.put(10, 100)).isNull();
          assertThat(wormhole.put(12, 120)).isNull();
          assertThat(wormhole.put(13, 130)).isNull();

          ExecutorService executorService = Executors.newFixedThreadPool(3);
          List<Future<List<Boolean>>> deleteFutures = new ArrayList<>();
          List<Future<List<Integer>>> putFutures = new ArrayList<>();
          CyclicBarrier barrier = new CyclicBarrier(2);

          try {
            // Act
            putFutures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier.await();
                              return Arrays.asList(wormhole.put(7, 70));
                            })));
            deleteFutures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier.await();
                              return Arrays.asList(wormhole.delete(12));
                            })));

            // Assert
            List<Integer> resultPutValues = new ArrayList<>();
            for (Future<List<Integer>> future : putFutures) {
              List<Integer> result =
                  future.get().stream().filter(Objects::nonNull).collect(toList());
              resultPutValues.addAll(result);
            }
            List<Boolean> resultDeleteValues = new ArrayList<>();
            for (Future<List<Boolean>> future : deleteFutures) {
              List<Boolean> result =
                  future.get().stream().filter(Objects::nonNull).collect(toList());
              resultDeleteValues.addAll(result);
            }
            assertThat(resultPutValues).hasSize(0);
            assertThat(resultDeleteValues).hasSize(1);
            assertThat(resultDeleteValues.get(0)).isTrue();

            assertThat(wormhole.get(7)).isEqualTo(70);
            assertThat(wormhole.get(8)).isEqualTo(80);
            assertThat(wormhole.get(10)).isEqualTo(100);
            assertThat(wormhole.get(12)).isNull();
            assertThat(wormhole.get(13)).isEqualTo(130);

            return null;
          } finally {
            executorService.shutdown();
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
              executorService.shutdownNow();
            }
          }
        });
  }

  @RepeatedTest(1000)
  void concurrentPutAndScan_ShouldReturnProperValues() throws Exception {
    withRegisteredWormhole(
        () -> {
          // Arrange
          assertThat(wormhole.put(10, 100)).isNull();
          assertThat(wormhole.put(11, 110)).isNull();

          ExecutorService executorService = Executors.newFixedThreadPool(3);
          List<Future<?>> futures = new ArrayList<>();
          CyclicBarrier barrier1 = new CyclicBarrier(3);
          CyclicBarrier barrier2 = new CyclicBarrier(3);

          try {
            // Act
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier1.await();
                              assertThat(wormhole.put(9, 90)).isNull();
                              barrier2.await();
                              List<Integer> scanned = new ArrayList<>();
                              wormhole.scan(2, 15, true, (k, v) -> scanned.add(v));
                              assertThat(scanned)
                                  .isIn(
                                      Arrays.asList(90, 100, 110, 130),
                                      Arrays.asList(70, 90, 100, 110, 130));
                              return null;
                            })));
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier1.await();
                              List<Integer> scanned = new ArrayList<>();
                              wormhole.scan(6, 16, true, (k, v) -> scanned.add(v));
                              assertThat(scanned)
                                  .isIn(
                                      Arrays.asList(100, 110),
                                      Arrays.asList(90, 100, 110),
                                      Arrays.asList(100, 110, 130),
                                      Arrays.asList(90, 100, 110, 130));
                              barrier2.await();
                              assertThat(wormhole.put(7, 70)).isNull();
                              return null;
                            })));
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier1.await();
                              assertThat(wormhole.put(13, 130)).isNull();
                              barrier2.await();
                              return null;
                            })));

            // Assert
            for (Future<?> future : futures) {
              future.get(5, TimeUnit.SECONDS);
            }
            assertThat(wormhole.get(7)).isEqualTo(70);
            assertThat(wormhole.get(9)).isEqualTo(90);
            assertThat(wormhole.get(10)).isEqualTo(100);
            assertThat(wormhole.get(11)).isEqualTo(110);
            assertThat(wormhole.get(13)).isEqualTo(130);

            return null;
          } finally {
            executorService.shutdown();
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
              executorService.shutdownNow();
            }
          }
        });
  }

  @RepeatedTest(1000)
  void concurrentPutAndGetAndDelete_ShouldReturnProperValues() throws Exception {
    withRegisteredWormhole(
        () -> {
          // Arrange
          assertThat(wormhole.put(10, 100)).isNull();
          assertThat(wormhole.put(11, 110)).isNull();
          assertThat(wormhole.put(9, 90)).isNull();
          assertThat(wormhole.put(7, 70)).isNull();
          assertThat(wormhole.put(6, 60)).isNull();
          assertThat(wormhole.put(8, 80)).isNull();
          assertThat(wormhole.put(5, 50)).isNull();
          assertThat(wormhole.delete(9)).isTrue();

          ExecutorService executorService = Executors.newFixedThreadPool(2);
          List<Future<?>> futures = new ArrayList<>();
          CyclicBarrier barrier1 = new CyclicBarrier(2);
          CyclicBarrier barrier2 = new CyclicBarrier(2);

          try {
            // Act
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier1.await();
                              assertThat(wormhole.put(10, 101)).isEqualTo(100);
                              barrier2.await();
                              assertThat(wormhole.get(10)).isEqualTo(101);
                              return null;
                            })));
            futures.add(
                executorService.submit(
                    () ->
                        withRegisteredWormhole(
                            () -> {
                              barrier1.await();
                              assertThat(wormhole.delete(8)).isTrue();
                              barrier2.await();
                              return null;
                            })));

            // Assert
            for (Future<?> future : futures) {
              future.get(5, TimeUnit.SECONDS);
            }
            assertThat(wormhole.get(5)).isEqualTo(50);
            assertThat(wormhole.get(6)).isEqualTo(60);
            assertThat(wormhole.get(7)).isEqualTo(70);
            assertThat(wormhole.get(8)).isNull();
            assertThat(wormhole.get(9)).isNull();
            assertThat(wormhole.get(10)).isEqualTo(101);
            assertThat(wormhole.get(11)).isEqualTo(110);

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
