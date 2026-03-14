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

import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;

class MultiThreadWormholeTest {
  @Test
  void putNewKeysAfterSplit_ShouldReturnNull() {
    // Arrange
    Wormhole<Integer, Integer> wormhole = new WormholeForIntKey.Builder<Integer>()
        .setThreadSafe(true)
        .setLeafNodeSize(4)
        .build();
    wormhole.registerThread();

    // Act Assert
    try {
      assertThat(wormhole.put(10, 100)).isNull();
      assertThat(wormhole.put(11, 110)).isNull();
      assertThat(wormhole.put(9, 90)).isNull();
      assertThat(wormhole.put(12, 120)).isNull();
      assertThat(wormhole.put(8, 80)).isNull();
    }
    finally {
      wormhole.unregisterThread();
    }
  }

  @Test
  void conflict2Puts_ShouldReturnNullAndExistingValue() throws ExecutionException, InterruptedException {
    for (int i = 0; i < 100; i++) {
      // Arrange
      Wormhole<Integer, Integer> wormhole = new WormholeForIntKey.Builder<Integer>()
          .setThreadSafe(true)
          .setLeafNodeSize(4)
          .build();
      ExecutorService executorService = Executors.newFixedThreadPool(2);
      List<Future<Integer>> futures = new ArrayList<>();

      // Act
      futures.add(executorService.submit(() -> {
        wormhole.registerThread();
        Integer existingValue = wormhole.put(1, 10);
        wormhole.unregisterThread();
        return existingValue;
      }));
      futures.add(executorService.submit(() -> {
        wormhole.registerThread();
        Integer existingValue = wormhole.put(1, 20);
        wormhole.unregisterThread();
        return existingValue;
      }));

      // Assert
      List<Integer> resultValues = new ArrayList<>();
      for (Future<Integer> future : futures) {
        Integer resultValue = future.get();
        if (resultValue != null) {
          resultValues.add(resultValue);
        }
      }
      assertThat(resultValues).hasSize(1);
      assertThat(resultValues.getFirst()).isIn(10, 20);
      executorService.shutdown();

      wormhole.registerThread();
      assertThat(wormhole.get(1)).isIn(10, 20);
      wormhole.unregisterThread();
    }
  }

  @Test
  void concurrent2PutsAfterSplit_ShouldReturnProperValues_1() throws ExecutionException, InterruptedException {
    for (int i = 0; i < 100; i++) {
      // Arrange
      Wormhole<Integer, Integer> wormhole = new WormholeForIntKey.Builder<Integer>()
          .setThreadSafe(true)
          .setLeafNodeSize(4)
          .build();
      wormhole.registerThread();
      assertThat(wormhole.put(8, 80)).isNull();
      assertThat(wormhole.put(9, 90)).isNull();
      assertThat(wormhole.put(10, 100)).isNull();
      assertThat(wormhole.put(11, 110)).isNull();

      ExecutorService executorService = Executors.newFixedThreadPool(2);
      List<Future<Integer>> futures = new ArrayList<>();
      CyclicBarrier barrier = new CyclicBarrier(2);

      // Act
      futures.add(executorService.submit(() -> {
        wormhole.registerThread();
        barrier.await();
        Integer existingValue = wormhole.put(9, 99);
        wormhole.unregisterThread();
        return existingValue;
      }));
      futures.add(executorService.submit(() -> {
        wormhole.registerThread();
        barrier.await();
        Integer existingValue = wormhole.put(12, 120);
        wormhole.unregisterThread();
        return existingValue;
      }));

      // Assert
      List<Integer> resultValues = new ArrayList<>();
      for (Future<Integer> future : futures) {
        Integer resultValue = future.get();
        if (resultValue != null) {
          resultValues.add(resultValue);
        }
      }
      assertThat(resultValues).hasSize(1);
      assertThat(resultValues.getFirst()).isEqualTo(90);
      executorService.shutdown();

      assertThat(wormhole.get(8)).isEqualTo(80);
      assertThat(wormhole.get(9)).isEqualTo(99);
      assertThat(wormhole.get(10)).isEqualTo(100);
      assertThat(wormhole.get(11)).isEqualTo(110);
      assertThat(wormhole.get(12)).isEqualTo(120);
      wormhole.unregisterThread();
    }
  }

  @Test
  void concurrent2PutsAfterSplit_ShouldReturnProperValues_2() throws ExecutionException, InterruptedException {
    for (int i = 0; i < 100; i++) {
      System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA: " + i);
      // Arrange
      Wormhole<Integer, Integer> wormhole = new WormholeForIntKey.Builder<Integer>()
          .setThreadSafe(true)
          .setLeafNodeSize(4)
          .build();
      wormhole.registerThread();
      assertThat(wormhole.put(10, 100)).isNull();
      assertThat(wormhole.put(11, 110)).isNull();
      assertThat(wormhole.put(12, 120)).isNull();
      assertThat(wormhole.put(9, 90)).isNull();

      ExecutorService executorService = Executors.newFixedThreadPool(2);
      List<Future<Integer>> futures = new ArrayList<>();
      CyclicBarrier barrier = new CyclicBarrier(2);

      // Act
      futures.add(executorService.submit(() -> {
        wormhole.registerThread();
        barrier.await();
        Integer existingValue = wormhole.put(8, 80);
        wormhole.unregisterThread();
        return existingValue;
      }));
      futures.add(executorService.submit(() -> {
        wormhole.registerThread();
        barrier.await();
        Integer existingValue = wormhole.put(11, 111);
        wormhole.unregisterThread();
        return existingValue;
      }));

      // Assert
      List<Integer> resultValues = new ArrayList<>();
      for (Future<Integer> future : futures) {
        Integer resultValue = future.get();
        if (resultValue != null) {
          resultValues.add(resultValue);
        }
      }
      assertThat(resultValues).hasSize(1);
      assertThat(resultValues.getFirst()).isEqualTo(110);
      executorService.shutdown();

      assertThat(wormhole.get(8)).isEqualTo(80);
      assertThat(wormhole.get(9)).isEqualTo(90);
      assertThat(wormhole.get(10)).isEqualTo(100);
      assertThat(wormhole.get(11)).isEqualTo(111);
      assertThat(wormhole.get(12)).isEqualTo(120);
      wormhole.unregisterThread();
    }
  }
}
