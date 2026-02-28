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
  void conflict2Puts_ShouldReturnNullAndExistingValue() throws ExecutionException, InterruptedException {
    // Arrange
    Wormhole<Integer, Integer> wormhole = new WormholeForIntKey<>(8);
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
