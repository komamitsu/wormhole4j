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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class QsbrMapTest {
  private static class Item {
    private final AtomicLong version = new AtomicLong();
    private final AtomicInteger value = new AtomicInteger();

    public Item(long version) {
      this.version.set(version);
    }
  }

  @Test
  void sequentialWriteAndReadInSeparateTransactions() {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key = 42;
    map.handleWriteOperation(
        (version, table) -> {
          Item item = table.get(key);
          assertThat(item).isNull();
          table.put(key, new Item(version));
        });

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleReadOperation(
        table -> {
          Item item = table.get(key);
          assertThat(item.version.get()).isEqualTo(1);
          assertThat(item.value.get()).isEqualTo(0);
        });
  }

  @Test
  void readModifyWriteInTransaction() {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key = 42;
    int value = 7;
    map.handleReadOperation(
        readTable -> {
          Item readItem = readTable.get(key);
          assertThat(readItem).isNull();
          map.handleWriteOperation(
              (version, writeTable) -> {
                Item item = new Item(version);
                item.value.set(value);
                writeTable.put(key, item);
              });
        });

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleReadOperation(
        table -> {
          Item item = table.get(key);
          assertThat(item.version.get()).isEqualTo(1);
          assertThat(item.value.get()).isEqualTo(value);
        });
  }

  @Test
  void readModifyWriteInTransactionWithConcurrentRead() throws InterruptedException {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key = 42;
    int value = 7;

    CountDownLatch readBeforeModifyLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newCachedThreadPool();
    executorService.execute(() -> map.handleReadOperation(
        readTable -> {
          assertThat(readTable.get(key)).isNull();
          readBeforeModifyLatch.countDown();
          sleep(1);
          assertThat(readTable.get(key)).isNull();
        }));

    map.handleReadOperation(
        readTable -> {
          Item readItem = readTable.get(key);
          assertThat(readItem).isNull();
          await(readBeforeModifyLatch);
          map.handleWriteOperation(
              (version, writeTable) -> {
                Item item = new Item(version);
                item.value.set(value);
                writeTable.put(key, item);
              });
        });

    assertThat(map.getVersion()).isEqualTo(1);

    executorService.shutdown();
    assertThat(executorService.awaitTermination(2, TimeUnit.SECONDS)).isTrue();

    map.handleReadOperation(
        table -> {
          Item item = table.get(key);
          assertThat(item.version.get()).isEqualTo(1);
          assertThat(item.value.get()).isEqualTo(value);
        });
  }

  private void sleep(int seconds) {
    try {
      TimeUnit.SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private void await(CountDownLatch countDownLatch) {
    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
}
