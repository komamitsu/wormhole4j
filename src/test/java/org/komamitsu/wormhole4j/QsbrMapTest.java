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
import static org.assertj.core.api.Assertions.fail;

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
  void separateSequentialWriteAndRead_ShouldReturnProperValue() {
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
  void singleReadModifyWriteInTransaction_ShouldReturnProperValue() {
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
  void multipleReadModifyWrite_ShouldReturnProperValue() {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key = 42;
    map.handleReadOperation(
        readTable -> {
          Item readItem = readTable.get(key);
          assertThat(readItem).isNull();
          map.handleWriteOperation(
              (version, writeTable) -> {
                Item item = new Item(version);
                item.value.set(10);
                writeTable.put(key, item);
              });
        });

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleReadOperation(
        readTable -> {
          Item readItem = readTable.get(key);
          assertThat(readItem).isNotNull();
          assertThat(readItem.version.get()).isEqualTo(1);
          assertThat(readItem.value.get()).isEqualTo(10);
          map.handleWriteOperation(
              (version, writeTable) -> {
                Item item = new Item(version);
                item.value.set(20);
                writeTable.put(key, item);
              });
        });

    assertThat(map.getVersion()).isEqualTo(2);

    map.handleReadOperation(
        readTable -> {
          Item readItem = readTable.get(key);
          assertThat(readItem).isNotNull();
          assertThat(readItem.version.get()).isEqualTo(2);
          assertThat(readItem.value.get()).isEqualTo(20);
          map.handleWriteOperation(
              (version, writeTable) -> {
                Item item = new Item(version);
                item.value.set(30);
                writeTable.put(key, item);
              });
        });

    assertThat(map.getVersion()).isEqualTo(3);

    map.handleReadOperation(
        table -> {
          Item item = table.get(key);
          assertThat(item.version.get()).isEqualTo(3);
          assertThat(item.value.get()).isEqualTo(30);
        });
  }

  @Test
  void readModifyWriteWithConcurrentRead_ShouldReturnProperValue() throws InterruptedException {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key = 42;
    int value = 7;

    CountDownLatch readBeforeModifyLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newCachedThreadPool();
    executorService.execute(
        () ->
            map.handleReadOperation(
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

  @Test
  void writeWithConcurrentRead_ShouldNotBlockRead() throws InterruptedException {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key = 42;
    int value = 7;

    CountDownLatch readBeforeModifyLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newCachedThreadPool();
    executorService.execute(
        () ->
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
                }));

    map.handleReadOperation(readTable -> assertThat(readTable.get(key)).isNull());

    assertThat(map.getVersion()).isEqualTo(0);

    readBeforeModifyLatch.countDown();

    executorService.shutdown();
    assertThat(executorService.awaitTermination(2, TimeUnit.SECONDS)).isTrue();

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleReadOperation(
        table -> {
          Item item = table.get(key);
          assertThat(item.version.get()).isEqualTo(1);
          assertThat(item.value.get()).isEqualTo(value);
        });
  }

  @Test
  void whenWriteFails_ShouldRevertOperation() {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key = 42;
    try {
      map.handleWriteOperation(
          (version, table) -> {
            Item item = table.get(key);
            assertThat(item).isNull();
            table.put(key, new Item(version));
            throw new RuntimeException("foo bar");
          });
      fail();
    } catch (RuntimeException e) {
      assertThat(e.getMessage()).isEqualTo("foo bar");
    }

    assertThat(map.getVersion()).isEqualTo(0);

    map.handleReadOperation(table -> assertThat(table.get(key)).isNull());
  }

  @Test
  void separateSequentialWriteAndRemove_ShouldRemoveIt() {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key = 42;
    map.handleWriteOperation(
        (version, table) -> {
          Item item = table.get(key);
          assertThat(item).isNull();
          Item newItem = new Item(version);
          newItem.value.set(10);
          table.put(key, newItem);
        });

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleWriteOperation(
        (version, table) -> {
          Item item = table.remove(key);
          assertThat(item).isNotNull();
          assertThat(item.version.get()).isEqualTo(1);
          assertThat(item.value.get()).isEqualTo(10);
        });

    assertThat(map.getVersion()).isEqualTo(2);

    map.handleReadOperation(
        table -> {
          Item item = table.get(key);
          assertThat(item).isNull();
        });
  }

  @Test
  void singleReadAndRemoveInTransaction_ShouldRemoveIt() {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key = 42;
    int value = 7;
    map.handleWriteOperation(
        (version, table) -> {
          Item item = table.get(key);
          assertThat(item).isNull();
          Item newItem = new Item(version);
          newItem.value.set(value);
          table.put(key, newItem);
        });

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleReadOperation(
        readTable -> {
          Item readItem = readTable.get(key);
          assertThat(readItem.version.get()).isEqualTo(1);
          assertThat(readItem.value.get()).isEqualTo(value);
          map.handleWriteOperation((version, writeTable) -> writeTable.remove(key));
        });

    assertThat(map.getVersion()).isEqualTo(2);

    map.handleReadOperation(
        table -> {
          Item item = table.get(key);
          assertThat(item).isNull();
        });
  }

  @Test
  void multipleReadModifyWriteAndRemove_ShouldReturnProperValue() {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    map.handleReadOperation(
        readTable -> {
          Item readItem = readTable.get(1);
          assertThat(readItem).isNull();
          map.handleWriteOperation(
              (version, writeTable) -> {
                Item item = new Item(version);
                item.value.set(10);
                writeTable.put(1, item);
              });
        });

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleReadOperation(
        readTable -> {
          Item readItem = readTable.get(1);
          assertThat(readItem).isNotNull();
          assertThat(readItem.version.get()).isEqualTo(1);
          assertThat(readItem.value.get()).isEqualTo(10);
          map.handleWriteOperation(
              (version, writeTable) -> {
                writeTable.remove(1);
                Item item = new Item(version);
                item.value.set(20);
                writeTable.put(2, item);
              });
        });

    assertThat(map.getVersion()).isEqualTo(2);

    map.handleReadOperation(
        readTable -> {
          assertThat(readTable.get(1)).isNull();
          Item readItem = readTable.get(2);
          assertThat(readItem).isNotNull();
          assertThat(readItem.version.get()).isEqualTo(2);
          assertThat(readItem.value.get()).isEqualTo(20);
          map.handleWriteOperation(
              (version, writeTable) -> {
                writeTable.remove(2);
                Item item = new Item(version);
                item.value.set(30);
                writeTable.put(3, item);
              });
        });

    assertThat(map.getVersion()).isEqualTo(3);

    map.handleReadOperation(
        table -> {
          assertThat(table.get(1)).isNull();
          assertThat(table.get(2)).isNull();
          Item item = table.get(3);
          assertThat(item.version.get()).isEqualTo(3);
          assertThat(item.value.get()).isEqualTo(30);
        });
  }

  @Test
  void readAndRemoveWithConcurrentRead_ShouldReturnProperValue() throws InterruptedException {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key = 42;
    int value = 7;

    map.handleWriteOperation(
        (version, table) -> {
          Item item = table.get(key);
          assertThat(item).isNull();
          Item newItem = new Item(version);
          newItem.value.set(value);
          table.put(key, newItem);
        });
    assertThat(map.getVersion()).isEqualTo(1);

    CountDownLatch readBeforeRemoveLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newCachedThreadPool();
    executorService.execute(
        () ->
            map.handleReadOperation(
                readTable -> {
                  assertThat(readTable.get(key)).isNotNull();
                  readBeforeRemoveLatch.countDown();
                  sleep(1);
                  assertThat(readTable.get(key)).isNotNull();
                }));

    map.handleReadOperation(
        readTable -> {
          Item readItem = readTable.get(key);
          assertThat(readItem).isNotNull();
          await(readBeforeRemoveLatch);
          map.handleWriteOperation((version, writeTable) -> writeTable.remove(key));
        });

    assertThat(map.getVersion()).isEqualTo(2);

    executorService.shutdown();
    assertThat(executorService.awaitTermination(2, TimeUnit.SECONDS)).isTrue();

    map.handleReadOperation(table -> assertThat(table.get(key)).isNull());
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
