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

import static org.assertj.core.api.Assertions.*;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class QsbrMapTest {
  private Set<Integer> randomIntegers;

  private static class Item {
    private final AtomicLong version = new AtomicLong();
    private final AtomicInteger value = new AtomicInteger();

    public Item(long version, int value) {
      this.version.set(version);
      this.value.set(value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Item item = (Item) o;
      return Objects.equals(version.get(), item.version.get())
          && Objects.equals(value.get(), item.value.get());
    }

    @Override
    public int hashCode() {
      return Objects.hash(version.get(), value.get());
    }

    @Override
    public String toString() {
      return "Item{" + "version=" + version + ", value=" + value + '}';
    }
  }

  @BeforeEach
  void beforeEach() {
    randomIntegers = new HashSet<>();
  }

  private int getRandomInt() {
    while (true) {
      int i = ThreadLocalRandom.current().nextInt();
      if (randomIntegers.add(i)) {
        return i;
      }
    }
  }

  @Test
  void separateSequentialPutAndGet_ShouldReturnProperValue() {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key = getRandomInt();
    int value = getRandomInt();

    map.handleWriteOperation(
        (version, table) -> {
          assertThat(table.get(key)).isNull();
          table.put(key, new Item(version, value));
        });

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleReadOperation(table -> assertThat(table.get(key)).isEqualTo(new Item(1, value)));
  }

  @Test
  void singleGetModifyPutInTransaction_ShouldReturnProperValue() {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key = getRandomInt();
    int value = getRandomInt();

    map.handleReadOperation(
        readTable -> {
          assertThat(readTable.get(key)).isNull();
          map.handleWriteOperation(
              (version, writeTable) -> writeTable.put(key, new Item(version, value)));
        });

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleReadOperation(table -> assertThat(table.get(key)).isEqualTo(new Item(1, value)));
  }

  @Test
  void multipleGetModifyPut_ShouldReturnProperValue() {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key = getRandomInt();
    int value1 = getRandomInt();
    int value2 = getRandomInt();
    int value3 = getRandomInt();

    map.handleReadOperation(
        readTable -> {
          assertThat(readTable.get(key)).isNull();
          map.handleWriteOperation(
              (version, writeTable) -> writeTable.put(key, new Item(version, value1)));
        });

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleReadOperation(
        readTable -> {
          assertThat(readTable.get(key)).isEqualTo(new Item(1, value1));
          map.handleWriteOperation(
              (version, writeTable) -> writeTable.put(key, new Item(version, value2)));
        });

    assertThat(map.getVersion()).isEqualTo(2);

    map.handleReadOperation(
        readTable -> {
          assertThat(readTable.get(key)).isEqualTo(new Item(2, value2));
          map.handleWriteOperation(
              (version, writeTable) -> writeTable.put(key, new Item(version, value3)));
        });

    assertThat(map.getVersion()).isEqualTo(3);

    map.handleReadOperation(table -> assertThat(table.get(key)).isEqualTo(new Item(3, value3)));
  }

  @Test
  void getModifyPutWithConcurrentGet_ShouldReturnProperValue() throws InterruptedException {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key = getRandomInt();
    int value = getRandomInt();

    CountDownLatch readBeforeModifyLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newCachedThreadPool();
    executorService.execute(
        () ->
            map.handleReadOperation(
                readTable -> {
                  assertThat(readTable.get(key)).isNull();
                  readBeforeModifyLatch.countDown();
                  sleep(1000);
                  assertThat(readTable.get(key)).isNull();
                }));

    map.handleReadOperation(
        readTable -> {
          assertThat(readTable.get(key)).isNull();
          await(readBeforeModifyLatch);
          map.handleWriteOperation(
              (version, writeTable) -> writeTable.put(key, new Item(version, value)));
        });

    assertThat(map.getVersion()).isEqualTo(1);

    executorService.shutdown();
    assertThat(executorService.awaitTermination(2, TimeUnit.SECONDS)).isTrue();

    map.handleReadOperation(table -> assertThat(table.get(key)).isEqualTo(new Item(1, value)));
  }

  @Test
  void putWithConcurrentGet_ShouldNotBlockGet() throws InterruptedException {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key = getRandomInt();
    int value = getRandomInt();

    CountDownLatch readBeforeModifyLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newCachedThreadPool();
    executorService.execute(
        () ->
            map.handleReadOperation(
                readTable -> {
                  assertThat(readTable.get(key)).isNull();
                  await(readBeforeModifyLatch);
                  map.handleWriteOperation(
                      (version, writeTable) -> writeTable.put(key, new Item(version, value)));
                }));

    map.handleReadOperation(readTable -> assertThat(readTable.get(key)).isNull());

    assertThat(map.getVersion()).isEqualTo(0);

    readBeforeModifyLatch.countDown();

    executorService.shutdown();
    assertThat(executorService.awaitTermination(2, TimeUnit.SECONDS)).isTrue();

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleReadOperation(table -> assertThat(table.get(key)).isEqualTo(new Item(1, value)));
  }

  @Test
  void separateSequentialPutAllAndGet_ShouldReturnProperValue() {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key1 = getRandomInt();
    int value1 = getRandomInt();
    int key2 = getRandomInt();
    int value2 = getRandomInt();

    map.handleWriteOperation(
        (version, table) -> {
          Map<Integer, Item> src = new HashMap<>();
          src.put(key1, new Item(version, value1));
          src.put(key2, new Item(version, value2));
          table.putAll(src);
        });

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleReadOperation(
        table -> {
          assertThat(table.get(key1)).isEqualTo(new Item(1, value1));
          assertThat(table.get(key2)).isEqualTo(new Item(1, value2));
        });
  }

  @Test
  void singleGetModifyPutAllInTransaction_ShouldReturnProperValue() {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key1 = getRandomInt();
    int value1 = getRandomInt();
    int key2 = getRandomInt();
    int value2 = getRandomInt();

    map.handleReadOperation(
        readTable ->
            map.handleWriteOperation(
                (version, writeTable) -> {
                  Map<Integer, Item> src = new HashMap<>();
                  src.put(key1, new Item(version, value1));
                  src.put(key2, new Item(version, value2));
                  writeTable.putAll(src);
                }));

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleReadOperation(
        table -> {
          assertThat(table.get(key1)).isEqualTo(new Item(1, value1));
          assertThat(table.get(key2)).isEqualTo(new Item(1, value2));
        });
  }

  @Test
  void multipleGetModifyPutAll_ShouldReturnProperValue() {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key1 = getRandomInt();
    int value1_1 = getRandomInt();
    int value1_2 = getRandomInt();
    int value1_3 = getRandomInt();
    int key2 = getRandomInt();
    int value2_1 = getRandomInt();
    int value2_2 = getRandomInt();
    int value2_3 = getRandomInt();

    map.handleReadOperation(
        readTable ->
            map.handleWriteOperation(
                (version, writeTable) -> {
                  Map<Integer, Item> src = new HashMap<>();
                  src.put(key1, new Item(version, value1_1));
                  src.put(key2, new Item(version, value2_1));
                  writeTable.putAll(src);
                }));

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleReadOperation(
        readTable -> {
          assertThat(readTable.get(key1)).isEqualTo(new Item(1, value1_1));
          assertThat(readTable.get(key2)).isEqualTo(new Item(1, value2_1));
          map.handleWriteOperation(
              (version, writeTable) -> {
                Map<Integer, Item> src = new HashMap<>();
                src.put(key1, new Item(version, value1_2));
                src.put(key2, new Item(version, value2_2));
                writeTable.putAll(src);
              });
        });

    assertThat(map.getVersion()).isEqualTo(2);

    map.handleReadOperation(
        readTable -> {
          assertThat(readTable.get(key1)).isEqualTo(new Item(2, value1_2));
          assertThat(readTable.get(key2)).isEqualTo(new Item(2, value2_2));
          map.handleWriteOperation(
              (version, writeTable) -> {
                Map<Integer, Item> src = new HashMap<>();
                src.put(key1, new Item(version, value1_3));
                src.put(key2, new Item(version, value2_3));
                writeTable.putAll(src);
              });
        });

    assertThat(map.getVersion()).isEqualTo(3);

    map.handleReadOperation(
        table -> {
          assertThat(table.get(key1)).isEqualTo(new Item(3, value1_3));
          assertThat(table.get(key2)).isEqualTo(new Item(3, value2_3));
        });
  }

  @Test
  void getModifyPutAllWithConcurrentGet_ShouldReturnProperValue() throws InterruptedException {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key1 = getRandomInt();
    int value1 = getRandomInt();
    int key2 = getRandomInt();
    int value2 = getRandomInt();

    CountDownLatch readBeforeModifyLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newCachedThreadPool();
    executorService.execute(
        () ->
            map.handleReadOperation(
                readTable -> {
                  assertThat(readTable.get(key1)).isNull();
                  assertThat(readTable.get(key2)).isNull();
                  readBeforeModifyLatch.countDown();
                  sleep(1000);
                  assertThat(readTable.get(key1)).isNull();
                  assertThat(readTable.get(key2)).isNull();
                }));

    map.handleReadOperation(
        readTable -> {
          assertThat(readTable.get(key1)).isNull();
          assertThat(readTable.get(key2)).isNull();
          await(readBeforeModifyLatch);
          map.handleWriteOperation(
              (version, writeTable) -> {
                Map<Integer, Item> src = new HashMap<>();
                src.put(key1, new Item(version, value1));
                src.put(key2, new Item(version, value2));
                writeTable.putAll(src);
              });
        });

    assertThat(map.getVersion()).isEqualTo(1);

    executorService.shutdown();
    assertThat(executorService.awaitTermination(2, TimeUnit.SECONDS)).isTrue();

    map.handleReadOperation(
        table -> {
          assertThat(table.get(key1)).isEqualTo(new Item(1, value1));
          assertThat(table.get(key2)).isEqualTo(new Item(1, value2));
        });
  }

  @Test
  void putAllWithConcurrentGet_ShouldNotBlockGet() throws InterruptedException {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key1 = getRandomInt();
    int value1 = getRandomInt();
    int key2 = getRandomInt();
    int value2 = getRandomInt();

    CountDownLatch readBeforeModifyLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newCachedThreadPool();
    executorService.execute(
        () ->
            map.handleReadOperation(
                readTable -> {
                  assertThat(readTable.get(key1)).isNull();
                  assertThat(readTable.get(key2)).isNull();
                  await(readBeforeModifyLatch);
                  map.handleWriteOperation(
                      (version, writeTable) -> {
                        Map<Integer, Item> src = new HashMap<>();
                        src.put(key1, new Item(version, value1));
                        src.put(key2, new Item(version, value2));
                        writeTable.putAll(src);
                      });
                }));

    map.handleReadOperation(
        readTable -> {
          assertThat(readTable.get(key1)).isNull();
          assertThat(readTable.get(key2)).isNull();
        });

    assertThat(map.getVersion()).isEqualTo(0);

    readBeforeModifyLatch.countDown();

    executorService.shutdown();
    assertThat(executorService.awaitTermination(2, TimeUnit.SECONDS)).isTrue();

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleReadOperation(
        table -> {
          assertThat(table.get(key1)).isEqualTo(new Item(1, value1));
          assertThat(table.get(key2)).isEqualTo(new Item(1, value2));
        });
  }

  @Test
  void separateSequentialPutAndRemove_ShouldRemoveIt() {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key = getRandomInt();
    int value1 = getRandomInt();

    map.handleWriteOperation(
        (version, table) -> {
          assertThat(table.get(key)).isNull();
          table.put(key, new Item(version, value1));
        });

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleWriteOperation(
        (version, table) -> assertThat(table.remove(key)).isEqualTo(new Item(1, value1)));

    assertThat(map.getVersion()).isEqualTo(2);

    map.handleReadOperation(table -> assertThat(table.get(key)).isNull());
  }

  @Test
  void singleGetAndRemoveInTransaction_ShouldRemoveIt() {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key = getRandomInt();
    int value = getRandomInt();
    map.handleWriteOperation(
        (version, table) -> {
          assertThat(table.get(key)).isNull();
          table.put(key, new Item(version, value));
        });

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleReadOperation(
        readTable -> {
          assertThat(readTable.get(key)).isEqualTo(new Item(1, value));
          map.handleWriteOperation((version, writeTable) -> writeTable.remove(key));
        });

    assertThat(map.getVersion()).isEqualTo(2);

    map.handleReadOperation(table -> assertThat(table.get(key)).isNull());
  }

  @Test
  void multipleGetModifyPutAndRemove_ShouldReturnProperValue() {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key1 = getRandomInt();
    int value1 = getRandomInt();
    int key2 = getRandomInt();
    int value2 = getRandomInt();
    int key3 = getRandomInt();
    int value3 = getRandomInt();

    map.handleReadOperation(
        readTable -> {
          assertThat(readTable.get(key1)).isNull();
          map.handleWriteOperation(
              (version, writeTable) -> writeTable.put(key1, new Item(version, value1)));
        });

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleReadOperation(
        readTable -> {
          assertThat(readTable.get(key1)).isEqualTo(new Item(1, value1));
          map.handleWriteOperation(
              (version, writeTable) -> {
                writeTable.remove(key1);
                writeTable.put(key2, new Item(version, value2));
              });
        });

    assertThat(map.getVersion()).isEqualTo(2);

    map.handleReadOperation(
        readTable -> {
          assertThat(readTable.get(key1)).isNull();
          assertThat(readTable.get(key2)).isEqualTo(new Item(2, value2));
          map.handleWriteOperation(
              (version, writeTable) -> {
                writeTable.remove(key2);
                writeTable.put(key3, new Item(version, value3));
              });
        });

    assertThat(map.getVersion()).isEqualTo(3);

    map.handleReadOperation(
        table -> {
          assertThat(table.get(key1)).isNull();
          assertThat(table.get(key2)).isNull();
          assertThat(table.get(key3)).isEqualTo(new Item(3, value3));
        });
  }

  @Test
  void getAndRemoveWithConcurrentGet_ShouldReturnProperValue() throws InterruptedException {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key = getRandomInt();
    int value = getRandomInt();

    map.handleWriteOperation(
        (version, table) -> {
          assertThat(table.get(key)).isNull();
          table.put(key, new Item(version, value));
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
                  sleep(1000);
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

  @Test
  void separateSequentialPutAndClear_ShouldRemoveIt() {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key = getRandomInt();
    int value1 = getRandomInt();

    map.handleWriteOperation(
        (version, table) -> {
          assertThat(table.get(key)).isNull();
          table.put(key, new Item(version, value1));
        });

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleWriteOperation((version, table) -> table.clear());

    assertThat(map.getVersion()).isEqualTo(2);

    map.handleReadOperation(table -> assertThat(table.get(key)).isNull());
  }

  @Test
  void singleGetAndClearInTransaction_ShouldRemoveIt() {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key = getRandomInt();
    int value = getRandomInt();
    map.handleWriteOperation(
        (version, table) -> {
          assertThat(table.get(key)).isNull();
          table.put(key, new Item(version, value));
        });

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleReadOperation(
        readTable -> {
          assertThat(readTable.get(key)).isEqualTo(new Item(1, value));
          map.handleWriteOperation((version, writeTable) -> writeTable.clear());
        });

    assertThat(map.getVersion()).isEqualTo(2);

    map.handleReadOperation(table -> assertThat(table.get(key)).isNull());
  }

  @Test
  void multipleGetModifyPutAndClear_ShouldReturnProperValue() {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key1 = getRandomInt();
    int value1 = getRandomInt();
    int key2 = getRandomInt();
    int value2 = getRandomInt();
    int key3 = getRandomInt();
    int value3 = getRandomInt();

    map.handleReadOperation(
        readTable -> {
          assertThat(readTable.get(key1)).isNull();
          map.handleWriteOperation(
              (version, writeTable) -> writeTable.put(key1, new Item(version, value1)));
        });

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleReadOperation(
        readTable -> {
          assertThat(readTable.get(key1)).isEqualTo(new Item(1, value1));
          map.handleWriteOperation(
              (version, writeTable) -> {
                writeTable.clear();
                writeTable.put(key2, new Item(version, value2));
              });
        });

    assertThat(map.getVersion()).isEqualTo(2);

    map.handleReadOperation(
        readTable -> {
          assertThat(readTable.get(key1)).isNull();
          assertThat(readTable.get(key2)).isEqualTo(new Item(2, value2));
          map.handleWriteOperation(
              (version, writeTable) -> {
                writeTable.clear();
                writeTable.put(key3, new Item(version, value3));
              });
        });

    assertThat(map.getVersion()).isEqualTo(3);

    map.handleReadOperation(
        table -> {
          assertThat(table.get(key1)).isNull();
          assertThat(table.get(key2)).isNull();
          assertThat(table.get(key3)).isEqualTo(new Item(3, value3));
        });
  }

  @Test
  void getAndClearWithConcurrentGet_ShouldReturnProperValue() throws InterruptedException {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key = getRandomInt();
    int value = getRandomInt();

    map.handleWriteOperation(
        (version, table) -> {
          assertThat(table.get(key)).isNull();
          table.put(key, new Item(version, value));
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
                  sleep(1000);
                  assertThat(readTable.get(key)).isNotNull();
                }));

    map.handleReadOperation(
        readTable -> {
          Item readItem = readTable.get(key);
          assertThat(readItem).isNotNull();
          await(readBeforeRemoveLatch);
          map.handleWriteOperation((version, writeTable) -> writeTable.clear());
        });

    assertThat(map.getVersion()).isEqualTo(2);

    executorService.shutdown();
    assertThat(executorService.awaitTermination(2, TimeUnit.SECONDS)).isTrue();

    map.handleReadOperation(table -> assertThat(table.get(key)).isNull());
  }

  @Test
  void multipleOperationWrite_whenSucceeds_ShouldMakeAllOperationsVisible() {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key1 = getRandomInt();
    int value1_init = getRandomInt();
    int value1_putAll = getRandomInt();
    int key2 = getRandomInt();
    int value2_putAll = getRandomInt();
    int key3 = getRandomInt();
    int value3_putAll = getRandomInt();
    int value3_put = getRandomInt();

    map.handleWriteOperation((version, table) -> table.put(key1, new Item(version, value1_init)));

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleWriteOperation(
        (version, table) -> {
          table.clear();
          assertThat(table.get(key1)).isNull();

          Map<Integer, Item> src = new HashMap<>();
          src.put(key1, new Item(version, value1_putAll));
          src.put(key2, new Item(version, value2_putAll));
          src.put(key3, new Item(version, value3_putAll));
          table.putAll(src);

          assertThat(table.get(key1)).isEqualTo(new Item(version, value1_putAll));
          assertThat(table.get(key2)).isEqualTo(new Item(version, value2_putAll));
          assertThat(table.get(key3)).isEqualTo(new Item(version, value3_putAll));

          assertThat(table.remove(key2)).isEqualTo(new Item(version, value2_putAll));

          assertThat(table.get(key1)).isEqualTo(new Item(version, value1_putAll));
          assertThat(table.get(key2)).isNull();
          assertThat(table.get(key3)).isEqualTo(new Item(version, value3_putAll));

          assertThat(table.put(key3, new Item(version, value3_put)))
              .isEqualTo(new Item(version, value3_putAll));

          assertThat(table.get(key1)).isEqualTo(new Item(version, value1_putAll));
          assertThat(table.get(key2)).isNull();
          assertThat(table.get(key3)).isEqualTo(new Item(version, value3_put));
        });

    assertThat(map.getVersion()).isEqualTo(2);

    map.handleReadOperation(
        table -> {
          assertThat(table.get(key1)).isEqualTo(new Item(2, value1_putAll));
          assertThat(table.get(key2)).isNull();
          assertThat(table.get(key3)).isEqualTo(new Item(2, value3_put));
        });
  }

  @Test
  void multipleOperationWrite_whenFails_ShouldRevertAllOperations() {
    QsbrMap<Integer, Item> map = new QsbrMap<>();

    assertThat(map.getVersion()).isEqualTo(0);

    int key1 = getRandomInt();
    int value1_init = getRandomInt();
    int value1_putAll = getRandomInt();
    int key2 = getRandomInt();
    int value2_putAll = getRandomInt();
    int key3 = getRandomInt();
    int value3_putAll = getRandomInt();
    int value3_put = getRandomInt();

    map.handleWriteOperation((version, table) -> table.put(key1, new Item(version, value1_init)));

    assertThat(map.getVersion()).isEqualTo(1);

    try {
      map.handleWriteOperation(
          (version, table) -> {
            table.clear();
            assertThat(table.get(key1)).isNull();

            Map<Integer, Item> src = new HashMap<>();
            src.put(key1, new Item(version, value1_putAll));
            src.put(key2, new Item(version, value2_putAll));
            src.put(key3, new Item(version, value3_putAll));
            table.putAll(src);

            assertThat(table.get(key1)).isEqualTo(new Item(version, value1_putAll));
            assertThat(table.get(key2)).isEqualTo(new Item(version, value2_putAll));
            assertThat(table.get(key3)).isEqualTo(new Item(version, value3_putAll));

            assertThat(table.remove(key2)).isEqualTo(new Item(version, value2_putAll));

            assertThat(table.get(key1)).isEqualTo(new Item(version, value1_putAll));
            assertThat(table.get(key2)).isNull();
            assertThat(table.get(key3)).isEqualTo(new Item(version, value3_putAll));

            assertThat(table.put(key3, new Item(version, value3_put)))
                .isEqualTo(new Item(version, value3_putAll));

            assertThat(table.get(key1)).isEqualTo(new Item(version, value1_putAll));
            assertThat(table.get(key2)).isNull();
            assertThat(table.get(key3)).isEqualTo(new Item(version, value3_put));

            throw new RuntimeException("foo bar");
          });
      fail();
    } catch (RuntimeException e) {
      assertThat(e.getMessage()).isEqualTo("foo bar");
    }

    assertThat(map.getVersion()).isEqualTo(1);

    map.handleReadOperation(
        table -> {
          assertThat(table.get(key1)).isEqualTo(new Item(1, value1_init));
          assertThat(table.get(key2)).isNull();
          assertThat(table.get(key3)).isNull();
        });
  }

  private static class Account {
    public long version;
    public int balance;

    public Account(long version, int balance) {
      this.version = version;
      this.balance = balance;
    }

    @Override
    public String toString() {
      return "Account{" + "version=" + version + ", balance=" + balance + '}';
    }
  }

  @Test
  void multiThreadOperations_ShouldReachProperState()
      throws ExecutionException, InterruptedException {
    //    int threadCount = 8;
    int threadCount = 2;
    int accountCount = 10;
    int maxAmount = 100;
    //    int durationMillis = 5000;
    int durationMillis = 10;

    QsbrMap<Integer, Account> map = new QsbrMap<>();

    Runnable mutableTransferOp =
        () -> {
          int fromAccountId = ThreadLocalRandom.current().nextInt(accountCount);
          int toAccountId = ThreadLocalRandom.current().nextInt(accountCount);
          int amount = ThreadLocalRandom.current().nextInt(maxAmount);
          map.handleReadOperation(
              (readTable) -> {
                debugPrint(
                    String.format("MUTABLE: Read-Start. Table:%s, Amount:%d", readTable, amount));
                Account fromAccount = readTable.get(fromAccountId);
                Account toAccount = readTable.get(toAccountId);
                int currentFromAccountBalance = fromAccount == null ? 0 : fromAccount.balance;
                int currentToAccountBalance = toAccount == null ? 0 : toAccount.balance;
                debugPrint(" <before>");
                debugPrint(String.format("  %d => %s", fromAccountId, currentFromAccountBalance));
                debugPrint(String.format("  %d => %s", toAccountId, currentToAccountBalance));
                map.handleWriteOperation(
                    (version, writeTable) -> {
                      debugPrint(
                          String.format(
                              "MUTABLE: Write-Start. Version:%d, Table:%s, Amount:%d",
                              version, writeTable, amount));
                      if (fromAccount == null) {
                        writeTable.put(
                            fromAccountId,
                            new Account(version, currentFromAccountBalance - amount));
                      } else {
                        fromAccount.balance -= amount;
                      }
                      if (toAccount == null) {
                        writeTable.put(
                            toAccountId, new Account(version, currentToAccountBalance + amount));
                      } else {
                        toAccount.balance += amount;
                      }
                      debugPrint(" <after>");
                      debugPrint(
                          String.format(
                              "  %d => %s", fromAccountId, currentFromAccountBalance - amount));
                      debugPrint(
                          String.format(
                              "  %d => %s", toAccountId, currentToAccountBalance + amount));
                    });
              });
        };
    Runnable immutableTransferOp =
        () -> {
          int fromAccountId = ThreadLocalRandom.current().nextInt(accountCount);
          int toAccountId = ThreadLocalRandom.current().nextInt(accountCount);
          if (fromAccountId == toAccountId) {
            return;
          }
          int amount = ThreadLocalRandom.current().nextInt(maxAmount);
          map.handleReadOperation(
              (readTable) -> {
                debugPrint(
                    String.format("IMMUTABLE: Read-Start. Table:%s, Amount:%d", readTable, amount));
                Account fromAccount = readTable.get(fromAccountId);
                Account toAccount = readTable.get(toAccountId);
                int currentFromAccountBalance = fromAccount == null ? 0 : fromAccount.balance;
                int currentToAccountBalance = toAccount == null ? 0 : toAccount.balance;
                debugPrint(" <before>");
                debugPrint(String.format("  %d => %s", fromAccountId, currentFromAccountBalance));
                debugPrint(String.format("  %d => %s", toAccountId, currentToAccountBalance));
                map.handleWriteOperation(
                    (version, writeTable) -> {
                      debugPrint(
                          String.format(
                              "IMMUTABLE: Write-Start. Version:%d, Table:%s, Amount:%d",
                              version, writeTable, amount));
                      debugPrint(" <before>");
                      debugPrint(
                          String.format("  %d => %s", fromAccountId, currentFromAccountBalance));
                      debugPrint(String.format("  %d => %s", toAccountId, currentToAccountBalance));
                      writeTable.put(
                          fromAccountId, new Account(version, currentFromAccountBalance - amount));
                      writeTable.put(
                          toAccountId, new Account(version, currentToAccountBalance + amount));
                      debugPrint(" <after>");
                      debugPrint(
                          String.format(
                              "  %d => %s", fromAccountId, currentFromAccountBalance - amount));
                      debugPrint(
                          String.format(
                              "  %d => %s", toAccountId, currentToAccountBalance + amount));
                    });
              });
        };
    // TODO: Audit operation:
    // TODO: Split operation:
    // TODO: Merge operation:

    AtomicBoolean shouldStop = new AtomicBoolean();
    List<Future<?>> futures = new ArrayList<>();
    ExecutorService executorService = Executors.newCachedThreadPool();
    for (int i = 0; i < threadCount; i++) {
      futures.add(
          executorService.submit(
              () -> {
                while (!shouldStop.get()) {
                  switch (ThreadLocalRandom.current().nextInt(2)) {
                    case 0:
                      mutableTransferOp.run();
                      break;
                    case 1:
                      immutableTransferOp.run();
                      break;
                    default:
                      throw new AssertionError();
                  }
                }
              }));
    }
    sleep(durationMillis);
    shouldStop.set(true);

    for (Future<?> future : futures) {
      future.get();
    }

    executorService.shutdown();
    assertThat(executorService.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

    map.handleReadOperation(
        (table) -> {
          int totalBalance = 0;
          for (int i = 0; i < accountCount; i++) {
            Account account = table.get(i);
            if (account != null) {
              totalBalance += account.balance;
            }
          }
          assertThat(totalBalance).isEqualTo(0);
        });
  }

  private void sleep(int milliSeconds) {
    try {
      TimeUnit.MILLISECONDS.sleep(milliSeconds);
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

  private void debugPrint(String msg) {
    System.out.printf("%s [%s] %s%n", Instant.now(), Thread.currentThread().getName(), msg);
  }
}
