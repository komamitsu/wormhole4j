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

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class QsbMapTest {
  private Set<Integer> randomIntegers;

  private static class Item implements QsbMap.Versionable<Item> {
    private long version;
    private int value;

    public Item(int value) {
      this.value = value;
    }

    public Item(long version, int value) {
      this.version = version;
      this.value = value;
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
      return Objects.equals(version, item.version) && Objects.equals(value, item.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(version, value);
    }

    @Override
    public String toString() {
      return "Item{" + "version=" + version + ", value=" + value + '}';
    }

    @Override
    public long getVersion() {
      return version;
    }

    @Override
    public void setVersion(long version) {
      this.version = version;
    }

    public void setValue(int value) {
      this.value = value;
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

  private interface ThrowableRunnable<R extends Throwable> {
    void run() throws R;
  }

  private <K, V extends QsbMap.Versionable<V>, R extends Throwable> void withThreadRegistration(
      QsbMap<K, V> map, ThrowableRunnable<R> task) throws R {
    try {
      map.registerThread();
      task.run();
    } finally {
      map.unregisterThread();
    }
  }

  @Test
  void separateSequentialPutAndGet_ShouldReturnProperValue() {
    QsbMap<Integer, Item> map = new QsbMap<>();
    withThreadRegistration(
        map,
        () -> {
          assertThat(map.getVersion()).isEqualTo(0);

          int key = getRandomInt();
          int value = getRandomInt();

          map.handleWriteOperation(
              (ctx, version, table) -> {
                assertThat(table.get(ctx, key)).isNull();
                table.put(ctx, key, new Item(version, value));
              });

          assertThat(map.getVersion()).isEqualTo(1);

          map.handleReadOperation(
              (ctx, table) -> assertThat(table.get(ctx, key)).isEqualTo(new Item(1, value)));
        });
  }

  @Test
  void singleGetModifyPutInTransaction_ShouldReturnProperValue() {
    QsbMap<Integer, Item> map = new QsbMap<>();
    withThreadRegistration(
        map,
        () -> {
          assertThat(map.getVersion()).isEqualTo(0);

          int key = getRandomInt();
          int value = getRandomInt();

          map.handleReadOperation(
              (readCtx, readTable) -> {
                assertThat(readTable.get(readCtx, key)).isNull();
                map.handleWriteOperation(
                    readCtx,
                    (writeCtx, version, writeTable) ->
                        writeTable.put(writeCtx, key, new Item(version, value)));
              });

          assertThat(map.getVersion()).isEqualTo(1);

          map.handleReadOperation(
              (ctx, table) -> assertThat(table.get(ctx, key)).isEqualTo(new Item(1, value)));
        });
  }

  @Test
  void multipleGetModifyPut_ShouldReturnProperValue() {
    QsbMap<Integer, Item> map = new QsbMap<>();
    withThreadRegistration(
        map,
        () -> {
          assertThat(map.getVersion()).isEqualTo(0);

          int key = getRandomInt();
          int value1 = getRandomInt();
          int value2 = getRandomInt();
          int value3 = getRandomInt();

          map.handleReadOperation(
              (readCtx, readTable) -> {
                assertThat(readTable.get(readCtx, key)).isNull();
                map.handleWriteOperation(
                    readCtx,
                    (writeCtx, version, writeTable) ->
                        writeTable.put(writeCtx, key, new Item(version, value1)));
              });

          assertThat(map.getVersion()).isEqualTo(1);

          map.handleReadOperation(
              (readCtx, readTable) -> {
                assertThat(readTable.get(readCtx, key)).isEqualTo(new Item(1, value1));
                map.handleWriteOperation(
                    readCtx,
                    (writeCtx, version, writeTable) ->
                        writeTable.put(writeCtx, key, new Item(version, value2)));
              });

          assertThat(map.getVersion()).isEqualTo(2);

          map.handleReadOperation(
              (readCtx, readTable) -> {
                assertThat(readTable.get(readCtx, key)).isEqualTo(new Item(2, value2));
                map.handleWriteOperation(
                    readCtx,
                    (writeCtx, version, writeTable) ->
                        writeTable.put(writeCtx, key, new Item(version, value3)));
              });

          assertThat(map.getVersion()).isEqualTo(3);

          map.handleReadOperation(
              (ctx, table) -> assertThat(table.get(ctx, key)).isEqualTo(new Item(3, value3)));
        });
  }

  @Test
  void getModifyPutWithConcurrentGet_ShouldReturnProperValue() throws InterruptedException {
    QsbMap<Integer, Item> map = new QsbMap<>();
    withThreadRegistration(
        map,
        () -> {
          assertThat(map.getVersion()).isEqualTo(0);

          int key = getRandomInt();
          int value = getRandomInt();

          CountDownLatch readBeforeModifyLatch = new CountDownLatch(1);
          ExecutorService executorService = Executors.newCachedThreadPool();
          executorService.execute(
              () ->
                  withThreadRegistration(
                      map,
                      () ->
                          map.handleReadOperation(
                              (readCtx, readTable) -> {
                                assertThat(readTable.get(readCtx, key)).isNull();
                                readBeforeModifyLatch.countDown();
                                sleep(1000);
                                assertThat(readTable.get(readCtx, key)).isNull();
                              })));

          map.handleReadOperation(
              (readCtx, readTable) -> {
                assertThat(readTable.get(readCtx, key)).isNull();
                await(readBeforeModifyLatch);
                map.handleWriteOperation(
                    readCtx,
                    (writeCtx, version, writeTable) ->
                        writeTable.put(writeCtx, key, new Item(version, value)));
              });

          assertThat(map.getVersion()).isEqualTo(1);

          executorService.shutdown();
          assertThat(executorService.awaitTermination(2, TimeUnit.SECONDS)).isTrue();

          map.handleReadOperation(
              (ctx, table) -> assertThat(table.get(ctx, key)).isEqualTo(new Item(1, value)));
        });
  }

  @Test
  void putWithConcurrentGet_ShouldNotBlockGet() throws InterruptedException {
    QsbMap<Integer, Item> map = new QsbMap<>();
    withThreadRegistration(
        map,
        () -> {
          assertThat(map.getVersion()).isEqualTo(0);

          int key = getRandomInt();
          int value = getRandomInt();

          CountDownLatch readBeforeModifyLatch = new CountDownLatch(1);
          ExecutorService executorService = Executors.newCachedThreadPool();
          executorService.execute(
              () ->
                  withThreadRegistration(
                      map,
                      () ->
                          map.handleReadOperation(
                              (readCtx, readTable) -> {
                                assertThat(readTable.get(readCtx, key)).isNull();
                                await(readBeforeModifyLatch);
                                map.handleWriteOperation(
                                    (writeCtx, version, writeTable) ->
                                        writeTable.put(writeCtx, key, new Item(version, value)));
                              })));

          map.handleReadOperation(
              (readCtx, readTable) -> assertThat(readTable.get(readCtx, key)).isNull());

          assertThat(map.getVersion()).isEqualTo(0);

          readBeforeModifyLatch.countDown();

          executorService.shutdown();
          assertThat(executorService.awaitTermination(2, TimeUnit.SECONDS)).isTrue();

          assertThat(map.getVersion()).isEqualTo(1);

          map.handleReadOperation(
              (ctx, table) -> assertThat(table.get(ctx, key)).isEqualTo(new Item(1, value)));
        });
  }

  @Test
  void separateSequentialPutAndRemove_ShouldRemoveIt() {
    QsbMap<Integer, Item> map = new QsbMap<>();
    withThreadRegistration(
        map,
        () -> {
          assertThat(map.getVersion()).isEqualTo(0);

          int key = getRandomInt();
          int value1 = getRandomInt();

          map.handleWriteOperation(
              (ctx, version, table) -> {
                assertThat(table.get(ctx, key)).isNull();
                table.put(ctx, key, new Item(version, value1));
              });

          assertThat(map.getVersion()).isEqualTo(1);

          map.handleWriteOperation(
              (ctx, version, table) ->
                  assertThat(table.remove(ctx, key)).isEqualTo(new Item(1, value1)));

          assertThat(map.getVersion()).isEqualTo(2);

          map.handleReadOperation((ctx, table) -> assertThat(table.get(ctx, key)).isNull());
        });
  }

  @Test
  void singleGetAndRemoveInTransaction_ShouldRemoveIt() {
    QsbMap<Integer, Item> map = new QsbMap<>();
    withThreadRegistration(
        map,
        () -> {
          assertThat(map.getVersion()).isEqualTo(0);

          int key = getRandomInt();
          int value = getRandomInt();
          map.handleWriteOperation(
              (ctx, version, table) -> {
                assertThat(table.get(ctx, key)).isNull();
                table.put(ctx, key, new Item(version, value));
              });

          assertThat(map.getVersion()).isEqualTo(1);

          map.handleReadOperation(
              (readCtx, readTable) -> {
                assertThat(readTable.get(readCtx, key)).isEqualTo(new Item(1, value));
                map.handleWriteOperation(
                    readCtx, (writeCtx, version, writeTable) -> writeTable.remove(writeCtx, key));
              });

          assertThat(map.getVersion()).isEqualTo(2);

          map.handleReadOperation((ctx, table) -> assertThat(table.get(ctx, key)).isNull());
        });
  }

  @Test
  void multipleGetModifyPutAndRemove_ShouldReturnProperValue() {
    QsbMap<Integer, Item> map = new QsbMap<>();
    withThreadRegistration(
        map,
        () -> {
          assertThat(map.getVersion()).isEqualTo(0);

          int key1 = getRandomInt();
          int value1 = getRandomInt();
          int key2 = getRandomInt();
          int value2 = getRandomInt();
          int key3 = getRandomInt();
          int value3 = getRandomInt();

          map.handleReadOperation(
              (readCtx, readTable) -> {
                assertThat(readTable.get(readCtx, key1)).isNull();
                map.handleWriteOperation(
                    readCtx,
                    (writeCtx, version, writeTable) ->
                        writeTable.put(writeCtx, key1, new Item(version, value1)));
              });

          assertThat(map.getVersion()).isEqualTo(1);

          map.handleReadOperation(
              (readCtx, readTable) -> {
                assertThat(readTable.get(readCtx, key1)).isEqualTo(new Item(1, value1));
                map.handleWriteOperation(
                    readCtx,
                    (writeCtx, version, writeTable) -> {
                      writeTable.remove(writeCtx, key1);
                      writeTable.put(writeCtx, key2, new Item(version, value2));
                    });
              });

          assertThat(map.getVersion()).isEqualTo(2);

          map.handleReadOperation(
              (readCtx, readTable) -> {
                assertThat(readTable.get(readCtx, key1)).isNull();
                assertThat(readTable.get(readCtx, key2)).isEqualTo(new Item(2, value2));
                map.handleWriteOperation(
                    readCtx,
                    (writeCtx, version, writeTable) -> {
                      writeTable.remove(writeCtx, key2);
                      writeTable.put(writeCtx, key3, new Item(version, value3));
                    });
              });

          assertThat(map.getVersion()).isEqualTo(3);

          map.handleReadOperation(
              (ctx, table) -> {
                assertThat(table.get(ctx, key1)).isNull();
                assertThat(table.get(ctx, key2)).isNull();
                assertThat(table.get(ctx, key3)).isEqualTo(new Item(3, value3));
              });
        });
  }

  @Test
  void getAndRemoveWithConcurrentGet_ShouldReturnProperValue() throws InterruptedException {
    QsbMap<Integer, Item> map = new QsbMap<>();
    withThreadRegistration(
        map,
        () -> {
          assertThat(map.getVersion()).isEqualTo(0);

          int key = getRandomInt();
          int value = getRandomInt();

          map.handleWriteOperation(
              (ctx, version, table) -> {
                assertThat(table.get(ctx, key)).isNull();
                table.put(ctx, key, new Item(version, value));
              });
          assertThat(map.getVersion()).isEqualTo(1);

          CountDownLatch readBeforeRemoveLatch = new CountDownLatch(1);
          ExecutorService executorService = Executors.newCachedThreadPool();
          executorService.execute(
              () ->
                  withThreadRegistration(
                      map,
                      () ->
                          map.handleReadOperation(
                              (readCtx, readTable) -> {
                                assertThat(readTable.get(readCtx, key)).isNotNull();
                                readBeforeRemoveLatch.countDown();
                                sleep(1000);
                                assertThat(readTable.get(readCtx, key)).isNotNull();
                              })));

          map.handleReadOperation(
              (readCtx, readTable) -> {
                Item readItem = readTable.get(readCtx, key);
                assertThat(readItem).isNotNull();
                await(readBeforeRemoveLatch);
                map.handleWriteOperation(
                    readCtx, (writeCtx, version, writeTable) -> writeTable.remove(writeCtx, key));
              });

          assertThat(map.getVersion()).isEqualTo(2);

          executorService.shutdown();
          assertThat(executorService.awaitTermination(2, TimeUnit.SECONDS)).isTrue();

          map.handleReadOperation((ctx, table) -> assertThat(table.get(ctx, key)).isNull());
        });
  }

  @Test
  void multipleOperationWriteUsingImmutablePut_whenSucceeds_ShouldMakeAllOperationsVisible() {
    QsbMap<Integer, Item> map = new QsbMap<>();
    withThreadRegistration(
        map,
        () -> {
          assertThat(map.getVersion()).isEqualTo(0);

          int key1 = getRandomInt();
          int value1_init = getRandomInt();
          int key2 = getRandomInt();
          int value2_init = getRandomInt();
          int value2_reinsert = getRandomInt();
          int key3 = getRandomInt();
          int value3_init = getRandomInt();
          int value3_update1 = getRandomInt();
          int value3_reinsert = getRandomInt();
          int value3_update2 = getRandomInt();
          int key4 = getRandomInt();
          int value4_init = getRandomInt();
          int value4_update1 = getRandomInt();
          int value4_update2 = getRandomInt();

          map.handleWriteOperation(
              (ctx, version, table) -> {
                table.put(ctx, key1, new Item(value1_init));
                table.put(ctx, key2, new Item(value2_init));
                table.put(ctx, key3, new Item(value3_init));
                table.put(ctx, key4, new Item(value4_init));
              });

          assertThat(map.getVersion()).isEqualTo(1);

          map.handleWriteOperation(
              (ctx, version, table) -> {
                assertThat(table.get(ctx, key1)).isEqualTo(new Item(1, value1_init));
                assertThat(table.get(ctx, key2)).isEqualTo(new Item(1, value2_init));
                assertThat(table.get(ctx, key3)).isEqualTo(new Item(1, value3_init));
                assertThat(table.get(ctx, key4)).isEqualTo(new Item(1, value4_init));

                table.put(ctx, key3, new Item(value3_update1));
                table.put(ctx, key4, new Item(value4_update1));

                assertThat(table.get(ctx, key1)).isEqualTo(new Item(1, value1_init));
                assertThat(table.get(ctx, key2)).isEqualTo(new Item(1, value2_init));
                assertThat(table.get(ctx, key3)).isEqualTo(new Item(2, value3_update1));
                assertThat(table.get(ctx, key4)).isEqualTo(new Item(2, value4_update1));

                table.remove(ctx, key2);
                table.remove(ctx, key3);

                assertThat(table.get(ctx, key1)).isEqualTo(new Item(1, value1_init));
                assertThat(table.get(ctx, key2)).isNull();
                assertThat(table.get(ctx, key3)).isNull();
                assertThat(table.get(ctx, key4)).isEqualTo(new Item(2, value4_update1));
              });

          assertThat(map.getVersion()).isEqualTo(2);

          map.handleReadOperation(
              (ctx, table) -> {
                assertThat(table.get(ctx, key1)).isEqualTo(new Item(1, value1_init));
                assertThat(table.get(ctx, key2)).isNull();
                assertThat(table.get(ctx, key3)).isNull();
                assertThat(table.get(ctx, key4)).isEqualTo(new Item(2, value4_update1));
              });

          map.handleWriteOperation(
              (ctx, version, table) -> {
                assertThat(table.get(ctx, key1)).isEqualTo(new Item(1, value1_init));
                assertThat(table.get(ctx, key2)).isNull();
                assertThat(table.get(ctx, key3)).isNull();
                assertThat(table.get(ctx, key4)).isEqualTo(new Item(2, value4_update1));

                table.put(ctx, key2, new Item(value2_reinsert));
                table.put(ctx, key3, new Item(value3_reinsert));

                assertThat(table.get(ctx, key1)).isEqualTo(new Item(1, value1_init));
                assertThat(table.get(ctx, key2)).isEqualTo(new Item(3, value2_reinsert));
                assertThat(table.get(ctx, key3)).isEqualTo(new Item(3, value3_reinsert));
                assertThat(table.get(ctx, key4)).isEqualTo(new Item(2, value4_update1));

                table.put(ctx, key3, new Item(value3_update2));
                table.put(ctx, key4, new Item(value4_update2));

                assertThat(table.get(ctx, key1)).isEqualTo(new Item(1, value1_init));
                assertThat(table.get(ctx, key2)).isEqualTo(new Item(3, value2_reinsert));
                assertThat(table.get(ctx, key3)).isEqualTo(new Item(3, value3_update2));
                assertThat(table.get(ctx, key4)).isEqualTo(new Item(3, value4_update2));

                table.remove(ctx, key1);
                table.remove(ctx, key3);

                assertThat(table.get(ctx, key1)).isNull();
                assertThat(table.get(ctx, key2)).isEqualTo(new Item(3, value2_reinsert));
                assertThat(table.get(ctx, key3)).isNull();
                assertThat(table.get(ctx, key4)).isEqualTo(new Item(3, value4_update2));
              });

          assertThat(map.getVersion()).isEqualTo(3);

          map.handleReadOperation(
              (ctx, table) -> {
                assertThat(table.get(ctx, key1)).isNull();
                assertThat(table.get(ctx, key2)).isEqualTo(new Item(3, value2_reinsert));
                assertThat(table.get(ctx, key3)).isNull();
                assertThat(table.get(ctx, key4)).isEqualTo(new Item(3, value4_update2));
              });
        });
  }

  @Test
  void multipleOperationWriteUsingMutablePut_whenSucceeds_ShouldMakeAllOperationsVisible() {
    QsbMap<Integer, Item> map = new QsbMap<>();
    withThreadRegistration(
        map,
        () -> {
          assertThat(map.getVersion()).isEqualTo(0);

          int key1 = getRandomInt();
          int value1_init = getRandomInt();
          int key2 = getRandomInt();
          int value2_init = getRandomInt();
          int value2_reinsert = getRandomInt();
          int key3 = getRandomInt();
          int value3_init = getRandomInt();
          int value3_update1 = getRandomInt();
          int value3_reinsert = getRandomInt();
          int value3_update2 = getRandomInt();
          int key4 = getRandomInt();
          int value4_init = getRandomInt();
          int value4_update1 = getRandomInt();
          int value4_update2 = getRandomInt();

          map.handleWriteOperation(
              (ctx, version, table) -> {
                table.put(ctx, key1, new Item(value1_init));
                table.put(ctx, key2, new Item(value2_init));
                table.put(ctx, key3, new Item(value3_init));
                table.put(ctx, key4, new Item(value4_init));
              });

          assertThat(map.getVersion()).isEqualTo(1);

          map.handleReadOperation(
              (readCtx, readTable) -> {
                Item item1 = readTable.get(readCtx, key1);
                Item item2 = readTable.get(readCtx, key2);
                Item item3 = readTable.get(readCtx, key3);
                Item item4 = readTable.get(readCtx, key4);
                map.handleWriteOperation(
                    readCtx,
                    (ctx, version, table) -> {
                      assertThat(item1).isEqualTo(new Item(1, value1_init));
                      assertThat(item2).isEqualTo(new Item(1, value2_init));
                      assertThat(item3).isEqualTo(new Item(1, value3_init));
                      assertThat(item4).isEqualTo(new Item(1, value4_init));

                      item3.mutableUpdate(version, x -> x.setValue(value3_update1));
                      item4.mutableUpdate(version, x -> x.setValue(value4_update1));

                      assertThat(table.get(ctx, key1)).isEqualTo(new Item(1, value1_init));
                      assertThat(table.get(ctx, key2)).isEqualTo(new Item(1, value2_init));
                      assertThat(table.get(ctx, key3)).isEqualTo(new Item(2, value3_update1));
                      assertThat(table.get(ctx, key4)).isEqualTo(new Item(2, value4_update1));

                      table.remove(ctx, key2);
                      table.remove(ctx, key3);

                      assertThat(table.get(ctx, key1)).isEqualTo(new Item(1, value1_init));
                      assertThat(table.get(ctx, key2)).isNull();
                      assertThat(table.get(ctx, key3)).isNull();
                      assertThat(table.get(ctx, key4)).isEqualTo(new Item(2, value4_update1));
                    });
              });

          assertThat(map.getVersion()).isEqualTo(2);

          map.handleReadOperation(
              (ctx, table) -> {
                assertThat(table.get(ctx, key1)).isEqualTo(new Item(1, value1_init));
                assertThat(table.get(ctx, key2)).isNull();
                assertThat(table.get(ctx, key3)).isNull();
                assertThat(table.get(ctx, key4)).isEqualTo(new Item(2, value4_update1));
              });

          map.handleReadOperation(
              (readCtx, readTable) -> {
                Item item1 = readTable.get(readCtx, key1);
                Item item2 = readTable.get(readCtx, key2);
                Item item3 = readTable.get(readCtx, key3);
                Item item4 = readTable.get(readCtx, key4);
                map.handleWriteOperation(
                    readCtx,
                    (ctx, version, table) -> {
                      assertThat(item1).isEqualTo(new Item(1, value1_init));
                      assertThat(item2).isNull();
                      assertThat(item3).isNull();
                      assertThat(item4).isEqualTo(new Item(2, value4_update1));

                      table.put(ctx, key2, new Item(value2_reinsert));
                      table.put(ctx, key3, new Item(value3_reinsert));

                      assertThat(table.get(ctx, key1)).isEqualTo(new Item(1, value1_init));
                      assertThat(table.get(ctx, key2)).isEqualTo(new Item(3, value2_reinsert));
                      assertThat(table.get(ctx, key3)).isEqualTo(new Item(3, value3_reinsert));
                      assertThat(table.get(ctx, key4)).isEqualTo(new Item(2, value4_update1));

                      Item updatedItem3 = table.get(ctx, key3);
                      Item updatedItem4 = table.get(ctx, key4);
                      updatedItem3.mutableUpdate(version, x -> x.setValue(value3_update2));
                      updatedItem4.mutableUpdate(version, x -> x.setValue(value4_update2));

                      assertThat(table.get(ctx, key1)).isEqualTo(new Item(1, value1_init));
                      assertThat(table.get(ctx, key2)).isEqualTo(new Item(3, value2_reinsert));
                      assertThat(table.get(ctx, key3)).isEqualTo(new Item(3, value3_update2));
                      assertThat(table.get(ctx, key4)).isEqualTo(new Item(3, value4_update2));

                      table.remove(ctx, key1);
                      table.remove(ctx, key3);

                      assertThat(table.get(ctx, key1)).isNull();
                      assertThat(table.get(ctx, key2)).isEqualTo(new Item(3, value2_reinsert));
                      assertThat(table.get(ctx, key3)).isNull();
                      assertThat(table.get(ctx, key4)).isEqualTo(new Item(3, value4_update2));
                    });
              });

          assertThat(map.getVersion()).isEqualTo(3);

          map.handleReadOperation(
              (ctx, table) -> {
                assertThat(table.get(ctx, key1)).isNull();
                assertThat(table.get(ctx, key2)).isEqualTo(new Item(3, value2_reinsert));
                assertThat(table.get(ctx, key3)).isNull();
                assertThat(table.get(ctx, key4)).isEqualTo(new Item(3, value4_update2));
              });
        });
  }

  @Test
  void getAfterWritePhase_ShouldThrowException() {
    QsbMap<Integer, Item> map = new QsbMap<>();
    withThreadRegistration(
        map,
        () -> {
          int key = getRandomInt();
          int value = getRandomInt();

          assertThatThrownBy(
                  () ->
                      map.handleReadOperation(
                          (readCtx, readTable) -> {
                            map.handleWriteOperation(
                                readCtx,
                                (writeCtx, version, writeTable) -> {
                                  assertThat(writeTable.get(writeCtx, key)).isNull();
                                  writeTable.put(writeCtx, key, new Item(version, value));
                                });
                            readTable.get(readCtx, key);
                          }))
              .isInstanceOf(IllegalStateException.class);
        });
  }

  // TODO: Add:
  //       - When an exception is thrown in write operation block, all the in-flight write
  // operations should be reverted
  //       - When read records are updated, the read operation block should throw an exception
  //       - When read records are updated after the thread acquires the write lock, the write
  // operation block should
  //         throw an exception

  private static class Account implements QsbMap.Versionable<Account> {
    public long version;
    public int balance;

    public Account(long version, int balance) {
      this.version = version;
      this.balance = balance;
    }

    @Override
    public long getVersion() {
      return version;
    }

    @Override
    public void setVersion(long version) {
      this.version = version;
    }

    @Override
    public String toString() {
      return "Account{" + "version=" + version + ", balance=" + balance + '}';
    }
  }

  @Test
  void multiThreadOperations_ShouldReachProperState()
      throws ExecutionException, InterruptedException {
    int threadCount = 8;
    int accountCount = 10;
    int maxAmount = 100;
    int durationMillis = 30000;

    QsbMap<Integer, Account> map = new QsbMap<>();

    Supplier<Integer> mutableTransferOp =
        () -> {
          int fromAccountId = ThreadLocalRandom.current().nextInt(accountCount);
          int toAccountId = ThreadLocalRandom.current().nextInt(accountCount);
          if (fromAccountId == toAccountId) {
            return null;
          }
          int amount = ThreadLocalRandom.current().nextInt(maxAmount);
          map.handleReadOperation(
              (readCtx, readTable) -> {
                debugPrint(
                    String.format("MUTABLE: Read-Start. Table:%s, Amount:%d", readTable, amount));
                Account fromAccount = readTable.get(readCtx, fromAccountId);
                Account toAccount = readTable.get(readCtx, toAccountId);
                int currentFromAccountBalance = fromAccount == null ? 0 : fromAccount.balance;
                int currentToAccountBalance = toAccount == null ? 0 : toAccount.balance;
                debugPrint(" <before>");
                debugPrint(String.format("  %d => %d", fromAccountId, currentFromAccountBalance));
                debugPrint(String.format("  %d => %d", toAccountId, currentToAccountBalance));
                map.handleWriteOperation(
                    readCtx,
                    (writeCtx, version, writeTable) -> {
                      debugPrint(
                          String.format(
                              "MUTABLE: Write-Start. Version:%d, Table:%s, Amount:%d",
                              version, writeTable, amount));
                      if (fromAccount == null) {
                        writeTable.put(
                            writeCtx,
                            fromAccountId,
                            new Account(version, currentFromAccountBalance - amount));
                      } else {
                        debugPrint(
                            String.format("UPDATE(START): %s -> %s", fromAccountId, fromAccount));
                        fromAccount.mutableUpdate(version, x -> x.balance -= amount);
                        debugPrint(
                            String.format("UPDATE(END)  : %s -> %s", fromAccountId, fromAccount));
                      }
                      if (toAccount == null) {
                        writeTable.put(
                            writeCtx,
                            toAccountId,
                            new Account(version, currentToAccountBalance + amount));
                      } else {
                        debugPrint(
                            String.format("UPDATE(START): %s -> %s", toAccountId, toAccount));
                        toAccount.mutableUpdate(version, x -> x.balance += amount);
                        debugPrint(
                            String.format("UPDATE(END)  : %s -> %s", toAccountId, toAccount));
                      }
                      debugPrint(" <after>");
                      debugPrint(
                          String.format(
                              "  %d => %d", fromAccountId, currentFromAccountBalance - amount));
                      debugPrint(
                          String.format(
                              "  %d => %d", toAccountId, currentToAccountBalance + amount));
                    });
              });
          return null;
        };
    Supplier<Integer> immutableTransferOp =
        () -> {
          int fromAccountId = ThreadLocalRandom.current().nextInt(accountCount);
          int toAccountId = ThreadLocalRandom.current().nextInt(accountCount);
          if (fromAccountId == toAccountId) {
            return null;
          }
          int amount = ThreadLocalRandom.current().nextInt(maxAmount);
          map.handleReadOperation(
              (readCtx, readTable) -> {
                debugPrint(
                    String.format("IMMUTABLE: Read-Start. Table:%s, Amount:%d", readTable, amount));
                Account fromAccount = readTable.get(readCtx, fromAccountId);
                Account toAccount = readTable.get(readCtx, toAccountId);
                int currentFromAccountBalance = fromAccount == null ? 0 : fromAccount.balance;
                int currentToAccountBalance = toAccount == null ? 0 : toAccount.balance;
                debugPrint(" <before>");
                debugPrint(String.format("  %d => %d", fromAccountId, currentFromAccountBalance));
                debugPrint(String.format("  %d => %d", toAccountId, currentToAccountBalance));
                map.handleWriteOperation(
                    readCtx,
                    (writeCtx, version, writeTable) -> {
                      debugPrint(
                          String.format(
                              "IMMUTABLE: Write-Start. Version:%d, Table:%s, Amount:%d",
                              version, writeTable, amount));
                      debugPrint(" <before>");
                      debugPrint(
                          String.format("  %d => %d", fromAccountId, currentFromAccountBalance));
                      debugPrint(String.format("  %d => %d", toAccountId, currentToAccountBalance));
                      writeTable.put(
                          writeCtx,
                          fromAccountId,
                          new Account(version, currentFromAccountBalance - amount));
                      writeTable.put(
                          writeCtx,
                          toAccountId,
                          new Account(version, currentToAccountBalance + amount));
                      debugPrint(" <after>");
                      debugPrint(
                          String.format(
                              "  %d => %d", fromAccountId, currentFromAccountBalance - amount));
                      debugPrint(
                          String.format(
                              "  %d => %d", toAccountId, currentToAccountBalance + amount));
                    });
              });
          return null;
        };
    Supplier<Integer> auditOp =
        () -> {
          AtomicReference<Integer> result = new AtomicReference<>();
          map.handleReadOperation(
              (readCtx, readTable) -> {
                debugPrint(String.format("AUDIT: START. ReadTable:%s", readTable));
                int total = 0;
                // TODO: Remove this.
                int[] balances = new int[accountCount];
                for (int i = 0; i < accountCount; i++) {
                  Account account = readTable.get(readCtx, i);
                  if (account != null) {
                    total += account.balance;
                    balances[i] = account.balance;
                  } else {
                    balances[i] = 0;
                  }
                }
                if (total != 0) {
                  debugPrint(
                      String.format(
                          "AUDIT: FAIL Total:%d, Balances:%s", total, Arrays.toString(balances)));
                }
                debugPrint(
                    String.format(
                        "AUDIT: END Total:%d, Balances:%s", total, Arrays.toString(balances)));
                result.set(total);
              });
          return result.get();
        };
    Supplier<Integer> mergeOp =
        () -> {
          int fromAccountId = ThreadLocalRandom.current().nextInt(accountCount);
          int toAccountId = ThreadLocalRandom.current().nextInt(accountCount);
          if (fromAccountId == toAccountId) {
            return null;
          }
          map.handleReadOperation(
              (readCtx, readTable) -> {
                debugPrint(String.format("MERGE: Read-Start. Table:%s", readTable));
                Account fromAccount = readTable.get(readCtx, fromAccountId);
                Account toAccount = readTable.get(readCtx, toAccountId);
                int currentFromAccountBalance = fromAccount == null ? 0 : fromAccount.balance;
                int currentToAccountBalance = toAccount == null ? 0 : toAccount.balance;
                debugPrint(" <before>");
                debugPrint(String.format("  %d => %d", fromAccountId, currentFromAccountBalance));
                debugPrint(String.format("  %d => %d", toAccountId, currentToAccountBalance));
                map.handleWriteOperation(
                    readCtx,
                    (writeCtx, version, writeTable) -> {
                      debugPrint(
                          String.format(
                              "MERGE: Write-Start. Version:%d, Table:%s", version, writeTable));
                      debugPrint(" <before>");
                      debugPrint(
                          String.format("  %d => %d", fromAccountId, currentFromAccountBalance));
                      debugPrint(String.format("  %d => %d", toAccountId, currentToAccountBalance));
                      writeTable.remove(writeCtx, fromAccountId);
                      int newToAccountBalance = currentToAccountBalance + currentFromAccountBalance;
                      if (toAccount == null || ThreadLocalRandom.current().nextInt() % 2 == 0) {
                        writeTable.put(
                            writeCtx, toAccountId, new Account(version, newToAccountBalance));
                      } else {
                        debugPrint(
                            String.format("UPDATE(START): %s -> %s", toAccountId, toAccount));
                        toAccount.mutableUpdate(version, x -> x.balance = newToAccountBalance);
                        debugPrint(
                            String.format("UPDATE(START): %s -> %s", toAccountId, toAccount));
                      }
                      debugPrint(" <after>");
                      debugPrint(String.format("  %d: removed", fromAccountId));
                      debugPrint(String.format("  %d => %d", toAccountId, newToAccountBalance));
                    });
              });
          return null;
        };

    Function<Supplier<Integer>, Integer> withRetry =
        task -> {
          while (true) {
            try {
              return task.get();
            } catch (QsbMap.QsbConflictException e) {
              // Retry
              debugPrint("Conflict. " + e.getMessage());
            }
          }
        };

    AtomicBoolean shouldStop = new AtomicBoolean();
    List<Future<?>> futures = new ArrayList<>();
    ExecutorService executorService = Executors.newCachedThreadPool();
    for (int i = 0; i < threadCount; i++) {
      futures.add(
          executorService.submit(
              () -> {
                map.registerThread();
                try {
                  while (!shouldStop.get()) {
                    switch (ThreadLocalRandom.current().nextInt(4)) {
                      case 0:
                        withRetry.apply(mutableTransferOp);
                        break;
                      case 1:
                        withRetry.apply(immutableTransferOp);
                        break;
                      case 2:
                        int total = withRetry.apply(auditOp);
                        assertThat(total).isEqualTo(0);
                        break;
                      case 3:
                        withRetry.apply(mergeOp);
                        break;
                      default:
                        throw new AssertionError();
                    }
                  }
                } finally {
                  map.unregisterThread();
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

    withThreadRegistration(
        map,
        () ->
            map.handleReadOperation(
                (ctx, table) -> {
                  int totalBalance = 0;
                  for (int i = 0; i < accountCount; i++) {
                    Account account = table.get(ctx, i);
                    if (account != null) {
                      totalBalance += account.balance;
                    }
                  }
                  assertThat(totalBalance).isEqualTo(0);
                }));
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
    /*
    String s = String.format("%s [%s] %s%n", Instant.now(), Thread.currentThread().getName(), msg);
    // System.out.print(s);
    try {
      LOG_FILE.append(s);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
     */
  }

  /*
  @AfterAll
  static void tearDown() {
    try {
      LOG_FILE.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
     */
}
