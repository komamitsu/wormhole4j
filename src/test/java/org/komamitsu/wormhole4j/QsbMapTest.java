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
              () -> {
                assertThat(map.getMap().get(key)).isNull();
                map.getMap().put(key, new Item(map.getWriteVersion(), value));
              });

          assertThat(map.getVersion()).isEqualTo(1);

          map.handleReadOperation(
              () -> assertThat(map.getMap().get(key)).isEqualTo(new Item(1, value)));
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
              () -> {
                assertThat(map.getMap().get(key)).isNull();
                map.handleWriteOperation(
                    () -> map.getMap().put(key, new Item(map.getWriteVersion(), value)));
              });

          assertThat(map.getVersion()).isEqualTo(1);

          map.handleReadOperation(
              () -> assertThat(map.getMap().get(key)).isEqualTo(new Item(1, value)));
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
              () -> {
                assertThat(map.get(key)).isNull();
                map.handleWriteOperation(
                    () -> map.put(key, new Item(map.getWriteVersion(), value1)));
              });

          assertThat(map.getVersion()).isEqualTo(1);

          map.handleReadOperation(
              () -> {
                assertThat(map.get(key)).isEqualTo(new Item(1, value1));
                map.handleWriteOperation(
                    () -> map.put(key, new Item(map.getWriteVersion(), value2)));
              });

          assertThat(map.getVersion()).isEqualTo(2);

          map.handleReadOperation(
              () -> {
                assertThat(map.get(key)).isEqualTo(new Item(2, value2));
                map.handleWriteOperation(
                    () -> map.put(key, new Item(map.getWriteVersion(), value3)));
              });

          assertThat(map.getVersion()).isEqualTo(3);

          map.handleReadOperation(() -> assertThat(map.get(key)).isEqualTo(new Item(3, value3)));
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
                              () -> {
                                assertThat(map.get(key)).isNull();
                                readBeforeModifyLatch.countDown();
                                sleep(1000);
                                assertThat(map.get(key)).isNull();
                              })));

          map.handleReadOperation(
              () -> {
                assertThat(map.get(key)).isNull();
                await(readBeforeModifyLatch);
                map.handleWriteOperation(
                    () -> map.put(key, new Item(map.getWriteVersion(), value)));
              });

          assertThat(map.getVersion()).isEqualTo(1);

          executorService.shutdown();
          assertThat(executorService.awaitTermination(2, TimeUnit.SECONDS)).isTrue();

          map.handleReadOperation(() -> assertThat(map.get(key)).isEqualTo(new Item(1, value)));
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
                              () -> {
                                assertThat(map.get(key)).isNull();
                                await(readBeforeModifyLatch);
                                map.handleWriteOperation(
                                    () -> map.put(key, new Item(map.getWriteVersion(), value)));
                              })));

          map.handleReadOperation(() -> assertThat(map.get(key)).isNull());

          assertThat(map.getVersion()).isEqualTo(0);

          readBeforeModifyLatch.countDown();

          executorService.shutdown();
          assertThat(executorService.awaitTermination(2, TimeUnit.SECONDS)).isTrue();

          assertThat(map.getVersion()).isEqualTo(1);

          map.handleReadOperation(() -> assertThat(map.get(key)).isEqualTo(new Item(1, value)));
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
              () -> {
                assertThat(map.get(key)).isNull();
                map.put(key, new Item(map.getWriteVersion(), value1));
              });

          assertThat(map.getVersion()).isEqualTo(1);

          map.handleWriteOperation(
              () -> assertThat(map.remove(key)).isEqualTo(new Item(1, value1)));

          assertThat(map.getVersion()).isEqualTo(2);

          map.handleReadOperation(() -> assertThat(map.get(key)).isNull());
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
              () -> {
                assertThat(map.get(key)).isNull();
                map.put(key, new Item(map.getWriteVersion(), value));
              });

          assertThat(map.getVersion()).isEqualTo(1);

          map.handleReadOperation(
              () -> {
                assertThat(map.get(key)).isEqualTo(new Item(1, value));
                map.handleWriteOperation(() -> map.remove(key));
              });

          assertThat(map.getVersion()).isEqualTo(2);

          map.handleReadOperation(() -> assertThat(map.get(key)).isNull());
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
              () -> {
                assertThat(map.get(key1)).isNull();
                map.handleWriteOperation(
                    () -> map.put(key1, new Item(map.getWriteVersion(), value1)));
              });

          assertThat(map.getVersion()).isEqualTo(1);

          map.handleReadOperation(
              () -> {
                assertThat(map.get(key1)).isEqualTo(new Item(1, value1));
                map.handleWriteOperation(
                    () -> {
                      map.remove(key1);
                      map.put(key2, new Item(map.getWriteVersion(), value2));
                    });
              });

          assertThat(map.getVersion()).isEqualTo(2);

          map.handleReadOperation(
              () -> {
                assertThat(map.get(key1)).isNull();
                assertThat(map.get(key2)).isEqualTo(new Item(2, value2));
                map.handleWriteOperation(
                    () -> {
                      map.remove(key2);
                      map.put(key3, new Item(map.getWriteVersion(), value3));
                    });
              });

          assertThat(map.getVersion()).isEqualTo(3);

          map.handleReadOperation(
              () -> {
                assertThat(map.get(key1)).isNull();
                assertThat(map.get(key2)).isNull();
                assertThat(map.get(key3)).isEqualTo(new Item(3, value3));
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
              () -> {
                assertThat(map.get(key)).isNull();
                map.put(key, new Item(map.getWriteVersion(), value));
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
                              () -> {
                                assertThat(map.get(key)).isNotNull();
                                readBeforeRemoveLatch.countDown();
                                sleep(1000);
                                assertThat(map.get(key)).isNotNull();
                              })));

          map.handleReadOperation(
              () -> {
                Item readItem = map.get(key);
                assertThat(readItem).isNotNull();
                await(readBeforeRemoveLatch);
                map.handleWriteOperation(() -> map.remove(key));
              });

          assertThat(map.getVersion()).isEqualTo(2);

          executorService.shutdown();
          assertThat(executorService.awaitTermination(2, TimeUnit.SECONDS)).isTrue();

          map.handleReadOperation(() -> assertThat(map.get(key)).isNull());
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
              () -> {
                map.put(key1, new Item(value1_init));
                map.put(key2, new Item(value2_init));
                map.put(key3, new Item(value3_init));
                map.put(key4, new Item(value4_init));
              });

          assertThat(map.getVersion()).isEqualTo(1);

          map.handleWriteOperation(
              () -> {
                assertThat(map.get(key1)).isEqualTo(new Item(1, value1_init));
                assertThat(map.get(key2)).isEqualTo(new Item(1, value2_init));
                assertThat(map.get(key3)).isEqualTo(new Item(1, value3_init));
                assertThat(map.get(key4)).isEqualTo(new Item(1, value4_init));

                map.put(key3, new Item(value3_update1));
                map.put(key4, new Item(value4_update1));

                assertThat(map.get(key1)).isEqualTo(new Item(1, value1_init));
                assertThat(map.get(key2)).isEqualTo(new Item(1, value2_init));
                assertThat(map.get(key3)).isEqualTo(new Item(2, value3_update1));
                assertThat(map.get(key4)).isEqualTo(new Item(2, value4_update1));

                map.remove(key2);
                map.remove(key3);

                assertThat(map.get(key1)).isEqualTo(new Item(1, value1_init));
                assertThat(map.get(key2)).isNull();
                assertThat(map.get(key3)).isNull();
                assertThat(map.get(key4)).isEqualTo(new Item(2, value4_update1));
              });

          assertThat(map.getVersion()).isEqualTo(2);

          map.handleReadOperation(
              () -> {
                assertThat(map.get(key1)).isEqualTo(new Item(1, value1_init));
                assertThat(map.get(key2)).isNull();
                assertThat(map.get(key3)).isNull();
                assertThat(map.get(key4)).isEqualTo(new Item(2, value4_update1));
              });

          map.handleWriteOperation(
              () -> {
                assertThat(map.get(key1)).isEqualTo(new Item(1, value1_init));
                assertThat(map.get(key2)).isNull();
                assertThat(map.get(key3)).isNull();
                assertThat(map.get(key4)).isEqualTo(new Item(2, value4_update1));

                map.put(key2, new Item(value2_reinsert));
                map.put(key3, new Item(value3_reinsert));

                assertThat(map.get(key1)).isEqualTo(new Item(1, value1_init));
                assertThat(map.get(key2)).isEqualTo(new Item(3, value2_reinsert));
                assertThat(map.get(key3)).isEqualTo(new Item(3, value3_reinsert));
                assertThat(map.get(key4)).isEqualTo(new Item(2, value4_update1));

                map.put(key3, new Item(value3_update2));
                map.put(key4, new Item(value4_update2));

                assertThat(map.get(key1)).isEqualTo(new Item(1, value1_init));
                assertThat(map.get(key2)).isEqualTo(new Item(3, value2_reinsert));
                assertThat(map.get(key3)).isEqualTo(new Item(3, value3_update2));
                assertThat(map.get(key4)).isEqualTo(new Item(3, value4_update2));

                map.remove(key1);
                map.remove(key3);

                assertThat(map.get(key1)).isNull();
                assertThat(map.get(key2)).isEqualTo(new Item(3, value2_reinsert));
                assertThat(map.get(key3)).isNull();
                assertThat(map.get(key4)).isEqualTo(new Item(3, value4_update2));
              });

          assertThat(map.getVersion()).isEqualTo(3);

          map.handleReadOperation(
              () -> {
                assertThat(map.get(key1)).isNull();
                assertThat(map.get(key2)).isEqualTo(new Item(3, value2_reinsert));
                assertThat(map.get(key3)).isNull();
                assertThat(map.get(key4)).isEqualTo(new Item(3, value4_update2));
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
              () -> {
                map.put(key1, new Item(value1_init));
                map.put(key2, new Item(value2_init));
                map.put(key3, new Item(value3_init));
                map.put(key4, new Item(value4_init));
              });

          assertThat(map.getVersion()).isEqualTo(1);

          map.handleReadOperation(
              () -> {
                Item item1 = map.get(key1);
                Item item2 = map.get(key2);
                Item item3 = map.get(key3);
                Item item4 = map.get(key4);
                map.handleWriteOperation(
                    () -> {
                      assertThat(item1).isEqualTo(new Item(1, value1_init));
                      assertThat(item2).isEqualTo(new Item(1, value2_init));
                      assertThat(item3).isEqualTo(new Item(1, value3_init));
                      assertThat(item4).isEqualTo(new Item(1, value4_init));

                      item3.mutableUpdate(map.getWriteVersion(), x -> x.setValue(value3_update1));
                      item4.mutableUpdate(map.getWriteVersion(), x -> x.setValue(value4_update1));

                      assertThat(map.get(key1)).isEqualTo(new Item(1, value1_init));
                      assertThat(map.get(key2)).isEqualTo(new Item(1, value2_init));
                      assertThat(map.get(key3)).isEqualTo(new Item(2, value3_update1));
                      assertThat(map.get(key4)).isEqualTo(new Item(2, value4_update1));

                      map.remove(key2);
                      map.remove(key3);

                      assertThat(map.get(key1)).isEqualTo(new Item(1, value1_init));
                      assertThat(map.get(key2)).isNull();
                      assertThat(map.get(key3)).isNull();
                      assertThat(map.get(key4)).isEqualTo(new Item(2, value4_update1));
                    });
              });

          assertThat(map.getVersion()).isEqualTo(2);

          map.handleReadOperation(
              () -> {
                assertThat(map.get(key1)).isEqualTo(new Item(1, value1_init));
                assertThat(map.get(key2)).isNull();
                assertThat(map.get(key3)).isNull();
                assertThat(map.get(key4)).isEqualTo(new Item(2, value4_update1));
              });

          map.handleReadOperation(
              () -> {
                Item item1 = map.get(key1);
                Item item2 = map.get(key2);
                Item item3 = map.get(key3);
                Item item4 = map.get(key4);
                map.handleWriteOperation(
                    () -> {
                      assertThat(item1).isEqualTo(new Item(1, value1_init));
                      assertThat(item2).isNull();
                      assertThat(item3).isNull();
                      assertThat(item4).isEqualTo(new Item(2, value4_update1));

                      map.put(key2, new Item(value2_reinsert));
                      map.put(key3, new Item(value3_reinsert));

                      assertThat(map.get(key1)).isEqualTo(new Item(1, value1_init));
                      assertThat(map.get(key2)).isEqualTo(new Item(3, value2_reinsert));
                      assertThat(map.get(key3)).isEqualTo(new Item(3, value3_reinsert));
                      assertThat(map.get(key4)).isEqualTo(new Item(2, value4_update1));

                      Item updatedItem3 = map.get(key3);
                      Item updatedItem4 = map.get(key4);
                      updatedItem3.mutableUpdate(
                          map.getWriteVersion(), x -> x.setValue(value3_update2));
                      updatedItem4.mutableUpdate(
                          map.getWriteVersion(), x -> x.setValue(value4_update2));

                      assertThat(map.get(key1)).isEqualTo(new Item(1, value1_init));
                      assertThat(map.get(key2)).isEqualTo(new Item(3, value2_reinsert));
                      assertThat(map.get(key3)).isEqualTo(new Item(3, value3_update2));
                      assertThat(map.get(key4)).isEqualTo(new Item(3, value4_update2));

                      map.remove(key1);
                      map.remove(key3);

                      assertThat(map.get(key1)).isNull();
                      assertThat(map.get(key2)).isEqualTo(new Item(3, value2_reinsert));
                      assertThat(map.get(key3)).isNull();
                      assertThat(map.get(key4)).isEqualTo(new Item(3, value4_update2));
                    });
              });

          assertThat(map.getVersion()).isEqualTo(3);

          map.handleReadOperation(
              () -> {
                assertThat(map.get(key1)).isNull();
                assertThat(map.get(key2)).isEqualTo(new Item(3, value2_reinsert));
                assertThat(map.get(key3)).isNull();
                assertThat(map.get(key4)).isEqualTo(new Item(3, value4_update2));
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
                          () -> {
                            map.handleWriteOperation(
                                () -> {
                                  assertThat(map.get(key)).isNull();
                                  map.put(key, new Item(map.getWriteVersion(), value));
                                });
                            map.get(key);
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
              () -> {
                debugPrint(
                    String.format(
                        "MUTABLE: Read-Start. Table:%s, Amount:%d", map.getMap(), amount));
                Account fromAccount = map.get(fromAccountId);
                Account toAccount = map.get(toAccountId);
                int currentFromAccountBalance = fromAccount == null ? 0 : fromAccount.balance;
                int currentToAccountBalance = toAccount == null ? 0 : toAccount.balance;
                debugPrint(" <before>");
                debugPrint(String.format("  %d => %d", fromAccountId, currentFromAccountBalance));
                debugPrint(String.format("  %d => %d", toAccountId, currentToAccountBalance));
                map.handleWriteOperation(
                    () -> {
                      debugPrint(
                          String.format(
                              "MUTABLE: Write-Start. Version:%d, Table:%s, Amount:%d",
                              map.getWriteVersion(), map.getMap(), amount));
                      if (fromAccount == null) {
                        map.put(
                            fromAccountId,
                            new Account(map.getWriteVersion(), currentFromAccountBalance - amount));
                      } else {
                        debugPrint(
                            String.format("UPDATE(START): %s -> %s", fromAccountId, fromAccount));
                        fromAccount.mutableUpdate(map.getWriteVersion(), x -> x.balance -= amount);
                        debugPrint(
                            String.format("UPDATE(END)  : %s -> %s", fromAccountId, fromAccount));
                      }
                      if (toAccount == null) {
                        map.put(
                            toAccountId,
                            new Account(map.getWriteVersion(), currentToAccountBalance + amount));
                      } else {
                        debugPrint(
                            String.format("UPDATE(START): %s -> %s", toAccountId, toAccount));
                        toAccount.mutableUpdate(map.getWriteVersion(), x -> x.balance += amount);
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
              () -> {
                debugPrint(
                    String.format(
                        "IMMUTABLE: Read-Start. Table:%s, Amount:%d", map.getMap(), amount));
                Account fromAccount = map.get(fromAccountId);
                Account toAccount = map.get(toAccountId);
                int currentFromAccountBalance = fromAccount == null ? 0 : fromAccount.balance;
                int currentToAccountBalance = toAccount == null ? 0 : toAccount.balance;
                debugPrint(" <before>");
                debugPrint(String.format("  %d => %d", fromAccountId, currentFromAccountBalance));
                debugPrint(String.format("  %d => %d", toAccountId, currentToAccountBalance));
                map.handleWriteOperation(
                    () -> {
                      debugPrint(
                          String.format(
                              "IMMUTABLE: Write-Start. Version:%d, Table:%s, Amount:%d",
                              map.getWriteVersion(), map.getMap(), amount));
                      debugPrint(" <before>");
                      debugPrint(
                          String.format("  %d => %d", fromAccountId, currentFromAccountBalance));
                      debugPrint(String.format("  %d => %d", toAccountId, currentToAccountBalance));
                      map.put(
                          fromAccountId,
                          new Account(map.getWriteVersion(), currentFromAccountBalance - amount));
                      map.put(
                          toAccountId,
                          new Account(map.getWriteVersion(), currentToAccountBalance + amount));
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
              () -> {
                debugPrint(String.format("AUDIT: START. ReadTable:%s", map.getMap()));
                int total = 0;
                // TODO: Remove this.
                int[] balances = new int[accountCount];
                for (int i = 0; i < accountCount; i++) {
                  Account account = map.get(i);
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
              () -> {
                debugPrint(String.format("MERGE: Read-Start. Table:%s", map.getMap()));
                Account fromAccount = map.get(fromAccountId);
                Account toAccount = map.get(toAccountId);
                int currentFromAccountBalance = fromAccount == null ? 0 : fromAccount.balance;
                int currentToAccountBalance = toAccount == null ? 0 : toAccount.balance;
                debugPrint(" <before>");
                debugPrint(String.format("  %d => %d", fromAccountId, currentFromAccountBalance));
                debugPrint(String.format("  %d => %d", toAccountId, currentToAccountBalance));
                map.handleWriteOperation(
                    () -> {
                      debugPrint(
                          String.format(
                              "MERGE: Write-Start. Version:%d, Table:%s",
                              map.getWriteVersion(), map.getMap()));
                      debugPrint(" <before>");
                      debugPrint(
                          String.format("  %d => %d", fromAccountId, currentFromAccountBalance));
                      debugPrint(String.format("  %d => %d", toAccountId, currentToAccountBalance));
                      map.remove(fromAccountId);
                      int newToAccountBalance = currentToAccountBalance + currentFromAccountBalance;
                      if (toAccount == null || ThreadLocalRandom.current().nextInt() % 2 == 0) {
                        map.put(
                            toAccountId, new Account(map.getWriteVersion(), newToAccountBalance));
                      } else {
                        debugPrint(
                            String.format("UPDATE(START): %s -> %s", toAccountId, toAccount));
                        toAccount.mutableUpdate(
                            map.getWriteVersion(), x -> x.balance = newToAccountBalance);
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
                () -> {
                  int totalBalance = 0;
                  for (int i = 0; i < accountCount; i++) {
                    Account account = map.get(i);
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
