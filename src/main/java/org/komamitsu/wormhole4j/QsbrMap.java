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

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javax.annotation.Nullable;

class QsbrMap<K, V extends QsbrMap.Versionable> {
  private final AtomicLong version = new AtomicLong();
  private final List<QsbrSlot> slots = new ArrayList<>();
  private volatile int activeSlotId = 0;
  private final Lock writerLock = new ReentrantLock(true);
  private final AtomicInteger threadIdCounter = new AtomicInteger();
  // TODO: Put these thread local values into a single instance.
  private final ThreadLocal<Integer> threadId =
      ThreadLocal.withInitial(threadIdCounter::getAndIncrement);
  private final ThreadLocal<Integer> readSlotId = new ThreadLocal<>();
  private final ThreadLocal<Long> readVersion = new ThreadLocal<>();
  private final ThreadLocal<Collection<Read<K>>> readSet = ThreadLocal.withInitial(HashSet::new);
  private final ThreadLocal<List<Write<K>>> writeList = ThreadLocal.withInitial(ArrayList::new);

  static class QsbrConflictException extends RuntimeException {
    public QsbrConflictException(String message) {
      super(message);
    }

    public QsbrConflictException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  // TODO: This can be removed with either of the following changes?
  //       - QsbrSlot.Map has an additional Map<K, Version>
  //       - QsbrSlot.Map manages Version in addition to V (i.e., Map<K, Pair<Version, V>>)
  interface Versionable<T extends Versionable<T>> {
    long getVersion();

    void setVersion(long version);

    @SuppressWarnings("unchecked")
    default void mutableUpdate(long version, Consumer<T> task) {
      setVersion(version);
      task.accept((T) this);
    }
  }

  private abstract static class Write<K> {
    protected final K key;

    private Write(K key) {
      this.key = key;
    }
  }

  private static class Put<K, V> extends Write<K> {
    private final V value;

    private Put(K key, V value) {
      super(key);
      this.value = value;
    }

    @Override
    public String toString() {
      return "Put{" + "value=" + value + ", key=" + key + '}';
    }
  }

  private static class Remove<K> extends Write<K> {
    private Remove(K key) {
      super(key);
    }

    @Override
    public String toString() {
      return "Remove{" + "key=" + key + '}';
    }
  }

  private static class Read<K> {
    private final K key;
    @Nullable private final Long version;

    private Read(K key, @Nullable Long version) {
      this.key = key;
      this.version = version;
    }

    @Override
    public String toString() {
      return "Read{" + "key=" + key + ", version=" + version + '}';
    }
  }

  class Map {
    private final java.util.Map<K, V> map = new HashMap<>();

    @Nullable
    V get(K key) {
      V value = map.get(key);
      debugPrint(String.format("COMMAND-GET: %s -> %s", key, value));
      if (value != null && readVersion.get() != null && value.getVersion() > readVersion.get()) {
        throw new QsbrConflictException(
            String.format(
                "The read value was updated after the reader entered the read phase. ReadPhaseVersion:%d, ValueVersion:%d",
                readVersion.get(), value.getVersion()));
      }
      readSet.get().add(new Read<>(key, value == null ? null : value.getVersion()));
      return value;
    }

    @Nullable
    V put(K key, V value) {
      value.setVersion(version.get() + 1);
      debugPrint(String.format("COMMAND-PUT: %s -> %s", key, value));
      V oldValue = map.put(key, value);
      writeList.get().add(new Put<>(key, value));
      return oldValue;
    }

    @Nullable
    V remove(K key) {
      V oldValue = map.remove(key);
      debugPrint(String.format("COMMAND-RMV: %s -> %s", key, oldValue));
      writeList.get().add(new Remove<>(key));
      return oldValue;
    }

    private V getWithoutRecording(K key) {
      return map.get(key);
    }

    private V putWithoutRecording(K key, V value) {
      return map.put(key, value);
    }

    private V removeWithoutRecording(K key) {
      return map.remove(key);
    }
  }

  private class QsbrSlot {
    private final Map table = new Map();
    private final BitSet readers = new BitSet();
    private final Object qsbrWaitLock = new Object();

    void enterReadPhase() {
      debugPrint(
          threadId.get(),
          String.format(
              "enterReadPhase: Start. ThreadId:%d, ActiveReaders:%s",
              threadId.get(), readers.cardinality()));
      synchronized (readers) {
        readers.set(threadId.get());
      }
      debugPrint(
          threadId.get(),
          String.format(
              "enterReadPhase: End. ThreadId:%d, ActiveReaders:%s",
              threadId.get(), readers.cardinality()));
    }

    void exitReadPhase() {
      debugPrint(
          threadId.get(),
          String.format(
              "exitReadPhase: Start. ThreadId:%d, ActiveReaders:%s",
              threadId.get(), readers.cardinality()));
      // If the reader is also the writer, the flag should be already unset and the reader count is
      // decremented.
      // Nothing to do.
      boolean isInReadPhase;
      synchronized (readers) {
        isInReadPhase = readers.get(threadId.get());
      }
      if (!isInReadPhase) {
        debugPrint(
            threadId.get(),
            String.format(
                "exitReadPhase: Already done. ThreadId:%d, ActiveReaders:%s",
                threadId.get(), readers.cardinality()));
        return;
      }
      boolean isNoReader;
      synchronized (readers) {
        readers.clear(threadId.get());
        isNoReader = readers.isEmpty();
      }
      if (isNoReader) {
        synchronized (qsbrWaitLock) {
          qsbrWaitLock.notify();
        }
      }
      debugPrint(
          threadId.get(),
          String.format(
              "exitReadPhase: End. ThreadId:%d, ActiveReaders:%s",
              threadId.get(), readers.cardinality()));
    }

    void waitUntilNoReader() {
      debugPrint(
          threadId.get(),
          String.format(
              "waitUntilNoReader: Start. ThreadId:%d, ActiveReaders:%s",
              threadId.get(), readers.cardinality()));
      synchronized (qsbrWaitLock) {
        while (true) {
          synchronized (readers) {
            if (readers.isEmpty()) {
              debugPrint(
                  threadId.get(),
                  String.format(
                      "waitUntilNoReader: Empty. ThreadId:%d, ActiveReaders:%s",
                      threadId.get(), readers.cardinality()));
              break;
            }
          }
          try {
            debugPrint(
                threadId.get(),
                String.format(
                    "waitUntilNoReader: Waiting. ThreadId:%d, ActiveReaders:%s",
                    threadId.get(), readers.cardinality()));
            qsbrWaitLock.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
      }
    }
  }

  public QsbrMap() {
    // Add active and inactive slots.
    slots.add(new QsbrSlot());
    slots.add(new QsbrSlot());
    debugPrint(
        String.format("Initialize: Map1:%s, Map2:%s", slots.get(0).table, slots.get(1).table));
  }

  void validateReadSet(QsbrSlot activeSlot) {
    Map activeMap = activeSlot.table;
    for (Read<K> read : readSet.get()) {
      V currentValue = activeMap.getWithoutRecording(read.key);
      debugPrint(
          String.format("validateReadSet: ReadSetValue:%s, CurrentValue:%s", read, currentValue));
      if (read.version == null) {
        if (currentValue != null) {
          throw new QsbrConflictException(
              String.format(
                  "Read key-value has been updated. Read value: null; Current value: %s",
                  currentValue));
        }
      } else {
        if (currentValue == null) {
          throw new QsbrConflictException(
              String.format(
                  "Read key-value has been updated. Read value: %s; Current value: null", read));
        }
        // TODO: Revisit here
        if (currentValue.getVersion() != read.version
            || currentValue.getVersion() > readVersion.get()) {
          throw new QsbrConflictException(
              String.format(
                  "Read key-value has been updated. Read value: %s; Current value: %s",
                  read, currentValue));
        }
      }
    }
    readVersion.remove();
  }

  void handleReadOperation(Consumer<Map> task) {
    readVersion.set(version.get());
    readSlotId.set(activeSlotId);
    debugPrint(
        threadId.get(),
        String.format(
            "handleReadOperation: Start. ReadSlotId:%d, ReadVersion:%d",
            readSlotId.get(), readVersion.get()));
    QsbrSlot readSlot = getSlot(readSlotId.get());
    Map table = readSlot.table;
    try {
      readSlot.enterReadPhase();
      debugPrint(
          threadId.get(),
          String.format("handleReadOperation: Executing Task. ReadSlotId:%d", readSlotId.get()));
      task.accept(table);
      debugPrint(
          threadId.get(),
          String.format("handleReadOperation: Executed Task. ReadSlotId:%d", readSlotId.get()));
      validateReadSet(readSlot);
    } finally {
      readSlot.exitReadPhase();
      readSlotId.remove();
      readVersion.remove();
      readSet.get().clear();
    }
  }

  void handleWriteOperation(BiConsumer<Long, Map> task) {
    debugPrint(
        threadId.get(),
        String.format(
            "handleWriteOperation: Start. ActiveSlotId:%d, WriteListSize:%d",
            activeSlotId, writeList.get().size()));

    // If the writer is also in the read phase, exit it to decrement the reader count.
    debugPrint(
        threadId.get(),
        String.format(
            "handleWriteOperation: Check if in read phase. ReadSlotId:%d", readSlotId.get()));
    if (readSlotId.get() != null) {
      QsbrSlot readSlot = slots.get(readSlotId.get());
      debugPrint(
          threadId.get(),
          String.format(
              "handleWriteOperation: Exiting read phase. ReadSlotId:%d, ActiveReaders:%s",
              readSlotId.get(), readSlot.readers.cardinality()));
      readSlot.exitReadPhase();
      readSlotId.remove();
      //      readVersion.remove();
    }

    Integer activeSlotIdBeforeSwitch = null;
    try {
      debugPrint(
          threadId.get(),
          String.format("handleWriteOperation: Waiting lock. ActiveSlotId:%d", activeSlotId));
      writerLock.lock();

      debugPrint(
          threadId.get(),
          String.format(
              "handleWriteOperation: Acquired Lock. ActiveSlotId:%d", activeSlotIdBeforeSwitch));

      activeSlotIdBeforeSwitch = activeSlotId;
      QsbrSlot activeSlotBeforeSwitch = getSlot(activeSlotIdBeforeSwitch);
      Map activeTableBeforeSwitch = activeSlotBeforeSwitch.table;

      int inactiveSlotIdBeforeSwitch = getInactiveSlotId(activeSlotIdBeforeSwitch);
      QsbrSlot inactiveSlotBeforeSwitch = getSlot(inactiveSlotIdBeforeSwitch);
      Map inactiveTableBeforeSwitch = inactiveSlotBeforeSwitch.table;

      // Check if the inactive slot has been updated by the preceding writer thread.
      validateReadSet(inactiveSlotBeforeSwitch);

      long nextVersion = version.get() + 1;
      task.accept(nextVersion, inactiveTableBeforeSwitch);

      // Switch the slots.
      debugPrint(
          threadId.get(),
          String.format(
              "handleWriteOperation: Switching ActiveSlotId. %d -> %d. Actual ActiveSlotId:%d",
              activeSlotIdBeforeSwitch, inactiveSlotIdBeforeSwitch, activeSlotId));
      assert activeSlotId == activeSlotIdBeforeSwitch;
      activeSlotId = inactiveSlotIdBeforeSwitch;

      // Increment the global version.
      version.set(nextVersion);

      // Wait for all readers to finish with the current inactive slot.
      debugPrint(
          threadId.get(),
          String.format(
              "handleWriteOperation: Wait until no readers. CurrentInactiveSlotId:%d",
              activeSlotIdBeforeSwitch));
      activeSlotBeforeSwitch.waitUntilNoReader();

      //      debugPrint(threadId.get(), String.format("  Commands:%s", inactiveTable.commands));

      debugPrint(
          threadId.get(),
          String.format(
              "handleWriteOperation: Applying write operations. TargetSlotId:%d",
              activeSlotIdBeforeSwitch));

      // Apply the recent commands to the current inactive slot table.
      for (Write<K> write : writeList.get()) {
        if (write instanceof Put) {
          Put<K, V> put = (Put<K, V>) write;
          activeTableBeforeSwitch.putWithoutRecording(put.key, put.value);
          debugPrint(threadId.get(), String.format("  APPLY-PUT: %s -> %s", put.key, put.value));
        } else if (write instanceof Remove) {
          Remove<K> remove = (Remove<K>) write;
          activeTableBeforeSwitch.removeWithoutRecording(remove.key);
          debugPrint(threadId.get(), String.format("  APPLY-RMV: %s", remove.key));
        } else {
          throw new AssertionError();
        }
      }
    } catch (Exception e) {
      debugPrint(
          threadId.get(),
          String.format("handleWriteOperation: Caught exception: %s", e.getMessage()));
      if (activeSlotIdBeforeSwitch == null) {
        // Something happened when acquiring the write lock.
        return;
      }
      QsbrSlot activeSlotBeforeSwitch = getSlot(activeSlotIdBeforeSwitch);
      Map activeTableBeforeSwitch = activeSlotBeforeSwitch.table;
      Map inactiveTableBeforeSwitch = getSlot(getInactiveSlotId(activeSlotIdBeforeSwitch)).table;

      Consumer<K> restorer =
          key -> {
            V origValue = activeTableBeforeSwitch.get(key);
            if (origValue == null) {
              inactiveTableBeforeSwitch.removeWithoutRecording(key);
            } else {
              inactiveTableBeforeSwitch.putWithoutRecording(key, origValue);
            }
          };
      // Revert commands applied to the slot.
      for (Write<K> write : writeList.get()) {
        restorer.accept(write.key);
      }
      throw e;
    } finally {
      readSet.get().clear();
      writeList.get().clear();
      writerLock.unlock();
      debugPrint(
          threadId.get(),
          String.format(
              "handleWriteOperation: Quiting. ActiveSlotId before switch:%d, Current ActiveSlotId:%d",
              activeSlotIdBeforeSwitch, activeSlotId));
    }
  }

  private int getInactiveSlotId(int activeSlotId) {
    return ++activeSlotId % 2;
  }

  Map getTable() {
    return getActiveSlot().table;
  }

  private QsbrSlot getSlot(int slotId) {
    return slots.get(slotId);
  }

  private QsbrSlot getActiveSlot() {
    return slots.get(activeSlotId);
  }

  long getVersion() {
    return version.get();
  }

  // TODO: Remove
  /*
  public static final BufferedWriter LOG_FILE;
  static {
    try {
      LOG_FILE = new BufferedWriter(new FileWriter("/tmp/wh.log"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
   */

  private static void debugPrint(int threadId, String msg) {
    /*
    String s = String.format("%s [%s] (%d) %s%n", Instant.now(), Thread.currentThread().getName(), threadId, msg);
    try {
      LOG_FILE.append(s);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
     */
  }

  private static void debugPrint(String msg) {
    /*
    String s = String.format("%s [%s] %s%n", Instant.now(), Thread.currentThread().getName(), msg);
    try {
      LOG_FILE.append(s);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
     */
  }
}
