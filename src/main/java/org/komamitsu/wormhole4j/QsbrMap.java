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
import java.util.function.Consumer;
import javax.annotation.Nullable;

class QsbrMap<K, V extends QsbrMap.Versionable<V>> {
  private final State<K> state = new State<>();
  private final List<QsbrSlot> slots = new ArrayList<>();
  private final Lock writerLock = new ReentrantLock(true);
  private final AtomicInteger threadIdCounter = new AtomicInteger();
  private final ThreadLocal<Integer> threadId =
      ThreadLocal.withInitial(threadIdCounter::getAndIncrement);

  private static class State<K> {
    private final AtomicLong version = new AtomicLong();
    private volatile int activeSlotId = 0;

    synchronized void update(long version, int activeSlotId) {
      this.version.set(version);
      this.activeSlotId = activeSlotId;
    }

    synchronized Context<K> createContext() {
      return new Context<>(this, version.get(), activeSlotId);
    }

    synchronized long nextVersion() {
      return version.get() + 1;
    }
  }

  static class Context<K> {
    private final State<K> globalState;
    private Long readVersion;
    private Integer readSlotId;
    private boolean isWritePhaseDone;
    private final Collection<Read<K>> readSet = new HashSet<>();
    private final List<Write<K>> writeList = new ArrayList<>();

    private Context(State<K> globalState) {
      this.globalState = globalState;
    }

    private Context(State<K> globalState, Long readVersion, Integer readSlotId) {
      this(globalState);
      this.readVersion = readVersion;
      this.readSlotId = readSlotId;
    }

    private void throwIfWritePhaseIsDone() {
      if (isWritePhaseDone) {
        throw new UnsupportedOperationException(
            "Any operation after the write operation is not permitted");
      }
    }
  }

  static class QsbrConflictException extends RuntimeException {
    public QsbrConflictException(String message) {
      super(message);
    }
  }

  interface ReadOperatable<K, V extends Versionable<V>> {
    void operate(Context<K> context, Map<K, V> table);
  }

  interface WriteOperatable<K, V extends Versionable<V>> {
    void operate(Context<K> context, long nextVersion, Map<K, V> table);
  }

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

  private static class Put<K, V extends Versionable<V>> extends Write<K> {
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

  static class Map<K, V extends Versionable<V>> {
    private final java.util.Map<K, V> map = new HashMap<>();

    @Nullable
    V get(Context<K> ctxt, K key) {
      ctxt.throwIfWritePhaseIsDone();
      V value = map.get(key);
      debugPrint(String.format("COMMAND-GET: %s -> %s", key, value));
      ctxt.readSet.add(new Read<>(key, value == null ? null : value.getVersion()));
      return value;
    }

    @Nullable
    V put(Context<K> ctxt, K key, V value) {
      ctxt.throwIfWritePhaseIsDone();
      value.setVersion(ctxt.globalState.nextVersion());
      debugPrint(String.format("COMMAND-PUT: %s -> %s", key, value));
      V oldValue = map.put(key, value);
      ctxt.writeList.add(new Put<>(key, value));
      return oldValue;
    }

    @Nullable
    V remove(Context<K> ctxt, K key) {
      ctxt.throwIfWritePhaseIsDone();
      V oldValue = map.remove(key);
      debugPrint(String.format("COMMAND-RMV: %s -> %s", key, oldValue));
      ctxt.writeList.add(new Remove<>(key));
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
    private final Map<K, V> table = new Map<>();
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

  void validateReadSet(Context<K> ctxt, QsbrSlot activeSlot) {
    if (ctxt.isWritePhaseDone) {
      // Read-set is already validated in the write phase.
      return;
    }
    Map<K, V> activeMap = activeSlot.table;
    for (Read<K> read : ctxt.readSet) {
      V currentValue = activeMap.getWithoutRecording(read.key);
      debugPrint(
          String.format("validateReadSet: ReadSetValue:%s, CurrentValue:%s", read, currentValue));
      if (read.version == null) {
        if (currentValue != null) {
          throw new QsbrConflictException(
              String.format(
                  "The read key-value has been updated. Read version: null; Current version: %d",
                  currentValue.getVersion()));
        }
      } else {
        if (currentValue == null) {
          throw new QsbrConflictException(
              String.format(
                  "The read key-value has been updated. Read version: %s; Current version: null",
                  read.version));
        }
        if (currentValue.getVersion() > ctxt.readVersion) {
          throw new QsbrConflictException(
              String.format(
                  "The read key-value has been updated. Read version: %d; Current version: %d",
                  read.version, currentValue.getVersion()));
        }
      }
    }
  }

  void handleReadOperation(ReadOperatable<K, V> task) {
    Context<K> ctxt = state.createContext();
    debugPrint(
        threadId.get(),
        String.format(
            "handleReadOperation: Start. ReadSlotId:%d, ReadVersion:%d",
            ctxt.readSlotId, ctxt.readVersion));
    QsbrSlot readSlot = getSlot(ctxt.readSlotId);
    Map<K, V> table = readSlot.table;
    try {
      readSlot.enterReadPhase();
      debugPrint(
          threadId.get(),
          String.format("handleReadOperation: Executing Task. ReadSlotId:%d", ctxt.readSlotId));
      task.operate(ctxt, table);
      debugPrint(
          threadId.get(),
          String.format("handleReadOperation: Executed Task. ReadSlotId:%d", ctxt.readSlotId));
      validateReadSet(ctxt, readSlot);
    } finally {
      readSlot.exitReadPhase();
    }
  }

  void handleWriteOperation(WriteOperatable<K, V> task) {
    handleWriteOperation(state.createContext(), task);
  }

  void handleWriteOperation(Context<K> ctxt, WriteOperatable<K, V> task) {
    debugPrint(
        threadId.get(),
        String.format(
            "handleWriteOperation: Start. ActiveSlotId:%d, WriteListSize:%d",
            state.activeSlotId, ctxt.writeList.size()));

    // If the writer is also in the read phase, exit it to decrement the reader count.
    debugPrint(
        threadId.get(),
        String.format(
            "handleWriteOperation: Check if in read phase. ReadSlotId:%d", ctxt.readSlotId));
    if (ctxt.readSlotId != null) {
      QsbrSlot readSlot = slots.get(ctxt.readSlotId);
      debugPrint(
          threadId.get(),
          String.format(
              "handleWriteOperation: Exiting read phase. ReadSlotId:%d, ActiveReaders:%s",
              ctxt.readSlotId, readSlot.readers.cardinality()));
      readSlot.exitReadPhase();
      ctxt.readSlotId = null;
    }

    Integer activeSlotIdBeforeSwitch = null;
    try {
      debugPrint(
          threadId.get(),
          String.format("handleWriteOperation: Waiting lock. ActiveSlotId:%d", state.activeSlotId));
      writerLock.lock();

      debugPrint(
          threadId.get(),
          String.format(
              "handleWriteOperation: Acquired Lock. ActiveSlotId:%d", activeSlotIdBeforeSwitch));

      activeSlotIdBeforeSwitch = state.activeSlotId;
      QsbrSlot activeSlotBeforeSwitch = getSlot(activeSlotIdBeforeSwitch);
      Map<K, V> activeTableBeforeSwitch = activeSlotBeforeSwitch.table;

      int inactiveSlotIdBeforeSwitch = getInactiveSlotId(activeSlotIdBeforeSwitch);
      QsbrSlot inactiveSlotBeforeSwitch = getSlot(inactiveSlotIdBeforeSwitch);
      Map<K, V> inactiveTableBeforeSwitch = inactiveSlotBeforeSwitch.table;

      // Check if the inactive slot has been updated by the preceding writer thread.
      validateReadSet(ctxt, inactiveSlotBeforeSwitch);

      long nextVersion = state.nextVersion();
      task.operate(ctxt, nextVersion, inactiveTableBeforeSwitch);

      // Switch the slots.
      debugPrint(
          threadId.get(),
          String.format(
              "handleWriteOperation: Switching ActiveSlotId. %d -> %d. Actual ActiveSlotId:%d",
              activeSlotIdBeforeSwitch, inactiveSlotIdBeforeSwitch, state.activeSlotId));
      assert state.activeSlotId == activeSlotIdBeforeSwitch;

      state.update(nextVersion, inactiveSlotIdBeforeSwitch);

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
      for (Write<K> write : ctxt.writeList) {
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
      Map<K, V> activeTableBeforeSwitch = activeSlotBeforeSwitch.table;
      Map<K, V> inactiveTableBeforeSwitch =
          getSlot(getInactiveSlotId(activeSlotIdBeforeSwitch)).table;

      Consumer<K> restorer =
          key -> {
            V origValue = activeTableBeforeSwitch.getWithoutRecording(key);
            if (origValue == null) {
              inactiveTableBeforeSwitch.removeWithoutRecording(key);
            } else {
              inactiveTableBeforeSwitch.putWithoutRecording(key, origValue);
            }
          };
      // Revert commands applied to the slot.
      for (Write<K> write : ctxt.writeList) {
        restorer.accept(write.key);
      }
      throw e;
    } finally {
      ctxt.isWritePhaseDone = true;
      writerLock.unlock();
      debugPrint(
          threadId.get(),
          String.format(
              "handleWriteOperation: Quiting. ActiveSlotId before switch:%d, Current ActiveSlotId:%d",
              activeSlotIdBeforeSwitch, state.activeSlotId));
    }
  }

  private int getInactiveSlotId(int activeSlotId) {
    return ++activeSlotId % 2;
  }

  private QsbrSlot getSlot(int slotId) {
    return slots.get(slotId);
  }

  long getVersion() {
    return state.version.get();
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
    String s =
        String.format(
            "%s [%s] (%d) %s%n", Instant.now(), Thread.currentThread().getName(), threadId, msg);
    // System.out.print(s);
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
    // System.out.print(s);
    try {
      LOG_FILE.append(s);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
     */
  }
}
