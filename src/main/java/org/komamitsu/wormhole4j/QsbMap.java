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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javax.annotation.Nullable;

/**
 * A thread-safe map implementation using Quiescent State Based (QSB) synchronization with
 * optimistic concurrency control.
 *
 * <p>This map uses a two-slot design where reads operate on one slot while writes prepare updates
 * on another slot. Writers wait for all readers to reach a quiescent state (exit their read phase)
 * before applying changes, ensuring readers always see a consistent snapshot without blocking.
 *
 * <p><b>Thread Registration:</b> Before any thread can access this map, it must call {@link
 * #registerThread()}. When the thread is permanently done, it must call {@link #unregisterThread()}
 * to allow thread ID reuse.
 *
 * <p><b>Concurrency Guarantees:</b>
 *
 * <ul>
 *   <li>Readers never block each other or writers (on the active slot)
 *   <li>Writers are serialized via a lock
 *   <li>Provides snapshot isolation for read operations
 *   <li>Detects conflicts via read-set validation, throwing {@link QsbConflictException}
 * </ul>
 *
 * <p><b>Usage Example:</b>
 *
 * <pre>{@code
 * QsbMap<String, Item> map = new QsbMap<>();
 *
 * // Register thread before use
 * map.registerThread();
 * try {
 *   // Read operation
 *   map.handleReadOperation((ctx, table) -> {
 *     Item item = table.get(ctx, "key");
 *     // ...
 *   });
 *
 *   // Read-modify-write transaction
 *   map.handleReadOperation((readCtx, readTable) -> {
 *     Item item = readTable.get(readCtx, "key");
 *     map.handleWriteOperation(readCtx, (writeCtx, version, writeTable) -> {
 *       writeTable.put(writeCtx, "key", new Item(version, newValue));
 *     });
 *   });
 * } finally {
 *   map.unregisterThread();
 * }
 * }</pre>
 *
 * <p><b>Performance Characteristics:</b>
 *
 * <ul>
 *   <li>Excellent for read-heavy workloads
 *   <li>Writers must wait for readers to quiesce (bounded by reader operation time)
 *   <li>Write operations are serialized (single writer at a time)
 *   <li>Failed transactions should be retried on {@link QsbConflictException}
 * </ul>
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values, must implement {@link Versionable}
 */
class QsbMap<K, V extends QsbMap.Versionable<V>> {
  private final State<K> state = new State<>();
  private final List<QsbrSlot> slots = new ArrayList<>();
  private final Lock writerLock = new ReentrantLock(true);
  private final BitSet threads = new BitSet();
  private final ThreadLocal<Context<K>> ctxt = new ThreadLocal<>();

  void registerThread() {
    if (ctxt.get() != null) {
      throw new IllegalStateException("This thread is already registered");
    }
    int threadId;
    synchronized (this) {
      threadId = threads.nextClearBit(0);
      threads.set(threadId);
    }
    ctxt.set(new Context<>(state, threadId));
  }

  void unregisterThread() {
    if (ctxt.get() == null) {
      throw new IllegalStateException("This thread is not registered");
    }
    synchronized (this) {
      threads.clear(ctxt.get().threadId);
      ctxt.remove();
    }
  }

  private static class State<K> {
    // TODO: Revisit here to consider if these need to be AtomicLong or volatile.
    private final AtomicLong version = new AtomicLong();
    private volatile int activeSlotId = 0;

    synchronized void update(long version, int activeSlotId) {
      this.version.set(version);
      this.activeSlotId = activeSlotId;
    }

    synchronized long nextVersion() {
      return version.get() + 1;
    }
  }

  enum ContextState {
    INITIALIZED,
    STARTED,
    FINISHED
  }

  static class Context<K> {
    private final State<K> globalState;
    private final int threadId;

    private Long readVersion;
    private Integer readSlotId;
    private ContextState readPhaseState = ContextState.INITIALIZED;
    private final Collection<Read<K>> readSet = new HashSet<>();

    private Integer writeSlotId;
    private Long writeVersion;
    private ContextState writePhaseState = ContextState.INITIALIZED;
    private final List<Write<K>> writeList = new ArrayList<>();

    private Context(State<K> globalState, int threadId) {
      this.globalState = globalState;
      this.threadId = threadId;
    }

    private void startReadPhase() {
      if (readPhaseState != ContextState.INITIALIZED) {
        throw new IllegalStateException("Read phase must be initialized");
      }
      if (writePhaseState == ContextState.STARTED) {
        throw new IllegalStateException("Write phase must not be started");
      }

      readPhaseState = ContextState.STARTED;
      // TODO: Refactoring
      synchronized (globalState) {
        readVersion = globalState.version.get();
        readSlotId = globalState.activeSlotId;
      }
      if (writePhaseState == ContextState.FINISHED) {
        writePhaseState = ContextState.INITIALIZED;
      }
    }

    private void finishReadPhase() {
      if (readPhaseState != ContextState.STARTED) {
        throw new IllegalStateException("Read phase must be started");
      }
      if (writePhaseState == ContextState.STARTED) {
        throw new IllegalStateException("Write phase must be finished");
      }

      readPhaseState = ContextState.INITIALIZED;
      if (writePhaseState == ContextState.FINISHED) {
        writePhaseState = ContextState.INITIALIZED;
      }
      readVersion = null;
      readSlotId = null;
      readSet.clear();
    }

    private void startWritePhase() {
      if (writePhaseState == ContextState.STARTED) {
        throw new IllegalStateException("Write phase must not be started");
      }
      writePhaseState = ContextState.STARTED;
      writeSlotId = getInactiveSlotId(globalState.activeSlotId);
      writeVersion = globalState.nextVersion();
    }

    private void finishWritePhase() {
      if (readPhaseState == ContextState.FINISHED) {
        throw new IllegalStateException("Read phase must not be finished");
      }
      if (writePhaseState != ContextState.STARTED) {
        throw new IllegalStateException("Write phase must be started");
      }
      writePhaseState = ContextState.FINISHED;
      writeVersion = null;
      writeSlotId = null;
      writeList.clear();
      // Read-set is already validated in the write phase.
      readSet.clear();
    }

    private void throwIfWritePhaseIsDone() {
      if (writePhaseState == ContextState.FINISHED) {
        throw new IllegalStateException("Any operation after the write operation is not permitted");
      }
    }
  }

  static class QsbConflictException extends RuntimeException {
    public QsbConflictException(String message) {
      super(message);
    }
  }

  interface ReadOperatable<K, V extends Versionable<V>> {
    void operate();
  }

  interface WriteOperatable<K, V extends Versionable<V>> {
    void operate();
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

  class Map {
    private final java.util.Map<K, V> map = new HashMap<>();

    @Nullable
    V get(K key) {
      Context<K> ctxt = QsbMap.this.ctxt.get();
      ctxt.throwIfWritePhaseIsDone();
      V value = map.get(key);
      debugPrint(String.format("COMMAND-GET: %s -> %s", key, value));
      if (ctxt.writePhaseState != ContextState.STARTED) {
        ctxt.readSet.add(new Read<>(key, value == null ? null : value.getVersion()));
      }
      return value;
    }

    @Nullable
    V put(K key, V value) {
      Context<K> ctxt = QsbMap.this.ctxt.get();
      if (ctxt.writePhaseState != ContextState.STARTED) {
        throw new IllegalStateException("put() cannot be executed in the read phase");
      }
      ctxt.throwIfWritePhaseIsDone();
      value.setVersion(ctxt.writeVersion);
      debugPrint(String.format("COMMAND-PUT: %s -> %s", key, value));
      V oldValue = map.put(key, value);
      ctxt.writeList.add(new Put<>(key, value));
      return oldValue;
    }

    @Nullable
    V remove(K key) {
      Context<K> ctxt = QsbMap.this.ctxt.get();
      if (ctxt.writePhaseState != ContextState.STARTED) {
        throw new IllegalStateException("remove() cannot be executed in the read phase");
      }
      ctxt.throwIfWritePhaseIsDone();
      V oldValue = map.remove(key);
      debugPrint(String.format("COMMAND-RMV: %s -> %s", key, oldValue));
      ctxt.writeList.add(new Remove<>(key));
      return oldValue;
    }

    void forEach(BiConsumer<K, V> consumer) {
      Context<K> ctxt = QsbMap.this.ctxt.get();
      ctxt.throwIfWritePhaseIsDone();
      for (java.util.Map.Entry<K, V> entry : map.entrySet()) {
        K key = entry.getKey();
        V value = entry.getValue();
        consumer.accept(key, value);
        if (ctxt.writePhaseState != ContextState.STARTED) {
          ctxt.readSet.add(new Read<>(key, value == null ? null : value.getVersion()));
        }
      }
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

    @VisibleForTesting
    java.util.Map<K, V> getUnderlyingMap() {
      return map;
    }
  }

  private class QsbrSlot {
    private final Map table = new Map();
    private final BitSet readers = new BitSet();
    private final Object qsbrWaitLock = new Object();

    void enterReadPhase() {
      debugPrint(
          ctxt.get().threadId,
          String.format(
              "enterReadPhase: Start. ThreadId:%d, ActiveReaders:%s",
              ctxt.get().threadId, readers.cardinality()));
      synchronized (readers) {
        readers.set(ctxt.get().threadId);
      }
      debugPrint(
          ctxt.get().threadId,
          String.format(
              "enterReadPhase: End. ThreadId:%d, ActiveReaders:%s",
              ctxt.get().threadId, readers.cardinality()));
    }

    void exitReadPhase() {
      debugPrint(
          ctxt.get().threadId,
          String.format(
              "exitReadPhase: Start. ThreadId:%d, ActiveReaders:%s",
              ctxt.get().threadId, readers.cardinality()));
      // If the reader is also the writer, the flag should be already unset and the reader count is
      // decremented.
      // Nothing to do.
      boolean isInReadPhase;
      synchronized (readers) {
        isInReadPhase = readers.get(ctxt.get().threadId);
      }
      if (!isInReadPhase) {
        debugPrint(
            ctxt.get().threadId,
            String.format(
                "exitReadPhase: Already done. ThreadId:%d, ActiveReaders:%s",
                ctxt.get().threadId, readers.cardinality()));
        return;
      }
      boolean isNoReader;
      synchronized (readers) {
        readers.clear(ctxt.get().threadId);
        isNoReader = readers.isEmpty();
      }
      if (isNoReader) {
        synchronized (qsbrWaitLock) {
          qsbrWaitLock.notify();
        }
      }
      debugPrint(
          ctxt.get().threadId,
          String.format(
              "exitReadPhase: End. ThreadId:%d, ActiveReaders:%s",
              ctxt.get().threadId, readers.cardinality()));
    }

    void waitUntilNoReader() {
      debugPrint(
          ctxt.get().threadId,
          String.format(
              "waitUntilNoReader: Start. ThreadId:%d, ActiveReaders:%s",
              ctxt.get().threadId, readers.cardinality()));
      synchronized (qsbrWaitLock) {
        while (true) {
          synchronized (readers) {
            if (readers.isEmpty()) {
              debugPrint(
                  ctxt.get().threadId,
                  String.format(
                      "waitUntilNoReader: Empty. ThreadId:%d, ActiveReaders:%s",
                      ctxt.get().threadId, readers.cardinality()));
              break;
            }
          }
          try {
            debugPrint(
                ctxt.get().threadId,
                String.format(
                    "waitUntilNoReader: Waiting. ThreadId:%d, ActiveReaders:%s",
                    ctxt.get().threadId, readers.cardinality()));
            qsbrWaitLock.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
      }
    }
  }

  public QsbMap() {
    // Add active and inactive slots.
    slots.add(new QsbrSlot());
    slots.add(new QsbrSlot());
    debugPrint(
        String.format("Initialize: Map1:%s, Map2:%s", slots.get(0).table, slots.get(1).table));
  }

  void validateReadSet(QsbrSlot activeSlot) {
    Context<K> ctxt = this.ctxt.get();
    Map activeMap = activeSlot.table;
    for (Read<K> read : ctxt.readSet) {
      V currentValue = activeMap.getWithoutRecording(read.key);
      debugPrint(
          String.format("validateReadSet: ReadSetValue:%s, CurrentValue:%s", read, currentValue));
      if (read.version == null) {
        if (currentValue != null) {
          throw new QsbConflictException(
              String.format(
                  "The read key-value has been updated. Read version: null; Current version: %d",
                  currentValue.getVersion()));
        }
      } else {
        if (currentValue == null) {
          throw new QsbConflictException(
              String.format(
                  "The read key-value has been updated. Read version: %s; Current version: null",
                  read.version));
        }
        if (currentValue.getVersion() > ctxt.readVersion) {
          throw new QsbConflictException(
              String.format(
                  "The read key-value has been updated. Read version: %d; Current version: %d",
                  read.version, currentValue.getVersion()));
        }
      }
    }
  }

  void handleReadOperation(ReadOperatable<K, V> task) {
    Context<K> ctxt = this.ctxt.get();
    ctxt.startReadPhase();
    debugPrint(
        ctxt.threadId,
        String.format(
            "handleReadOperation: Start. ReadSlotId:%d, ReadVersion:%d",
            ctxt.readSlotId, ctxt.readVersion));
    QsbrSlot readSlot = getSlot(ctxt.readSlotId);
    Map table = readSlot.table;
    try {
      readSlot.enterReadPhase();
      debugPrint(
          ctxt.threadId,
          String.format("handleReadOperation: Executing Task. ReadSlotId:%d", ctxt.readSlotId));
      task.operate();
      debugPrint(
          ctxt.threadId,
          String.format("handleReadOperation: Executed Task. ReadSlotId:%d", ctxt.readSlotId));
      validateReadSet(readSlot);
    } finally {
      readSlot.exitReadPhase();
      ctxt.finishReadPhase();
    }
  }

  void handleWriteOperation(WriteOperatable<K, V> task) {
    Context<K> ctxt = this.ctxt.get();
    debugPrint(
        ctxt.threadId,
        String.format(
            "handleWriteOperation: Start. ActiveSlotId:%d, WriteListSize:%d",
            state.activeSlotId, ctxt.writeList.size()));

    // If the writer is also in the read phase, exit it to decrement the reader count.
    debugPrint(
        ctxt.threadId,
        String.format(
            "handleWriteOperation: Check if in read phase. ReadSlotId:%d", ctxt.readSlotId));
    if (ctxt.readSlotId != null) {
      QsbrSlot readSlot = slots.get(ctxt.readSlotId);
      debugPrint(
          ctxt.threadId,
          String.format(
              "handleWriteOperation: Exiting read phase. ReadSlotId:%d, ActiveReaders:%s",
              ctxt.readSlotId, readSlot.readers.cardinality()));
      readSlot.exitReadPhase();
    }

    Integer activeSlotIdBeforeSwitch = null;
    try {
      debugPrint(
          ctxt.threadId,
          String.format("handleWriteOperation: Waiting lock. ActiveSlotId:%d", state.activeSlotId));
      writerLock.lock();
      ctxt.startWritePhase();

      debugPrint(
          ctxt.threadId,
          String.format(
              "handleWriteOperation: Acquired Lock. ActiveSlotId:%d", activeSlotIdBeforeSwitch));

      activeSlotIdBeforeSwitch = state.activeSlotId;
      QsbrSlot activeSlotBeforeSwitch = getSlot(activeSlotIdBeforeSwitch);
      Map activeTableBeforeSwitch = activeSlotBeforeSwitch.table;

      int inactiveSlotIdBeforeSwitch = getInactiveSlotId(activeSlotIdBeforeSwitch);
      QsbrSlot inactiveSlotBeforeSwitch = getSlot(inactiveSlotIdBeforeSwitch);
      Map inactiveTableBeforeSwitch = inactiveSlotBeforeSwitch.table;

      // Check if the inactive slot has been updated by the preceding writer thread.
      validateReadSet(inactiveSlotBeforeSwitch);

      long nextVersion = state.nextVersion();
      task.operate();

      // Switch the slots.
      debugPrint(
          ctxt.threadId,
          String.format(
              "handleWriteOperation: Switching ActiveSlotId. %d -> %d. Actual ActiveSlotId:%d",
              activeSlotIdBeforeSwitch, inactiveSlotIdBeforeSwitch, state.activeSlotId));
      assert state.activeSlotId == activeSlotIdBeforeSwitch;

      state.update(nextVersion, inactiveSlotIdBeforeSwitch);

      // Wait for all readers to finish with the current inactive slot.
      debugPrint(
          ctxt.threadId,
          String.format(
              "handleWriteOperation: Wait until no readers. CurrentInactiveSlotId:%d",
              activeSlotIdBeforeSwitch));
      activeSlotBeforeSwitch.waitUntilNoReader();

      //      debugPrint(context.get().threadId, String.format("  Commands:%s",
      // inactiveTable.commands));

      debugPrint(
          ctxt.threadId,
          String.format(
              "handleWriteOperation: Applying write operations. TargetSlotId:%d",
              activeSlotIdBeforeSwitch));

      // Apply the recent commands to the current inactive slot table.
      for (Write<K> write : ctxt.writeList) {
        if (write instanceof Put) {
          Put<K, V> put = (Put<K, V>) write;
          activeTableBeforeSwitch.putWithoutRecording(put.key, put.value);
          debugPrint(
              this.ctxt.get().threadId, String.format("  APPLY-PUT: %s -> %s", put.key, put.value));
        } else if (write instanceof Remove) {
          Remove<K> remove = (Remove<K>) write;
          activeTableBeforeSwitch.removeWithoutRecording(remove.key);
          debugPrint(this.ctxt.get().threadId, String.format("  APPLY-RMV: %s", remove.key));
        } else {
          throw new AssertionError();
        }
      }
    } catch (Exception e) {
      debugPrint(
          ctxt.threadId,
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
      ctxt.finishWritePhase();
      writerLock.unlock();
      debugPrint(
          ctxt.threadId,
          String.format(
              "handleWriteOperation: Quiting. ActiveSlotId before switch:%d, Current ActiveSlotId:%d",
              activeSlotIdBeforeSwitch, state.activeSlotId));
    }
  }

  Map getMap() {
    int slotId;
    if (ctxt.get().writePhaseState == ContextState.STARTED) {
      slotId = ctxt.get().writeSlotId;
    } else {
      slotId = ctxt.get().readSlotId;
    }
    return slots.get(slotId).table;
  }

  long getWriteVersion() {
    Long writeVersion = ctxt.get().writeVersion;
    if (writeVersion == null) {
      throw new IllegalStateException();
    }
    return writeVersion;
  }

  private static int getInactiveSlotId(int activeSlotId) {
    return ++activeSlotId % 2;
  }

  private QsbrSlot getSlot(int slotId) {
    return slots.get(slotId);
  }

  @VisibleForTesting
  long getVersionForTesting() {
    return state.version.get();
  }

  @VisibleForTesting
  Map getActiveMapForTesting() {
    return slots.get(state.activeSlotId).table;
  }

  @Nullable
  V get(K key) {
    return getMap().get(key);
  }

  @Nullable
  V put(K key, V value) {
    return getMap().put(key, value);
  }

  @Nullable
  V remove(K key) {
    return getMap().remove(key);
  }

  void forEach(BiConsumer<K, V> consumer) {
    getMap().forEach(consumer);
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
