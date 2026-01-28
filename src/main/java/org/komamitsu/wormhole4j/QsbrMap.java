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

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

class QsbrMap<K, V> {
  private final AtomicLong version = new AtomicLong();
  private final List<QsbrSlot> slots = new ArrayList<>();
  private volatile int activeSlotId = 0;
  private final Lock writerLock = new ReentrantLock(true);
  private final AtomicInteger threadIdCounter = new AtomicInteger();
  private final ThreadLocal<Integer> threadId =
      ThreadLocal.withInitial(threadIdCounter::getAndIncrement);
  private final ThreadLocal<Integer> readSlotId = new ThreadLocal<>();
  // TODO: Should manage read-set?

  private static class CommandRecordingMap<K, V> extends HashMap<K, V> {
    private final List<Command> commands = new ArrayList<>();

    private interface Command {}

    private static class PutCommand<K, V> implements Command {
      private final K key;
      private final V value;

      private PutCommand(K key, V value) {
        this.key = key;
        this.value = value;
      }
    }

    private static class RemoveCommand<K> implements Command {
      private final K key;

      public RemoveCommand(K key) {
        this.key = key;
      }
    }

    private static class ClearCommand implements Command {}

    @Override
    public V put(K key, V value) {
      //      debugPrint(String.format("COMMAND-PUT: %s -> %s", key, value));
      V oldValue = super.put(key, value);
      commands.add(new PutCommand<>(key, value));
      return oldValue;
    }

    @Override
    public V remove(Object key) {
      //      debugPrint(String.format("COMMAND-RMV: %s", key));
      V oldValue = super.remove(key);
      commands.add(new RemoveCommand<>(key));
      return oldValue;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
      //      debugPrint(String.format("COMMAND-PAL"));
      super.putAll(m);
      m.forEach((key, value) -> commands.add(new PutCommand<>(key, value)));
    }

    @Override
    public void clear() {
      //      debugPrint(String.format("COMMAND-CLR"));
      super.clear();
      commands.add(new ClearCommand());
    }

    @Override
    public boolean remove(Object key, Object value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
      throw new UnsupportedOperationException();
    }

    @Override
    public V replace(K key, V value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
      throw new UnsupportedOperationException();
    }

    @Override
    public V computeIfPresent(
        K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      throw new UnsupportedOperationException();
    }

    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      throw new UnsupportedOperationException();
    }

    @Override
    public V merge(
        K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
      super.replaceAll(function);
      throw new UnsupportedOperationException();
    }

    @Override
    public V putIfAbsent(K key, V value) {
      throw new UnsupportedOperationException();
    }

    private V putWithoutRecordingCommand(K key, V value) {
      return super.put(key, value);
    }

    private V removeWithoutRecordingCommand(Object key) {
      return super.remove(key);
    }

    private void putAllWithoutRecordingCommand(Map<? extends K, ? extends V> m) {
      super.putAll(m);
    }

    private void clearWithoutRecordingCommand() {
      super.clear();
    }

    private void resetCommands() {
      commands.clear();
    }
  }

  private class QsbrSlot {
    private final CommandRecordingMap<K, V> table = new CommandRecordingMap<>();
    private final BitSet readers = new BitSet();
    private final Object qsbrWaitLock = new Object();

    void enterReadPhase() {
      debugPrint(
          threadId.get(),
          String.format("enterReadPhase: Start. ThreadId:%d, Readers:%s", threadId.get(), readers));
      readers.set(threadId.get());
      debugPrint(
          threadId.get(),
          String.format("enterReadPhase: End. ThreadId:%d, Readers:%s", threadId.get(), readers));
    }

    void exitReadPhase() {
      debugPrint(
          threadId.get(),
          String.format("exitReadPhase: Start. ThreadId:%d, Readers:%s", threadId.get(), readers));
      // If the reader is also the writer, the flag should be already unset and the reader count is
      // decremented.
      // Nothing to do.
      if (!readers.get(threadId.get())) {
        debugPrint(
            threadId.get(),
            String.format(
                "exitReadPhase: Already done. ThreadId:%d, Readers:%s", threadId.get(), readers));
        return;
      }
      readers.clear(threadId.get());
      if (readers.isEmpty()) {
        synchronized (qsbrWaitLock) {
          qsbrWaitLock.notify();
        }
      }
      debugPrint(
          threadId.get(),
          String.format("exitReadPhase: End. ThreadId:%d, Readers:%s", threadId.get(), readers));
    }

    void waitUntilNoReader() {
      debugPrint(
          threadId.get(),
          String.format(
              "waitUntilNoReader: Start. ThreadId:%d, Readers:%s", threadId.get(), readers));
      synchronized (qsbrWaitLock) {
        while (true) {
          if (readers.isEmpty()) {
            debugPrint(
                threadId.get(),
                String.format(
                    "waitUntilNoReader: Empty. ThreadId:%d, Readers:%s", threadId.get(), readers));
            break;
          }
          try {
            debugPrint(
                threadId.get(),
                String.format(
                    "waitUntilNoReader: Waiting. ThreadId:%d, Readers:%s",
                    threadId.get(), readers));
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
  }

  void handleReadOperation(Consumer<Map<K, V>> task) {
    readSlotId.set(activeSlotId);
    debugPrint(
        threadId.get(),
        String.format("handleReadOperation: Start. ReadSlotId:%d", readSlotId.get()));
    QsbrSlot readSlot = getSlot(readSlotId.get());
    Map<K, V> table = readSlot.table;
    try {
      readSlot.enterReadPhase();
      debugPrint(
          threadId.get(),
          String.format("handleReadOperation: Executing Task. ReadSlotId:%d", readSlotId.get()));
      task.accept(table);
      debugPrint(
          threadId.get(),
          String.format("handleReadOperation: Executed Task. ReadSlotId:%d", readSlotId.get()));
    } finally {
      readSlot.exitReadPhase();
      readSlotId.remove();
    }
  }

  void handleWriteOperation(BiConsumer<Long, Map<K, V>> task) {
    debugPrint(
        threadId.get(),
        String.format("handleWriteOperation: Start. ActiveSlotId:%d", activeSlotId));

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
              "handleWriteOperation: Exiting read phase. ReadSlotId:%d, Readers:%s",
              readSlotId.get(), readSlot.readers));
      readSlot.exitReadPhase();
      readSlotId.remove();
    }

    Integer activeSlotIdBeforeSwitch = null;
    try {
      debugPrint(
          threadId.get(),
          String.format("handleWriteOperation: Waiting lock. ActiveSlotId:%d", activeSlotId));
      writerLock.lock();
      activeSlotIdBeforeSwitch = activeSlotId;
      QsbrSlot activeSlotBeforeSwitch = getSlot(activeSlotIdBeforeSwitch);
      Map<K, V> activeTableBeforeSwitch = activeSlotBeforeSwitch.table;
      CommandRecordingMap<K, V> inactiveTableBeforeSwitch =
          getSlot(getInactiveSlotId(activeSlotIdBeforeSwitch)).table;

      debugPrint(
          threadId.get(),
          String.format(
              "handleWriteOperation: Acquired Lock. ActiveSlotId:%d", activeSlotIdBeforeSwitch));

      long nextVersion = version.get() + 1;
      task.accept(nextVersion, inactiveTableBeforeSwitch);
      version.set(nextVersion);
      // Switch the slots.
      debugPrint(
          threadId.get(),
          String.format(
              "handleWriteOperation: Switching ActiveSlotId. %d -> %d. Actual ActiveSlotId:%d",
              activeSlotIdBeforeSwitch, getInactiveSlotId(activeSlotIdBeforeSwitch), activeSlotId));
      assert activeSlotId == activeSlotIdBeforeSwitch;
      activeSlotId = getInactiveSlotId(activeSlotIdBeforeSwitch);

      // Wait for all readers to finish with the current inactive slot.
      debugPrint(
          threadId.get(),
          String.format(
              "handleWriteOperation: Wait until no readers. CurrentInactiveSlotId:%d",
              activeSlotIdBeforeSwitch));
      activeSlotBeforeSwitch.waitUntilNoReader();

      //      debugPrint(threadId.get(), String.format("  Commands:%s", inactiveTable.commands));

      // Apply the recent commands to the current inactive slot table.
      for (CommandRecordingMap.Command command : inactiveTableBeforeSwitch.commands) {
        if (command instanceof CommandRecordingMap.PutCommand) {
          CommandRecordingMap.PutCommand<K, V> specificCommand =
              (CommandRecordingMap.PutCommand<K, V>) command;
          activeTableBeforeSwitch.put(specificCommand.key, specificCommand.value);
          //          debugPrint(threadId.get(), String.format("  PUT: %s -> %s",
          // specificCommand.key, specificCommand.value));
        } else if (command instanceof CommandRecordingMap.RemoveCommand) {
          CommandRecordingMap.RemoveCommand<K> specificCommand =
              (CommandRecordingMap.RemoveCommand<K>) command;
          activeTableBeforeSwitch.remove(specificCommand.key);
          //          debugPrint(threadId.get(), String.format("  RM:  %s", specificCommand.key));
        } else {
          assert command instanceof CommandRecordingMap.ClearCommand;
          activeTableBeforeSwitch.clear();
          //          debugPrint(threadId.get(), String.format("  CLR"));
        }
      }
      inactiveTableBeforeSwitch.resetCommands();
    } catch (Exception e) {
      if (activeSlotIdBeforeSwitch == null) {
        // Something happened when acquiring the write lock.
        return;
      }
      QsbrSlot activeSlotBeforeSwitch = getSlot(activeSlotIdBeforeSwitch);
      Map<K, V> activeTableBeforeSwitch = activeSlotBeforeSwitch.table;
      CommandRecordingMap<K, V> inactiveTableBeforeSwitch =
          getSlot(getInactiveSlotId(activeSlotIdBeforeSwitch)).table;

      Consumer<K> restorer =
          key -> {
            V origValue = activeTableBeforeSwitch.get(key);
            if (origValue == null) {
              inactiveTableBeforeSwitch.removeWithoutRecordingCommand(key);
            } else {
              inactiveTableBeforeSwitch.putWithoutRecordingCommand(key, origValue);
            }
          };
      // Revert commands applied to the slot.
      for (int i = 0; i < inactiveTableBeforeSwitch.commands.size(); i++) {
        CommandRecordingMap.Command command = inactiveTableBeforeSwitch.commands.get(i);
        if (command instanceof CommandRecordingMap.PutCommand) {
          CommandRecordingMap.PutCommand<K, V> putCommand =
              (CommandRecordingMap.PutCommand<K, V>) command;
          restorer.accept(putCommand.key);
        } else if (command instanceof CommandRecordingMap.RemoveCommand) {
          CommandRecordingMap.RemoveCommand<K> removeCommand =
              (CommandRecordingMap.RemoveCommand<K>) command;
          restorer.accept(removeCommand.key);
        } else {
          assert command instanceof CommandRecordingMap.ClearCommand;
          inactiveTableBeforeSwitch.clearWithoutRecordingCommand();
          inactiveTableBeforeSwitch.putAllWithoutRecordingCommand(activeTableBeforeSwitch);
        }
        inactiveTableBeforeSwitch.resetCommands();
      }
      throw e;
    } finally {
      writerLock.unlock();
      debugPrint(
          threadId.get(),
          String.format(
              "handleWriteOperation: Released Lock. ActiveSlotId before switch:%d, Current ActiveSlotId:%d",
              activeSlotIdBeforeSwitch, activeSlotId));
    }
  }

  private int getInactiveSlotId(int activeSlotId) {
    return ++activeSlotId % 2;
  }

  Map<K, V> getTable() {
    return getActiveSlot().table;
  }

  private QsbrSlot getSlot(int slotId) {
    return slots.get(slotId);
  }

  private QsbrSlot getActiveSlot() {
    return slots.get(activeSlotId);
  }

  private QsbrSlot getInactiveSlot() {
    return slots.get(getInactiveSlotId(activeSlotId));
  }

  long getVersion() {
    return version.get();
  }

  // TODO: Remove
  private static void debugPrint(int threadId, String msg) {
    System.out.printf(
        "%s [%s] (%d) %s%n", Instant.now(), Thread.currentThread().getName(), threadId, msg);
  }

  private static void debugPrint(String msg) {
    System.out.printf("%s [%s] %s%n", Instant.now(), Thread.currentThread().getName(), msg);
  }
}
