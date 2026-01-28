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
  private final ThreadLocal<Boolean> isInReadPhase = ThreadLocal.withInitial(() -> false);
  private final Lock writerLock = new ReentrantLock(true);

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
      debugPrint(String.format("COMMAND-PUT: %s -> %s", key, value));
      V oldValue = super.put(key, value);
      commands.add(new PutCommand<>(key, value));
      return oldValue;
    }

    @Override
    public V remove(Object key) {
      debugPrint(String.format("COMMAND-RMV: %s", key));
      V oldValue = super.remove(key);
      commands.add(new RemoveCommand<>(key));
      return oldValue;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
      debugPrint(String.format("COMMAND-PAL"));
      super.putAll(m);
      m.forEach((key, value) -> commands.add(new PutCommand<>(key, value)));
    }

    @Override
    public void clear() {
      debugPrint(String.format("COMMAND-CLR"));
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
    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      throw new UnsupportedOperationException();
    }

    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      throw new UnsupportedOperationException();
    }

    @Override
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
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
    private final AtomicInteger readerCount = new AtomicInteger();
    private final Object qsbrWaitLock = new Object();

    void enterReadPhase() {
      debugPrint(String.format("enterReadPhase: Start. ReaderCount:%d, InReadPhase:%s", readerCount.get(), isInReadPhase.get()));
      readerCount.incrementAndGet();
      isInReadPhase.set(true);
      debugPrint(String.format("enterReadPhase: End. ReaderCount:%d, InReadPhase:%s", readerCount.get(), isInReadPhase.get()));
    }

    void exitReadPhase() {
      debugPrint(String.format("exitReadPhase: Start. ReaderCount:%d, InReadPhase:%s", readerCount.get(), isInReadPhase.get()));
      // If the reader is also the writer, the flag should be already unset and the reader count is
      // decremented.
      // Nothing to do.
      if (!isInReadPhase.get()) {
        debugPrint(String.format("exitReadPhase: Already done. ReaderCount:%d, InReadPhase:%s", readerCount.get(), isInReadPhase.get()));
        return;
      }
      int currentReaderCount = readerCount.decrementAndGet();
      assert currentReaderCount >= 0;
      if (currentReaderCount == 0) {
        synchronized (qsbrWaitLock) {
          qsbrWaitLock.notify();
        }
      }
      isInReadPhase.set(false);
      debugPrint(String.format("exitReadPhase: End. ReaderCount:%d, InReadPhase:%s", readerCount.get(), isInReadPhase.get()));
    }

    void waitUntilNoReader() {
      debugPrint("waitUntilNoReader: Start");
      synchronized (qsbrWaitLock) {
        while (true) {
          int currentReaderCount = readerCount.get();
          debugPrint(String.format("waitUntilNoReader: ReadCounter:%d", currentReaderCount));
          assert currentReaderCount >= 0;
          if (currentReaderCount == 0) {
            break;
          }
          try {
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
    debugPrint("handleReadOperation: Start");
    QsbrSlot activeSlot = getActiveSlot();
    Map<K, V> table = activeSlot.table;
    try {
      activeSlot.enterReadPhase();
      debugPrint("handleReadOperation: Executing Task");
      task.accept(table);
      debugPrint("handleReadOperation: Executed Task");
    } finally {
      activeSlot.exitReadPhase();
    }
  }

  void handleWriteOperation(BiConsumer<Long, Map<K, V>> task) {
    debugPrint(String.format("handleWriteOperation: Start. ActiveSlotId:%d", activeSlotId));
    // If the writer is also in the read phase, exit it to decrement the reader count.
    if (isInReadPhase.get()) {
      getActiveSlot().exitReadPhase();
    }

    writerLock.lock();

    debugPrint(String.format("handleWriteOperation: Acquired Lock. ActiveSlotId:%d", activeSlotId));

    Map<K, V> activeTable = getActiveSlot().table;
    QsbrSlot inactiveSlot = getInactiveSlot();
    CommandRecordingMap<K, V> inactiveTable = inactiveSlot.table;
    try {
      long nextVersion = version.get() + 1;
      task.accept(nextVersion, inactiveTable);
      version.set(nextVersion);
      // Switch the slots.
      activeSlotId = getInactiveSlotId(activeSlotId);

      // Wait for all readers to finish with the current inactive slot.
      QsbrSlot currentInactiveSlot = getInactiveSlot();
      currentInactiveSlot.waitUntilNoReader();

      debugPrint(String.format("  Commands:%s", inactiveTable.commands));

      // Apply the recent commands to the current inactive slot table.
      for (CommandRecordingMap.Command command : inactiveTable.commands) {
        if (command instanceof CommandRecordingMap.PutCommand) {
          CommandRecordingMap.PutCommand<K, V> specificCommand =
              (CommandRecordingMap.PutCommand<K, V>) command;
          currentInactiveSlot.table.put(specificCommand.key, specificCommand.value);
          debugPrint(String.format("  PUT: %s -> %s", specificCommand.key, specificCommand.value));
        } else if (command instanceof CommandRecordingMap.RemoveCommand) {
          CommandRecordingMap.RemoveCommand<K> specificCommand =
              (CommandRecordingMap.RemoveCommand<K>) command;
          currentInactiveSlot.table.remove(specificCommand.key);
          debugPrint(String.format("  RM:  %s", specificCommand.key));
        } else {
          assert command instanceof CommandRecordingMap.ClearCommand;
          currentInactiveSlot.table.clear();
          debugPrint(String.format("  CLR"));
        }
      }
      inactiveTable.resetCommands();
    } catch (Exception e) {
      Consumer<K> restorer =
          key -> {
            V origValue = activeTable.get(key);
            if (origValue == null) {
              inactiveTable.removeWithoutRecordingCommand(key);
            } else {
              inactiveTable.putWithoutRecordingCommand(key, origValue);
            }
          };
      // Revert commands applied to the slot.
      for (int i = 0; i < inactiveTable.commands.size(); i++) {
        CommandRecordingMap.Command command = inactiveTable.commands.get(i);
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
          inactiveTable.clearWithoutRecordingCommand();
          inactiveTable.putAllWithoutRecordingCommand(activeTable);
        }
        inactiveTable.resetCommands();
      }
      throw e;
    } finally {
      writerLock.unlock();
    }
  }

  private int getInactiveSlotId(int activeSlotId) {
    return ++activeSlotId % 2;
  }

  Map<K, V> getTable() {
    return getActiveSlot().table;
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
  private static void debugPrint(String msg) {
    System.out.printf("%s [%s] %s%n", Instant.now(), Thread.currentThread().getName(), msg);
  }
}
