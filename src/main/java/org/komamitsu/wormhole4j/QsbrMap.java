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
import java.util.function.BiConsumer;
import java.util.function.Consumer;

class QsbrMap<K, V> {
  private final AtomicLong version = new AtomicLong();
  private final List<QsbrSlot> slots = new ArrayList<>();
  private volatile int activeSlotId = 0;
  private final ThreadLocal<Boolean> isInReadPhase = ThreadLocal.withInitial(() -> false);

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
      V oldValue = super.put(key, value);
      commands.add(new PutCommand<>(key, value));
      return oldValue;
    }

    @Override
    public V remove(Object key) {
      V oldValue = super.remove(key);
      commands.add(new RemoveCommand<>(key));
      return oldValue;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
      super.putAll(m);
      m.forEach((key, value) -> commands.add(new PutCommand<>(key, value)));
    }

    @Override
    public void clear() {
      super.clear();
      commands.add(new ClearCommand());
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
    private final Object lock = new Object();

    void enterReadPhase() {
      readerCount.incrementAndGet();
      isInReadPhase.set(true);
    }

    void exitReadPhase() {
      if (readerCount.decrementAndGet() == 0) {
        synchronized (lock) {
          lock.notify();
        }
      }
      isInReadPhase.set(false);
    }

    void waitUntilNoReader() {
      synchronized (lock) {
        int expectedReaderCountToExit = isInReadPhase.get() ? 1 : 0;
        while (readerCount.get() != expectedReaderCountToExit) {
          try {
            lock.wait();
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

  synchronized void handleReadOperation(Consumer<Map<K, V>> task) {
    QsbrSlot activeSlot = getActiveSlot();
    Map<K, V> table = activeSlot.table;
    try {
      activeSlot.enterReadPhase();
      task.accept(table);
    } finally {
      activeSlot.exitReadPhase();
    }
  }

  synchronized void handleWriteOperation(BiConsumer<Long, Map<K, V>> task) {
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

      // Apply the recent commands to the current inactive slot table.
      for (CommandRecordingMap.Command command : inactiveTable.commands) {
        if (command instanceof CommandRecordingMap.PutCommand) {
          CommandRecordingMap.PutCommand<K, V> putCommand =
              (CommandRecordingMap.PutCommand<K, V>) command;
          currentInactiveSlot.table.put(putCommand.key, putCommand.value);
        } else if (command instanceof CommandRecordingMap.RemoveCommand) {
          CommandRecordingMap.RemoveCommand<K> removeCommand =
              (CommandRecordingMap.RemoveCommand<K>) command;
          currentInactiveSlot.table.remove(removeCommand.key);
        } else {
          assert command instanceof CommandRecordingMap.ClearCommand;
          currentInactiveSlot.table.clear();
        }
      }
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
      }
      throw e;
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
}
