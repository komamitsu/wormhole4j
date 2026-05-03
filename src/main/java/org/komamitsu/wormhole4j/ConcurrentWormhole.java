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

import java.io.BufferedWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.komamitsu.wormhole4j.MetaTrieHashTable.NodeMetaLeaf;

/**
 * Wormhole is an in-memory ordered index for key-value pairs.
 *
 * <p>This implementation supports fast lookups, inserts, deletes, and range scans.
 *
 * @param <K> the type of keys stored in this index
 * @param <V> the type of values stored in this index
 */
abstract class ConcurrentWormhole<K, V> extends Wormhole<K, V> {
  private static final int SPIN_INTERVAL = 64;
  // These don't need to be volatile since they are only updated in synchronized blocks.
  private long version;
  private int metaTableIndex;

  private final List<MetaTrieHashTable<K, V>> metaTables;
  private final StampedLock metaTableLock = new StampedLock();
  private final List<AtomicReference<Long>> qsbrVersions;
  private final BitSet qsbrThreads;
  private final ThreadLocal<Integer> qsbrThreadLocalIndexes = new ThreadLocal<>();
  private final ThreadLocal<AtomicReference<Long>> qsbrThreadLocalVersions = new ThreadLocal<>();
  private final ThreadLocal<MetaTrieHashTable<K, V>> qsbrThreadLocalMetaTables =
      new ThreadLocal<>();

  /**
   * Creates a Wormhole.
   *
   * @param encodedKeyType the encoded key type
   * @param leafNodeSize maximum number of entries in a leaf node
   */
  protected ConcurrentWormhole(EncodedKeyType encodedKeyType, int leafNodeSize) {
    super(encodedKeyType, leafNodeSize);
    this.metaTables =
        Arrays.asList(
            new MetaTrieHashTable<>(encodedKeyType), new MetaTrieHashTable<>(encodedKeyType));
    this.qsbrVersions = new ArrayList<>();
    this.qsbrThreads = new BitSet();
    initialize();
  }

  private void initialize() {
    Object encodedKey = createEmptyEncodedKey();
    LeafNode<K, V> rootLeafNode = createRootLeafNode(encodedKey);
    metaTables.get(0).put(encodedKey, new NodeMetaLeaf<>(encodedKey, rootLeafNode));
    metaTables.get(1).put(encodedKey, new NodeMetaLeaf<>(encodedKey, rootLeafNode));
  }

  @Override
  protected MetaTrieHashTable<K, V> getActiveMetaTable() {
    return metaTables.get(metaTableIndex);
  }

  protected MetaTrieHashTable<K, V> getInactiveMetaTable() {
    return metaTables.get(getInactiveMetaTableIndex());
  }

  private int getQsbrThreadIndex() {
    return qsbrThreadLocalIndexes.get();
  }

  @Override
  public synchronized void registerThread() {
    if (qsbrThreadLocalIndexes.get() != null) {
      return;
    }
    int availableThreadIndex = qsbrThreads.nextClearBit(0);
    AtomicReference<Long> versionContainer = new AtomicReference<>();
    qsbrThreadLocalIndexes.set(availableThreadIndex);
    qsbrThreadLocalVersions.set(versionContainer);
    qsbrThreads.set(availableThreadIndex);
    registerQsbrVersion(availableThreadIndex, versionContainer);
  }

  @Override
  public synchronized void unregisterThread() {
    if (qsbrThreadLocalIndexes.get() == null) {
      return;
    }
    int qsbrThreadIndex = getQsbrThreadIndex();
    qsbrVersions.set(qsbrThreadIndex, null);
    qsbrThreads.clear(qsbrThreadIndex);
    qsbrThreadLocalMetaTables.remove();
    qsbrThreadLocalVersions.remove();
    qsbrThreadLocalIndexes.remove();
  }

  private void registerQsbrVersion(int threadIndex, AtomicReference<Long> versionContainer) {
    while (qsbrVersions.size() <= threadIndex) {
      qsbrVersions.add(null);
    }
    qsbrVersions.set(threadIndex, versionContainer);
  }

  private void qsbrEnter() {
    AtomicReference<Long> qsbrThreadLocalVersion = qsbrThreadLocalVersions.get();
    if (qsbrThreadLocalVersion == null) {
      throw new IllegalStateException("This thread is not registered yet");
    }
    qsbrThreadLocalVersion.set(version);
    qsbrThreadLocalMetaTables.set(metaTables.get(metaTableIndex));
  }

  private void qsbrExit() {
    AtomicReference<Long> qsbrThreadLocalVersion = qsbrThreadLocalVersions.get();
    if (qsbrThreadLocalVersion == null) {
      throw new IllegalStateException("This thread is not registered yet");
    }
    qsbrThreadLocalMetaTables.set(null);
    qsbrThreadLocalVersion.set(null);
  }

  private long getLocalVersion() {
    return qsbrThreadLocalVersions.get().get();
  }

  // This method is called by a thread which has acquired the meta table lock.
  // Also, only a few methods takes the synchronized lock which are not frequently called.
  // So method scope synchronized is fine.
  private synchronized void qsbrWait(long newVersion, int threadIndexSkipped) {
    int threadIndex = 0;
    while (true) {
      threadIndex = qsbrThreads.nextSetBit(threadIndex);
      if (threadIndex < 0) {
        break;
      }
      if (threadIndex == threadIndexSkipped) {
        threadIndex++;
        continue;
      }
      int loopCount = 0;
      while (true) {
        if (++loopCount % SPIN_INTERVAL == 0) {
          // TODO: Use Thread.onSpinWait() if possible.
          Thread.yield();
        }
        Long localVersion = qsbrVersions.get(threadIndex).get();
        if (localVersion == null || localVersion == newVersion) {
          break;
        }
      }
      threadIndex++;
    }
  }

  private long tryLockOnMetaTable() {
    return metaTableLock.tryWriteLock();
  }

  private long acquireReadLockOnMetaTable() {
    return metaTableLock.readLock();
  }

  private long acquireWriteLockOnMetaTable() {
    return metaTableLock.writeLock();
  }

  private void releaseLockOnMetaTable(long stamp) {
    metaTableLock.unlock(stamp);
  }

  private int getInactiveMetaTableIndex() {
    return metaTableIndex == 0 ? 1 : 0;
  }

  private synchronized void switchMetaTable(long newVersion) {
    metaTableIndex = getInactiveMetaTableIndex();
    version = newVersion;
  }

  void p(String op, Object key, String msg) {
    if (writer != null) {
//      System.out.printf("[%s] (%s) <%s:%s> %s%n", Instant.now(), Thread.currentThread().getName(), op, key, msg);
      try {
        writer.append(
            String.format("[%s] (%s) <%s:%s> %s%n", Instant.now(), Thread.currentThread().getName(), op, key, msg));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static BufferedWriter writer;
  static String testName = "";
  static int counter;

  /**
   * Inserts or updates a key-value pair.
   *
   * @param key the key (must not be {@code null})
   * @param value the value to associate with the key
   * @return the previous value associated with the key, or {@code null} if there was no previous
   *     mapping
   */
  @Override
  @Nullable
  public V put(K key, V value) {
    String op = "PUT";
    p(op, key, "start. test name=" + testName + ", counter=" + counter);
    Object encodedKey = createEncodedKey(key);
    int loopCount = 0;
    while (true) {
      if (++loopCount % SPIN_INTERVAL == 0) {
        Thread.yield();
      }
      qsbrEnter();
      p(op, key, "entering qsbr");
      LeafNode<K, V> leafNode = searchTrieHashTable(qsbrThreadLocalMetaTables.get(), encodedKey);
      p(op, key, "got leaf node. id=" + leafNode.id);
      long writeLockOnLeafNode = leafNode.tryWriteLock();
      if (writeLockOnLeafNode == 0) {
        p(op, key, "failed to get write lock on leaf node");
        qsbrExit();
        p(op, key, "exited qsbr(1)");
        continue;
      }
      try {
        if (leafNode.getVersion() > qsbrThreadLocalVersions.get().get()) {
          p(op, key, "got stale leaf node");
          continue;
        }
        Optional<V> existingValue = leafNode.lookupAndPutValue(encodedKey, key, value);
        if (existingValue != null) {
          p(op, key, "updating leaf node and returning");
          return existingValue.orElse(null);
        }
        long writeLockOnMetaTable = tryLockOnMetaTable();
        if (writeLockOnMetaTable == 0) {
          p(op, key, "failed to get write lock on meta table");
          continue;
        }
        try {
          long newVersion = version + 1;

          // This new leaf is already locked.
          LeafNode<K, V> newLeafNode = splitLeafNode(leafNode, encodedKey, key, value);
          p(op, key, "got new leaf node. id=" + newLeafNode.id);
          addNewLeafNodeToMetaTable(getInactiveMetaTable(), newLeafNode);
          p(op, key, "added new leaf node to meta table(1)");

          // Increment versions and switch to the updated meta table.
          leafNode.setVersion(newVersion);
          newLeafNode.setVersion(newVersion);
          p(op, key, "updated leaf node versions. new version" + newVersion);
          switchMetaTable(newVersion);
          p(op, key, "switched meta table");
          newLeafNode.releaseLock(newLeafNode.getInitialLockStamp());
          leafNode.releaseLock(writeLockOnLeafNode);
          writeLockOnLeafNode = 0;
          p(op, key, "released leaf node locks(1)");

          // Wait until no reader threads on the previously active meta table.
          qsbrThreadLocalVersions.get().set(newVersion);
          qsbrWait(newVersion, getQsbrThreadIndex());
          p(op, key, "waited");

          addNewLeafNodeToMetaTable(getInactiveMetaTable(), newLeafNode);
          p(op, key, "added new leaf node to meta table(1)");
          return null;
        } finally {
          releaseLockOnMetaTable(writeLockOnMetaTable);
          p(op, key, "released meta table lock");
        }
      } finally {
        qsbrExit();
        p(op, key, "exited qsbr(2)");
        if (writeLockOnLeafNode != 0) {
          leafNode.releaseLock(writeLockOnLeafNode);
          p(op, key, "released leaf node locks(2)");
        }
      }
    }
  }

  /**
   * Deletes a key-value pair if present.
   *
   * @param key the key (must not be {@code null})
   * @return {@code true} if the key was removed, {@code false} otherwise
   */
  @Override
  public boolean delete(K key) {
    String op = "DEL";
    p(op, key, "start. test name=" + testName + ", counter=" + counter);
    Object encodedKey = createEncodedKey(key);
    int loopCount = 0;
    while (true) {
      if (++loopCount % SPIN_INTERVAL == 0) {
        Thread.yield();
      }
      qsbrEnter();
      p(op, key, "entering qsbr");
      long writeLockOnLeafNode = 0;
      long writeLockOnLeftLeafNode = 0;
      long writeLockOnRightLeafNode = 0;
      LeafNode<K, V> leafNode = searchTrieHashTable(qsbrThreadLocalMetaTables.get(), encodedKey);
      p(op, key, "got leaf node. id=" + leafNode.id);
      LeafNode<K, V> leftLeafNode = null;
      LeafNode<K, V> rightLeafNode = null;
      try {
        writeLockOnLeafNode = leafNode.tryWriteLock();
        if (writeLockOnLeafNode == 0) {
          p(op, key, "failed to get write lock on leaf node");
          continue;
        }
        if (leafNode.getVersion() > qsbrThreadLocalVersions.get().get()) {
          p(op, key, "got stale leaf node");
          continue;
        }
        leftLeafNode = leafNode.getLeft();
        if (leftLeafNode != null) {
          writeLockOnLeftLeafNode = leftLeafNode.tryWriteLock();
          if (writeLockOnLeftLeafNode == 0) {
            p(op, key, "failed to get write lock on left leaf node");
            continue;
          }
          if (leftLeafNode.getVersion() > qsbrThreadLocalVersions.get().get()) {
            p(op, key, "got stale left leaf node");
            continue;
          }
        }
        rightLeafNode = leafNode.getRight();
        if (rightLeafNode != null) {
          writeLockOnRightLeafNode = rightLeafNode.tryWriteLock();
          if (writeLockOnRightLeafNode == 0) {
            p(op, key, "failed to get write lock on right leaf node");
            continue;
          }
          if (rightLeafNode.getVersion() > qsbrThreadLocalVersions.get().get()) {
            p(op, key, "got stale right leaf node");
            continue;
          }
        }

        if (leafNode.lookupValue(encodedKey) == null) {
          p(op, key, "no deleted record. returning");
          return false;
        }
        long writeLockOnMetaTable = tryLockOnMetaTable();
        if (writeLockOnMetaTable == 0) {
          p(op, key, "failed to get write lock on meta table");
          continue;
        }
        boolean deleted = leafNode.delete(encodedKey);
        assert deleted;
        try {
          // Merge the nodes on the inactive meta table if needed.
          Tuple<LeafNode<K, V>, LeafNode<K, V>> mergedLeafNodes = mergeLeafNodesIfNeeded(leafNode);
          LeafNode<K, V> updatedLeafNode = null;
          LeafNode<K, V> mergedLeafNode = null;
          if (mergedLeafNodes != null) {
            updatedLeafNode = mergedLeafNodes.first;
            mergedLeafNode = mergedLeafNodes.second;
            p(op, key, "got updated leaf node. id=" + updatedLeafNode.id);
            p(op, key, "got merged leaf node. id=" + mergedLeafNode.id);
            removeMergedLeafNodeFromMetaTable(getInactiveMetaTable(), mergedLeafNode);
            p(op, key, "removed merged leaf node from meta table(1)");
          }

          // Increment versions and switch to the updated meta table.
          long newVersion = version + 1;
          leafNode.setVersion(newVersion);
          if (updatedLeafNode != null) {
            updatedLeafNode.setVersion(newVersion);
          }
          if (mergedLeafNode != null) {
            mergedLeafNode.setVersion(newVersion);
          }
          p(op, key, "updated leaf node versions. new version" + newVersion);
          switchMetaTable(newVersion);
          p(op, key, "switched meta table");
          leafNode.releaseLock(writeLockOnLeafNode);
          writeLockOnLeafNode = 0;
          if (writeLockOnRightLeafNode != 0) {
            rightLeafNode.releaseLock(writeLockOnRightLeafNode);
            writeLockOnRightLeafNode = 0;
          }
          if (writeLockOnLeftLeafNode != 0) {
            leftLeafNode.releaseLock(writeLockOnLeftLeafNode);
            writeLockOnLeftLeafNode = 0;
          }
          p(op, key, "released leaf node locks(1)");

          // Wait until no reader threads on the previously active meta table.
          qsbrThreadLocalVersions.get().set(newVersion);
          qsbrWait(newVersion, getQsbrThreadIndex());
          p(op, key, "waited");
          // Remove the merged leaf node from the inactive meta table.
          if (mergedLeafNode != null) {
            removeMergedLeafNodeFromMetaTable(getInactiveMetaTable(), mergedLeafNode);
            p(op, key, "removed merged leaf node from meta table(2)");
          }
          return true;
        } finally {
          releaseLockOnMetaTable(writeLockOnMetaTable);
          p(op, key, "released meta table lock");
        }
      } finally {
        qsbrExit();
        p(op, key, "exited qsbr(2)");
        if (writeLockOnRightLeafNode != 0) {
          rightLeafNode.releaseLock(writeLockOnRightLeafNode);
        }
        if (writeLockOnLeftLeafNode != 0) {
          leftLeafNode.releaseLock(writeLockOnLeftLeafNode);
        }
        if (writeLockOnLeafNode != 0) {
          leafNode.releaseLock(writeLockOnLeafNode);
        }
        p(op, key, "released leaf node locks(2)");
      }
    }
  }

  /**
   * Retrieves the value associated with the specified key.
   *
   * @param key the key (must not be {@code null})
   * @return the value, or {@code null} if not found
   */
  @Override
  @Nullable
  public V get(K key) {
    String op = "GET";
    p(op, key, "start. test name=" + testName + ", counter=" + counter);
    Object encodedKey = createEncodedKey(key);
    int loopCount = 0;
    while (true) {
      if (++loopCount % SPIN_INTERVAL == 0) {
        Thread.yield();
      }
      qsbrEnter();
      p(op, key, "entering qsbr");
      try {
        LeafNode<K, V> leafNode = searchTrieHashTable(qsbrThreadLocalMetaTables.get(), encodedKey);
        p(op, key, "got leaf node. id=" + leafNode.id);
        long readLockOnLeafNode = leafNode.tryReadLock();
        if (readLockOnLeafNode == 0) {
          p(op, key, "failed to get read lock on leaf node");
          continue;
        }
        try {
          V result = leafNode.lookupValue(encodedKey);
          if (leafNode.getVersion() <= getLocalVersion()) {
            p(op, key, "got valid value. value=" + result);
            return result;
          }
          p(op, key, "got stale leaf node");
        } finally {
          leafNode.releaseLock(readLockOnLeafNode);
          p(op, key, "released leaf node lock");
        }
      } finally {
        qsbrExit();
        p(op, key, "exited qsbr(2)");
      }
    }
  }

  @Override
  protected void scanInternal(
      @Nullable K startKey,
      @Nullable K endKey,
      boolean isEndKeyExclusive,
      @Nullable Integer count,
      BiFunction<K, V, Boolean> function) {
    String op = "SCAN";
    String key = startKey + ":" + endKey;
    p(op, key, "start. test name=" + testName + ", counter=" + counter);
    Object encodedStartKey =
        startKey == null ? createEmptyEncodedKey() : createEncodedKey(startKey);
    Object encodedEndKey = endKey == null ? null : createEncodedKey(endKey);
    BiFunction<K, V, Boolean> actualFunction = prepareScanFunction(count, function);

    long readLockOnMetaTable = acquireReadLockOnMetaTable();
    try {
      qsbrEnter();
      p(op, key, "entering qsbr");
      LeafNode<K, V> leafNode =
          searchTrieHashTable(qsbrThreadLocalMetaTables.get(), encodedStartKey);
      while (leafNode != null) {
        p(op, key, "got leaf node. id=" + leafNode.id);
        long lockOnLeafNode;
        boolean writeLockOnLeafNode = false;
        int loopCount = 0;
        while (true) {
          if (++loopCount % SPIN_INTERVAL == 0) {
            Thread.yield();
          }
          lockOnLeafNode = writeLockOnLeafNode ? leafNode.tryWriteLock() : leafNode.tryReadLock();
          if (lockOnLeafNode == 0) {
            p(op, key, "failed to get write/read lock on leaf node");
            continue;
          }
          if (writeLockOnLeafNode || leafNode.isKeyRefsSorted()) {
            p(op, key, "retrying with write lock");
            break;
          }
          leafNode.releaseLock(lockOnLeafNode);
          writeLockOnLeafNode = true;
        }
        try {
          // A read lock on the meta table is already acquired. So, the version of leaf node doesn't
          // need to be checked.

          leafNode.incSort();
          if (!leafNode.iterateKeyValues(
              encodedStartKey, encodedEndKey, isEndKeyExclusive, actualFunction)) {
            p(op, key, "returning");
            return;
          }
        } finally {
          leafNode.releaseLock(lockOnLeafNode);
          p(op, key, "released leaf node lock");
        }
        leafNode = leafNode.getRight();
        encodedStartKey = null;
      }
    } finally {
      qsbrExit();
      p(op, key, "exited qsbr(2)");
      releaseLockOnMetaTable(readLockOnMetaTable);
      p(op, key, "released meta table lock");
    }
  }

  @Nullable
  protected LeafNode<K, V> createLeafNode(
      EncodedKeyType encodedKeyType,
      Function<Object, Object> validAnchorKeyProvider,
      Object anchorKey,
      int maxSize,
      @Nullable LeafNode<K, V> leftNode,
      @Nullable LeafNode<K, V> rightNode) {
    return new ConcurrentLeafNode<>(
        encodedKeyType, validAnchorKeyProvider, anchorKey, maxSize, leftNode, rightNode);
  }

  @Override
  protected Object provideValidAnchorKeyForSplit(Object anchorKey) {
    MetaTrieHashTable.NodeMeta existingNodeMeta = getInactiveMetaTable().get(anchorKey);
    if (existingNodeMeta == null) {
      return anchorKey;
    }
    return null;
  }
}
