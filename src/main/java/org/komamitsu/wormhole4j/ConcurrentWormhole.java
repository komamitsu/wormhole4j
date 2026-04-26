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
import java.util.concurrent.TimeUnit;
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
  // TODO: Make this configurable.
  private static final int MAX_THREADS = 512;
  // These don't need to be volatile since they are only updated in synchronized blocks.
  private int metaTableIndex;
  private long version;
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
    this.qsbrVersions = new ArrayList<>(MAX_THREADS);
    this.qsbrThreads = new BitSet(MAX_THREADS);
    initialize();
  }

  private void initialize() {
    Object encodedKey = createEmptyEncodedKey();
    LeafNode<K, V> rootLeafNode = createRootLeafNode(encodedKey);
    metaTables.get(0).put(encodedKey, new NodeMetaLeaf<>(encodedKey, rootLeafNode));
    metaTables.get(1).put(encodedKey, new NodeMetaLeaf<>(encodedKey, rootLeafNode));
  }

  @Override
  protected MetaTrieHashTable<K, V> getMetaTable() {
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
    if (availableThreadIndex >= MAX_THREADS) {
      throw new IllegalStateException("The number of registered threads exceeds " + MAX_THREADS);
    }
    AtomicReference<Long> versionContainer = new AtomicReference<>();
    qsbrThreadLocalIndexes.set(availableThreadIndex);
    qsbrThreadLocalVersions.set(versionContainer);
    qsbrThreads.set(availableThreadIndex);
    registerQsbrVersion(availableThreadIndex, versionContainer);
  }

  @Override
  public synchronized void unregisterThread() {
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
    qsbrThreadLocalMetaTables.set(getMetaTable());
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
  private synchronized void qsbrWait(long newVersion) {
    int threadIndex = 0;
    while (true) {
      threadIndex = qsbrThreads.nextSetBit(threadIndex);
      if (threadIndex < 0) {
        break;
      }
      while (true) {
        Long localVersion = qsbrVersions.get(threadIndex).get();
        if (localVersion == null || localVersion == newVersion) {
          break;
        }
        // Or, Thread.onSpinWait() with Java 9 or later.
        Thread.yield();
      }
      threadIndex++;
    }
  }

  private long tryLockOnMetaTable() {
    try {
      // TODO: Consider no wait.
      return metaTableLock.tryWriteLock(5, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
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
    Object encodedKey = createEncodedKey(key);
    while (true) {
      qsbrEnter();
      LeafNode<K, V> leafNode = searchTrieHashTable(encodedKey);
      Long writeLockOnLeafNode = leafNode.tryWriteLock();
      if (writeLockOnLeafNode == 0) {
        qsbrExit();
        continue;
      }
      try {
        if (leafNode.getVersion() > qsbrThreadLocalVersions.get().get()) {
          continue;
        }
        Optional<V> existingValue = leafNode.lookupAndPutValue(encodedKey, key, value);
        if (existingValue != null) {
          return existingValue.orElse(null);
        }
        qsbrExit();
        long writeLockOnMetaTable = tryLockOnMetaTable();
        if (writeLockOnMetaTable == 0) {
          continue;
        }
        try {
          long newVersion = version + 1;

          // This new leaf is already locked.
          LeafNode<K, V> newLeafNode =
              splitLeafNode(getInactiveMetaTable(), leafNode, encodedKey, key, value);
          addNewLeafNodeToMetaTable(getInactiveMetaTable(), newLeafNode);

          // Increment versions and switch to the updated meta table.
          leafNode.setVersion(newVersion);
          newLeafNode.setVersion(newVersion);
          switchMetaTable(newVersion);
          newLeafNode.releaseLock(newLeafNode.getInitialLockStamp());
          leafNode.releaseLock(writeLockOnLeafNode);
          writeLockOnLeafNode = null;

          // Wait until no reader threads on the previously active meta table.
          qsbrWait(newVersion);

          addNewLeafNodeToMetaTable(getInactiveMetaTable(), newLeafNode);
          return null;
        } finally {
          releaseLockOnMetaTable(writeLockOnMetaTable);
        }
      } finally {
        qsbrExit();
        if (writeLockOnLeafNode != null) {
          leafNode.releaseLock(writeLockOnLeafNode);
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
    Object encodedKey = createEncodedKey(key);
    while (true) {
      qsbrEnter();
      LeafNode<K, V> leafNode = searchTrieHashTable(encodedKey);
      Long writeLockOnLeafNode = leafNode.tryWriteLock();
      if (writeLockOnLeafNode == 0) {
        qsbrExit();
        continue;
      }
      try {
        if (leafNode.getVersion() > qsbrThreadLocalVersions.get().get()) {
          continue;
        }
        if (leafNode.lookupValue(encodedKey) == null) {
          return false;
        }
        long writeLockOnMetaTable = tryLockOnMetaTable();
        if (writeLockOnMetaTable == 0) {
          continue;
        }
        assert leafNode.delete(encodedKey);
        try {
          // Merge the nodes on the inactive meta table if needed.
          Tuple<LeafNode<K, V>, LeafNode<K, V>> mergedLeafNodes = mergeLeafNodesIfNeeded(leafNode);
          LeafNode<K, V> updatedLeafNode = null;
          LeafNode<K, V> mergedLeafNode = null;
          if (mergedLeafNodes != null) {
            updatedLeafNode = mergedLeafNodes.first;
            mergedLeafNode = mergedLeafNodes.second;
            removeMergedLeafNodeFromMetaTable(getInactiveMetaTable(), mergedLeafNode);
          }
          // Increment versions and switch to the updated meta table.
          long newVersion = version + 1;
          leafNode.setVersion(newVersion);
          if (updatedLeafNode != null && !updatedLeafNode.equals(leafNode)) {
            updatedLeafNode.setVersion(newVersion);
          }
          switchMetaTable(newVersion);
          leafNode.releaseLock(writeLockOnLeafNode);
          writeLockOnLeafNode = null;
          // Wait until no reader threads on the previously active meta table.
          qsbrExit();
          qsbrWait(newVersion);
          // Remove the merged leaf node from the inactive meta table.
          if (mergedLeafNode != null) {
            removeMergedLeafNodeFromMetaTable(getInactiveMetaTable(), mergedLeafNode);
          }
          return true;
        } finally {
          releaseLockOnMetaTable(writeLockOnMetaTable);
        }
      } finally {
        qsbrExit();
        if (writeLockOnLeafNode != null) {
          leafNode.releaseLock(writeLockOnLeafNode);
        }
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
    Object encodedKey = createEncodedKey(key);
    while (true) {
      qsbrEnter();
      try {
        LeafNode<K, V> leafNode = searchTrieHashTable(encodedKey);
        long readLockOnLeafNode = leafNode.tryReadLock();
        if (readLockOnLeafNode == 0) {
          continue;
        }
        try {
          V result = leafNode.lookupValue(encodedKey);
          if (leafNode.getVersion() <= getLocalVersion()) {
            return result;
          }
        } finally {
          leafNode.releaseLock(readLockOnLeafNode);
        }
      } finally {
        qsbrExit();
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
    Object encodedStartKey =
        startKey == null ? createEmptyEncodedKey() : createEncodedKey(startKey);
    Object encodedEndKey = endKey == null ? null : createEncodedKey(endKey);
    BiFunction<K, V, Boolean> actualFunction = prepareScanFunction(count, function);

    long readLockOnMetaTable = acquireReadLockOnMetaTable();
    try {
      qsbrEnter();
      LeafNode<K, V> leafNode = searchTrieHashTable(encodedStartKey);
      while (leafNode != null) {
        long lockOnLeafNode;
        boolean writeLockOnLeafNode = false;
        while (true) {
          lockOnLeafNode = writeLockOnLeafNode ? leafNode.tryWriteLock() : leafNode.tryReadLock();
          if (lockOnLeafNode == 0) {
            continue;
          }
          if (writeLockOnLeafNode || leafNode.isKeyRefsSorted()) {
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
            return;
          }
        } finally {
          leafNode.releaseLock(lockOnLeafNode);
        }
        leafNode = leafNode.getRight();
        encodedStartKey = null;
      }
    } finally {
      qsbrExit();
      releaseLockOnMetaTable(readLockOnMetaTable);
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
