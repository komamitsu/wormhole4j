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

  protected abstract Object createEncodedKey(K key);

  protected abstract Object createEmptyEncodedKey();

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
  public synchronized void register() {
    int availableThreadIndex = qsbrThreads.nextClearBit(0);
    AtomicReference<Long> versionContainer = new AtomicReference<>();
    qsbrThreadLocalIndexes.set(availableThreadIndex);
    qsbrThreadLocalVersions.set(versionContainer);
    qsbrThreads.set(availableThreadIndex);
    registerQsbrVersion(availableThreadIndex, versionContainer);
  }

  @Override
  public synchronized void unregister() {
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
    qsbrThreadLocalVersions.get().set(version);
    qsbrThreadLocalMetaTables.set(getMetaTable());
  }

  private void qsbrExit() {
    qsbrThreadLocalMetaTables.remove();
    qsbrThreadLocalVersions.get().set(null);
  }

  private long getLocalVersion() {
    return qsbrThreadLocalVersions.get().get();
  }

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
      }
      threadIndex++;
    }
  }

  private long acquireLockOnMetaTable() {
    return metaTableLock.writeLock();
  }

  private void releaseLockOnMetaTable(long stamp) {
    metaTableLock.unlockWrite(stamp);
  }

  private int getInactiveMetaTableIndex() {
    return metaTableIndex == 0 ? 1 : 0;
  }

  private void switchMetaTable(long newVersion) {
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
    qsbrEnter();
    LeafNode<K, V> leafNode = searchTrieHashTable(encodedKey);
    Long writeLockOnLeafNode = leafNode.acquireWriteLock();
    try {
      Optional<V> existingValue = leafNode.lookupAndPutValue(encodedKey, key, value);
      if (existingValue != null) {
        return existingValue.orElse(null);
      }
      long writeLockOnMetaTable = acquireLockOnMetaTable();
      try {
        long newVersion = version + 1;

        LeafNode<K, V> newLeafNode =
            splitLeafNode(getInactiveMetaTable(), leafNode, encodedKey, key, value);
        addNewLeafNodeToMetaTable(getInactiveMetaTable(), newLeafNode);

        // Increment versions and switch to the updated meta table.
        leafNode.setVersion(newVersion);
        newLeafNode.setVersion(newVersion);
        switchMetaTable(newVersion);
        leafNode.releaseLock(writeLockOnLeafNode);
        writeLockOnLeafNode = null;

        // Wait until no reader threads on the previously active meta table.
        qsbrExit();
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

  /**
   * Deletes a key-value pair if present.
   *
   * @param key the key (must not be {@code null})
   * @return {@code true} if the key was removed, {@code false} otherwise
   */
  @Override
  public boolean delete(K key) {
    Object encodedKey = createEncodedKey(key);
    qsbrEnter();
    LeafNode<K, V> leafNode = searchTrieHashTable(encodedKey);
    Long writeLockOnLeafNode = leafNode.acquireWriteLock();
    try {
      if (!leafNode.delete(encodedKey)) {
        return false;
      }
      long writeLockOnMetaTable = acquireLockOnMetaTable();
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
        long readLockOnLeafNode = leafNode.acquireReadLock();
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

    try {
      qsbrEnter();
      LeafNode<K, V> leafNode = searchTrieHashTable(encodedStartKey);
      while (leafNode != null) {
        long lockOnLeafNode;
        boolean writeLockOnLeafNode = false;
        while (true) {
          lockOnLeafNode =
              writeLockOnLeafNode ? leafNode.acquireWriteLock() : leafNode.acquireReadLock();
          if (writeLockOnLeafNode || leafNode.isKeyRefsSorted()) {
            break;
          }
          leafNode.releaseLock(lockOnLeafNode);
          writeLockOnLeafNode = true;
        }
        try {
          if (leafNode.getVersion() > getLocalVersion()) {
            // FIXME: This break leads to a retry from the beginning, but `actualFunction` can be
            //        executed multiple times on the same leaf nodes...
            break;
          }
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
}
