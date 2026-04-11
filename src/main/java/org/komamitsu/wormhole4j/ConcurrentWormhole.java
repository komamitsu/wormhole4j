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

import java.util.Optional;
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
  private final MetaTrieHashTable<K, V> metaTable;

  /**
   * Creates a Wormhole.
   *
   * @param encodedKeyType the encoded key type
   * @param leafNodeSize maximum number of entries in a leaf node
   */
  protected ConcurrentWormhole(EncodedKeyType encodedKeyType, int leafNodeSize) {
    super(encodedKeyType, leafNodeSize);
    this.metaTable = new MetaTrieHashTable<>(encodedKeyType, true);
    initialize();
  }

  private void initialize() {
    Object encodedKey = createEmptyEncodedKey();
    LeafNode<K, V> rootLeafNode = createRootLeafNode(encodedKey);
    getMetaTable().put(encodedKey, new NodeMetaLeaf<>(encodedKey, rootLeafNode));
  }

  protected abstract Object createEncodedKey(K key);

  protected abstract Object createEmptyEncodedKey();

  @Override
  protected MetaTrieHashTable<K, V> getMetaTable() {
    return metaTable;
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
    boolean writeLockOnTable = false;
    while (true) {
      long tableLock =
          writeLockOnTable ? metaTable.acquireWriteLock() : metaTable.acquireReadLock();
      try {
        LeafNode<K, V> leafNode = searchTrieHashTable(encodedKey);
        long writeLockOnLeafNode = leafNode.acquireWriteLock();
        try {
          Optional<V> existingValue = leafNode.lookupAndPutValue(encodedKey, key, value);
          if (existingValue != null) {
            return existingValue.orElse(null);
          }

          // Retry with a write lock when the current lock is a read lock.
          if (!writeLockOnTable) {
            writeLockOnTable = true;
            continue;
          }

          // Split the node and insert the value to a new leaf node.
          splitAndInsert(leafNode, encodedKey, key, value);
          return null;
        } finally {
          leafNode.releaseLock(writeLockOnLeafNode);
        }
      } finally {
        metaTable.releaseLock(tableLock);
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
    long tableLock = metaTable.acquireWriteLock();
    try {
      LeafNode<K, V> leafNode = searchTrieHashTable(encodedKey);
      long writeLockOnLeafNode = leafNode.acquireWriteLock();
      try {
        if (!leafNode.delete(encodedKey)) {
          return false;
        }

        mergeIfNeeded(leafNode);
      } finally {
        leafNode.releaseLock(writeLockOnLeafNode);
      }
    } finally {
      metaTable.releaseLock(tableLock);
    }
    return true;
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
    long tableLock = metaTable.acquireReadLock();
    try {
      LeafNode<K, V> leafNode = searchTrieHashTable(encodedKey);
      long readLockOnLeafNode = leafNode.acquireReadLock();
      try {
        return leafNode.lookupValue(encodedKey);
      } finally {
        leafNode.releaseLock(readLockOnLeafNode);
      }
    } finally {
      metaTable.releaseLock(tableLock);
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

    long tableLock = metaTable.acquireReadLock();
    try {
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
      metaTable.releaseLock(tableLock);
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
