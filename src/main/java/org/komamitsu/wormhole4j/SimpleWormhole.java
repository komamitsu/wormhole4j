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
abstract class SimpleWormhole<K, V> extends Wormhole<K, V> {
  private final MetaTrieHashTable<K, V> metaTable;
  @Nullable private final WormholeValidator<K, V> validator;

  /**
   * Creates a Wormhole.
   *
   * @param encodedKeyType the encoded key type
   * @param leafNodeSize maximum number of entries in a leaf node
   * @param debugMode enables internal consistency checks if {@code true}
   */
  protected SimpleWormhole(EncodedKeyType encodedKeyType, int leafNodeSize, boolean debugMode) {
    super(encodedKeyType, leafNodeSize);
    this.metaTable = new MetaTrieHashTable<>(encodedKeyType);
    validator = debugMode ? new WormholeValidator<>(this) : null;
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
  public void register() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unregister() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected MetaTrieHashTable<K, V> getMetaTable() {
    return metaTable;
  }

  private void validateIfNeeded() {
    if (validator == null) {
      return;
    }
    validator.validate();
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
    LeafNode<K, V> leafNode = searchTrieHashTable(encodedKey);
    Optional<V> existingValue = leafNode.lookupAndPutValue(encodedKey, key, value);
    if (existingValue != null) {
      validateIfNeeded();
      return existingValue.orElse(null);
    }

    LeafNode<K, V> newLeafNode = splitLeafNode(metaTable, leafNode, encodedKey, key, value);
    addNewLeafNodeToMetaTable(metaTable, newLeafNode);
    validateIfNeeded();
    return null;
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
    LeafNode<K, V> leafNode = searchTrieHashTable(encodedKey);
    if (!leafNode.delete(encodedKey)) {
      validateIfNeeded();
      return false;
    }
    Tuple<LeafNode<K, V>, LeafNode<K, V>> mergedLeafNodes = mergeLeafNodesIfNeeded(leafNode);
    if (mergedLeafNodes != null) {
      removeMergedLeafNodeFromMetaTable(metaTable, mergedLeafNodes.second);
    }
    validateIfNeeded();
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
    LeafNode<K, V> leafNode = searchTrieHashTable(encodedKey);
    return leafNode.lookupValue(encodedKey);
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
    LeafNode<K, V> leafNode = searchTrieHashTable(encodedStartKey);
    while (leafNode != null) {
      leafNode.incSort();
      if (!leafNode.iterateKeyValues(
          encodedStartKey, encodedEndKey, isEndKeyExclusive, actualFunction)) {
        validateIfNeeded();
        return;
      }
      leafNode = leafNode.getRight();
      encodedStartKey = null;
    }
    validateIfNeeded();
  }

  @Nullable
  protected LeafNode<K, V> createLeafNode(
      EncodedKeyType encodedKeyType,
      Function<Object, Object> validAnchorKeyProvider,
      Object anchorKey,
      int maxSize,
      @Nullable LeafNode<K, V> leftNode,
      @Nullable LeafNode<K, V> rightNode) {
    return new LeafNode<>(
        encodedKeyType, validAnchorKeyProvider, anchorKey, maxSize, leftNode, rightNode);
  }
}
