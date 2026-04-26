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
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.komamitsu.wormhole4j.MetaTrieHashTable.NodeMetaInternal;
import org.komamitsu.wormhole4j.MetaTrieHashTable.NodeMetaLeaf;

/**
 * Wormhole is an in-memory ordered index for key-value pairs.
 *
 * <p>This implementation supports fast lookups, inserts, deletes, and range scans.
 *
 * @param <K> the type of keys stored in this index
 * @param <V> the type of values stored in this index
 */
public abstract class Wormhole<K, V> {
  static final int DEFAULT_LEAF_NODE_SIZE = 128;
  final EncodedKeyType encodedKeyType;
  private final int leafNodeSize;
  private final int leafNodeMergeSize;
  private final Function<Object, Object> validAnchorKeyProvider;

  abstract MetaTrieHashTable<K, V> getActiveMetaTable();

  /**
   * Creates a Wormhole.
   *
   * @param encodedKeyType the encoded key type
   * @param leafNodeSize maximum number of entries in a leaf node
   */
  Wormhole(EncodedKeyType encodedKeyType, int leafNodeSize) {
    this.encodedKeyType = encodedKeyType;
    this.leafNodeSize = leafNodeSize;
    this.leafNodeMergeSize = leafNodeSize * 3 / 4;
    validAnchorKeyProvider = this::provideValidAnchorKeyForSplit;
  }

  LeafNode<K, V> createRootLeafNode(Object encodedKey) {
    return createLeafNode(
        encodedKeyType, validAnchorKeyProvider, encodedKey, leafNodeSize, null, null);
  }

  protected abstract Object createEncodedKey(K key);

  protected abstract Object createEmptyEncodedKey();

  public abstract void registerThread();

  public abstract void unregisterThread();

  /**
   * Inserts or updates a key-value pair.
   *
   * @param key the key (must not be {@code null})
   * @param value the value to associate with the key
   * @return the previous value associated with the key, or {@code null} if there was no previous
   *     mapping
   */
  @Nullable
  public abstract V put(K key, V value);

  /**
   * Deletes a key-value pair if present.
   *
   * @param key the key (must not be {@code null})
   * @return {@code true} if the key was removed, {@code false} otherwise
   */
  public abstract boolean delete(K key);

  /**
   * Retrieves the value associated with the specified key.
   *
   * @param key the key (must not be {@code null})
   * @return the value, or {@code null} if not found
   */
  @Nullable
  public abstract V get(K key);

  /**
   * Scans the key range.
   *
   * @param startKey the start key (inclusive)
   * @param endKey the end key
   * @param isEndKeyExclusive whether the end key is exclusive
   * @param function a function applied to each key-value pair; if it returns {@code true}, the scan
   *     continues, otherwise the scan stops
   */
  public void scan(
      @Nullable K startKey,
      @Nullable K endKey,
      boolean isEndKeyExclusive,
      BiFunction<K, V, Boolean> function) {
    scanInternal(startKey, endKey, isEndKeyExclusive, null, function);
  }

  /**
   * Scans the index starting from a key and collects up to {@code count} pairs.
   *
   * @param startKey the start key (inclusive), or {@code null} to start from the beginning
   * @param count maximum number of results to return
   * @return a list of key-value pairs
   */
  public List<KeyValue<K, V>> scanWithCount(@Nullable K startKey, int count) {
    List<KeyValue<K, V>> result = new ArrayList<>(count);

    scanInternal(
        startKey,
        null, /* Not used */
        false,
        count,
        (k, v) -> {
          result.add(new KeyValue<>(k, v));
          return true;
        });
    return result;
  }

  protected BiFunction<K, V, Boolean> prepareScanFunction(
      @Nullable Integer count, BiFunction<K, V, Boolean> origFunction) {
    if (count == null) {
      return origFunction;
    }

    AtomicInteger counter = new AtomicInteger();
    return (k, v) -> {
      if (counter.getAndIncrement() >= count) {
        return false;
      }
      return origFunction.apply(k, v);
    };
  }

  protected abstract void scanInternal(
      @Nullable K startKey,
      @Nullable K endKey,
      boolean isEndKeyExclusive,
      @Nullable Integer count,
      BiFunction<K, V, Boolean> function);

  abstract LeafNode<K, V> createLeafNode(
      EncodedKeyType encodedKeyType,
      Function<Object, Object> validAnchorKeyProvider,
      Object anchorKey,
      int maxSize,
      @Nullable LeafNode<K, V> leftNode,
      @Nullable LeafNode<K, V> rightNode);

  LeafNode<K, V> searchTrieHashTable(MetaTrieHashTable<K, V> metaTable, Object encodedKey) {
    MetaTrieHashTable.NodeMeta nodeMeta =
        metaTable.searchLongestPrefixMatch(encodedKeyType, encodedKey);
    if (nodeMeta instanceof MetaTrieHashTable.NodeMetaLeaf) {
      return ((NodeMetaLeaf<K, V>) nodeMeta).leafNode;
    }

    NodeMetaInternal<K, V> nodeMetaInternal = (NodeMetaInternal<K, V>) nodeMeta;
    int anchorPrefixLength = EncodedKeyUtils.length(encodedKeyType, nodeMetaInternal.anchorPrefix);

    // The leaf type is INTERNAL.
    int encodedKeyLength = EncodedKeyUtils.length(encodedKeyType, encodedKey);
    if (anchorPrefixLength == encodedKeyLength) {
      LeafNode<K, V> leafNode = nodeMetaInternal.getLeftMostLeafNode();
      if (EncodedKeyUtils.compare(encodedKeyType, encodedKey, leafNode.anchorKey) < 0) {
        // For example, if the paper's example had key "J" in the second leaf node and the search
        // key is "J", this special treatment would be necessary.
        return leafNode.getLeft();
      } else {
        return leafNode;
      }
    }

    if (anchorPrefixLength > encodedKeyLength) {
      throw new AssertionError(
          "The length of the anchor prefix is longer than the length of the key");
    }

    int missingToken = EncodedKeyUtils.get(encodedKeyType, encodedKey, anchorPrefixLength);
    Integer siblingToken = nodeMetaInternal.findOneSibling(missingToken);
    if (siblingToken == null) {
      return nodeMetaInternal.getLeftMostLeafNode();
    }

    MetaTrieHashTable.NodeMeta childNode =
        metaTable.get(
            EncodedKeyUtils.append(encodedKeyType, nodeMetaInternal.anchorPrefix, siblingToken));
    if (childNode == null) {
      throw new AssertionError("Child node is not found");
    }

    if (childNode instanceof MetaTrieHashTable.NodeMetaLeaf) {
      LeafNode<K, V> leafNode = ((NodeMetaLeaf<K, V>) childNode).leafNode;
      if (missingToken < siblingToken) {
        return leafNode.getLeft();
      } else {
        return leafNode;
      }
    } else {
      NodeMetaInternal<K, V> childNodeInternal = (NodeMetaInternal<K, V>) childNode;
      if (missingToken < siblingToken) {
        // The child node is a subtree right to the target node.
        return childNodeInternal.getLeftMostLeafNode().getLeft();
      } else {
        // The child node is a subtree left to the target node.
        return childNodeInternal.getRightMostLeafNode();
      }
    }
  }

  @Nullable
  abstract Object provideValidAnchorKeyForSplit(Object anchorKey);

  LeafNode<K, V> splitLeafNode(LeafNode<K, V> leafNode, Object encodedKey, K key, V value) {
    LeafNode<K, V> newLeafNode = leafNode.splitToNewLeafNode();
    if (EncodedKeyUtils.compare(encodedKeyType, encodedKey, newLeafNode.anchorKey) < 0) {
      leafNode.add(encodedKey, key, value);
    } else {
      newLeafNode.add(encodedKey, key, value);
    }
    return newLeafNode;
  }

  void addNewLeafNodeToMetaTable(MetaTrieHashTable<K, V> metaTable, LeafNode<K, V> newLeafNode) {
    metaTable.handleSplitNodes(newLeafNode.anchorKey, newLeafNode);
  }

  @Nullable
  Tuple<LeafNode<K, V>, LeafNode<K, V>> mergeLeafNodesIfNeeded(LeafNode<K, V> leafNode) {
    if (leafNode.getLeft() != null
        && leafNode.size() + leafNode.getLeft().size() < leafNodeMergeSize) {
      LeafNode<K, V> leftLeafNode = leafNode.getLeft();
      leftLeafNode.merge(leafNode);
      return new Tuple<>(leftLeafNode, leafNode);
    } else if (leafNode.getRight() != null
        && leafNode.size() + leafNode.getRight().size() < leafNodeMergeSize) {
      LeafNode<K, V> rightLeafNode = leafNode.getRight();
      leafNode.merge(rightLeafNode);
      return new Tuple<>(leafNode, rightLeafNode);
    }
    return null;
  }

  void removeMergedLeafNodeFromMetaTable(MetaTrieHashTable<K, V> metaTable, LeafNode<K, V> victim) {
    boolean childNodeRemoved = false;
    for (int prefixlen = EncodedKeyUtils.length(encodedKeyType, victim.anchorKey);
        prefixlen >= 0;
        prefixlen--) {
      Object prefix = EncodedKeyUtils.slice(encodedKeyType, victim.anchorKey, prefixlen);
      MetaTrieHashTable.NodeMeta nodeMeta = metaTable.get(prefix);
      NodeMetaInternal<K, V> nodeMetaInternal = null;
      NodeMetaLeaf<K, V> nodeMetaLeaf = null;
      if (nodeMeta instanceof NodeMetaInternal) {
        nodeMetaInternal = (NodeMetaInternal<K, V>) nodeMeta;
      } else {
        assert nodeMeta instanceof MetaTrieHashTable.NodeMetaLeaf;
        nodeMetaLeaf = (NodeMetaLeaf<K, V>) nodeMeta;
      }

      // The pseudocode in the paper always clears the bitmap index for the child token.
      // However, it should probably be cleared only when the child node has been removed.
      if (childNodeRemoved) {
        if (nodeMetaInternal != null) {
          nodeMetaInternal.bitmap.clear(
              EncodedKeyUtils.get(encodedKeyType, victim.anchorKey, prefixlen));
        }
      }
      // The root node meta must be left.
      // Remove internal node meta if it points a single leaf node.
      // This condition is added following the reference implementation.
      if (EncodedKeyUtils.length(encodedKeyType, nodeMeta.anchorPrefix) > 0
          && (nodeMetaLeaf != null
              || nodeMetaInternal
                  .getLeftMostLeafNode()
                  .equals(nodeMetaInternal.getRightMostLeafNode()))) {
        metaTable.removeNodeMeta(prefix);
        childNodeRemoved = true;
      } else {
        childNodeRemoved = false;
        if (nodeMetaInternal != null) {
          if (nodeMetaInternal.getLeftMostLeafNode() == victim) {
            nodeMetaInternal.setLeftMostLeafNode(victim.getRight());
          }
          if (nodeMetaInternal.getRightMostLeafNode() == victim) {
            nodeMetaInternal.setRightMostLeafNode(victim.getLeft());
          }
        }
      }
    }
  }

  IntWrapper createEncodedIntKey(Integer key) {
    assert key != null;
    return new IntWrapper(key ^ 0x80000000);
  }

  LongWrapper createEncodedLongKey(Long key) {
    assert key != null;
    return new LongWrapper(key ^ 0x8000000000000000L);
  }
}
