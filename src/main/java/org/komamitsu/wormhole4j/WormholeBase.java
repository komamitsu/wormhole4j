/*
 * Copyright 2025 Mitsunori Komatsu
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
import java.util.function.Function;
import javax.annotation.Nullable;
import org.komamitsu.wormhole4j.MetaTrieHashTable.NodeMetaInternal;
import org.komamitsu.wormhole4j.MetaTrieHashTable.NodeMetaLeaf;

/**
 * Wormhole is an in-memory ordered index for key-value pairs.
 *
 * <p>This implementation supports fast lookups, inserts, deletes, and range scans. Keys are {@link
 * String} only.
 *
 * @param <K> the type of keys stored in this index
 * @param <V> the type of values stored in this index
 */
abstract class WormholeBase<K, E extends EncodedKey<E>, V> {
  private static final int DEFAULT_LEAF_NODE_SIZE = 128;
  private final MetaTrieHashTable<K, E, V> table = new MetaTrieHashTable<>();
  private final int leafNodeSize;
  private final int leafNodeMergeSize;
  @Nullable private final Validator<K, E, V> validator;
  private final Function<E, E> validAnchorKeyProvider;

  /** Creates a Wormhole with the default leaf node size. */
  public WormholeBase() {
    this(DEFAULT_LEAF_NODE_SIZE);
  }

  /**
   * Creates a Wormhole with the specified leaf node size.
   *
   * @param leafNodeSize maximum number of entries in a leaf node
   */
  public WormholeBase(int leafNodeSize) {
    this(leafNodeSize, false);
  }

  /**
   * Creates a Wormhole with the specified leaf node size and optional debug mode.
   *
   * @param leafNodeSize maximum number of entries in a leaf node
   * @param debugMode enables internal consistency checks if {@code true}
   */
  public WormholeBase(int leafNodeSize, boolean debugMode) {
    this.leafNodeSize = leafNodeSize;
    this.leafNodeMergeSize = leafNodeSize * 3 / 4;
    validAnchorKeyProvider = this::provideValidAnchorKey;
    initialize();
    validator = debugMode ? new Validator<>(this) : null;
  }

  private void validateIfNeeded() {
    if (validator == null) {
      return;
    }
    validator.validate();
  }

  abstract E encodeKey(K key);

  abstract E emptyEncodedKey();

  /**
   * Inserts or updates a key-value pair.
   *
   * @param key the key (must not be {@code null})
   * @param value the value to associate with the key
   */
  public void put(K key, V value) {
    E encodedKey = encodeKey(key);
    LeafNode<K, E, V> leafNode = searchTrieHashTable(encodedKey);
    KeyValue<K, E, V> existingKeyValue = leafNode.pointSearchLeaf(encodedKey);
    if (existingKeyValue != null) {
      existingKeyValue.setValue(value);
      validateIfNeeded();
      return;
    }

    Key<K, E> k = new Key<>(key, encodedKey);

    if (leafNode.size() == leafNodeSize) {
      // Split the node and get a new right leaf node.
      LeafNode<K, E, V> newLeafNode = split(leafNode);
      if (encodedKey.compareTo(newLeafNode.anchorKey) < 0) {
        leafNode.add(k, value);
      } else {
        newLeafNode.add(k, value);
      }
    } else {
      assert leafNode.size() < leafNodeSize;
      leafNode.add(k, value);
    }
    validateIfNeeded();
  }

  /**
   * Deletes a key-value pair if present.
   *
   * @param key the key to remove
   * @return {@code true} if the key was removed, {@code false} otherwise
   */
  public boolean delete(K key) {
    E encodedKey = encodeKey(key);
    LeafNode<K, E, V> leafNode = searchTrieHashTable(encodedKey);
    if (!leafNode.delete(encodedKey)) {
      return false;
    }

    if (leafNode.getLeft() != null
        && leafNode.size() + leafNode.getLeft().size() < leafNodeMergeSize) {
      merge(leafNode.getLeft(), leafNode);
    } else if (leafNode.getRight() != null
        && leafNode.size() + leafNode.getRight().size() < leafNodeMergeSize) {
      merge(leafNode, leafNode.getRight());
    }

    validateIfNeeded();
    return true;
  }

  /**
   * Retrieves the value associated with the specified key.
   *
   * @param key the key to search for
   * @return the value, or {@code null} if not found
   */
  @Nullable
  public V get(K key) {
    E encodedKey = encodeKey(key);
    LeafNode<K, E, V> leafNode = searchTrieHashTable(encodedKey);
    KeyValue<K, E, V> keyValue = leafNode.pointSearchLeaf(encodedKey);
    if (keyValue == null) {
      return null;
    }
    return keyValue.getValue();
  }

  private void scanInternal(
      E encodedStartKey,
      @Nullable E encodedEndKey,
      boolean isEndKeyExclusive,
      @Nullable Integer count,
      Function<KeyValue<K, E, V>, Boolean> function) {
    Function<KeyValue<K, E, V>, Boolean> actualFunction = function;
    if (count != null) {
      AtomicInteger counter = new AtomicInteger();
      actualFunction =
          kv -> {
            if (counter.getAndIncrement() >= count) {
              return false;
            }
            return function.apply(kv);
          };
    }

    LeafNode<K, E, V> leafNode = searchTrieHashTable(encodedStartKey);
    while (leafNode != null) {
      leafNode.incSort();
      if (!leafNode.iterateKeyValues(
          encodedStartKey, encodedEndKey, isEndKeyExclusive, actualFunction)) {
        return;
      }
      leafNode = leafNode.getRight();
      encodedStartKey = null;
    }
  }

  /**
   * Scans the index starting from a key and collects up to {@code count} pairs.
   *
   * @param startKey the starting key (inclusive)
   * @param count maximum number of results to return
   * @return a list of key-value pairs
   */
  public List<KeyValue<K, E, V>> scanWithCount(K startKey, int count) {
    List<KeyValue<K, E, V>> result = new ArrayList<>(count);
    scanInternal(
        encodeKey(startKey),
        null, /* Not used */
        false,
        count,
        kv -> {
          result.add(kv);
          return true;
        });
    return result;
  }

  /**
   * Scans a range of keys and applies a function to each result.
   *
   * @param startKey the start key (inclusive) or {@code null} for the first key
   * @param endKey the end key (exclusive or inclusive based on {@code isEndKeyExclusive})
   * @param isEndKeyExclusive whether {@code endKey} is exclusive
   * @param function a function applied to each key-value pair; return {@code false} to stop
   *     scanning
   */
  public void scan(
      @Nullable K startKey,
      @Nullable K endKey,
      boolean isEndKeyExclusive,
      Function<KeyValue<K, E, V>, Boolean> function) {
    scanInternal(
        startKey != null ? encodeKey(startKey) : emptyEncodedKey(),
        endKey != null ? encodeKey(endKey) : null,
        isEndKeyExclusive,
        null,
        function);
  }

  /**
   * Scans a range of keys where the end key is exclusive.
   *
   * @param startKey the start key (inclusive) or {@code null} for the first key
   * @param endKey the end key (exclusive)
   * @param function a function applied to each key-value pair; return {@code false} to stop
   *     scanning
   */
  public void scanWithExclusiveEndKey(
      @Nullable K startKey, @Nullable K endKey, Function<KeyValue<K, E, V>, Boolean> function) {
    scanInternal(
        startKey != null ? encodeKey(startKey) : emptyEncodedKey(),
        endKey != null ? encodeKey(endKey) : null,
        true,
        null,
        function);
  }

  /**
   * Scans a range of keys where the end key is inclusive.
   *
   * @param startKey the start key (inclusive) or {@code null} for the first key
   * @param endKey the end key (inclusive)
   * @param function a function applied to each key-value pair; return {@code false} to stop
   *     scanning
   */
  public void scanWithInclusiveEndKey(
      @Nullable K startKey, @Nullable K endKey, Function<KeyValue<K, E, V>, Boolean> function) {
    scanInternal(
        startKey != null ? encodeKey(startKey) : emptyEncodedKey(),
        endKey != null ? encodeKey(endKey) : null,
        false,
        null,
        function);
  }

  private void initialize() {
    E key = emptyEncodedKey();
    LeafNode<K, E, V> rootLeafNode =
        new LeafNode<>(validAnchorKeyProvider, key, leafNodeSize, null, null);
    // Add the root.
    table.put(key, new MetaTrieHashTable.NodeMetaLeaf<>(key, rootLeafNode));
  }

  private LeafNode<K, E, V> searchTrieHashTable(E encodedKey) {
    MetaTrieHashTable.NodeMeta<E> nodeMeta = table.searchLongestPrefixMatch(encodedKey);
    if (nodeMeta instanceof MetaTrieHashTable.NodeMetaLeaf) {
      return ((MetaTrieHashTable.NodeMetaLeaf<K, E, V>) nodeMeta).leafNode;
    }

    NodeMetaInternal<K, E, V> nodeMetaInternal = (NodeMetaInternal<K, E, V>) nodeMeta;
    int anchorPrefixLength = nodeMetaInternal.anchorPrefix.length();

    // The leaf type is INTERNAL.
    if (anchorPrefixLength == encodedKey.length()) {
      LeafNode<K, E, V> leafNode = nodeMetaInternal.getLeftMostLeafNode();
      if (encodedKey.compareTo(leafNode.anchorKey) < 0) {
        // For example, if the paper's example had key "J" in the second leaf node and the search
        // key is "J", this special treatment would be necessary.
        return leafNode.getLeft();
      } else {
        return leafNode;
      }
    }

    if (anchorPrefixLength > encodedKey.length()) {
      throw new AssertionError(
          "The length of the anchor prefix is longer than the length of the key");
    }

    int missingToken = encodedKey.get(anchorPrefixLength);
    Integer siblingToken = nodeMetaInternal.findOneSibling(missingToken);
    if (siblingToken == null) {
      return nodeMetaInternal.getLeftMostLeafNode();
    }

    MetaTrieHashTable.NodeMeta<E> childNode =
        table.get(nodeMetaInternal.anchorPrefix.append(siblingToken));
    if (childNode == null) {
      throw new AssertionError("Child node is not found");
    }

    if (childNode instanceof MetaTrieHashTable.NodeMetaLeaf) {
      LeafNode<K, E, V> leafNode = ((MetaTrieHashTable.NodeMetaLeaf<K, E, V>) childNode).leafNode;
      if (missingToken < siblingToken) {
        return leafNode.getLeft();
      } else {
        return leafNode;
      }
    } else {
      NodeMetaInternal<K, E, V> childNodeInternal = (NodeMetaInternal<K, E, V>) childNode;
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
  private E provideValidAnchorKey(E anchorKey) {
    MetaTrieHashTable.NodeMeta<E> existingNodeMeta = table.get(anchorKey);
    if (existingNodeMeta == null) {
      return anchorKey;
    }
    return null;
  }

  private LeafNode<K, E, V> split(LeafNode<K, E, V> leafNode) {
    Tuple<E, LeafNode<K, E, V>> newLeafNodeAndAnchor = leafNode.splitToNewLeafNode();
    E newAnchor = newLeafNodeAndAnchor.first;
    LeafNode<K, E, V> newLeafNode = newLeafNodeAndAnchor.second;
    table.handleSplitNodes(newAnchor, newLeafNode);
    return newLeafNode;
  }

  private void merge(LeafNode<K, E, V> left, LeafNode<K, E, V> victim) {
    left.merge(victim);
    boolean childNodeRemoved = false;
    for (int prefixlen = victim.anchorKey.length(); prefixlen >= 0; prefixlen--) {
      E prefix = victim.anchorKey.slice(0, prefixlen);
      MetaTrieHashTable.NodeMeta<E> nodeMeta = table.get(prefix);
      NodeMetaInternal<K, E, V> nodeMetaInternal = null;
      MetaTrieHashTable.NodeMetaLeaf<K, E, V> nodeMetaLeaf = null;
      if (nodeMeta instanceof NodeMetaInternal) {
        nodeMetaInternal = (NodeMetaInternal<K, E, V>) nodeMeta;
      } else {
        assert nodeMeta instanceof MetaTrieHashTable.NodeMetaLeaf;
        nodeMetaLeaf = (NodeMetaLeaf<K, E, V>) nodeMeta;
      }

      // The pseudocode in the paper always clears the bitmap index for the child token.
      // However, it should probably be cleared only when the child node has been removed.
      if (childNodeRemoved) {
        if (nodeMetaInternal != null) {
          nodeMetaInternal.bitmap.clear(victim.anchorKey.get(prefixlen));
        }
      }
      // The root node meta must be left.
      // Remove internal node meta if it points a single leaf node.
      // This condition is added following the reference implementation.
      if (!nodeMeta.anchorPrefix.isEmpty()
          && (nodeMetaLeaf != null
              || nodeMetaInternal
                  .getLeftMostLeafNode()
                  .equals(nodeMetaInternal.getRightMostLeafNode()))) {
        table.removeNodeMeta(prefix);
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

  @Override
  public String toString() {
    return "Wormhole{" + "table=" + table + ", leafNodeSize=" + leafNodeSize + '}';
  }

  static class Validator<K, E extends EncodedKey<E>, V> {
    private final WormholeBase<K, E, V> wormhole;

    Validator(WormholeBase<K, E, V> wormhole) {
      this.wormhole = wormhole;
    }

    void validate() {
      try {
        validateInternal();
      } catch (AssertionError e) {
        System.err.println(wormhole);
        throw e;
      }
    }

    private void validateLeafNodes(
        LeafNode<K, E, V> leftMostLeafNode, LeafNode<K, E, V> rightMostLeafNode) {
      LeafNode<K, E, V> leafNode = leftMostLeafNode;
      LeafNode<K, E, V> lastLeafNode = null;
      while (leafNode != null) {
        leafNode.validate();
        if (leafNode != leftMostLeafNode) {
          if (leafNode.getLeft().getRight() != leafNode) {
            throw new AssertionError(
                String.format(
                    "The left node of the leaf node doesn't point the leaf node. Leaf node: %s; Left leaf node: %s",
                    leafNode, leafNode.getLeft()));
          }
        }
        if (leafNode != rightMostLeafNode) {
          if (leafNode.getRight().getLeft() != leafNode) {
            throw new AssertionError(
                String.format(
                    "The right node of the leaf node doesn't point the leaf node. Leaf node: %s; Right leaf node: %s",
                    leafNode, leafNode.getRight()));
          }
        }
        lastLeafNode = leafNode;
        leafNode = leafNode.getRight();
      }

      if (lastLeafNode != rightMostLeafNode) {
        throw new AssertionError(
            String.format(
                "The last leaf node isn't the right most leaf node. Last leaf node: %s; Right most leaf node: %s",
                lastLeafNode, rightMostLeafNode));
      }
    }

    private void validateHashTable() {
      for (Map.Entry<E, MetaTrieHashTable.NodeMeta<E>> entry : wormhole.table.entrySet()) {
        E key = entry.getKey();
        MetaTrieHashTable.NodeMeta<E> nodeMeta = entry.getValue();
        if (!nodeMeta.anchorPrefix.equals(key)) {
          throw new AssertionError(
              String.format(
                  "The node metadata anchor key is different from the key of the hash table. Key: %s, Node metadata anchor key: %s",
                  key, nodeMeta.anchorPrefix));
        }
      }

      Collection<MetaTrieHashTable.NodeMeta<E>> nodeMetas = new HashSet<>(wormhole.table.values());
      LinkedList<E> anchorKeyQueue = new LinkedList<>();
      anchorKeyQueue.addLast(wormhole.emptyEncodedKey());
      while (!anchorKeyQueue.isEmpty()) {
        E anchorKey = anchorKeyQueue.removeFirst();
        MetaTrieHashTable.NodeMeta<E> nodeMeta = wormhole.table.get(anchorKey);
        if (!(nodeMeta instanceof NodeMetaInternal)) {
          if (!nodeMetas.remove(nodeMeta)) {
            throw new AssertionError(
                String.format("Unexpected node meta. Node meta: %s", nodeMeta));
          }
          continue;
        }

        NodeMetaInternal<K, E, V> nodeMetaInternal = (NodeMetaInternal<K, E, V>) nodeMeta;

        LeafNode<K, E, V> leftMostLeafNode = nodeMetaInternal.getLeftMostLeafNode();
        if (leftMostLeafNode != null) {
          if (!leftMostLeafNode.anchorKey.startsWith(anchorKey)) {
            throw new AssertionError(
                String.format(
                    "The anchor key of the node meta internal's left most leaf node doesn't start with the node meta internal's anchor key. Node meta internal: %s, Leaf node: %s",
                    nodeMeta, leftMostLeafNode));
          }
          if (leftMostLeafNode.getLeft() != null) {
            LeafNode<K, E, V> adjacentLeafNode = leftMostLeafNode.getLeft();
            if (adjacentLeafNode.anchorKey.startsWith(anchorKey)) {
              throw new AssertionError(
                  String.format(
                      "The anchor key of the adjacent leaf node left to the node meta internal's left most leaf node starts with the node meta internal's anchor key. Node meta internal: %s, Leaf node: %s",
                      nodeMeta, adjacentLeafNode));
            }
          }
        }

        LeafNode<K, E, V> rightMostLeafNode = nodeMetaInternal.getRightMostLeafNode();
        if (rightMostLeafNode != null) {
          if (!rightMostLeafNode.anchorKey.startsWith(anchorKey)) {
            throw new AssertionError(
                String.format(
                    "The anchor key of the node meta internal's right most leaf node doesn't start with the node meta internal's anchor key. Node meta internal: %s, Leaf node: %s",
                    nodeMeta, rightMostLeafNode));
          }
          if (rightMostLeafNode.getRight() != null) {
            LeafNode<K, E, V> adjacentLeafNode = rightMostLeafNode.getRight();
            if (adjacentLeafNode.anchorKey.startsWith(anchorKey)) {
              throw new AssertionError(
                  String.format(
                      "The anchor key of the adjacent leaf node right to the node meta internal's left most leaf node starts with the node meta internal's anchor key. Node meta internal: %s, Leaf node: %s",
                      nodeMeta, adjacentLeafNode));
            }
          }
        }

        if (!nodeMetas.remove(nodeMetaInternal)) {
          throw new AssertionError(String.format("Unexpected node meta. Node meta: %s", nodeMeta));
        }
        nodeMetaInternal.bitmap.stream()
            .forEach(childHeadChar -> anchorKeyQueue.addLast(anchorKey.append(childHeadChar)));
      }

      if (!nodeMetas.isEmpty()) {
        throw new AssertionError(
            String.format("There are orphan node metas. Orphan node metas: %s", nodeMetas));
      }
    }

    private void validateInternal() {
      MetaTrieHashTable<K, E, V> table = wormhole.table;

      LeafNode<K, E, V> leftMostLeafNode;
      LeafNode<K, E, V> rightMostLeafNode;
      MetaTrieHashTable.NodeMeta<E> rootNodeMeta = table.get(wormhole.emptyEncodedKey());
      if (rootNodeMeta instanceof MetaTrieHashTable.NodeMetaLeaf) {
        MetaTrieHashTable.NodeMetaLeaf<K, E, V> nodeMetaLeaf =
            (MetaTrieHashTable.NodeMetaLeaf<K, E, V>) rootNodeMeta;
        leftMostLeafNode = nodeMetaLeaf.leafNode;
        rightMostLeafNode = nodeMetaLeaf.leafNode;
      } else {
        NodeMetaInternal<K, E, V> nodeMetaInternal = (NodeMetaInternal<K, E, V>) rootNodeMeta;
        leftMostLeafNode = nodeMetaInternal.getLeftMostLeafNode();
        rightMostLeafNode = nodeMetaInternal.getRightMostLeafNode();
      }
      validateLeafNodes(leftMostLeafNode, rightMostLeafNode);

      validateHashTable();
    }
  }
}
