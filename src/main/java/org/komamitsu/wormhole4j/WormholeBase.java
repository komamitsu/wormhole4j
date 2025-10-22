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
 * @param <T> the type of values stored in this index
 */
abstract class WormholeBase<K, T> {
  private static final int DEFAULT_LEAF_NODE_SIZE = 128;
  private final MetaTrieHashTable<K, T> table = new MetaTrieHashTable<>();
  private final int leafNodeSize;
  private final int leafNodeMergeSize;
  @Nullable private final Validator<K, T> validator;
  private final Function<String, String> validAnchorKeyProvider;

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

  abstract String encodeKey(K key);

  /**
   * Inserts or updates a key-value pair.
   *
   * @param key the key (must not be {@code null})
   * @param value the value to associate with the key
   */
  public void put(K key, T value) {
    String encodedKey = encodeKey(key);
    LeafNode<K, T> leafNode = searchTrieHashTable(encodedKey);
    KeyValue<K, T> existingKeyValue = leafNode.pointSearchLeaf(encodedKey);
    if (existingKeyValue != null) {
      existingKeyValue.setValue(value);
      validateIfNeeded();
      return;
    }

    Key<K> k = new Key<>(key, encodedKey);

    if (leafNode.size() == leafNodeSize) {
      // Split the node and get a new right leaf node.
      LeafNode<K, T> newLeafNode = split(leafNode);
      if (Utils.compareAnchorKeys(encodedKey, newLeafNode.anchorKey) < 0) {
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
    String encodedKey = encodeKey(key);
    LeafNode<K, T> leafNode = searchTrieHashTable(encodedKey);
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
  public T get(K key) {
    String encodedKey = encodeKey(key);
    LeafNode<K, T> leafNode = searchTrieHashTable(encodedKey);
    KeyValue<K, T> keyValue = leafNode.pointSearchLeaf(encodedKey);
    if (keyValue == null) {
      return null;
    }
    return keyValue.getValue();
  }

  private void scanInternal(
      String encodedStartKey,
      @Nullable String encodedEndKey,
      boolean isEndKeyExclusive,
      @Nullable Integer count,
      Function<KeyValue<K, T>, Boolean> function) {
    Function<KeyValue<K, T>, Boolean> actualFunction = function;
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

    LeafNode<K, T> leafNode = searchTrieHashTable(encodedStartKey);
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
  public List<KeyValue<K, T>> scanWithCount(K startKey, int count) {
    List<KeyValue<K, T>> result = new ArrayList<>(count);
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
      Function<KeyValue<K, T>, Boolean> function) {
    scanInternal(
        startKey != null ? encodeKey(startKey) : "",
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
      @Nullable K startKey, @Nullable K endKey, Function<KeyValue<K, T>, Boolean> function) {
    scanInternal(
        startKey != null ? encodeKey(startKey) : "",
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
      @Nullable K startKey, @Nullable K endKey, Function<KeyValue<K, T>, Boolean> function) {
    scanInternal(
        startKey != null ? encodeKey(startKey) : "",
        endKey != null ? encodeKey(endKey) : null,
        false,
        null,
        function);
  }

  private void initialize() {
    String key = "";
    LeafNode<K, T> rootLeafNode =
        new LeafNode<>(validAnchorKeyProvider, key, leafNodeSize, null, null);
    // Add the root.
    table.put(key, new MetaTrieHashTable.NodeMetaLeaf<>(key, rootLeafNode));
  }

  private LeafNode<K, T> searchTrieHashTable(String encodedKey) {
    MetaTrieHashTable.NodeMeta<K, T> nodeMeta = table.searchLongestPrefixMatch(encodedKey);
    if (nodeMeta instanceof MetaTrieHashTable.NodeMetaLeaf) {
      return ((MetaTrieHashTable.NodeMetaLeaf<K, T>) nodeMeta).leafNode;
    }

    NodeMetaInternal<K, T> nodeMetaInternal = (NodeMetaInternal<K, T>) nodeMeta;
    int anchorPrefixLength = nodeMetaInternal.anchorPrefix.length();

    // The leaf type is INTERNAL.
    if (anchorPrefixLength == encodedKey.length()) {
      LeafNode<K, T> leafNode = nodeMetaInternal.getLeftMostLeafNode();
      if (Utils.compareAnchorKeys(encodedKey, leafNode.anchorKey) < 0) {
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

    char missingToken = encodedKey.charAt(anchorPrefixLength);
    Character siblingToken = nodeMetaInternal.findOneSibling(missingToken);
    if (siblingToken == null) {
      // TODO: Test and add comment.
      // throw new AssertionError("Any sibling token is not found");
      return nodeMetaInternal.getLeftMostLeafNode();
    }

    MetaTrieHashTable.NodeMeta<K, T> childNode =
        table.get(nodeMetaInternal.anchorPrefix + siblingToken);
    if (childNode == null) {
      throw new AssertionError("Child node is not found");
    }

    if (childNode instanceof MetaTrieHashTable.NodeMetaLeaf) {
      LeafNode<K, T> leafNode = ((MetaTrieHashTable.NodeMetaLeaf<K, T>) childNode).leafNode;
      if (missingToken < siblingToken) {
        return leafNode.getLeft();
      } else {
        return leafNode;
      }
    } else {
      NodeMetaInternal<K, T> childNodeInternal = (NodeMetaInternal<K, T>) childNode;
      if (missingToken < siblingToken) {
        // The child node is a subtree right to the target node.
        return childNodeInternal.getLeftMostLeafNode().getLeft();
      } else {
        // The child node is a subtree left to the target node.
        return childNodeInternal.getRightMostLeafNode();
      }
    }
  }

  private String provideValidAnchorKey(String anchorKey) {
    MetaTrieHashTable.NodeMeta<K, T> existingNodeMeta = table.get(anchorKey);
    if (existingNodeMeta == null) {
      return anchorKey;
    }
    return null;

    // TODO: Remove this. SMALLEST_TOKEN shouldn't be used.
    /*
    // "Append 0s to key when necessary"
    anchorKey = anchorKey + Constants.SMALLEST_TOKEN;
    existingNodeMeta = table.get(anchorKey);
    if (existingNodeMeta instanceof MetaTrieHashTable.NodeMetaLeaf) {
      return null;
    }
    return anchorKey;
     */
  }

  private LeafNode<K, T> split(LeafNode<K, T> leafNode) {
    Tuple<String, LeafNode<K, T>> newLeafNodeAndAnchor = leafNode.splitToNewLeafNode();
    String newAnchor = newLeafNodeAndAnchor.first;
    LeafNode<K, T> newLeafNode = newLeafNodeAndAnchor.second;
    table.handleSplitNodes(newAnchor, newLeafNode);
    return newLeafNode;
  }

  private void merge(LeafNode<K, T> left, LeafNode<K, T> victim) {
    left.merge(victim);
    boolean childNodeRemoved = false;
    for (int prefixlen = victim.anchorKey.length(); prefixlen >= 0; prefixlen--) {
      String prefix = victim.anchorKey.substring(0, prefixlen);
      MetaTrieHashTable.NodeMeta<K, T> nodeMeta = table.get(prefix);
      NodeMetaInternal<K, T> nodeMetaInternal = null;
      MetaTrieHashTable.NodeMetaLeaf<K, T> nodeMetaLeaf = null;
      if (nodeMeta instanceof NodeMetaInternal<K, T>) {
        nodeMetaInternal = (NodeMetaInternal<K, T>) nodeMeta;
      } else {
        assert nodeMeta instanceof MetaTrieHashTable.NodeMetaLeaf<K, T>;
        nodeMetaLeaf = (NodeMetaLeaf<K, T>) nodeMeta;
      }

      // The pseudocode in the paper always clears the bitmap index for the child token.
      // However, it should probably be cleared only when the child node has been removed.
      if (childNodeRemoved) {
        if (nodeMetaInternal != null) {
          nodeMetaInternal.bitmap.clear(victim.anchorKey.charAt(prefixlen));
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

  static class Validator<K, T> {
    private final WormholeBase<K, T> wormhole;

    Validator(WormholeBase<K, T> wormhole) {
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
        LeafNode<K, T> leftMostLeafNode, LeafNode<K, T> rightMostLeafNode) {
      LeafNode<K, T> leafNode = leftMostLeafNode;
      LeafNode<K, T> lastLeafNode = null;
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
                    leafNode, leafNode.getLeft()));
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
      for (Map.Entry<String, MetaTrieHashTable.NodeMeta<K, T>> entry : wormhole.table.entrySet()) {
        String key = entry.getKey();
        MetaTrieHashTable.NodeMeta<K, T> nodeMeta = entry.getValue();
        if (!nodeMeta.anchorPrefix.equals(key)) {
          throw new AssertionError(
              String.format(
                  "The node metadata anchor key is different from the key of the hash table. Key: %s, Node metadata anchor key: %s",
                  key, nodeMeta.anchorPrefix));
        }
      }

      Collection<MetaTrieHashTable.NodeMeta<K, T>> nodeMetas =
          new HashSet<>(wormhole.table.values());
      LinkedList<String> anchorKeyQueue = new LinkedList<>();
      anchorKeyQueue.addLast("");
      while (!anchorKeyQueue.isEmpty()) {
        String anchorKey = anchorKeyQueue.removeFirst();
        MetaTrieHashTable.NodeMeta<K, T> nodeMeta = wormhole.table.get(anchorKey);
        if (!(nodeMeta instanceof NodeMetaInternal)) {
          if (!nodeMetas.remove(nodeMeta)) {
            throw new AssertionError(
                String.format("Unexpected node meta. Node meta: %s", nodeMeta));
          }
          continue;
        }

        NodeMetaInternal<K, T> nodeMetaInternal = (NodeMetaInternal<K, T>) nodeMeta;

        LeafNode<K, T> leftMostLeafNode = nodeMetaInternal.getLeftMostLeafNode();
        if (leftMostLeafNode != null) {
          if (!leftMostLeafNode.anchorKey.startsWith(anchorKey)) {
            throw new AssertionError(
                String.format(
                    "The anchor key of the node meta internal's left most leaf node doesn't start with the node meta internal's anchor key. Node meta internal: %s, Leaf node: %s",
                    nodeMeta, leftMostLeafNode));
          }
          if (leftMostLeafNode.getLeft() != null) {
            LeafNode<K, T> adjacentLeafNode = leftMostLeafNode.getLeft();
            if (adjacentLeafNode.anchorKey.startsWith(anchorKey)) {
              throw new AssertionError(
                  String.format(
                      "The anchor key of the adjacent leaf node left to the node meta internal's left most leaf node starts with the node meta internal's anchor key. Node meta internal: %s, Leaf node: %s",
                      nodeMeta, adjacentLeafNode));
            }
          }
        }

        LeafNode<K, T> rightMostLeafNode = nodeMetaInternal.getRightMostLeafNode();
        if (rightMostLeafNode != null) {
          if (!rightMostLeafNode.anchorKey.startsWith(anchorKey)) {
            throw new AssertionError(
                String.format(
                    "The anchor key of the node meta internal's right most leaf node doesn't start with the node meta internal's anchor key. Node meta internal: %s, Leaf node: %s",
                    nodeMeta, rightMostLeafNode));
          }
          if (rightMostLeafNode.getRight() != null) {
            LeafNode<K, T> adjacentLeafNode = rightMostLeafNode.getRight();
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
            .forEach(childHeadChar -> anchorKeyQueue.addLast(anchorKey + ((char) childHeadChar)));
      }

      if (!nodeMetas.isEmpty()) {
        throw new AssertionError(
            String.format("There are orphan node metas. Orphan node metas: %s", nodeMetas));
      }
    }

    private void validateInternal() {
      MetaTrieHashTable<K, T> table = wormhole.table;

      LeafNode<K, T> leftMostLeafNode;
      LeafNode<K, T> rightMostLeafNode;
      MetaTrieHashTable.NodeMeta<K, T> rootNodeMeta = table.get("");
      if (rootNodeMeta instanceof MetaTrieHashTable.NodeMetaLeaf<K, T>) {
        MetaTrieHashTable.NodeMetaLeaf<K, T> nodeMetaLeaf =
            (MetaTrieHashTable.NodeMetaLeaf<K, T>) rootNodeMeta;
        leftMostLeafNode = nodeMetaLeaf.leafNode;
        rightMostLeafNode = nodeMetaLeaf.leafNode;
      } else {
        NodeMetaInternal<K, T> nodeMetaInternal = (NodeMetaInternal<K, T>) rootNodeMeta;
        leftMostLeafNode = nodeMetaInternal.getLeftMostLeafNode();
        rightMostLeafNode = nodeMetaInternal.getRightMostLeafNode();
      }
      validateLeafNodes(leftMostLeafNode, rightMostLeafNode);

      validateHashTable();
    }
  }
}
