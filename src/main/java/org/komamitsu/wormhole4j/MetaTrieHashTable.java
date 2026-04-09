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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;
import javax.annotation.Nullable;

class MetaTrieHashTable<K, V> {
  private final EncodedKeyType encodedKeyType;
  private int maxAnchorLength;

  private final Map<Object, NodeMeta> table;
  private final boolean isConcurrent;
  private final StampedLock lock = new StampedLock();

  MetaTrieHashTable(EncodedKeyType encodedKeyType, boolean isConcurrent) {
    this.encodedKeyType = encodedKeyType;
    this.isConcurrent = isConcurrent;
    if (isConcurrent) {
      table = new ConcurrentHashMap<>();
    } else {
      table = new HashMap<>();
    }
  }

  long acquireReadLock() {
    if (isConcurrent) {
      return lock.readLock();
    }
    throw new UnsupportedOperationException();
  }

  long acquireWriteLock() {
    if (isConcurrent) {
      return lock.writeLock();
    }
    throw new UnsupportedOperationException();
  }

  void releaseLock(long stamp) {
    if (isConcurrent) {
      lock.unlock(stamp);
      return;
    }
    throw new UnsupportedOperationException();
  }

  long tryReadLock() {
    if (isConcurrent) {
      return lock.tryReadLock();
    }
    throw new UnsupportedOperationException();
  }

  abstract static class NodeMeta {
    final Object anchorPrefix;

    public NodeMeta(Object anchorPrefix) {
      this.anchorPrefix = anchorPrefix;
    }
  }

  static class NodeMetaLeaf<K, V> extends NodeMeta {
    final LeafNode<K, V> leafNode;

    NodeMetaLeaf(Object anchorPrefix, LeafNode<K, V> leafNode) {
      super(anchorPrefix);
      this.leafNode = leafNode;
    }

    @Override
    public String toString() {
      return "NodeMetaLeaf{" + "leafNode=" + leafNode + ", anchorPrefix=" + anchorPrefix + '}';
    }
  }

  static class NodeMetaInternal<K, V> extends NodeMeta {
    private LeafNode<K, V> leftMostLeafNode;
    private LeafNode<K, V> rightMostLeafNode;
    final BitSet bitmap;

    NodeMetaInternal(
        Object anchorPrefix,
        LeafNode<K, V> leftMostLeafNode,
        LeafNode<K, V> rightMostLeafNode,
        @Nullable Integer initBitIndex) {
      super(anchorPrefix);
      this.leftMostLeafNode = leftMostLeafNode;
      this.rightMostLeafNode = rightMostLeafNode;
      this.bitmap = new BitSet(128);
      if (initBitIndex != null) {
        bitmap.set(initBitIndex);
      }
    }

    Integer findOneSibling(int fromIndex) {
      // Search left siblings.
      int index = bitmap.previousSetBit(fromIndex);
      if (index >= 0) {
        return index;
      }
      // Search right siblings.
      index = bitmap.nextSetBit(fromIndex);
      if (index >= 0) {
        return index;
      }
      return null;
    }

    LeafNode<K, V> getLeftMostLeafNode() {
      return leftMostLeafNode;
    }

    LeafNode<K, V> getRightMostLeafNode() {
      return rightMostLeafNode;
    }

    void setLeftMostLeafNode(LeafNode<K, V> leftMostLeafNode) {
      this.leftMostLeafNode = leftMostLeafNode;
    }

    void setRightMostLeafNode(LeafNode<K, V> rightMostLeafNode) {
      this.rightMostLeafNode = rightMostLeafNode;
    }

    @Override
    public String toString() {
      return "NodeMetaInternal{"
          + "leftMostLeafNode="
          + leftMostLeafNode
          + ", rightMostLeafNode="
          + rightMostLeafNode
          + ", bitmap="
          + bitmap
          + '}';
    }
  }

  NodeMeta get(Object key) {
    return table.get(key);
  }

  void put(Object key, NodeMeta nodeMeta) {
    table.put(key, nodeMeta);
    maxAnchorLength = Math.max(maxAnchorLength, EncodedKeyUtils.length(encodedKeyType, key));
  }

  @VisibleForTesting
  Collection<NodeMeta> valuesForValidate() {
    return table.values();
  }

  @VisibleForTesting
  Set<Map.Entry<Object, NodeMeta>> entrySetForValidate() {
    return table.entrySet();
  }

  @VisibleForTesting
  NodeMeta getForValidate(Object key) {
    return table.get(key);
  }

  void handleSplitNodes(Object newAnchorKey, LeafNode<K, V> newLeafNode) {
    NodeMetaLeaf<K, V> newNodeMeta = new NodeMetaLeaf<>(newAnchorKey, newLeafNode);
    NodeMeta existingNodeMeta = get(newAnchorKey);
    if (existingNodeMeta != null) {
      throw new AssertionError(
          String.format(
              "There is a node meta that has the same key. Key: %s, Node meta: %s",
              newAnchorKey, existingNodeMeta));
    }
    put(newAnchorKey, newNodeMeta);

    // Update the ancestor NodeMeta instances for the new leaf node.
    for (int prefixLen = 0;
        prefixLen < EncodedKeyUtils.length(encodedKeyType, newAnchorKey);
        prefixLen++) {
      Object prefix = EncodedKeyUtils.slice(encodedKeyType, newAnchorKey, prefixLen);
      NodeMeta node = get(prefix);
      if (node == null) {
        put(
            prefix,
            new NodeMetaInternal<>(
                prefix,
                newLeafNode,
                newLeafNode,
                EncodedKeyUtils.get(encodedKeyType, newAnchorKey, prefixLen)));
        continue;
      }

      if (node instanceof NodeMetaLeaf) {
        LeafNode<K, V> leafNode = ((NodeMetaLeaf<K, V>) node).leafNode;

        // In the original paper, if there is a leaf node which has the same prefix, append the
        // smallest token to the prefix of the leaf node and add an internal node with the same
        // prefix instead of the original leaf node. Wormhole4j followed this approach in
        // v0.0.*.
        // However, the author improved the approach by removing the smallest token in their
        // reference implementation https://github.com/wuxb45/wormhole. Wormhole4j v0.1.* or
        // later follows the approach.
        /*
        String prefixWithSmallestToken = prefix + Constants.SMALLEST_TOKEN;
        NodeMetaLeaf<K, V> updatedNode = new NodeMetaLeaf<>(prefixWithSmallestToken, leafNode);
        put(prefixWithSmallestToken, updatedNode);

        NodeMetaInternal<K, V> parent =
            new NodeMetaInternal<>(
                prefix, leafNode, leafNode, Constants.BITMAP_INDEX_OF_SMALLEST_TOKEN);
        put(prefix, parent);

        node = parent;
         */
        node = new NodeMetaInternal<>(prefix, leafNode, newLeafNode, null);
        put(prefix, node);
      }

      assert node instanceof NodeMetaInternal;

      NodeMetaInternal<K, V> internalNode = (NodeMetaInternal<K, V>) node;

      // The pseudocode in the paper does not update existing internal nodes' bitmap. However,
      // this update is probably necessary.
      internalNode.bitmap.set(EncodedKeyUtils.get(encodedKeyType, newAnchorKey, prefixLen));

      // The pseudocode in the paper checks and updates the original leaf node. However, this
      // loop traverses the ancestors of the new leaf node's anchor key, and the ancestors'
      // left-most and right-most ranges are updated to include the new leaf node.
      // Therefore, the new leaf node should probably be checked and set if needed.
      if (internalNode.getLeftMostLeafNode() == newLeafNode.getRight()) {
        internalNode.setLeftMostLeafNode(newLeafNode);
      }
      if (internalNode.getRightMostLeafNode() == newLeafNode.getLeft()) {
        internalNode.setRightMostLeafNode(newLeafNode);
      }
    }
  }

  NodeMeta searchLongestPrefixMatch(EncodedKeyType encodedKeyType, Object searchKey) {
    Object lpm = searchLongestPrefixMatchKey(encodedKeyType, searchKey);
    return get(lpm);
  }

  private Object searchLongestPrefixMatchKey(EncodedKeyType encodedKeyType, Object searchKey) {
    int m = 0;
    int n = Math.min(EncodedKeyUtils.length(encodedKeyType, searchKey), maxAnchorLength) + 1;
    while (m + 1 < n) {
      int prefixLen = (m + n) / 2;
      if (table.get(EncodedKeyUtils.slice(encodedKeyType, searchKey, prefixLen)) != null) {
        m = prefixLen;
      } else {
        n = prefixLen;
      }
    }
    return EncodedKeyUtils.slice(encodedKeyType, searchKey, m);
  }

  void removeNodeMeta(Object anchorKey) {
    NodeMeta removed = table.remove(anchorKey);
    if (removed == null) {
      throw new AssertionError(
          String.format("Node meta leaf for anchor key '%s' not found for removal", anchorKey));
    }
    if (EncodedKeyUtils.length(encodedKeyType, anchorKey) >= maxAnchorLength) {
      maxAnchorLength = calcMaxAnchorLength();
    }
  }

  Object removeNodeMetaInternal(Object anchorKey) {
    NodeMeta removed = table.remove(anchorKey);
    if (removed == null) {
      throw new AssertionError(
          String.format("Node meta internal for anchor key '%s' not found for removal", anchorKey));
    }
    if (EncodedKeyUtils.length(encodedKeyType, anchorKey) >= maxAnchorLength) {
      maxAnchorLength = calcMaxAnchorLength();
    }
    if (removed instanceof NodeMetaInternal) {
      return anchorKey;
    }

    throw new AssertionError(
        String.format(
            "Removed node meta is an unexpected type. Expected: %s, Actual: %s",
            NodeMetaInternal.class.getName(), removed.getClass().getName()));
  }

  private int calcMaxAnchorLength() {
    int max = 0;
    for (Object key : table.keySet()) {
      int keyLen = EncodedKeyUtils.length(encodedKeyType, key);
      if (max < keyLen) {
        max = keyLen;
      }
    }
    return max;
  }

  @Override
  public String toString() {
    return "MetaTrieHashTable{" + "table=" + table + '}';
  }
}
