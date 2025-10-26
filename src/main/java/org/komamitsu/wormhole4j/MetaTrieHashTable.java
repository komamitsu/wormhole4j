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
import javax.annotation.Nullable;

class MetaTrieHashTable<K, E extends EncodedKey<E>, V> {
  private final Map<E, NodeMeta<E>> table = new HashMap<>();
  private int maxAnchorLength;

  abstract static class NodeMeta<E extends EncodedKey<E>> {
    final E anchorPrefix;

    public NodeMeta(E anchorPrefix) {
      this.anchorPrefix = anchorPrefix;
    }
  }

  static class NodeMetaLeaf<K, E extends EncodedKey<E>, V> extends NodeMeta<E> {
    final LeafNode<K, E, V> leafNode;

    NodeMetaLeaf(E anchorPrefix, LeafNode<K, E, V> leafNode) {
      super(anchorPrefix);
      this.leafNode = leafNode;
    }

    @Override
    public String toString() {
      return "NodeMetaLeaf{" + "leafNode=" + leafNode + '}';
    }
  }

  static class NodeMetaInternal<K, E extends EncodedKey<E>, V> extends NodeMeta<E> {
    private LeafNode<K, E, V> leftMostLeafNode;
    private LeafNode<K, E, V> rightMostLeafNode;
    final BitSet bitmap;

    NodeMetaInternal(
        E anchorPrefix,
        LeafNode<K, E, V> leftMostLeafNode,
        LeafNode<K, E, V> rightMostLeafNode,
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

    LeafNode<K, E, V> getLeftMostLeafNode() {
      return leftMostLeafNode;
    }

    LeafNode<K, E, V> getRightMostLeafNode() {
      return rightMostLeafNode;
    }

    void setLeftMostLeafNode(LeafNode<K, E, V> leftMostLeafNode) {
      this.leftMostLeafNode = leftMostLeafNode;
    }

    void setRightMostLeafNode(LeafNode<K, E, V> rightMostLeafNode) {
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

  NodeMeta<E> get(E key) {
    return table.get(key);
  }

  void put(E key, NodeMeta<E> nodeMeta) {
    table.put(key, nodeMeta);
    maxAnchorLength = Math.max(maxAnchorLength, key.length());
  }

  Collection<NodeMeta<E>> values() {
    return table.values();
  }

  Set<Map.Entry<E , NodeMeta<E>>> entrySet() {
    return table.entrySet();
  }

  void handleSplitNodes(E newAnchorKey, LeafNode<K, E, V> newLeafNode) {
    NodeMetaLeaf<K, E, V> newNodeMeta = new NodeMetaLeaf<>(newAnchorKey, newLeafNode);
    NodeMeta<E> existingNodeMeta = get(newAnchorKey);
    if (existingNodeMeta != null) {
      throw new AssertionError(
          String.format(
              "There is a node meta that has the same key. Key: %s, Node meta: %s",
              newAnchorKey, existingNodeMeta));
    }
    put(newAnchorKey, newNodeMeta);

    // Update the ancestor NodeMeta instances for the new leaf node.
    for (int prefixLen = 0; prefixLen < newAnchorKey.length(); prefixLen++) {
      E prefix = newAnchorKey.slice(0, prefixLen);
      NodeMeta<E> node = table.get(prefix);
      if (node == null) {
        put(
            prefix,
            new NodeMetaInternal<>(prefix, newLeafNode, newLeafNode, newAnchorKey.get(prefixLen)));
        continue;
      }

      if (node instanceof NodeMetaLeaf) {
        LeafNode<K, E, V> leafNode = ((NodeMetaLeaf<K, E, V>) node).leafNode;

        // In the original paper, if there is a leaf node which has the same prefix, append the
        // smallest token to the prefix of the leaf node and add an internal node with the same
        // prefix instead of the original leaf node. Wormhole4j followed this approach in
        // v0.0.*.
        // However, the author improved the approach by removing the smallest token in their
        // reference implementation https://github.com/wuxb45/wormhole. Wormhole4j v0.1.* or
        // later follows the approach.
        /*
        String prefixWithSmallestToken = prefix + Constants.SMALLEST_TOKEN;
        NodeMetaLeaf<K, E, V> updatedNode = new NodeMetaLeaf<>(prefixWithSmallestToken, leafNode);
        put(prefixWithSmallestToken, updatedNode);

        NodeMetaInternal<K, E, V> parent =
            new NodeMetaInternal<>(
                prefix, leafNode, leafNode, Constants.BITMAP_INDEX_OF_SMALLEST_TOKEN);
        put(prefix, parent);

        node = parent;
         */
        node = new NodeMetaInternal<>(prefix, leafNode, newLeafNode, null);
        put(prefix, node);
      }

      assert node instanceof NodeMetaInternal;

      NodeMetaInternal<K, E, V> internalNode = (NodeMetaInternal<K, E, V>) node;

      // The pseudocode in the paper does not update existing internal nodes' bitmap. However,
      // this update is probably necessary.
      internalNode.bitmap.set(newAnchorKey.get(prefixLen));

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

  NodeMeta<E> searchLongestPrefixMatch(E searchKey) {
    E lpm = searchLongestPrefixMatchKey(searchKey);
    return table.get(lpm);
  }

  private E searchLongestPrefixMatchKey(E searchKey) {
    int m = 0;
    int n = Math.min(searchKey.length(), maxAnchorLength) + 1;
    while (m + 1 < n) {
      int prefixLen = (m + n) / 2;
      if (table.containsKey(searchKey.slice(0, prefixLen))) {
        m = prefixLen;
      } else {
        n = prefixLen;
      }
    }
    return searchKey.slice(0, m);
  }

  void removeNodeMeta(E anchorKey) {
    NodeMeta<E> removed = table.remove(anchorKey);
    if (removed == null) {
      throw new AssertionError(
          String.format("Node meta leaf for anchor key '%s' not found for removal", anchorKey));
    }
    if (anchorKey.length() >= maxAnchorLength) {
      maxAnchorLength = calcMaxAnchorLength();
    }
  }

  E removeNodeMetaInternal(E anchorKey) {
    NodeMeta<E> removed = table.remove(anchorKey);
    if (removed == null) {
      throw new AssertionError(
          String.format("Node meta internal for anchor key '%s' not found for removal", anchorKey));
    }
    if (anchorKey.length() >= maxAnchorLength) {
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
    for (E key : table.keySet()) {
      if (max < key.length()) {
        max = key.length();
      }
    }
    return max;
  }

  @Override
  public String toString() {
    return "MetaTrieHashTable{" + "table=" + table + '}';
  }
}
