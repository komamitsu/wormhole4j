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

class MetaTrieHashTable<K, V> {
  private final Map<EncodedKey, NodeMeta<K, V>> table = new HashMap<>();
  private int maxAnchorLength;

  abstract static class NodeMeta<K, T> {
    final EncodedKey anchorPrefix;

    public NodeMeta(EncodedKey anchorPrefix) {
      this.anchorPrefix = anchorPrefix;
    }
  }

  static class NodeMetaLeaf<K, T> extends NodeMeta<K, T> {
    final LeafNode<K, T> leafNode;

    NodeMetaLeaf(EncodedKey anchorPrefix, LeafNode<K, T> leafNode) {
      super(anchorPrefix);
      this.leafNode = leafNode;
    }

    @Override
    public String toString() {
      return "NodeMetaLeaf{" + "leafNode=" + leafNode + '}';
    }
  }

  static class NodeMetaInternal<K, T> extends NodeMeta<K, T> {
    private LeafNode<K, T> leftMostLeafNode;
    private LeafNode<K, T> rightMostLeafNode;
    final BitSet bitmap;

    NodeMetaInternal(
        EncodedKey anchorPrefix,
        LeafNode<K, T> leftMostLeafNode,
        LeafNode<K, T> rightMostLeafNode,
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

    LeafNode<K, T> getLeftMostLeafNode() {
      return leftMostLeafNode;
    }

    LeafNode<K, T> getRightMostLeafNode() {
      return rightMostLeafNode;
    }

    void setLeftMostLeafNode(LeafNode<K, T> leftMostLeafNode) {
      this.leftMostLeafNode = leftMostLeafNode;
    }

    void setRightMostLeafNode(LeafNode<K, T> rightMostLeafNode) {
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

  NodeMeta<K, V> get(EncodedKey key) {
    return table.get(key);
  }

  void put(EncodedKey key, NodeMeta<K, V> nodeMeta) {
    table.put(key, nodeMeta);
    maxAnchorLength = Math.max(maxAnchorLength, key.length());
  }

  Collection<NodeMeta<K, V>> values() {
    return table.values();
  }

  Set<Map.Entry<EncodedKey, NodeMeta<K, V>>> entrySet() {
    return table.entrySet();
  }

  void handleSplitNodes(EncodedKey newAnchorKey, LeafNode<K, V> newLeafNode) {
    NodeMetaLeaf<K, V> newNodeMeta = new NodeMetaLeaf<>(newAnchorKey, newLeafNode);
    NodeMeta<K, V> existingNodeMeta = get(newAnchorKey);
    if (existingNodeMeta != null) {
      throw new AssertionError(
          String.format(
              "There is a node meta that has the same key. Key: %s, Node meta: %s",
              newAnchorKey, existingNodeMeta));
    }
    put(newAnchorKey, newNodeMeta);

    // Update the ancestor NodeMeta instances for the new leaf node.
    for (int prefixLen = 0; prefixLen < newAnchorKey.length(); prefixLen++) {
      EncodedKey prefix = newAnchorKey.slice(0, prefixLen);
      NodeMeta<K, V> node = table.get(prefix);
      if (node == null) {
        put(
            prefix,
            new NodeMetaInternal<>(prefix, newLeafNode, newLeafNode, newAnchorKey.get(prefixLen)));
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

  NodeMeta<K, V> searchLongestPrefixMatch(EncodedKey searchKey) {
    EncodedKey lpm = searchLongestPrefixMatchKey(searchKey);
    return table.get(lpm);
  }

  private EncodedKey searchLongestPrefixMatchKey(EncodedKey searchKey) {
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

  void removeNodeMeta(EncodedKey anchorKey) {
    NodeMeta<K, V> removed = table.remove(anchorKey);
    if (removed == null) {
      throw new AssertionError(
          String.format("Node meta leaf for anchor key '%s' not found for removal", anchorKey));
    }
    if (anchorKey.length() >= maxAnchorLength) {
      maxAnchorLength = calcMaxAnchorLength();
    }
  }

  EncodedKey removeNodeMetaInternal(EncodedKey anchorKey) {
    NodeMeta<K, V> removed = table.remove(anchorKey);
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
    for (EncodedKey key : table.keySet()) {
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
