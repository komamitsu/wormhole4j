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

class MetaTrieHashTable<K, T> {
  private final Map<String, NodeMeta<K, T>> table = new HashMap<>();
  private int maxAnchorLength;

  abstract static class NodeMeta<K, T> {
    final String anchorPrefix;

    public NodeMeta(String anchorPrefix) {
      this.anchorPrefix = anchorPrefix;
    }
  }

  static class NodeMetaLeaf<K, T> extends NodeMeta<K, T> {
    final LeafNode<K, T> leafNode;

    NodeMetaLeaf(String anchorPrefix, LeafNode<K, T> leafNode) {
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
        String anchorPrefix,
        LeafNode<K, T> leftMostLeafNode,
        LeafNode<K, T> rightMostLeafNode,
        char initBitIndex) {
      super(anchorPrefix);
      this.leftMostLeafNode = leftMostLeafNode;
      this.rightMostLeafNode = rightMostLeafNode;
      this.bitmap = new BitSet(128);
      bitmap.set(initBitIndex);
    }

    Character findOneSibling(char fromIndex) {
      // Search left siblings.
      int index = bitmap.previousSetBit(fromIndex);
      if (index >= 0) {
        return (char) index;
      }
      // Search right siblings.
      index = bitmap.nextSetBit(fromIndex);
      if (index >= 0) {
        return (char) index;
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

  NodeMeta<K, T> get(String key) {
    return table.get(key);
  }

  void put(String key, NodeMeta<K, T> nodeMeta) {
    table.put(key, nodeMeta);
    maxAnchorLength = Math.max(maxAnchorLength, key.length());
  }

  Collection<NodeMeta<K, T>> values() {
    return table.values();
  }

  Set<Map.Entry<String, NodeMeta<K, T>>> entrySet() {
    return table.entrySet();
  }

  void handleSplitNodes(String newAnchorKey, LeafNode<K, T> newLeafNode) {
    NodeMetaLeaf<K, T> newNodeMeta = new NodeMetaLeaf<>(newAnchorKey, newLeafNode);
    NodeMeta<K, T> existingNodeMeta = get(newAnchorKey);
    if (existingNodeMeta != null) {
      throw new AssertionError(
          String.format(
              "There is a node meta that has the same key. Key: %s, Node meta: %s",
              newAnchorKey, existingNodeMeta));
    }
    put(newAnchorKey, newNodeMeta);

    // Update the ancestor NodeMeta instances for the new leaf node.
    for (int prefixLen = 0; prefixLen < newAnchorKey.length(); prefixLen++) {
      String prefix = newAnchorKey.substring(0, prefixLen);
      NodeMeta<K, T> node = table.get(prefix);
      if (node == null) {
        put(
            prefix,
            new NodeMetaInternal<>(
                prefix, newLeafNode, newLeafNode, newAnchorKey.charAt(prefixLen)));
        continue;
      }

      if (node instanceof NodeMetaLeaf) {
        LeafNode<K, T> leafNode = ((NodeMetaLeaf<K, T>) node).leafNode;

        // If there is a leaf node which has the same prefix, append the smallest token to the
        // prefix of the leaf node and add an internal node with the same prefix instead of the
        // original leaf node.
        String prefixWithSmallestToken = prefix + WormholeForStringKey.SMALLEST_TOKEN;
        NodeMetaLeaf<K, T> updatedNode = new NodeMetaLeaf<>(prefixWithSmallestToken, leafNode);
        put(prefixWithSmallestToken, updatedNode);

        NodeMetaInternal<K, T> parent =
            new NodeMetaInternal<>(
                prefix, leafNode, leafNode, WormholeForStringKey.BITMAP_INDEX_OF_SMALLEST_TOKEN);
        put(prefix, parent);

        node = parent;
      }

      assert node instanceof NodeMetaInternal;

      NodeMetaInternal<K, T> internalNode = (NodeMetaInternal<K, T>) node;

      // The pseudocode in the paper does not update existing internal nodes' bitmap. However,
      // this update is probably necessary.
      internalNode.bitmap.set(newAnchorKey.charAt(prefixLen));

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

  NodeMeta<K, T> searchLongestPrefixMatch(String searchKey) {
    String lpm = searchLongestPrefixMatchKey(searchKey);
    return table.get(lpm);
  }

  private String searchLongestPrefixMatchKey(String searchKey) {
    int m = 0;
    int n = Math.min(searchKey.length(), maxAnchorLength) + 1;
    while (m + 1 < n) {
      int prefixLen = (m + n) / 2;
      if (table.containsKey(searchKey.substring(0, prefixLen))) {
        m = prefixLen;
      } else {
        n = prefixLen;
      }
    }
    return searchKey.substring(0, m);
  }

  @Nullable
  NodeMetaLeaf<K, T> findNodeMetaLeaf(String anchorKey) {
    {
      NodeMeta<K, T> nodeMeta = table.get(anchorKey);
      if (nodeMeta instanceof NodeMetaLeaf) {
        return (NodeMetaLeaf<K, T>) nodeMeta;
      }
    }

    {
      String anchorKeyWithSmallestToken = anchorKey + WormholeForStringKey.SMALLEST_TOKEN;
      NodeMeta<K, T> nodeMeta = table.get(anchorKeyWithSmallestToken);
      if (nodeMeta instanceof NodeMetaLeaf) {
        return (NodeMetaLeaf<K, T>) nodeMeta;
      }
    }

    return null;
  }

  @Nullable
  NodeMetaInternal<K, T> findNodeMetaInternal(String anchorKey) {
    {
      NodeMeta<K, T> nodeMeta = table.get(anchorKey);
      if (nodeMeta instanceof NodeMetaInternal) {
        return (NodeMetaInternal<K, T>) nodeMeta;
      }
    }

    {
      String anchorKeyWithSmallestToken = anchorKey + WormholeForStringKey.SMALLEST_TOKEN;
      NodeMeta<K, T> nodeMeta = table.get(anchorKeyWithSmallestToken);
      if (nodeMeta instanceof NodeMetaInternal) {
        return (NodeMetaInternal<K, T>) nodeMeta;
      }
    }

    return null;
  }

  String removeNodeMetaLeaf(String origAnchorKey) {
    NodeMeta<K, T> nodeMeta = table.get(origAnchorKey);
    String anchorKey =
        nodeMeta instanceof NodeMetaLeaf
            ? origAnchorKey
            : origAnchorKey + WormholeForStringKey.SMALLEST_TOKEN;

    NodeMeta<K, T> removed = table.remove(anchorKey);
    if (removed == null) {
      throw new AssertionError(
          String.format("Node meta leaf for anchor key '%s' not found for removal", anchorKey));
    }
    if (anchorKey.length() >= maxAnchorLength) {
      maxAnchorLength = calcMaxAnchorLength();
    }
    if (removed instanceof NodeMetaLeaf) {
      return anchorKey;
    }

    throw new AssertionError(
        String.format(
            "Removed node meta is an unexpected type. Expected: %s, Actual: %s",
            NodeMetaLeaf.class.getName(), removed.getClass().getName()));
  }

  String removeNodeMetaInternal(String origAnchorKey) {
    NodeMeta<K, T> nodeMeta = table.get(origAnchorKey);
    String anchorKey =
        nodeMeta instanceof NodeMetaInternal
            ? origAnchorKey
            : origAnchorKey + WormholeForStringKey.SMALLEST_TOKEN;

    NodeMeta<K, T> removed = table.remove(anchorKey);
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
    for (String key : table.keySet()) {
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
