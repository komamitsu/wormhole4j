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

class MetaTrieHashTable<T> {
  private final Map<String, NodeMeta<T>> table = new HashMap<>();
  private int maxAnchorLength;

  abstract static class NodeMeta<T> {
    final String anchorPrefix;

    public NodeMeta(String anchorPrefix) {
      this.anchorPrefix = anchorPrefix;
    }
  }

  static class NodeMetaLeaf<T> extends NodeMeta<T> {
    final LeafNode<T> leafNode;

    NodeMetaLeaf(String anchorPrefix, LeafNode<T> leafNode) {
      super(anchorPrefix);
      this.leafNode = leafNode;
    }

    @Override
    public String toString() {
      return "NodeMetaLeaf{" + "leafNode=" + leafNode + '}';
    }
  }

  static class NodeMetaInternal<T> extends NodeMeta<T> {
    private LeafNode<T> leftMostLeafNode;
    private LeafNode<T> rightMostLeafNode;
    final BitSet bitmap;

    NodeMetaInternal(
        String anchorPrefix,
        LeafNode<T> leftMostLeafNode,
        LeafNode<T> rightMostLeafNode,
        char initBitIndex) {
      super(anchorPrefix);
      this.leftMostLeafNode = leftMostLeafNode;
      this.rightMostLeafNode = rightMostLeafNode;
      this.bitmap = new BitSet();
      bitmap.set(initBitIndex);
    }

    Character findOneSibling(char sibling) {
      // Search left siblings.
      int index = bitmap.previousSetBit(sibling);
      if (index >= 0) {
        return (char) index;
      }
      // Search right siblings.
      index = bitmap.nextSetBit(sibling);
      if (index >= 0) {
        return (char) index;
      }
      return null;
    }

    LeafNode<T> getLeftMostLeafNode() {
      return leftMostLeafNode;
    }

    LeafNode<T> getRightMostLeafNode() {
      return rightMostLeafNode;
    }

    void setLeftMostLeafNode(LeafNode<T> leftMostLeafNode) {
      this.leftMostLeafNode = leftMostLeafNode;
    }

    void setRightMostLeafNode(LeafNode<T> rightMostLeafNode) {
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

  NodeMeta<T> get(String key) {
    return table.get(key);
  }

  void put(String key, NodeMeta<T> nodeMeta) {
    table.put(key, nodeMeta);
    maxAnchorLength = Math.max(maxAnchorLength, key.length());
  }

  Collection<NodeMeta<T>> values() {
    return table.values();
  }

  Set<Map.Entry<String, NodeMeta<T>>> entrySet() {
    return table.entrySet();
  }

  void handleSplitNodes(String newAnchorKey, LeafNode<T> newLeafNode) {
    NodeMetaLeaf<T> newNodeMeta = new NodeMetaLeaf<>(newAnchorKey, newLeafNode);
    NodeMeta<T> existingNodeMeta = get(newAnchorKey);
    if (existingNodeMeta != null) {
      throw new AssertionError(
          String.format(
              "There is a node meta that has the same key. Key: %s, Node meta: %s",
              newAnchorKey, existingNodeMeta));
    }
    put(newAnchorKey, newNodeMeta);

    // Update node meta that have a shorter anchor prefix.
    for (int prefixLen = 0; prefixLen < newAnchorKey.length(); prefixLen++) {
      String prefix = newAnchorKey.substring(0, prefixLen);
      NodeMeta<T> node = table.get(prefix);
      if (node == null) {
        put(
            prefix,
            new NodeMetaInternal<>(
                prefix, newLeafNode, newLeafNode, newAnchorKey.charAt(prefixLen)));
        continue;
      }

      if (node instanceof NodeMetaLeaf) {
        LeafNode<T> leafNode = ((NodeMetaLeaf<T>) node).leafNode;

        // If there is a leaf node which has the same prefix, append the smallest token to the
        // prefix of the leaf node and add an internal node with the same prefix instead of the
        // original leaf node.
        String prefixWithSmallestToken = prefix + Wormhole.SMALLEST_TOKEN;
        NodeMetaLeaf<T> updatedNode = new NodeMetaLeaf<>(prefixWithSmallestToken, leafNode);
        put(prefixWithSmallestToken, updatedNode);

        NodeMetaInternal<T> parent =
            new NodeMetaInternal<>(
                prefix, leafNode, leafNode, Wormhole.BITMAP_INDEX_OF_SMALLEST_TOKEN);
        put(prefix, parent);

        node = parent;
      }

      assert node instanceof NodeMetaInternal;

      NodeMetaInternal<T> internalNode = (NodeMetaInternal<T>) node;

      // The pseudocode in the paper doesn't update existing internal nodes' bitmap. However, it's
      // necessary.
      internalNode.bitmap.set(newAnchorKey.charAt(prefixLen));

      // The pseudocode in the paper checks and updates the original leaf node. However, this
      // loop traverses the ancestors of the new leaf node's anchor key, and the ancestors'
      // left-most and right-most ranges are updated to include the new leaf node.
      // Therefore, the new leaf node should be checked and set if needed.
      if (internalNode.getLeftMostLeafNode() == newLeafNode.getRight()) {
        internalNode.setLeftMostLeafNode(newLeafNode);
      }
      if (internalNode.getRightMostLeafNode() == newLeafNode.getLeft()) {
        internalNode.setRightMostLeafNode(newLeafNode);
      }
    }
  }

  NodeMeta<T> searchLongestPrefixMatch(String searchKey) {
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
  NodeMetaLeaf<T> findNodeMetaLeaf(String anchorKey) {
    {
      NodeMeta<T> nodeMeta = table.get(anchorKey);
      if (nodeMeta instanceof NodeMetaLeaf) {
        return (NodeMetaLeaf<T>) nodeMeta;
      }
    }

    {
      String anchorKeyWithSmallestToken = anchorKey + Wormhole.SMALLEST_TOKEN;
      NodeMeta<T> nodeMeta = table.get(anchorKeyWithSmallestToken);
      if (nodeMeta instanceof NodeMetaLeaf) {
        return (NodeMetaLeaf<T>) nodeMeta;
      }
    }

    return null;
  }

  @Nullable
  NodeMetaInternal<T> findNodeMetaInternal(String anchorKey) {
    {
      NodeMeta<T> nodeMeta = table.get(anchorKey);
      if (nodeMeta instanceof NodeMetaInternal) {
        return (NodeMetaInternal<T>) nodeMeta;
      }
    }

    {
      String anchorKeyWithSmallestToken = anchorKey + Wormhole.SMALLEST_TOKEN;
      NodeMeta<T> nodeMeta = table.get(anchorKeyWithSmallestToken);
      if (nodeMeta instanceof NodeMetaInternal) {
        return (NodeMetaInternal<T>) nodeMeta;
      }
    }

    return null;
  }

  String removeNodeMetaLeaf(String origAnchorKey) {
    NodeMeta<T> nodeMeta = table.get(origAnchorKey);
    String anchorKey =
        nodeMeta instanceof NodeMetaLeaf ? origAnchorKey : origAnchorKey + Wormhole.SMALLEST_TOKEN;

    NodeMeta<T> removed = table.remove(anchorKey);
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
    NodeMeta<T> nodeMeta = table.get(origAnchorKey);
    String anchorKey =
        nodeMeta instanceof NodeMetaInternal
            ? origAnchorKey
            : origAnchorKey + Wormhole.SMALLEST_TOKEN;

    NodeMeta<T> removed = table.remove(anchorKey);
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
