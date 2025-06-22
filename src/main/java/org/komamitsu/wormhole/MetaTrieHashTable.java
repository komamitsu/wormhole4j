package org.komamitsu.wormhole;

import javax.annotation.Nullable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

class MetaTrieHashTable<T> {
  private final Map<String, NodeMeta<T>> table = new HashMap<>();

  static abstract class NodeMeta<T> {
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
  }

  static class NodeMetaInternal<T> extends NodeMeta<T> {
    @Nullable
    private LeafNode<T> leftMostLeafNode;
    @Nullable
    private LeafNode<T> rightMostLeafNode;
    final BitSet bitmap;

    NodeMetaInternal(String anchorPrefix, LeafNode<T> leftMostLeafNode, LeafNode<T> rightMostLeafNode, char initBitId) {
      super(anchorPrefix);
      this.leftMostLeafNode = leftMostLeafNode;
      this.rightMostLeafNode = rightMostLeafNode;
      this.bitmap = new BitSet();
      bitmap.set(initBitId);
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

    @Nullable
    LeafNode<T> getLeftMostLeafNode() {
      return leftMostLeafNode;
    }

    @Nullable
    LeafNode<T> getRightMostLeafNode() {
      return rightMostLeafNode;
    }

    void setLeftMostLeafNode(@Nullable LeafNode<T> leftMostLeafNode) {
      this.leftMostLeafNode = leftMostLeafNode;
    }

    void setRightMostLeafNode(@Nullable LeafNode<T> rightMostLeafNode) {
      this.rightMostLeafNode = rightMostLeafNode;
    }
  }

  NodeMeta<T> get(String key) {
    return table.get(key);
  }

  void put(String key, NodeMeta<T> nodeMeta) {
    table.put(key, nodeMeta);
  }

  void handleSplitNodes(String key, LeafNode<T> origLeafNode, LeafNode<T> newLeafNode) {
    NodeMetaLeaf<T> newNodeMeta = new NodeMetaLeaf<>(key, newLeafNode);
    put(key, newNodeMeta);

    // Update node meta that have a shorter anchor prefix.
    for (int prefixLen = 0; prefixLen < key.length(); prefixLen++) {
      String prefix = key.substring(0, prefixLen);
      NodeMeta<T> node = table.get(prefix);
      if (node == null) {
        table.put(prefix, new NodeMetaInternal<>(prefix, newLeafNode, newLeafNode, key.charAt(prefixLen)));
        continue;
      }

      if (node instanceof NodeMetaLeaf) {
        LeafNode<T> leafNode = ((NodeMetaLeaf<T>) node).leafNode;

        // If there is a leaf node which has the same prefix, append the smallest token to the prefix of the leaf node
        // and add an internal node with the same prefix instead of the original leaf node.
        String prefixWithSmallestToken = prefix + Wormhole.SMALLEST_TOKEN;
        NodeMetaLeaf<T> updatedNode = new NodeMetaLeaf<>(prefixWithSmallestToken, leafNode);
        table.put(prefixWithSmallestToken, updatedNode);

        NodeMetaInternal<T> parent = new NodeMetaInternal<T>(prefix, leafNode, leafNode, Wormhole.BITMAP_ID_OF_SMALLEST_TOKEN);
        table.put(prefix, parent);
      }
      else {
        assert node instanceof NodeMetaInternal;

        NodeMetaInternal<T> internalNode = (NodeMetaInternal<T>) node;

        if (internalNode.getLeftMostLeafNode() == origLeafNode.getRight()) {
          internalNode.setLeftMostLeafNode(origLeafNode);
        }
        if (internalNode.getRightMostLeafNode() == origLeafNode.getLeft()) {
          internalNode.setRightMostLeafNode(origLeafNode);
        }
      }
      return;
    }

    throw new RuntimeException(
        String.format("Failed to handle split nodes. Key: %s, Original leaf node: %s, New leaf node: %s",
            key, origLeafNode, newLeafNode));
  }

  NodeMeta<T> searchLongestPrefixMatch(String searchKey) {
    String lpm = searchLongestPrefixMatchKey(searchKey);
    return table.get(lpm);
  }
  
  private String searchLongestPrefixMatchKey(String searchKey) {
    int m = 0;
    int n = Math.min(searchKey.length(), maxAnchorLength()) + 1;
    while (m + 1 < n) {
      int prefixLen = (m + n) / 2;
      if (table.containsKey(searchKey.substring(0, prefixLen - 1))) {
        m = prefixLen;
      }
      else {
        n = prefixLen;
      }
    }
    return searchKey.substring(0, m - 1);
  }

  // TODO: Memoize
  private int maxAnchorLength() {
    int max = 0;
    for (String key : table.keySet()) {
      if (max < key.length()) {
        max = key.length();
      }
    }
    return max;
  }
}
