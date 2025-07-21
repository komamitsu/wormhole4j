package org.komamitsu.wormhole;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

class MetaTrieHashTable<T> {
  // Visible for testing
  final Map<String, NodeMeta<T>> table = new HashMap<>();

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

    @Override
    public String toString() {
      return "NodeMetaLeaf{" +
          "leafNode=" + leafNode +
          '}';
    }
  }

  static class NodeMetaInternal<T> extends NodeMeta<T> {
    private LeafNode<T> leftMostLeafNode;
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
      return "NodeMetaInternal{" +
          "leftMostLeafNode=" + leftMostLeafNode +
          ", rightMostLeafNode=" + rightMostLeafNode +
          ", bitmap=" + bitmap +
          '}';
    }
  }

  NodeMeta<T> get(String key) {
    return table.get(key);
  }

  void put(String key, NodeMeta<T> nodeMeta) {
    table.put(key, nodeMeta);
  }

  void handleSplitNodes(String key, LeafNode<T> newLeafNode) {
    NodeMetaLeaf<T> newNodeMeta = new NodeMetaLeaf<>(key, newLeafNode);
    NodeMeta<T> existingNodeMeta = get(key);
    if (existingNodeMeta != null) {
      throw new AssertionError(
          String.format(
              "There is a node meta that has the same key. Key: %s, Node meta: %s", key, existingNodeMeta));
    }
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

        NodeMetaInternal<T> parent = new NodeMetaInternal<>(prefix, leafNode, leafNode, Wormhole.BITMAP_ID_OF_SMALLEST_TOKEN);
        table.put(prefix, parent);

        node = parent;
      }

      assert node instanceof NodeMetaInternal;

      NodeMetaInternal<T> internalNode = (NodeMetaInternal<T>) node;

      // The pseudocode of the 'split()` function on the paper doesn't update existing internal nodes' bitmap.
      // However, it's necessary.
      internalNode.bitmap.set(key.charAt(prefixLen));

      // Note that the pseudocode on the paper checks and update the original leaf node, but I think it should be the
      // new leaf node.
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
    int n = Math.min(searchKey.length(), maxAnchorLength()) + 1;
    while (m + 1 < n) {
      int prefixLen = (m + n) / 2;
      if (table.containsKey(searchKey.substring(0, prefixLen))) {
        m = prefixLen;
      }
      else {
        n = prefixLen;
      }
    }
    return searchKey.substring(0, m);
  }

  void removeLeafNodeMeta(String anchorKey) {
    NodeMeta<T> removed = table.remove(anchorKey);
    if (!(removed instanceof NodeMetaLeaf)) {
      throw new AssertionError(
          String.format(
              "Removed node meta is an unexpected type. Expected: %s, Actual: %s",
              NodeMetaLeaf.class.getName(), removed.getClass().getName()));
    }
  }

  void removeInternalNodeMeta(String anchorKey) {
    NodeMeta<T> removed = table.remove(anchorKey);
    if (!(removed instanceof NodeMetaInternal)) {
      throw new AssertionError(
          String.format(
              "Removed node meta is an unexpected type. Expected: %s, Actual: %s",
              NodeMetaInternal.class.getName(), removed.getClass().getName()));
    }
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

  @Override
  public String toString() {
    return "MetaTrieHashTable{" +
        "table=" + table +
        '}';
  }
}
