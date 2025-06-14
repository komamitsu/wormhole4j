package org.komamitsu.wormhole;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

class MetaTrieHashTable {
  private final Map<String, NodeMeta> table = new HashMap<>();

  static abstract class NodeMeta {
    final String anchorPrefix;

    public NodeMeta(String anchorPrefix) {
      this.anchorPrefix = anchorPrefix;
    }
  }

  static class NodeMetaLeaf extends NodeMeta {
    final int leafIndex;

    public NodeMetaLeaf(String anchorPrefix, int leafIndex) {
      super(anchorPrefix);
      this.leafIndex = leafIndex;
    }
  }

  static class NodeMetaInternal extends NodeMeta {
    final int leftMostLeafIndex;
    final int rightMostLeafIndex;
    final BitSet bitmap;

    NodeMetaInternal(String anchorPrefix, int leftMostLeafIndex, int rightMostLeafIndex, char initBitId) {
      super(anchorPrefix);
      this.leftMostLeafIndex = leftMostLeafIndex;
      this.rightMostLeafIndex = rightMostLeafIndex;
      this.bitmap = new BitSet(initBitId);
    }

    NodeMetaInternal(String anchorPrefix, int leftMostLeafIndex, int rightMostLeafIndex, BitSet bitmap) {
      super(anchorPrefix);
      this.leftMostLeafIndex = leftMostLeafIndex;
      this.rightMostLeafIndex = rightMostLeafIndex;
      this.bitmap = bitmap;
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
  }

  NodeMeta get(String key) {
    return table.get(key);
  }

  void put(String key, NodeMeta nodeMeta, int nodeIndex) {
    table.put(key, nodeMeta);

    // Update node meta that have a shorter anchor prefix.
    for (int prefixLen = 0; prefixLen < key.length(); prefixLen++) {
      String prefix = key.substring(0, prefixLen);
      NodeMeta node = table.get(prefix);
      if (node == null) {

        continue;
      }

      if (node instanceof NodeMetaLeaf) {
        int leafIndex = ((NodeMetaLeaf) node).leafIndex;

        // If there is a leaf node which has the same prefix, append the smallest token to the prefix of the leaf node
        // and add an internal node with the same prefix instead of the original leaf node.
        String prefixWithSmallestToken = prefix + Wormhole.SMALLEST_TOKEN;
        NodeMetaLeaf updatedNode = new NodeMetaLeaf(prefixWithSmallestToken, leafIndex);
        table.put(prefixWithSmallestToken, updatedNode);

        // TODO: Define '\0'.
        NodeMetaInternal parent = new NodeMetaInternal(prefix, leafIndex, leafIndex, '\0');
        table.put(prefix, parent);
      }
      else {
        assert node instanceof NodeMetaInternal;

        NodeMetaInternal internalNode = (NodeMetaInternal) node;
        int leftMostLeafIndex = internalNode.leftMostLeafIndex;
        int rightMostLeafIndex = internalNode.rightMostLeafIndex;
        if (leftMostLeafIndex == nodeIndex + 1) {
          leftMostLeafIndex = nodeIndex;
        }
        if (rightMostLeafIndex == nodeIndex - 1) {
          rightMostLeafIndex = nodeIndex;
        }
        if (internalNode.leftMostLeafIndex != leftMostLeafIndex || internalNode.rightMostLeafIndex == rightMostLeafIndex) {
          table.put(prefix, new NodeMetaInternal(internalNode.anchorPrefix, leftMostLeafIndex, rightMostLeafIndex, internalNode.bitmap));
        }
      }
    }
  }

  NodeMeta searchLongestPrefixMatch(String searchKey) {
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
