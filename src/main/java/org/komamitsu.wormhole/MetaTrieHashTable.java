package org.komamitsu.wormhole;

import java.util.BitSet;
import java.util.HashMap;

class MetaTrieHashTable extends HashMap<String, MetaTrieHashTable.NodeMeta> {
  enum NodeType {
    INTERNAL, LEAF;
  }

  public static class NodeMeta {
    final NodeType type;
    final int leftMostLeafIndex;
    final int rightMostLeafIndex;
    final String anchorPrefix;
    final BitSet bitmap;

    NodeMeta(NodeType type, int leftMostLeafIndex, int rightMostLeafIndex, String anchorPrefix, BitSet bitmap) {
      this.type = type;
      this.leftMostLeafIndex = leftMostLeafIndex;
      this.rightMostLeafIndex = rightMostLeafIndex;
      this.anchorPrefix = anchorPrefix;
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

  NodeMeta searchLongestPrefixMatch(String searchKey) {
    String lpm = searchLongestPrefixMatchKey(searchKey);
    return get(lpm);
  }
  
  private String searchLongestPrefixMatchKey(String searchKey) {
    int m = 0;
    int n = Math.min(searchKey.length(), maxAnchorLength()) + 1;
    while (m + 1 < n) {
      int prefixLen = (m + n) / 2;
      if (keySet().contains(searchKey.substring(0, prefixLen - 1))) {
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
    for (String key : keySet()) {
      if (max < key.length()) {
        max = key.length();
      }
    }
    return max;
  }
}
