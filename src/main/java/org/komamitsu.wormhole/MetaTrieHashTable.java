package org.komamitsu.wormhole;

import java.util.HashMap;

public class MetaTrieHashTable extends HashMap<String, MetaTrieHashTable.Item> {
  public enum ItemType {
    INTERNAL, LEAF;
  }

  public static class Item {
    private final ItemType itemType;
    private final int leftMostLeafIndex;
    private final int rightMostLeafIndex;
    private final String anchorPrefix;
    private final byte[] bitmap = new byte[16];

    public Item(ItemType itemType, int leftMostLeafIndex, int rightMostLeafIndex, String anchorPrefix) {
      this.itemType = itemType;
      this.leftMostLeafIndex = leftMostLeafIndex;
      this.rightMostLeafIndex = rightMostLeafIndex;
      this.anchorPrefix = anchorPrefix;
    }
  }

  public Item searchLongestPrefixMatchItem(String searchKey) {
    String lpm = searchLongestPrefixMatch(searchKey);
    return get(lpm);
  }
  
  private String searchLongestPrefixMatch(String searchKey) {
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
