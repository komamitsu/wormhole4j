package org.komamitsu.wormhole;

public class Wormhole<T> {
  private final MetaTrieHashTable table = new MetaTrieHashTable();
  private final LeafList<T> leafList = new LeafList<>();

  public void searchTrieHashTable(String key) {
    MetaTrieHashTable.Item item = table.searchLongestPrefixMatchItem(key);
  }
}
