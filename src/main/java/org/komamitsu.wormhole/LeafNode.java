package org.komamitsu.wormhole;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class LeafNode<T> {
  private static class Entry<T> {
    private final short hashTag;
    private final String key;
    private final T value;

    public Entry(short hashTag, String key, T value) {
      this.hashTag = hashTag;
      this.key = key;
      this.value = value;
    }
  }

  private final List<Entry<T>> entries;

  public LeafNode(int size) {
    entries = new ArrayList<>(size);
  }

  @Nullable
  public T pointSearchLeaf(String key) {
    int keyHash = 0x7FFF & key.hashCode();
    int leafSize = entries.size();
    int i = keyHash * leafSize / (Short.MAX_VALUE + 1);
    while (i > 0 && keyHash <= entries.get(i - 1).hashTag) {
      i--;
    }
    while (i < leafSize && entries.get(i).hashTag < keyHash) {
      i++;
    }
    while (i < leafSize && entries.get(i).hashTag == keyHash) {
      Entry<T> entry = entries.get(i);
      if (entry.key.equals(key)) {
        return entry.value;
      }
      i++;
    }
    return null;
  }

  public void sort() {
    // TODO: Implement `incSort()`.
    entries.sort(Comparator.comparingInt(a -> a.hashTag));
  }
}
