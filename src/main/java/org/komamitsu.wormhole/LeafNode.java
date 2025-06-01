package org.komamitsu.wormhole;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class LeafNode<T> {
  private final short[] hashTag;
  private final String[] keys;
  private final List<T> values;

  public LeafNode(int size) {
    this.hashTag = new short[size];
    this.keys = new String[size];
    this.values = new ArrayList<>(size);
  }

  @Nullable
  public T pointSearchLeaf(String key) {
    int keyHash = 0x7FFF & key.hashCode();
    int leafSize = hashTag.length;
    int i = keyHash * leafSize / (Short.MAX_VALUE + 1);
    while (i > 0 && keyHash <= hashTag[i - 1]) {
      i--;
    }
    while (i < leafSize && hashTag[i] < keyHash) {
      i++;
    }
    while (i < leafSize && hashTag[i] == keyHash) {
      if (keys[i].equals(key)) {
        return values.get(i);
      }
      i++;
    }
    return null;
  }
}
