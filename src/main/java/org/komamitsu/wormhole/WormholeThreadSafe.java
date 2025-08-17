package org.komamitsu.wormhole;

import javax.annotation.Nullable;

public class WormholeThreadSafe<T> extends Wormhole<T> {
  public WormholeThreadSafe() {
    super();
  }

  public WormholeThreadSafe(int leafNodeSize) {
    super(leafNodeSize);
  }

  public WormholeThreadSafe(int leafNodeSize, boolean debugMode) {
    super(leafNodeSize, debugMode);
  }

  @Override
  MetaTrieHashTable<T> createMetaTrieHashTable() {
    return new MetaTrieHashTableThreadSafe<>();
  }

  @Override
  LeafNode<T> createLeafNode(
      String anchorKey, int maxSize, @Nullable LeafNode<T> left, @Nullable LeafNode<T> right) {
    return new LeafNodeThreadSafe<>(anchorKey, maxSize, left, right);
  }
}
