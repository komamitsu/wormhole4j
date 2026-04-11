package org.komamitsu.wormhole4j;

public abstract class WormholeBuilder<
    W extends Wormhole<K, V>, B extends WormholeBuilder<W, B, K, V>, K, V> {
  protected boolean isConcurrent = false;
  protected boolean isDebugMode = false;
  protected int leafNodeSize = Wormhole.DEFAULT_LEAF_NODE_SIZE;

  protected abstract B self();

  public B setConcurrent(boolean isConcurrent) {
    this.isConcurrent = isConcurrent;
    return self();
  }

  public B setDebugMode(boolean isDebugMode) {
    this.isDebugMode = isDebugMode;
    return self();
  }

  public B setLeafNodeSize(int leafNodeSize) {
    this.leafNodeSize = leafNodeSize;
    return self();
  }

  abstract W build();
}
