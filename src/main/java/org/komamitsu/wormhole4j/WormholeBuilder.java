/*
 * Copyright 2026 Mitsunori Komatsu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

  protected void validate() {
    if (isConcurrent && isDebugMode) {
      throw new IllegalArgumentException("Both 'concurrent' and 'debugMode' cannot be enabled");
    }
  }

  public abstract W build();

  public static class ForIntKey<V>
      extends WormholeBuilder<Wormhole<Integer, V>, ForIntKey<V>, Integer, V> {
    @Override
    protected ForIntKey<V> self() {
      return this;
    }

    @Override
    public Wormhole<Integer, V> build() {
      validate();
      if (isConcurrent) {
        return new ConcurrentWormholeForIntKey<>(leafNodeSize);
      } else {
        return new WormholeForIntKey<>(leafNodeSize, isDebugMode);
      }
    }
  }

  public static class ForLongKey<V>
      extends WormholeBuilder<Wormhole<Long, V>, ForLongKey<V>, Long, V> {
    @Override
    protected ForLongKey<V> self() {
      return this;
    }

    @Override
    public Wormhole<Long, V> build() {
      validate();
      if (isConcurrent) {
        return new ConcurrentWormholeForLongKey<>(leafNodeSize);
      } else {
        return new WormholeForLongKey<>(leafNodeSize, isDebugMode);
      }
    }
  }

  public static class ForStringKey<V>
      extends WormholeBuilder<Wormhole<String, V>, ForStringKey<V>, String, V> {
    @Override
    protected ForStringKey<V> self() {
      return this;
    }

    @Override
    public Wormhole<String, V> build() {
      validate();
      if (isConcurrent) {
        return new ConcurrentWormholeForStringKey<>(leafNodeSize);
      } else {
        return new WormholeForStringKey<>(leafNodeSize, isDebugMode);
      }
    }
  }
}
