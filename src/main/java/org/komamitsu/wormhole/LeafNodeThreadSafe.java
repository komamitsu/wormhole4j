package org.komamitsu.wormhole;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import javax.annotation.Nullable;

class LeafNodeThreadSafe<T> extends LeafNode<T> {
  private final ReentrantLock lock = new ReentrantLock();

  LeafNodeThreadSafe(
      String anchorKey, int maxSize, @Nullable LeafNode<T> left, @Nullable LeafNode<T> right) {
    super(anchorKey, maxSize, left, right);
  }

  @Override
  void add(String key, T value) {
    try {
      lock.lock();
      super.add(key, value);
    } finally {
      lock.unlock();
    }
  }

  @Override
  void incSort() {
    try {
      lock.lock();
      super.incSort();
    } finally {
      lock.unlock();
    }
  }

  @Override
  boolean delete(String key) {
    try {
      lock.lock();
      return super.delete(key);
    } finally {
      lock.unlock();
    }
  }

  @Nullable
  @Override
  KeyValue<T> pointSearchLeaf(String key) {
    try {
      lock.lock();
      return super.pointSearchLeaf(key);
    } finally {
      lock.unlock();
    }
  }

  @Override
  boolean iterateKeyValues(
      @Nullable String startKey,
      @Nullable String endKey,
      boolean isEndKeyExclusive,
      Function<KeyValue<T>, Boolean> function) {
    try {
      lock.lock();
      return super.iterateKeyValues(startKey, endKey, isEndKeyExclusive, function);
    } finally {
      lock.unlock();
    }
  }
}
