package org.komamitsu.wormhole;

import javax.annotation.Nullable;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;

class LeafNodeThreadSafe<T> extends LeafNode<T> {
  private final ReentrantLock lock = new ReentrantLock();

  LeafNodeThreadSafe(String anchorKey, int maxSize, @Nullable LeafNode<T> left, @Nullable LeafNode<T> right) {
    super(anchorKey, maxSize, left, right);
  }

  @Override
  void add(String key, T value) {
    try {
      lock.lock();
      super.add(key, value);
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  void incSort() {
    try {
      lock.lock();
      super.incSort();
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  boolean delete(String key) {
    try {
      lock.lock();
      return super.delete(key);
    }
    finally {
      lock.unlock();
    }
  }

  @Nullable
  @Override
  KeyValue<T> pointSearchLeaf(String key) {
    try {
      lock.lock();
      return super.pointSearchLeaf(key);
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  boolean iterateKeyValues(@Nullable String startKey, @Nullable String endKey, boolean isEndKeyExclusive, Function<KeyValue<T>, Boolean> function) {
    try {
      lock.lock();
      return super.iterateKeyValues(startKey, endKey, isEndKeyExclusive, function);
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  void splitIfNeededAndAdd(String key, T value, Function<String, String> validNewAnchorFunction, BiFunction<String, LeafNode<T>, Void> splitCallback) {
    try {
      lock.lock();
      super.splitIfNeededAndAdd(key, value, validNewAnchorFunction, splitCallback);
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  void splitAndAdd(String key, T value, Function<String, String> validNewAnchorFunction, BiFunction<String, LeafNode<T>, Void> splitCallback) {
    try {
      lock.lock();
      super.splitAndAdd(key, value, validNewAnchorFunction, splitCallback);
    }
    finally {
      lock.unlock();
    }
  }
}
