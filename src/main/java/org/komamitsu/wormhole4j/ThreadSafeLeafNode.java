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

import static java.util.concurrent.locks.ReentrantReadWriteLock.*;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nullable;

class ThreadSafeLeafNode<K, V> extends LeafNode<K, V> {
  private long version;
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  ThreadSafeLeafNode(
      EncodedKeyType encodedKeyType,
      Function<Object, Object> validAnchorKeyProvider,
      Object anchorKey,
      int maxSize,
      @Nullable LeafNode<K, V> left,
      @Nullable LeafNode<K, V> right) {
    super(encodedKeyType, validAnchorKeyProvider, anchorKey, maxSize, left, right);
  }

  long getVersion() {
    return version;
  }

  void setVersion(long version) {
    this.version = version;
  }

  @Nullable
  @Override
  LeafNode<K, V> getLeft() {
    ReadLock readLock = lock.readLock();
    readLock.lock();
    try {
      return super.getLeft();
    } finally {
      readLock.unlock();
    }
  }

  @Nullable
  @Override
  LeafNode<K, V> getRight() {
    ReadLock readLock = lock.readLock();
    readLock.lock();
    try {
      return super.getRight();
    } finally {
      readLock.unlock();
    }
  }

  @Nullable
  @Override
  V lookupAndSetValue(Object encodedKey, V newValue) {
    WriteLock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      return super.lookupAndSetValue(encodedKey, newValue);
    } finally {
      writeLock.unlock();
    }
  }

  @Nullable
  @Override
  V lookupValue(Object encodedKey) {
    ReadLock readLock = lock.readLock();
    readLock.lock();
    try {
      return super.lookupValue(encodedKey);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  void incSort() {
    WriteLock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      super.incSort();
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  Tuple<Object, LeafNode<K, V>> splitToNewLeafNode() {
    WriteLock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      return super.splitToNewLeafNode();
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  int size() {
    ReadLock readLock = lock.readLock();
    readLock.lock();
    try {
      return super.size();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  boolean iterateKeyValues(
      @Nullable Object startKey,
      @Nullable Object endKey,
      boolean isEndKeyExclusive,
      BiFunction<K, V, Boolean> function) {
    ReadLock readLock = lock.readLock();
    readLock.lock();
    try {
      return super.iterateKeyValues(startKey, endKey, isEndKeyExclusive, function);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  void add(Object encodedKey, K key, V value) {
    WriteLock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      super.add(encodedKey, key, value);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  boolean delete(Object key) {
    WriteLock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      return super.delete(key);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  void merge(LeafNode<K, V> right) {
    WriteLock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      super.merge(right);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  protected LeafNode<K, V> createLeafNode(
      EncodedKeyType encodedKeyType,
      Function<Object, Object> validAnchorKeyProvider,
      Object anchorKey,
      int maxSize,
      @Nullable LeafNode<K, V> left,
      @Nullable LeafNode<K, V> right) {
    return new ThreadSafeLeafNode<>(
        encodedKeyType, validAnchorKeyProvider, anchorKey, maxSize, left, right);
  }
}
