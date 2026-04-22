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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;
import javax.annotation.Nullable;

class ConcurrentLeafNode<K, V> extends LeafNode<K, V> {
  private final StampedLock lock = new StampedLock();
  private Long initialLockStamp;
  private long version;

  ConcurrentLeafNode(
      EncodedKeyType encodedKeyType,
      Function<Object, Object> validAnchorKeyProvider,
      Object anchorKey,
      int maxSize,
      @Nullable LeafNode<K, V> left,
      @Nullable LeafNode<K, V> right) {
    super(encodedKeyType, validAnchorKeyProvider, anchorKey, maxSize, left, right);
  }

  @Override
  long acquireWriteLock() {
    return lock.writeLock();
  }

  @Override
  long acquireReadLock() {
    return lock.readLock();
  }

  @Override
  long tryReadLock() {
    try {
      // TODO: Consider no wait.
      return lock.tryReadLock(5, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  long tryWriteLock() {
    try {
      // TODO: Consider no wait.
      return lock.tryWriteLock(5, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  void releaseLock(long stamp) {
    this.lock.unlock(stamp);
  }

  @Override
  long getInitialLockStamp() {
    return initialLockStamp;
  }

  @Override
  long getVersion() {
    return version;
  }

  @Override
  void setVersion(long version) {
    this.version = version;
  }

  @Override
  protected LeafNode<K, V> createLeafNode(
      EncodedKeyType encodedKeyType,
      Function<Object, Object> validAnchorKeyProvider,
      Object anchorKey,
      int maxSize,
      @Nullable LeafNode<K, V> left,
      @Nullable LeafNode<K, V> right) {
    ConcurrentLeafNode<K, V> leafNode =
        new ConcurrentLeafNode<>(
            encodedKeyType, validAnchorKeyProvider, anchorKey, maxSize, left, right);
    leafNode.initialLockStamp = leafNode.acquireWriteLock();
    return leafNode;
  }
}
