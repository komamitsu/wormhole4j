package org.komamitsu.wormhole;


import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MetaTrieHashTableThreadSafe<T> extends MetaTrieHashTable<T> {
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  @Override
  NodeMeta<T> get(String key) {
    try {
      lock.readLock().lock();
      return super.get(key);
    }
    finally {
      lock.readLock().unlock();
    }
  }

  @Override
  void put(String key, NodeMeta<T> nodeMeta) {
    try {
      lock.writeLock().lock();
      super.put(key, nodeMeta);
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  NodeMeta<T> searchLongestPrefixMatch(String searchKey) {
    try {
      lock.readLock().lock();
      return super.searchLongestPrefixMatch(searchKey);
    }
    finally {
      lock.readLock().unlock();
    }
  }

  @Override
  void lock() {
    lock.writeLock().lock();
  }

  @Override
  void unlock() {
    lock.writeLock().unlock();
  }
}
