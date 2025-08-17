package org.komamitsu.wormhole;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetaTrieHashTableThreadSafe<T> extends MetaTrieHashTable<T> {
  @Override
  Map<String, NodeMeta<T>> createTable() {
    return new ConcurrentHashMap<>();
  }
}
