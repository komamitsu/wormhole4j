/*
 * Copyright 2025 Mitsunori Komatsu
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

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.komamitsu.wormhole4j.MetaTrieHashTable.NodeMetaInternal;
import org.komamitsu.wormhole4j.MetaTrieHashTable.NodeMetaLeaf;

import static java.util.concurrent.locks.ReentrantReadWriteLock.*;

/**
 * Wormhole is an in-memory ordered index for key-value pairs.
 *
 * <p>This implementation supports fast lookups, inserts, deletes, and range scans. Keys are {@link
 * String} only.
 *
 * @param <K> the type of keys stored in this index
 * @param <V> the type of values stored in this index
 */
abstract class Wormhole<K, V> {
  private static final int DEFAULT_LEAF_NODE_SIZE = 128;
  private final EncodedKeyType encodedKeyType;
  private final boolean isThreadSafe;
  private final MetaTrieHashTable<K, V> table;
  private final int leafNodeSize;
  private final int leafNodeMergeSize;
  private final Function<Object, Object> validAnchorKeyProvider;
  private final ThreadLocal<Collection<ThreadSafeLeafNode<K, V>>> leafNodeTracer =
      ThreadLocal.withInitial(ArrayList::new);
  @Nullable private final Validator<K, V> validator;

  /**
   * Creates a Wormhole.
   *
   * @param encodedKeyType the encoded key type
   */
  protected Wormhole(EncodedKeyType encodedKeyType) {
    this(encodedKeyType, true);
  }

  /**
   * Creates a Wormhole.
   *
   * @param encodedKeyType the encoded key type
   * @param isThreadSafe whether thread-safe is enabled
   */
  protected Wormhole(EncodedKeyType encodedKeyType, boolean isThreadSafe) {
    this(encodedKeyType, isThreadSafe, DEFAULT_LEAF_NODE_SIZE);
  }

  /**
   * Creates a Wormhole.
   *
   * @param encodedKeyType the encoded key type
   * @param isThreadSafe whether thread-safe is enabled
   * @param leafNodeSize maximum number of entries in a leaf node
   */
  protected Wormhole(EncodedKeyType encodedKeyType, boolean isThreadSafe, int leafNodeSize) {
    this(encodedKeyType, isThreadSafe, leafNodeSize, false);
  }

  /**
   * Creates a Wormhole.
   *
   * @param encodedKeyType the encoded key type
   * @param leafNodeSize maximum number of entries in a leaf node
   */
  protected Wormhole(EncodedKeyType encodedKeyType, int leafNodeSize) {
    this(encodedKeyType, false, leafNodeSize, false);
  }

  /**
   * Creates a Wormhole.
   *
   * @param encodedKeyType the encoded key type
   * @param leafNodeSize maximum number of entries in a leaf node
   * @param debugMode enables internal consistency checks if {@code true}
   */
  protected Wormhole(EncodedKeyType encodedKeyType, int leafNodeSize, boolean debugMode) {
    this(encodedKeyType, false, leafNodeSize, debugMode);
  }
  /**
   * Creates a Wormhole.
   *
   * @param encodedKeyType the encoded key type
   * @param isThreadSafe whether thread-safe is enabled
   * @param leafNodeSize maximum number of entries in a leaf node
   * @param debugMode enables internal consistency checks if {@code true}
   */
  protected Wormhole(
      EncodedKeyType encodedKeyType, boolean isThreadSafe, int leafNodeSize, boolean debugMode) {
    this.encodedKeyType = encodedKeyType;
    this.isThreadSafe = isThreadSafe;
    this.leafNodeSize = leafNodeSize;
    this.leafNodeMergeSize = leafNodeSize * 3 / 4;
    this.table = new MetaTrieHashTable<>(encodedKeyType, isThreadSafe);
    validAnchorKeyProvider = this::provideValidAnchorKey;
    validator = debugMode ? new Validator<>(this) : null;
    initialize();
  }

  private void validateIfNeeded() {
    if (validator == null) {
      return;
    }
    validator.validate();
  }

  protected abstract Object createEncodedKey(K key);

  protected abstract Object createEmptyEncodedKey();

  /**
   * Inserts or updates a key-value pair.
   *
   * @param key the key (must not be {@code null})
   * @param value the value to associate with the key
   * @return the previous value associated with the key, or {@code null} if there was no previous
   *     mapping
   */
  @Nullable
  public V put(K key, V value) {
    Object encodedKey = createEncodedKey(key);
    boolean writeLockOnTable = false;
    while (true) {
      long tableLock = 0;
      if (isThreadSafe) {
        tableLock = writeLockOnTable ? table.acquireWriteLock() : table.acquireReadLock();
      }
      try {
        LeafNode<K, V> leafNode = searchTrieHashTable(encodedKey);
        long writeLockOnLeafNode = 0;
        if (isThreadSafe) {
          writeLockOnLeafNode = leafNode.acquireWriteLock();
        }
        try {
          Optional<V> existingValue = leafNode.lookupAndPutValue(encodedKey, key, value);
          if (existingValue != null) {
            validateIfNeeded();
            return existingValue.orElse(null);
          }

          // Retry with a write lock when the current lock is a read lock.
          if (!writeLockOnTable) {
            writeLockOnTable = true;
            continue;
          }

          // Split the node and get a new right leaf node.
          LeafNode<K, V> newLeafNode = split(leafNode);

          if (EncodedKeyUtils.compare(encodedKeyType, encodedKey, newLeafNode.anchorKey) < 0) {
            leafNode.add(encodedKey, key, value);
          } else {
            newLeafNode.add(encodedKey, key, value);
          }
          validateIfNeeded();
          return null;
        } finally{
          if (isThreadSafe) {
            leafNode.releaseLock(writeLockOnLeafNode);
          }
        }
      } finally {
        if (isThreadSafe) {
          table.releaseLock(tableLock);
        }
      }
    }
  }

  /**
   * Deletes a key-value pair if present.
   *
   * @param key the key (must not be {@code null})
   * @return {@code true} if the key was removed, {@code false} otherwise
   */
  public boolean delete(K key) {
    Object encodedKey = createEncodedKey(key);
    long tableLock = 0;
    if (isThreadSafe) {
      tableLock = table.acquireWriteLock();
    }
    try {
      LeafNode<K, V> leafNode = searchTrieHashTable(encodedKey);
      long writeLockOnLeafNode = 0;
      if (isThreadSafe) {
        writeLockOnLeafNode = leafNode.acquireWriteLock();
      }
      try {
        if (!leafNode.delete(encodedKey)) {
          return false;
        }

        if (leafNode.getLeft() != null
            && leafNode.size() + leafNode.getLeft().size() < leafNodeMergeSize) {
          merge(leafNode.getLeft(), leafNode);
        } else if (leafNode.getRight() != null
            && leafNode.size() + leafNode.getRight().size() < leafNodeMergeSize) {
          merge(leafNode, leafNode.getRight());
        }
      } finally {
        if (isThreadSafe) {
          leafNode.releaseLock(writeLockOnLeafNode);
        }
      }
    }
    finally {
      if (isThreadSafe) {
        table.releaseLock(tableLock);
      }
    }
    validateIfNeeded();
    return true;
  }

  /**
   * Retrieves the value associated with the specified key.
   *
   * @param key the key (must not be {@code null})
   * @return the value, or {@code null} if not found
   */
  @Nullable
  public V get(K key) {
    Object encodedKey = createEncodedKey(key);
    long tableLock = 0;
    if (isThreadSafe) {
      tableLock = table.acquireReadLock();
    }
    try {
      LeafNode<K, V> leafNode = searchTrieHashTable(encodedKey);
      long readLockOnLeafNode = 0;
      if (isThreadSafe) {
        readLockOnLeafNode = leafNode.acquireReadLock();
      }
      try {
        return leafNode.lookupValue(encodedKey);
      } finally {
        if (isThreadSafe) {
          leafNode.releaseLock(readLockOnLeafNode);
        }
      }
    }
    finally {
      if (isThreadSafe) {
        table.releaseLock(tableLock);
      }
    }
  }

  private void scan(
      @Nullable K startKey,
      @Nullable K endKey,
      boolean isEndKeyExclusive,
      @Nullable Integer count,
      BiFunction<K, V, Boolean> function) {
    Object encodedStartKey =
        startKey == null ? createEmptyEncodedKey() : createEncodedKey(startKey);
    Object encodedEndKey = endKey == null ? null : createEncodedKey(endKey);
    BiFunction<K, V, Boolean> actualFunction = function;
    if (count != null) {
      AtomicInteger counter = new AtomicInteger();
      actualFunction =
          (k, v) -> {
            if (counter.getAndIncrement() >= count) {
              return false;
            }
            return function.apply(k, v);
          };
    }

    long tableLock = 0;
    if (isThreadSafe) {
      tableLock = table.acquireReadLock();
    }
    try {
      List<LeafNode<K, V>> leafNodes = null;
      List<Long> readLocksOnLeafNodes = null;
      try {
        LeafNode<K, V> leafNode = searchTrieHashTable(encodedStartKey);
        while (leafNode != null) {
          if (isThreadSafe) {
            if (leafNodes == null) {
              leafNodes = new ArrayList<>();
              readLocksOnLeafNodes = new ArrayList<>();
            }
            leafNodes.add(leafNode);
            long readLock = leafNode.acquireReadLock();
            readLocksOnLeafNodes.add(readLock);
          }
          leafNode.incSort();
          if (!leafNode.iterateKeyValues(
              encodedStartKey,
              encodedEndKey,
              isEndKeyExclusive,
              actualFunction)) {
            return;
          }
          leafNode = leafNode.getRight();
          encodedStartKey = null;
        }
      } finally {
        if (leafNodes != null) {
          for (int i = 0; i < leafNodes.size(); i++) {
            leafNodes.get(i).releaseLock(readLocksOnLeafNodes.get(i));
          }
        }
      }
    }
    finally {
      if (isThreadSafe) {
        table.releaseLock(tableLock);
      }
    }
  }

  /**
   * Scans the key range.
   *
   * @param startKey the start key (inclusive)
   * @param endKey the end key
   * @param isEndKeyExclusive whether the end key is exclusive
   * @param function a function applied to each key-value pair; if it returns {@code true}, the scan
   *     continues, otherwise the scan stops
   */
  public void scanRange(
      @Nullable K startKey,
      @Nullable K endKey,
      boolean isEndKeyExclusive,
      BiFunction<K, V, Boolean> function) {
      scan(startKey, endKey, isEndKeyExclusive, null, function);
  }

  /**
   * Scans the index starting from a key and collects up to {@code count} pairs.
   *
   * @param startKey the start key (inclusive), or {@code null} to start from the beginning
   * @param count maximum number of results to return
   * @return a list of key-value pairs
   */
  public List<KeyValue<K, V>> scanWithCount(@Nullable K startKey, int count) {
    return scanWithCountInternal(startKey, count);
  }

  private List<KeyValue<K, V>> scanWithCountInternal(@Nullable K startKey, int count) {
    List<KeyValue<K, V>> result = new ArrayList<>(count);
    scan(
        startKey,
        null, /* Not used */
        false,
        count,
        (k, v) -> {
          result.add(new KeyValue<>(k, v));
          return true;
        });
    return result;
  }

  private LeafNode<K, V> createLeafNode(
      EncodedKeyType encodedKeyType,
      Function<Object, Object> validAnchorKeyProvider,
      Object anchorKey,
      int maxSize,
      @Nullable LeafNode<K, V> leftNode,
      @Nullable LeafNode<K, V> rightNode) {
    LeafNode<K, V> leafNode;
    if (isThreadSafe) {
      leafNode =
          new ThreadSafeLeafNode<>(
              encodedKeyType, validAnchorKeyProvider, anchorKey, maxSize, leftNode, rightNode);
    } else {
      leafNode =
          new LeafNode<>(
              encodedKeyType, validAnchorKeyProvider, anchorKey, maxSize, leftNode, rightNode);
    }
    return leafNode;
  }

  private void initialize() {
    Object key = createEmptyEncodedKey();
    LeafNode<K, V> rootLeafNode = createLeafNode(
        encodedKeyType, validAnchorKeyProvider, key, leafNodeSize, null, null);

    // Add the root.
    table.put(key, new NodeMetaLeaf<>(key, rootLeafNode));
  }

  private LeafNode<K, V> searchTrieHashTable(Object encodedKey) {
    MetaTrieHashTable.NodeMeta nodeMeta =
        table.searchLongestPrefixMatch(encodedKeyType, encodedKey);
    if (nodeMeta instanceof MetaTrieHashTable.NodeMetaLeaf) {
      return ((MetaTrieHashTable.NodeMetaLeaf<K, V>) nodeMeta).leafNode;
    }

    NodeMetaInternal<K, V> nodeMetaInternal = (NodeMetaInternal<K, V>) nodeMeta;
    int anchorPrefixLength = EncodedKeyUtils.length(encodedKeyType, nodeMetaInternal.anchorPrefix);

    // The leaf type is INTERNAL.
    int encodedKeyLength = EncodedKeyUtils.length(encodedKeyType, encodedKey);
    if (anchorPrefixLength == encodedKeyLength) {
      LeafNode<K, V> leafNode = nodeMetaInternal.getLeftMostLeafNode();
      if (EncodedKeyUtils.compare(encodedKeyType, encodedKey, leafNode.anchorKey) < 0) {
        // For example, if the paper's example had key "J" in the second leaf node and the search
        // key is "J", this special treatment would be necessary.
        return leafNode.getLeft();
      } else {
        return leafNode;
      }
    }

    if (anchorPrefixLength > encodedKeyLength) {
      throw new AssertionError(
          "The length of the anchor prefix is longer than the length of the key");
    }

    int missingToken = EncodedKeyUtils.get(encodedKeyType, encodedKey, anchorPrefixLength);
    Integer siblingToken = nodeMetaInternal.findOneSibling(missingToken);
    if (siblingToken == null) {
      return nodeMetaInternal.getLeftMostLeafNode();
    }

    MetaTrieHashTable.NodeMeta childNode =
        table.get(
            EncodedKeyUtils.append(encodedKeyType, nodeMetaInternal.anchorPrefix, siblingToken));
    if (childNode == null) {
      throw new AssertionError("Child node is not found");
    }

    if (childNode instanceof MetaTrieHashTable.NodeMetaLeaf) {
      LeafNode<K, V> leafNode = ((MetaTrieHashTable.NodeMetaLeaf<K, V>) childNode).leafNode;
      if (missingToken < siblingToken) {
        return leafNode.getLeft();
      } else {
        return leafNode;
      }
    } else {
      NodeMetaInternal<K, V> childNodeInternal = (NodeMetaInternal<K, V>) childNode;
      if (missingToken < siblingToken) {
        // The child node is a subtree right to the target node.
        return childNodeInternal.getLeftMostLeafNode().getLeft();
      } else {
        // The child node is a subtree left to the target node.
        return childNodeInternal.getRightMostLeafNode();
      }
    }
  }

  @Nullable
  private Object provideValidAnchorKey(Object anchorKey) {
    MetaTrieHashTable.NodeMeta existingNodeMeta = table.get(anchorKey);
    if (existingNodeMeta == null) {
      return anchorKey;
    }
    return null;
  }

  private LeafNode<K, V> split(LeafNode<K, V> leafNode) {
    Tuple<Object, LeafNode<K, V>> newLeafNodeAndAnchor = leafNode.splitToNewLeafNode();
    Object newAnchor = newLeafNodeAndAnchor.first;
    LeafNode<K, V> newLeafNode = newLeafNodeAndAnchor.second;
    table.handleSplitNodes(newAnchor, newLeafNode);
    return newLeafNode;
  }

  private void merge(LeafNode<K, V> left, LeafNode<K, V> victim) {
    left.merge(victim);
    boolean childNodeRemoved = false;
    for (int prefixlen = EncodedKeyUtils.length(encodedKeyType, victim.anchorKey);
        prefixlen >= 0;
        prefixlen--) {
      Object prefix = EncodedKeyUtils.slice(encodedKeyType, victim.anchorKey, prefixlen);
      MetaTrieHashTable.NodeMeta nodeMeta = table.get(prefix);
      NodeMetaInternal<K, V> nodeMetaInternal = null;
      MetaTrieHashTable.NodeMetaLeaf<K, V> nodeMetaLeaf = null;
      if (nodeMeta instanceof NodeMetaInternal) {
        nodeMetaInternal = (NodeMetaInternal<K, V>) nodeMeta;
      } else {
        assert nodeMeta instanceof MetaTrieHashTable.NodeMetaLeaf;
        nodeMetaLeaf = (NodeMetaLeaf<K, V>) nodeMeta;
      }

      // The pseudocode in the paper always clears the bitmap index for the child token.
      // However, it should probably be cleared only when the child node has been removed.
      if (childNodeRemoved) {
        if (nodeMetaInternal != null) {
          nodeMetaInternal.bitmap.clear(
              EncodedKeyUtils.get(encodedKeyType, victim.anchorKey, prefixlen));
        }
      }
      // The root node meta must be left.
      // Remove internal node meta if it points a single leaf node.
      // This condition is added following the reference implementation.
      if (EncodedKeyUtils.length(encodedKeyType, nodeMeta.anchorPrefix) > 0
          && (nodeMetaLeaf != null
              || nodeMetaInternal
                  .getLeftMostLeafNode()
                  .equals(nodeMetaInternal.getRightMostLeafNode()))) {
        table.removeNodeMeta(prefix);
        childNodeRemoved = true;
      } else {
        childNodeRemoved = false;
        if (nodeMetaInternal != null) {
          if (nodeMetaInternal.getLeftMostLeafNode() == victim) {
            nodeMetaInternal.setLeftMostLeafNode(victim.getRight());
          }
          if (nodeMetaInternal.getRightMostLeafNode() == victim) {
            nodeMetaInternal.setRightMostLeafNode(victim.getLeft());
          }
        }
      }
    }
  }

  @Override
  public String toString() {
    return "Wormhole{" + "table=" + table + ", leafNodeSize=" + leafNodeSize + '}';
  }

  static abstract class Builder<W extends Wormhole<K, V>, B extends Builder<W, B, K, V>, K, V> {
    protected boolean isThreadSafe = false;
    protected boolean isDebugMode = false;
    protected int leafNodeSize = DEFAULT_LEAF_NODE_SIZE;

    protected abstract B self();

    public B setThreadSafe(boolean isThreadSafe) {
      this.isThreadSafe = isThreadSafe;
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

  static class Validator<K, T> {
    private final Wormhole<K, T> wormhole;

    Validator(Wormhole<K, T> wormhole) {
      this.wormhole = wormhole;
    }

    void validate() {
      try {
        validateInternal();
      } catch (AssertionError e) {
        System.err.println(wormhole);
        throw e;
      }
    }

    private void validateLeafNodes(
        LeafNode<K, T> leftMostLeafNode, LeafNode<K, T> rightMostLeafNode) {
      LeafNode<K, T> leafNode = leftMostLeafNode;
      LeafNode<K, T> lastLeafNode = null;
      while (leafNode != null) {
        leafNode.validate();
        if (leafNode != leftMostLeafNode) {
          if (leafNode.getLeft().getRight() != leafNode) {
            throw new AssertionError(
                String.format(
                    "The left node of the leaf node doesn't point the leaf node. Leaf node: %s; Left leaf node: %s",
                    leafNode, leafNode.getLeft()));
          }
        }
        if (leafNode != rightMostLeafNode) {
          if (leafNode.getRight().getLeft() != leafNode) {
            throw new AssertionError(
                String.format(
                    "The right node of the leaf node doesn't point the leaf node. Leaf node: %s; Right leaf node: %s",
                    leafNode, leafNode.getRight()));
          }
        }
        lastLeafNode = leafNode;
        leafNode = leafNode.getRight();
      }

      if (lastLeafNode != rightMostLeafNode) {
        throw new AssertionError(
            String.format(
                "The last leaf node isn't the right most leaf node. Last leaf node: %s; Right most leaf node: %s",
                lastLeafNode, rightMostLeafNode));
      }
    }

    private void validateHashTable() {
      for (Map.Entry<Object, MetaTrieHashTable.NodeMeta> entry :
          wormhole.table.entrySetForValidate()) {
        Object key = entry.getKey();
        MetaTrieHashTable.NodeMeta nodeMeta = entry.getValue();
        if (!nodeMeta.anchorPrefix.equals(key)) {
          throw new AssertionError(
              String.format(
                  "The node metadata anchor key is different from the key of the hash table. Key: %s, Node metadata anchor key: %s",
                  EncodedKeyUtils.toString(key), EncodedKeyUtils.toString(nodeMeta.anchorPrefix)));
        }
      }

      Collection<MetaTrieHashTable.NodeMeta> nodeMetas =
          new HashSet<>(wormhole.table.valuesForValidate());
      LinkedList<Object> anchorKeyQueue = new LinkedList<>();
      anchorKeyQueue.addLast(wormhole.createEmptyEncodedKey());
      while (!anchorKeyQueue.isEmpty()) {
        Object anchorKey = anchorKeyQueue.removeFirst();
        MetaTrieHashTable.NodeMeta nodeMeta = wormhole.table.getForValidate(anchorKey);
        if (!(nodeMeta instanceof NodeMetaInternal)) {
          if (!nodeMetas.remove(nodeMeta)) {
            throw new AssertionError(
                String.format("Unexpected node meta. Node meta: %s", nodeMeta));
          }
          continue;
        }

        NodeMetaInternal<K, T> nodeMetaInternal = (NodeMetaInternal<K, T>) nodeMeta;

        LeafNode<K, T> leftMostLeafNode = nodeMetaInternal.getLeftMostLeafNode();
        if (leftMostLeafNode != null) {
          if (!EncodedKeyUtils.startsWith(
              wormhole.encodedKeyType, leftMostLeafNode.anchorKey, anchorKey)) {
            throw new AssertionError(
                String.format(
                    "The anchor key of the node meta internal's left most leaf node doesn't start with the node meta internal's anchor key. Node meta internal: %s, Leaf node: %s",
                    nodeMeta, leftMostLeafNode));
          }
          if (leftMostLeafNode.getLeft() != null) {
            LeafNode<K, T> adjacentLeafNode = leftMostLeafNode.getLeft();
            if (EncodedKeyUtils.startsWith(
                wormhole.encodedKeyType, adjacentLeafNode.anchorKey, anchorKey)) {
              throw new AssertionError(
                  String.format(
                      "The anchor key of the adjacent leaf node left to the node meta internal's left most leaf node starts with the node meta internal's anchor key. Node meta internal: %s, Leaf node: %s",
                      nodeMeta, adjacentLeafNode));
            }
          }
        }

        LeafNode<K, T> rightMostLeafNode = nodeMetaInternal.getRightMostLeafNode();
        if (rightMostLeafNode != null) {
          if (!(EncodedKeyUtils.startsWith(
              wormhole.encodedKeyType, rightMostLeafNode.anchorKey, anchorKey))) {
            throw new AssertionError(
                String.format(
                    "The anchor key of the node meta internal's right most leaf node doesn't start with the node meta internal's anchor key. Node meta internal: %s, Leaf node: %s",
                    nodeMeta, rightMostLeafNode));
          }
          if (rightMostLeafNode.getRight() != null) {
            LeafNode<K, T> adjacentLeafNode = rightMostLeafNode.getRight();
            if (EncodedKeyUtils.startsWith(
                wormhole.encodedKeyType, adjacentLeafNode.anchorKey, anchorKey)) {
              throw new AssertionError(
                  String.format(
                      "The anchor key of the adjacent leaf node right to the node meta internal's left most leaf node starts with the node meta internal's anchor key. Node meta internal: %s, Leaf node: %s",
                      nodeMeta, adjacentLeafNode));
            }
          }
        }

        if (!nodeMetas.remove(nodeMetaInternal)) {
          throw new AssertionError(String.format("Unexpected node meta. Node meta: %s", nodeMeta));
        }
        nodeMetaInternal.bitmap.stream()
            .forEach(
                childHeadChar ->
                    anchorKeyQueue.addLast(
                        EncodedKeyUtils.append(wormhole.encodedKeyType, anchorKey, childHeadChar)));
      }

      if (!nodeMetas.isEmpty()) {
        throw new AssertionError(
            String.format("There are orphan node metas. Orphan node metas: %s", nodeMetas));
      }
    }

    private void validateInternal() {
      MetaTrieHashTable<K, T> table = wormhole.table;

      LeafNode<K, T> leftMostLeafNode;
      LeafNode<K, T> rightMostLeafNode;
      MetaTrieHashTable.NodeMeta rootNodeMeta =
          table.getForValidate(wormhole.createEmptyEncodedKey());
      if (rootNodeMeta instanceof MetaTrieHashTable.NodeMetaLeaf) {
        MetaTrieHashTable.NodeMetaLeaf<K, T> nodeMetaLeaf =
            (MetaTrieHashTable.NodeMetaLeaf<K, T>) rootNodeMeta;
        leftMostLeafNode = nodeMetaLeaf.leafNode;
        rightMostLeafNode = nodeMetaLeaf.leafNode;
      } else {
        NodeMetaInternal<K, T> nodeMetaInternal = (NodeMetaInternal<K, T>) rootNodeMeta;
        leftMostLeafNode = nodeMetaInternal.getLeftMostLeafNode();
        rightMostLeafNode = nodeMetaInternal.getRightMostLeafNode();
      }
      validateLeafNodes(leftMostLeafNode, rightMostLeafNode);

      validateHashTable();
    }
  }
}
