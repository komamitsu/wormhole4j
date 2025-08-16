package org.komamitsu.wormhole;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.annotation.Nullable;

public class Wormhole<T> {
  private static final int DEFAULT_LEAF_NODE_SIZE = 128;
  public static final String SMALLEST_TOKEN = "\0";
  public static final char BITMAP_ID_OF_SMALLEST_TOKEN = 0;
  // Visible for testing.
  final MetaTrieHashTable<T> table = new MetaTrieHashTable<>();
  private final int leafNodeSize;
  private final int leafNodeMergeSize;
  @Nullable private final Validator<T> validator;

  public Wormhole() {
    this(DEFAULT_LEAF_NODE_SIZE);
  }

  public Wormhole(int leafNodeSize) {
    this(leafNodeSize, false);
  }

  public Wormhole(int leafNodeSize, boolean debugMode) {
    this.leafNodeSize = leafNodeSize;
    this.leafNodeMergeSize = leafNodeSize * 3 / 4;
    initialize();
    validator = debugMode ? new Validator<>(this) : null;
  }

  private void validateIfNeeded() {
    if (validator == null) {
      return;
    }
    validator.validate();
  }

  public void put(String key, T value) {
    LeafNode<T> leafNode = searchTrieHashTable(key);
    KeyValue<T> existingKeyValue = leafNode.pointSearchLeaf(key);
    if (existingKeyValue != null) {
      existingKeyValue.setValue(value);
      validateIfNeeded();
      return;
    }

    if (leafNode.size() == leafNodeSize) {
      // Split the node and get a new right leaf node.
      LeafNode<T> newLeafNode = split(leafNode);
      if (Utils.compareAnchorKeys(key, newLeafNode.anchorKey) < 0) {
        leafNode.add(key, value);
      } else {
        newLeafNode.add(key, value);
      }
    } else {
      assert leafNode.size() < leafNodeSize;
      leafNode.add(key, value);
    }
    validateIfNeeded();
  }

  public boolean delete(String key) {
    LeafNode<T> leafNode = searchTrieHashTable(key);
    if (!leafNode.delete(key)) {
      return false;
    }

    if (leafNode.getLeft() != null
        && leafNode.size() + leafNode.getLeft().size() < leafNodeMergeSize) {
      merge(leafNode.getLeft(), leafNode);
    } else if (leafNode.getRight() != null
        && leafNode.size() + leafNode.getRight().size() < leafNodeMergeSize) {
      merge(leafNode, leafNode.getRight());
    }

    validateIfNeeded();
    return true;
  }

  @Nullable
  public T get(String key) {
    LeafNode<T> leafNode = searchTrieHashTable(key);
    KeyValue<T> keyValue = leafNode.pointSearchLeaf(key);
    if (keyValue == null) {
      return null;
    }
    return keyValue.getValue();
  }

  private void scanInternal(
      String startKey,
      @Nullable String endKey,
      boolean isEndKeyExclusive,
      @Nullable Integer count,
      Function<KeyValue<T>, Boolean> function) {
    Function<KeyValue<T>, Boolean> actualFunction = function;
    if (count != null) {
      AtomicInteger counter = new AtomicInteger();
      actualFunction =
          kv -> {
            if (counter.getAndIncrement() >= count) {
              return false;
            }
            return function.apply(kv);
          };
    }

    LeafNode<T> leafNode = searchTrieHashTable(startKey);
    while (leafNode != null) {
      leafNode.incSort();
      if (!leafNode.iterateKeyValues(startKey, endKey, isEndKeyExclusive, actualFunction)) {
        return;
      }
      leafNode = leafNode.getRight();
      startKey = null;
    }
  }

  public List<KeyValue<T>> scanWithCount(String startKey, int count) {
    List<KeyValue<T>> result = new ArrayList<>(count);
    scanInternal(
        startKey,
        null, /* Not used */
        false,
        count,
        kv -> {
          result.add(kv);
          return true;
        });
    return result;
  }

  public void scan(
      @Nullable String startKey,
      @Nullable String endKey,
      boolean isEndKeyExclusive,
      Function<KeyValue<T>, Boolean> function) {
    scanInternal(startKey == null ? "" : startKey, endKey, isEndKeyExclusive, null, function);
  }

  public void scanWithExclusiveEndKey(
      @Nullable String startKey, @Nullable String endKey, Function<KeyValue<T>, Boolean> function) {
    scanInternal(startKey == null ? "" : startKey, endKey, true, null, function);
  }

  public void scanWithInclusiveEndKey(
      @Nullable String startKey, @Nullable String endKey, Function<KeyValue<T>, Boolean> function) {
    scanInternal(startKey == null ? "" : startKey, endKey, false, null, function);
  }

  private void initialize() {
    LeafNode<T> rootLeafNode = new LeafNode<>(SMALLEST_TOKEN, leafNodeSize, null, null);
    {
      // Add the root.
      String key = "";
      table.put(
          key,
          new MetaTrieHashTable.NodeMetaInternal<>(
              key, rootLeafNode, rootLeafNode, BITMAP_ID_OF_SMALLEST_TOKEN));
    }
    {
      // Add the first node.
      String key = SMALLEST_TOKEN;
      table.put(key, new MetaTrieHashTable.NodeMetaLeaf<>(key, rootLeafNode));
    }
  }

  private LeafNode<T> searchTrieHashTable(String key) {
    MetaTrieHashTable.NodeMeta<T> nodeMeta = table.searchLongestPrefixMatch(key);
    if (nodeMeta instanceof MetaTrieHashTable.NodeMetaLeaf) {
      return ((MetaTrieHashTable.NodeMetaLeaf<T>) nodeMeta).leafNode;
    }

    MetaTrieHashTable.NodeMetaInternal<T> nodeMetaInternal =
        (MetaTrieHashTable.NodeMetaInternal<T>) nodeMeta;
    int anchorPrefixLength = nodeMetaInternal.anchorPrefix.length();

    // The leaf type is INTERNAL.
    if (anchorPrefixLength == key.length()) {
      LeafNode<T> leafNode = nodeMetaInternal.getLeftMostLeafNode();
      if (Utils.compareAnchorKeys(key, leafNode.anchorKey) < 0) {
        // For example, if the paper's example had key "J" in the second leaf node and the search
        // key is "J",
        // this special treatment would be necessary.
        return leafNode.getLeft();
      } else {
        return leafNode;
      }
    }

    if (anchorPrefixLength > key.length()) {
      throw new AssertionError(
          "The length of the anchor prefix is longer than the length of the key");
    }

    char missingToken = key.charAt(anchorPrefixLength);
    Character siblingToken = nodeMetaInternal.findOneSibling(missingToken);
    if (siblingToken == null) {
      throw new AssertionError("Any sibling token is not found");
    }

    MetaTrieHashTable.NodeMeta<T> childNode =
        table.get(nodeMetaInternal.anchorPrefix + siblingToken);
    if (childNode == null) {
      throw new AssertionError("Child node is not found");
    }

    if (childNode instanceof MetaTrieHashTable.NodeMetaLeaf) {
      LeafNode<T> leafNode = ((MetaTrieHashTable.NodeMetaLeaf<T>) childNode).leafNode;
      if (missingToken < siblingToken) {
        return leafNode.getLeft();
      } else {
        return leafNode;
      }
    } else {
      MetaTrieHashTable.NodeMetaInternal<T> childNodeInternal =
          (MetaTrieHashTable.NodeMetaInternal<T>) childNode;
      if (missingToken < siblingToken) {
        // The child node is a subtree right to the target node.
        return childNodeInternal.getLeftMostLeafNode().getLeft();
      } else {
        // The child node is a subtree left to the target node.
        return childNodeInternal.getRightMostLeafNode();
      }
    }
  }

  private String extractLongestCommonPrefix(String a, String b) {
    int minLen = Math.min(a.length(), b.length());
    for (int i = 0; i < minLen; i++) {
      char ca = a.charAt(i);
      char cb = b.charAt(i);
      if (ca == cb) {
        continue;
      }
      return a.substring(0, i);
    }
    return a.substring(0, minLen);
  }

  private Tuple<Integer, String> findSplitPositionAndNewAnchorInLeafNode(LeafNode<T> leafNode) {
    for (int i = leafNode.size() / 2; i < leafNode.size(); i++) {
      assert i > 0;
      String k1 = leafNode.getKeyByKeyRefIndex(i - 1);
      String k2 = leafNode.getKeyByKeyRefIndex(i);

      String lcp = extractLongestCommonPrefix(k1, k2);
      String newAnchor = lcp + k2.charAt(lcp.length());

      // Check the anchor key ordering condition: left-key < anchor-key ≤ node-key
      if (newAnchor.compareTo(k1) <= 0) {
        continue;
      }
      // For anchor-key ≤ node-key, the relationship of `newAnchor` and `k2` always satisfy it.

      // Check the anchor key prefix condition.
      MetaTrieHashTable.NodeMeta<T> existingNodeMeta = table.get(newAnchor);
      if (existingNodeMeta != null) {
        // "Append 0s to key when necessary"
        newAnchor = newAnchor + SMALLEST_TOKEN;
        existingNodeMeta = table.get(newAnchor);
        if (existingNodeMeta instanceof MetaTrieHashTable.NodeMetaLeaf) {
          continue;
        }
      }
      return new Tuple<>(i, newAnchor);
    }
    throw new IllegalStateException("Cannot split the leaf node. Leaf node: " + leafNode);
  }

  private LeafNode<T> split(LeafNode<T> leafNode) {
    leafNode.incSort();
    // TODO: This can be moved to LeafNode.splitToNewLeafNode() ?
    Tuple<Integer, String> found = findSplitPositionAndNewAnchorInLeafNode(leafNode);
    int splitPosIndex = found.first;
    String newAnchor = found.second;
    LeafNode<T> newLeafNode = leafNode.splitToNewLeafNode(newAnchor, splitPosIndex);

    table.handleSplitNodes(newAnchor, newLeafNode);

    return newLeafNode;
  }

  private void merge(LeafNode<T> left, LeafNode<T> victim) {
    left.merge(victim);
    String anchorKey = table.removeNodeMetaLeaf(victim.anchorKey);
    boolean childNodeRemoved = true;
    for (int prefixlen = anchorKey.length() - 1; prefixlen >= 0; prefixlen--) {
      String prefix = anchorKey.substring(0, prefixlen);
      MetaTrieHashTable.NodeMetaInternal<T> nodeMetaInternal = table.findNodeMetaInternal(prefix);
      assert nodeMetaInternal != null;
      if (childNodeRemoved) {
        nodeMetaInternal.bitmap.clear(anchorKey.charAt(prefixlen));
      }
      if (nodeMetaInternal.bitmap.isEmpty()) {
        table.removeNodeMetaInternal(prefix);
        childNodeRemoved = true;
      } else {
        childNodeRemoved = false;
        if (nodeMetaInternal.getLeftMostLeafNode() == victim) {
          nodeMetaInternal.setLeftMostLeafNode(victim.getRight());
        }
        if (nodeMetaInternal.getRightMostLeafNode() == victim) {
          nodeMetaInternal.setRightMostLeafNode(victim.getLeft());
        }
      }
    }
  }

  @Override
  public String toString() {
    return "Wormhole{" + "table=" + table + ", leafNodeSize=" + leafNodeSize + '}';
  }

  static class Validator<T> {
    private final Wormhole<T> wormhole;

    Validator(Wormhole<T> wormhole) {
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

    private void validateLeafNodes(List<LeafNode<T>> leafNodes) {
      leafNodes.sort(Comparator.comparing(a -> a.anchorKey));
      for (int i = 0; i < leafNodes.size(); i++) {
        LeafNode<T> leafNode = leafNodes.get(i);
        leafNode.validate();
        if (i > 0) {
          if (leafNode.getLeft() != leafNodes.get(i - 1)) {
            throw new AssertionError(
                String.format(
                    "The left node of the leaf node is wrong. Leaf node: %s, Expected left node: %s",
                    leafNode, leafNodes.get(i - 1)));
          }
        }
        if (i < leafNodes.size() - 1) {
          if (leafNode.getRight() != leafNodes.get(i + 1)) {
            throw new AssertionError(
                String.format(
                    "The right node of the leaf node is wrong. Leaf node: %s, Expected right node: %s",
                    leafNode, leafNodes.get(i + 1)));
          }
        }
      }
    }

    private void validateHashTable(Collection<MetaTrieHashTable.NodeMeta<T>> nodeMetas) {
      LinkedList<String> anchorKeyQueue = new LinkedList<>();
      anchorKeyQueue.addLast("");
      while (!anchorKeyQueue.isEmpty()) {
        String anchorKey = anchorKeyQueue.removeFirst();
        MetaTrieHashTable.NodeMeta<T> nodeMeta = wormhole.table.get(anchorKey);
        if (!(nodeMeta instanceof MetaTrieHashTable.NodeMetaInternal)) {
          if (!nodeMetas.remove(nodeMeta)) {
            throw new AssertionError(
                String.format("Unexpected node meta. Node meta: %s", nodeMeta));
          }
          continue;
        }

        MetaTrieHashTable.NodeMetaInternal<T> nodeMetaInternal =
            (MetaTrieHashTable.NodeMetaInternal<T>) nodeMeta;

        LeafNode<T> leftMostLeafNode = nodeMetaInternal.getLeftMostLeafNode();
        if (leftMostLeafNode != null) {
          if (!leftMostLeafNode.anchorKey.startsWith(anchorKey)) {
            throw new AssertionError(
                String.format(
                    "The anchor key of the node meta internal's left most leaf node doesn't start with the node meta internal's anchor key. Node meta internal: %s, Leaf node: %s",
                    nodeMeta, leftMostLeafNode));
          }
          if (leftMostLeafNode.getLeft() != null) {
            LeafNode<T> adjacentLeafNode = leftMostLeafNode.getLeft();
            if (adjacentLeafNode.anchorKey.startsWith(anchorKey)) {
              throw new AssertionError(
                  String.format(
                      "The anchor key of the adjacent leaf node left to the node meta internal's left most leaf node starts with the node meta internal's anchor key. Node meta internal: %s, Leaf node: %s",
                      nodeMeta, adjacentLeafNode));
            }
          }
        }

        LeafNode<T> rightMostLeafNode = nodeMetaInternal.getRightMostLeafNode();
        if (rightMostLeafNode != null) {
          if (!rightMostLeafNode.anchorKey.startsWith(anchorKey)) {
            throw new AssertionError(
                String.format(
                    "The anchor key of the node meta internal's right most leaf node doesn't start with the node meta internal's anchor key. Node meta internal: %s, Leaf node: %s",
                    nodeMeta, rightMostLeafNode));
          }
          if (rightMostLeafNode.getRight() != null) {
            LeafNode<T> adjacentLeafNode = rightMostLeafNode.getRight();
            if (adjacentLeafNode.anchorKey.startsWith(anchorKey)) {
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
            .forEach(childHeadChar -> anchorKeyQueue.addLast(anchorKey + ((char) childHeadChar)));
      }
    }

    private void validateInternal() {
      List<LeafNode<T>> leafNodes = new ArrayList<>();
      MetaTrieHashTable<T> table = wormhole.table;
      Collection<MetaTrieHashTable.NodeMeta<T>> nodeMetas = new HashSet<>(table.values());
      // Collect leaf nodes.
      for (Map.Entry<String, MetaTrieHashTable.NodeMeta<T>> entry : table.entrySet()) {
        String key = entry.getKey();
        MetaTrieHashTable.NodeMeta<T> nodeMeta = entry.getValue();
        if (!nodeMeta.anchorPrefix.equals(key)) {
          throw new AssertionError(
              String.format(
                  "The node metadata anchor key is different from the key of the hash table. Key: %s, Node metadata anchor key: %s",
                  key, nodeMeta.anchorPrefix));
        }
        if (nodeMeta instanceof MetaTrieHashTable.NodeMetaLeaf) {
          LeafNode<T> leafNode = ((MetaTrieHashTable.NodeMetaLeaf<T>) nodeMeta).leafNode;
          leafNodes.add(leafNode);
        }
        // Node meta internals are validated later.
      }

      validateLeafNodes(leafNodes);

      validateHashTable(nodeMetas);

      if (!nodeMetas.isEmpty()) {
        throw new AssertionError(
            String.format("There are orphan node metas. Orphan node metas: %s", nodeMetas));
      }
    }
  }
}
