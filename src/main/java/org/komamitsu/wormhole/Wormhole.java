package org.komamitsu.wormhole;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class Wormhole<T> {
  private static final int DEFAULT_LEAF_NODE_SIZE = 128;
  public static final String SMALLEST_TOKEN = "\0";
  public static final char BITMAP_ID_OF_SMALLEST_TOKEN = 0;
  // Visible for testing.
  final MetaTrieHashTable<T> table = new MetaTrieHashTable<>();
  private final int leafNodeSize;

  public Wormhole() {
    this(DEFAULT_LEAF_NODE_SIZE);
  }

  public Wormhole(int leafNodeSize) {
    this.leafNodeSize = leafNodeSize;
    initialize();
  }

  public void put(String key, T value) {
    LeafNode<T> leafNode = searchTrieHashTable(key);
    LeafNode.KeyValue<T> existingKeyValue = leafNode.pointSearchLeaf(key);
    if (existingKeyValue != null) {
      existingKeyValue.setValue(value);
      return;
    }

    if (leafNode.size() == leafNodeSize) {
      // Split the node and get a new right leaf node.
      LeafNode<T> newLeafNode = split(leafNode);
      if (Utils.compareAnchorKeys(key, newLeafNode.anchorKey) < 0) {
        leafNode.add(key, value);
      }
      else {
        newLeafNode.add(key, value);
      }
    }
    else {
      assert leafNode.size() < leafNodeSize;
      leafNode.add(key, value);
    }
  }

  @Nullable
  public T get(String key) {
    LeafNode<T> leafNode = searchTrieHashTable(key);
    LeafNode.KeyValue<T> keyValue = leafNode.pointSearchLeaf(key);
    if (keyValue == null) {
      return null;
    }
    return keyValue.getValue();
  }

  private void initialize() {
    LeafNode<T> rootLeafNode = new LeafNode<>(SMALLEST_TOKEN, leafNodeSize, null, null);
    {
      // Add the root.
      String key = "";
      table.put(key, new MetaTrieHashTable.NodeMetaInternal<>(key, rootLeafNode, rootLeafNode, BITMAP_ID_OF_SMALLEST_TOKEN));
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

    MetaTrieHashTable.NodeMetaInternal<T> nodeMetaInternal = (MetaTrieHashTable.NodeMetaInternal<T>) nodeMeta;
    int anchorPrefixLength = nodeMetaInternal.anchorPrefix.length();

    // The leaf type is INTERNAL.
    if (anchorPrefixLength == key.length()) {
      LeafNode<T> leafNode = nodeMetaInternal.getLeftMostLeafNode();
      if (Utils.compareAnchorKeys(key, leafNode.anchorKey) < 0) {
        // For example, if the paper's example had key "J" in the second leaf node and the search key is "J",
        // this special treatment would be necessary.
        return leafNode.getLeft();
      }
      else {
        return leafNode;
      }
    }

    if (anchorPrefixLength > key.length()) {
      throw new AssertionError("The length of the anchor prefix is longer than the length of the key");
    }

    char missingToken = key.charAt(anchorPrefixLength);
    Character siblingToken = nodeMetaInternal.findOneSibling(missingToken);
    if (siblingToken == null) {
      throw new AssertionError("Any sibling token is not found");
    }

    MetaTrieHashTable.NodeMeta<T> childNode = table.get(nodeMetaInternal.anchorPrefix + siblingToken);
    if (childNode == null) {
      throw new AssertionError("Child node is not found");
    }

    if (childNode instanceof MetaTrieHashTable.NodeMetaLeaf) {
      LeafNode<T> leafNode = ((MetaTrieHashTable.NodeMetaLeaf<T>) childNode).leafNode;
      if (missingToken < siblingToken) {
        return leafNode.getLeft();
      }
      else {
        return leafNode;
      }
    }
    else {
      MetaTrieHashTable.NodeMetaInternal<T> childNodeInternal = (MetaTrieHashTable.NodeMetaInternal<T>) childNode;
      if (missingToken < siblingToken) {
        // The child node is a subtree right to the target node.
        return childNodeInternal.getLeftMostLeafNode().getLeft();
      }
      else {
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

      // TODO: When to use the terminator character?

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
    throw new RuntimeException("Cannot split the leaf node. Leaf node: " + leafNode);
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

  @Override
  public String toString() {
    return "Wormhole{" +
        "table=" + table +
        ", leafNodeSize=" + leafNodeSize +
        '}';
  }

  void validate() {
    new Validator<T>(this).validate();
  }

  private static class Validator<T> {
    private final Wormhole<T> wormhole;

    Validator(Wormhole<T> wormhole) {
      this.wormhole = wormhole;
    }

    void validate() {
      try {
        validateInternal();
      }
      catch (AssertionError e) {
        System.err.println(wormhole);
        throw e;
      }
    }

    private void validateInternal() {
      List<LeafNode<T>> leafNodes = new ArrayList<>();
      MetaTrieHashTable<T> table = wormhole.table;
      for (Map.Entry<String, MetaTrieHashTable.NodeMeta<T>> entry : table.table.entrySet()) {
        String key = entry.getKey();
        MetaTrieHashTable.NodeMeta<T> nodeMeta = entry.getValue();
        if (!nodeMeta.anchorPrefix.equals(key)) {
          throw new AssertionError(
              String.format(
                  "The node metadata anchor key is different from the key of MetaTrieHashTable. Key: %s, Node metadata anchor key: %s",
                  key, nodeMeta.anchorPrefix));
        }
        if (nodeMeta instanceof MetaTrieHashTable.NodeMetaLeaf) {
          LeafNode<T> leafNode = ((MetaTrieHashTable.NodeMetaLeaf<T>) nodeMeta).leafNode;
          leafNode.validate();
          leafNodes.add(leafNode);
        }
        else {
          // TODO
        }
      }

      leafNodes.sort(Comparator.comparing(a -> a.anchorKey));
      for (int i = 0; i < leafNodes.size(); i++) {
        LeafNode<T> leafNode = leafNodes.get(i);
        if (i > 0) {
          if (leafNode.getLeft() != leafNodes.get(i - 1)) {
            throw new AssertionError(
                String.format(
                    "The left node of the leaf node is wrong. Leaf node: %s, Expected left node: %s", leafNode, leafNodes.get(i - 1)));
          }
        }
        if (i < leafNodes.size() - 1) {
          if (leafNode.getRight() != leafNodes.get(i + 1)) {
            throw new AssertionError(
                String.format(
                    "The right node of the leaf node is wrong. Leaf node: %s, Expected right node: %s", leafNode, leafNodes.get(i + 1)));
          }
        }
      }
    }
  }
}
