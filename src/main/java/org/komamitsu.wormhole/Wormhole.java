package org.komamitsu.wormhole;

import java.util.BitSet;

public class Wormhole<T> {
  private static final int DEFAULT_LEAF_NODE_SIZE = 128;
  public static final String SMALLEST_TOKEN = "\0";
  private final MetaTrieHashTable table = new MetaTrieHashTable();
  private final LeafList<T> leafList = new LeafList<>();
  private final int leafNodeSize;

  public Wormhole() {
    this(DEFAULT_LEAF_NODE_SIZE);
  }

  public Wormhole(int leafNodeSize) {
    this.leafNodeSize = leafNodeSize;
    initialize();
  }

  private void initialize() {
    leafList.add(new LeafNode<>(leafNodeSize));
    {
      // Add the root.
      String key = "";
      BitSet bitSet = new BitSet();
      bitSet.set(0);
      table.put(key, new MetaTrieHashTable.NodeMetaInternal(key, 0, 0, bitSet), 0);
    }
    {
      // Add the first node.
      String key = SMALLEST_TOKEN;
      table.put(key, new MetaTrieHashTable.NodeMetaLeaf(key, 0), 0);
    }
  }

  private LeafNode<T> searchTrieHashTable(String key) {
    MetaTrieHashTable.NodeMeta nodeMeta = table.searchLongestPrefixMatch(key);
    if (nodeMeta instanceof MetaTrieHashTable.NodeMetaLeaf) {
      return leafList.get(((MetaTrieHashTable.NodeMetaLeaf) nodeMeta).leafIndex);
    }

    MetaTrieHashTable.NodeMetaInternal nodeMetaInternal = (MetaTrieHashTable.NodeMetaInternal) nodeMeta;
    int anchorPrefixLength = nodeMetaInternal.anchorPrefix.length();

    // The leaf type is INTERNAL.
    if (anchorPrefixLength == key.length()) {
      int leafNodeIndex = nodeMetaInternal.leftMostLeafIndex;
      if (key.compareTo(nodeMeta.anchorPrefix) < 0) {
        if (leafNodeIndex <= 0) {
          throw new AssertionError();
        }
        // For example, if the paper's example had key "J" in the second leaf node and the search key is "J",
        // this special treatment would be necessary.
        return leafList.get(leafNodeIndex - 1);
      }
      else {
        return leafList.get(leafNodeIndex);
      }
    }

    if (anchorPrefixLength > key.length()) {
      throw new AssertionError();
    }

    char missingToken = key.charAt(anchorPrefixLength);
    Character siblingToken = nodeMetaInternal.findOneSibling(missingToken);
    if (siblingToken == null) {
      return null;
    }

    MetaTrieHashTable.NodeMeta childNode = table.get(nodeMetaInternal.anchorPrefix + siblingToken);
    if (childNode == null) {
      throw new AssertionError();
    }

    if (childNode instanceof MetaTrieHashTable.NodeMetaLeaf) {
      int leafNodeIndex = ((MetaTrieHashTable.NodeMetaLeaf) childNode).leafIndex;
      if (missingToken < siblingToken) {
        return leafList.get(leafNodeIndex - 1);
      }
      else {
        return leafList.get(leafNodeIndex);
      }
    }
    else {
      MetaTrieHashTable.NodeMetaInternal childNodeInternal = (MetaTrieHashTable.NodeMetaInternal) childNode;
      if (missingToken < siblingToken) {
        // The child node is a subtree right to the target node.
        return leafList.get(childNodeInternal.leftMostLeafIndex - 1);
      }
      else {
        // The child node is a subtree left to the target node.
        return leafList.get(childNodeInternal.rightMostLeafIndex);
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

      String newAnchor = extractLongestCommonPrefix(k1, k2);

      // TODO: When to use the terminator character?

      // Check the anchor key ordering condition: left-key < anchor-key ≤ node-key
      if (newAnchor.compareTo(k1) <= 0) {
        continue;
      }
      if (k2.compareTo(newAnchor) < 0) {
        continue;
      }

      // Check the anchor key prefix condition.
      MetaTrieHashTable.NodeMeta existingNodeMeta = table.get(newAnchor);
      if (existingNodeMeta instanceof MetaTrieHashTable.NodeMetaLeaf) {
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

  private void split(int leafNodeIndex, LeafNode<T> leafNode) {
    // TODO: This can be moved to LeafNode.splitToNewLeafNode() ?
    Tuple<Integer, String> found = findSplitPositionAndNewAnchorInLeafNode(leafNode);
    int splitPosIndex = found.first;
    String newAnchor = found.second;
    LeafNode<T> newLeafNode = leafNode.splitToNewLeafNode(splitPosIndex);

    int newLeafNodeIndex = leafNodeIndex + 1;
    leafList.add(newLeafNodeIndex, newLeafNode);

    table.put(newAnchor, new MetaTrieHashTable.NodeMetaLeaf(newAnchor, newLeafNodeIndex), newLeafNodeIndex);
  }
}
