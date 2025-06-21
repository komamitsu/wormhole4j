package org.komamitsu.wormhole;


public class Wormhole<T> {
  private static final int DEFAULT_LEAF_NODE_SIZE = 128;
  public static final String SMALLEST_TOKEN = "\0";
  public static final char BITMAP_ID_OF_SMALLEST_TOKEN = 0;
  private final MetaTrieHashTable<T> table = new MetaTrieHashTable<>();
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
      if (key.compareTo(newLeafNode.anchorKey) < 0) {
        leafNode.add(key, value);
      }
      else {
        newLeafNode.add(key, value);
      }
    }
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
      if (key.compareTo(nodeMeta.anchorPrefix) < 0) {
        // For example, if the paper's example had key "J" in the second leaf node and the search key is "J",
        // this special treatment would be necessary.
        return nodeMetaInternal.getLeftMostLeafNode().getLeft();
      }
      else {
        return nodeMetaInternal.getLeftMostLeafNode();
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
      MetaTrieHashTable.NodeMeta<T> existingNodeMeta = table.get(newAnchor);
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

  private LeafNode<T> split(LeafNode<T> leafNode) {
    // TODO: This can be moved to LeafNode.splitToNewLeafNode() ?
    Tuple<Integer, String> found = findSplitPositionAndNewAnchorInLeafNode(leafNode);
    int splitPosIndex = found.first;
    String newAnchor = found.second;
    LeafNode<T> newLeafNode = leafNode.splitToNewLeafNode(newAnchor, splitPosIndex);

    table.handleSplitNodes(newAnchor, leafNode, newLeafNode);

    return newLeafNode;
  }
}
