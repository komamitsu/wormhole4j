package org.komamitsu.wormhole;

public class Wormhole<T> {
  private final MetaTrieHashTable table = new MetaTrieHashTable();
  private final LeafList<T> leafList = new LeafList<>();

  LeafNode<T> searchTrieHashTable(String key) {
    MetaTrieHashTable.NodeMeta nodeMeta = table.searchLongestPrefixMatch(key);
    if (nodeMeta.type == MetaTrieHashTable.NodeType.LEAF) {
      return leafList.get(nodeMeta.leftMostLeafIndex);
    }

    int anchorPrefixLength = nodeMeta.anchorPrefix.length();

    // The leaf type is INTERNAL.
    if (anchorPrefixLength == key.length()) {
      int leafNodeIndex = nodeMeta.leftMostLeafIndex;
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
    Character siblingToken = nodeMeta.findOneSibling(missingToken);
    if (siblingToken == null) {
      return null;
    }

    MetaTrieHashTable.NodeMeta childNode = table.get(nodeMeta.anchorPrefix + siblingToken);
    if (childNode == null) {
      throw new AssertionError();
    }

    if (childNode.type == MetaTrieHashTable.NodeType.LEAF) {
      int leafNodeIndex = childNode.leftMostLeafIndex;
      if (missingToken < siblingToken) {
        return leafList.get(leafNodeIndex - 1);
      }
      else {
        return leafList.get(leafNodeIndex);
      }
    }
    else {
      if (missingToken < siblingToken) {
        // The child node is a subtree right to the target node.
        return leafList.get(childNode.leftMostLeafIndex - 1);
      }
      else {
        // The child node is a subtree left to the target node.
        return leafList.get(childNode.rightMostLeafIndex);
      }
    }
  }
}
