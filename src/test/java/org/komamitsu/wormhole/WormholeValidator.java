package org.komamitsu.wormhole;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class WormholeValidator<T> {
  private final Wormhole<T> wormhole;

  WormholeValidator(Wormhole<T> wormhole) {
    this.wormhole = wormhole;
  }

  void validateLeafNode(LeafNode<T> leafNode) {
    int leafNodeSize = leafNode.size();
    assertThat(leafNode.tags.size()).isEqualTo(leafNodeSize);
    for (int i = 0; i < leafNode.size(); i++) {
      if (i < leafNodeSize - 1) {
        assertThat(leafNode.tags.getHashTagByIndex(i)).isLessThan(leafNode.tags.getHashTagByIndex(i + 1));
      }
    }

    assertThat(leafNode.keyReferences.size()).isEqualTo(leafNodeSize);
    assertThat(leafNode.keyReferences.getNumOfSortedValues()).isLessThanOrEqualTo(leafNodeSize);
    for (int i = 0; i < leafNode.size(); i++) {
      if (i > 0 && i < leafNode.keyReferences.getNumOfSortedValues() - 1) {
        assertThat(leafNode.keyReferences.getKey(i)).isLessThan(leafNode.keyReferences.getKey(i + 1));
      }
    }
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
      assertThat(nodeMeta.anchorPrefix).isEqualTo(key);
      if (nodeMeta instanceof MetaTrieHashTable.NodeMetaLeaf) {
        LeafNode<T> leafNode = ((MetaTrieHashTable.NodeMetaLeaf<T>) nodeMeta).leafNode;
        assertThat(leafNode.anchorKey).isEqualTo(nodeMeta.anchorPrefix);
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
        assertThat(leafNode.getLeft()).isEqualTo(leafNodes.get(i - 1));
      }
      if (i < leafNodes.size() - 1) {
        assertThat(leafNode.getRight()).isEqualTo(leafNodes.get(i + 1));
      }
      validateLeafNode(leafNode);
    }
  }
}
