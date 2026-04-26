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

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

class WormholeValidator<K, T> {
  private final Wormhole<K, T> wormhole;

  WormholeValidator(Wormhole<K, T> wormhole) {
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
    MetaTrieHashTable<K, T> metaTable = wormhole.getActiveMetaTable();
    for (Map.Entry<Object, MetaTrieHashTable.NodeMeta> entry : metaTable.entrySetForValidate()) {
      Object key = entry.getKey();
      MetaTrieHashTable.NodeMeta nodeMeta = entry.getValue();
      if (!nodeMeta.anchorPrefix.equals(key)) {
        throw new AssertionError(
            String.format(
                "The node metadata anchor key is different from the key of the hash table. Key: %s, Node metadata anchor key: %s",
                EncodedKeyUtils.toString(key), EncodedKeyUtils.toString(nodeMeta.anchorPrefix)));
      }
    }

    Collection<MetaTrieHashTable.NodeMeta> nodeMetas = new HashSet<>(metaTable.valuesForValidate());
    LinkedList<Object> anchorKeyQueue = new LinkedList<>();
    anchorKeyQueue.addLast(wormhole.createEmptyEncodedKey());
    while (!anchorKeyQueue.isEmpty()) {
      Object anchorKey = anchorKeyQueue.removeFirst();
      MetaTrieHashTable.NodeMeta nodeMeta = metaTable.getForValidate(anchorKey);
      if (!(nodeMeta instanceof MetaTrieHashTable.NodeMetaInternal)) {
        if (!nodeMetas.remove(nodeMeta)) {
          throw new AssertionError(String.format("Unexpected node meta. Node meta: %s", nodeMeta));
        }
        continue;
      }

      MetaTrieHashTable.NodeMetaInternal<K, T> nodeMetaInternal =
          (MetaTrieHashTable.NodeMetaInternal<K, T>) nodeMeta;

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
                    "The anchor key of the adjacent leaf node right to the node meta internal's right most leaf node starts with the node meta internal's anchor key. Node meta internal: %s, Leaf node: %s",
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
    MetaTrieHashTable<K, T> metaTable = wormhole.getActiveMetaTable();

    LeafNode<K, T> leftMostLeafNode;
    LeafNode<K, T> rightMostLeafNode;
    MetaTrieHashTable.NodeMeta rootNodeMeta =
        metaTable.getForValidate(wormhole.createEmptyEncodedKey());
    if (rootNodeMeta instanceof MetaTrieHashTable.NodeMetaLeaf) {
      MetaTrieHashTable.NodeMetaLeaf<K, T> nodeMetaLeaf =
          (MetaTrieHashTable.NodeMetaLeaf<K, T>) rootNodeMeta;
      leftMostLeafNode = nodeMetaLeaf.leafNode;
      rightMostLeafNode = nodeMetaLeaf.leafNode;
    } else {
      MetaTrieHashTable.NodeMetaInternal<K, T> nodeMetaInternal =
          (MetaTrieHashTable.NodeMetaInternal<K, T>) rootNodeMeta;
      leftMostLeafNode = nodeMetaInternal.getLeftMostLeafNode();
      rightMostLeafNode = nodeMetaInternal.getRightMostLeafNode();
    }
    validateLeafNodes(leftMostLeafNode, rightMostLeafNode);

    validateHashTable();
  }
}
