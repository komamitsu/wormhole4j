package org.komamitsu.wormhole;

import javax.annotation.Nullable;
import java.util.*;

class LeafNode<T> {
  public final String anchorKey;
  private final int maxSize;
  private final List<KeyValue<T>> keyValues;
  // All references are always sorted by hash.
  private final List<Tag<T>> tags;
  // Some references are sorted by key.
  private final List<KeyReference<T>> keyReferences;
  private int numOfSortedKeyReferences;

  @Nullable
  private LeafNode<T> left;
  @Nullable
  private LeafNode<T> right;

  static class KeyValue<T> {
    public final String key;
    private T value;

    private KeyValue(String key, T value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public String toString() {
      return "KeyValue{" +
          "key='" + key + '\'' +
          ", value=" + value +
          '}';
    }

    public T getValue() {
      return value;
    }

    public void setValue(T value) {
      this.value = value;
    }
  }

  // TODO: This can be replaced with Integer, using the first 16 bits for hash and the second 16 bits for index.
  private static class Tag<T> implements Comparable<Tag<T>> {
    private final short hash;
    private final KeyValue<T> keyValue;

    private Tag(short hash, KeyValue<T> keyValue) {
      assert hash >= 0;
      this.hash = hash;
      this.keyValue = keyValue;
    }

    @Override
    public String toString() {
      return "Tag{" +
          "hash=" + hash +
          ", keyValue=" + keyValue +
          '}';
    }

    @Override
    public int compareTo(Tag<T> other) {
      return Short.compare(hash, other.hash);
    }
  }

  // A pointer to key via Tag.
  private static class KeyReference<T> implements Comparable<KeyReference<T>> {
    private final Tag<T> tag;

    private KeyReference(Tag<T> tag) {
      this.tag = tag;
    }

    @Override
    public String toString() {
      return "KeyReference{" +
          "tag=" + tag +
          '}';
    }

    @Override
    public int compareTo(KeyReference<T> other) {
      return tag.keyValue.key.compareTo(other.tag.keyValue.key);
    }
  }

  LeafNode(String anchorKey, int size, @Nullable LeafNode<T> left, @Nullable LeafNode<T> right) {
    this.anchorKey = anchorKey;
    this.maxSize = size;
    keyValues = new ArrayList<>(size);
    tags = new ArrayList<>(size);
    keyReferences = new ArrayList<>(size);
    this.left = left;
    this.right = right;
  }

  @Nullable
  LeafNode<T> getLeft() {
    return left;
  }

  @Nullable
  LeafNode<T> getRight() {
    return right;
  }

  void setLeft(@Nullable LeafNode<T> left) {
    this.left = left;
  }

  void setRight(@Nullable LeafNode<T> right) {
    this.right = right;
  }

  private short getHashTag(int tagIndex) {
    return tags.get(tagIndex).hash;
  }

  private KeyValue<T> getKeyValueByTagIndex(int tagIndex) {
    return tags.get(tagIndex).keyValue;
  }

  private Tag<T> getTagByKeyRefIndex(int keyReferenceIndex) {
    return keyReferences.get(keyReferenceIndex).tag;
  }

  private KeyValue<T> getKeyValueByKeyRefIndex(int keyReferenceIndex) {
    return keyReferences.get(keyReferenceIndex).tag.keyValue;
  }

  String getKeyByKeyRefIndex(int keyReferenceIndex) {
    return keyReferences.get(keyReferenceIndex).tag.keyValue.key;
  }

  private KeyReference<T> getKeyReference(int keyReferenceIndex) {
    return keyReferences.get(keyReferenceIndex);
  }

  private short calculateKeyHash(String key) {
    return (short) (0x7FFF & key.hashCode());
  }

  @Nullable
  KeyValue<T> pointSearchLeaf(String key) {
    short keyHash = calculateKeyHash(key);
    int leafSize = keyValues.size();
    int tagIndex = keyHash * leafSize / (Short.MAX_VALUE + 1);
    while (tagIndex > 0 && keyHash <= getHashTag(tagIndex - 1)) {
      tagIndex--;
    }
    while (tagIndex < leafSize && getHashTag(tagIndex) < keyHash) {
      tagIndex++;
    }
    while (tagIndex < leafSize && getHashTag(tagIndex) == keyHash) {
      KeyValue<T> kv = getKeyValueByTagIndex(tagIndex);
      if (kv.key.equals(key)) {
        return kv;
      }
      tagIndex++;
    }
    return null;
  }

  void incSort() {
    int size = size();

    // Sort unsorted key references.
    List<KeyReference<T>> unsortedKeyReferences = keyReferences.subList(numOfSortedKeyReferences, size);
    Collections.sort(unsortedKeyReferences);

    // Merge sorted and unsorted key references.
    List<KeyReference<T>> mergedKeyReferences = new ArrayList<>(size);

    int idxForSortedKeyRef = 0;
    int idxForUnsortedKeyRef = 0;
    String keyFromSortedKeyRef = null;
    String keyFromUnsortedKeyRef = null;
    while (true) {
      if (keyFromSortedKeyRef == null && idxForSortedKeyRef < numOfSortedKeyReferences) {
        keyFromSortedKeyRef = getKeyValueByKeyRefIndex(idxForSortedKeyRef).key;
      }
      if (keyFromUnsortedKeyRef == null && idxForUnsortedKeyRef < keyValues.size()) {
        keyFromUnsortedKeyRef = getKeyValueByKeyRefIndex(idxForUnsortedKeyRef).key;
      }

      KeyReference<T> keyReference;
      if (keyFromSortedKeyRef != null) {
        if (keyFromUnsortedKeyRef != null) {
          if (keyFromSortedKeyRef.compareTo(keyFromUnsortedKeyRef) < 0) {
            keyReference = getKeyReference(idxForSortedKeyRef++);
            keyFromSortedKeyRef = null;
          }
          else {
            keyReference = getKeyReference(idxForUnsortedKeyRef++);
            keyFromUnsortedKeyRef = null;
          }
        }
        else {
          keyReference = getKeyReference(idxForSortedKeyRef++);
          keyFromSortedKeyRef = null;
        }
      }
      else {
        if (keyFromUnsortedKeyRef != null) {
          keyReference = getKeyReference(idxForUnsortedKeyRef++);
          keyFromUnsortedKeyRef = null;
        }
        else {
          break;
        }
      }
      mergedKeyReferences.add(keyReference);
    }
    Collections.copy(keyReferences, mergedKeyReferences);
    numOfSortedKeyReferences = size;
  }

  private Tuple<LeafNode<T>, Set<KeyValue<T>>> copyToNewLeafNode(String newAnchor, int startKeyRefIndex) {
    if (numOfSortedKeyReferences != keyValues.size()) {
      throw new AssertionError(
          String.format("The leaf node doesn't seem to be sorted. The number of sorted key references: %d, The key value size: %d",
              numOfSortedKeyReferences, keyValues.size()));
    }

    int currentSize = keyValues.size();

    // Copy entries to a new leaf node.
    LeafNode<T> newLeafNode = new LeafNode<>(newAnchor, maxSize, this, this.right);
    Set<KeyValue<T>> keyValuesInNewLeafNode = new HashSet<>(maxSize);
    for (int i = startKeyRefIndex; i < currentSize; i++) {
      Tag<T> tag = getTagByKeyRefIndex(i);
      KeyValue<T> kv = tag.keyValue;
      newLeafNode.keyValues.add(kv);
      keyValuesInNewLeafNode.add(kv);
      // This needs to be sorted later.
      newLeafNode.tags.add(tag);
      newLeafNode.keyReferences.add(keyReferences.get(i));
    }
    int newLeafNodeSize = newLeafNode.size();
    Collections.sort(newLeafNode.tags);
    newLeafNode.numOfSortedKeyReferences = newLeafNodeSize;

    setRight(newLeafNode);

    return new Tuple<>(newLeafNode, keyValuesInNewLeafNode);
  }

  private void removeMovedEntries(Set<KeyValue<T>> keyValuesInNewLeafNode) {
    keyValues.removeIf(keyValuesInNewLeafNode::contains);

    tags.removeIf(tag -> keyValuesInNewLeafNode.contains(tag.keyValue));
    Collections.sort(tags);

    keyReferences.removeIf(keyRef -> keyValuesInNewLeafNode.contains(keyRef.tag.keyValue));
    Collections.sort(keyReferences);

    numOfSortedKeyReferences = size();
  }

  LeafNode<T> splitToNewLeafNode(String newAnchor, int startKeyRefIndex) {
    Tuple<LeafNode<T>, Set<KeyValue<T>>> copied = copyToNewLeafNode(newAnchor, startKeyRefIndex);
    LeafNode<T> newLeafNode = copied.first;
    Set<KeyValue<T>> keyValuesInNewLeafNode = copied.second;

    removeMovedEntries(keyValuesInNewLeafNode);

    return newLeafNode;
  }

  int size() {
    return keyValues.size();
  }

  @Override
  public String toString() {
    return "LeafNode{" +
        "anchorKey='" + anchorKey + '\'' +
        ", maxSize=" + maxSize +
        ", keyValues=" + keyValues +
        ", tags=" + tags +
        ", keyReferences=" + keyReferences +
        ", numOfSortedKeyReferences=" + numOfSortedKeyReferences +
        ", left=" + (left == null ? "null" : left.anchorKey) +
        ", right=" + (right == null ? "null" : right.anchorKey) +
        '}';
  }

  void add(String key, T value) {
    KeyValue<T> keyValue = new KeyValue<>(key, value);
    keyValues.add(keyValue);

    short keyHash = calculateKeyHash(key);

    Tag<T> tag = new Tag<>(keyHash, keyValue);
    tags.add(tag);
    Collections.sort(tags);

    // Sorting this will be delayed until range scan or split.
    keyReferences.add(new KeyReference<>(tag));
  }
}
