package org.komamitsu.wormhole;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Predicate;

class LeafNode<T> {
  public final String anchorKey;
  private final int maxSize;
  private final List<KeyValue<T>> keyValues;
  // All references are always sorted by hash.
  private final Tags<T> tags;
  // Some references are sorted by key.
  private final KeyReferences<T> keyReferences;

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

  // Tag

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

  private static class Tags<T> {
    private final List<Tag<T>> values;

    private Tags(int maxSize) {
      this.values = new ArrayList<>(maxSize);
    }

    private void add(Tag<T> value) {
      values.add(value);
    }

    private void sort() {
      Collections.sort(values);
    }

    private void removeIf(Predicate<Tag<T>> predicate) {
      values.removeIf(predicate);
    }

    private short getHashTagByIndex(int index) {
      return values.get(index).hash;
    }

    private KeyValue<T> getKeyValueByIndex(int index) {
      return values.get(index).keyValue;
    }

    @Override
    public String toString() {
      return "Tags{" +
          "values=" + values +
          '}';
    }
  }

  // Key reference

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

  private static class KeyReferences<T> {
    private final List<KeyReference<T>> values;
    private int numOfSortedValues;

    private KeyReferences(int maxSize) {
      this.values = new ArrayList<>(maxSize);
    }

    private void add(KeyReference<T> value) {
      values.add(value);
    }

    private KeyReference<T> get(int index) {
      return values.get(index);
    }

    private Tag<T> getTag(int index) {
      return values.get(index).tag;
    }

    private KeyValue<T> getKeyValue(int index) {
      return values.get(index).tag.keyValue;
    }

    private String getKey(int index) {
      return values.get(index).tag.keyValue.key;
    }

    private void removeIf(Predicate<KeyReference<T>> predicate) {
      values.removeIf(predicate);
      // The original values should be sorted, then the current values should be sorted after removing values.
      markAsSorted();
    }

    private void sort() {
      int totalSize = values.size();

      List<KeyReference<T>> unsortedValues = values.subList(numOfSortedValues, totalSize);
      Collections.sort(unsortedValues);

      // Merge sorted and unsorted key references.
      List<KeyReference<T>> mergedValues = new ArrayList<>(totalSize);

      int idxForSortedKeyRef = 0;
      int idxForUnsortedKeyRef = 0;
      String keyFromSortedKeyRef = null;
      String keyFromUnsortedKeyRef = null;
      while (true) {
        if (keyFromSortedKeyRef == null && idxForSortedKeyRef < numOfSortedValues) {
          keyFromSortedKeyRef = values.get(idxForSortedKeyRef).tag.keyValue.key;
        }
        if (keyFromUnsortedKeyRef == null && idxForUnsortedKeyRef < unsortedValues.size()) {
          keyFromUnsortedKeyRef = unsortedValues.get(idxForUnsortedKeyRef).tag.keyValue.key;
        }

        KeyReference<T> keyReference;
        if (keyFromSortedKeyRef != null) {
          if (keyFromUnsortedKeyRef != null) {
            if (keyFromSortedKeyRef.compareTo(keyFromUnsortedKeyRef) < 0) {
              keyReference = values.get(idxForSortedKeyRef++);
              keyFromSortedKeyRef = null;
            }
            else {
              keyReference = unsortedValues.get(idxForUnsortedKeyRef++);
              keyFromUnsortedKeyRef = null;
            }
          }
          else {
            keyReference = values.get(idxForSortedKeyRef++);
            keyFromSortedKeyRef = null;
          }
        }
        else {
          if (keyFromUnsortedKeyRef != null) {
            keyReference = unsortedValues.get(idxForUnsortedKeyRef++);
            keyFromUnsortedKeyRef = null;
          }
          else {
            break;
          }
        }
        mergedValues.add(keyReference);
      }
      Collections.copy(values, mergedValues);
      numOfSortedValues = totalSize;
    }

    private boolean isSorted() {
      return values.size() == numOfSortedValues;
    }

    private void markAsSorted() {
      numOfSortedValues = values.size();
    }

    @Override
    public String toString() {
      return "KeyReferences{" +
          "values=" + values +
          ", numOfSortedValues=" + numOfSortedValues +
          '}';
    }
  }

  LeafNode(String anchorKey, int maxSize, @Nullable LeafNode<T> left, @Nullable LeafNode<T> right) {
    this.anchorKey = anchorKey;
    this.maxSize = maxSize;
    keyValues = new ArrayList<>(maxSize);
    tags = new Tags<>(maxSize);
    keyReferences = new KeyReferences<>(maxSize);
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

  String getKeyByKeyRefIndex(int keyRefIndex) {
    return keyReferences.getKey(keyRefIndex);
  }

  private short calculateKeyHash(String key) {
    return (short) (0x7FFF & key.hashCode());
  }

  @Nullable
  KeyValue<T> pointSearchLeaf(String key) {
    short keyHash = calculateKeyHash(key);
    int leafSize = keyValues.size();
    int tagIndex = keyHash * leafSize / (Short.MAX_VALUE + 1);
    while (tagIndex > 0 && keyHash <= tags.getHashTagByIndex(tagIndex - 1)) {
      tagIndex--;
    }
    while (tagIndex < leafSize && tags.getHashTagByIndex(tagIndex) < keyHash) {
      tagIndex++;
    }
    while (tagIndex < leafSize && tags.getHashTagByIndex(tagIndex) == keyHash) {
      KeyValue<T> kv = tags.getKeyValueByIndex(tagIndex);
      if (kv.key.equals(key)) {
        return kv;
      }
      tagIndex++;
    }
    return null;
  }

  void incSort() {
    keyReferences.sort();
  }

  private Tuple<LeafNode<T>, Set<KeyValue<T>>> copyToNewLeafNode(String newAnchor, int startKeyRefIndex) {
    if (!keyReferences.isSorted()) {
      throw new AssertionError(
          String.format("The leaf node doesn't seem to be sorted. Key references: %s", keyReferences));
    }

    int currentSize = keyValues.size();

    // Copy entries to a new leaf node.
    LeafNode<T> newLeafNode = new LeafNode<>(newAnchor, maxSize, this, this.right);
    Set<KeyValue<T>> keyValuesInNewLeafNode = new HashSet<>(maxSize);
    for (int i = startKeyRefIndex; i < currentSize; i++) {
      Tag<T> tag = keyReferences.getTag(i);
      KeyValue<T> kv = tag.keyValue;
      newLeafNode.keyValues.add(kv);
      keyValuesInNewLeafNode.add(kv);
      // This needs to be sorted later.
      newLeafNode.tags.add(tag);
      newLeafNode.keyReferences.add(keyReferences.get(i));
    }
    tags.sort();
    newLeafNode.keyReferences.markAsSorted();

    setRight(newLeafNode);

    return new Tuple<>(newLeafNode, keyValuesInNewLeafNode);
  }

  private void removeMovedEntries(Set<KeyValue<T>> keyValuesInNewLeafNode) {
    keyValues.removeIf(keyValuesInNewLeafNode::contains);

    tags.removeIf(tag -> keyValuesInNewLeafNode.contains(tag.keyValue));
    tags.sort();

    keyReferences.removeIf(keyRef -> keyValuesInNewLeafNode.contains(keyRef.tag.keyValue));
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
    tags.sort();

    // Sorting this will be delayed until range scan or split.
    keyReferences.add(new KeyReference<>(tag));
  }
}
