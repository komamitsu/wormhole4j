package org.komamitsu.wormhole;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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

    private void addAll(Tags<T> tags) {
      this.values.addAll(tags.values);
    }

    private void sort() {
      Collections.sort(values);
    }

    private void removeIf(Predicate<Tag<T>> predicate) {
      values.removeIf(predicate);
    }

    private void remove(Tag<T> tag) {
      values.remove(tag);
    }

    private short getHashTagByIndex(int index) {
      return values.get(index).hash;
    }

    private KeyValue<T> getKeyValueByIndex(int index) {
      return values.get(index).keyValue;
    }

    private int size() {
      return values.size();
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
      return tag.keyValue.getKey().compareTo(other.tag.keyValue.getKey());
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

    private void addAll(KeyReferences<T> keyReferences) {
      values.addAll(keyReferences.values);
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
      return values.get(index).tag.keyValue.getKey();
    }

    private int getNumOfSortedValues() {
      return numOfSortedValues;
    }

    private void removeIf(Predicate<KeyReference<T>> predicate) {
      values.removeIf(predicate);
      // The original values should be sorted, then the current values should be sorted after removing values.
      markAsSorted();
    }

    private void remove(int index) {
      if (index < numOfSortedValues) {
        numOfSortedValues--;
      }
      values.remove(index);
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
          keyFromSortedKeyRef = values.get(idxForSortedKeyRef).tag.keyValue.getKey();
        }
        if (keyFromUnsortedKeyRef == null && idxForUnsortedKeyRef < unsortedValues.size()) {
          keyFromUnsortedKeyRef = unsortedValues.get(idxForUnsortedKeyRef).tag.keyValue.getKey();
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

    private int size() {
      return values.size();
    }

    private List<KeyValue<T>> getKeyValues(int count) {
      return values.stream().limit(count).map(x -> x.tag.keyValue).collect(Collectors.toList());
    }

    private <R> Tuple<Integer,List<R>> getKeyValuesEqualOrGreaterThan(String key, int count, Function<KeyReference<T>, R> resultConv) {
      if (values.isEmpty()) {
        return new Tuple<>(null, Collections.emptyList());
      }
      int l = 0;
      int r = values.size();
      int m;
      while (l < r) {
        m = (l + r) / 2;
        String k = values.get(m).tag.keyValue.getKey();
        int compared = key.compareTo(k);
        if (compared < 0) {
          r = m;
        } else if (compared > 0) {
          l = m + 1;
        }
        else {
          return new Tuple<>(m, values.subList(m, values.size()).stream().limit(count).map(resultConv).collect(Collectors.toList()));
        }
      }
      return new Tuple<>(null, values.subList(l, values.size()).stream().limit(count).map(resultConv).collect(Collectors.toList()));
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

  private void setLeft(@Nullable LeafNode<T> left) {
    this.left = left;
  }

  private void setRight(@Nullable LeafNode<T> right) {
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
      if (kv.getKey().equals(key)) {
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
      newLeafNode.tags.add(tag);
      newLeafNode.keyReferences.add(keyReferences.get(i));
    }
    newLeafNode.tags.sort();
    // The original leaf node's key references were sorted. Therefore, the new leaf node's ones should be sorted.
    newLeafNode.keyReferences.markAsSorted();

    LeafNode<T> rightLeafNode = getRight();
    if (rightLeafNode != null) {
      rightLeafNode.setLeft(newLeafNode);
    }

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
        "anchorKey='" + Utils.printableKey(anchorKey) + '\'' +
        ", maxSize=" + maxSize +
        ", keyValues=" + keyValues +
        ", tags=" + tags +
        ", keyReferences=" + keyReferences +
        ", left=" + (left == null ? "null" : Utils.printableKey(left.anchorKey)) +
        ", right=" + (right == null ? "null" : Utils.printableKey(right.anchorKey)) +
        '}';
  }

  List<KeyValue<T>> getKeyValuesEqualOrGreaterThan(String key, int count) {
    return keyReferences.getKeyValuesEqualOrGreaterThan(key, count, x -> x.tag.keyValue).second;
  }

  List<KeyValue<T>> getKeyValues(int count) {
    return keyReferences.getKeyValues(count);
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

  boolean delete(String key) {
    incSort();
    Tuple<Integer, List<Tag<T>>> keyRefIndexAndTags = keyReferences.getKeyValuesEqualOrGreaterThan(key, 1, x -> x.tag);
    Integer keyRefIndex = keyRefIndexAndTags.first;
    List<Tag<T>> tags = keyRefIndexAndTags.second;
    if (keyRefIndex == null) {
      return false;
    }
    assert !tags.isEmpty();
    Tag<T> tag = tags.get(0);
    assert tag.keyValue.getKey().equals(key);
    KeyValue<T> keyValue = tag.keyValue;

    keyValues.remove(keyValue);
    this.tags.remove(tag);
    keyReferences.remove(keyRefIndex);

    return true;
  }

  void merge(LeafNode<T> right) {
    keyValues.addAll(right.keyValues);
    tags.addAll(right.tags);
    tags.sort();
    keyReferences.addAll(right.keyReferences);

    if (right.getRight() != null) {
      right.getRight().setLeft(this);
    }
    setRight(right.getRight());
  }

  void validate() {
    String normalizedAnchorKey = anchorKey;
    if (normalizedAnchorKey.endsWith(Wormhole.SMALLEST_TOKEN)) {
      normalizedAnchorKey = normalizedAnchorKey.substring(0, normalizedAnchorKey.length() - 1);
    }
    String normalizedRightAnchorKey = null;
    if (right != null) {
      normalizedRightAnchorKey = right.anchorKey;
      if (normalizedRightAnchorKey.endsWith(Wormhole.SMALLEST_TOKEN)) {
        normalizedRightAnchorKey = normalizedRightAnchorKey.substring(0, normalizedRightAnchorKey.length() - 1);
      }
    }

    for (int i = 0; i < size(); i++) {
      KeyValue<T> kv = keyValues.get(i);
      if (kv.getKey().compareTo(normalizedAnchorKey) < 0) {
        throw new AssertionError(
            String.format(
                "The key is smaller than the anchor key. Key: %s, Anchor key: %s",
                Utils.printableKey(kv.getKey()), normalizedAnchorKey));
      }
      if (normalizedRightAnchorKey != null && normalizedRightAnchorKey.compareTo(kv.getKey()) < 0) {
        throw new AssertionError(
            String.format(
                "The anchor key of the right leaf node is smaller than the key. Key: %s, Right leaf node's anchor key: %s",
                Utils.printableKey(kv.getKey()), normalizedRightAnchorKey));
      }
    }

    if (tags.size() != size()) {
      throw new AssertionError(
          String.format(
              "The number of tags is different from the number of keys. Keys: %s, Tags: %s",
              keyValues.stream().map(kv -> Utils.printableKey(kv.getKey())).collect(Collectors.toList()), tags));
    }
    for (int i = 0; i < size(); i++) {
      if (i < size() - 1) {
        if (tags.getHashTagByIndex(i) > tags.getHashTagByIndex(i + 1)) {
          throw new AssertionError(String.format("The tags are not sorted. Tags: %s", tags));
        }
      }
    }

    if (keyReferences.size() != size()) {
      throw new AssertionError(
          String.format(
              "The number of key references is different from the number of keys. Keys: %s, Key references: %s",
              keyValues.stream().map(kv -> Utils.printableKey(kv.getKey())).collect(Collectors.toList()), keyReferences));
    }

    if (keyReferences.numOfSortedValues > keyReferences.size()) {
      throw new AssertionError(
          String.format("The number of sorted key references is larger than the number of key references. Key references: %s",
              keyReferences));
    }

    for (int i = 0; i < size(); i++) {
      if (i > 0 && i < keyReferences.getNumOfSortedValues() - 1) {
        if (keyReferences.getKey(i).compareTo(keyReferences.getKey(i + 1)) > 0) {
          throw new AssertionError(String.format("The key references are not ordered. Key references: %s", keyReferences));
        }
      }
    }
  }
}
