/*
 * Copyright 2025 Mitsunori Komatsu
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

import static org.komamitsu.wormhole4j.WormholeForStringKey.SMALLEST_TOKEN;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

class LeafNode<K, T> {
  final String anchorKey;
  private final int maxSize;
  private final KeyValues<K, T> keyValues;
  // All references are always sorted by hash.
  private final Tags<K, T> tags;
  // Some references are sorted by key.
  private final KeyReferences<K, T> keyReferences;
  private final Function<String, String> validAnchorKeyProvider;

  @Nullable private LeafNode<K, T> left;
  @Nullable private LeafNode<K, T> right;

  private static class KeyValues<K, T> {
    private int count;
    private final KeyValue<K, T>[] entries;

    @SuppressWarnings("unchecked")
    private KeyValues(int maxSize) {
      entries = (KeyValue<K, T>[]) new KeyValue[maxSize];
    }

    private KeyValue<K, T> get(int index) {
      return entries[index];
    }

    private int size() {
      return count;
    }

    private int getLastIndex() {
      return count - 1;
    }

    private void clear() {
      count = 0;
      Arrays.fill(entries, null);
    }

    private void addAll(KeyValues<K, T> other) {
      System.arraycopy(other.entries, 0, entries, count, other.count);
      count += other.count;
    }

    private void add(KeyValue<K, T> kv) {
      entries[count] = kv;
      count++;
    }

    private void remove(int index) {
      if (index < count - 1) {
        System.arraycopy(entries, index + 1, entries, index, count - 1 - index);
      }
      entries[count - 1] = null;
      count--;
    }

    @Override
    public String toString() {
      return "KeyValues{"
          + "count="
          + count
          + ", entries="
          + Arrays.stream(entries).limit(count).collect(Collectors.toList())
          + '}';
    }
  }

  private static class Tags<K, T> {
    private int count;
    private final KeyValues<K, T> keyValues;
    private final int[] entries;

    private Tags(int maxSize, KeyValues<K, T> keyValues) {
      this.entries = new int[maxSize];
      this.keyValues = keyValues;
    }

    private int calcTag(int keyValueIndex, KeyValue<K, T> keyValue) {
      assert keyValueIndex <= 0xFFFF;
      String key = keyValue.getEncodedKey();
      return calculateKeyHash(key) << 16 | keyValueIndex;
    }

    private void addWithoutSort(int keyValueIndex, KeyValue<K, T> keyValue) {
      int tag = calcTag(keyValueIndex, keyValue);
      entries[count] = tag;
      count++;
    }

    private void addWithSort(int keyValueIndex, KeyValue<K, T> keyValue) {
      int tag = calcTag(keyValueIndex, keyValue);
      int index;
      if (count == 0) {
        index = 0;
      } else {
        index = Arrays.binarySearch(entries, 0, count, tag);
        if (index < 0) {
          index = -(index + 1);
        }
        System.arraycopy(entries, index, entries, index + 1, count - index);
      }
      entries[index] = tag;
      count++;
    }

    private void sort() {
      Arrays.sort(entries, 0, count);
    }

    private void remove(int index) {
      int keyValueIndex = getKeyValueIndex(index);

      if (index < count - 1) {
        System.arraycopy(entries, index + 1, entries, index, (count - 1) - index);
      }
      count--;

      // Decrement key value indexes if needed.
      for (int i = 0; i < count; i++) {
        int kvIndex = getKeyValueIndex(i);
        assert kvIndex != keyValueIndex;
        if (kvIndex > keyValueIndex) {
          entries[i] = (entries[i] & 0xFFFF0000) | ((kvIndex - 1) & 0x0000FFFF);
        }
      }
    }

    private int getHashTag(int index) {
      return entries[index] >> 16;
    }

    private int getKeyValueIndex(int index) {
      return entries[index] & 0xFFFF;
    }

    private KeyValue<K, T> getKeyValue(int index) {
      return keyValues.get(getKeyValueIndex(index));
    }

    private void clear() {
      count = 0;
    }

    private int size() {
      return count;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("Tags{values=[");
      boolean isFirst = true;
      for (int i = 0; i < count; i++) {
        if (isFirst) {
          isFirst = false;
        } else {
          sb.append(", ");
        }
        sb.append("{hash=");
        sb.append(getHashTag(i));
        sb.append(", kv=");
        sb.append(getKeyValue(i));
        sb.append("}");
      }
      sb.append("]");
      return sb.toString();
    }
  }

  private static class KeyReferences<K, T> {
    private final KeyValues<K, T> keyValues;
    private int count;
    private final int[] entries;
    private int numOfSortedEntries;

    private KeyReferences(int maxSize, KeyValues<K, T> keyValues) {
      this.keyValues = keyValues;
      this.entries = new int[maxSize];
    }

    private void add(int keyValueIndex) {
      entries[count] = keyValueIndex;
      count++;
    }

    private int getKeyValueIndex(int index) {
      return entries[index];
    }

    private String getEncodedKey(int index) {
      return keyValues.get(getKeyValueIndex(index)).getEncodedKey();
    }

    private KeyValue<K, T> getKeyValue(int index) {
      return keyValues.get(getKeyValueIndex(index));
    }

    private void partialSort(int low, int high) {
      if (low >= high) {
        return;
      }
      String pivot = getEncodedKey((low + high) >>> 1);
      int l = low;
      int h = high;
      while (l <= h) {
        while (getEncodedKey(l).compareTo(pivot) < 0) {
          l++;
        }
        while (getEncodedKey(h).compareTo(pivot) > 0) {
          h--;
        }
        if (l <= h) {
          int tmp = entries[l];
          entries[l] = entries[h];
          entries[h] = tmp;
          l++;
          h--;
        }
      }
      partialSort(low, h);
      partialSort(l, high);
    }

    private void sort() {
      partialSort(numOfSortedEntries, count - 1);

      // Merge sorted and unsorted key references.
      int[] mergedValues = new int[count];
      int mergedValuesIndex = 0;

      int idxForSortedKeyRef = 0;
      int idxForUnsortedKeyRef = numOfSortedEntries;
      String keyFromSortedKeyRef = null;
      String keyFromUnsortedKeyRef = null;
      while (true) {
        if (keyFromSortedKeyRef == null && idxForSortedKeyRef < numOfSortedEntries) {
          keyFromSortedKeyRef = getEncodedKey(idxForSortedKeyRef);
        }
        if (keyFromUnsortedKeyRef == null && idxForUnsortedKeyRef < count) {
          keyFromUnsortedKeyRef = getEncodedKey(idxForUnsortedKeyRef);
        }

        int keyValueIndex;
        if (keyFromSortedKeyRef != null) {
          if (keyFromUnsortedKeyRef != null) {
            if (keyFromSortedKeyRef.compareTo(keyFromUnsortedKeyRef) < 0) {
              keyValueIndex = entries[idxForSortedKeyRef++];
              keyFromSortedKeyRef = null;
            } else {
              keyValueIndex = entries[idxForUnsortedKeyRef++];
              keyFromUnsortedKeyRef = null;
            }
          } else {
            keyValueIndex = entries[idxForSortedKeyRef++];
            keyFromSortedKeyRef = null;
          }
        } else {
          if (keyFromUnsortedKeyRef != null) {
            keyValueIndex = entries[idxForUnsortedKeyRef++];
            keyFromUnsortedKeyRef = null;
          } else {
            break;
          }
        }
        mergedValues[mergedValuesIndex++] = keyValueIndex;
      }
      System.arraycopy(mergedValues, 0, entries, 0, count);
      markAsSorted();
    }

    private void clear() {
      count = 0;
      numOfSortedEntries = 0;
    }

    private int getNumOfSortedEntries() {
      return numOfSortedEntries;
    }

    private void remove(int index) {
      int tagIndex = entries[index];
      if (index < count - 1) {
        System.arraycopy(entries, index + 1, entries, index, (count - 1) - index);
      }
      count--;
      if (index < numOfSortedEntries) {
        numOfSortedEntries--;
      }
      // Decrement tag indexes if needed.
      for (int i = 0; i < count; i++) {
        if (entries[i] > tagIndex) {
          entries[i]--;
        }
      }
    }

    private boolean isSorted() {
      return count == numOfSortedEntries;
    }

    private void markAsSorted() {
      numOfSortedEntries = count;
    }

    private int size() {
      return count;
    }

    private boolean iterateKeyValues(
        int startIndexInclusive,
        int endIndexInclusive,
        Function<KeyValue<K, T>, Boolean> function) {
      for (int i = startIndexInclusive; i <= endIndexInclusive; i++) {
        if (!function.apply(getKeyValue(i))) {
          return false;
        }
      }
      return true;
    }

    /**
     * Binary search. The specification of return value follows {@link Arrays#binarySearch(int[],
     * int)}
     *
     * @param key the target key
     * @return index of the search key, if it is contained in the array; otherwise,
     *     <tt>(-(<i>insertion point</i>) - 1)</tt>. The <i>insertion point</i> is defined as the
     *     point at which the key would be inserted into the array: the index of the first element
     *     greater than the key, or <tt>a.length</tt> if all elements in the array are less than the
     *     specified key. Note that this guarantees that the return value will be &gt;= 0 if and
     *     only if the key is found.
     */
    private int search(String key) {
      if (count == 0) {
        return 0;
      }
      int l = 0;
      int r = count;
      int m;
      while (l < r) {
        m = (l + r) >>> 1;
        String k = getEncodedKey(m);
        int compared = key.compareTo(k);
        if (compared < 0) {
          r = m;
        } else if (compared > 0) {
          l = m + 1;
        } else {
          return m;
        }
      }
      return -l - 1;
    }

    @Override
    public String toString() {
      return "KeyReferences{"
          + "kvs="
          + Arrays.stream(entries)
              .limit(count)
              .mapToObj(this::getKeyValue)
              .collect(Collectors.toList())
          + ", numOfSortedValues="
          + numOfSortedEntries
          + '}';
    }
  }

  LeafNode(
      Function<String, String> validAnchorKeyProvider,
      String anchorKey,
      int maxSize,
      @Nullable LeafNode<K, T> left,
      @Nullable LeafNode<K, T> right) {
    this.validAnchorKeyProvider = validAnchorKeyProvider;
    this.anchorKey = anchorKey;
    this.maxSize = maxSize;
    keyValues = new KeyValues<>(maxSize);
    tags = new Tags<>(maxSize, keyValues);
    keyReferences = new KeyReferences<>(maxSize, keyValues);
    this.left = left;
    this.right = right;
  }

  @Nullable
  LeafNode<K, T> getLeft() {
    return left;
  }

  @Nullable
  LeafNode<K, T> getRight() {
    return right;
  }

  private void setLeft(@Nullable LeafNode<K, T> left) {
    this.left = left;
  }

  private void setRight(@Nullable LeafNode<K, T> right) {
    this.right = right;
  }

  private String getKeyByKeyRefIndex(int keyRefIndex) {
    return keyReferences.getEncodedKey(keyRefIndex);
  }

  private static short calculateKeyHash(String key) {
    return (short) (0x7FFF & key.hashCode());
  }

  @Nullable
  private <R> R pointSearchLeaf(
      String encodedKey, BiFunction<KeyValue<K, T>, Integer, R> kvAndTagIndexReceivingFunc) {
    short keyHash = calculateKeyHash(encodedKey);
    int leafSize = keyValues.size();
    int tagIndex = keyHash * leafSize / (Short.MAX_VALUE + 1);
    while (tagIndex > 0 && keyHash <= tags.getHashTag(tagIndex - 1)) {
      tagIndex--;
    }
    while (tagIndex < leafSize && tags.getHashTag(tagIndex) < keyHash) {
      tagIndex++;
    }
    while (tagIndex < leafSize && tags.getHashTag(tagIndex) == keyHash) {
      KeyValue<K, T> kv = tags.getKeyValue(tagIndex);
      if (kv.getEncodedKey().equals(encodedKey)) {
        return kvAndTagIndexReceivingFunc.apply(kv, tagIndex);
      }
      tagIndex++;
    }
    return null;
  }

  @Nullable
  KeyValue<K, T> pointSearchLeaf(String encodedKey) {
    return pointSearchLeaf(encodedKey, (kv, tagIndex) -> kv);
  }

  void incSort() {
    if (!keyReferences.isSorted()) {
      keyReferences.sort();
    }
  }

  private Tuple<LeafNode<K, T>, List<Integer>> copyToNewLeafNode(
      String newAnchor, int startKeyRefIndex) {
    if (!keyReferences.isSorted()) {
      throw new AssertionError(
          String.format(
              "The leaf node doesn't seem to be sorted. Key references: %s", keyReferences));
    }

    int currentSize = keyValues.size();

    // Copy entries to a new leaf node.
    LeafNode<K, T> newLeafNode =
        new LeafNode<>(validAnchorKeyProvider, newAnchor, maxSize, this, this.right);
    List<Integer> keyValueIndexListOfNewLeafNode = new ArrayList<>(currentSize);
    for (int i = startKeyRefIndex; i < currentSize; i++) {
      int keyValueIndex = keyReferences.getKeyValueIndex(i);
      KeyValue<K, T> kv = keyValues.get(keyValueIndex);
      newLeafNode.keyValues.add(kv);
      keyValueIndexListOfNewLeafNode.add(keyValueIndex);
      int newLeafNodeLastIndex = newLeafNode.keyValues.getLastIndex();
      newLeafNode.tags.addWithoutSort(newLeafNodeLastIndex, kv);
      newLeafNode.keyReferences.add(newLeafNodeLastIndex);
    }
    // TODO: Optimize building `tags`.
    newLeafNode.tags.sort();
    // The key references are not sorted.

    LeafNode<K, T> rightLeafNode = getRight();
    if (rightLeafNode != null) {
      rightLeafNode.setLeft(newLeafNode);
    }

    setRight(newLeafNode);

    return new Tuple<>(newLeafNode, keyValueIndexListOfNewLeafNode);
  }

  private void removeMovedEntries(List<Integer> keyValueIndexListOfNewLeafNode) {
    boolean[] toRemove = new boolean[keyValues.size()];
    for (int index : keyValueIndexListOfNewLeafNode) {
      toRemove[index] = true;
    }

    KeyValues<K, T> tmpNewKeyValues =
        new KeyValues<>(keyValues.size() - keyValueIndexListOfNewLeafNode.size());
    for (int i = 0; i < keyValues.size(); i++) {
      if (!toRemove[i]) {
        tmpNewKeyValues.add(keyValues.get(i));
      }
    }
    keyValues.clear();
    keyValues.addAll(tmpNewKeyValues);

    tags.clear();
    keyReferences.clear();
    for (int i = 0; i < keyValues.size(); i++) {
      KeyValue<K, T> keyValue = keyValues.get(i);
      tags.addWithoutSort(i, keyValue);
      keyReferences.add(i);
    }
    // TODO: Optimize building `tags`.
    tags.sort();
  }

  Tuple<String, LeafNode<K, T>> splitToNewLeafNode() {
    incSort();

    Tuple<Integer, String> found = findSplitPositionAndNewAnchorInLeafNode();
    int splitPosIndex = found.first;
    String newAnchor = found.second;

    Tuple<LeafNode<K, T>, List<Integer>> copied = copyToNewLeafNode(newAnchor, splitPosIndex);
    LeafNode<K, T> newLeafNode = copied.first;
    List<Integer> keyValuesIndexListOfNewLeafNode = copied.second;

    removeMovedEntries(keyValuesIndexListOfNewLeafNode);

    return new Tuple<>(newAnchor, newLeafNode);
  }

  int size() {
    return keyValues.size();
  }

  @Override
  public String toString() {
    return "LeafNode{"
        + "anchorKey='"
        + Utils.printableKey(anchorKey)
        + '\''
        + ", maxSize="
        + maxSize
        + ", keyValues="
        + keyValues
        + ", tags="
        + tags
        + ", keyReferences="
        + keyReferences
        + ", left="
        + (left == null ? "null" : Utils.printableKey(left.anchorKey))
        + ", right="
        + (right == null ? "null" : Utils.printableKey(right.anchorKey))
        + '}';
  }

  boolean iterateKeyValues(
      @Nullable String startKey,
      @Nullable String endKey,
      boolean isEndKeyExclusive,
      Function<KeyValue<K, T>, Boolean> function) {
    int startIndexInclusive;
    if (startKey == null) {
      startIndexInclusive = 0;
    } else {
      startIndexInclusive = keyReferences.search(startKey);
      if (startIndexInclusive < 0) {
        startIndexInclusive = -startIndexInclusive - 1;
      }
    }

    int endIndexInclusive;
    if (endKey == null) {
      endIndexInclusive = size() - 1;
    } else {
      endIndexInclusive = keyReferences.search(endKey);
      if (endIndexInclusive >= 0) {
        if (isEndKeyExclusive) {
          endIndexInclusive--;
        }
      } else {
        endIndexInclusive = -endIndexInclusive - 1;
        endIndexInclusive--;
      }
    }

    boolean fullyIterated =
        keyReferences.iterateKeyValues(startIndexInclusive, endIndexInclusive, function);
    if (!fullyIterated) {
      return false;
    }
    return endIndexInclusive >= size() - 1;
  }

  void add(Key<K> key, T value) {
    KeyValue<K, T> keyValue = new KeyValue<>(key, value);
    keyValues.add(keyValue);
    tags.addWithSort(keyValues.size() - 1, keyValue);
    // Sorting this will be delayed until range scan or split.
    keyReferences.add(keyValues.getLastIndex());
  }

  boolean delete(String key) {
    incSort();
    int keyReferenceIndex = keyReferences.search(key);
    if (keyReferenceIndex < 0) {
      return false;
    }
    Integer tagIndex = pointSearchLeaf(key, (kv, tagIdx) -> tagIdx);
    assert tagIndex != null;
    int keyValueIndex = tags.getKeyValueIndex(tagIndex);

    keyReferences.remove(keyReferenceIndex);
    tags.remove(tagIndex);
    keyValues.remove(keyValueIndex);

    return true;
  }

  void merge(LeafNode<K, T> right) {
    keyValues.addAll(right.keyValues);
    tags.clear();
    keyReferences.clear();
    for (int i = 0; i < keyValues.size(); i++) {
      tags.addWithoutSort(i, keyValues.get(i));
      keyReferences.add(i);
    }
    // TODO: Optimize building `tags`.
    tags.sort();

    if (right.getRight() != null) {
      right.getRight().setLeft(this);
    }
    setRight(right.getRight());
  }

  private Tuple<Integer, String> findSplitPositionAndNewAnchorInLeafNode() {
    for (int i = size() / 2; i < size(); i++) {
      assert i > 0;
      String k1 = getKeyByKeyRefIndex(i - 1);
      String k2 = getKeyByKeyRefIndex(i);

      String lcp = Utils.extractLongestCommonPrefix(k1, k2);
      String newAnchor = lcp + k2.charAt(lcp.length());

      // Check the anchor key ordering condition: left-key < anchor-key ≤ node-key
      if (newAnchor.compareTo(k1) <= 0) {
        continue;
      }
      // For anchor-key ≤ node-key, the relationship of `newAnchor` and `k2` always satisfy it.

      // Check the anchor key prefix condition.
      String validatedAnchorKey = validAnchorKeyProvider.apply(newAnchor);
      if (validatedAnchorKey == null) {
        continue;
      }
      return new Tuple<>(i, validatedAnchorKey);
    }
    throw new IllegalStateException("Cannot split the leaf node. Leaf node: " + this);
  }

  void validate() {
    String normalizedAnchorKey = anchorKey;
    if (normalizedAnchorKey.endsWith(SMALLEST_TOKEN)) {
      normalizedAnchorKey = normalizedAnchorKey.substring(0, normalizedAnchorKey.length() - 1);
    }
    String normalizedRightAnchorKey = null;
    if (right != null) {
      normalizedRightAnchorKey = right.anchorKey;
      if (normalizedRightAnchorKey.endsWith(SMALLEST_TOKEN)) {
        normalizedRightAnchorKey =
            normalizedRightAnchorKey.substring(0, normalizedRightAnchorKey.length() - 1);
      }
    }

    for (int i = 0; i < size(); i++) {
      KeyValue<K, T> kv = keyValues.get(i);
      if (kv.getEncodedKey().compareTo(normalizedAnchorKey) < 0) {
        throw new AssertionError(
            String.format(
                "The key is smaller than the anchor key. Key: %s, Anchor key: %s",
                kv.getKey(), normalizedAnchorKey));
      }
      if (normalizedRightAnchorKey != null
          && normalizedRightAnchorKey.compareTo(kv.getEncodedKey()) < 0) {
        throw new AssertionError(
            String.format(
                "The anchor key of the right leaf node is smaller than the key. Key: %s, Right leaf node's anchor key: %s",
                Utils.printableKey(kv.getEncodedKey()), normalizedRightAnchorKey));
      }
    }

    if (tags.size() != size()) {
      throw new AssertionError(
          String.format(
              "The number of tags is different from the number of keys. Keys: %s, Tags: %s",
              Arrays.stream(keyValues.entries)
                  .limit(keyValues.count)
                  .map(kv -> kv.getKey().toString())
                  .collect(Collectors.toList()),
              tags));
    }
    Set<Integer> keyValueIndexes = new HashSet<>(size());
    for (int i = 0; i < size(); i++) {
      if (i < size() - 1) {
        if (tags.getHashTag(i) > tags.getHashTag(i + 1)) {
          throw new AssertionError(String.format("The tags are not sorted. Tags: %s", tags));
        }
      }
      int keyValueIndex = tags.getKeyValueIndex(i);
      if (keyValueIndex < 0 || keyValueIndex >= size()) {
        throw new AssertionError(
            String.format(
                "The key-value index is out-of-range. Tags: %s, Index: %d, Key-value index: %d",
                tags, i, keyValueIndex));
      }
      if (!keyValueIndexes.add(keyValueIndex)) {
        throw new AssertionError(
            String.format(
                "The tags contains duplicated key-value index. Tags: %s, Index: %d, Key-value index: %d",
                tags, i, keyValueIndex));
      }
    }

    if (keyReferences.size() != size()) {
      throw new AssertionError(
          String.format(
              "The number of key references is different from the number of keys. Keys: %s, Key references: %s",
              Arrays.stream(keyValues.entries)
                  .limit(keyValues.count)
                  .map(kv -> kv.getKey().toString())
                  .collect(Collectors.toList()),
              keyReferences));
    }

    if (keyReferences.numOfSortedEntries > keyReferences.size()) {
      throw new AssertionError(
          String.format(
              "The number of sorted key references is larger than the number of key references. Key references: %s",
              keyReferences));
    }

    for (int i = 0; i < size(); i++) {
      if (i > 0 && i < keyReferences.getNumOfSortedEntries() - 1) {
        if (keyReferences.getEncodedKey(i).compareTo(keyReferences.getEncodedKey(i + 1)) > 0) {
          throw new AssertionError(
              String.format(
                  "The key references are not ordered. Key references: %s", keyReferences));
        }
      }
    }
  }
}
