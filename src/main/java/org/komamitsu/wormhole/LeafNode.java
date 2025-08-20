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

package org.komamitsu.wormhole;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

class LeafNode<T> {
  public final String anchorKey;
  private final int maxSize;
  private final List<KeyValue<T>> keyValues;
  // All references are always sorted by hash.
  private final Tags<T> tags;
  // Some references are sorted by key.
  private final KeyReferences<T> keyReferences;

  @Nullable private LeafNode<T> left;
  @Nullable private LeafNode<T> right;

  // Tags
  private static class Tags<T> {
    private int count;
    private final List<KeyValue<T>> keyValues;
    private final int[] values;

    private Tags(int maxSize, List<KeyValue<T>> keyValues) {
      this.values = new int[maxSize];
      this.keyValues = keyValues;
    }

    private int calcTag(int keyValueIndex, KeyValue<T> keyValue) {
      assert keyValueIndex <= 0xFFFF;
      String key = keyValue.getKey();
      return calculateKeyHash(key) << 16 | keyValueIndex;
    }

    private void addWithoutSort(int keyValueIndex, KeyValue<T> keyValue) {
      int tag = calcTag(keyValueIndex, keyValue);
      values[count] = tag;
      count++;
    }

    private void addWithSort(int keyValueIndex, KeyValue<T> keyValue) {
      int tag = calcTag(keyValueIndex, keyValue);
      int index;
      if (count == 0) {
        index = 0;
      } else {
        index = Arrays.binarySearch(values, 0, count, tag);
        if (index < 0) {
          index = -(index + 1);
        }
        System.arraycopy(values, index, values, index + 1, count - index);
      }
      values[index] = tag;
      count++;
    }

    private void sort() {
      Arrays.sort(values, 0, count);
    }

    private void remove(int index) {
      int keyValueIndex = getKeyValueIndex(index);

      if (index < count - 1) {
        System.arraycopy(values, index + 1, values, index, (count - 1) - index);
      }
      count--;

      // Decrement key value indexes if needed.
      for (int i = 0; i < count; i++) {
        int kvIndex = getKeyValueIndex(i);
        assert kvIndex != keyValueIndex;
        if (kvIndex > keyValueIndex) {
          values[i] = (values[i] & 0xFFFF0000) | ((kvIndex - 1) & 0x0000FFFF);
        }
      }
    }

    private int getHashTag(int index) {
      return values[index] >> 16;
    }

    private int getKeyValueIndex(int index) {
      return values[index] & 0xFFFF;
    }

    private KeyValue<T> getKeyValue(int index) {
      return keyValues.get(getKeyValueIndex(index));
    }

    private String getKey(int index) {
      return keyValues.get(getKeyValueIndex(index)).getKey();
    }

    private void clear() {
      count = 0;
    }

    private int size() {
      return count;
    }

    private int getLastIndex() {
      return count - 1;
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

  // Key reference
  private static class KeyReferences<T> {
    private final Tags<T> tags;
    private int count;
    private final int[] values;
    private int numOfSortedValues;

    private KeyReferences(int maxSize, Tags<T> tags) {
      this.tags = tags;
      this.values = new int[maxSize];
    }

    private void add(int tagIndex) {
      values[count] = tagIndex;
      count++;
    }

    private void addAll(KeyReferences<T> src) {
      System.arraycopy(src.values, 0, values, count, src.count);
    }

    private int getTagIndex(int index) {
      return values[index];
    }

    private String getKey(int index) {
      return tags.getKey(values[index]);
    }

    private KeyValue<T> getKeyValue(int index) {
      return tags.getKeyValue(values[index]);
    }

    private void partialSort(int low, int high) {
      if (low >= high) {
        return;
      }
      String pivot = getKey(low + high >> 1);
      int l = low;
      int h = high;
      while (l <= h) {
        while (getKey(l).compareTo(pivot) < 0) {
          l++;
        }
        while (getKey(h).compareTo(pivot) > 0) {
          h--;
        }
        if (l <= h) {
          int tmp = values[l];
          values[l] = values[h];
          values[h] = tmp;
          l++;
          h--;
        }
      }
      partialSort(low, h);
      partialSort(l, high);
    }

    void sort() {
      partialSort(numOfSortedValues, count - 1);

      // Merge sorted and unsorted key references.
      int[] mergedValues = new int[count];
      int mergedValuesIndex = 0;

      int idxForSortedKeyRef = 0;
      int idxForUnsortedKeyRef = numOfSortedValues;
      String keyFromSortedKeyRef = null;
      String keyFromUnsortedKeyRef = null;
      while (true) {
        if (keyFromSortedKeyRef == null && idxForSortedKeyRef < numOfSortedValues) {
          keyFromSortedKeyRef = getKey(idxForSortedKeyRef);
        }
        if (keyFromUnsortedKeyRef == null && idxForUnsortedKeyRef < count) {
          keyFromUnsortedKeyRef = getKey(idxForUnsortedKeyRef);
        }

        int keyValueIndex;
        if (keyFromSortedKeyRef != null) {
          if (keyFromUnsortedKeyRef != null) {
            if (keyFromSortedKeyRef.compareTo(keyFromUnsortedKeyRef) < 0) {
              keyValueIndex = values[idxForSortedKeyRef++];
              keyFromSortedKeyRef = null;
            } else {
              keyValueIndex = values[idxForUnsortedKeyRef++];
              keyFromUnsortedKeyRef = null;
            }
          } else {
            keyValueIndex = values[idxForSortedKeyRef++];
            keyFromSortedKeyRef = null;
          }
        } else {
          if (keyFromUnsortedKeyRef != null) {
            keyValueIndex = values[idxForUnsortedKeyRef++];
            keyFromUnsortedKeyRef = null;
          } else {
            break;
          }
        }
        mergedValues[mergedValuesIndex++] = keyValueIndex;
      }
      System.arraycopy(mergedValues, 0, values, 0, count);
      markAsSorted();
    }

    private int getTag(int index) {
      return values[index];
    }

    private void clear() {
      count = 0;
      numOfSortedValues = 0;
    }

    private int getNumOfSortedValues() {
      return numOfSortedValues;
    }

    private void remove(int index) {
      int tagIndex = values[index];
      if (index < count - 1) {
        System.arraycopy(values, index + 1, values, index, (count - 1) - index);
      }
      count--;
      if (index < numOfSortedValues) {
        numOfSortedValues--;
      }
      // Decrement tag indexes if needed.
      for (int i = 0; i < count; i++) {
        if (values[i] > tagIndex) {
          values[i]--;
        }
      }
    }

    private boolean isSorted() {
      return count == numOfSortedValues;
    }

    private void markAsSorted() {
      numOfSortedValues = count;
    }

    private int size() {
      return count;
    }

    private boolean iterateKeyValues(
        int startIndexInclusive, int endIndexInclusive, Function<KeyValue<T>, Boolean> function) {
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
        m = (l + r) / 2;
        String k = getKey(m);
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
          + Arrays.stream(values)
              .limit(count)
              .mapToObj(this::getKeyValue)
              .collect(Collectors.toList())
          + ", numOfSortedValues="
          + numOfSortedValues
          + '}';
    }
  }

  LeafNode(String anchorKey, int maxSize, @Nullable LeafNode<T> left, @Nullable LeafNode<T> right) {
    this.anchorKey = anchorKey;
    this.maxSize = maxSize;
    keyValues = new ArrayList<>(maxSize);
    tags = new Tags<>(maxSize, keyValues);
    keyReferences = new KeyReferences<>(maxSize, tags);
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

  private static short calculateKeyHash(String key) {
    return (short) (0x7FFF & key.hashCode());
  }

  @Nullable
  KeyValue<T> pointSearchLeaf(String key) {
    short keyHash = calculateKeyHash(key);
    int leafSize = keyValues.size();
    int tagIndex = keyHash * leafSize / (Short.MAX_VALUE + 1);
    while (tagIndex > 0 && keyHash <= tags.getHashTag(tagIndex - 1)) {
      tagIndex--;
    }
    while (tagIndex < leafSize && tags.getHashTag(tagIndex) < keyHash) {
      tagIndex++;
    }
    while (tagIndex < leafSize && tags.getHashTag(tagIndex) == keyHash) {
      KeyValue<T> kv = tags.getKeyValue(tagIndex);
      if (kv.getKey().equals(key)) {
        return kv;
      }
      tagIndex++;
    }
    return null;
  }

  void incSort() {
    if (!keyReferences.isSorted()) {
      keyReferences.sort();
    }
  }

  private Tuple<LeafNode<T>, List<Integer>> copyToNewLeafNode(
      String newAnchor, int startKeyRefIndex) {
    if (!keyReferences.isSorted()) {
      throw new AssertionError(
          String.format(
              "The leaf node doesn't seem to be sorted. Key references: %s", keyReferences));
    }

    int currentSize = keyValues.size();

    // Copy entries to a new leaf node.
    LeafNode<T> newLeafNode = new LeafNode<>(newAnchor, maxSize, this, this.right);
    List<Integer> keyValueIndexListOfNewLeafNode = new ArrayList<>(currentSize);
    for (int i = startKeyRefIndex; i < currentSize; i++) {
      int keyValueIndex = tags.getKeyValueIndex(keyReferences.getTagIndex(i));
      KeyValue<T> kv = keyValues.get(keyValueIndex);
      newLeafNode.keyValues.add(kv);
      keyValueIndexListOfNewLeafNode.add(keyValueIndex);
      newLeafNode.tags.addWithoutSort(newLeafNode.keyValues.size() - 1, kv);
      newLeafNode.keyReferences.add(newLeafNode.tags.getLastIndex());
    }
    newLeafNode.tags.sort();
    // The key references are not sorted.

    LeafNode<T> rightLeafNode = getRight();
    if (rightLeafNode != null) {
      rightLeafNode.setLeft(newLeafNode);
    }

    setRight(newLeafNode);

    return new Tuple<>(newLeafNode, keyValueIndexListOfNewLeafNode);
  }

  private void removeMovedEntries(List<Integer> keyValueIndexListOfNewLeafNode) {
    Collections.sort(keyValueIndexListOfNewLeafNode);
    for (int i = keyValueIndexListOfNewLeafNode.size() - 1; i >= 0; i--) {
      int keyValueIndex = keyValueIndexListOfNewLeafNode.get(i);
      keyValues.remove(keyValueIndex);
    }

    tags.clear();
    keyReferences.clear();
    for (int i = 0; i < keyValues.size(); i++) {
      KeyValue<T> keyValue = keyValues.get(i);
      tags.addWithoutSort(i, keyValue);
      keyReferences.add(i);
    }
    tags.sort();
  }

  LeafNode<T> splitToNewLeafNode(String newAnchor, int startKeyRefIndex) {
    Tuple<LeafNode<T>, List<Integer>> copied = copyToNewLeafNode(newAnchor, startKeyRefIndex);
    LeafNode<T> newLeafNode = copied.first;
    List<Integer> keyValuesIndexListOfNewLeafNode = copied.second;

    removeMovedEntries(keyValuesIndexListOfNewLeafNode);

    return newLeafNode;
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
      Function<KeyValue<T>, Boolean> function) {
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

  void add(String key, T value) {
    KeyValue<T> keyValue = new KeyValue<>(key, value);
    keyValues.add(keyValue);
    tags.addWithSort(keyValues.size() - 1, keyValue);
    // Sorting this will be delayed until range scan or split.
    keyReferences.add(tags.getLastIndex());
  }

  boolean delete(String key) {
    incSort();
    int keyReferenceIndex = keyReferences.search(key);
    if (keyReferenceIndex < 0) {
      return false;
    }
    int tagIndex = keyReferences.getTagIndex(keyReferenceIndex);
    int keyValueIndex = tags.getKeyValueIndex(tagIndex);

    keyReferences.remove(keyReferenceIndex);
    tags.remove(tagIndex);
    keyValues.remove(keyValueIndex);

    return true;
  }

  void merge(LeafNode<T> right) {
    keyValues.addAll(right.keyValues);
    tags.clear();
    keyReferences.clear();
    for (int i = 0; i < keyValues.size(); i++) {
      tags.addWithoutSort(i, keyValues.get(i));
      keyReferences.add(tags.getLastIndex());
    }
    tags.sort();

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
        normalizedRightAnchorKey =
            normalizedRightAnchorKey.substring(0, normalizedRightAnchorKey.length() - 1);
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
              keyValues.stream()
                  .map(kv -> Utils.printableKey(kv.getKey()))
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
              keyValues.stream()
                  .map(kv -> Utils.printableKey(kv.getKey()))
                  .collect(Collectors.toList()),
              keyReferences));
    }

    if (keyReferences.numOfSortedValues > keyReferences.size()) {
      throw new AssertionError(
          String.format(
              "The number of sorted key references is larger than the number of key references. Key references: %s",
              keyReferences));
    }

    for (int i = 0; i < size(); i++) {
      if (i > 0 && i < keyReferences.getNumOfSortedValues() - 1) {
        if (keyReferences.getKey(i).compareTo(keyReferences.getKey(i + 1)) > 0) {
          throw new AssertionError(
              String.format(
                  "The key references are not ordered. Key references: %s", keyReferences));
        }
      }
    }
  }
}
