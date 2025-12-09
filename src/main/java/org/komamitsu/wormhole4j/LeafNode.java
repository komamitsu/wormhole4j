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

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

final class LeafNode<K, V> {
  private final EncodedKeyType encodedKeyType;
  final Object anchorKey;
  private final int maxSize;
  private final Function<Object, Object> validAnchorKeyProvider;

  @Nullable private LeafNode<K, V> left;
  @Nullable private LeafNode<K, V> right;

  // For KeyValues
  private int keyValuesCount;
  private final KeyValue<K, V>[] keyValues;

  private KeyValue<K, V> getKeyValue(int index) {
    return keyValues[index];
  }

  private int keyValuesCount() {
    return keyValuesCount;
  }

  private int getLastKeyValueIndex() {
    return keyValuesCount - 1;
  }

  private void clearKeyValues() {
    keyValuesCount = 0;
    Arrays.fill(keyValues, null);
  }

  private void addAllKeyValues(int keyValuesCount, KeyValue<K, V>[] keyValues) {
    System.arraycopy(keyValues, 0, this.keyValues, this.keyValuesCount, keyValuesCount);
    this.keyValuesCount += keyValuesCount;
  }

  private void addKeyValue(KeyValue<K, V> kv) {
    keyValues[keyValuesCount] = kv;
    keyValuesCount++;
  }

  private void removeKeyValue(int index) {
    if (index < keyValuesCount - 1) {
      System.arraycopy(keyValues, index + 1, keyValues, index, keyValuesCount - 1 - index);
    }
    keyValues[keyValuesCount - 1] = null;
    keyValuesCount--;
  }

  private String keyValuesToString() {
    StringBuilder sb = new StringBuilder();
    sb.append("KeyValues=[");
    boolean isFirst = true;
    for (int i = 0; i < keyValuesCount; i++) {
      if (isFirst) {
        isFirst = false;
      } else {
        sb.append(", ");
      }
      sb.append(keyValues[i]);
    }
    sb.append("]");
    return sb.toString();
  }

  // For Tags
  private int tagsCount;
  private final int[] tags;

  private int calcTag(int keyValueIndex, KeyValue<K, V> keyValue) {
    assert keyValueIndex <= 0xFFFF;
    Object key = keyValue.getEncodedKey();
    return calculateKeyHash(key) << 16 | keyValueIndex;
  }

  private void addTagWithoutSort(int keyValueIndex, KeyValue<K, V> keyValue) {
    int tag = calcTag(keyValueIndex, keyValue);
    tags[tagsCount] = tag;
    tagsCount++;
  }

  private void addTagWithSort(int keyValueIndex, KeyValue<K, V> keyValue) {
    int tag = calcTag(keyValueIndex, keyValue);
    int index;
    if (tagsCount == 0) {
      index = 0;
    } else {
      index = Arrays.binarySearch(tags, 0, tagsCount, tag);
      if (index < 0) {
        index = -(index + 1);
      }
      System.arraycopy(tags, index, tags, index + 1, tagsCount - index);
    }
    tags[index] = tag;
    tagsCount++;
  }

  private void sortTags() {
    Arrays.sort(tags, 0, tagsCount);
  }

  private void removeTag(int index) {
    int keyValueIndex = getKeyValueIndexFromTag(index);

    if (index < tagsCount - 1) {
      System.arraycopy(tags, index + 1, tags, index, (tagsCount - 1) - index);
    }
    tagsCount--;

    // Decrement key value indexes if needed.
    for (int i = 0; i < tagsCount; i++) {
      int kvIndex = getKeyValueIndexFromTag(i);
      assert kvIndex != keyValueIndex;
      if (kvIndex > keyValueIndex) {
        tags[i] = (tags[i] & 0xFFFF0000) | ((kvIndex - 1) & 0x0000FFFF);
      }
    }
  }

  private int getHashTag(int index) {
    return tags[index] >> 16;
  }

  private int getKeyValueIndexFromTag(int index) {
    return tags[index] & 0xFFFF;
  }

  private KeyValue<K, V> getKeyValueFromTag(int index) {
    return getKeyValue(getKeyValueIndexFromTag(index));
  }

  private void clearTags() {
    tagsCount = 0;
  }

  private int tagsSize() {
    return tagsCount;
  }

  private String tagsToString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Tags{values=[");
    boolean isFirst = true;
    for (int i = 0; i < tagsCount; i++) {
      if (isFirst) {
        isFirst = false;
      } else {
        sb.append(", ");
      }
      sb.append("{hash=");
      sb.append(getHashTag(i));
      sb.append(", keyValue=");
      sb.append(getKeyValueFromTag(i));
      sb.append("}");
    }
    sb.append("]");
    return sb.toString();
  }

  // For KeyRefs

  private int keyRefsCount;
  private final int[] keyRefs;
  private int numOfSortedKeyRefs;

  private void addKeyReference(int keyValueIndex) {
    keyRefs[keyRefsCount] = keyValueIndex;
    keyRefsCount++;
  }

  private int getKeyValueIndexFromKeyReference(int index) {
    return keyRefs[index];
  }

  private Object getEncodedKeyFromKeyRef(int index) {
    return getKeyValue(getKeyValueIndexFromKeyReference(index)).getEncodedKey();
  }

  private KeyValue<K, V> getKeyValueFromKeyRef(int index) {
    return getKeyValue(getKeyValueIndexFromKeyReference(index));
  }

  private void partiallySortKeyRefs(int low, int high) {
    if (low >= high) {
      return;
    }
    Object pivot = getEncodedKeyFromKeyRef((low + high) >>> 1);
    int l = low;
    int h = high;
    while (l <= h) {
      while (EncodedKeyUtils.compare(encodedKeyType, getEncodedKeyFromKeyRef(l), pivot) < 0) {
        l++;
      }
      while (EncodedKeyUtils.compare(encodedKeyType, getEncodedKeyFromKeyRef(h), pivot) > 0) {
        h--;
      }
      if (l <= h) {
        int tmp = keyRefs[l];
        keyRefs[l] = keyRefs[h];
        keyRefs[h] = tmp;
        l++;
        h--;
      }
    }
    partiallySortKeyRefs(low, h);
    partiallySortKeyRefs(l, high);
  }

  private void sortKeyRefs() {
    partiallySortKeyRefs(numOfSortedKeyRefs, keyRefsCount - 1);

    // Merge sorted and unsorted key references.
    int[] mergedValues = new int[keyRefsCount];
    int mergedValuesIndex = 0;

    int idxForSortedKeyRef = 0;
    int idxForUnsortedKeyRef = numOfSortedKeyRefs;
    Object keyFromSortedKeyRef = null;
    Object keyFromUnsortedKeyRef = null;
    while (true) {
      if (keyFromSortedKeyRef == null && idxForSortedKeyRef < numOfSortedKeyRefs) {
        keyFromSortedKeyRef = getEncodedKeyFromKeyRef(idxForSortedKeyRef);
      }
      if (keyFromUnsortedKeyRef == null && idxForUnsortedKeyRef < keyRefsCount) {
        keyFromUnsortedKeyRef = getEncodedKeyFromKeyRef(idxForUnsortedKeyRef);
      }

      int keyValueIndex;
      if (keyFromSortedKeyRef != null) {
        if (keyFromUnsortedKeyRef != null) {
          if (EncodedKeyUtils.compare(encodedKeyType, keyFromSortedKeyRef, keyFromUnsortedKeyRef)
              < 0) {
            keyValueIndex = keyRefs[idxForSortedKeyRef++];
            keyFromSortedKeyRef = null;
          } else {
            keyValueIndex = keyRefs[idxForUnsortedKeyRef++];
            keyFromUnsortedKeyRef = null;
          }
        } else {
          keyValueIndex = keyRefs[idxForSortedKeyRef++];
          keyFromSortedKeyRef = null;
        }
      } else {
        if (keyFromUnsortedKeyRef != null) {
          keyValueIndex = keyRefs[idxForUnsortedKeyRef++];
          keyFromUnsortedKeyRef = null;
        } else {
          break;
        }
      }
      mergedValues[mergedValuesIndex++] = keyValueIndex;
    }
    System.arraycopy(mergedValues, 0, keyRefs, 0, keyRefsCount);
    markKeyRefsAsSorted();
  }

  private void clearKeyRefs() {
    keyRefsCount = 0;
    numOfSortedKeyRefs = 0;
  }

  private int getNumOfSortedKeyRefs() {
    return numOfSortedKeyRefs;
  }

  private void removeKeyRef(int index) {
    int tagIndex = keyRefs[index];
    if (index < keyRefsCount - 1) {
      System.arraycopy(keyRefs, index + 1, keyRefs, index, (keyRefsCount - 1) - index);
    }
    keyRefsCount--;
    if (index < numOfSortedKeyRefs) {
      numOfSortedKeyRefs--;
    }
    // Decrement tag indexes if needed.
    for (int i = 0; i < keyRefsCount; i++) {
      if (keyRefs[i] > tagIndex) {
        keyRefs[i]--;
      }
    }
  }

  private boolean isKeyRefsSorted() {
    return keyRefsCount == numOfSortedKeyRefs;
  }

  private void markKeyRefsAsSorted() {
    numOfSortedKeyRefs = keyRefsCount;
  }

  private int keyRefsSize() {
    return keyRefsCount;
  }

  private boolean iterateKeyValues(
      int startIndexInclusive, int endIndexInclusive, Function<KeyValue<K, V>, Boolean> function) {
    for (int i = startIndexInclusive; i <= endIndexInclusive; i++) {
      KeyValue<K, V> keyValue = getKeyValueFromKeyRef(i);
      if (!function.apply(keyValue)) {
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
   *     specified key. Note that this guarantees that the return value will be &gt;= 0 if and only
   *     if the key is found.
   */
  private int searchKeyRefs(Object key) {
    int l = 0;
    int r = keyRefsCount;
    int m;
    while (l < r) {
      m = (l + r) >>> 1;
      Object k = getEncodedKeyFromKeyRef(m);
      int compared = EncodedKeyUtils.compare(encodedKeyType, key, k);
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

  private String keyRefsToString() {
    return "KeyRefs{"
        + "keyValues="
        + Arrays.stream(keyRefs)
            .limit(keyRefsCount)
            .mapToObj(this::getKeyValue)
            .collect(Collectors.toList())
        + ", numOfSortedValues="
        + numOfSortedKeyRefs
        + '}';
  }

  @SuppressWarnings("unchecked")
  LeafNode(
      EncodedKeyType encodedKeyType,
      Function<Object, Object> validAnchorKeyProvider,
      Object anchorKey,
      int maxSize,
      @Nullable LeafNode<K, V> left,
      @Nullable LeafNode<K, V> right) {
    this.encodedKeyType = encodedKeyType;
    this.validAnchorKeyProvider = validAnchorKeyProvider;
    this.anchorKey = anchorKey;
    this.maxSize = maxSize;
    keyValues = new KeyValue[maxSize];
    tags = new int[maxSize];
    keyRefs = new int[maxSize];
    this.left = left;
    this.right = right;
  }

  @Nullable
  LeafNode<K, V> getLeft() {
    return left;
  }

  @Nullable
  LeafNode<K, V> getRight() {
    return right;
  }

  private void setLeft(@Nullable LeafNode<K, V> left) {
    this.left = left;
  }

  private void setRight(@Nullable LeafNode<K, V> right) {
    this.right = right;
  }

  private Object getKeyByKeyRefIndex(int keyRefIndex) {
    return getEncodedKeyFromKeyRef(keyRefIndex);
  }

  private static short calculateKeyHash(Object key) {
    return (short) (0x7FFF & key.hashCode());
  }

  @Nullable
  private <R> R pointSearchLeaf(
      Object encodedKey, BiFunction<KeyValue<K, V>, Integer, R> kvAndTagIndexReceivingFunc) {
    short keyHash = calculateKeyHash(encodedKey);
    int leafSize = keyValuesCount();
    int tagIndex = keyHash * leafSize / (Short.MAX_VALUE + 1);
    while (tagIndex > 0 && keyHash <= getHashTag(tagIndex - 1)) {
      tagIndex--;
    }
    while (tagIndex < leafSize && getHashTag(tagIndex) < keyHash) {
      tagIndex++;
    }
    while (tagIndex < leafSize && getHashTag(tagIndex) == keyHash) {
      KeyValue<K, V> kv = getKeyValueFromTag(tagIndex);
      if (kv.getEncodedKey().equals(encodedKey)) {
        return kvAndTagIndexReceivingFunc.apply(kv, tagIndex);
      }
      tagIndex++;
    }
    return null;
  }

  @Nullable
  KeyValue<K, V> pointSearchLeaf(Object encodedKey) {
    return pointSearchLeaf(encodedKey, (kv, tagIndex) -> kv);
  }

  void incSort() {
    if (!isKeyRefsSorted()) {
      sortKeyRefs();
    }
  }

  private Tuple<LeafNode<K, V>, List<Integer>> copyToNewLeafNode(
      Object newAnchor, int startKeyRefIndex) {
    if (!isKeyRefsSorted()) {
      throw new AssertionError(
          String.format(
              "The leaf node doesn't seem to be sorted. Key references: %s",
              Arrays.toString(keyRefs)));
    }

    int currentSize = keyValuesCount();

    // Copy entries to a new leaf node.
    LeafNode<K, V> newLeafNode =
        new LeafNode<>(
            encodedKeyType, validAnchorKeyProvider, newAnchor, maxSize, this, this.right);
    List<Integer> keyValueIndexListOfNewLeafNode = new ArrayList<>(currentSize);
    for (int i = startKeyRefIndex; i < currentSize; i++) {
      int keyValueIndex = getKeyValueIndexFromKeyReference(i);
      KeyValue<K, V> kv = getKeyValue(keyValueIndex);
      newLeafNode.addKeyValue(kv);
      keyValueIndexListOfNewLeafNode.add(keyValueIndex);
      int newLeafNodeLastIndex = newLeafNode.getLastKeyValueIndex();
      newLeafNode.addTagWithoutSort(newLeafNodeLastIndex, kv);
      newLeafNode.addKeyReference(newLeafNodeLastIndex);
    }
    // TODO: Optimize building `tags`.
    newLeafNode.sortTags();
    // The key references are not sorted.

    LeafNode<K, V> rightLeafNode = getRight();
    if (rightLeafNode != null) {
      rightLeafNode.setLeft(newLeafNode);
    }

    setRight(newLeafNode);

    return new Tuple<>(newLeafNode, keyValueIndexListOfNewLeafNode);
  }

  @SuppressWarnings("unchecked")
  private void removeMovedEntries(List<Integer> keyValueIndexListOfNewLeafNode) {
    boolean[] toRemove = new boolean[keyValuesCount()];
    for (int index : keyValueIndexListOfNewLeafNode) {
      toRemove[index] = true;
    }

    KeyValue<K, V>[] remainingKeyValues =
        new KeyValue[keyValuesCount() - keyValueIndexListOfNewLeafNode.size()];
    int remainingKeyValuesIndex = 0;
    for (int i = 0; i < keyValuesCount(); i++) {
      if (!toRemove[i]) {
        remainingKeyValues[remainingKeyValuesIndex++] = getKeyValue(i);
      }
    }
    clearKeyValues();
    addAllKeyValues(remainingKeyValues.length, remainingKeyValues);

    clearTags();
    clearKeyRefs();
    for (int i = 0; i < keyValuesCount(); i++) {
      KeyValue<K, V> keyValue = getKeyValue(i);
      addTagWithoutSort(i, keyValue);
      addKeyReference(i);
    }
    // TODO: Optimize building `tags`.
    sortTags();
  }

  Tuple<Object, LeafNode<K, V>> splitToNewLeafNode() {
    incSort();

    Tuple<Integer, Object> found = findSplitPositionAndNewAnchorInLeafNode();
    int splitPosIndex = found.first;
    Object newAnchor = found.second;

    Tuple<LeafNode<K, V>, List<Integer>> copied = copyToNewLeafNode(newAnchor, splitPosIndex);
    LeafNode<K, V> newLeafNode = copied.first;
    List<Integer> keyValuesIndexListOfNewLeafNode = copied.second;

    removeMovedEntries(keyValuesIndexListOfNewLeafNode);

    return new Tuple<>(newAnchor, newLeafNode);
  }

  int size() {
    return keyValuesCount();
  }

  @Override
  public String toString() {
    return "LeafNode{"
        + "anchorKey="
        + EncodedKeyUtils.toString(anchorKey)
        + ", maxSize="
        + maxSize
        + ", keyValues="
        + keyValuesToString()
        + ", tags="
        + tagsToString()
        + ", keyReferences="
        + keyRefsToString()
        + ", left="
        + (left == null ? "null" : EncodedKeyUtils.toString(left.anchorKey))
        + ", right="
        + (right == null ? "null" : EncodedKeyUtils.toString(right.anchorKey))
        + '}';
  }

  boolean iterateKeyValues(
      @Nullable Object startKey,
      @Nullable Object endKey,
      boolean isEndKeyExclusive,
      Function<KeyValue<K, V>, Boolean> function) {
    int startIndexInclusive;
    if (startKey == null) {
      startIndexInclusive = 0;
    } else {
      startIndexInclusive = searchKeyRefs(startKey);
      if (startIndexInclusive < 0) {
        startIndexInclusive = -startIndexInclusive - 1;
      }
    }

    int endIndexInclusive;
    if (endKey == null) {
      endIndexInclusive = size() - 1;
    } else {
      endIndexInclusive = searchKeyRefs(endKey);
      if (endIndexInclusive >= 0) {
        if (isEndKeyExclusive) {
          endIndexInclusive--;
        }
      } else {
        endIndexInclusive = -endIndexInclusive - 1;
        endIndexInclusive--;
      }
    }

    boolean fullyIterated = iterateKeyValues(startIndexInclusive, endIndexInclusive, function);
    if (!fullyIterated) {
      return false;
    }
    return endIndexInclusive >= size() - 1;
  }

  void add(Object encodedKey, K originalKey, V value) {
    KeyValue<K, V> keyValue =
        EncodedKeyUtils.createKeyValue(encodedKeyType, encodedKey, originalKey, value);
    addKeyValue(keyValue);
    addTagWithSort(keyValuesCount() - 1, keyValue);
    // Sorting this will be delayed until range scan or split.
    addKeyReference(getLastKeyValueIndex());
  }

  boolean delete(Object key) {
    incSort();
    int keyReferenceIndex = searchKeyRefs(key);
    if (keyReferenceIndex < 0) {
      return false;
    }
    Integer tagIndex = pointSearchLeaf(key, (kv, tagIdx) -> tagIdx);
    assert tagIndex != null;
    int keyValueIndex = getKeyValueIndexFromTag(tagIndex);

    removeKeyRef(keyReferenceIndex);
    removeTag(tagIndex);
    removeKeyValue(keyValueIndex);

    return true;
  }

  void merge(LeafNode<K, V> right) {
    addAllKeyValues(right.keyValuesCount, right.keyValues);
    clearTags();
    clearKeyRefs();
    for (int i = 0; i < keyValuesCount(); i++) {
      addTagWithoutSort(i, getKeyValue(i));
      addKeyReference(i);
    }
    // TODO: Optimize building `tags`.
    sortTags();

    if (right.getRight() != null) {
      right.getRight().setLeft(this);
    }
    setRight(right.getRight());
  }

  private Tuple<Integer, Object> findSplitPositionAndNewAnchorInLeafNode() {
    for (int i = size() / 2; i < size(); i++) {
      assert i > 0;
      Object k1 = getKeyByKeyRefIndex(i - 1);
      Object k2 = getKeyByKeyRefIndex(i);

      // Create a new anchor by extracting common longest prefix and appending the second key's
      // token.
      Object newAnchor = EncodedKeyUtils.createNewAnchorKey(encodedKeyType, k1, k2);

      // Check the anchor key ordering condition: left-key < anchor-key ≤ node-key
      if (EncodedKeyUtils.compare(encodedKeyType, newAnchor, k1) <= 0) {
        continue;
      }
      // For anchor-key ≤ node-key, the relationship of `newAnchor` and `k2` always satisfy it.

      // Check the anchor key prefix condition.
      Object validatedAnchorKey = validAnchorKeyProvider.apply(newAnchor);
      if (validatedAnchorKey == null) {
        continue;
      }
      return new Tuple<>(i, validatedAnchorKey);
    }
    throw new IllegalStateException("Cannot split the leaf node. Leaf node: " + this);
  }

  void validate() {
    Object normalizedAnchorKey = anchorKey;
    Object normalizedRightAnchorKey = null;
    if (right != null) {
      normalizedRightAnchorKey = right.anchorKey;
    }

    for (int i = 0; i < size(); i++) {
      KeyValue<K, V> kv = getKeyValue(i);
      if (EncodedKeyUtils.compare(encodedKeyType, kv.getEncodedKey(), normalizedAnchorKey) < 0) {
        throw new AssertionError(
            String.format(
                "The key is smaller than the anchor key. Key: %s, Anchor key: %s",
                kv.getKey(), EncodedKeyUtils.toString(normalizedAnchorKey)));
      }
      if (normalizedRightAnchorKey != null
          && EncodedKeyUtils.compare(encodedKeyType, normalizedRightAnchorKey, kv.getEncodedKey())
              < 0) {
        throw new AssertionError(
            String.format(
                "The anchor key of the right leaf node is smaller than the key. Key: %s, Right leaf node's anchor key: %s",
                kv.getKey(), EncodedKeyUtils.toString(normalizedRightAnchorKey)));
      }
    }

    if (tagsSize() != size()) {
      throw new AssertionError(
          String.format(
              "The number of tags is different from the number of keys. Keys: %s, Tags: %s",
              Arrays.stream(keyValues)
                  .limit(keyValuesCount)
                  .map(kv -> kv.getKey().toString())
                  .collect(Collectors.toList()),
              tagsToString()));
    }
    Set<Integer> keyValueIndexes = new HashSet<>(size());
    for (int i = 0; i < size(); i++) {
      if (i < size() - 1) {
        if (getHashTag(i) > getHashTag(i + 1)) {
          throw new AssertionError(
              String.format("The tags are not sorted. Tags: %s", tagsToString()));
        }
      }
      int keyValueIndex = getKeyValueIndexFromTag(i);
      if (keyValueIndex < 0 || keyValueIndex >= size()) {
        throw new AssertionError(
            String.format(
                "The key-value index is out-of-range. Tags: %s, Index: %d, Key-value index: %d",
                tagsToString(), i, keyValueIndex));
      }
      if (!keyValueIndexes.add(keyValueIndex)) {
        throw new AssertionError(
            String.format(
                "The tags contains duplicated key-value index. Tags: %s, Index: %d, Key-value index: %d",
                tagsToString(), i, keyValueIndex));
      }
    }

    if (keyRefsSize() != size()) {
      throw new AssertionError(
          String.format(
              "The number of key references is different from the number of keys. Keys: %s, Key references: %s",
              Arrays.stream(keyValues)
                  .limit(keyValuesCount)
                  .map(kv -> kv.getKey().toString())
                  .collect(Collectors.toList()),
              keyRefsToString()));
    }

    if (numOfSortedKeyRefs > size()) {
      throw new AssertionError(
          String.format(
              "The number of sorted key references is larger than the number of key references. Key references: %s",
              keyRefsToString()));
    }

    for (int i = 0; i < size(); i++) {
      if (i > 0 && i < getNumOfSortedKeyRefs() - 1) {
        if (EncodedKeyUtils.compare(
                encodedKeyType, getEncodedKeyFromKeyRef(i), getEncodedKeyFromKeyRef(i + 1))
            > 0) {
          throw new AssertionError(
              String.format(
                  "The key references are not ordered. Key references: %s", keyRefsToString()));
        }
      }
    }
  }
}
