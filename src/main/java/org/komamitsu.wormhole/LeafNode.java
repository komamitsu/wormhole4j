package org.komamitsu.wormhole;

import javax.annotation.Nullable;
import java.util.*;

class LeafNode<T> {
  private final List<KeyValue<T>> keyValues;
  // All references are always sorted by hash.
  private final Tag[] tags;
  // Some references are sorted by key.
  private final KeyReference[] keyReferences;
  private int numOfSortedKeyReferences;

  @Nullable
  private LeafNode<T> left;
  @Nullable
  private LeafNode<T> right;

  private static class KeyValue<T> {
    private final String key;
    private final T value;

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
  }

  // TODO: This can be replaced with Integer, using the first 16 bits for hash and the second 16 bits for index.
  private static class Tag {
    private final short hash;
    private final int kvIndex;

    private Tag(short hash, int kvIndex) {
      this.hash = hash;
      this.kvIndex = kvIndex;
    }

    @Override
    public String toString() {
      return "Tag{" +
          "hash=" + hash +
          ", kvIndex=" + kvIndex +
          '}';
    }
  }

  // A pointer to key via Tag.
  private static class KeyReference {
    private final Tag tag;

    private KeyReference(Tag tag) {
      this.tag = tag;
    }

    @Override
    public String toString() {
      return "KeyReference{" +
          "tag=" + tag +
          '}';
    }
  }

  LeafNode(int size, @Nullable LeafNode<T> left, @Nullable LeafNode<T> right) {
    keyValues = new ArrayList<>(size);
    tags = new Tag[size];
    keyReferences = new KeyReference[size];
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
    return tags[tagIndex].hash;
  }

  private KeyValue<T> getKeyValueByTagIndex(int tagIndex) {
    return keyValues.get(tags[tagIndex].kvIndex);
  }

  private Tag getTagByKeyRefIndex(int keyReferenceIndex) {
    return keyReferences[keyReferenceIndex].tag;
  }

  private KeyValue<T> getKeyValueByKeyRefIndex(int keyReferenceIndex) {
    return keyValues.get(keyReferences[keyReferenceIndex].tag.kvIndex);
  }

  String getKeyByKeyRefIndex(int keyReferenceIndex) {
    return keyValues.get(keyReferences[keyReferenceIndex].tag.kvIndex).key;
  }

  @Nullable
  T pointSearchLeaf(String key) {
    int keyHash = 0x7FFF & key.hashCode();
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
        return kv.value;
      }
      tagIndex++;
    }
    return null;
  }

  void incSort() {
    // Sort unsorted key references.
    Arrays.sort(keyReferences,
        numOfSortedKeyReferences,
        keyValues.size(),
        Comparator.comparing(keyReference -> keyValues.get(keyReference.tag.kvIndex).key));

    // Merge sorted and unsorted key references.
    KeyReference[] tmp = new KeyReference[keyValues.size()];
    // [0, numOfSortedKeyReferences)
    int idxForSortedKeyRef = 0;
    // [numOfSortedKeyReferences, keyValues.size())
    int idxForUnsortedKeyRef = numOfSortedKeyReferences;
    int outputIndex = 0;
    String keyFromSortedKeyRef = null;
    String keyFromUnsortedKeyRef = null;
    while (true) {
      if (keyFromSortedKeyRef == null && idxForSortedKeyRef < numOfSortedKeyReferences) {
        keyFromSortedKeyRef = getKeyValueByKeyRefIndex(idxForSortedKeyRef).key;
      }
      if (keyFromUnsortedKeyRef == null && idxForUnsortedKeyRef < keyValues.size()) {
        keyFromUnsortedKeyRef = getKeyValueByKeyRefIndex(idxForUnsortedKeyRef).key;
      }

      if (keyFromSortedKeyRef != null) {
        if (keyFromUnsortedKeyRef != null) {
          if (keyFromSortedKeyRef.compareTo(keyFromUnsortedKeyRef) < 0) {
            tmp[outputIndex] = keyReferences[idxForSortedKeyRef++];
            keyFromSortedKeyRef = null;
          }
          else {
            tmp[outputIndex] = keyReferences[idxForUnsortedKeyRef++];
            keyFromUnsortedKeyRef = null;
          }
        }
        else {
          tmp[outputIndex] = keyReferences[idxForSortedKeyRef++];
          keyFromSortedKeyRef = null;
        }
      }
      else {
        if (keyFromUnsortedKeyRef != null) {
          tmp[outputIndex] = keyReferences[idxForUnsortedKeyRef++];
          keyFromUnsortedKeyRef = null;
        }
        else {
          break;
        }
      }
      outputIndex++;
    }
    System.arraycopy(tmp, 0, keyReferences, 0, keyValues.size());
    numOfSortedKeyReferences = keyValues.size();
  }

  private Tuple<LeafNode<T>, Set<Tag>> copyToNewLeafNode(int startKeyRefIndex) {
    assert numOfSortedKeyReferences == keyValues.size();

    int currentSize = keyValues.size();

    // Copy entries to a new leaf node.
    LeafNode<T> newLeafNode = new LeafNode<>(maxSize(), this, this.right);
    Set<Tag> tagsInNewLeafNode = new HashSet<>(maxSize());
    for (int i = startKeyRefIndex; i < currentSize; i++) {
      Tag tag = getTagByKeyRefIndex(i);
      tagsInNewLeafNode.add(tag);
      KeyValue<T> kv = keyValues.get(tag.kvIndex);
      keyValues.set(tag.kvIndex, null);
      newLeafNode.keyValues.add(kv);
      // This needs to be sorted later.
      newLeafNode.tags[i] = tag;
      newLeafNode.keyReferences[i] = keyReferences[i];
    }
    Arrays.sort(newLeafNode.tags);
    newLeafNode.numOfSortedKeyReferences = newLeafNode.keyValues.size();

    return new Tuple<>(newLeafNode, tagsInNewLeafNode);
  }

  private void removeMovedEntries(int startKeyRefIndex, Set<Tag> tagsInNewLeafNode) {
    keyValues.removeIf(Objects::isNull);

    Tag[] srcTags = Arrays.copyOf(tags, tags.length);
    int srcTagIndex = 0, dstTagIndex = 0;
    while (srcTagIndex < srcTags.length) {
      Tag srcTag = srcTags[srcTagIndex++];
      if (srcTag == null) {
        // All original stored tags are scanned.
        break;
      }
      if (tagsInNewLeafNode.contains(srcTag)) {
        // The tag is moved to the new leaf node and should be skipped.
        continue;
      }
      tags[dstTagIndex++] = srcTag;
    }
    for (; dstTagIndex < tags.length; dstTagIndex++) {
      tags[dstTagIndex] = null;
    }

    for (int i = startKeyRefIndex; i < keyReferences.length; i++) {
      keyReferences[i] = null;
    }
  }

  LeafNode<T> splitToNewLeafNode(int startKeyRefIndex) {
    Tuple<LeafNode<T>, Set<Tag>> copied = copyToNewLeafNode(startKeyRefIndex);
    LeafNode<T> newLeafNode = copied.first;
    Set<Tag> tagsInNewLeafNode = copied.second;

    removeMovedEntries(startKeyRefIndex, tagsInNewLeafNode);

    return newLeafNode;
  }

  private int maxSize() {
    return tags.length;
  }

  int size() {
    return keyValues.size();
  }

  @Override
  public String toString() {
    return "LeafNode{" +
        "keyValues=" + keyValues +
        ", tags=" + Arrays.toString(tags) +
        ", keyReferences=" + Arrays.toString(keyReferences) +
        ", numOfSortedKeyReferences=" + numOfSortedKeyReferences +
        '}';
  }
}
