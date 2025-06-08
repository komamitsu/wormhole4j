package org.komamitsu.wormhole;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class LeafNode<T> {
  private final List<KeyValue<T>> keyValues;
  // All references are always sorted by hash.
  private final Tag[] tags;
  // Some references are sorted by key.
  private final KeyReference[] keyReferences;
  private int numOfSortedKeyReferences;

  private static class KeyValue<T> {
    private final String key;
    private final T value;

    private KeyValue(String key, T value) {
      this.key = key;
      this.value = value;
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
  }

  // A pointer to key via Tag.
  private static class KeyReference {
    private final Tag tag;

    private KeyReference(Tag tag) {
      this.tag = tag;
    }
  }

  public LeafNode(int size) {
    keyValues = new ArrayList<>(size);
    tags = new Tag[size];
    keyReferences = new KeyReference[size];
  }

  private short getHashTag(int tagIndex) {
    return tags[tagIndex].hash;
  }

  private KeyValue<T> getKeyValueByTagIndex(int tagIndex) {
    return keyValues.get(tags[tagIndex].kvIndex);
  }

  private KeyValue<T> getKeyValueByKeyRefIndex(int keyReferenceIndex) {
    return keyValues.get(keyReferences[keyReferenceIndex].tag.kvIndex);
  }

  @Nullable
  public T pointSearchLeaf(String key) {
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

  public void incSort() {
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
  }
}
