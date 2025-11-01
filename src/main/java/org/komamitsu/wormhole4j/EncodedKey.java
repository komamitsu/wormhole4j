package org.komamitsu.wormhole4j;

public interface EncodedKey<T extends EncodedKey<T>> extends Comparable<T> {
  int length();

  boolean isEmpty();

  boolean startsWith(T encodedKey);

  T slice(int pos, int length);

  int get(int pos);

  T appendFrom(T encodedKey, int pos);

  T append(int value);

  T extractLongestCommonPrefix(T encodedKey);
}
