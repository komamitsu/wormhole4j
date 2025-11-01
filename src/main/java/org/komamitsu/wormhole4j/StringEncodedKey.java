package org.komamitsu.wormhole4j;

import java.util.Objects;

class StringEncodedKey implements EncodedKey<StringEncodedKey> {
  static final StringEncodedKey EMPTY_INSTANCE = new StringEncodedKey("");
  private final String content;

  StringEncodedKey(String content) {
    this.content = content;
  }

  @Override
  public int length() {
    return content.length();
  }

  @Override
  public boolean isEmpty() {
    return content.isEmpty();
  }

  @Override
  public boolean startsWith(StringEncodedKey other) {
    return content.startsWith(other.content);
  }

  @Override
  public StringEncodedKey slice(int pos, int length) {
    return new StringEncodedKey(content.substring(pos, length));
  }

  @Override
  public int get(int pos) {
    return content.charAt(pos);
  }

  @Override
  public StringEncodedKey appendFrom(StringEncodedKey other, int pos) {
    return new StringEncodedKey(content + other.content.charAt(pos));
  }

  @Override
  public StringEncodedKey append(int value) {
    return new StringEncodedKey(content + ((char)value));
  }

  @Override
  public StringEncodedKey extractLongestCommonPrefix(StringEncodedKey other) {
    int minLen = Math.min(content.length(), other.content.length());
    if (minLen == 0) {
      return EMPTY_INSTANCE;
    }
    for (int i = 0; i < minLen; i++) {
      if (content.charAt(i) == other.content.charAt(i)) {
        continue;
      }
      return new StringEncodedKey(content.substring(0, i));
    }
    return new StringEncodedKey(content.substring(0, minLen));
  }

  @Override
  public int compareTo(StringEncodedKey other) {
    return content.compareTo(other.content);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) return false;
    StringEncodedKey that = (StringEncodedKey) o;
    return Objects.equals(content, that.content);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(content);
  }
}
