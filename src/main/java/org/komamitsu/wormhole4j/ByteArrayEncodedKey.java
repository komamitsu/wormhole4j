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

import java.util.Arrays;

class ByteArrayEncodedKey implements EncodedKey<ByteArrayEncodedKey> {
  private static final byte[] EMPTY_BYTES = new byte[] {};
  static final ByteArrayEncodedKey EMPTY_INSTANCE = new ByteArrayEncodedKey(EMPTY_BYTES);
  private static final char[] HEX_CHARS = "0123456789ABCDEF".toCharArray();

  private final byte[] content;

  ByteArrayEncodedKey(byte[] content) {
    this.content = content;
  }

  @Override
  public String toString() {
    return "EncodedKey" + printableKey(content);
  }

  static String printableKey(byte[] bytes) {
    if (bytes.length == 0) {
      return "{}";
    }
    StringBuilder sb = new StringBuilder(2 + bytes.length * 6); // "{0x11, 0x22, ...}"
    sb.append("{");
    for (int i = 0; i < bytes.length; i++) {
      int v = bytes[i] & 0xFF;
      sb.append("0x").append(HEX_CHARS[v >>> 4]).append(HEX_CHARS[v & 0x0F]);
      if (i != bytes.length - 1) {
        sb.append(", ");
      }
    }
    sb.append("}");
    return sb.toString();
  }

  static int compareKeys(byte[] xs1, byte[] xs2) {
    int minLen = Math.min(xs1.length, xs2.length);
    for (int i = 0; i < minLen; i++) {
      int x1 = xs1[i] & 0xFF;
      int x2 = xs2[i] & 0xFF;
      if (x1 != x2) {
        return x1 - x2;
      }
    }
    return xs1.length - xs2.length;
  }

  static byte[] extractLongestCommonPrefix(byte[] xs1, byte[] xs2) {
    int minLen = Math.min(xs1.length, xs2.length);
    if (minLen == 0) {
      return EMPTY_BYTES;
    }
    for (int i = 0; i < minLen; i++) {
      int x1 = xs1[i] & 0xFF;
      int x2 = xs2[i] & 0xFF;
      if (x1 == x2) {
        continue;
      }
      return sliceBytes(xs1, 0, i);
    }
    return sliceBytes(xs1, 0, minLen);
  }

  private static byte[] sliceBytes(byte[] src, int pos, int length) {
    byte[] newBytes = new byte[length];
    System.arraycopy(src, pos, newBytes, 0, length);
    return newBytes;
  }

  static byte[] appendCharToKey(byte[] key, byte x) {
    byte[] newKey = new byte[key.length + 1];
    System.arraycopy(key, 0, newKey, 0, key.length);
    newKey[key.length] = x;
    return newKey;
  }

  @Override
  public int compareTo(ByteArrayEncodedKey encodedKey) {
    return compareKeys(content, encodedKey.content);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ByteArrayEncodedKey that = (ByteArrayEncodedKey) o;
    return Arrays.equals(content, that.content);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(content);
  }

  @Override
  public int length() {
    return content.length;
  }

  @Override
  public boolean isEmpty() {
    return content.length == 0;
  }

  @Override
  public boolean startsWith(ByteArrayEncodedKey encodedKey) {
    if (content.length < encodedKey.content.length) {
      return false;
    }
    for (int i = 0; i < encodedKey.content.length; i++) {
      if (content[i] != encodedKey.content[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public ByteArrayEncodedKey slice(int pos, int length) {
    return new ByteArrayEncodedKey(sliceBytes(content, pos, length));
  }

  @Override
  public int get(int pos) {
    return content[pos] & 0xFF;
  }

  @Override
  public ByteArrayEncodedKey appendFrom(ByteArrayEncodedKey encodedKey, int pos) {
    return new ByteArrayEncodedKey(appendCharToKey(content, encodedKey.content[pos]));
  }

  @Override
  public ByteArrayEncodedKey append(int value) {
    return new ByteArrayEncodedKey(appendCharToKey(content, (byte) value));
  }

  @Override
  public ByteArrayEncodedKey extractLongestCommonPrefix(ByteArrayEncodedKey encodedKey) {
    return new ByteArrayEncodedKey(extractLongestCommonPrefix(content, encodedKey.content));
  }
}
