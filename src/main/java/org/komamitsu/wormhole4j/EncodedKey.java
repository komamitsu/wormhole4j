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

import java.nio.ByteBuffer;
import java.util.Objects;
import net.openhft.hashing.LongHashFunction;

class EncodedKey implements Comparable<EncodedKey> {
  private static final byte[] EMPTY_BYTES = new byte[] {};
  static final EncodedKey EMPTY_INSTANCE = new EncodedKey(EMPTY_BYTES);
  private static final char[] HEX_CHARS = "0123456789ABCDEF".toCharArray();

  private final byte[] content;

  public EncodedKey(byte[] content) {
    this.content = content;
  }

  @Override
  public String toString() {
    return "EncodedKey" + printableKey(content);
  }

  public static String printableKey(byte[] bytes) {
    if (bytes.length == 0) {
      return "{}";
    }
    StringBuilder sb = new StringBuilder(2 + bytes.length * 6); // "{ 0x11, 0x22, ... }"
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
      return ByteBuffer.wrap(xs1, 0, i).slice().array();
    }
    return ByteBuffer.wrap(xs1, 0, minLen).slice().array();
  }

  static byte[] appendCharToKey(byte[] key, byte x) {
    byte[] newKey = new byte[key.length + 1];
    System.arraycopy(key, 0, newKey, 0, key.length);
    newKey[key.length] = x;
    return newKey;
  }

  @Override
  public int compareTo(EncodedKey encodedKey) {
    return compareKeys(content, encodedKey.content);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    EncodedKey that = (EncodedKey) o;
    return Objects.deepEquals(content, that.content);
  }

  @Override
  public int hashCode() {
    // return Arrays.hashCode(content);
    return (short) (0x7FFF & LongHashFunction.xx3().hashBytes(content));
  }

  int length() {
    return content.length;
  }

  boolean isEmpty() {
    return content.length == 0;
  }

  boolean startsWith(EncodedKey encodedKey) {
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

  EncodedKey slice(int pos, int length) {
    return new EncodedKey(ByteBuffer.wrap(content, pos, length).slice().array());
  }

  int get(int pos) {
    return content[pos];
  }

  EncodedKey appendFrom(EncodedKey encodedKey, int pos) {
    return new EncodedKey(appendCharToKey(content, encodedKey.content[pos]));
  }

  EncodedKey append(int value) {
    return new EncodedKey(appendCharToKey(content, (byte) value));
  }

  EncodedKey extractLongestCommonPrefix(EncodedKey encodedKey) {
    return new EncodedKey(extractLongestCommonPrefix(content, encodedKey.content));
  }
}
