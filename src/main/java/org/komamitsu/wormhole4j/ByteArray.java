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
import java.util.Objects;

final class ByteArray implements Comparable<ByteArray> {
  static final byte[] EMPTY_BYTES = new byte[] {};
  static final ByteArray EMPTY_INSTANCE = new ByteArray(EMPTY_BYTES);
  private static final char[] HEX_CHARS = "0123456789ABCDEF".toCharArray();
  private final byte[] bytes;

  ByteArray(byte[] bytes) {
    this.bytes = bytes;
  }

  @Override
  public int compareTo(ByteArray other) {
    int minLen = Math.min(bytes.length, other.bytes.length);
    for (int i = 0; i < minLen; i++) {
      int x1 = bytes[i] & 0xFF;
      int x2 = other.bytes[i] & 0xFF;
      if (x1 != x2) {
        return x1 - x2;
      }
    }
    return bytes.length - other.bytes.length;
  }

  int longestCommonPrefixLength(ByteArray other) {
    int minLen = Math.min(bytes.length, other.bytes.length);
    if (minLen == 0) {
      return 0;
    }
    for (int i = 0; i < minLen; i++) {
      int x1 = bytes[i] & 0xFF;
      int x2 = other.bytes[i] & 0xFF;
      if (x1 == x2) {
        continue;
      }
      return i;
    }
    return minLen;
  }

  ByteArray slice(int pos, int length) {
    byte[] newBytes = new byte[length];
    System.arraycopy(bytes, pos, newBytes, 0, length);
    return new ByteArray(newBytes);
  }

  ByteArray append(int x) {
    byte[] newKey = new byte[bytes.length + 1];
    System.arraycopy(bytes, 0, newKey, 0, bytes.length);
    newKey[bytes.length] = (byte) x;
    return new ByteArray(newKey);
  }

  boolean startsWith(ByteArray prefix) {
    if (bytes.length < prefix.bytes.length) {
      return false;
    }
    for (int i = 0; i < prefix.bytes.length; i++) {
      if (bytes[i] != prefix.bytes[i]) {
        return false;
      }
    }
    return true;
  }

  int length() {
    return bytes.length;
  }

  int get(int pos) {
    return bytes[pos] & 0xFF;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) return false;
    ByteArray byteArray = (ByteArray) o;
    return Objects.deepEquals(bytes, byteArray.bytes);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(bytes);
  }

  @Override
  public String toString() {
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
}
