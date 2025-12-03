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

final class IntWrapper implements Comparable<IntWrapper> {
  private static final int SIZE = 4;
  private static final int[] BITMASKS =
      new int[] {
        0x00000000, // Length: 0
        0xFF000000, // Length: 1
        0xFFFF0000, // Length: 2
        0xFFFFFF00, // Length: 3
        0xFFFFFFFF, // Length: 4
      };
  static final IntWrapper EMPTY_INSTANCE = new IntWrapper(0, 0);
  private static final char[] HEX_CHARS = "0123456789ABCDEF".toCharArray();
  // The value is already masked based on the length.
  private final int value;
  private final int length;

  IntWrapper(int value) {
    this.length = SIZE;
    this.value = value;
  }

  private IntWrapper(int value, int length) {
    assert length <= SIZE;
    this.length = length;
    this.value = value & BITMASKS[length];
  }

  @Override
  public int compareTo(IntWrapper other) {
    int minLen = Math.min(length, other.length);
    int mask = BITMASKS[minLen];
    int x1 = value & mask;
    int x2 = other.value & mask;
    if (x1 != x2) {
      return Integer.compareUnsigned(x1, x2);
    }
    return length - other.length;
  }

  int longestCommonPrefixLength(IntWrapper other) {
    int minLen = Math.min(length, other.length);
    if (minLen == 0) {
      return 0;
    }
    int diff = value ^ other.value;
    if (diff == 0) {
      return minLen;
    }
    int leadingZeroBytes = Integer.numberOfLeadingZeros(diff) >>> 3;
    return Math.min(minLen, leadingZeroBytes);
  }

  IntWrapper slice(int length) {
    assert length <= this.length;
    if (length == 0) {
      return EMPTY_INSTANCE;
    }
    return new IntWrapper(value, length);
  }

  IntWrapper append(int x) {
    assert length < SIZE;
    assert (x & 0xFFFFFF00) == 0;
    int shift = (SIZE - length - 1) << 3;
    // The value is already masked.
    int newValue = value | (x << shift);
    return new IntWrapper(newValue, length + 1);
  }

  boolean startsWith(IntWrapper prefix) {
    if (length < prefix.length) {
      return false;
    }
    int mask = BITMASKS[prefix.length];
    int maskedValue = value & mask;
    return maskedValue == prefix.value;
  }

  int length() {
    return length;
  }

  int get(int pos) {
    assert pos < length;
    return (value >>> ((SIZE - 1 - pos) << 3)) & 0xFF;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof IntWrapper)) {
      return false;
    }
    IntWrapper other = (IntWrapper) o;
    return length == other.length && value == other.value;
  }

  @Override
  public int hashCode() {
    return value * 31 + length;
  }

  @Override
  public String toString() {
    if (length == 0) {
      return "{}";
    }
    StringBuilder sb = new StringBuilder(2 + length * 6); // "{0x11, 0x22, ...}"
    sb.append("{");
    for (int i = 0; i < length; i++) {
      int v = get(i);
      sb.append("0x").append(HEX_CHARS[v >>> 4]).append(HEX_CHARS[v & 0x0F]);
      if (i != length - 1) {
        sb.append(", ");
      }
    }
    sb.append("}");
    return sb.toString();
  }
}
