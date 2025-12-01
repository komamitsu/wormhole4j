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

final class LongWrapper implements Comparable<LongWrapper> {
  private static final int SIZE = 8;
  private static final long[] BITMASKS =
      new long[] {
        0x0000000000000000L, // Length: 0
        0xFF00000000000000L, // Length: 1
        0xFFFF000000000000L, // Length: 2
        0xFFFFFF0000000000L, // Length: 3
        0xFFFFFFFF00000000L, // Length: 4
        0xFFFFFFFFFF000000L, // Length: 5
        0xFFFFFFFFFFFF0000L, // Length: 6
        0xFFFFFFFFFFFFFF00L, // Length: 7
        0xFFFFFFFFFFFFFFFFL, // Length: 8
      };
  static final LongWrapper EMPTY_INSTANCE = new LongWrapper(0, 0);
  private static final char[] HEX_CHARS = "0123456789ABCDEF".toCharArray();
  // The value is already masked based on the length.
  private final long value;
  private final int length;

  LongWrapper(long value) {
    this.length = SIZE;
    this.value = value & BITMASKS[length];
  }

  private LongWrapper(long value, int length) {
    assert length <= SIZE;
    this.length = length;
    this.value = value & BITMASKS[length];
  }

  @Override
  public int compareTo(LongWrapper other) {
    int minLen = Math.min(length, other.length);
    long mask = BITMASKS[minLen];
    long x1 = value & mask;
    long x2 = other.value & mask;
    if (x1 != x2) {
      return Long.compareUnsigned(x1, x2);
    }
    return length - other.length;
  }

  int longestCommonPrefixLength(LongWrapper other) {
    int minLen = Math.min(length, other.length);
    if (minLen == 0) {
      return 0;
    }
    long diff = value ^ other.value;
    if (diff == 0) {
      return minLen;
    }
    int leadingZeroBytes = Long.numberOfLeadingZeros(diff) >>> 3;
    return Math.min(minLen, leadingZeroBytes);
  }

  LongWrapper slice(int length) {
    assert length <= this.length;
    if (length == 0) {
      return EMPTY_INSTANCE;
    }
    return new LongWrapper(value, length);
  }

  LongWrapper append(int x) {
    assert length < SIZE;
    assert (x & 0xFFFFFF00) == 0;
    int shift = (SIZE - length - 1) << 3;
    // The value is already masked.
    long newValue = value | ((long) x << shift);
    return new LongWrapper(newValue, length + 1);
  }

  boolean startsWith(LongWrapper prefix) {
    if (length < prefix.length) {
      return false;
    }
    long maskedValue = value & BITMASKS[prefix.length];
    return maskedValue == prefix.value;
  }

  int length() {
    return length;
  }

  int get(int pos) {
    assert pos < length;
    return (int) ((value >>> ((SIZE - 1 - pos) << 3)) & 0xFF);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LongWrapper)) {
      return false;
    }
    LongWrapper other = (LongWrapper) o;
    return length == other.length && value == other.value;
  }

  @Override
  public int hashCode() {
    return (int) (value * 31 + length);
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
