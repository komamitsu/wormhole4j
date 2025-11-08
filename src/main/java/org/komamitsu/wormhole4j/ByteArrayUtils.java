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

final class ByteArrayUtils {
  static final byte[] EMPTY_BYTES = new byte[] {};
  private static final char[] HEX_CHARS = "0123456789ABCDEF".toCharArray();

  private ByteArrayUtils() {}

  static String toString(byte[] xs) {
    if (xs.length == 0) {
      return "{}";
    }
    StringBuilder sb = new StringBuilder(2 + xs.length * 6); // "{0x11, 0x22, ...}"
    sb.append("{");
    for (int i = 0; i < xs.length; i++) {
      int v = xs[i] & 0xFF;
      sb.append("0x").append(HEX_CHARS[v >>> 4]).append(HEX_CHARS[v & 0x0F]);
      if (i != xs.length - 1) {
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
      return slice(xs1, 0, i);
    }
    return slice(xs1, 0, minLen);
  }

  static byte[] slice(byte[] src, int pos, int length) {
    byte[] newBytes = new byte[length];
    System.arraycopy(src, pos, newBytes, 0, length);
    return newBytes;
  }

  static byte[] append(byte[] xs, int x) {
    byte[] newKey = new byte[xs.length + 1];
    System.arraycopy(xs, 0, newKey, 0, xs.length);
    newKey[xs.length] = (byte) x;
    return newKey;
  }

  static byte[] appendFromOther(byte[] src, byte[] dst, int pos) {
    byte[] newKey = new byte[src.length + 1];
    System.arraycopy(src, 0, newKey, 0, src.length);
    newKey[src.length] = dst[pos];
    return newKey;
  }

  static boolean startsWith(byte[] xs, byte[] prefix) {
    if (xs.length < prefix.length) {
      return false;
    }
    for (int i = 0; i < xs.length; i++) {
      if (xs[i] != prefix[i]) {
        return false;
      }
    }
    return true;
  }

  static int get(byte[] xs, int pos) {
    return xs[pos] & 0xFF;
  }
}
