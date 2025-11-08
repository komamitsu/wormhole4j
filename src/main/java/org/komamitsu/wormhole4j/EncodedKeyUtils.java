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

final class EncodedKeyUtils {
  private EncodedKeyUtils() {}

  static Object createEmpty(EncodedKeyType encodedKeyType) {
    switch (encodedKeyType) {
      case STRING:
        return "";
      case BYTE_ARRAY:
        return ByteArrayUtils.EMPTY_BYTES;
      default:
        throw new AssertionError();
    }
  }

  static int length(EncodedKeyType encodedKeyType, Object obj) {
    switch (encodedKeyType) {
      case STRING:
        assert obj instanceof String;
        return ((String) obj).length();
      case BYTE_ARRAY:
        assert obj instanceof byte[];
        return ((byte[]) obj).length;
      default:
        throw new AssertionError();
    }
  }

  static String toString(EncodedKeyType encodedKeyType, Object obj) {
    switch (encodedKeyType) {
      case STRING:
        assert obj instanceof String;
        return obj.toString();
      case BYTE_ARRAY:
        assert obj instanceof byte[];
        return ByteArrayUtils.toString((byte[]) obj);
      default:
        throw new AssertionError();
    }
  }

  static int compare(EncodedKeyType encodedKeyType, Object obj1, Object obj2) {
    switch (encodedKeyType) {
      case STRING:
        assert obj1 instanceof String;
        assert obj2 instanceof String;
        return ((String) obj1).compareTo((String) obj2);
      case BYTE_ARRAY:
        assert obj1 instanceof byte[];
        assert obj2 instanceof byte[];
        return ByteArrayUtils.compareKeys((byte[]) obj1, (byte[]) obj2);
      default:
        throw new AssertionError();
    }
  }

  static Object extractLongestCommonPrefix(
      EncodedKeyType encodedKeyType, Object obj1, Object obj2) {
    switch (encodedKeyType) {
      case STRING:
        assert obj1 instanceof String;
        assert obj2 instanceof String;
        return extractLongestCommonPrefixForStrings((String) obj1, (String) obj2);
      case BYTE_ARRAY:
        assert obj1 instanceof byte[];
        assert obj2 instanceof byte[];
        return ByteArrayUtils.extractLongestCommonPrefix((byte[]) obj1, (byte[]) obj2);
      default:
        throw new AssertionError();
    }
  }

  static String extractLongestCommonPrefixForStrings(String s1, String s2) {
    int minLen = Math.min(s1.length(), s2.length());
    if (minLen == 0) {
      return "";
    }
    for (int i = 0; i < minLen; i++) {
      if (s1.charAt(i) == s2.charAt(i)) {
        continue;
      }
      return s1.substring(0, i);
    }
    return s1.substring(0, minLen);
  }

  static Object createNewAnchorKey(EncodedKeyType encodedKeyType, Object obj1, Object obj2) {
    switch (encodedKeyType) {
      case STRING:
        assert obj1 instanceof String;
        assert obj2 instanceof String;
        String s1 = (String) obj1;
        String s2 = (String) obj2;
        String strLcp = extractLongestCommonPrefixForStrings(s1, s2);
        return strLcp + s2.charAt(strLcp.length());
      case BYTE_ARRAY:
        assert obj1 instanceof byte[];
        assert obj2 instanceof byte[];
        byte[] xs1 = (byte[]) obj1;
        byte[] xs2 = (byte[]) obj2;
        byte[] bytesLcp = ByteArrayUtils.extractLongestCommonPrefix(xs1, xs2);
        return ByteArrayUtils.append(bytesLcp, xs2[bytesLcp.length]);
      default:
        throw new AssertionError();
    }
  }

  static Object append(EncodedKeyType encodedKeyType, Object obj, int x) {
    switch (encodedKeyType) {
      case STRING:
        assert obj instanceof String;
        return ((String) obj) + ((char) x);
      case BYTE_ARRAY:
        assert obj instanceof byte[];
        return ByteArrayUtils.append((byte[]) obj, x);
      default:
        throw new AssertionError();
    }
  }

  static Object appendFromOther(EncodedKeyType encodedKeyType, Object src, Object dst, int pos) {
    switch (encodedKeyType) {
      case STRING:
        assert src instanceof String;
        assert dst instanceof String;
        return ((String) src) + ((String) dst).charAt(pos);
      case BYTE_ARRAY:
        assert src instanceof byte[];
        assert dst instanceof byte[];
        return ByteArrayUtils.appendFromOther((byte[]) src, (byte[]) dst, pos);
      default:
        throw new AssertionError();
    }
  }

  static boolean startsWith(EncodedKeyType encodedKeyType, Object obj, Object prefix) {
    switch (encodedKeyType) {
      case STRING:
        assert obj instanceof String;
        assert prefix instanceof String;
        return ((String) obj).startsWith((String) prefix);
      case BYTE_ARRAY:
        assert obj instanceof byte[];
        assert prefix instanceof byte[];
        return ByteArrayUtils.startsWith((byte[]) obj, (byte[]) prefix);
      default:
        throw new AssertionError();
    }
  }

  static Object slice(EncodedKeyType encodedKeyType, Object obj, int length) {
    switch (encodedKeyType) {
      case STRING:
        assert obj instanceof String;
        return ((String) obj).substring(0, length);
      case BYTE_ARRAY:
        assert obj instanceof byte[];
        return ByteArrayUtils.slice((byte[]) obj, 0, length);
      default:
        throw new AssertionError();
    }
  }

  static int get(EncodedKeyType encodedKeyType, Object obj, int pos) {
    switch (encodedKeyType) {
      case STRING:
        assert obj instanceof String;
        return ((String) obj).charAt(pos);
      case BYTE_ARRAY:
        assert obj instanceof byte[];
        return ByteArrayUtils.get((byte[]) obj, pos);
      default:
        throw new AssertionError();
    }
  }

  static <K, V> KeyValue<K, V> createKeyValue(
      EncodedKeyType encodedKeyType, Object encodedKey, K key, V value) {
    switch (encodedKeyType) {
      case STRING:
        assert encodedKey instanceof String;
        return new KeyValue<>((String) encodedKey, key, value);
      case BYTE_ARRAY:
        assert encodedKey instanceof byte[];
        return new KeyValue<>((byte[]) encodedKey, key, value);
      default:
        throw new AssertionError();
    }
  }
}
