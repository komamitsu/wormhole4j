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
        return ByteArray.EMPTY_INSTANCE;
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
        assert obj instanceof ByteArray;
        return ((ByteArray) obj).length();
      default:
        throw new AssertionError();
    }
  }

  static String toString(EncodedKeyType encodedKeyType, Object obj) {
    return obj.toString();
  }

  static int compare(EncodedKeyType encodedKeyType, Object obj1, Object obj2) {
    switch (encodedKeyType) {
      case STRING:
        assert obj1 instanceof String;
        assert obj2 instanceof String;
        return ((String) obj1).compareTo((String) obj2);
      case BYTE_ARRAY:
        assert obj1 instanceof ByteArray;
        assert obj2 instanceof ByteArray;
        return ((ByteArray) obj1).compareTo((ByteArray) obj2);
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
        assert obj1 instanceof ByteArray;
        assert obj2 instanceof ByteArray;
        return ((ByteArray) obj1).extractLongestCommonPrefix((ByteArray) obj2);
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
        assert obj1 instanceof ByteArray;
        assert obj2 instanceof ByteArray;
        ByteArray xs1 = (ByteArray) obj1;
        ByteArray xs2 = (ByteArray) obj2;
        // TODO: Should we add ByteArray.createNewAnchorKey() to reduce instantiations?
        ByteArray bytesLcp = xs1.extractLongestCommonPrefix(xs2);
        return bytesLcp.appendFromOther(xs2, bytesLcp.length());
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
        assert obj instanceof ByteArray;
        return ((ByteArray) obj).append(x);
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
        assert src instanceof ByteArray;
        assert dst instanceof ByteArray;
        return ((ByteArray) src).appendFromOther((ByteArray) dst, pos);
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
        assert obj instanceof ByteArray;
        assert prefix instanceof ByteArray;
        return ((ByteArray) obj).startsWith((ByteArray) prefix);
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
        assert obj instanceof ByteArray;
        return ((ByteArray) obj).slice(0, length);
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
        assert obj instanceof ByteArray;
        return ((ByteArray) obj).get(pos);
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
        assert encodedKey instanceof ByteArray;
        return new KeyValue<>((ByteArray) encodedKey, key, value);
      default:
        throw new AssertionError();
    }
  }
}
