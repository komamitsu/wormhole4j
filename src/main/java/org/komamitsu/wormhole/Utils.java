package org.komamitsu.wormhole;

final class Utils {
  static String printableKey(String key) {
    return key.replace("\0", "⊥");
  }

  static int compareAnchorKeys(String s1, String s2) {
    int len1 = s1.endsWith(Wormhole.SMALLEST_TOKEN) ? s1.length() - 1 : s1.length();
    int len2 = s2.endsWith(Wormhole.SMALLEST_TOKEN) ? s2.length() - 1 : s2.length();
    int len = Math.min(len1, len2);
    for (int i = 0; i < len; i++) {
      char c1 = s1.charAt(i);
      char c2 = s2.charAt(i);
      if (c1 != c2) {
        return c1 - c2;
      }
    }
    return len1 - len2;
  }

  static String extractLongestCommonPrefix(String a, String b) {
    int minLen = Math.min(a.length(), b.length());
    for (int i = 0; i < minLen; i++) {
      char ca = a.charAt(i);
      char cb = b.charAt(i);
      if (ca == cb) {
        continue;
      }
      return a.substring(0, i);
    }
    return a.substring(0, minLen);
  }
}
