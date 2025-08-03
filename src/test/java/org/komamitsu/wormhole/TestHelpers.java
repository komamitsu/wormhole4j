package org.komamitsu.wormhole;

import java.util.concurrent.ThreadLocalRandom;

class TestHelpers {
  static String genRandomKey(int maxKeyLength) {
    int keyLength = ThreadLocalRandom.current().nextInt(0, maxKeyLength);
    StringBuilder sb = new StringBuilder(keyLength);
    for (int j = 0; j < keyLength; j++) {
      char c = (char) ThreadLocalRandom.current().nextInt('a', 'z' + 1);
      sb.append(c);
    }
    return sb.toString();
  }
}
