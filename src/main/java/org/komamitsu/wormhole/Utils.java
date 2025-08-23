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
} 
