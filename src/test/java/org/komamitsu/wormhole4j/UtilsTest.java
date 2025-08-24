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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class UtilsTest {

  @Test
  void printableKey() {
    assertThat(Utils.printableKey("abc\0")).isEqualTo("abc⊥");
  }

  @Test
  void compareAnchorKeys() {
    assertThat(Utils.compareAnchorKeys("", "")).isEqualTo(0);
    assertThat(Utils.compareAnchorKeys("a", "")).isGreaterThan(0);
    assertThat(Utils.compareAnchorKeys("", "a")).isLessThan(0);
    assertThat(Utils.compareAnchorKeys("a", "a")).isEqualTo(0);
    assertThat(Utils.compareAnchorKeys("ab", "a")).isGreaterThan(0);
    assertThat(Utils.compareAnchorKeys("a", "ab")).isLessThan(0);
    assertThat(Utils.compareAnchorKeys("ab", "ac")).isLessThan(0);
    assertThat(Utils.compareAnchorKeys("abc", "abc")).isEqualTo(0);

    assertThat(Utils.compareAnchorKeys("\0", "")).isEqualTo(0);
    assertThat(Utils.compareAnchorKeys("a\0", "")).isGreaterThan(0);
    assertThat(Utils.compareAnchorKeys("\0", "a")).isLessThan(0);
    assertThat(Utils.compareAnchorKeys("a\0", "a")).isEqualTo(0);
    assertThat(Utils.compareAnchorKeys("ab\0", "a")).isGreaterThan(0);
    assertThat(Utils.compareAnchorKeys("a\0", "ab")).isLessThan(0);
    assertThat(Utils.compareAnchorKeys("ab\0", "ac")).isLessThan(0);
    assertThat(Utils.compareAnchorKeys("abc\0", "abc")).isEqualTo(0);

    assertThat(Utils.compareAnchorKeys("", "\0")).isEqualTo(0);
    assertThat(Utils.compareAnchorKeys("a", "\0")).isGreaterThan(0);
    assertThat(Utils.compareAnchorKeys("", "a\0")).isLessThan(0);
    assertThat(Utils.compareAnchorKeys("a", "a\0")).isEqualTo(0);
    assertThat(Utils.compareAnchorKeys("ab", "a\0")).isGreaterThan(0);
    assertThat(Utils.compareAnchorKeys("a", "ab\0")).isLessThan(0);
    assertThat(Utils.compareAnchorKeys("ab", "ac\0")).isLessThan(0);
    assertThat(Utils.compareAnchorKeys("abc", "abc\0")).isEqualTo(0);

    assertThat(Utils.compareAnchorKeys("\0", "\0")).isEqualTo(0);
    assertThat(Utils.compareAnchorKeys("a\0", "\0")).isGreaterThan(0);
    assertThat(Utils.compareAnchorKeys("\0", "a\0")).isLessThan(0);
    assertThat(Utils.compareAnchorKeys("a\0", "a\0")).isEqualTo(0);
    assertThat(Utils.compareAnchorKeys("ab\0", "a\0")).isGreaterThan(0);
    assertThat(Utils.compareAnchorKeys("a\0", "ab\0")).isLessThan(0);
    assertThat(Utils.compareAnchorKeys("ab\0", "ac\0")).isLessThan(0);
    assertThat(Utils.compareAnchorKeys("abc\0", "abc\0")).isEqualTo(0);
  }
}
