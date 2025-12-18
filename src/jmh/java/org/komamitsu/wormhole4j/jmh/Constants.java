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

package org.komamitsu.wormhole4j.jmh;

final class Constants {
  static final int RECORD_COUNT = 100000;
  static final int INSERT_OPS_COUNT = 100000000;
  static final int GET_OPS_COUNT = RECORD_COUNT;
  static final int SCAN_OPS_COUNT = RECORD_COUNT / 10;
  static final int MIN_STRING_KEY_LEN = 32;
  static final int MAX_STRING_KEY_LEN = 256;
  static final int SCAN_RANGE_SIZE = 512;

  private Constants() {}
}
