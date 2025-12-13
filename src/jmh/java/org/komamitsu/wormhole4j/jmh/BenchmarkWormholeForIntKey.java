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

import static org.komamitsu.wormhole4j.jmh.Constants.*;
import static org.komamitsu.wormhole4j.jmh.Utils.*;

import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.komamitsu.wormhole4j.WormholeForIntKey;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class BenchmarkWormholeForIntKey {

  @State(Scope.Thread)
  public static class EmptyState {
    WormholeForIntKey<Integer> map;

    @Setup(Level.Iteration)
    public void setup() {
      map = new WormholeForIntKey<>();
    }
  }

  @Benchmark
  @OperationsPerInvocation(INSERT_OPS_COUNT)
  public void benchmarkInsert(IntKeysState keysState, EmptyState emptyState) {
    iterateWithKey(INSERT_OPS_COUNT, keysState, key -> emptyState.map.put(key, 42));
  }

  @State(Scope.Thread)
  public static class FullState {
    WormholeForIntKey<Integer> map;

    @Setup(Level.Iteration)
    public void setup(IntKeysState data) {
      map = new WormholeForIntKey<>();
      for (int key : data.keys) {
        map.put(key, randomInt());
      }
    }
  }

  @Benchmark
  @OperationsPerInvocation(GET_OPS_COUNT)
  public void benchmarkGet(IntKeysState keysState, FullState fullState, Blackhole blackhole) {
    iterateWithKey(GET_OPS_COUNT, keysState, key -> blackhole.consume(fullState.map.get(key)));
  }

  @Benchmark
  @OperationsPerInvocation(SCAN_OPS_COUNT)
  public void benchmarkScan(IntKeysState keysState, FullState fullState, Blackhole blackhole) {
    BiFunction<Integer, Integer, Boolean> function =
        (k, v) -> {
          blackhole.consume(k);
          blackhole.consume(v);
          return true;
        };
    iterateWithKeysRange(
        SCAN_OPS_COUNT, keysState, (k1, k2) -> fullState.map.scan(k1, k2, true, function));
  }
}
