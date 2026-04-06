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

package org.komamitsu.wormhole4j.jmh.benchmark.singlethread;

import static org.komamitsu.wormhole4j.jmh.Utils.*;

import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.komamitsu.wormhole4j.jmh.state.IntKeysState;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class WormholeForIntKey {

  @State(Scope.Thread)
  public static class FullState {
    org.komamitsu.wormhole4j.WormholeForIntKey<Integer> map;

    @Setup(Level.Trial)
    public void setup(IntKeysState data) {
      map = new org.komamitsu.wormhole4j.WormholeForIntKey.Builder<Integer>().build();
      for (int key : data.keys) {
        map.put(key, randomInt());
      }
    }
  }

  @Benchmark
  public void benchmarkGet(IntKeysState keysState, FullState fullState, Blackhole blackhole) {
    blackhole.consume(fullState.map.get(keysState.getRandomKey()));
  }

  @Benchmark
  public void benchmarkPut(IntKeysState keysState, FullState fullState) {
    fullState.map.put(keysState.getRandomKey(), 42);
  }

  @Benchmark
  public void benchmarkScan(IntKeysState keysState, FullState fullState, Blackhole blackhole) {
    BiFunction<Integer, Integer, Boolean> function =
        (k, v) -> {
          blackhole.consume(k);
          return true;
        };
    keysState.withRandomKeyRange(
        (startKey, endKey) -> fullState.map.scan(startKey, endKey, true, function));
  }
}
