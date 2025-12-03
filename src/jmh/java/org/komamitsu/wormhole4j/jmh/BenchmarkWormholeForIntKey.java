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

import java.util.concurrent.TimeUnit;
import org.komamitsu.wormhole4j.WormholeForIntKey;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class BenchmarkWormholeForIntKey {
  @State(Scope.Benchmark)
  public static class KeysState {
    int[] keys = new int[Constants.RECORD_COUNT];

    @Setup(Level.Trial)
    public void setup() {
      for (int i = 0; i < Constants.RECORD_COUNT; i++) {
        keys[i] = Utils.randomInt();
      }
    }
  }

  @State(Scope.Thread)
  public static class InsertState {
    WormholeForIntKey<Integer> wormhole;

    @Setup(Level.Iteration)
    public void setup(KeysState data) {
      wormhole = new WormholeForIntKey<>();
    }
  }

  @Benchmark
  @OperationsPerInvocation(Constants.RECORD_COUNT)
  public void benchmarkInsert(KeysState keysState, InsertState insertState, Blackhole blackhole) {
    for (int i = 0; i < Constants.RECORD_COUNT; i++) {
      insertState.wormhole.put(keysState.keys[i], i);
    }
  }

  @State(Scope.Thread)
  public static class GetState {
    WormholeForIntKey<Integer> wormhole;

    @Setup(Level.Iteration)
    public void setup(KeysState data) {
      wormhole = new WormholeForIntKey<>();
      for (int key : data.keys) {
        wormhole.put(key, Utils.randomInt());
      }
    }
  }

  @Benchmark
  @OperationsPerInvocation(Constants.RECORD_COUNT)
  public void benchmarkGet(KeysState keysState, GetState getState, Blackhole blackhole) {
    for (int i = 0; i < Constants.RECORD_COUNT; i++) {
      blackhole.consume(getState.wormhole.get(keysState.keys[i]));
    }
  }
}
