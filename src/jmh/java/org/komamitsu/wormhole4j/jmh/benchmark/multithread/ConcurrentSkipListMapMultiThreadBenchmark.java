/*
 * Copyright 2026 Mitsunori Komatsu
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

package org.komamitsu.wormhole4j.jmh.benchmark.multithread;

import static org.komamitsu.wormhole4j.jmh.Utils.randomInt;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import org.komamitsu.wormhole4j.jmh.Constants;
import org.komamitsu.wormhole4j.jmh.state.IntKeysState;
import org.komamitsu.wormhole4j.jmh.state.KeysState;
import org.komamitsu.wormhole4j.jmh.state.LongKeysState;
import org.komamitsu.wormhole4j.jmh.state.StringKeysState;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

public abstract class ConcurrentSkipListMapMultiThreadBenchmark<K extends Comparable<K>>
    extends MultiThreadBenchmark {

  protected abstract static class FullState<K extends Comparable<K>> {
    ConcurrentSkipListMap<K, Integer> map;

    protected void setup(KeysState<K> keysState) {
      map = new ConcurrentSkipListMap<>();
      for (K key : keysState.keys) {
        map.put(key, randomInt());
      }
    }
  }

  protected void execGet(KeysState<K> keysState, FullState<K> fullState, Blackhole blackhole) {
    blackhole.consume(fullState.map.get(keysState.getRandomKey()));
  }

  protected void execUpdate(KeysState<K> keysState, FullState<K> fullState) {
    fullState.map.put(keysState.getRandomKey(), ThreadLocalRandom.current().nextInt());
  }

  protected void execScan(KeysState<K> keysState, FullState<K> fullState, Blackhole blackhole) {
    keysState.withRandomKeyRange(
        (startKey, endKey) ->
            fullState.map.subMap(startKey, endKey).forEach((key, value) -> blackhole.consume(key)));
  }

  public static class ForIntKey extends ConcurrentSkipListMapMultiThreadBenchmark<Integer> {
    @State(Scope.Benchmark)
    public static class FullState
        extends ConcurrentSkipListMapMultiThreadBenchmark.FullState<Integer> {
      @Setup(Level.Trial)
      public void setup(IntKeysState keysState) {
        super.setup(keysState);
      }
    }

    @Group("UpdateAndGet")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndGetBenchmarkUpdate(IntKeysState keysState, FullState fullState) {
      execUpdate(keysState, fullState);
    }

    @Group("UpdateAndGet")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndGetBenchmarkGet(
        IntKeysState keysState, FullState fullState, Blackhole blackhole) {
      execGet(keysState, fullState, blackhole);
    }

    @Group("PutAndScan")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndScanBenchmarkUpdate(IntKeysState keysState, FullState fullState) {
      execUpdate(keysState, fullState);
    }

    @Group("PutAndScan")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndScanBenchmarkScan(
        IntKeysState keysState, FullState fullState, Blackhole blackhole) {
      execScan(keysState, fullState, blackhole);
    }
  }

  public static class ForLongKey extends ConcurrentSkipListMapMultiThreadBenchmark<Long> {
    @State(Scope.Benchmark)
    public static class FullState
        extends ConcurrentSkipListMapMultiThreadBenchmark.FullState<Long> {
      @Setup(Level.Trial)
      public void setup(LongKeysState keysState) {
        super.setup(keysState);
      }
    }

    @Group("UpdateAndGet")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndGetBenchmarkUpdate(LongKeysState keysState, FullState fullState) {
      execUpdate(keysState, fullState);
    }

    @Group("UpdateAndGet")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndGetBenchmarkGet(
        LongKeysState keysState, FullState fullState, Blackhole blackhole) {
      execGet(keysState, fullState, blackhole);
    }

    @Group("PutAndScan")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndScanBenchmarkUpdate(LongKeysState keysState, FullState fullState) {
      execUpdate(keysState, fullState);
    }

    @Group("PutAndScan")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndScanBenchmarkScan(
        LongKeysState keysState, FullState fullState, Blackhole blackhole) {
      execScan(keysState, fullState, blackhole);
    }
  }

  public static class ForStringKey extends ConcurrentSkipListMapMultiThreadBenchmark<String> {
    @State(Scope.Benchmark)
    public static class FullState
        extends ConcurrentSkipListMapMultiThreadBenchmark.FullState<String> {
      @Setup(Level.Trial)
      public void setup(StringKeysState keysState) {
        super.setup(keysState);
      }
    }

    @Group("UpdateAndGet")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndGetBenchmarkUpdate(StringKeysState keysState, FullState fullState) {
      execUpdate(keysState, fullState);
    }

    @Group("UpdateAndGet")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndGetBenchmarkGet(
        StringKeysState keysState, FullState fullState, Blackhole blackhole) {
      execGet(keysState, fullState, blackhole);
    }

    @Group("PutAndScan")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndScanBenchmarkUpdate(StringKeysState keysState, FullState fullState) {
      execUpdate(keysState, fullState);
    }

    @Group("PutAndScan")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndScanBenchmarkScan(
        StringKeysState keysState, FullState fullState, Blackhole blackhole) {
      execScan(keysState, fullState, blackhole);
    }
  }
}
