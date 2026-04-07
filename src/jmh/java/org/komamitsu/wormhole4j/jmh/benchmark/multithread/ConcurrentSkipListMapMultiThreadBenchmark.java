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

package org.komamitsu.wormhole4j.jmh.benchmark.multithread;

import static org.komamitsu.wormhole4j.jmh.Utils.randomInt;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import org.komamitsu.wormhole4j.jmh.state.IntKeysState;
import org.komamitsu.wormhole4j.jmh.state.KeysState;
import org.komamitsu.wormhole4j.jmh.state.LongKeysState;
import org.komamitsu.wormhole4j.jmh.state.StringKeysState;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public abstract class ConcurrentSkipListMapMultiThreadBenchmark<K extends Comparable<K>> {

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

  protected void execPut(KeysState<K> keysState, FullState<K> fullState) {
    fullState.map.put(keysState.getRandomKey(), 42);
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

    @Group("PutAndGet")
    @GroupThreads(4)
    @Benchmark
    public void putAndGetBenchmarkPut(IntKeysState keysState, FullState fullState) {
      execPut(keysState, fullState);
    }

    @Group("PutAndGet")
    @GroupThreads(4)
    @Benchmark
    public void putAndGetBenchmarkGet(
        IntKeysState keysState, FullState fullState, Blackhole blackhole) {
      execGet(keysState, fullState, blackhole);
    }

    @Group("PutAndScan")
    @GroupThreads(4)
    @Benchmark
    public void putAndScanBenchmarkPut(IntKeysState keysState, FullState fullState) {
      execPut(keysState, fullState);
    }

    @Group("PutAndScan")
    @GroupThreads(4)
    @Benchmark
    public void putAndScanBenchmarkScan(
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

    @Group("PutAndGet")
    @GroupThreads(4)
    @Benchmark
    public void putAndGetBenchmarkPut(LongKeysState keysState, FullState fullState) {
      execPut(keysState, fullState);
    }

    @Group("PutAndGet")
    @GroupThreads(4)
    @Benchmark
    public void putAndGetBenchmarkGet(
        LongKeysState keysState, FullState fullState, Blackhole blackhole) {
      execGet(keysState, fullState, blackhole);
    }

    @Group("PutAndScan")
    @GroupThreads(4)
    @Benchmark
    public void putAndScanBenchmarkPut(LongKeysState keysState, FullState fullState) {
      execPut(keysState, fullState);
    }

    @Group("PutAndScan")
    @GroupThreads(4)
    @Benchmark
    public void putAndScanBenchmarkScan(
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

    @Group("PutAndGet")
    @GroupThreads(4)
    @Benchmark
    public void putAndGetBenchmarkPut(StringKeysState keysState, FullState fullState) {
      execPut(keysState, fullState);
    }

    @Group("PutAndGet")
    @GroupThreads(4)
    @Benchmark
    public void putAndGetBenchmarkGet(
        StringKeysState keysState, FullState fullState, Blackhole blackhole) {
      execGet(keysState, fullState, blackhole);
    }

    @Group("PutAndScan")
    @GroupThreads(4)
    @Benchmark
    public void putAndScanBenchmarkPut(StringKeysState keysState, FullState fullState) {
      execPut(keysState, fullState);
    }

    @Group("PutAndScan")
    @GroupThreads(4)
    @Benchmark
    public void putAndScanBenchmarkScan(
        StringKeysState keysState, FullState fullState, Blackhole blackhole) {
      execScan(keysState, fullState, blackhole);
    }
  }
}
