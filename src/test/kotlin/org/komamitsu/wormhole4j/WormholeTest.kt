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
package org.komamitsu.wormhole4j

import org.jetbrains.lincheck.datastructures.*
import org.junit.jupiter.api.Test
import java.util.TreeMap

@Param(name = "key", gen = IntGen::class, conf = "1:20")
@Param(name = "value", gen = IntGen::class, conf = "1:100")
@Param(name = "key1", gen = IntGen::class, conf = "1:20")
@Param(name = "key2", gen = IntGen::class, conf = "1:20")
class WormholeTest {
    private val wormhole = WormholeBuilder.ForIntKey<Int>().setConcurrent(true).setLeafNodeSize(4).build()

    fun ensureThreadRegistered() {
        wormhole.registerThread()
    }

    @Operation(params = ["key", "value"])
    fun put(key: Int, value: Int): Int? {
        ensureThreadRegistered()
        return wormhole.put(key, value)
    }

    @Operation(params = ["key"])
    fun get(key: Int): Int? {
        ensureThreadRegistered()
        return wormhole.get(key)
    }

    @Operation(params = ["key"])
    fun delete(key: Int): Boolean {
        ensureThreadRegistered()
        return wormhole.delete(key)
    }

    @Operation(params = ["key1", "key2"])
    fun scan(key1: Int, key2: Int): List<Int> {
        ensureThreadRegistered()
        val (startKey, endKey) = if (key1 < key2) Pair(key1, key2) else Pair(key2, key1)
        val result = ArrayList<Int>()
        wormhole.scan(startKey, endKey, true) { _, v ->
            result.add(v)
            true
        }
        return result
    }

    @Test
    fun stressTest() = StressOptions()
        .sequentialSpecification(SequentialMap::class.java)
        .threads(3)
        .invocationsPerIteration(100)
        .iterations(100)
        .check(this::class)

    class SequentialMap {
        private val map = TreeMap<Int, Int>()

        fun put(key: Int, value: Int): Int? = map.put(key, value)

        fun get(key: Int): Int? = map[key]

        fun delete(key: Int): Boolean = map.remove(key) != null

        fun scan(key1: Int, key2: Int): List<Int> {
            val (startKey, endKey) = if (key1 < key2) Pair(key1, key2) else Pair(key2, key1)
            return map.subMap(startKey, endKey).toList().map { it.second }
        }
    }
}
