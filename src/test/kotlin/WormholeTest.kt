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

import org.jetbrains.lincheck.datastructures.*
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.komamitsu.wormhole4j.WormholeForIntKey
import java.util.TreeMap

@Param(name = "key", gen = IntGen::class, conf = "1:20")
@Param(name = "value", gen = IntGen::class, conf = "1:100")
class WormholeTest {
    private val wormhole = WormholeForIntKey<Int>(4)

    private fun ensureThreadRegistered() {
        if (!wormhole.isThreadRegistered) {
            wormhole.registerThread()
        }
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

    @Test
    fun stressTest() = StressOptions()
        .sequentialSpecification(SequentialMap::class.java)
        .threads(3)
        .invocationsPerIteration(20)
        .iterations(100)
        .check(this::class)

    class SequentialMap {
        private val map = TreeMap<Int, Int>()

        fun put(key: Int, value: Int): Int? = map.put(key, value)

        fun get(key: Int): Int? = map[key]

        fun delete(key: Int): Boolean = map.remove(key) != null
    }
}
