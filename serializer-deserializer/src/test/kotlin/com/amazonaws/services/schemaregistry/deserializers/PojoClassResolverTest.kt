/*
 * Copyright 2026 Mobsuccess.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.schemaregistry.deserializers

import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class PojoClassResolverTest {
    @Test
    fun testResolve_asksTheContextClassLoaderFirst() {
        val recorder = RecordingClassLoader(javaClass.classLoader)

        val resolved = withContextClassLoader(recorder) { PojoClassResolver.resolve(TARGET) }

        assertEquals(JsonDataWithSchema::class.java, resolved)
        assertTrue(recorder.requested.contains(TARGET))
    }

    @Test
    fun testResolve_fallsBackWhenTheContextClassLoaderCannotSeeTheClass() {
        val resolved = withContextClassLoader(BlindClassLoader()) { PojoClassResolver.resolve(TARGET) }

        assertEquals(JsonDataWithSchema::class.java, resolved)
    }

    @Test
    fun testResolve_fallsBackWhenThereIsNoContextClassLoader() {
        val resolved = withContextClassLoader(null) { PojoClassResolver.resolve(TARGET) }

        assertEquals(JsonDataWithSchema::class.java, resolved)
    }

    @Test
    fun testResolve_stillRaisesForAnUnknownClassName() {
        assertThrows(ClassNotFoundException::class.java) { PojoClassResolver.resolve("com.example.NoSuchPojo") }
    }

    private fun <T> withContextClassLoader(
        classLoader: ClassLoader?,
        block: () -> T,
    ): T {
        val thread = Thread.currentThread()
        val previous = thread.contextClassLoader
        thread.contextClassLoader = classLoader
        try {
            return block()
        } finally {
            thread.contextClassLoader = previous
        }
    }

    private class RecordingClassLoader(
        parent: ClassLoader,
    ) : ClassLoader(parent) {
        val requested: MutableList<String> = mutableListOf()

        override fun loadClass(
            name: String,
            resolve: Boolean,
        ): Class<*> {
            requested.add(name)
            return super.loadClass(name, resolve)
        }
    }

    private class BlindClassLoader : ClassLoader(null) {
        override fun loadClass(
            name: String,
            resolve: Boolean,
        ): Class<*> = throw ClassNotFoundException(name)
    }

    private companion object {
        private val TARGET = JsonDataWithSchema::class.java.name
    }
}
