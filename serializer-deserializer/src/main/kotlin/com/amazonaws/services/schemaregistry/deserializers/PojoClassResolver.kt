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

/**
 * Resolves the application class a deserialized record is to be read into.
 *
 * The thread context class loader is tried first, then the class loader that defined this
 * library. The two differ wherever the application is loaded apart from its dependencies —
 * a Kafka Connect plugin directory, a repackaged Spring Boot jar, an application server — and
 * only the first of them can see the application's own classes.
 */
internal object PojoClassResolver {
    @JvmStatic
    @Throws(ClassNotFoundException::class)
    fun resolve(className: String): Class<*> {
        val contextClassLoader = Thread.currentThread().contextClassLoader
        if (contextClassLoader != null) {
            try {
                return Class.forName(className, true, contextClassLoader)
            } catch (_: ClassNotFoundException) {
                return Class.forName(className)
            } catch (_: LinkageError) {
                return Class.forName(className)
            }
        }
        return Class.forName(className)
    }
}
