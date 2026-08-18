/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
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

package com.amazonaws.services.schemaregistry.caching

interface GlueSchemaRegistryCache<K, V, Stats> {
    /**
     * Get the Value corresponding to the key.
     *
     * @param key key for cache entry
     * @return Value associated with the key, or null when absent.
     */
    fun get(key: K): V?

    /**
     * Put the key and value in the cache for subsequent use.
     */
    fun put(
        key: K,
        value: V,
    )

    /**
     * Deletes the given element from the cache.
     */
    fun delete(key: K)

    /**
     * Flushes the content of the cache.
     */
    fun flushCache()

    /**
     * Get the current cache size.
     *
     * @return number of entries in the cache
     */
    fun getCacheSize(): Long

    /**
     * Get the cache stats. This will give the detailed view of the cache.
     */
    fun getCacheStats(): Stats
}
