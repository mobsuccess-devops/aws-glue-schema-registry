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

package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata

import com.google.protobuf.Descriptors
import java.util.LinkedList

object DescriptorTree {
    /**
     * Does a level order traversal on the nested descriptors to build a map of the absolute path of
     * a descriptor to that descriptor. For `message A { message B {} } message C {}` the result is
     * `{".A" -> A, ".A.B" -> B, ".C" -> C}`.
     */
    @JvmStatic
    fun parseAllDescriptors(fileDescriptor: Descriptors.FileDescriptor): Map<String, Descriptors.Descriptor> {
        val parentPath = "."
        val traversalQueue: java.util.Queue<DescriptorWithPath> = LinkedList()
        val messagesByName = LinkedHashMap<String, Descriptors.Descriptor>()

        // Add all the top level types to the queue to begin with.
        fileDescriptor.messageTypes.forEach {
            traversalQueue.add(DescriptorWithPath(it, parentPath + it.name))
        }

        while (traversalQueue.isNotEmpty()) {
            val descriptorWithPath = traversalQueue.remove()
            val descriptor = descriptorWithPath.descriptor
            val descriptorPath = descriptorWithPath.path

            messagesByName[descriptorPath] = descriptor

            // Add the nested types to the queue.
            descriptor.nestedTypes.forEach {
                traversalQueue.add(DescriptorWithPath(it, "$descriptorPath.${it.name}"))
            }
        }

        return messagesByName
    }

    private data class DescriptorWithPath(
        val descriptor: Descriptors.Descriptor,
        val path: String,
    )
}
