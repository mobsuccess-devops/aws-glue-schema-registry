package com.amazonaws.services.schemaregistry.serializers.protobuf

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.google.common.collect.BiMap
import com.google.common.collect.HashBiMap
import com.google.protobuf.Descriptors
import java.util.LinkedList

/**
 * MessageIndexFinder finds the position of message type in the overall schema.
 * This position is used to serialize / deserialize the correct Message type.
 */
class MessageIndexFinder {
    /**
     * Parses the given Schema descriptor, assigns indices and finds the index for the given descriptor.
     */
    fun getByDescriptor(
        schemaDescriptor: Descriptors.FileDescriptor,
        descriptorToFind: Descriptors.Descriptor,
    ): Int {
        val descriptorToIndexBiMap = getAll(schemaDescriptor)

        if (!descriptorToIndexBiMap.containsKey(descriptorToFind)) {
            throw AWSSchemaRegistryException(
                "Provided descriptor is not present in the schema: ${descriptorToFind.fullName}",
            )
        }

        return descriptorToIndexBiMap[descriptorToFind]!!
    }

    /**
     * Parses the given Schema descriptor, assigns indices and finds the descriptor for given Index.
     */
    // indexToFind stays boxed: the Java signature took an Integer and callers, including
    // the tests, pass null to assert the rejection.
    fun getByIndex(
        schemaDescriptor: Descriptors.FileDescriptor,
        indexToFind: Int?,
    ): Descriptors.Descriptor {
        val index = indexToFind!!
        val indexToDescriptorBiMap = getAll(schemaDescriptor).inverse()

        if (!indexToDescriptorBiMap.containsKey(index)) {
            throw AWSSchemaRegistryException("No corresponding descriptor found for the index: $index")
        }

        return indexToDescriptorBiMap[index]!!
    }

    /**
     * Parse the Protobuf Schema descriptor using level-order traversal, sort the descriptors
     * lexicographically and assign an index for each message type.
     *
     * TODO: Referencing other proto schemas using import statements is not supported yet.
     * https://github.com/awslabs/aws-glue-schema-registry/issues/32
     */
    fun getAll(schemaDescriptor: Descriptors.FileDescriptor): BiMap<Descriptors.Descriptor, Int> {
        val descriptorQueue: java.util.Queue<Descriptors.Descriptor> = LinkedList()
        val allDescriptors = ArrayList<Descriptors.Descriptor>()

        descriptorQueue.addAll(schemaDescriptor.messageTypes)

        while (descriptorQueue.isNotEmpty()) {
            val descriptor = descriptorQueue.remove()
            allDescriptors.add(descriptor)
            descriptorQueue.addAll(descriptor.nestedTypes)
        }

        // Sort descriptor names by lexicographical order and assign an index.
        allDescriptors.sortBy { it.fullName }

        val messageIndices: BiMap<Descriptors.Descriptor, Int> = HashBiMap.create(allDescriptors.size)
        for (index in allDescriptors.indices) {
            messageIndices[allDescriptors[index]] = index
        }

        return messageIndices
    }
}
