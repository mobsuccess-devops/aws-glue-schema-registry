package com.amazonaws.services.schemaregistry.serializers.protobuf

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.Basic
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.ComplexNestingSyntax2
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.ComplexNestingSyntax3
import com.amazonaws.services.schemaregistry.utils.nullOf
import com.google.common.collect.BiMap
import com.google.protobuf.Descriptors
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Named
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource

class MessageIndexFinderTest {
    @ParameterizedTest
    @MethodSource("testBasicProtoFileProvider")
    fun testGetAll_IdentifiesMessageIndex_ForBasicProtobufSchemas(testCase: ProtobufTestCase) {
        val schema = testCase.getSchema()
        val expected = HashMap<String, Int>()
        val packageName = testCase.getPackage()
        expected["$packageName.Address"] = 0
        expected["$packageName.Customer"] = 1

        val actual = getDescriptorNameMap(messageIndexFinder.getAll(schema.file))

        assertEquals(expected, actual, testCase.fileName)
    }

    @ParameterizedTest
    @MethodSource("testNestedProtoFileProvider")
    fun testGetAll_IdentifiesMessageIndex_ForNestedProtobufSchemas(testCase: ProtobufTestCase) {
        val schema = testCase.getSchema()
        val expected = HashMap<String, Int>()
        val packageName = testCase.getPackage()
        expected["$packageName.A"] = 0
        expected["$packageName.A.B"] = 1
        expected["$packageName.A.B.C"] = 2
        expected["$packageName.A.B.C.J"] = 3
        expected["$packageName.A.B.C.J.K"] = 4
        expected["$packageName.A.B.C.X"] = 5
        expected["$packageName.A.B.C.X.D"] = 6
        expected["$packageName.A.B.C.X.D.F"] = 7
        expected["$packageName.A.B.C.X.D.F.M"] = 8
        expected["$packageName.A.B.C.X.D.G"] = 9
        expected["$packageName.A.B.C.X.L"] = 10
        expected["$packageName.A.I"] = 11
        expected["$packageName.A.X"] = 12
        expected["$packageName.N"] = 13
        expected["$packageName.O"] = 14
        expected["$packageName.O.A"] = 15

        val actual = getDescriptorNameMap(messageIndexFinder.getAll(schema.file))

        assertEquals(expected, actual, testCase.toString())
    }

    @ParameterizedTest
    @MethodSource("testDescriptorProvider")
    fun testGetByDescriptor_IdentifiesMessageIndex_ForGeneratedPOJO(descriptor: Descriptors.Descriptor) {
        val actual = messageIndexFinder.getByDescriptor(descriptor.file, descriptor)

        // A.B.C.X.D
        val expected = 6

        assertEquals(expected, actual, descriptor.fullName)
    }

    @ParameterizedTest
    @MethodSource("testMissingDescriptorProvider")
    fun testGetByDescriptor_WhenNonExistentTypePassed_ThrowsSchemaRegistryError(
        fileDescriptor: Descriptors.FileDescriptor,
        descriptor: Descriptors.Descriptor,
    ) {
        val ex =
            assertThrows(AWSSchemaRegistryException::class.java) {
                messageIndexFinder.getByDescriptor(fileDescriptor, descriptor)
            }
        val errorMessage = "Provided descriptor is not present in the schema: ${descriptor.fullName}"

        assertEquals(errorMessage, ex.message)
    }

    @ParameterizedTest
    @MethodSource("testBasicDescriptorFileProvider")
    fun testGetByDescriptor_WhenNullsArePassed_ThrowsValidationException(descriptor: Descriptors.Descriptor) {
        assertThrows(NullPointerException::class.java) {
            messageIndexFinder.getByDescriptor(descriptor.file, nullOf())
        }

        assertThrows(NullPointerException::class.java) {
            messageIndexFinder.getByDescriptor(nullOf(), descriptor)
        }
    }

    @ParameterizedTest
    @MethodSource("testBasicDescriptorFileProvider")
    fun testGetByIndex_IdentifiesDescriptor_ForGeneratedPOJO(descriptor: Descriptors.Descriptor) {
        val actual = messageIndexFinder.getByIndex(descriptor.file, 1)

        // Basic.Customer
        val expected = Basic.Customer.getDescriptor()

        assertEquals(expected, actual, descriptor.fullName)
    }

    @ParameterizedTest
    @MethodSource("testBasicDescriptorFileProvider")
    fun testGetByIndex_WhenNullsArePassed_ThrowsValidationException(descriptor: Descriptors.Descriptor) {
        assertThrows(NullPointerException::class.java) {
            messageIndexFinder.getByIndex(descriptor.file, null)
        }

        assertThrows(NullPointerException::class.java) {
            messageIndexFinder.getByIndex(nullOf(), 1)
        }
    }

    @ParameterizedTest
    @MethodSource("testMissingDescriptorProvider")
    fun testGetByIndex_WhenNonExistentIdPassed_ThrowsSchemaRegistryError(
        fileDescriptor: Descriptors.FileDescriptor,
        descriptor: Descriptors.Descriptor,
    ) {
        val missingIndex = 100
        val ex =
            assertThrows(AWSSchemaRegistryException::class.java) {
                messageIndexFinder.getByIndex(fileDescriptor, missingIndex)
            }
        val errorMessage = "No corresponding descriptor found for the index: $missingIndex"

        assertEquals(errorMessage, ex.message)
    }

    private fun getDescriptorNameMap(descriptorMap: BiMap<Descriptors.Descriptor, Int>): Map<String, Int> = descriptorMap.entries.associate { kv -> kv.key.fullName to kv.value }

    companion object {
        private val messageIndexFinder = MessageIndexFinder()

        @JvmStatic
        fun testBasicProtoFileProvider(): List<Arguments> {
            val testCases = ProtobufTestCaseReader.getTestCasesByNames("Basic.proto")
            return testCases.map { Arguments.of(it) }
        }

        @JvmStatic
        fun testNestedProtoFileProvider(): List<Arguments> {
            val testCases =
                ProtobufTestCaseReader.getTestCasesByNames(
                    "ComplexNestingSyntax3.proto",
                    "ComplexNestingSyntax2.proto",
                )
            return testCases.map { Arguments.of(it) }
        }

        @JvmStatic
        fun testBasicDescriptorFileProvider(): List<Arguments> = listOf(Arguments.of(named(Basic.Customer.getDescriptor())))

        @JvmStatic
        fun testDescriptorProvider(): List<Arguments> = listOf(
            Arguments.of(named(ComplexNestingSyntax3.A.B.C.X.D.getDescriptor())),
            Arguments.of(named(ComplexNestingSyntax2.A.B.C.X.D.getDescriptor())),
        )

        @JvmStatic
        fun testMissingDescriptorProvider(): List<Arguments> = listOf(
            Arguments.of(
                // Search for Basic type Address in ComplexNesting.
                named(ComplexNestingSyntax3.getDescriptor().file),
                named(Basic.Address.getDescriptor()),
            ),
        )

        private fun named(descriptor: Descriptors.Descriptor): Named<Descriptors.Descriptor> = Named.of(descriptor.fullName, descriptor)

        private fun named(fileDescriptor: Descriptors.FileDescriptor): Named<Descriptors.FileDescriptor> = Named.of(fileDescriptor.name, fileDescriptor)
    }
}
