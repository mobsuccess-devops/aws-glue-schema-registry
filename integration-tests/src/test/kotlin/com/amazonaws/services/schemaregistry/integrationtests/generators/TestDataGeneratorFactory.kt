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
package com.amazonaws.services.schemaregistry.integrationtests.generators

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import java.util.concurrent.ConcurrentHashMap

/**
 * Factory to create a new instance of test data generator.
 */
class TestDataGeneratorFactory {
    private val dataGeneratorMap = ConcurrentHashMap<TestDataGeneratorType, TestDataGenerator<*>>()

    /**
     * Lazy initializes and returns a test data generator instance.
     *
     * @param testDataGeneratorType testDataGeneratorType
     * @return test data generator instance.
     */
    fun getInstance(testDataGeneratorType: TestDataGeneratorType): TestDataGenerator<*> {
        val factory: () -> TestDataGenerator<*> =
            when (testDataGeneratorType) {
                TestDataGeneratorType.AVRO_GENERIC_NONE -> ::AvroGenericNoneCompatDataGenerator
                TestDataGeneratorType.AVRO_GENERIC_BACKWARD -> ::AvroGenericBackwardCompatDataGenerator
                TestDataGeneratorType.AVRO_SPECIFIC_NONE -> ::AvroSpecificNoneCompatDataGenerator
                TestDataGeneratorType.JSON_GENERIC_NONE -> ::JsonSchemaGenericNoneCompatDataGenerator
                TestDataGeneratorType.JSON_GENERIC_BACKWARD -> ::JsonSchemaGenericBackwardCompatDataGenerator
                TestDataGeneratorType.JSON_SPECIFIC_NONE -> ::JsonSchemaSpecificNoneCompatDataGenerator
                TestDataGeneratorType.PROTOBUF_SPECIFIC_NONE -> ::ProtobufSpecificNoneCompatDataGenerator
                TestDataGeneratorType.PROTOBUF_GENERIC_NONE -> ::ProtobufGenericNoneCompatDataGenerator
                TestDataGeneratorType.PROTOBUF_GENERIC_BACKWARD -> ::ProtobufGenericBackwardDataGenerator
                else -> throw AWSSchemaRegistryException(
                    String.format("Unsupported data generator type: %s", testDataGeneratorType),
                )
            }
        return dataGeneratorMap.computeIfAbsent(testDataGeneratorType) { factory() }
    }
}
