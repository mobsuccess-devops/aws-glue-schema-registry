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
import com.amazonaws.services.schemaregistry.utils.AvroRecordType
import software.amazon.awssdk.services.glue.model.Compatibility
import software.amazon.awssdk.services.glue.model.DataFormat

enum class TestDataGeneratorType(
    val dataFormat: DataFormat,
    val recordType: AvroRecordType,
    val compatibility: Compatibility,
) {
    AVRO_GENERIC_NONE(DataFormat.AVRO, AvroRecordType.GENERIC_RECORD, Compatibility.NONE),
    AVRO_GENERIC_BACKWARD(DataFormat.AVRO, AvroRecordType.GENERIC_RECORD, Compatibility.BACKWARD),
    AVRO_GENERIC_BACKWARD_ALL(DataFormat.AVRO, AvroRecordType.GENERIC_RECORD, Compatibility.BACKWARD_ALL),
    AVRO_SPECIFIC_NONE(DataFormat.AVRO, AvroRecordType.SPECIFIC_RECORD, Compatibility.NONE),
    JSON_GENERIC_NONE(DataFormat.JSON, AvroRecordType.GENERIC_RECORD, Compatibility.NONE),
    JSON_GENERIC_BACKWARD(DataFormat.JSON, AvroRecordType.GENERIC_RECORD, Compatibility.BACKWARD),
    JSON_SPECIFIC_NONE(DataFormat.JSON, AvroRecordType.SPECIFIC_RECORD, Compatibility.NONE),
    PROTOBUF_GENERIC_BACKWARD(DataFormat.PROTOBUF, AvroRecordType.GENERIC_RECORD, Compatibility.BACKWARD),
    PROTOBUF_SPECIFIC_NONE(DataFormat.PROTOBUF, AvroRecordType.SPECIFIC_RECORD, Compatibility.NONE),
    PROTOBUF_GENERIC_NONE(DataFormat.PROTOBUF, AvroRecordType.GENERIC_RECORD, Compatibility.NONE),
    ;

    companion object {
        @JvmStatic
        fun valueOf(
            dataFormat: DataFormat,
            recordType: AvroRecordType,
            compatibility: Compatibility,
        ): TestDataGeneratorType {
            for (generatorType in entries) {
                if (generatorType.dataFormat == dataFormat &&
                    generatorType.recordType == recordType &&
                    generatorType.compatibility == compatibility
                ) {
                    return generatorType
                }
            }
            val message =
                String.format("Unsupported data generator type : %s_%s_%s", dataFormat, recordType, compatibility)
            throw AWSSchemaRegistryException(message)
        }
    }
}
