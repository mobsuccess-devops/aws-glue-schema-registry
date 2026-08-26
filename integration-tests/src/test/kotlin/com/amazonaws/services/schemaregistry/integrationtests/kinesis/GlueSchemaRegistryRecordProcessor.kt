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
package com.amazonaws.services.schemaregistry.integrationtests.kinesis

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializer
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerFactory
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl
import org.apache.logging.log4j.LogManager
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.services.glue.model.DataFormat
import software.amazon.kinesis.exceptions.InvalidStateException
import software.amazon.kinesis.exceptions.ShutdownException
import software.amazon.kinesis.lifecycle.events.InitializationInput
import software.amazon.kinesis.lifecycle.events.LeaseLostInput
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput
import software.amazon.kinesis.lifecycle.events.ShardEndedInput
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput
import software.amazon.kinesis.processor.ShardRecordProcessor
import java.nio.ByteBuffer

class GlueSchemaRegistryRecordProcessor(
    private val recordProcessor: RecordProcessor,
    private val glueSchemaRegistryDeserializerFactory: GlueSchemaRegistryDeserializerFactory,
    private val gsrConfig: GlueSchemaRegistryConfiguration,
) : ShardRecordProcessor {
    private val glueSchemaRegistryDeserializer: GlueSchemaRegistryDeserializer =
        GlueSchemaRegistryDeserializerImpl(DefaultCredentialsProvider.builder().build(), gsrConfig)

    override fun initialize(initializationInput: InitializationInput) {
        recordProcessor.creationSuccess = true
        LOGGER.info("Initializing GlueSchemaRegistryRecordProcessor")
    }

    override fun processRecords(processRecordsInput: ProcessRecordsInput) {
        recordProcessor.consumptionSuccess = true
        try {
            LOGGER.info("Processing {} record(s)", processRecordsInput.records().size)
            for (r in processRecordsInput.records()) {
                val bb = r.data()
                val bytes = ByteArray(bb.remaining())
                bb.get(bytes)

                val gsrSchema = glueSchemaRegistryDeserializer.getSchema(bytes)
                LOGGER.info("Consumed Schema from GSR : {}", gsrSchema.schemaDefinition)
                val decodedRecord =
                    glueSchemaRegistryDeserializerFactory
                        .getInstance(DataFormat.valueOf(gsrSchema.dataFormat), gsrConfig)
                        .deserialize(ByteBuffer.wrap(bytes), gsrSchema)

                recordProcessor.consumedRecords.add(decodedRecord)

                LOGGER.info("Processed record: {}", decodedRecord)
            }
        } catch (e: Exception) {
            LOGGER.error("Failed while processing records. Aborting", e)
            Runtime.getRuntime().halt(1)
        }
    }

    override fun leaseLost(leaseLostInput: LeaseLostInput) {
        LOGGER.info("Lost lease, so terminating.")
    }

    override fun shardEnded(shardEndedInput: ShardEndedInput) {
        try {
            LOGGER.info("Reached shard end checkpointing.")
            shardEndedInput.checkpointer().checkpoint()
        } catch (e: ShutdownException) {
            LOGGER.error("Exception while checkpointing at shard end. Giving up.", e)
        } catch (e: InvalidStateException) {
            LOGGER.error("Exception while checkpointing at shard end. Giving up.", e)
        }
    }

    override fun shutdownRequested(shutdownRequestedInput: ShutdownRequestedInput) {
        try {
            LOGGER.info("Scheduler is shutting down, checkpointing.")
            shutdownRequestedInput.checkpointer().checkpoint()
        } catch (e: ShutdownException) {
            LOGGER.error("Exception while checkpointing at requested shutdown. Giving up.", e)
        } catch (e: InvalidStateException) {
            LOGGER.error("Exception while checkpointing at requested shutdown. Giving up.", e)
        }
    }

    companion object {
        private val LOGGER = LogManager.getLogger(GlueSchemaRegistryRecordProcessor::class.java)
    }
}
