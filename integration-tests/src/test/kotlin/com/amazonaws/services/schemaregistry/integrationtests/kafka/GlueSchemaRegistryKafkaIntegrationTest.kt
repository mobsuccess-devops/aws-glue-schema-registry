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
package com.amazonaws.services.schemaregistry.integrationtests.kafka

import com.amazonaws.services.schemaregistry.deserializers.protobuf.ProtobufClassName
import com.amazonaws.services.schemaregistry.integrationtests.generators.AvroGenericBackwardCompatDataGenerator
import com.amazonaws.services.schemaregistry.integrationtests.generators.JsonSchemaGenericBackwardCompatDataGenerator
import com.amazonaws.services.schemaregistry.integrationtests.generators.ProtobufGenericBackwardDataGenerator
import com.amazonaws.services.schemaregistry.integrationtests.generators.ProtobufSpecificNoneCompatDataGenerator
import com.amazonaws.services.schemaregistry.integrationtests.generators.TestDataGeneratorFactory
import com.amazonaws.services.schemaregistry.integrationtests.generators.TestDataGeneratorType
import com.amazonaws.services.schemaregistry.integrationtests.properties.GlueSchemaRegistryConnectionProperties
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.AvroRecordType
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Message
import org.apache.avro.generic.GenericRecord
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.tuple.Pair
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.equalTo
import org.hamcrest.Matchers.hasKey
import org.hamcrest.Matchers.`is`
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.MethodSource
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.glue.GlueClient
import software.amazon.awssdk.services.glue.model.Compatibility
import software.amazon.awssdk.services.glue.model.DataFormat
import software.amazon.awssdk.services.glue.model.DeleteSchemaRequest
import software.amazon.awssdk.services.glue.model.EntityNotFoundException
import software.amazon.awssdk.services.glue.model.SchemaId
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Collections
import java.util.stream.Collectors
import java.util.stream.Stream

/**
 * The test class for Kafka related tests for Glue Schema Registry
 */
class GlueSchemaRegistryKafkaIntegrationTest {
    private val testDataGeneratorFactory = TestDataGeneratorFactory()

    @Test
    fun testProduceConsumeWithoutGlueSchemaRegistry() {
        log.info("Starting the test for producing and consuming messages via Kafka ...")

        val kafkaHelperPair = createAndGetKafkaHelper(TOPIC_NAME_PREFIX)
        val topic = kafkaHelperPair.key
        val kafkaHelper = kafkaHelperPair.value

        val recordsProduced = 20

        kafkaHelper.doProduce(topic, recordsProduced)

        val consumerProperties =
            ConsumerProperties
                .builder()
                .topicName(topic)
                .build()

        val recordsConsumed = kafkaHelper.doConsume(consumerProperties)
        log.info("Producing {} records, and consuming {} records", recordsProduced, recordsConsumed)

        assertEquals(recordsConsumed, recordsProduced)
        log.info("Finish the test for producing/consuming messages via Kafka.")
    }

    // TODO : Invalid JSON Tests
    @ParameterizedTest
    @MethodSource("testArgumentsProvider")
    fun testProduceConsumeWithSchemaRegistry(
        dataFormat: DataFormat,
        avroRecordType: AvroRecordType,
        compatibility: Compatibility,
        compression: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        log.info("Starting the test for producing and consuming {} messages via Kafka ...", dataFormat.name)
        val kafkaHelperPair = createAndGetKafkaHelper(TOPIC_NAME_PREFIX)
        val topic = kafkaHelperPair.key
        val kafkaHelper = kafkaHelperPair.value

        val testDataGenerator =
            testDataGeneratorFactory.getInstance(
                TestDataGeneratorType.valueOf(dataFormat, avroRecordType, compatibility),
            )
        val records = testDataGenerator.createRecords()

        val schemaName = String.format("%s-%s-%s", topic, dataFormat.name, compatibility)
        schemasToCleanUp.add(schemaName)

        val producerProperties =
            ProducerProperties
                .builder()
                .topicName(topic)
                .schemaName(schemaName)
                .dataFormat(dataFormat.name)
                .compatibilityType(compatibility.name)
                .compressionType(compression.name)
                .autoRegistrationEnabled("true")
                .build()

        val producerRecords = kafkaHelper.doProduceRecords<Any>(producerProperties, records)

        val consumerPropertiesBuilder = ConsumerProperties.builder().topicName(topic)
        consumerPropertiesBuilder.protobufMessageType(ProtobufMessageType.DYNAMIC_MESSAGE.getName())
        consumerPropertiesBuilder.avroRecordType(avroRecordType.getName()) // Only required for the case of AVRO
        // Only the specific-record JSON generator emits a schema carrying a className; opt in to
        // resolving it for that combination so the consumer gets a POJO back rather than a
        // JsonDataWithSchema.
        if (dataFormat == DataFormat.JSON && AvroRecordType.SPECIFIC_RECORD == avroRecordType) {
            consumerPropertiesBuilder.jsonClassNameResolutionEnabled(true)
            consumerPropertiesBuilder.jsonClassNameAllowlist(JSON_SPECIFIC_RECORD_CLASS_NAME)
        }

        val consumerRecords = kafkaHelper.doConsumeRecords<Any>(consumerPropertiesBuilder.build())

        assertRecordsEquality(producerRecords, consumerRecords)
        log.info("Finished test for producing/consuming {} messages via Kafka.", dataFormat.name)
    }

    @ParameterizedTest
    @MethodSource("testArgumentsProvider")
    fun testProduceConsumeWithSchemaRegistryMultiThreaded(
        dataFormat: DataFormat,
        avroRecordType: AvroRecordType,
        compatibility: Compatibility,
        compression: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        log.info("Starting the test for producing and consuming {} messages via Kafka ...", dataFormat.name)
        val kafkaHelperPair = createAndGetKafkaHelper(TOPIC_NAME_PREFIX)
        val topic = kafkaHelperPair.key
        val kafkaHelper = kafkaHelperPair.value

        val testDataGenerator =
            testDataGeneratorFactory.getInstance(
                TestDataGeneratorType.valueOf(dataFormat, avroRecordType, compatibility),
            )
        val records = testDataGenerator.createRecords()

        val schemaName = String.format("%s-%s-%s", topic, dataFormat.name, compatibility)
        schemasToCleanUp.add(schemaName)

        val producerProperties =
            ProducerProperties
                .builder()
                .topicName(topic)
                .schemaName(schemaName)
                .dataFormat(dataFormat.name)
                .compatibilityType(compatibility.name)
                .compressionType(compression.name)
                .autoRegistrationEnabled("true")
                .build()

        val producerRecords = kafkaHelper.doProduceRecordsMultithreaded<Any>(producerProperties, records)

        val consumerPropertiesBuilder = ConsumerProperties.builder().topicName(topic)
        consumerPropertiesBuilder.protobufMessageType(ProtobufMessageType.DYNAMIC_MESSAGE.getName())
        consumerPropertiesBuilder.avroRecordType(avroRecordType.getName()) // Only required for the case of AVRO
        // Only the specific-record JSON generator emits a schema carrying a className; opt in to
        // resolving it for that combination so the consumer gets a POJO back rather than a
        // JsonDataWithSchema.
        if (dataFormat == DataFormat.JSON && AvroRecordType.SPECIFIC_RECORD == avroRecordType) {
            consumerPropertiesBuilder.jsonClassNameResolutionEnabled(true)
            consumerPropertiesBuilder.jsonClassNameAllowlist(JSON_SPECIFIC_RECORD_CLASS_NAME)
        }

        val consumerRecords = kafkaHelper.doConsumeRecords<Any>(consumerPropertiesBuilder.build())

        assertEquals(producerRecords.size, consumerRecords.size)
        log.info("Finished test for producing/consuming {} messages via Kafka.", dataFormat.name)
    }

    @Test
    fun testProduceConsumeMultipleDataFormatRecords() {
        val compression = AWSSchemaRegistryConstants.COMPRESSION.ZLIB
        val compatibility = Compatibility.NONE
        val recordType = AvroRecordType.GENERIC_RECORD

        val kafkaHelperPair = createAndGetKafkaHelper(TOPIC_NAME_PREFIX)
        val topic = kafkaHelperPair.key
        val kafkaHelper = kafkaHelperPair.value

        val producerRecords = ArrayList<ProducerRecord<String, Any>>()

        for (dataFormat in listOf(DataFormat.AVRO, DataFormat.JSON, DataFormat.PROTOBUF)) {
            log.info("Starting the test for producing {} messages via Kafka ...", dataFormat.name)
            val testDataGenerator =
                testDataGeneratorFactory.getInstance(
                    TestDataGeneratorType.valueOf(dataFormat, recordType, compatibility),
                )
            val records = Collections.singletonList(testDataGenerator.createRecords()[0])

            val schemaName = String.format("%s-%s-%s", topic, dataFormat.name, compatibility)
            schemasToCleanUp.add(schemaName)

            val producerProperties =
                ProducerProperties
                    .builder()
                    .topicName(topic)
                    .schemaName(schemaName)
                    .dataFormat(dataFormat.name)
                    .compatibilityType(compatibility.name)
                    .compressionType(compression.name)
                    .autoRegistrationEnabled("true")
                    .build()

            producerRecords.addAll(kafkaHelper.doProduceRecords(producerProperties, records))
        }

        val consumerProperties =
            ConsumerProperties
                .builder()
                .topicName(topic)
                .avroRecordType(recordType.getName()) // Only required for the case of AVRO
                .build()

        log.info("Starting the test for consuming multi-format messages via Kafka ...")

        val consumerRecords = kafkaHelper.doConsumeRecords<Any>(consumerProperties)

        assertEquals(producerRecords.size, consumerRecords.size)
        log.info("Finished test for producing/consuming multi-format messages via Kafka.")
    }

    @Test
    fun testProduceConsumeWithSerDeSchemaRegistry() {
        val dataFormat = DataFormat.AVRO
        val compression = AWSSchemaRegistryConstants.COMPRESSION.ZLIB
        val recordType = AvroRecordType.GENERIC_RECORD
        val compatibility = Compatibility.NONE
        log.info(
            "Serde Test Starting the test for producing and consuming {} messages via Kafka ...",
            dataFormat.name,
        )
        val kafkaHelperPair = createAndGetKafkaHelper(TOPIC_NAME_PREFIX)
        val topic = kafkaHelperPair.key
        val kafkaHelper = kafkaHelperPair.value

        val testDataGenerator =
            testDataGeneratorFactory.getInstance(
                TestDataGeneratorType.valueOf(dataFormat, recordType, compatibility),
            )
        val records = testDataGenerator.createRecords()

        val schemaName = String.format("%s-%s-%s", topic, dataFormat.name, compatibility)
        schemasToCleanUp.add(schemaName)

        val producerProperties =
            ProducerProperties
                .builder()
                .topicName(topic)
                .schemaName(schemaName)
                .dataFormat(dataFormat.name)
                .compatibilityType(compatibility.toString())
                .compressionType(compression.name)
                .autoRegistrationEnabled("true")
                .build()

        val producerRecords = kafkaHelper.doProduceAvroRecordsSerde(producerProperties, records)

        val consumerProperties =
            ConsumerProperties
                .builder()
                .topicName(topic)
                .avroRecordType(recordType.getName()) // Only required for the case of AVRO
                .build()
        val consumerRecords = kafkaHelper.doConsumeAvroRecordsSerde(consumerProperties)

        assertRecordsEquality(producerRecords, consumerRecords)

        log.info(
            "Finish the test for producing/consuming {} messages via Kafka with passing serde from constructor.",
            dataFormat.name,
        )
    }

    @ParameterizedTest
    @EnumSource(value = DataFormat::class, mode = EnumSource.Mode.EXCLUDE, names = ["UNKNOWN_TO_SDK_VERSION"])
    fun testKafkaStreamsProcess(dataFormat: DataFormat) {
        val compatibility = Compatibility.BACKWARD
        val recordType = AvroRecordType.GENERIC_RECORD
        log.info("Serde Test Starting the test for processing {} message streaming via Kafka ...", dataFormat.name)

        val kafkaHelperInputTopicPair = createAndGetKafkaHelper(INPUT_TOPIC_NAME_PREFIX_FOR_STREAMS)
        val inputTopic = kafkaHelperInputTopicPair.key
        val kafkaHelper = kafkaHelperInputTopicPair.value

        val kafkaHelperOutputTopicPair = createAndGetKafkaHelper(OUTPUT_TOPIC_NAME_PREFIX_FOR_STREAMS)
        val outputTopic = kafkaHelperOutputTopicPair.key

        val schemaName = String.format("%s-%s-%s", inputTopic, dataFormat.name, compatibility)
        schemasToCleanUp.add(schemaName)

        val producerProperties =
            ProducerProperties
                .builder()
                .topicName(inputTopic)
                .inputTopic(inputTopic)
                .outputTopic(outputTopic)
                .schemaName(schemaName)
                .dataFormat(dataFormat.name)
                .recordType(getRecordType(dataFormat, recordType))
                .compatibilityType(compatibility.name)
                .compressionType(AWSSchemaRegistryConstants.COMPRESSION.ZLIB.name)
                .autoRegistrationEnabled("true")
                .build()

        val testDataGenerator =
            testDataGeneratorFactory.getInstance(
                TestDataGeneratorType.valueOf(dataFormat, recordType, compatibility),
            )
        val records = testDataGenerator.createRecords()

        val producerRecords = kafkaHelper.doProduceRecords<Any>(producerProperties, records)
        kafkaHelper.doKafkaStreamsProcess(producerProperties)

        val consumerProperties =
            ConsumerProperties
                .builder()
                .topicName(outputTopic)
                .avroRecordType(recordType.getName()) // Only required for the case of AVRO
                .build()
        val consumerRecords = kafkaHelper.doConsumeRecords<Any>(consumerProperties)

        assertStreamsRecordsEquality(dataFormat, producerRecords, consumerRecords)

        log.info(
            "Finish the test for processing {} message streaming via Kafka with passing serde from constructor.",
            dataFormat.name,
        )
    }

    // This test doesn't fit into the existing test framework. We have to refactor it a lot make this test case fit.
    @ParameterizedTest
    @MethodSource("testProtobufDataProviderForPOJOs")
    fun testKafkaDeserializeProtobufForPOJODeserialization(message: Message) {
        val kafkaHelperPair = createAndGetKafkaHelper(TOPIC_NAME_PREFIX)
        val topic = kafkaHelperPair.key
        val kafkaHelper = kafkaHelperPair.value

        // Schema name needs to be different for every test case.
        val schemaName = ProtobufClassName.normalize(message.descriptorForType.file.fullName)
        schemasToCleanUp.add(schemaName)

        val producerProperties =
            ProducerProperties
                .builder()
                .topicName(topic)
                .schemaName(schemaName)
                .dataFormat(DataFormat.PROTOBUF.toString())
                .compatibilityType(Compatibility.NONE.name)
                .compressionType(AWSSchemaRegistryConstants.COMPRESSION.ZLIB.name)
                .autoRegistrationEnabled("true")
                .build()

        val messages = Collections.singletonList(message)
        val producerRecords = kafkaHelper.doProduceRecords<Any>(producerProperties, messages)

        val consumerProperties =
            ConsumerProperties
                .builder()
                .topicName(topic)
                .protobufMessageType(ProtobufMessageType.POJO.getName())
                .build()

        val consumerRecords = kafkaHelper.doConsumeRecords<Any>(consumerProperties)

        assertRecordsEquality(producerRecords, consumerRecords)
        log.info("Finished test for producing/consuming {} POJO messages via Kafka.", DataFormat.PROTOBUF)
    }

    private fun <T> assertRecordsEquality(
        producerRecords: List<ProducerRecord<String, T>>,
        consumerRecords: List<ConsumerRecord<String, T>>,
    ) {
        assertThat(producerRecords.size, `is`(equalTo(consumerRecords.size)))
        val producerRecordsMap =
            producerRecords
                .stream()
                .collect(Collectors.toMap({ it.key() }, { it.value() }))

        for (consumerRecord in consumerRecords) {
            assertThat(producerRecordsMap, hasKey(consumerRecord.key()))
            if (consumerRecord.value() is DynamicMessage) {
                assertDynamicRecords(consumerRecord, producerRecordsMap)
            } else {
                assertThat(consumerRecord.value(), `is`(equalTo(producerRecordsMap[consumerRecord.key()])))
            }
        }
    }

    private fun <T> assertStreamsRecordsEquality(
        dataFormat: DataFormat,
        producerRecords: List<ProducerRecord<String, T>>,
        consumerRecords: List<ConsumerRecord<String, T>>,
    ) {
        val producerRecordsMap: Map<String, T> =
            when (dataFormat) {
                DataFormat.AVRO ->
                    producerRecords
                        .stream()
                        .filter { AvroGenericBackwardCompatDataGenerator.filterRecords(it.value() as GenericRecord) }
                        .collect(Collectors.toMap({ it.key() }, { it.value() }))
                DataFormat.JSON ->
                    producerRecords
                        .stream()
                        .filter {
                            JsonSchemaGenericBackwardCompatDataGenerator.filterRecords(
                                it.value() as JsonDataWithSchema,
                            )
                        }.collect(Collectors.toMap({ it.key() }, { it.value() }))
                DataFormat.PROTOBUF ->
                    producerRecords
                        .stream()
                        .filter { ProtobufGenericBackwardDataGenerator.filterRecords(it.value() as Message) }
                        .collect(Collectors.toMap({ it.key() }, { it.value() }))
                else -> throw RuntimeException("Data format is not supported")
            }

        assertThat(producerRecordsMap.size, `is`(equalTo(consumerRecords.size)))
        for (consumerRecord in consumerRecords) {
            assertThat(producerRecordsMap, hasKey(consumerRecord.key()))
            if (consumerRecord.value() is DynamicMessage) {
                assertDynamicRecords(consumerRecord, producerRecordsMap)
            } else {
                assertThat(consumerRecord.value(), `is`(equalTo(producerRecordsMap[consumerRecord.key()])))
            }
        }
    }

    private fun <T> assertDynamicRecords(
        consumerRecord: ConsumerRecord<String, T>,
        producerRecordsMap: Map<String, T>,
    ) {
        val consumerDynamicMessage = consumerRecord.value() as DynamicMessage
        val producerDynamicMessage = producerRecordsMap[consumerRecord.key()] as Message
        // In case of DynamicMessage de-serialization, we cannot equate them to POJO records,
        // so we check for their byte equality.
        assertThat(consumerDynamicMessage.toByteArray(), `is`(producerDynamicMessage.toByteArray()))
    }

    private fun getRecordType(
        dataFormat: DataFormat,
        avroRecordType: AvroRecordType,
    ): String {
        if (dataFormat == DataFormat.PROTOBUF) {
            return ProtobufMessageType.DYNAMIC_MESSAGE.getName()
        }

        return avroRecordType.getName()
    }

    /**
     * Test to verify that Kafka's native LZ4 compression works correctly with at.yawk.lz4:lz4-java dependency.
     * This test ensures that the replacement of org.lz4:lz4-java with at.yawk.lz4:lz4-java is functioning properly.
     */
    @Test
    fun testProduceConsumeWithKafkaLZ4Compression() {
        log.info("Starting test for Kafka native LZ4 compression...")

        val dataFormat = DataFormat.AVRO
        val compression = AWSSchemaRegistryConstants.COMPRESSION.NONE
        val recordType = AvroRecordType.GENERIC_RECORD
        val compatibility = Compatibility.NONE

        val kafkaHelperPair = createAndGetKafkaHelper(TOPIC_NAME_PREFIX)
        val topic = kafkaHelperPair.left
        val kafkaHelper = kafkaHelperPair.right

        val testDataGenerator =
            testDataGeneratorFactory.getInstance(
                TestDataGeneratorType.valueOf(dataFormat, recordType, compatibility),
            )
        val records = testDataGenerator.createRecords()

        val schemaName = String.format("%s-%s-%s-LZ4", topic, dataFormat.name, compatibility)
        schemasToCleanUp.add(schemaName)

        val producerProperties =
            ProducerProperties
                .builder()
                .topicName(topic)
                .schemaName(schemaName)
                .dataFormat(dataFormat.name)
                .compatibilityType(compatibility.name)
                .compressionType(compression.name)
                .autoRegistrationEnabled("true")
                // Enable Kafka's native LZ4 compression
                .kafkaCompressionType("lz4")
                .build()

        val producerRecords = kafkaHelper.doProduceRecords<Any>(producerProperties, records)

        val consumerPropertiesBuilder = ConsumerProperties.builder().topicName(topic)
        consumerPropertiesBuilder.avroRecordType(recordType.getName())

        val consumerRecords = kafkaHelper.doConsumeRecords<Any>(consumerPropertiesBuilder.build())

        assertRecordsEquality(producerRecords, consumerRecords)
        log.info("Successfully completed test for Kafka native LZ4 compression with {} records", consumerRecords.size)
    }

    companion object {
        private val log = LoggerFactory.getLogger(GlueSchemaRegistryKafkaIntegrationTest::class.java)
        private const val TOPIC_NAME_PREFIX = "SchemaRegistryTests"

        // The class named by the specific-record JSON generator's schema, allowlisted so that
        // JSON + SPECIFIC_RECORD deserializes back into the POJO rather than a JsonDataWithSchema.
        private const val JSON_SPECIFIC_RECORD_CLASS_NAME =
            "com.amazonaws.services.schemaregistry.integrationtests.generators.Car"
        private const val INPUT_TOPIC_NAME_PREFIX_FOR_STREAMS = "SchemaRegistryTestsStreamsInput"
        private const val OUTPUT_TOPIC_NAME_PREFIX_FOR_STREAMS = "SchemaRegistryTestsStreamsOutput"
        private val SCHEMA_REGISTRY_ENDPOINT_OVERRIDE = GlueSchemaRegistryConnectionProperties.ENDPOINT
        private val REGION = GlueSchemaRegistryConnectionProperties.REGION
        private val RECORD_TYPES: List<AvroRecordType> =
            AvroRecordType.entries.filter { it != AvroRecordType.UNKNOWN }
        private val COMPATIBILITIES: List<Compatibility> =
            Compatibility.knownValues().filter { it.toString() == "NONE" }
        private val localKafkaClusterHelper = LocalKafkaClusterHelper()
        private val awsCredentialsProvider = DefaultCredentialsProvider.builder().build()
        private val schemasToCleanUp = LinkedHashSet<String>()

        @JvmStatic
        fun testArgumentsProvider(): Stream<Arguments> {
            val argumentBuilder = Stream.builder<Arguments>()
            for (dataFormat in DataFormat.knownValues()) {
                for (recordType in RECORD_TYPES) {
                    for (compatibility in COMPATIBILITIES) {
                        for (compression in AWSSchemaRegistryConstants.COMPRESSION.entries) {
                            argumentBuilder.add(Arguments.of(dataFormat, recordType, compatibility, compression))
                        }
                    }
                }
            }
            return argumentBuilder.build()
        }

        @JvmStatic
        fun testProtobufDataProviderForPOJOs(): Stream<Arguments> = ProtobufSpecificNoneCompatDataGenerator()
            .createRecords()
            .stream()
            .map { Arguments.of(it) }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            log.info("Starting Clean-up of schemas created with GSR.")
            val glueClient =
                GlueClient
                    .builder()
                    .credentialsProvider(awsCredentialsProvider)
                    .region(Region.of(REGION))
                    .endpointOverride(URI(SCHEMA_REGISTRY_ENDPOINT_OVERRIDE))
                    .httpClient(UrlConnectionHttpClient.builder().build())
                    .build()

            for (schemaName in schemasToCleanUp) {
                log.info("Cleaning up schema {}..", schemaName)
                val deleteSchemaRequest =
                    DeleteSchemaRequest
                        .builder()
                        .schemaId(
                            SchemaId
                                .builder()
                                .registryName("default-registry")
                                .schemaName(schemaName)
                                .build(),
                        ).build()

                try {
                    glueClient.deleteSchema(deleteSchemaRequest)
                } catch (e: EntityNotFoundException) {
                    log.info("Schema {} is already gone, nothing to clean up: {}", schemaName, e.message)
                }
            }

            log.info("Finished Cleaning up {} schemas created with GSR.", schemasToCleanUp.size)
        }

        private fun createAndGetKafkaHelper(topicNamePrefix: String): Pair<String, KafkaHelper> {
            val topic =
                String.format(
                    "%s-%s-%s",
                    topicNamePrefix,
                    Instant
                        .now()
                        .atOffset(ZoneOffset.UTC)
                        .format(DateTimeFormatter.ofPattern("yy-MM-dd-HH-mm")),
                    RandomStringUtils.randomAlphanumeric(4),
                )

            val bootstrapString = localKafkaClusterHelper.getBootstrapString()
            val zookeeperConnectString = localKafkaClusterHelper.getZookeeperConnectString()
            val kafkaHelper =
                KafkaHelper(bootstrapString, zookeeperConnectString, localKafkaClusterHelper.getOrCreateCluster())
            kafkaHelper.createTopic(
                topic,
                localKafkaClusterHelper.getNumberOfPartitions(),
                localKafkaClusterHelper.getReplicationFactor(),
            )
            return Pair.of(topic, kafkaHelper)
        }
    }
}
