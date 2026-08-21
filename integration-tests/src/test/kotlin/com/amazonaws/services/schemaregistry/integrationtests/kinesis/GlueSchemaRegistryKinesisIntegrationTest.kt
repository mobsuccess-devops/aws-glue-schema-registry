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

import cloud.localstack.Constants
import cloud.localstack.ServiceName
import com.amazonaws.services.kinesis.producer.KinesisProducer
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration
import com.amazonaws.services.kinesis.producer.UserRecordResult
import com.amazonaws.services.schemaregistry.common.Schema
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializer
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerFactory
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl
import com.amazonaws.services.schemaregistry.integrationtests.generators.TestDataGeneratorFactory
import com.amazonaws.services.schemaregistry.integrationtests.generators.TestDataGeneratorType
import com.amazonaws.services.schemaregistry.integrationtests.properties.GlueSchemaRegistryConnectionProperties
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerFactory
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.AvroRecordType
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType
import com.google.protobuf.Message
import org.apache.commons.lang3.RandomStringUtils
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.LogManager
import org.awaitility.Awaitility
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.core.SdkSystemSetting
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.glue.GlueClient
import software.amazon.awssdk.services.glue.model.Compatibility
import software.amazon.awssdk.services.glue.model.DataFormat
import software.amazon.awssdk.services.glue.model.DeleteSchemaRequest
import software.amazon.awssdk.services.glue.model.SchemaId
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType
import software.amazon.awssdk.services.kinesis.model.StreamStatus
import software.amazon.kinesis.common.ConfigsBuilder
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.metrics.NullMetricsFactory
import software.amazon.kinesis.retrieval.polling.PollingConfig
import java.net.URI
import java.nio.ByteBuffer
import java.time.Instant
import java.util.Collections
import java.util.LinkedList
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.stream.Stream

class GlueSchemaRegistryKinesisIntegrationTest {
    private val testDataGeneratorFactory = TestDataGeneratorFactory()
    private val glueSchemaRegistrySerializerFactory = GlueSchemaRegistrySerializerFactory()
    private val glueSchemaRegistryDeserializerFactory = GlueSchemaRegistryDeserializerFactory()

    @BeforeEach
    fun setUp() {
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false")
        kinesisClient =
            KinesisAsyncClient
                .builder()
                .endpointOverride(URI(LOCALSTACK_ENDPOINT))
                .region(Region.of(GlueSchemaRegistryConnectionProperties.REGION))
                .build()

        streamName = String.format("%s%s", TEST_KINESIS_STREAM_PREFIX, RandomStringUtils.randomAlphanumeric(4))
        LOGGER.info("Creating Kinesis Stream : {} with {} shards on localStack..", streamName, SHARD_COUNT)

        val createStreamRequest =
            CreateStreamRequest
                .builder()
                .streamName(streamName)
                .shardCount(SHARD_COUNT)
                .build()
        try {
            kinesisClient.createStream(createStreamRequest).get()
        } catch (e: ExecutionException) {
            // CreateStream is not idempotent and carries no idempotency token. If the first
            // attempt is slow enough that the SDK classifies it as a retryable failure, the
            // retry is a second, genuinely new create, and it reports ResourceInUseException
            // even though the original attempt succeeded server-side. The postcondition this
            // fixture needs is that the stream exists, and that exception is evidence it does,
            // so it is not a failure. The Awaitility barrier below still gates on ACTIVE.
            if (e.cause !is ResourceInUseException) {
                throw e
            }
            LOGGER.info("Kinesis Stream {} already exists; a create was retried. Continuing.", streamName)
        }
        Awaitility.await().until {
            StreamStatus.ACTIVE ==
                kinesisClient
                    .describeStream(
                        DescribeStreamRequest.builder().streamName(streamName).build(),
                    ).get()
                    .streamDescription()
                    .streamStatus()
        }
        LOGGER.info("Finished creating Kinesis Stream : {}", streamName)
    }

    @Test
    fun testKinesisProduceConsume() {
        LOGGER.info("Starting the test for producing/consuming messages on Kinesis ...")

        val message = "Hello World"

        val timestamp = Instant.now()

        val putRecordRequest =
            PutRecordRequest
                .builder()
                .streamName(streamName)
                .partitionKey(timestamp.toEpochMilli().toString())
                .data(SdkBytes.fromUtf8String(message))
                .build()
        val shardId =
            kinesisClient
                .putRecord(putRecordRequest)
                .get()
                .shardId()

        assertNotNull(shardId)

        val getShardIteratorRequest =
            GetShardIteratorRequest
                .builder()
                .streamName(streamName)
                .shardId(shardId)
                .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                .build()

        val shardIterator =
            kinesisClient
                .getShardIterator(getShardIteratorRequest)
                .get()
                .shardIterator()

        val getRecordRequest =
            GetRecordsRequest
                .builder()
                .shardIterator(shardIterator)
                .build()
        val recordsResponse = kinesisClient.getRecords(getRecordRequest).get()

        val records = recordsResponse.records().map { it.data().asUtf8String() }

        assertEquals(records.size, 1)
        assertEquals(message, records[0])

        LOGGER.info("Finished test for producing/consuming messages on Kinesis.")
    }

    @ParameterizedTest
    @MethodSource("testArgumentsProvider")
    fun testKinesisProduceConsumeWithGlueSchemaRegistry(
        dataFormat: DataFormat,
        recordType: AvroRecordType,
        compatibility: Compatibility,
        compression: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        LOGGER.info("Starting test for producing/consuming messages on Kinesis with Glue Schema Registry")

        val testDataGenerator =
            testDataGeneratorFactory.getInstance(
                TestDataGeneratorType.valueOf(dataFormat, recordType, compatibility),
            )
        val producerRecords = testDataGenerator.createRecords()

        val gsrConfig = getSchemaRegistryConfiguration(compatibility, compression, recordType, dataFormat)

        val shardId = produceRecordsWithKinesisSDK(streamName, producerRecords, dataFormat, compatibility, gsrConfig)

        val consumerRecords = consumeRecordsWithKinesisSDK(streamName, shardId, dataFormat, gsrConfig)

        assertNotEquals(0, consumerRecords.size)
        assertEquals(producerRecords.size, consumerRecords.size)
        assertKinesisRecords(dataFormat, producerRecords.toTypedArray(), consumerRecords.toTypedArray())

        LOGGER.info("Finished test for producing/consuming messages on Kinesis with Glue Schema Registry")
    }

    @ParameterizedTest
    @MethodSource("testArgumentsProvider")
    fun testProduceConsumeWithKPLAndKCL(
        dataFormat: DataFormat,
        recordType: AvroRecordType,
        compatibility: Compatibility,
        compression: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        LOGGER.info(
            "Starting test for producing/consuming messages on Kinesis Producer Library with Glue Schema Registry",
        )

        val testDataGenerator =
            testDataGeneratorFactory.getInstance(
                TestDataGeneratorType.valueOf(dataFormat, recordType, compatibility),
            )
        val producerRecords = testDataGenerator.createRecords()

        val gsrConfig = getSchemaRegistryConfiguration(compatibility, compression, recordType, dataFormat)

        val recordProcessor = RecordProcessor()
        val scheduler = startConsumingWithKCL(gsrConfig, recordProcessor)

        produceRecordsWithKPL(streamName, producerRecords, dataFormat, compatibility, gsrConfig)

        TimeUnit.SECONDS.sleep(KCL_SCHEDULER_SHUT_DOWN_WAIT_TIME_SECONDS.toLong())
        scheduler.shutdown()

        assertTrue(recordProcessor.creationSuccess)
        assertTrue(recordProcessor.consumptionSuccess)
        assertKinesisRecords(
            dataFormat,
            producerRecords.toTypedArray(),
            recordProcessor.consumedRecords.toTypedArray(),
        )

        LOGGER.info(
            "Finished test for producing/consuming messages on Kinesis Producer Library with Glue Schema Registry",
        )
    }

    // Used for Canary tests.
    @ParameterizedTest
    @MethodSource("testSingleKCLKPLDataProvider")
    fun testProduceConsumeSingleRecordWithKPLAndKCL(dataFormat: DataFormat) {
        val compatibility = Compatibility.NONE
        val recordType = AvroRecordType.GENERIC_RECORD
        val compression = AWSSchemaRegistryConstants.COMPRESSION.NONE

        val testDataGenerator =
            testDataGeneratorFactory.getInstance(
                TestDataGeneratorType.valueOf(dataFormat, recordType, compatibility),
            )
        val producerRecords = Collections.singletonList(testDataGenerator.createRecords()[0])

        val gsrConfig = getSchemaRegistryConfiguration(compatibility, compression, recordType, dataFormat)

        val recordProcessor = RecordProcessor()
        val scheduler = startConsumingWithKCL(gsrConfig, recordProcessor)

        produceRecordsWithKPL(streamName, producerRecords, dataFormat, compatibility, gsrConfig)

        TimeUnit.SECONDS.sleep(KCL_SCHEDULER_SHUT_DOWN_WAIT_TIME_SECONDS.toLong())
        scheduler.shutdown()

        assertTrue(recordProcessor.creationSuccess)
        assertTrue(recordProcessor.consumptionSuccess)
        assertEquals(producerRecords.size, recordProcessor.consumedRecords.size)
    }

    private fun produceRecordsWithKinesisSDK(
        streamName: String,
        producerRecords: List<*>,
        dataFormat: DataFormat,
        compatibility: Compatibility,
        gsrConfig: GlueSchemaRegistryConfiguration,
    ): String {
        val glueSchemaRegistrySerializer = GlueSchemaRegistrySerializerImpl(awsCredentialsProvider, gsrConfig)
        val dataFormatSerializer = glueSchemaRegistrySerializerFactory.getInstance(dataFormat, gsrConfig)

        var shardId: String? = null
        val timestamp = Instant.now()

        val schemaName = String.format("%s-%s-%s", streamName, dataFormat.name, compatibility)
        schemasToCleanUp.add(schemaName)

        for (i in producerRecords.indices) {
            val record = producerRecords[i]!!
            val gsrSchema = Schema(dataFormatSerializer.getSchemaDefinition(record), dataFormat.name, schemaName)

            val serializedBytes = dataFormatSerializer.serialize(record)

            val gsrEncodedBytes = glueSchemaRegistrySerializer.encode(streamName, gsrSchema, serializedBytes)

            val partitionKey = timestamp.toEpochMilli().toString() + "-" + i
            val putRecordRequest =
                PutRecordRequest
                    .builder()
                    .streamName(streamName)
                    .partitionKey(partitionKey)
                    .data(SdkBytes.fromByteArray(gsrEncodedBytes))
                    .build()
            shardId =
                kinesisClient
                    .putRecord(putRecordRequest)
                    .get()
                    .shardId()

            assertNotNull(shardId)
        }
        return shardId!!
    }

    private fun consumeRecordsWithKinesisSDK(
        streamName: String,
        shardId: String,
        dataFormat: DataFormat,
        gsrConfig: GlueSchemaRegistryConfiguration,
    ): List<Any?> {
        glueSchemaRegistryDeserializer = GlueSchemaRegistryDeserializerImpl(awsCredentialsProvider, gsrConfig)

        val gsrDataFormatDeserializer = glueSchemaRegistryDeserializerFactory.getInstance(dataFormat, gsrConfig)

        val getShardIteratorRequest =
            GetShardIteratorRequest
                .builder()
                .streamName(streamName)
                .shardId(shardId)
                .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                .build()

        val shardIterator =
            kinesisClient
                .getShardIterator(getShardIteratorRequest)
                .get()
                .shardIterator()

        val getRecordRequest =
            GetRecordsRequest
                .builder()
                .shardIterator(shardIterator)
                .build()
        val recordsResponse = kinesisClient.getRecords(getRecordRequest).get()

        val consumerRecords = ArrayList<Any?>()
        val recordsFromKinesis = recordsResponse.records()

        for (i in recordsFromKinesis.indices) {
            val consumedBytes =
                recordsFromKinesis[i]
                    .data()
                    .asByteArray()

            val gsrSchema = glueSchemaRegistryDeserializer.getSchema(consumedBytes)
            LOGGER.info("Consumed Schema from GSR : {}", gsrSchema.schemaDefinition)
            val decodedRecord = gsrDataFormatDeserializer.deserialize(ByteBuffer.wrap(consumedBytes), gsrSchema)
            consumerRecords.add(decodedRecord)
        }

        LOGGER.info("Decoded {} Records from Kinesis Stream {}", consumerRecords.size, streamName)

        return consumerRecords
    }

    private fun produceRecordsWithKPL(
        streamName: String,
        producerRecords: List<*>,
        dataFormat: DataFormat,
        compatibility: Compatibility,
        gsrConfig: GlueSchemaRegistryConfiguration,
    ): String? {
        val config =
            KinesisProducerConfiguration()
                .setRegion(REGION)
                .setKinesisEndpoint(LOCALSTACK_HOSTNAME)
                .setKinesisPort(LOCALSTACK_KINESIS_PORT.toLong())
                .setCloudwatchEndpoint(LOCALSTACK_HOSTNAME)
                .setCloudwatchPort(LOCALSTACK_CLOUDWATCH_PORT.toLong())
                // The native producer resolves the stream ARN through STS GetCallerIdentity.
                // Without these it asks the real AWS endpoint, which rejects the emulator's
                // dummy credentials and takes the child process down with it. The port is the
                // LocalStack edge port, the same one Kinesis is served from.
                .setStsEndpoint(LOCALSTACK_HOSTNAME)
                .setStsPort(LOCALSTACK_KINESIS_PORT.toLong())
                .setVerifyCertificate(false)
                .setAggregationEnabled(false)
                .setLogLevel(Level.ERROR.name().lowercase())
                .setGlueSchemaRegistryConfiguration(gsrConfig)

        val producer = KinesisProducer(config)

        val dataFormatSerializer = glueSchemaRegistrySerializerFactory.getInstance(dataFormat, gsrConfig)

        val putFutures: MutableList<Future<UserRecordResult>> = LinkedList()

        val timestamp = Instant.now()
        val schemaName = String.format("%s-%s-%s", streamName, dataFormat.name, compatibility)
        schemasToCleanUp.add(schemaName)
        val partitionKey = timestamp.toEpochMilli().toString()
        for (i in producerRecords.indices) {
            val record = producerRecords[i]!!
            val gsrSchema = Schema(dataFormatSerializer.getSchemaDefinition(record), dataFormat.name, schemaName)

            val serializedBytes = dataFormatSerializer.serialize(record)

            putFutures.add(
                producer.addUserRecord(
                    streamName,
                    partitionKey,
                    null,
                    ByteBuffer.wrap(serializedBytes),
                    gsrSchema,
                ),
            )
            producer.flushSync()
        }

        var shardId: String? = null

        for (future in putFutures) {
            val userRecordResult = future.get()
            shardId = userRecordResult.shardId

            assertTrue(userRecordResult.isSuccessful)
            assertNotNull(userRecordResult.shardId)
        }

        return shardId
    }

    private fun startConsumingWithKCL(
        gsrConfig: GlueSchemaRegistryConfiguration,
        recordProcessor: RecordProcessor,
    ): Scheduler {
        val glueSchemaRegistryRecordProcessorFactory =
            GlueSchemaRegistryRecordProcessorFactory(
                recordProcessor,
                glueSchemaRegistryDeserializerFactory,
                gsrConfig,
            )

        val configsBuilder =
            ConfigsBuilder(
                streamName,
                streamName,
                kinesisClient,
                dynamoClient,
                cloudWatchClient,
                streamName,
                glueSchemaRegistryRecordProcessorFactory,
            )
        val retrievalConfig =
            configsBuilder.retrievalConfig().retrievalSpecificConfig(PollingConfig(streamName, kinesisClient))

        val scheduler =
            Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig().metricsFactory(NullMetricsFactory()),
                configsBuilder.processorConfig(),
                retrievalConfig,
            )

        Thread(scheduler).start()

        TimeUnit.SECONDS.sleep(KCL_SCHEDULER_START_UP_WAIT_TIME_SECONDS.toLong())

        return scheduler
    }

    private fun assertKinesisRecords(
        dataFormat: DataFormat,
        expected: Array<out Any?>,
        actual: Array<out Any?>,
    ) {
        assertEquals(expected.size, actual.size)

        if (dataFormat == DataFormat.PROTOBUF) {
            val messageToBytes = { obj: Any? -> (obj as Message).toByteArray() }
            val expectedByteArray = expected.map(messageToBytes).toTypedArray()
            val actualBytesArray = actual.map(messageToBytes).toTypedArray()

            assertArrayEquals(expectedByteArray, actualBytesArray)
        } else {
            assertArrayEquals(expected, actual)
        }
    }

    companion object {
        private val LOGGER = LogManager.getLogger(GlueSchemaRegistryKinesisIntegrationTest::class.java)
        private const val LOCALSTACK_HOSTNAME = "localhost"
        private const val LOCALSTACK_KINESIS_PORT = 4566
        private val LOCALSTACK_ENDPOINT = String.format("http://%s:%d", LOCALSTACK_HOSTNAME, LOCALSTACK_KINESIS_PORT)
        private val LOCALSTACK_CLOUDWATCH_PORT = Constants.DEFAULT_PORTS[ServiceName.CLOUDWATCH]!!
        private const val KCL_SCHEDULER_START_UP_WAIT_TIME_SECONDS = 15
        private const val KCL_SCHEDULER_SHUT_DOWN_WAIT_TIME_SECONDS = 5

        private val SCHEMA_REGISTRY_ENDPOINT_OVERRIDE = GlueSchemaRegistryConnectionProperties.ENDPOINT
        private val REGION = GlueSchemaRegistryConnectionProperties.REGION
        private const val TEST_KINESIS_STREAM_PREFIX = "gsr-integ-test-kinesis-stream-"

        // The class named by the specific-record JSON generator's schema, allowlisted so that
        // JSON + SPECIFIC_RECORD deserializes back into the POJO rather than a JsonDataWithSchema.
        private const val JSON_SPECIFIC_RECORD_CLASS_NAME =
            "com.amazonaws.services.schemaregistry.integrationtests.generators.Car"

        // Testing with single shard - can be increased but will require code changes to iterate
        // from multiple shard Ids
        private const val SHARD_COUNT = 1
        private val RECORD_TYPES: List<AvroRecordType> =
            AvroRecordType.entries.filter { it != AvroRecordType.UNKNOWN }
        private val COMPATIBILITIES: List<Compatibility> =
            Compatibility.knownValues().filter { it.toString() == "NONE" }
        private lateinit var kinesisClient: KinesisAsyncClient
        private lateinit var streamName: String
        private lateinit var glueSchemaRegistryDeserializer: GlueSchemaRegistryDeserializer
        private val awsCredentialsProvider = DefaultCredentialsProvider.builder().build()

        private val dynamoClient: DynamoDbAsyncClient =
            DynamoDbAsyncClient
                .builder()
                .endpointOverride(URI(LOCALSTACK_ENDPOINT))
                .region(Region.of(GlueSchemaRegistryConnectionProperties.REGION))
                .credentialsProvider(awsCredentialsProvider)
                .build()

        private val cloudWatchClient: CloudWatchAsyncClient =
            CloudWatchAsyncClient
                .builder()
                .endpointOverride(URI(LOCALSTACK_ENDPOINT))
                .region(Region.of(GlueSchemaRegistryConnectionProperties.REGION))
                .credentialsProvider(awsCredentialsProvider)
                .build()

        private val schemasToCleanUp = ArrayList<String>()

        private fun getMetadata(): Map<String, String> {
            val metadata = HashMap<String, String>()
            metadata["event-source-1"] = "stream1"
            metadata["event-source-2"] = "stream2"
            return metadata
        }

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

        private fun getSchemaRegistryConfiguration(
            compatibility: Compatibility,
            compression: AWSSchemaRegistryConstants.COMPRESSION,
            avroRecordType: AvroRecordType,
            dataFormat: DataFormat,
        ): GlueSchemaRegistryConfiguration {
            val configs = GlueSchemaRegistryConfiguration(REGION)
            configs.endPoint = SCHEMA_REGISTRY_ENDPOINT_OVERRIDE
            configs.isSchemaAutoRegistrationEnabled = true
            configs.metadata = getMetadata()
            if (dataFormat == DataFormat.PROTOBUF) {
                configs.protobufMessageType = ProtobufMessageType.DYNAMIC_MESSAGE
            } else {
                configs.avroRecordType = avroRecordType
            }
            // As of 2.0.0 the JSON deserializer only resolves a schema's "className" into a POJO when
            // resolution is opted into and the class is allowlisted. Only the specific-record JSON
            // generator emits a schema carrying a className, so opt in for that combination alone.
            if (dataFormat == DataFormat.JSON && AvroRecordType.SPECIFIC_RECORD == avroRecordType) {
                configs.isJsonClassNameResolutionEnabled = true
                configs.jsonClassNameAllowlist = Collections.singleton(JSON_SPECIFIC_RECORD_CLASS_NAME)
            }
            configs.compatibilitySetting = compatibility
            configs.compressionType = compression
            return configs
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            LOGGER.info("Starting Clean-up of schemas created with GSR.")
            val glueClient =
                GlueClient
                    .builder()
                    .credentialsProvider(awsCredentialsProvider)
                    .region(Region.of(REGION))
                    .endpointOverride(URI(SCHEMA_REGISTRY_ENDPOINT_OVERRIDE))
                    .httpClient(UrlConnectionHttpClient.builder().build())
                    .build()

            for (schemaName in schemasToCleanUp) {
                LOGGER.info("Cleaning up schema {}..", schemaName)
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

                glueClient.deleteSchema(deleteSchemaRequest)
            }

            LOGGER.info("Finished Cleaning up {} schemas created with GSR.", schemasToCleanUp.size)
        }

        @JvmStatic
        fun testSingleKCLKPLDataProvider(): Stream<Arguments> = DataFormat.knownValues().stream().map { Arguments.of(it) }
    }
}
