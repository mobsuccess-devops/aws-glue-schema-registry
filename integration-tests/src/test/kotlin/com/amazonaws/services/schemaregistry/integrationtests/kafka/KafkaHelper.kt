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

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer
import com.amazonaws.services.schemaregistry.integrationtests.generators.AvroGenericBackwardCompatDataGenerator
import com.amazonaws.services.schemaregistry.integrationtests.generators.JsonSchemaGenericBackwardCompatDataGenerator
import com.amazonaws.services.schemaregistry.integrationtests.generators.ProtobufGenericBackwardDataGenerator
import com.amazonaws.services.schemaregistry.integrationtests.properties.GlueSchemaRegistryConnectionProperties
import com.amazonaws.services.schemaregistry.kafkastreams.GlueSchemaRegistryKafkaStreamsSerde
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.google.protobuf.Message
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.glue.model.DataFormat
import java.time.Duration
import java.util.Collections
import java.util.Properties
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException

class KafkaHelper(
    private val bootstrapBrokers: String,
    private val zookeeperConnect: String,
    private val clusterArn: String,
) {
    /**
     * Helper function to create test topic
     *
     * @param topic             topic name to be created
     * @param numPartitions     number of numPartitions
     * @param replicationFactor replicationFactor count
     */
    fun createTopic(
        topic: String,
        numPartitions: Int,
        replicationFactor: Short,
    ) {
        val properties = Properties()
        properties["bootstrap.servers"] = bootstrapBrokers
        properties["client.id"] = "gsr-integration-tests"

        log.info("Creating Kafka topic {} with bootstrap {}...", topic, bootstrapBrokers)
        try {
            AdminClient.create(properties).use { kafkaAdminClient ->
                val newTopic = NewTopic(topic, numPartitions, replicationFactor)
                val createTopicsResult = kafkaAdminClient.createTopics(Collections.singleton(newTopic))
                createTopicsResult.values()[topic]!!.get()
            }
        } catch (e: Exception) {
            e.printStackTrace()
            throw e
        }
    }

    /**
     * Helper function to test producer can send messages
     *
     * @param topic      topic to send messages to
     * @param numRecords number of records to be sent
     */
    fun doProduce(
        topic: String,
        numRecords: Int,
    ) {
        log.info("Start producing to cluster {} with bootstrap {}...", clusterArn, bootstrapBrokers)

        val properties = getKafkaProducerProperties()
        properties["key.serializer"] = StringSerializer::class.java.name
        properties["value.serializer"] = StringSerializer::class.java.name

        KafkaProducer<String, String>(properties).use { producer ->
            for (i in 0 until numRecords) {
                log.info("Producing record $i")
                producer.send(ProducerRecord(topic, i.toString(), i.toString())).get()
            }
        }

        log.info("Finishing producing messages via Kafka.")
    }

    /**
     * Helper method to test consumption of records
     *
     * @param consumerProperties consumerProperties
     */
    fun doConsume(consumerProperties: ConsumerProperties): Int {
        val properties = getKafkaConsumerProperties(consumerProperties)
        properties["key.deserializer"] = StringDeserializer::class.java.name
        properties["value.deserializer"] = StringDeserializer::class.java.name
        val consumer = KafkaConsumer<String, String>(properties)
        return consumeRecords(consumer, consumerProperties.topicName).size
    }

    /**
     * Helper function to produce test AVRO records for Streams
     *
     * @param producerProperties producerProperties
     */
    fun doProduceAvroRecordsSerde(
        producerProperties: ProducerProperties,
        records: List<*>,
    ): List<ProducerRecord<String, Any>> {
        val properties = getProducerProperties(producerProperties)
        return KafkaProducer<String, Any>(
            properties,
            StringSerializer(),
            GlueSchemaRegistryKafkaSerializer(getMapFromPropertiesFile(properties)),
        ).use { producer ->
            produceRecords(producer, producerProperties, records)
        }
    }

    /**
     * Helper function to consume test AVRO records for Streams
     *
     * @param consumerProperties consumerProperties
     */
    fun doConsumeAvroRecordsSerde(consumerProperties: ConsumerProperties): List<ConsumerRecord<String, Any>> {
        val properties = getConsumerProperties(consumerProperties)
        val consumer =
            KafkaConsumer(
                properties,
                StringDeserializer(),
                GlueSchemaRegistryKafkaDeserializer(getMapFromPropertiesFile(properties)),
            )
        return consumeRecords(consumer, consumerProperties.topicName)
    }

    /**
     * Helper function to produce test AVRO records
     *
     * @param producerProperties producer properties
     * @return list of produced records
     */
    fun <T> doProduceRecords(
        producerProperties: ProducerProperties,
        records: List<*>,
    ): List<ProducerRecord<String, T>> {
        val properties = getProducerProperties(producerProperties)
        properties["key.serializer"] = StringSerializer::class.java.name
        properties["value.serializer"] = GlueSchemaRegistryKafkaSerializer::class.java.name

        return KafkaProducer<String, T>(properties).use { producer ->
            produceRecords(producer, producerProperties, records)
        }
    }

    /**
     * Helper function to test consumption of records
     */
    fun <T> doConsumeRecords(consumerProperties: ConsumerProperties): List<ConsumerRecord<String, T>> {
        val properties = getConsumerProperties(consumerProperties)
        properties["key.deserializer"] = StringDeserializer::class.java.name
        properties["value.deserializer"] = GlueSchemaRegistryKafkaDeserializer::class.java.name
        val consumer = KafkaConsumer<String, T>(properties)
        return consumeRecords(consumer, consumerProperties.topicName)
    }

    /**
     * Helper function to process Kafka Streams
     *
     * @param producerProperties producerProperties
     */
    fun doKafkaStreamsProcess(producerProperties: ProducerProperties) {
        log.info(
            "Start processing {} message streaming from cluster {} with bootstrap {}...",
            producerProperties.dataFormat,
            clusterArn,
            bootstrapBrokers,
        )

        val properties = getKafkaStreamsProperties(producerProperties)
        setSchemaRegistrySerializerProperties(properties, producerProperties)

        val builder = StreamsBuilder()
        val source: KStream<String, Any> = builder.stream(producerProperties.inputTopic)

        // Filter records whose value match to criteria of the records sent by the producer.
        val result: KStream<String, Any> =
            when (DataFormat.fromValue(producerProperties.dataFormat)) {
                DataFormat.AVRO ->
                    source.filter { _, value ->
                        AvroGenericBackwardCompatDataGenerator.filterRecords(value as GenericRecord)
                    }
                DataFormat.JSON ->
                    source.filter { _, value ->
                        JsonSchemaGenericBackwardCompatDataGenerator.filterRecords(value as JsonDataWithSchema)
                    }
                DataFormat.PROTOBUF ->
                    source.filter { _, value ->
                        ProtobufGenericBackwardDataGenerator.filterRecords(value as Message)
                    }
                else -> throw RuntimeException("Data format is not supported")
            }
        result.to(producerProperties.outputTopic)

        val streams = KafkaStreams(builder.build(), properties)
        streams.cleanUp()
        streams.start()
        Thread.sleep(5000L)
        streams.close()

        log.info("Finish processing {} message streaming via Kafka.", producerProperties.dataFormat)
    }

    private fun <T> produceRecords(
        producer: Producer<String, T>,
        producerProperties: ProducerProperties,
        records: List<*>,
    ): List<ProducerRecord<String, T>> {
        log.info("Start producing to cluster {} with bootstrap {}...", clusterArn, bootstrapBrokers)
        val producerRecords = ArrayList<ProducerRecord<String, T>>()

        for (i in records.indices) {
            @Suppress("UNCHECKED_CAST")
            val record = records[i] as T
            log.info("Fetching record {} for Kafka: {}", i, record)

            // Verify and use a unique field present in the schema as a key for the producer record.
            val producerRecord = ProducerRecord(producerProperties.topicName, "message-$i", record)

            producerRecords.add(producerRecord)
            producer.send(producerRecord)
            Thread.sleep(500)
            log.info("Sent {} message {}", producerProperties.dataFormat, i)
        }
        producer.flush()
        log.info(
            "Successfully produced {} messages to a topic called {}",
            records.size,
            producerProperties.topicName,
        )
        return producerRecords
    }

    private fun <T> consumeRecords(
        consumer: KafkaConsumer<String, T>,
        topic: String?,
    ): List<ConsumerRecord<String, T>> {
        log.info("Start consuming from cluster {} with bootstrap {} ...", clusterArn, bootstrapBrokers)

        consumer.subscribe(Collections.singleton(topic))
        val consumerRecords = ArrayList<ConsumerRecord<String, T>>()
        val now = System.currentTimeMillis()
        while (System.currentTimeMillis() - now < CONSUMER_RUNTIME.toMillis()) {
            @Suppress("DEPRECATION")
            val recordsReceived = consumer.poll(CONSUMER_RUNTIME.toMillis())
            var i = 0
            for (record in recordsReceived) {
                val key = record.key()
                val value = record.value()
                log.info("Received message {}: key = {}, value = {}", i, key, value)
                consumerRecords.add(record)
                i++
            }
        }

        consumer.close()
        log.info("Finished consuming messages via Kafka.")
        return consumerRecords
    }

    /**
     * Helper function to produce test AVRO records in multithreaded manner
     *
     * @param producerProperties producerProperties
     */
    fun <T> doProduceRecordsMultithreaded(
        producerProperties: ProducerProperties,
        records: List<*>,
    ): List<ProducerRecord<String, T>> {
        val properties = getProducerProperties(producerProperties)
        properties["key.serializer"] = StringSerializer::class.java.name
        properties["value.serializer"] = GlueSchemaRegistryKafkaSerializer::class.java.name

        val numberOfThreads = 4
        val futures = ArrayList<CompletableFuture<Void>>()
        // Every thread below appends to this list, so it cannot be a bare ArrayList: the lost
        // updates made the returned count smaller than what was actually produced, and it is
        // that count the test compares against the records it reads back.
        val producerRecords: MutableList<ProducerRecord<String, T>> =
            Collections.synchronizedList(ArrayList())

        for (i in 0 until numberOfThreads) {
            futures.add(
                CompletableFuture.runAsync {
                    try {
                        KafkaProducer<String, T>(properties).use { producer ->
                            producerRecords.addAll(produceRecords(producer, producerProperties, records))
                        }
                    } catch (e: Exception) {
                        throw CompletionException(e)
                    }
                },
            )
        }

        val future = CompletableFuture.allOf(*futures.toTypedArray())

        future.get()
        return producerRecords
    }

    private fun getProducerProperties(producerProperties: ProducerProperties): Properties {
        val properties = getKafkaProducerProperties()

        // Add Kafka's native compression if specified
        val kafkaCompressionType = producerProperties.kafkaCompressionType
        if (!kafkaCompressionType.isNullOrEmpty()) {
            properties["compression.type"] = kafkaCompressionType
            log.info("Setting Kafka compression.type to: {}", kafkaCompressionType)
        }

        setSchemaRegistrySerializerProperties(properties, producerProperties)
        return properties
    }

    private fun getKafkaProducerProperties(): Properties {
        val properties = Properties()
        properties["bootstrap.servers"] = bootstrapBrokers
        properties["acks"] = "all"
        properties["retries"] = 0
        properties["batch.size"] = 16384
        properties["linger.ms"] = 1
        properties["buffer.memory"] = 33554432
        properties["block.on.buffer.full"] = false
        properties["request.timeout.ms"] = "1000"
        return properties
    }

    private fun getConsumerProperties(consumerProperties: ConsumerProperties): Properties = getKafkaConsumerProperties(consumerProperties)

    private fun getKafkaConsumerProperties(consumerProperties: ConsumerProperties): Properties {
        val properties = Properties()
        properties["bootstrap.servers"] = bootstrapBrokers
        properties["group.id"] = UUID.randomUUID().toString()
        properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        properties[AWSSchemaRegistryConstants.AWS_ENDPOINT] = GlueSchemaRegistryConnectionProperties.ENDPOINT
        properties[AWSSchemaRegistryConstants.AWS_REGION] = GlueSchemaRegistryConnectionProperties.REGION
        consumerProperties.avroRecordType?.let {
            properties[AWSSchemaRegistryConstants.AVRO_RECORD_TYPE] = it
        }
        consumerProperties.protobufMessageType?.let {
            properties[AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE] = it
        }
        consumerProperties.jsonClassNameResolutionEnabled?.let {
            properties[AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED] = it
        }
        consumerProperties.jsonClassNameAllowlist?.let {
            properties[AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST] = it
        }
        return properties
    }

    private fun getKafkaStreamsProperties(producerProperties: ProducerProperties): Properties {
        val properties = Properties()
        properties[StreamsConfig.APPLICATION_ID_CONFIG] = "kafka-streams-test-" + producerProperties.dataFormat
        properties[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapBrokers
        properties[StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG] = 0
        properties[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
        properties[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = GlueSchemaRegistryKafkaStreamsSerde::class.java
        properties[AWSSchemaRegistryConstants.DATA_FORMAT] = producerProperties.dataFormat
        val recordType = producerProperties.recordType
        if (recordType != null) {
            if (DataFormat.PROTOBUF.name == producerProperties.dataFormat) {
                properties[AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE] = recordType
            } else {
                properties[AWSSchemaRegistryConstants.AVRO_RECORD_TYPE] = recordType
            }
        }
        properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        return properties
    }

    private fun setSchemaRegistrySerializerProperties(
        properties: Properties,
        producerProperties: ProducerProperties,
    ) {
        properties[AWSSchemaRegistryConstants.AWS_ENDPOINT] = GlueSchemaRegistryConnectionProperties.ENDPOINT
        properties[AWSSchemaRegistryConstants.AWS_REGION] = GlueSchemaRegistryConnectionProperties.REGION
        properties[AWSSchemaRegistryConstants.SCHEMA_NAME] = producerProperties.schemaName
        properties[AWSSchemaRegistryConstants.DATA_FORMAT] = producerProperties.dataFormat
        properties[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = producerProperties.compressionType
        properties[AWSSchemaRegistryConstants.COMPATIBILITY_SETTING] = producerProperties.compatibilityType
        properties[AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING] =
            producerProperties.autoRegistrationEnabled
    }

    /**
     * Create Config map from the properties Object passed.
     *
     * @param properties properties of configuration elements.
     * @return map of configs.
     */
    private fun getMapFromPropertiesFile(properties: Properties): Map<String, *> = HashMap(properties.entries.associate { it.key.toString() to it.value })

    companion object {
        private val log = LoggerFactory.getLogger(KafkaHelper::class.java)
        private val CONSUMER_RUNTIME: Duration = Duration.ofMillis(10000)
    }
}
