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

package com.amazonaws.services.schemaregistry.examples.kds

import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.model.DescribeStreamRequest
import com.amazonaws.services.kinesis.model.DescribeStreamResult
import com.amazonaws.services.kinesis.model.GetRecordsRequest
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest
import com.amazonaws.services.kinesis.model.PutRecordsRequest
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry
import com.amazonaws.services.kinesis.model.Shard
import com.amazonaws.services.schemaregistry.common.Schema
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializer
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializer
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl
import com.amazonaws.services.schemaregistry.utils.AVROUtils
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.commons.cli.BasicParser
import org.apache.commons.cli.Options
import org.joda.time.DateTime
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.services.glue.model.DataFormat
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.IOException
import java.io.UncheckedIOException
import java.nio.ByteBuffer
import java.util.Date
import java.util.logging.Logger

/**
 * This is an example of how to use Glue Schema Registry (GSR) with Kinesis Data Streams Get / Put Record APIs.
 * This code is **not** applicable if you are using KCL / KPL libraries.
 * GSR is already available in KCL / KPL libraries. See, https://docs.aws.amazon.com/glue/latest/dg/schema-registry-integrations.html#schema-registry-integrations-kds
 */
object PutRecordGetRecordExample {
    private const val AVRO_USER_SCHEMA_FILE = "src/main/resources/user.avsc"
    private val LOGGER: Logger = Logger.getLogger(PutRecordGetRecordExample::class.java.simpleName)
    private val awsCredentialsProvider: AwsCredentialsProvider =
        DefaultCredentialsProvider
            .builder()
            .build()

    private lateinit var kinesisClient: AmazonKinesis
    private lateinit var glueSchemaRegistrySerializer: GlueSchemaRegistrySerializer
    private lateinit var glueSchemaRegistryDeserializer: GlueSchemaRegistryDeserializer

    @JvmStatic
    @Throws(Exception::class)
    fun main(args: Array<String>) {
        val options = Options()
        options.addOption("region", true, "Specify region")
        options.addOption("stream", true, "Specify stream")
        options.addOption("schema", true, "Specify schema")
        options.addOption("numRecords", true, "Specify number of records")
        val parser = BasicParser()
        val cmd = parser.parse(options, args)

        require(cmd.hasOption("stream")) { "Stream name needs to be provided." }
        val regionName = cmd.getOptionValue("region", "us-west-2")
        val schemaName = cmd.getOptionValue("schema", "testSchema")
        val streamName = cmd.getOptionValue("stream")
        val numOfRecords = cmd.getOptionValue("numRecords", "10").toInt()

        // Kinesis data streams client initialization.
        kinesisClient = AmazonKinesisClientBuilder.standard().withRegion(regionName).build()

        // Glue Schema Registry serializer initialization for the producer.
        glueSchemaRegistrySerializer =
            GlueSchemaRegistrySerializerImpl(
                awsCredentialsProvider,
                getSchemaRegistryConfiguration(regionName),
            )

        // Glue Schema Registry de-serializer initialization for the consumer.
        glueSchemaRegistryDeserializer =
            GlueSchemaRegistryDeserializerImpl(awsCredentialsProvider, getSchemaRegistryConfiguration(regionName))

        // Define the Glue Schema Registry schema object that will be used to encode data.
        val gsrSchema = Schema(getAvroSchema().toString(), DataFormat.AVRO.name, schemaName)

        LOGGER.info("Client initialization complete.")

        val timestamp = DateTime.now().toDate()

        // Put records into Kinesis stream.
        putRecordsWithSchema(streamName, numOfRecords, gsrSchema, timestamp)

        // Start receiving records from the stream.
        getRecordsWithSchema(streamName, timestamp)
    }

    @Throws(IOException::class)
    private fun getRecordsWithSchema(
        streamName: String,
        timestamp: Date,
    ) {
        // Standard Kinesis code to getRecords from a Kinesis Data Stream.
        val describeStreamRequest = DescribeStreamRequest()
        describeStreamRequest.streamName = streamName
        val shards: MutableList<Shard> = ArrayList()

        var streamRes: DescribeStreamResult
        do {
            streamRes = kinesisClient.describeStream(describeStreamRequest)
            shards.addAll(streamRes.streamDescription.shards)

            if (shards.size > 0) {
                shards[shards.size - 1].shardId
            }
        } while (streamRes.streamDescription.hasMoreShards)

        val itReq = GetShardIteratorRequest()
        itReq.streamName = streamName
        itReq.shardId = shards[0].shardId
        itReq.timestamp = timestamp
        itReq.shardIteratorType = "AT_TIMESTAMP"

        val shardIteratorResult = kinesisClient.getShardIterator(itReq)
        val shardIterator: String = shardIteratorResult.shardIterator

        // Create new GetRecordsRequest with existing shardIterator.
        val recordsRequest = GetRecordsRequest()
        recordsRequest.shardIterator = shardIterator
        recordsRequest.limit = 1000

        val result = kinesisClient.getRecords(recordsRequest)

        for (record in result.records) {
            val recordAsByteBuffer = record.data
            val decodedRecord = decodeRecord(recordAsByteBuffer)
            LOGGER.info("Decoded Record: $decodedRecord")
        }
    }

    private fun putRecordsWithSchema(
        streamName: String,
        numOfRecords: Int,
        gsrSchema: Schema,
        timestamp: Date,
    ) {
        // Standard Kinesis code to putRecords into a Kinesis Data Stream.
        val putRecordsRequest = PutRecordsRequest()
        putRecordsRequest.streamName = streamName

        val recordsRequestEntries: MutableList<PutRecordsRequestEntry> = ArrayList()

        LOGGER.info("Putting $numOfRecords into $streamName with schema$gsrSchema")
        for (i in 0 until numOfRecords) {
            val record = getTestRecord(i) as GenericRecord
            val recordWithSchema = encodeRecord(record, streamName, gsrSchema)
            val entry = PutRecordsRequestEntry()
            entry.data = ByteBuffer.wrap(recordWithSchema)
            entry.partitionKey =
                timestamp
                    .toInstant()
                    .toEpochMilli()
                    .toString()

            recordsRequestEntries.add(entry)
        }

        putRecordsRequest.setRecords(recordsRequestEntries)

        val putRecordResult = kinesisClient.putRecords(putRecordsRequest)

        LOGGER.info("Successfully put records: $putRecordResult")
    }

    private fun encodeRecord(
        record: GenericRecord,
        streamName: String,
        gsrSchema: Schema,
    ): ByteArray {
        val recordAsBytes = convertRecordToBytes(record)
        // Pass the GSR Schema and record payload to glueSchemaRegistrySerializer.encode method.
        return glueSchemaRegistrySerializer.encode(streamName, gsrSchema, recordAsBytes)
    }

    @Throws(IOException::class)
    private fun decodeRecord(recordByteBuffer: ByteBuffer): GenericRecord? {
        // Copy the data to a mutable buffer.
        val recordWithSchemaHeaderBytes = ByteArray(recordByteBuffer.remaining())
        recordByteBuffer.get(recordWithSchemaHeaderBytes, 0, recordWithSchemaHeaderBytes.size)

        // Passing the buffer to glueSchemaRegistryDeserializer.getSchema to extract schema object.
        val awsSchema = glueSchemaRegistryDeserializer.getSchema(recordWithSchemaHeaderBytes)

        // Passing the buffer to glueSchemaRegistryDeserializer.getData to extract the actual message payload.
        val data = glueSchemaRegistryDeserializer.getData(recordWithSchemaHeaderBytes)

        var genericRecord: GenericRecord? = null
        // Convert the decoded payload and schema to Avro object.
        if (DataFormat.AVRO.name == awsSchema.dataFormat) {
            val avroSchema = org.apache.avro.Schema.Parser().parse(awsSchema.schemaDefinition)
            genericRecord = convertBytesToRecord(avroSchema, data)
        }
        return genericRecord
    }

    private fun getAvroSchema(): org.apache.avro.Schema = try {
        org.apache.avro.Schema.Parser().parse(File(AVRO_USER_SCHEMA_FILE))
    } catch (e: IOException) {
        LOGGER.warning("Error parsing Avro schema from file" + e.message)
        throw UncheckedIOException(e)
    }

    private fun convertRecordToBytes(record: Any): ByteArray {
        // Standard Avro code to convert records into bytes.
        val recordAsBytes = ByteArrayOutputStream()
        val encoder = EncoderFactory.get().directBinaryEncoder(recordAsBytes, null)
        val datumWriter = GenericDatumWriter<Any>(AVROUtils.getInstance().getSchema(record))
        try {
            datumWriter.write(record, encoder)
            encoder.flush()
        } catch (e: IOException) {
            LOGGER.warning("Failed to convert record to Bytes" + e.message)
            throw UncheckedIOException(e)
        }
        return recordAsBytes.toByteArray()
    }

    private fun convertBytesToRecord(
        avroSchema: org.apache.avro.Schema,
        record: ByteArray,
    ): GenericRecord? {
        // Standard Avro code to convert bytes to records.
        val datumReader = GenericDatumReader<GenericRecord>(avroSchema)
        val decoder = DecoderFactory.get().binaryDecoder(record, null)
        val genericRecord: GenericRecord?
        try {
            genericRecord = datumReader.read(null, decoder)
        } catch (e: IOException) {
            LOGGER.warning("Failed to convert bytes to record" + e.message)
            throw UncheckedIOException(e)
        }
        return genericRecord
    }

    private fun getMetadata(): Map<String, String> {
        // Metadata is optionally used by GSR while auto-registering a new schema version.
        val metadata: MutableMap<String, String> = HashMap()
        metadata["event-source-1"] = "topic1"
        metadata["event-source-2"] = "topic2"
        metadata["event-source-3"] = "topic3"
        return metadata
    }

    private fun getSchemaRegistryConfiguration(regionName: String): GlueSchemaRegistryConfiguration {
        val configs = GlueSchemaRegistryConfiguration(regionName)
        // Optional setting to enable auto-registration.
        configs.isSchemaAutoRegistrationEnabled = true
        // Optional setting to define metadata for the schema version while auto-registering.
        configs.metadata = getMetadata()
        return configs
    }

    private fun getTestRecord(i: Int): Any {
        // Creating some sample Avro records.
        val genericRecord: GenericRecord = GenericData.Record(getAvroSchema())
        genericRecord.put("name", "testName$i")
        genericRecord.put("favorite_number", i)
        genericRecord.put("favorite_color", "color$i")

        return genericRecord
    }
}
