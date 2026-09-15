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
package com.amazonaws.services.schemaregistry.integrationtests.compat

import com.amazonaws.services.schemaregistry.common.Schema
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl
import com.amazonaws.services.schemaregistry.integrationtests.generators.TestDataGeneratorFactory
import com.amazonaws.services.schemaregistry.integrationtests.generators.TestDataGeneratorType
import com.amazonaws.services.schemaregistry.integrationtests.properties.GlueSchemaRegistryConnectionProperties
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerFactory
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.AvroRecordType
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.glue.GlueClient
import software.amazon.awssdk.services.glue.model.Compatibility
import software.amazon.awssdk.services.glue.model.DataFormat
import software.amazon.awssdk.services.glue.model.DeleteSchemaRequest
import software.amazon.awssdk.services.glue.model.EntityNotFoundException
import software.amazon.awssdk.services.glue.model.SchemaId
import java.net.URI
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * Backward-compatibility integration test for the injectable HTTP client.
 *
 * The change lets a caller inject a custom `SdkHttpClient.Builder` — Apache, say — instead of the
 * hard-coded `UrlConnectionHttpClient`. The concern it addresses is whether a consumer of a
 * released version can still read data produced by a build carrying the change, and the other way
 * round, for every supported data format.
 *
 * The change only affects how the internal Glue client's HTTP transport is built; it does not
 * touch the serialized wire format. This test proves that against a real registry for **AVRO,
 * JSON and PROTOBUF**: for each format it serializes the same record twice, once on the default
 * HTTP client and once on an injected Apache client, and asserts that both the schema and the data
 * are encoded identically. Because the default path is unchanged from the released version,
 * byte-identical output means anything a released consumer could read before it can still read
 * now, whichever HTTP client the producer used. Round trips through a default and an
 * Apache-injected deserializer confirm the schema and the data decode back to the original in
 * every producer/consumer combination.
 *
 * [injectedClientBuildFailure_surfacesError] is the negative case: an injected client pointed at
 * an unreachable endpoint must surface an error rather than silently succeeding.
 *
 * Needs a Glue-compatible endpoint, resolved the way every integration test in this module
 * resolves one — through [GlueSchemaRegistryConnectionProperties]. Schemas created here are
 * deleted in [cleanUpSchemas].
 */
class CrossClientWireCompatIntegrationTest {
    private val credentials = DefaultCredentialsProvider.builder().build()
    private val serializerFactory = GlueSchemaRegistrySerializerFactory()
    private val testDataGeneratorFactory = TestDataGeneratorFactory()

    private fun baseConfigs(): Map<String, Any> = mapOf(
        AWSSchemaRegistryConstants.AWS_REGION to REGION,
        AWSSchemaRegistryConstants.AWS_ENDPOINT to ENDPOINT,
        AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING to "true",
        AWSSchemaRegistryConstants.REGISTRY_NAME to REGISTRY_NAME,
    )

    private fun defaultConfig() = GlueSchemaRegistryConfiguration(baseConfigs())

    private fun apacheConfig() = GlueSchemaRegistryConfiguration(baseConfigs()).apply {
        httpClientBuilder = ApacheHttpClient.builder()
    }

    /**
     * For each supported format: the default HTTP client and an injected Apache HTTP client must
     * produce byte-identical schema and data, and every producer/consumer client combination must
     * round-trip the schema and the data back to the original.
     */
    @ParameterizedTest(name = "{0}")
    @EnumSource(value = DataFormat::class, names = ["AVRO", "JSON", "PROTOBUF"])
    fun injectedApacheClient_producesIdenticalSchemaAndData_andRoundTrips(dataFormat: DataFormat) {
        val generatorType =
            TestDataGeneratorType.valueOf(dataFormat, AvroRecordType.GENERIC_RECORD, Compatibility.NONE)
        val record = testDataGeneratorFactory.getInstance(generatorType).createRecords().first()!!

        val schemaName = "xver-wire-compat-${dataFormat.name}-${UUID.randomUUID()}"
        SCHEMAS_TO_CLEAN_UP.add(schemaName)

        val formatSerializer = serializerFactory.getInstance(dataFormat, defaultConfig())
        val schemaDefinition = formatSerializer.getSchemaDefinition(record)
        val payload = formatSerializer.serialize(record)
        val schema = Schema(schemaDefinition, dataFormat.name, schemaName)

        val defaultSerializer = GlueSchemaRegistrySerializerImpl(credentials, defaultConfig())
        val apacheSerializer = GlueSchemaRegistrySerializerImpl(credentials, apacheConfig())

        val encodedByDefault = defaultSerializer.encode(TRANSPORT_NAME, schema, payload)
        val encodedByApache = apacheSerializer.encode(TRANSPORT_NAME, schema, payload)

        assertArrayEquals(
            encodedByDefault,
            encodedByApache,
            "$dataFormat: injecting an Apache HTTP client must not change the serialized wire bytes",
        )

        val defaultDeserializer = GlueSchemaRegistryDeserializerImpl(credentials, defaultConfig())
        val apacheDeserializer = GlueSchemaRegistryDeserializerImpl(credentials, apacheConfig())

        assertArrayEquals(
            payload,
            defaultDeserializer.getData(encodedByDefault),
            "$dataFormat: default consumer must read default-produced data",
        )
        assertArrayEquals(
            payload,
            defaultDeserializer.getData(encodedByApache),
            "$dataFormat: default consumer must read Apache-produced data",
        )
        assertArrayEquals(
            payload,
            apacheDeserializer.getData(encodedByDefault),
            "$dataFormat: Apache consumer must read default-produced data",
        )
        assertArrayEquals(
            payload,
            apacheDeserializer.getData(encodedByApache),
            "$dataFormat: Apache consumer must read Apache-produced data",
        )

        val schemaFromDefault = defaultDeserializer.getSchema(encodedByDefault).schemaDefinition
        val schemaFromApache = apacheDeserializer.getSchema(encodedByApache).schemaDefinition
        assertEquals(
            schemaDefinition,
            schemaFromDefault,
            "$dataFormat: schema resolved from default-produced data must match the registered definition",
        )
        assertEquals(
            schemaDefinition,
            schemaFromApache,
            "$dataFormat: schema resolved from Apache-produced data must match the registered definition",
        )
        assertEquals(
            schemaFromDefault,
            schemaFromApache,
            "$dataFormat: the resolved schema must be identical across default and injected clients",
        )
    }

    /**
     * Negative case: an injected HTTP client that cannot reach Glue — an unresolvable endpoint —
     * must surface an error on use rather than silently succeeding. This confirms a failure of the
     * injected transport propagates to the caller.
     */
    @Test
    fun injectedClientBuildFailure_surfacesError() {
        val dataFormat = DataFormat.AVRO
        val generatorType =
            TestDataGeneratorType.valueOf(dataFormat, AvroRecordType.GENERIC_RECORD, Compatibility.NONE)
        val record = testDataGeneratorFactory.getInstance(generatorType).createRecords().first()!!

        val badEndpointConfig =
            GlueSchemaRegistryConfiguration(baseConfigs()).apply {
                httpClientBuilder = ApacheHttpClient.builder().connectionTimeout(Duration.ofSeconds(2))
                endPoint = "https://glue.this-endpoint-does-not-exist.aws.invalid"
            }

        val formatSerializer = serializerFactory.getInstance(dataFormat, defaultConfig())
        val schema =
            Schema(
                formatSerializer.getSchemaDefinition(record),
                dataFormat.name,
                "xver-wire-compat-negative-${UUID.randomUUID()}",
            )
        val payload = formatSerializer.serialize(record)

        val badSerializer = GlueSchemaRegistrySerializerImpl(credentials, badEndpointConfig)

        assertThrows(Exception::class.java) {
            badSerializer.encode(TRANSPORT_NAME, schema, payload)
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(CrossClientWireCompatIntegrationTest::class.java)
        private val REGION = GlueSchemaRegistryConnectionProperties.REGION
        private val ENDPOINT = GlueSchemaRegistryConnectionProperties.ENDPOINT
        private const val REGISTRY_NAME = "default-registry"
        private const val TRANSPORT_NAME = "xver-compat"

        private val SCHEMAS_TO_CLEAN_UP: MutableSet<String> = ConcurrentHashMap.newKeySet()

        @AfterAll
        @JvmStatic
        fun cleanUpSchemas() {
            if (SCHEMAS_TO_CLEAN_UP.isEmpty()) {
                return
            }
            log.info("Starting Clean-up of schemas created with GSR.")
            GlueClient
                .builder()
                .region(Region.of(REGION))
                .endpointOverride(URI(ENDPOINT))
                .credentialsProvider(DefaultCredentialsProvider.builder().build())
                .httpClient(UrlConnectionHttpClient.builder().build())
                .build()
                .use { glueClient ->
                    for (schemaName in SCHEMAS_TO_CLEAN_UP) {
                        try {
                            glueClient.deleteSchema(
                                DeleteSchemaRequest
                                    .builder()
                                    .schemaId(
                                        SchemaId
                                            .builder()
                                            .registryName(REGISTRY_NAME)
                                            .schemaName(schemaName)
                                            .build(),
                                    ).build(),
                            )
                        } catch (e: EntityNotFoundException) {
                            log.info("Schema {} is already gone, nothing to clean up.", schemaName, e)
                        }
                    }
                }
            SCHEMAS_TO_CLEAN_UP.clear()
        }
    }
}
