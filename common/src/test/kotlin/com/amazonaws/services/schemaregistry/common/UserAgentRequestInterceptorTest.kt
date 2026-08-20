package com.amazonaws.services.schemaregistry.common

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.common.configs.UserAgents
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.core.SdkRequest
import software.amazon.awssdk.core.interceptor.Context
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest
import java.util.stream.Stream

class UserAgentRequestInterceptorTest {
    @ParameterizedTest
    @MethodSource("getClientConfigTestCases")
    fun test_UserAgentInterceptor_ReturnsSdkRequestWithUserAgent(
        config: GlueSchemaRegistryConfiguration,
        expectedName: String,
    ) {
        val mockAwsCredentialsProvider = mock<AwsCredentialsProvider>()

        val awsSchemaRegistryClient = AWSSchemaRegistryClient(mockAwsCredentialsProvider, config)

        val userAgentRequestInterceptor = awsSchemaRegistryClient.UserAgentRequestInterceptor()

        val modifyRequest = mock<Context.ModifyRequest>()
        val glueRequest = GetSchemaVersionRequest.builder().build()
        doReturn(glueRequest).whenever(modifyRequest).request()

        val sdkHttpRequest = userAgentRequestInterceptor.modifyRequest(modifyRequest, null)
        assertNotNull(sdkHttpRequest)
        assertTrue(sdkHttpRequest.overrideConfiguration().isPresent)

        val actualApiName =
            sdkHttpRequest
                .overrideConfiguration()
                .get()
                .apiNames()[0]

        assertEquals(MavenPackaging.VERSION, actualApiName.version())
        assertEquals(expectedName, actualApiName.name())
    }

    @Test
    fun test_UserAgentInterceptor_ReturnsSameRequestForNonGlueRequests() {
        val mockAwsCredentialsProvider = mock<AwsCredentialsProvider>()

        val awsSchemaRegistryClient =
            AWSSchemaRegistryClient(mockAwsCredentialsProvider, GlueSchemaRegistryConfiguration(REGION))

        val userAgentRequestInterceptor = awsSchemaRegistryClient.UserAgentRequestInterceptor()

        val nonGlueRequest = mock<SdkRequest>()
        val modifyRequest = mock<Context.ModifyRequest>()
        doReturn(nonGlueRequest).whenever(modifyRequest).request()

        val actualRequest = userAgentRequestInterceptor.modifyRequest(modifyRequest, null)

        assertEquals(nonGlueRequest, actualRequest)
    }

    companion object {
        const val REGION = "us-east-1"

        @JvmStatic
        fun getClientConfigTestCases(): Stream<Arguments> = Stream.of(
            Arguments.of(
                GlueSchemaRegistryConfiguration(
                    mapOf(
                        AWSSchemaRegistryConstants.AWS_REGION to REGION,
                        AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING to true,
                        AWSSchemaRegistryConstants.COMPRESSION_TYPE to
                            AWSSchemaRegistryConstants.COMPRESSION.ZLIB.toString(),
                        AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER to
                            GlueSchemaRegistryDataFormatSerializer::class.java,
                        AWSSchemaRegistryConstants.USER_AGENT_APP to UserAgents.KAFKA,
                    ),
                ),
                "autoreg/1:compress/1:secdeser/1:app/kafka",
            ),
            Arguments.of(
                GlueSchemaRegistryConfiguration(
                    mapOf(
                        AWSSchemaRegistryConstants.AWS_REGION to REGION,
                        AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING to false,
                        AWSSchemaRegistryConstants.COMPRESSION_TYPE to
                            AWSSchemaRegistryConstants.COMPRESSION.NONE.toString(),
                    ),
                ),
                "autoreg/0:compress/0:secdeser/0:app/default",
            ),
        )
    }
}
