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
package com.amazonaws.services.schemaregistry.integrationtests.properties;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;

public interface GlueSchemaRegistryConnectionProperties {
    // Glue Service Endpoint
    String REGION = resolveRegion();
    String ENDPOINT = resolveEndpoint();

    static String resolveRegion() {
        try {
            return new DefaultAwsRegionProviderChain().getRegion().id();
        } catch (SdkClientException e) {
            return "us-east-2";
        }
    }

    /**
     * GLUE_ENDPOINT points the tests at a Glue-compatible endpoint other than the public
     * one — a local emulator, or a VPC endpoint. Unset, the tests use the public endpoint
     * of {@link #REGION}. The region itself needs no override of its own: the provider
     * chain above already reads AWS_REGION.
     */
    static String resolveEndpoint() {
        String override = System.getenv("GLUE_ENDPOINT");
        if (override != null && !override.isEmpty()) {
            return override;
        }
        return String.format("https://glue.%s.amazonaws.com", REGION);
    }
}
