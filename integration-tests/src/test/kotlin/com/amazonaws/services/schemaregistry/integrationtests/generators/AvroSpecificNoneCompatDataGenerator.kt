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

/**
 * Avro Specific Record generator with None compatibility
 */
class AvroSpecificNoneCompatDataGenerator : TestDataGenerator<Person> {
    override fun createRecords(): List<Person> {
        val jane =
            Person
                .newBuilder()
                .setAge(18)
                .setFirstName("Jane")
                .setLastName("Doe")
                .setHeight(178f)
                .setEmployed(true)
                .build()

        val john =
            Person
                .newBuilder()
                .setAge(28)
                .setFirstName("John")
                .setLastName("Doe")
                .setHeight(188f)
                .setEmployed(false)
                .build()

        return listOf(jane, john)
    }
}
