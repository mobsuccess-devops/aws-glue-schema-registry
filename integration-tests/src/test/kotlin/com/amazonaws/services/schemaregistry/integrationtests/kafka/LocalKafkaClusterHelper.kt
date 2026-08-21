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

class LocalKafkaClusterHelper : KafkaClusterHelper {
    override fun getOrCreateCluster(): String = FAKE_CLUSTER_ARN

    /**
     * KAFKA_BOOTSTRAP lets a runner point the tests at a broker that is not on the
     * default port, so a machine already using 9092 can still run them.
     */
    override fun getBootstrapString(): String {
        val override = System.getenv("KAFKA_BOOTSTRAP")
        return if (!override.isNullOrEmpty()) override else BOOTSTRAP_STRING
    }

    override fun getZookeeperConnectString(): String = ZOOKEEPER_STRING

    override fun getNumberOfPartitions(): Int = NUMBER_OF_PARTITIONS

    override fun getReplicationFactor(): Short = REPLICATION_FACTOR

    companion object {
        private const val FAKE_CLUSTER_ARN = "FAKE_CLUSTER_ARN"
        private const val BOOTSTRAP_STRING = "127.0.0.1:9092"
        private const val ZOOKEEPER_STRING = "127.0.0.1:2181"
        private const val NUMBER_OF_PARTITIONS = 1
        private const val REPLICATION_FACTOR: Short = 1
    }
}
