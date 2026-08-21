package com.amazonaws.services.schemaregistry.integrationtests.generators

import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator
import com.google.protobuf.DynamicMessage

class ProtobufGenericNoneCompatDataGenerator : TestDataGenerator<DynamicMessage> {
    override fun createRecords(): List<DynamicMessage> = ProtobufGenerator.getAllDynamicMessages()
}
