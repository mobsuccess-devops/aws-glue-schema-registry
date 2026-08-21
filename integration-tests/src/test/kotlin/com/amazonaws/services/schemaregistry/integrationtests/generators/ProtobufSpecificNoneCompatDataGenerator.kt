package com.amazonaws.services.schemaregistry.integrationtests.generators

import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator
import com.google.protobuf.Message

class ProtobufSpecificNoneCompatDataGenerator : TestDataGenerator<Message> {
    override fun createRecords(): List<Message> = ProtobufGenerator.getAllPOJOMessages()
}
