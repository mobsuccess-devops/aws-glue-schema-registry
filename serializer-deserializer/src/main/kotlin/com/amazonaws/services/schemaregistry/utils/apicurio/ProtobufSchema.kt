/*
 * Copyright 2021 Red Hat
 * Portions Copyright 2020 Amazon.com, Inc. or its affiliates.
 * All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.schemaregistry.utils.apicurio

import com.google.protobuf.Descriptors.FileDescriptor
import com.squareup.wire.schema.internal.parser.ProtoFileElement
import java.util.Objects

/**
 * @author Fabian Martinez
 */
class ProtobufSchema(
    private val fileDescriptor: FileDescriptor,
    private val protoFileElement: ProtoFileElement,
) {
    private var protobufFile: ProtobufFile? = null

    init {
        Objects.requireNonNull(fileDescriptor)
        Objects.requireNonNull(protoFileElement)
    }

    /**
     * @return the fileDescriptor
     */
    fun getFileDescriptor(): FileDescriptor = fileDescriptor

    /**
     * @return the protoFileElement
     */
    fun getProtoFileElement(): ProtoFileElement = protoFileElement

    /**
     * @return the protobufFile
     */
    fun getProtobufFile(): ProtobufFile {
        if (protobufFile == null) {
            protobufFile = ProtobufFile(protoFileElement)
        }
        return protobufFile!!
    }
}
