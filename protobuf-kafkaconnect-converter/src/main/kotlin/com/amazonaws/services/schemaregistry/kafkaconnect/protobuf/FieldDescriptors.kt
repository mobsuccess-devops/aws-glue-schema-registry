/*
 * Copyright 2026 Mobsuccess.
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

package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf

import com.google.protobuf.Descriptors

internal fun Descriptors.FieldDescriptor.hasOptionalKeyword(): Boolean = toProto().proto3Optional ||
    (isProto2Syntax && !isRequired && !isRepeated && containingOneof == null)

private val Descriptors.FieldDescriptor.isProto2Syntax: Boolean
    get() =
        when (file.toProto().syntax) {
            "editions", "proto3" -> false
            else -> true
        }
