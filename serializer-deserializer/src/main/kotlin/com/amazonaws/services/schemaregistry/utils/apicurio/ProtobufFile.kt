/*
 * Copyright 2020 Red Hat
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
 *
 * This will be removed once Apicurio releases the latest version with the json_name fix
 * https://github.com/Apicurio/apicurio-registry/blob/master/utils/protobuf-schema-utilities/src/main/java/io/apicurio/registry/utils/protobuf/schema/ProtobufFile.java
 */

package com.amazonaws.services.schemaregistry.utils.apicurio

import com.google.common.collect.ContiguousSet
import com.google.common.collect.DiscreteDomain
import com.google.common.collect.Range
import com.google.common.io.Files
import com.squareup.wire.Syntax
import com.squareup.wire.schema.Location
import com.squareup.wire.schema.internal.parser.EnumConstantElement
import com.squareup.wire.schema.internal.parser.EnumElement
import com.squareup.wire.schema.internal.parser.FieldElement
import com.squareup.wire.schema.internal.parser.MessageElement
import com.squareup.wire.schema.internal.parser.ProtoFileElement
import com.squareup.wire.schema.internal.parser.ProtoParser
import java.io.File
import java.io.IOException
import java.nio.charset.StandardCharsets

/**
 * Indexed representation of the data resulting from parsing a single .proto protobuf schema file,
 * used mainly for schema validation.
 *
 * @author Jonathan Halliday
 * @author Ales Justin
 * @see <a href="https://github.com/nilslice/protolock">Protolock</a>
 */
class ProtobufFile(
    private val element: ProtoFileElement,
) {
    private val reservedFields: MutableMap<String, MutableSet<Any>> = HashMap()

    private val fieldMap: MutableMap<String, MutableMap<String, FieldElement>> = HashMap()
    private val enumFieldMap: MutableMap<String, MutableMap<String, EnumConstantElement>> = HashMap()

    private val mapMap: MutableMap<String, MutableMap<String, FieldElement>> = HashMap()

    private val nonReservedFields: MutableMap<String, MutableSet<Any>> = HashMap()
    private val nonReservedEnumFields: MutableMap<String, MutableSet<Any>> = HashMap()

    private val fieldsById: MutableMap<String, MutableMap<Int, String>> = HashMap()
    private val enumFieldsById: MutableMap<String, MutableMap<Int, String>> = HashMap()

    private val serviceRPCnames: MutableMap<String, MutableSet<String>> = HashMap()
    private val serviceRPCSignatures: MutableMap<String, MutableMap<String, String>> = HashMap()

    init {
        buildIndexes()
    }

    constructor(data: String) : this(toProtoFileElement(data))

    @Throws(IOException::class)
    constructor(file: File) : this(
        toProtoFileElement(
            java.lang.String.join("\n", Files.readLines(file, StandardCharsets.UTF_8)),
        ),
    )

    fun getPackageName(): String? = element.packageName

    /*
     * message name -> Set { Integer/tag || String/name }
     */
    fun getReservedFields(): Map<String, Set<Any>> = reservedFields

    /*
     * message name -> Map { field name -> FieldElement }
     */
    fun getFieldMap(): Map<String, Map<String, FieldElement>> = fieldMap

    /*
     * enum name -> Map { String/name -> EnumConstantElement }
     */
    fun getEnumFieldMap(): Map<String, Map<String, EnumConstantElement>> = enumFieldMap

    /*
     * message name -> Map { field name -> FieldElement }
     */
    fun getMapMap(): Map<String, Map<String, FieldElement>> = mapMap

    /*
     * message name -> Set { Integer/tag || String/name }
     */
    fun getNonReservedFields(): Map<String, Set<Any>> = nonReservedFields

    /*
     * enum name -> Set { Integer/tag || String/name }
     */
    fun getNonReservedEnumFields(): Map<String, Set<Any>> = nonReservedEnumFields

    /*
     * message name -> Map { field id -> field name }
     */
    fun getFieldsById(): Map<String, Map<Int, String>> = fieldsById

    /*
     * enum name -> Map { field id -> field name }
     */
    fun getEnumFieldsById(): Map<String, Map<Int, String>> = enumFieldsById

    /*
     * service name -> Set { rpc name }
     */
    fun getServiceRPCnames(): Map<String, Set<String>> = serviceRPCnames

    /*
     * service name -> Map { rpc name -> method signature }
     */
    fun getServiceRPCSignatures(): Map<String, Map<String, String>> = serviceRPCSignatures

    fun getSyntax(): Syntax? = element.syntax

    private fun buildIndexes() {
        for (typeElement in element.types) {
            if (typeElement is MessageElement) {
                processMessageElement("", typeElement)
            } else if (typeElement is EnumElement) {
                processEnumElement("", typeElement)
            } else {
                throw RuntimeException()
            }
        }

        for (serviceElement in element.services) {
            val rpcNames: MutableSet<String> = HashSet()
            val rpcSignatures: MutableMap<String, String> = HashMap()
            for (rpcElement in serviceElement.rpcs) {
                rpcNames.add(rpcElement.name)

                val signature =
                    rpcElement.requestType + ":" + rpcElement.requestStreaming + "->" +
                        rpcElement.responseType + ":" + rpcElement.responseStreaming
                rpcSignatures[rpcElement.name] = signature
            }
            if (rpcNames.isNotEmpty()) {
                serviceRPCnames[serviceElement.name] = rpcNames
                serviceRPCSignatures[serviceElement.name] = rpcSignatures
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun processMessageElement(
        scope: String,
        messageElement: MessageElement,
    ) {
        // reservedFields
        val reservedFieldSet: MutableSet<Any> = HashSet()
        for (reservedElement in messageElement.reserveds) {
            for (value in reservedElement.values) {
                if (value is Range<*>) {
                    reservedFieldSet.addAll(
                        ContiguousSet.create(value as Range<Int>, DiscreteDomain.integers()),
                    )
                } else {
                    reservedFieldSet.add(value)
                }
            }
        }
        if (reservedFieldSet.isNotEmpty()) {
            reservedFields[scope + messageElement.name] = reservedFieldSet
        }

        // fieldMap, mapMap, FieldsIDName
        val fieldTypeMap: MutableMap<String, FieldElement> = HashMap()
        val mapMap: MutableMap<String, FieldElement> = HashMap()
        val idsToNames: MutableMap<Int, String> = HashMap()
        for (fieldElement in messageElement.fields) {
            fieldTypeMap[fieldElement.name] = fieldElement
            if (fieldElement.type.startsWith("map<")) {
                mapMap[fieldElement.name] = fieldElement
            }
            idsToNames[fieldElement.tag] = fieldElement.name
        }
        for (oneOfElement in messageElement.oneOfs) {
            for (fieldElement in oneOfElement.fields) {
                fieldTypeMap[fieldElement.name] = fieldElement
                if (fieldElement.type.startsWith("map<")) {
                    mapMap[fieldElement.name] = fieldElement
                }
                idsToNames[fieldElement.tag] = fieldElement.name
            }
        }

        if (fieldTypeMap.isNotEmpty()) {
            fieldMap[scope + messageElement.name] = fieldTypeMap
        }
        if (mapMap.isNotEmpty()) {
            this.mapMap[scope + messageElement.name] = mapMap
        }
        if (idsToNames.isNotEmpty()) {
            fieldsById[scope + messageElement.name] = idsToNames
        }

        // nonReservedFields
        val fieldKeySet: MutableSet<Any> = HashSet()
        for (fieldElement in messageElement.fields) {
            fieldKeySet.add(fieldElement.tag)
            fieldKeySet.add(fieldElement.name)
        }
        for (oneOfElement in messageElement.oneOfs) {
            for (fieldElement in oneOfElement.fields) {
                fieldKeySet.add(fieldElement.tag)
                fieldKeySet.add(fieldElement.name)
            }
        }

        if (fieldKeySet.isNotEmpty()) {
            nonReservedFields[scope + messageElement.name] = fieldKeySet
        }

        for (typeElement in messageElement.nestedTypes) {
            if (typeElement is MessageElement) {
                processMessageElement(messageElement.name + ".", typeElement)
            } else if (typeElement is EnumElement) {
                processEnumElement(messageElement.name + ".", typeElement)
            }
        }
    }

    private fun processEnumElement(
        scope: String,
        enumElement: EnumElement,
    ) {
        // TODO reservedEnumFields - wire doesn't preserve these
        // https://github.com/square/wire/issues/797 RFE: capture EnumElement reserved info

        // enumFieldMap, enumFieldsIDName, nonReservedEnumFields
        val map: MutableMap<String, EnumConstantElement> = HashMap()
        val idsToNames: MutableMap<Int, String> = HashMap()
        val fieldKeySet: MutableSet<Any> = HashSet()
        for (enumConstantElement in enumElement.constants) {
            map[enumConstantElement.name] = enumConstantElement
            idsToNames[enumConstantElement.tag] = enumConstantElement.name

            fieldKeySet.add(enumConstantElement.tag)
            fieldKeySet.add(enumConstantElement.name)
        }
        if (map.isNotEmpty()) {
            enumFieldMap[scope + enumElement.name] = map
        }
        if (idsToNames.isNotEmpty()) {
            enumFieldsById[scope + enumElement.name] = idsToNames
        }
        if (fieldKeySet.isNotEmpty()) {
            nonReservedEnumFields[scope + enumElement.name] = fieldKeySet
        }
    }

    companion object {
        @JvmStatic
        fun toProtoFileElement(data: String): ProtoFileElement = ProtoParser.parse(Location.get(""), data)
    }
}
