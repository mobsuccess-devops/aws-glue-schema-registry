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

import additionalTypes.Decimals
import com.google.common.base.CaseFormat.LOWER_UNDERSCORE
import com.google.common.base.CaseFormat.UPPER_CAMEL
import com.google.common.collect.ImmutableList
import com.google.protobuf.AnyProto
import com.google.protobuf.ApiProto
import com.google.protobuf.DescriptorProtos
import com.google.protobuf.DescriptorProtos.DescriptorProto
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto
import com.google.protobuf.DescriptorProtos.FileDescriptorProto
import com.google.protobuf.DescriptorProtos.FileOptions
import com.google.protobuf.DescriptorProtos.MethodDescriptorProto
import com.google.protobuf.DescriptorProtos.MethodOptions
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto
import com.google.protobuf.Descriptors
import com.google.protobuf.Descriptors.DescriptorValidationException
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.Descriptors.FileDescriptor
import com.google.protobuf.DurationProto
import com.google.protobuf.EmptyProto
import com.google.protobuf.FieldMaskProto
import com.google.protobuf.SourceContextProto
import com.google.protobuf.StructProto
import com.google.protobuf.TimestampProto
import com.google.protobuf.TypeProto
import com.google.protobuf.WrappersProto
import com.google.type.CalendarPeriodProto
import com.google.type.ColorProto
import com.google.type.DateProto
import com.google.type.DayOfWeek
import com.google.type.ExprProto
import com.google.type.FractionProto
import com.google.type.IntervalProto
import com.google.type.LatLng
import com.google.type.LocalizedTextProto
import com.google.type.MoneyProto
import com.google.type.MonthProto
import com.google.type.PhoneNumberProto
import com.google.type.PostalAddressProto
import com.google.type.QuaternionProto
import com.google.type.TimeOfDayProto
import com.squareup.wire.Syntax
import com.squareup.wire.schema.EnumType
import com.squareup.wire.schema.Field
import com.squareup.wire.schema.Location
import com.squareup.wire.schema.MessageType
import com.squareup.wire.schema.OneOf
import com.squareup.wire.schema.Options
import com.squareup.wire.schema.ProtoFile
import com.squareup.wire.schema.ProtoType
import com.squareup.wire.schema.Schema
import com.squareup.wire.schema.Service
import com.squareup.wire.schema.Type
import com.squareup.wire.schema.internal.parser.EnumConstantElement
import com.squareup.wire.schema.internal.parser.EnumElement
import com.squareup.wire.schema.internal.parser.ExtensionsElement
import com.squareup.wire.schema.internal.parser.FieldElement
import com.squareup.wire.schema.internal.parser.MessageElement
import com.squareup.wire.schema.internal.parser.OneOfElement
import com.squareup.wire.schema.internal.parser.OptionElement
import com.squareup.wire.schema.internal.parser.ProtoFileElement
import com.squareup.wire.schema.internal.parser.ReservedElement
import com.squareup.wire.schema.internal.parser.RpcElement
import com.squareup.wire.schema.internal.parser.ServiceElement
import com.squareup.wire.schema.internal.parser.TypeElement
import metadata.ProtobufSchemaMetadata
import java.util.Collections
import java.util.LinkedHashSet
import java.util.Objects
import java.util.Optional
import java.util.TreeMap

/**
 * @author Fabian Martinez, Ravindranath Kakarla, Carles Arnal
 */
object FileDescriptorUtils {
    @JvmField
    val DEFAULT_LOCATION: Location = Location.get("")

    private const val PROTO3 = "proto3"
    private const val ALLOW_ALIAS_OPTION = "allow_alias"
    private const val MAP_ENTRY_OPTION = "map_entry"
    private const val KEY_FIELD = "key"
    private const val VALUE_FIELD = "value"
    private const val MAP_ENTRY_SUFFIX = "Entry"
    private const val DEPRECATED_OPTION = "deprecated"
    private const val OPTIONAL = "optional"

    // field options
    private const val PACKED_OPTION = "packed"
    private const val JSON_NAME_OPTION = "json_name"
    private const val CTYPE_OPTION = "ctype"
    private const val JSTYPE_OPTION = "jstype"

    // file options
    private const val CC_GENERIC_SERVICES_OPTION = "cc_generic_services"
    private const val CC_ENABLE_ARENAS_OPTION = "cc_enable_arenas"
    private const val CSHARP_NAMESPACE_OPTION = "csharp_namespace"
    private const val GO_PACKAGE_OPTION = "go_package"
    private const val JAVA_GENERIC_SERVICES_OPTION = "java_generic_services"
    private const val JAVA_MULTIPLE_FILES_OPTION = "java_multiple_files"
    private const val JAVA_OUTER_CLASSNAME_OPTION = "java_outer_classname"
    private const val JAVA_PACKAGE_OPTION = "java_package"
    private const val JAVA_STRING_CHECK_UTF8_OPTION = "java_string_check_utf8"
    private const val OBJC_CLASS_PREFIX_OPTION = "objc_class_prefix"
    private const val OPTIMIZE_FOR_OPTION = "optimize_for"
    private const val PHP_CLASS_PREFIX_OPTION = "php_class_prefix"
    private const val PHP_GENERIC_SERVICES_OPTION = "php_generic_services"
    private const val PHP_METADATA_NAMESPACE_OPTION = "php_metadata_namespace"
    private const val PHP_NAMESPACE_OPTION = "php_namespace"
    private const val PY_GENERIC_SERVICES_OPTION = "py_generic_services"
    private const val RUBY_PACKAGE_OPTION = "ruby_package"
    private const val SWIFT_PREFIX_OPTION = "swift_prefix"

    // message options
    private const val NO_STANDARD_DESCRIPTOR_OPTION = "no_standard_descriptor_accessor"

    // rpc options
    private const val IDEMPOTENCY_LEVEL_OPTION = "idempotency_level"

    private val booleanKind = OptionElement.Kind.BOOLEAN
    private val stringKind = OptionElement.Kind.STRING
    private val enumKind = OptionElement.Kind.ENUM

    @JvmStatic
    fun baseDependencies(): Array<FileDescriptor> = // Support all the Protobuf WellKnownTypes
        // and the protos from Google API, https://github.com/googleapis/googleapis
        arrayOf(
            ApiProto.getDescriptor().file,
            FieldMaskProto.getDescriptor().file,
            SourceContextProto.getDescriptor().file,
            StructProto.getDescriptor().file,
            TypeProto.getDescriptor().file,
            TimestampProto.getDescriptor().file,
            WrappersProto.getDescriptor().file,
            AnyProto.getDescriptor().file,
            EmptyProto.getDescriptor().file,
            DurationProto.getDescriptor().file,
            TimeOfDayProto.getDescriptor().file,
            DateProto.getDescriptor().file,
            CalendarPeriodProto.getDescriptor().file,
            ColorProto.getDescriptor().file,
            DayOfWeek.getDescriptor().file,
            LatLng.getDescriptor().file,
            FractionProto.getDescriptor().file,
            MoneyProto.getDescriptor().file,
            MonthProto.getDescriptor().file,
            PhoneNumberProto.getDescriptor().file,
            PostalAddressProto.getDescriptor().file,
            CalendarPeriodProto.getDescriptor().file,
            LocalizedTextProto.getDescriptor().file,
            IntervalProto.getDescriptor().file,
            ExprProto.getDescriptor().file,
            QuaternionProto.getDescriptor().file,
            PostalAddressProto.getDescriptor().file,
            ProtobufSchemaMetadata.getDescriptor().file,
            Decimals.getDescriptor().file,
        )

    @JvmStatic
    @Throws(DescriptorValidationException::class)
    fun protoFileToFileDescriptor(element: ProtoFileElement): FileDescriptor = protoFileToFileDescriptor(element, "default.proto")

    @JvmStatic
    @Throws(DescriptorValidationException::class)
    fun protoFileToFileDescriptor(
        element: ProtoFileElement,
        protoFileName: String,
    ): FileDescriptor {
        Objects.requireNonNull(element)
        Objects.requireNonNull(protoFileName)

        return protoFileToFileDescriptor(
            element.toSchema(),
            protoFileName,
            Optional.ofNullable(element.packageName),
        )
    }

    @JvmStatic
    @Throws(DescriptorValidationException::class)
    fun protoFileToFileDescriptor(
        schemaDefinition: String,
        protoFileName: String,
        optionalPackageName: Optional<String>,
    ): FileDescriptor {
        Objects.requireNonNull(schemaDefinition)
        Objects.requireNonNull(protoFileName)

        return FileDescriptor.buildFrom(
            toFileDescriptorProto(schemaDefinition, protoFileName, optionalPackageName),
            baseDependencies(),
        )
    }

    private fun toFileDescriptorProto(
        schemaDefinition: String,
        protoFileName: String,
        optionalPackageName: Optional<String>,
    ): FileDescriptorProto {
        val protobufSchemaLoaderContext: ProtobufSchemaLoader.ProtobufSchemaLoaderContext =
            try {
                ProtobufSchemaLoader.loadSchema(optionalPackageName, protoFileName, schemaDefinition)
            } catch (e: Exception) {
                throw RuntimeException(e)
            }

        val schema = FileDescriptorProto.newBuilder()

        val element = protobufSchemaLoaderContext.getProtoFile()
        val schemaContext = protobufSchemaLoaderContext.getSchema()

        schema.setName(protoFileName)

        val syntax = element.syntax
        if (Syntax.PROTO_3 == syntax) {
            schema.setSyntax(syntax.toString())
        }
        if (element.packageName != null) {
            schema.setPackage(element.packageName)
        }

        for (protoType in schemaContext.types) {
            if (!isParentLevelType(protoType, optionalPackageName)) {
                continue
            }

            val type = schemaContext.getType(protoType)
            if (type is MessageType) {
                val message = messageElementToDescriptorProto(type, schemaContext, element)
                schema.addMessageType(message)
            } else if (type is EnumType) {
                val message = enumElementToProto(type)
                schema.addEnumType(message)
            }
        }

        for (service in element.services) {
            val serviceDescriptorProto = serviceElementToProto(service)
            schema.addService(serviceDescriptorProto)
        }

        // dependencies on protobuf default types are always added
        for (ref in element.imports) {
            schema.addDependency(ref)
        }
        for (ref in element.publicImports) {
            var add = true
            for (i in 0 until schema.dependencyCount) {
                if (schema.getDependency(i) == ref) {
                    schema.addPublicDependency(i)
                    add = false
                }
            }
            if (add) {
                schema.addDependency(ref)
                schema.addPublicDependency(schema.dependencyCount - 1)
            }
        }

        val javaPackageName = findOptionString(JAVA_PACKAGE_OPTION, element.options)
        if (javaPackageName != null) {
            schema.mergeOptions(FileOptions.newBuilder().setJavaPackage(javaPackageName).build())
        }

        val javaOuterClassname = findOptionString(JAVA_OUTER_CLASSNAME_OPTION, element.options)
        if (javaOuterClassname != null) {
            schema.mergeOptions(FileOptions.newBuilder().setJavaOuterClassname(javaOuterClassname).build())
        }

        val javaMultipleFiles = findOptionBoolean(JAVA_MULTIPLE_FILES_OPTION, element.options)
        if (javaMultipleFiles != null) {
            schema.mergeOptions(FileOptions.newBuilder().setJavaMultipleFiles(javaMultipleFiles).build())
        }

        val javaStringCheckUtf8 = findOptionBoolean(JAVA_STRING_CHECK_UTF8_OPTION, element.options)
        if (javaStringCheckUtf8 != null) {
            schema.mergeOptions(FileOptions.newBuilder().setJavaStringCheckUtf8(javaStringCheckUtf8).build())
        }

        val javaGenericServices = findOptionBoolean(JAVA_GENERIC_SERVICES_OPTION, element.options)
        if (javaGenericServices != null) {
            schema.mergeOptions(FileOptions.newBuilder().setJavaGenericServices(javaGenericServices).build())
        }

        val ccGenericServices = findOptionBoolean(CC_GENERIC_SERVICES_OPTION, element.options)
        if (ccGenericServices != null) {
            schema.mergeOptions(FileOptions.newBuilder().setCcGenericServices(ccGenericServices).build())
        }

        val ccEnableArenas = findOptionBoolean(CC_ENABLE_ARENAS_OPTION, element.options)
        if (ccEnableArenas != null) {
            schema.mergeOptions(FileOptions.newBuilder().setCcEnableArenas(ccEnableArenas).build())
        }

        val csharpNamespace = findOptionString(CSHARP_NAMESPACE_OPTION, element.options)
        if (csharpNamespace != null) {
            schema.mergeOptions(FileOptions.newBuilder().setCsharpNamespace(csharpNamespace).build())
        }

        val goPackageName = findOptionString(GO_PACKAGE_OPTION, element.options)
        if (goPackageName != null) {
            schema.mergeOptions(FileOptions.newBuilder().setGoPackage(goPackageName).build())
        }

        val objcClassPrefix = findOptionString(OBJC_CLASS_PREFIX_OPTION, element.options)
        if (objcClassPrefix != null) {
            schema.mergeOptions(FileOptions.newBuilder().setObjcClassPrefix(objcClassPrefix).build())
        }

        val phpGenericServices = findOptionBoolean(PHP_GENERIC_SERVICES_OPTION, element.options)
        if (phpGenericServices != null) {
            schema.mergeOptions(FileOptions.newBuilder().setPhpGenericServices(phpGenericServices).build())
        }

        val phpClassPrefix = findOptionString(PHP_CLASS_PREFIX_OPTION, element.options)
        if (phpClassPrefix != null) {
            schema.mergeOptions(FileOptions.newBuilder().setPhpClassPrefix(phpClassPrefix).build())
        }

        val phpMetadataNamespace = findOptionString(PHP_METADATA_NAMESPACE_OPTION, element.options)
        if (phpMetadataNamespace != null) {
            schema.mergeOptions(FileOptions.newBuilder().setPhpMetadataNamespace(phpMetadataNamespace).build())
        }

        val phpNamespace = findOptionString(PHP_NAMESPACE_OPTION, element.options)
        if (phpNamespace != null) {
            schema.mergeOptions(FileOptions.newBuilder().setPhpNamespace(phpNamespace).build())
        }

        val pyGenericServices = findOptionBoolean(PY_GENERIC_SERVICES_OPTION, element.options)
        if (pyGenericServices != null) {
            schema.mergeOptions(FileOptions.newBuilder().setPyGenericServices(pyGenericServices).build())
        }

        val rubyPackage = findOptionString(RUBY_PACKAGE_OPTION, element.options)
        if (rubyPackage != null) {
            schema.mergeOptions(FileOptions.newBuilder().setRubyPackage(rubyPackage).build())
        }

        val swiftPrefix = findOptionString(SWIFT_PREFIX_OPTION, element.options)
        if (swiftPrefix != null) {
            schema.mergeOptions(FileOptions.newBuilder().setSwiftPrefix(swiftPrefix).build())
        }

        val optimizeFor =
            findOption(OPTIMIZE_FOR_OPTION, element.options)
                .map { o -> FileOptions.OptimizeMode.valueOf(o.value.toString()) }
                .orElse(null)
        if (optimizeFor != null) {
            schema.mergeOptions(FileOptions.newBuilder().setOptimizeFor(optimizeFor).build())
        }

        return schema.build()
    }

    /**
     * When schema loader links the schema, it also includes google.protobuf types in it.
     * We want to ignore all the other types except for the ones that are present in the current file.
     *
     * @return true if a type is a parent type, false otherwise.
     */
    private fun isParentLevelType(
        protoType: ProtoType,
        optionalPackageName: Optional<String>,
    ): Boolean {
        val typeName = protoType.toString()
        if (optionalPackageName.isPresent) {
            val packageName = optionalPackageName.get()

            // If the type doesn't start with the package name, ignore it.
            if (!typeName.startsWith(packageName)) {
                return false
            }
            // We only want to consider the parent level types. The list can contain following,
            // [io.apicurio.foo.bar.Customer.Address, io.apicurio.foo.bar.Customer, google.protobuf.Timestamp]
            // We want to only get the type "io.apicurio.foo.bar.Customer" which is parent level type.
            val typeNames =
                typeName
                    .split(packageName.toRegex())
                    .dropLastWhile { it.isEmpty() }[1]
                    .split("\\.".toRegex())
                    .dropLastWhile { it.isEmpty() }
            return typeNames.size <= 2
        }

        // In case the package is not defined, we select the types that are not google types or metadata types.
        return !typeName.startsWith("google.type") &&
            !typeName.startsWith("google.protobuf") &&
            !typeName.startsWith("metadata") &&
            !typeName.startsWith("additionalTypes")
    }

    @Suppress("UNCHECKED_CAST")
    private fun messageElementToDescriptorProto(
        messageElem: MessageType,
        schema: Schema,
        element: ProtoFile,
    ): DescriptorProto {
        val message = ProtobufMessage()
        message.protoBuilder().setName(messageElem.type.simpleName)

        val locationComparator = compareBy<Location>({ it.line }, { it.column })
        val allNestedTypes: MutableMap<Location, DescriptorProto> = TreeMap(locationComparator)
        val allFields: MutableList<FieldDescriptorProto> = ArrayList()

        for (type in messageElem.nestedTypes) {
            if (type is MessageType) {
                allNestedTypes[type.location] = messageElementToDescriptorProto(type, schema, element)
            } else if (type is EnumType) {
                message.protoBuilder().addEnumType(enumElementToProto(type))
            }
        }

        val isProto3Optional = { field: Field ->
            Field.Label.OPTIONAL == field.label && Syntax.PROTO_3 == element.syntax
        }

        val oneOfs = messageElem.oneOfs as MutableList<OneOf>
        val proto3OptionalOneOfs =
            messageElem.fieldsAndOneOfFields
                .filter(isProto3Optional)
                .map { getProto3OptionalField(it) }

        // Proto3 Optionals are considered as "synthetic-oneofs" by Protobuf compiler.
        oneOfs.addAll(proto3OptionalOneOfs)

        val findOneOfByFieldName = { fieldName: String ->
            var found: Optional<OneOf> = Optional.empty()
            for (oneOf in oneOfs) {
                if (oneOf.fields.map { it.name }.any { it == fieldName }) {
                    found = Optional.of(oneOf)
                    break
                }
            }
            found
        }

        // Add all the declared fields first skipping oneOfs.
        for (field in messageElem.declaredFields) {
            val optionalOneOf = findOneOfByFieldName(field.name)
            if (!optionalOneOf.isPresent) {
                val fieldLabel = field.label
                // Fields are optional by default in Proto3.
                var label = if (fieldLabel != null) fieldLabel.toString().lowercase() else OPTIONAL

                var fieldType = determineFieldType(field.type!!, schema)
                val protoType = field.type!!
                var fieldTypeName = protoType.toString()
                val keyType = protoType.keyType
                val valueType = protoType.valueType
                // Map fields are only permitted in messages
                if (protoType.isMap && keyType != null && valueType != null) {
                    label = "repeated"
                    fieldType = "message"
                    var fieldMapEntryName = toMapEntry(field.name)
                    // Map entry field name is capitalized
                    fieldMapEntryName =
                        fieldMapEntryName.substring(0, 1).uppercase() + fieldMapEntryName.substring(1)
                    // Map field type name is resolved with reference to the package
                    fieldTypeName = String.format("%s.%s", messageElem.type, fieldMapEntryName)
                    val protobufMapMessage = ProtobufMessage()
                    val mapMessage =
                        protobufMapMessage
                            .protoBuilder()
                            .setName(fieldMapEntryName)
                            .mergeOptions(
                                DescriptorProtos.MessageOptions
                                    .newBuilder()
                                    .setMapEntry(true)
                                    .build(),
                            )

                    protobufMapMessage.addField(
                        OPTIONAL, determineFieldType(keyType, schema), keyType.toString(), KEY_FIELD, 1,
                        null, null, null, null, null, null, null, null, null, null,
                    )
                    protobufMapMessage.addField(
                        OPTIONAL, determineFieldType(valueType, schema), valueType.toString(), VALUE_FIELD, 2,
                        null, null, null, null, null, null, null, null, null, null,
                    )
                    allNestedTypes[field.location] = mapMessage.build()
                }

                val jsonName =
                    if (getDefaultJsonName(field.name) == field.declaredJsonName) null else field.declaredJsonName
                val isDeprecated = findOptionBoolean(DEPRECATED_OPTION, field.options)
                val isPacked = findOptionBoolean(PACKED_OPTION, field.options)
                val cType =
                    findOption(CTYPE_OPTION, field.options)
                        .map { o -> DescriptorProtos.FieldOptions.CType.valueOf(o.value.toString()) }
                        .orElse(null)
                val jsType =
                    findOption(JSTYPE_OPTION, field.options)
                        .map { o -> DescriptorProtos.FieldOptions.JSType.valueOf(o.value.toString()) }
                        .orElse(null)

                val metadataKey =
                    findOptionString(
                        ProtobufSchemaMetadata.metadataKey.descriptor.fullName,
                        field.options,
                    )
                val metadataValue =
                    findOptionString(
                        ProtobufSchemaMetadata.metadataValue.descriptor.fullName,
                        field.options,
                    )

                allFields.add(
                    ProtobufMessage.buildFieldDescriptorProto(
                        label, fieldType, fieldTypeName, field.name, field.tag, field.default,
                        jsonName, isDeprecated, isPacked, cType, jsType, metadataKey, metadataValue, null, null,
                    ),
                )
            }
        }

        val addedOneOfs: MutableSet<OneOf> = LinkedHashSet()

        // Add the oneOfs next including Proto3 Optionals.
        for (oneOf in oneOfs) {
            if (addedOneOfs.contains(oneOf)) {
                continue
            }

            var isProto3OptionalField: Boolean? = null
            if (proto3OptionalOneOfs.contains(oneOf)) {
                isProto3OptionalField = true
            }
            val oneofBuilder = OneofDescriptorProto.newBuilder().setName(oneOf.name)
            message.protoBuilder().addOneofDecl(oneofBuilder)

            for (oneOfField in oneOf.fields) {
                val oneOfJsonName =
                    if (getDefaultJsonName(oneOfField.name) == oneOfField.declaredJsonName) {
                        null
                    } else {
                        oneOfField.declaredJsonName
                    }
                val oneOfIsDeprecated = findOptionBoolean(DEPRECATED_OPTION, oneOfField.options)
                val oneOfIsPacked = findOptionBoolean(PACKED_OPTION, oneOfField.options)
                val oneOfCType =
                    findOption(CTYPE_OPTION, oneOfField.options)
                        .map { o -> DescriptorProtos.FieldOptions.CType.valueOf(o.value.toString()) }
                        .orElse(null)
                val oneOfJsType =
                    findOption(JSTYPE_OPTION, oneOfField.options)
                        .map { o -> DescriptorProtos.FieldOptions.JSType.valueOf(o.value.toString()) }
                        .orElse(null)
                val metadataKey =
                    findOptionString(
                        ProtobufSchemaMetadata.metadataKey.descriptor.fullName,
                        oneOfField.options,
                    )
                val metadataValue =
                    findOptionString(
                        ProtobufSchemaMetadata.metadataValue.descriptor.fullName,
                        oneOfField.options,
                    )

                allFields.add(
                    ProtobufMessage.buildFieldDescriptorProto(
                        OPTIONAL,
                        determineFieldType(oneOfField.type!!, schema),
                        oneOfField.type.toString(),
                        oneOfField.name,
                        oneOfField.tag,
                        oneOfField.default,
                        oneOfJsonName,
                        oneOfIsDeprecated,
                        oneOfIsPacked,
                        oneOfCType,
                        oneOfJsType,
                        metadataKey,
                        metadataValue,
                        message.protoBuilder().oneofDeclCount - 1,
                        isProto3OptionalField,
                    ),
                )
            }
            addedOneOfs.add(oneOf)
        }

        for (reserved in messageElem.toElement().reserveds) {
            for (elem in reserved.values) {
                if (elem is String) {
                    message.protoBuilder().addReservedName(elem)
                } else if (elem is Int) {
                    val rangeBuilder =
                        DescriptorProto.ReservedRange
                            .newBuilder()
                            .setStart(elem)
                            .setEnd(elem + 1)
                    message.protoBuilder().addReservedRange(rangeBuilder.build())
                } else if (elem is IntRange) {
                    val rangeBuilder =
                        DescriptorProto.ReservedRange
                            .newBuilder()
                            .setStart(elem.start)
                            .setEnd(elem.endInclusive + 1)
                    message.protoBuilder().addReservedRange(rangeBuilder.build())
                } else {
                    throw IllegalStateException("Unsupported reserved type: " + elem.javaClass.name)
                }
            }
        }
        for (extensions in messageElem.toElement().extensions) {
            for (elem in extensions.values) {
                if (elem is Int) {
                    val extensionBuilder =
                        DescriptorProto.ExtensionRange
                            .newBuilder()
                            .setStart(elem)
                            .setEnd(elem + 1)
                    message.protoBuilder().addExtensionRange(extensionBuilder.build())
                } else if (elem is IntRange) {
                    val extensionBuilder =
                        DescriptorProto.ExtensionRange
                            .newBuilder()
                            .setStart(elem.start)
                            .setEnd(elem.endInclusive + 1)
                    message.protoBuilder().addExtensionRange(extensionBuilder.build())
                } else {
                    throw IllegalStateException("Unsupported extension type: " + elem.javaClass.name)
                }
            }
        }

        val isMapEntry = findOptionBoolean(MAP_ENTRY_OPTION, messageElem.options)
        if (isMapEntry != null) {
            val optionsBuilder =
                DescriptorProtos.MessageOptions.newBuilder().setMapEntry(isMapEntry)
            message.protoBuilder().mergeOptions(optionsBuilder.build())
        }
        val noStandardDescriptorAccessor =
            findOptionBoolean(NO_STANDARD_DESCRIPTOR_OPTION, messageElem.options)
        if (noStandardDescriptorAccessor != null) {
            val optionsBuilder =
                DescriptorProtos.MessageOptions
                    .newBuilder()
                    .setNoStandardDescriptorAccessor(noStandardDescriptorAccessor)
            message.protoBuilder().mergeOptions(optionsBuilder.build())
        }

        message.protoBuilder().addAllNestedType(allNestedTypes.values)
        message.protoBuilder().addAllField(allFields)
        return message.build()
    }

    private fun determineFieldType(
        protoType: ProtoType,
        schema: Schema,
    ): String? {
        val typeReference = schema.getType(protoType)
        if (typeReference != null) {
            if (typeReference is MessageType) {
                return "message"
            }
            if (typeReference is EnumType) {
                return "enum"
            }
        }
        return null
    }

    /**
     * Proto3 optional fields are "synthetic one-ofs" and are written as one-of fields over the wire.
     * This method generates the synthetic one-of from a Proto3 optional field.
     */
    private fun getProto3OptionalField(field: Field): OneOf = OneOf(
        "_" + field.name,
        "",
        Collections.singletonList(field),
        DEFAULT_LOCATION,
        Options(Options.FIELD_OPTIONS, Collections.emptyList()),
    )

    private fun enumElementToProto(enumElem: EnumType): EnumDescriptorProto {
        val allowAlias = findOptionBoolean(ALLOW_ALIAS_OPTION, enumElem.options)

        val builder = EnumDescriptorProto.newBuilder().setName(enumElem.name)
        if (allowAlias != null) {
            val optionsBuilder = DescriptorProtos.EnumOptions.newBuilder().setAllowAlias(allowAlias)
            builder.mergeOptions(optionsBuilder.build())
        }
        for (constant in enumElem.constants) {
            builder.addValue(
                EnumValueDescriptorProto
                    .newBuilder()
                    .setName(constant.name)
                    .setNumber(constant.tag)
                    .build(),
            )
        }
        return builder.build()
    }

    private fun serviceElementToProto(serviceElem: Service): ServiceDescriptorProto {
        val builder = ServiceDescriptorProto.newBuilder().setName(serviceElem.name)

        for (rpc in serviceElem.rpcs) {
            val methodBuilder =
                MethodDescriptorProto
                    .newBuilder()
                    .setName(rpc.name)
                    .setInputType(getTypeName(rpc.requestType.toString()))
                    .setOutputType(getTypeName(rpc.responseType.toString()))
            if (rpc.requestStreaming) {
                methodBuilder.setClientStreaming(rpc.requestStreaming)
            }
            if (rpc.responseStreaming) {
                methodBuilder.setServerStreaming(rpc.responseStreaming)
            }
            val deprecated = findOptionBoolean(DEPRECATED_OPTION, rpc.options)
            if (deprecated != null) {
                methodBuilder.mergeOptions(MethodOptions.newBuilder().setDeprecated(deprecated).build())
            }
            val idempotencyLevel =
                findOption(IDEMPOTENCY_LEVEL_OPTION, rpc.options)
                    .map { o -> MethodOptions.IdempotencyLevel.valueOf(o.value.toString()) }
                    .orElse(null)
            if (idempotencyLevel != null) {
                methodBuilder.mergeOptions(
                    MethodOptions.newBuilder().setIdempotencyLevel(idempotencyLevel).build(),
                )
            }

            builder.addMethod(methodBuilder.build())
        }

        val deprecated = findOptionBoolean(DEPRECATED_OPTION, serviceElem.options)
        if (deprecated != null) {
            val optionsBuilder = DescriptorProtos.ServiceOptions.newBuilder().setDeprecated(deprecated)
            builder.mergeOptions(optionsBuilder.build())
        }

        return builder.build()
    }

    private fun toMapEntry(s: String): String {
        var name = s
        if (name.contains("_")) {
            name = LOWER_UNDERSCORE.to(UPPER_CAMEL, name)
        }
        return name + MAP_ENTRY_SUFFIX
    }

    private fun findOption(
        name: String,
        options: Options,
    ): Optional<OptionElement> = options.elements.stream().filter { o -> o.name == name }.findFirst()

    private fun findOptionString(
        name: String,
        options: Options,
    ): String? = findOption(name, options).map { o -> o.value.toString() }.orElse(null)

    private fun findOptionBoolean(
        name: String,
        options: Options,
    ): Boolean? = findOption(name, options).map { o -> java.lang.Boolean.valueOf(o.value.toString()) }.orElse(null)

    @JvmStatic
    fun fileDescriptorWithDepsToProtoFile(
        file: FileDescriptor,
        dependencies: MutableMap<String, ProtoFileElement>,
    ): ProtoFileElement {
        for (dependency in file.dependencies) {
            val depName = dependency.name
            dependencies[depName] = fileDescriptorWithDepsToProtoFile(dependency, dependencies)
        }
        return fileDescriptorToProtoFile(file.toProto())
    }

    @JvmStatic
    fun fileDescriptorToProtoFile(file: FileDescriptorProto): ProtoFileElement {
        var packageName: String? = file.getPackage()
        if ("" == packageName) {
            packageName = null
        }

        var syntax: Syntax? = null
        when (file.syntax) {
            "proto2" -> syntax = Syntax.PROTO_2
            "proto3" -> syntax = Syntax.PROTO_3
            else -> Unit
        }
        val types = ImmutableList.builder<TypeElement>()
        for (md in file.messageTypeList) {
            types.add(toMessage(file, md))
        }
        for (ed in file.enumTypeList) {
            types.add(toEnum(ed))
        }
        val services = ImmutableList.builder<ServiceElement>()
        for (sv in file.serviceList) {
            services.add(toService(sv))
        }
        val imports = ImmutableList.builder<String>()
        val publicImports = ImmutableList.builder<String>()
        val dependencyList = file.dependencyList
        val publicDependencyList: Set<Int> = HashSet(file.publicDependencyList)
        for (i in dependencyList.indices) {
            val depName = dependencyList[i]
            if (publicDependencyList.contains(i)) {
                publicImports.add(depName)
            } else {
                imports.add(depName)
            }
        }
        val options = ImmutableList.builder<OptionElement>()
        if (file.options.hasJavaPackage()) {
            options.add(OptionElement(JAVA_PACKAGE_OPTION, stringKind, file.options.javaPackage, false))
        }
        if (file.options.hasJavaOuterClassname()) {
            options.add(
                OptionElement(JAVA_OUTER_CLASSNAME_OPTION, stringKind, file.options.javaOuterClassname, false),
            )
        }
        if (file.options.hasJavaMultipleFiles()) {
            options.add(
                OptionElement(JAVA_MULTIPLE_FILES_OPTION, booleanKind, file.options.javaMultipleFiles, false),
            )
        }
        if (file.options.hasJavaGenericServices()) {
            options.add(
                OptionElement(JAVA_GENERIC_SERVICES_OPTION, booleanKind, file.options.javaGenericServices, false),
            )
        }
        if (file.options.hasJavaStringCheckUtf8()) {
            options.add(
                OptionElement(JAVA_STRING_CHECK_UTF8_OPTION, booleanKind, file.options.javaStringCheckUtf8, false),
            )
        }
        if (file.options.hasCcGenericServices()) {
            options.add(
                OptionElement(CC_GENERIC_SERVICES_OPTION, booleanKind, file.options.ccGenericServices, false),
            )
        }
        if (file.options.hasCcEnableArenas()) {
            options.add(OptionElement(CC_ENABLE_ARENAS_OPTION, booleanKind, file.options.ccEnableArenas, false))
        }
        if (file.options.hasCsharpNamespace()) {
            options.add(OptionElement(CSHARP_NAMESPACE_OPTION, stringKind, file.options.csharpNamespace, false))
        }
        if (file.options.hasGoPackage()) {
            options.add(OptionElement(GO_PACKAGE_OPTION, stringKind, file.options.goPackage, false))
        }
        if (file.options.hasObjcClassPrefix()) {
            options.add(OptionElement(OBJC_CLASS_PREFIX_OPTION, stringKind, file.options.objcClassPrefix, false))
        }
        if (file.options.hasPhpClassPrefix()) {
            options.add(OptionElement(PHP_CLASS_PREFIX_OPTION, stringKind, file.options.phpClassPrefix, false))
        }
        if (file.options.hasPhpGenericServices()) {
            options.add(
                OptionElement(PHP_GENERIC_SERVICES_OPTION, booleanKind, file.options.phpGenericServices, false),
            )
        }
        if (file.options.hasPhpMetadataNamespace()) {
            options.add(
                OptionElement(PHP_METADATA_NAMESPACE_OPTION, stringKind, file.options.phpMetadataNamespace, false),
            )
        }
        if (file.options.hasPhpNamespace()) {
            options.add(OptionElement(PHP_NAMESPACE_OPTION, stringKind, file.options.phpNamespace, false))
        }
        if (file.options.hasPyGenericServices()) {
            options.add(
                OptionElement(PY_GENERIC_SERVICES_OPTION, booleanKind, file.options.pyGenericServices, false),
            )
        }
        if (file.options.hasRubyPackage()) {
            options.add(OptionElement(RUBY_PACKAGE_OPTION, stringKind, file.options.rubyPackage, false))
        }
        if (file.options.hasSwiftPrefix()) {
            options.add(OptionElement(SWIFT_PREFIX_OPTION, stringKind, file.options.swiftPrefix, false))
        }
        if (file.options.hasOptimizeFor()) {
            options.add(OptionElement(OPTIMIZE_FOR_OPTION, enumKind, file.options.optimizeFor, false))
        }
        return ProtoFileElement(
            DEFAULT_LOCATION,
            packageName,
            syntax,
            imports.build(),
            publicImports.build(),
            types.build(),
            services.build(),
            Collections.emptyList(),
            options.build(),
        )
    }

    private fun toMessage(
        file: FileDescriptorProto,
        descriptor: DescriptorProto,
    ): MessageElement {
        val name = descriptor.name
        val fields = ImmutableList.builder<FieldElement>()
        val nested = ImmutableList.builder<TypeElement>()
        val reserved = ImmutableList.builder<ReservedElement>()
        val extensions = ImmutableList.builder<ExtensionsElement>()
        val oneofsMap = LinkedHashMap<String, ImmutableList.Builder<FieldElement>>()
        for (od in descriptor.oneofDeclList) {
            oneofsMap[od.name] = ImmutableList.builder()
        }
        val oneofs = ArrayList(oneofsMap.entries)
        val proto3OptionalFields: MutableList<FieldElement> = ArrayList()
        for (fd in descriptor.fieldList) {
            if (fd.hasProto3Optional()) {
                proto3OptionalFields.add(toField(file, fd, false))
                continue
            }
            if (fd.hasOneofIndex()) {
                oneofs[fd.oneofIndex].value.add(toField(file, fd, true))
            } else {
                fields.add(toField(file, fd, false))
            }
        }
        fields.addAll(proto3OptionalFields)
        for (nestedDesc in descriptor.nestedTypeList) {
            nested.add(toMessage(file, nestedDesc))
        }
        for (nestedDesc in descriptor.enumTypeList) {
            nested.add(toEnum(nestedDesc))
        }
        for (reservedName in descriptor.reservedNameList) {
            reserved.add(
                ReservedElement(DEFAULT_LOCATION, "", Collections.singletonList(reservedName)),
            )
        }
        for (reservedRange in descriptor.reservedRangeList) {
            val values: MutableList<IntRange> = ArrayList()
            val start = reservedRange.start
            val end = reservedRange.end - 1
            values.add(IntRange(start, end))
            reserved.add(ReservedElement(DEFAULT_LOCATION, "", values))
        }
        for (extensionRange in descriptor.extensionRangeList) {
            val values: MutableList<IntRange> = ArrayList()
            val start = extensionRange.start
            val end = extensionRange.end - 1
            values.add(IntRange(start, end))
            extensions.add(
                ExtensionsElement(DEFAULT_LOCATION, "", values, Collections.emptyList()),
            )
        }
        val options = ImmutableList.builder<OptionElement>()
        if (descriptor.options.hasMapEntry()) {
            options.add(OptionElement(MAP_ENTRY_OPTION, booleanKind, descriptor.options.mapEntry, false))
        }
        if (descriptor.options.hasNoStandardDescriptorAccessor()) {
            options.add(
                OptionElement(
                    NO_STANDARD_DESCRIPTOR_OPTION,
                    booleanKind,
                    descriptor.options.noStandardDescriptorAccessor,
                    false,
                ),
            )
        }
        return MessageElement(
            DEFAULT_LOCATION,
            name,
            "",
            nested.build(),
            options.build(),
            reserved.build(),
            fields.build(),
            oneofs
                // Ignore oneOfs with no fields (like Proto3 Optional)
                .filter { e -> e.value.build().size != 0 }
                .map { e -> toOneof(e.key, e.value) },
            extensions.build(),
            Collections.emptyList(),
            Collections.emptyList(),
        )
    }

    private fun toOneof(
        name: String,
        fields: ImmutableList.Builder<FieldElement>,
    ): OneOfElement = OneOfElement(
        name,
        "",
        fields.build(),
        Collections.emptyList(),
        Collections.emptyList(),
        DEFAULT_LOCATION,
    )

    private fun toEnum(ed: EnumDescriptorProto): EnumElement {
        val name = ed.name
        val constants = ImmutableList.builder<EnumConstantElement>()
        val reserved = ImmutableList.builder<ReservedElement>()
        for (ev in ed.valueList) {
            val options = ImmutableList.builder<OptionElement>()
            constants.add(
                EnumConstantElement(DEFAULT_LOCATION, ev.name, ev.number, "", options.build()),
            )
        }
        for (reservedName in ed.reservedNameList) {
            reserved.add(
                ReservedElement(DEFAULT_LOCATION, "", Collections.singletonList(reservedName)),
            )
        }
        val options = ImmutableList.builder<OptionElement>()
        if (ed.options.hasAllowAlias()) {
            options.add(OptionElement(ALLOW_ALIAS_OPTION, booleanKind, ed.options.allowAlias, false))
        }
        return EnumElement(DEFAULT_LOCATION, name, "", options.build(), constants.build(), reserved.build())
    }

    private fun toService(sv: ServiceDescriptorProto): ServiceElement {
        val name = sv.name
        val rpcs = ImmutableList.builder<RpcElement>()
        for (md in sv.methodList) {
            rpcs.add(
                RpcElement(
                    DEFAULT_LOCATION,
                    md.name,
                    "",
                    md.inputType,
                    md.outputType,
                    md.clientStreaming,
                    md.serverStreaming,
                    getMethodOptionList(md.options),
                ),
            )
        }

        return ServiceElement(
            DEFAULT_LOCATION,
            name,
            "",
            rpcs.build(),
            getOptionList(sv.options.hasDeprecated(), sv.options.deprecated),
        )
    }

    private fun toField(
        file: FileDescriptorProto,
        fd: FieldDescriptorProto,
        inOneof: Boolean,
    ): FieldElement {
        val name = fd.name
        val fieldDescriptorOptions = fd.options
        val options = ImmutableList.builder<OptionElement>()
        if (fieldDescriptorOptions.hasPacked()) {
            options.add(OptionElement(PACKED_OPTION, booleanKind, fd.options.packed, false))
        }
        if (fd.hasJsonName() && fd.jsonName != getDefaultJsonName(name)) {
            options.add(OptionElement(JSON_NAME_OPTION, stringKind, fd.jsonName, false))
        }
        if (fieldDescriptorOptions.hasDeprecated()) {
            options.add(OptionElement(DEPRECATED_OPTION, booleanKind, fieldDescriptorOptions.deprecated, false))
        }
        if (fieldDescriptorOptions.hasCtype()) {
            options.add(OptionElement(CTYPE_OPTION, enumKind, fieldDescriptorOptions.ctype, false))
        }
        if (fieldDescriptorOptions.hasJstype()) {
            options.add(OptionElement(JSTYPE_OPTION, enumKind, fieldDescriptorOptions.jstype, false))
        }
        if (fieldDescriptorOptions.hasExtension(ProtobufSchemaMetadata.metadataKey)) {
            options.add(
                OptionElement(
                    ProtobufSchemaMetadata.metadataKey.descriptor.fullName,
                    stringKind,
                    fieldDescriptorOptions.getExtension(ProtobufSchemaMetadata.metadataKey),
                    false,
                ),
            )
        }
        if (fieldDescriptorOptions.hasExtension(ProtobufSchemaMetadata.metadataValue)) {
            options.add(
                OptionElement(
                    ProtobufSchemaMetadata.metadataValue.descriptor.fullName,
                    stringKind,
                    fieldDescriptorOptions.getExtension(ProtobufSchemaMetadata.metadataValue),
                    false,
                ),
            )
        }

        // Implicitly jsonName to null as Options is already setting it. Setting it here results in duplicate json_name
        // option in inferred schema.
        val jsonName: String? = null
        val defaultValue = if (fd.hasDefaultValue() && fd.defaultValue != null) fd.defaultValue else null
        return FieldElement(
            DEFAULT_LOCATION,
            if (inOneof) null else label(file, fd),
            dataType(fd),
            name,
            defaultValue,
            jsonName,
            fd.number,
            "",
            options.build(),
        )
    }

    private fun label(
        file: FileDescriptorProto,
        fd: FieldDescriptorProto,
    ): Field.Label? {
        val isProto3 = file.syntax == PROTO3
        return when (fd.label) {
            FieldDescriptorProto.Label.LABEL_REQUIRED -> if (isProto3) null else Field.Label.REQUIRED
            FieldDescriptorProto.Label.LABEL_OPTIONAL ->
                // If it's a Proto3 optional, we have to print the optional label.
                if (isProto3 && !fd.hasProto3Optional()) null else Field.Label.OPTIONAL

            FieldDescriptorProto.Label.LABEL_REPEATED -> Field.Label.REPEATED
            else -> throw IllegalArgumentException("Unsupported label")
        }
    }

    private fun dataType(field: FieldDescriptorProto): String = if (field.hasTypeName()) {
        field.typeName
    } else {
        FieldDescriptor.Type.valueOf(field.type).name.lowercase()
    }

    private fun getOptionList(
        hasDeprecated: Boolean,
        deprecated: Boolean,
    ): List<OptionElement> {
        val options = ImmutableList.builder<OptionElement>()
        if (hasDeprecated) {
            options.add(OptionElement(DEPRECATED_OPTION, booleanKind, deprecated, false))
        }

        return options.build()
    }

    private fun getMethodOptionList(methodOptions: MethodOptions): List<OptionElement> {
        val options = ImmutableList.builder<OptionElement>()
        if (methodOptions.hasDeprecated()) {
            options.add(OptionElement(DEPRECATED_OPTION, booleanKind, methodOptions.deprecated, false))
        }
        if (methodOptions.hasIdempotencyLevel()) {
            options.add(OptionElement(IDEMPOTENCY_LEVEL_OPTION, enumKind, methodOptions.idempotencyLevel, false))
        }

        return options.build()
    }

    private fun getTypeName(typeName: String): String = if (typeName.startsWith(".")) typeName else ".$typeName"

    // Default json_name is constructed following lower camel case
    // https://github.com/protocolbuffers/protobuf/blob/3e1967e10be786062ccd026275866c3aef487eba/src/google/protobuf/descriptor.cc#L405
    private fun getDefaultJsonName(fieldName: String): String {
        val parts = fieldName.split("_".toRegex()).dropLastWhile { it.isEmpty() }
        val defaultJsonName = StringBuilder(parts[0])
        for (i in 1 until parts.size) {
            defaultJsonName.append(parts[i].substring(0, 1).uppercase()).append(parts[i].substring(1))
        }
        return defaultJsonName.toString()
    }

    @JvmStatic
    fun toDescriptor(
        name: String,
        protoFileElement: ProtoFileElement,
        dependencies: Map<String, ProtoFileElement>,
    ): Descriptors.Descriptor? = toDynamicSchema(name, protoFileElement, dependencies).getMessageDescriptor(name)

    @JvmStatic
    fun firstMessage(fileElement: ProtoFileElement): MessageElement? {
        for (typeElement in fileElement.types) {
            if (typeElement is MessageElement) {
                return typeElement
            }
        }
        // Intended null return
        return null
    }

    /*
     * DynamicSchema is used as a temporary helper class and should not be exposed in the API.
     */
    private fun toDynamicSchema(
        name: String,
        rootElem: ProtoFileElement,
        dependencies: Map<String, ProtoFileElement>,
    ): DynamicSchema {
        val schema = DynamicSchema.newBuilder()
        try {
            val syntax = rootElem.syntax
            if (syntax != null) {
                schema.setSyntax(syntax.toString())
            }
            if (rootElem.packageName != null) {
                schema.setPackage(rootElem.packageName!!)
            }
            for (typeElem in rootElem.types) {
                if (typeElem is MessageElement) {
                    schema.addMessageDefinition(toDynamicMessage(typeElem))
                } else if (typeElem is EnumElement) {
                    schema.addEnumDefinition(toDynamicEnum(typeElem))
                }
            }
            for (ref in rootElem.imports) {
                val dep = dependencies[ref]
                if (dep != null) {
                    schema.addDependency(ref)
                    schema.addSchema(toDynamicSchema(ref, dep, dependencies))
                }
            }
            for (ref in rootElem.publicImports) {
                val dep = dependencies[ref]
                if (dep != null) {
                    schema.addPublicDependency(ref)
                    schema.addSchema(toDynamicSchema(ref, dep, dependencies))
                }
            }
            val javaPackageName =
                findOption("java_package", rootElem.options).map { o -> o.value.toString() }.orElse(null)
            if (javaPackageName != null) {
                schema.setJavaPackage(javaPackageName)
            }
            val javaOuterClassname =
                findOption("java_outer_classname", rootElem.options).map { o -> o.value.toString() }.orElse(null)
            if (javaOuterClassname != null) {
                schema.setJavaOuterClassname(javaOuterClassname)
            }
            val javaMultipleFiles =
                findOption("java_multiple_files", rootElem.options)
                    .map { o -> java.lang.Boolean.valueOf(o.value.toString()) }
                    .orElse(null)
            if (javaMultipleFiles != null) {
                schema.setJavaMultipleFiles(javaMultipleFiles)
            }
            schema.setName(name)
            return schema.build()
        } catch (e: DescriptorValidationException) {
            throw IllegalStateException(e)
        }
    }

    private fun toDynamicMessage(messageElem: MessageElement): MessageDefinition {
        val message = MessageDefinition.newBuilder(messageElem.name)
        for (type in messageElem.nestedTypes) {
            if (type is MessageElement) {
                message.addMessageDefinition(toDynamicMessage(type))
            } else if (type is EnumElement) {
                message.addEnumDefinition(toDynamicEnum(type))
            }
        }
        val added: MutableSet<String> = HashSet()
        for (oneof in messageElem.oneOfs) {
            val oneofBuilder = message.addOneof(oneof.name)
            for (field in oneof.fields) {
                val defaultVal = field.defaultValue
                val jsonName =
                    findOption("json_name", field.options).map { o -> o.value.toString() }.orElse(null)
                oneofBuilder.addField(field.type, field.name, field.tag, defaultVal, jsonName)
                added.add(field.name)
            }
        }
        // Process fields after messages so that any newly created map entry messages are at the end
        for (field in messageElem.fields) {
            if (added.contains(field.name)) {
                continue
            }
            val fieldLabel = field.label
            var label = if (fieldLabel != null) fieldLabel.toString().lowercase() else null
            var fieldType = field.type
            val defaultVal = field.defaultValue
            val jsonName = field.jsonName
            val isPacked =
                findOption("packed", field.options)
                    .map { o -> java.lang.Boolean.valueOf(o.value.toString()) }
                    .orElse(null)
            val protoType = ProtoType.get(fieldType)
            val keyType = protoType.keyType
            val valueType = protoType.valueType
            // Map fields are only permitted in messages
            if (protoType.isMap && keyType != null && valueType != null) {
                label = "repeated"
                fieldType = toMapEntry(field.name)
                val mapMessage = MessageDefinition.newBuilder(fieldType)
                mapMessage.setMapEntry(true)
                mapMessage.addField(null, keyType.simpleName, KEY_FIELD, 1, null)
                mapMessage.addField(null, valueType.simpleName, VALUE_FIELD, 2, null)
                message.addMessageDefinition(mapMessage.build())
            }
            message.addField(label, fieldType, field.name, field.tag, defaultVal, jsonName, isPacked)
        }
        for (reserved in messageElem.reserveds) {
            for (elem in reserved.values) {
                if (elem is String) {
                    message.addReservedName(elem)
                } else if (elem is Int) {
                    message.addReservedRange(elem, elem)
                } else if (elem is IntRange) {
                    message.addReservedRange(elem.start, elem.endInclusive)
                } else {
                    throw IllegalStateException("Unsupported reserved type: " + elem.javaClass.name)
                }
            }
        }
        val isMapEntry =
            findOption("map_entry", messageElem.options)
                .map { o -> java.lang.Boolean.valueOf(o.value.toString()) }
                .orElse(null)
        if (isMapEntry != null) {
            message.setMapEntry(isMapEntry)
        }
        return message.build()
    }

    @JvmStatic
    fun findOption(
        name: String,
        options: List<OptionElement>,
    ): Optional<OptionElement> = options.stream().filter { o -> o.name == name }.findFirst()

    private fun toDynamicEnum(enumElem: EnumElement): EnumDefinition {
        val allowAlias =
            findOption("allow_alias", enumElem.options)
                .map { o -> java.lang.Boolean.valueOf(o.value.toString()) }
                .orElse(null)
        val enumer = EnumDefinition.newBuilder(enumElem.name, allowAlias)
        for (constant in enumElem.constants) {
            enumer.addValue(constant.name, constant.tag)
        }
        return enumer.build()
    }

    @JvmStatic
    fun toMapField(s: String): String {
        var name = s
        if (name.endsWith(MAP_ENTRY_SUFFIX)) {
            name = name.substring(0, name.length - MAP_ENTRY_SUFFIX.length)
            name = UPPER_CAMEL.to(LOWER_UNDERSCORE, name)
        }
        return name
    }
}
