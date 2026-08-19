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

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.util.TreeSet

class DynamicSchema private constructor(
    private val mFileDescSet: DescriptorProtos.FileDescriptorSet,
) {
    private val mMsgDescriptorMapFull: MutableMap<String, Descriptors.Descriptor> = HashMap()
    private val mMsgDescriptorMapShort: MutableMap<String, Descriptors.Descriptor> = HashMap()
    private val mEnumDescriptorMapFull: MutableMap<String, Descriptors.EnumDescriptor> = HashMap()
    private val mEnumDescriptorMapShort: MutableMap<String, Descriptors.EnumDescriptor> = HashMap()

    init {
        val fileDescMap = init(mFileDescSet)

        val msgDupes: MutableSet<String> = HashSet()
        val enumDupes: MutableSet<String> = HashSet()
        for (fileDesc in fileDescMap.values) {
            for (msgType in fileDesc.messageTypes) {
                addMessageType(msgType, null, msgDupes, enumDupes)
            }
            for (enumType in fileDesc.enumTypes) {
                addEnumType(enumType, null, enumDupes)
            }
        }

        for (msgName in msgDupes) {
            mMsgDescriptorMapShort.remove(msgName)
        }
        for (enumName in enumDupes) {
            mEnumDescriptorMapShort.remove(enumName)
        }
    }

    // --- public ---

    /**
     * Gets the protobuf file descriptor proto
     *
     * @return the file descriptor proto
     */
    fun getFileDescriptorProto(): DescriptorProtos.FileDescriptorProto = mFileDescSet.getFile(0)

    /**
     * Creates a new dynamic message builder for the given message type
     *
     * @param msgTypeName the message type name
     * @return the message builder (null if not found)
     */
    fun newMessageBuilder(msgTypeName: String): DynamicMessage.Builder? {
        val msgType = getMessageDescriptor(msgTypeName) ?: return null
        return DynamicMessage.newBuilder(msgType)
    }

    /**
     * Gets the protobuf message descriptor for the given message type
     *
     * @param msgTypeName the message type name
     * @return the message descriptor (null if not found)
     */
    fun getMessageDescriptor(msgTypeName: String): Descriptors.Descriptor? {
        var msgType = mMsgDescriptorMapShort[msgTypeName]
        if (msgType == null) {
            msgType = mMsgDescriptorMapFull[msgTypeName]
        }
        return msgType
    }

    /**
     * Gets the enum value for the given enum type and name
     *
     * @param enumTypeName the enum type name
     * @param enumName     the enum name
     * @return the enum value descriptor (null if not found)
     */
    fun getEnumValue(
        enumTypeName: String,
        enumName: String,
    ): Descriptors.EnumValueDescriptor? {
        val enumType = getEnumDescriptor(enumTypeName) ?: return null
        return enumType.findValueByName(enumName)
    }

    /**
     * Gets the enum value for the given enum type and number
     *
     * @param enumTypeName the enum type name
     * @param enumNumber   the enum number
     * @return the enum value descriptor (null if not found)
     */
    fun getEnumValue(
        enumTypeName: String,
        enumNumber: Int,
    ): Descriptors.EnumValueDescriptor? {
        val enumType = getEnumDescriptor(enumTypeName) ?: return null
        return enumType.findValueByNumber(enumNumber)
    }

    /**
     * Gets the protobuf enum descriptor for the given enum type
     *
     * @param enumTypeName the enum type name
     * @return the enum descriptor (null if not found)
     */
    fun getEnumDescriptor(enumTypeName: String): Descriptors.EnumDescriptor? {
        var enumType = mEnumDescriptorMapShort[enumTypeName]
        if (enumType == null) {
            enumType = mEnumDescriptorMapFull[enumTypeName]
        }
        return enumType
    }

    /**
     * Returns the message types registered with the schema
     *
     * @return the set of message type names
     */
    fun getMessageTypes(): Set<String> = TreeSet(mMsgDescriptorMapFull.keys)

    /**
     * Returns the enum types registered with the schema
     *
     * @return the set of enum type names
     */
    fun getEnumTypes(): Set<String> = TreeSet(mEnumDescriptorMapFull.keys)

    /**
     * Serializes the schema
     *
     * @return the serialized schema descriptor
     */
    fun toByteArray(): ByteArray = mFileDescSet.toByteArray()

    /**
     * Returns a string representation of the schema
     *
     * @return the schema string
     */
    override fun toString(): String {
        val msgTypes = getMessageTypes()
        val enumTypes = getEnumTypes()
        return "types: " + msgTypes + "\nenums: " + enumTypes + "\n" + mFileDescSet
    }

    // --- private ---

    @Throws(Descriptors.DescriptorValidationException::class)
    private fun init(fileDescSet: DescriptorProtos.FileDescriptorSet): Map<String, Descriptors.FileDescriptor> {
        // check for dupes
        val allFdProtoNames: MutableSet<String> = HashSet()
        for (fdProto in fileDescSet.fileList) {
            require(!allFdProtoNames.contains(fdProto.name)) { "duplicate name: " + fdProto.name }
            allFdProtoNames.add(fdProto.name)
        }

        // build FileDescriptors, resolve dependencies (imports) if any
        val resolvedFileDescMap: MutableMap<String, Descriptors.FileDescriptor> = HashMap()
        while (resolvedFileDescMap.size < fileDescSet.fileCount) {
            for (fdProto in fileDescSet.fileList) {
                if (resolvedFileDescMap.containsKey(fdProto.name)) {
                    continue
                }

                val dependencyList: List<String> = fdProto.dependencyList

                val resolvedFdList: MutableList<Descriptors.FileDescriptor> = ArrayList()
                for (depName in dependencyList) {
                    require(allFdProtoNames.contains(depName)) {
                        "cannot resolve import " + depName + " in " + fdProto.name
                    }
                    val fd = resolvedFileDescMap[depName]
                    if (fd != null) {
                        resolvedFdList.add(fd)
                    }
                }

                if (resolvedFdList.size == dependencyList.size) { // dependencies resolved
                    val fd = Descriptors.FileDescriptor.buildFrom(fdProto, resolvedFdList.toTypedArray())
                    resolvedFileDescMap[fdProto.name] = fd
                }
            }
        }

        return resolvedFileDescMap
    }

    private fun addMessageType(
        msgType: Descriptors.Descriptor,
        scope: String?,
        msgDupes: MutableSet<String>,
        enumDupes: MutableSet<String>,
    ) {
        val msgTypeNameFull = msgType.fullName
        val msgTypeNameShort = if (scope == null) msgType.name else scope + "." + msgType.name

        require(!mMsgDescriptorMapFull.containsKey(msgTypeNameFull)) { "duplicate name: $msgTypeNameFull" }
        if (mMsgDescriptorMapShort.containsKey(msgTypeNameShort)) {
            msgDupes.add(msgTypeNameShort)
        }

        mMsgDescriptorMapFull[msgTypeNameFull] = msgType
        mMsgDescriptorMapShort[msgTypeNameShort] = msgType

        for (nestedType in msgType.nestedTypes) {
            addMessageType(nestedType, msgTypeNameShort, msgDupes, enumDupes)
        }
        for (enumType in msgType.enumTypes) {
            addEnumType(enumType, msgTypeNameShort, enumDupes)
        }
    }

    private fun addEnumType(
        enumType: Descriptors.EnumDescriptor,
        scope: String?,
        enumDupes: MutableSet<String>,
    ) {
        val enumTypeNameFull = enumType.fullName
        val enumTypeNameShort = if (scope == null) enumType.name else scope + "." + enumType.name

        require(!mEnumDescriptorMapFull.containsKey(enumTypeNameFull)) { "duplicate name: $enumTypeNameFull" }
        if (mEnumDescriptorMapShort.containsKey(enumTypeNameShort)) {
            enumDupes.add(enumTypeNameShort)
        }

        mEnumDescriptorMapFull[enumTypeNameFull] = enumType
        mEnumDescriptorMapShort[enumTypeNameShort] = enumType
    }

    /**
     * DynamicSchema.Builder
     */
    class Builder internal constructor() {
        // --- private ---
        private val mFileDescProtoBuilder: DescriptorProtos.FileDescriptorProto.Builder =
            DescriptorProtos.FileDescriptorProto.newBuilder()
        private val mFileDescSetBuilder: DescriptorProtos.FileDescriptorSet.Builder =
            DescriptorProtos.FileDescriptorSet.newBuilder()

        // --- public ---

        /**
         * Builds a dynamic schema
         *
         * @return the schema object
         */
        @Throws(Descriptors.DescriptorValidationException::class)
        fun build(): DynamicSchema {
            val fileDescSetBuilder = DescriptorProtos.FileDescriptorSet.newBuilder()
            fileDescSetBuilder.addFile(mFileDescProtoBuilder.build())
            fileDescSetBuilder.mergeFrom(mFileDescSetBuilder.build())
            return DynamicSchema(fileDescSetBuilder.build())
        }

        fun setSyntax(syntax: String): Builder {
            mFileDescProtoBuilder.setSyntax(syntax)
            return this
        }

        fun setName(name: String): Builder {
            mFileDescProtoBuilder.setName(name)
            return this
        }

        fun setPackage(name: String): Builder {
            mFileDescProtoBuilder.setPackage(name)
            return this
        }

        fun addMessageDefinition(msgDef: MessageDefinition): Builder {
            mFileDescProtoBuilder.addMessageType(msgDef.getMessageType())
            return this
        }

        fun addEnumDefinition(enumDef: EnumDefinition): Builder {
            mFileDescProtoBuilder.addEnumType(enumDef.getEnumType())
            return this
        }

        // Note: added
        fun addDependency(dependency: String): Builder {
            mFileDescProtoBuilder.addDependency(dependency)
            return this
        }

        // Note: added
        fun addPublicDependency(dependency: String): Builder {
            for (i in 0 until mFileDescProtoBuilder.dependencyCount) {
                if (mFileDescProtoBuilder.getDependency(i) == dependency) {
                    mFileDescProtoBuilder.addPublicDependency(i)
                    return this
                }
            }
            mFileDescProtoBuilder.addDependency(dependency)
            mFileDescProtoBuilder.addPublicDependency(mFileDescProtoBuilder.dependencyCount - 1)
            return this
        }

        // Note: added
        fun setJavaPackage(javaPackage: String): Builder {
            val optionsBuilder = DescriptorProtos.FileOptions.newBuilder()
            optionsBuilder.setJavaPackage(javaPackage)
            mFileDescProtoBuilder.mergeOptions(optionsBuilder.build())
            return this
        }

        // Note: added
        fun setJavaOuterClassname(javaOuterClassname: String): Builder {
            val optionsBuilder = DescriptorProtos.FileOptions.newBuilder()
            optionsBuilder.setJavaOuterClassname(javaOuterClassname)
            mFileDescProtoBuilder.mergeOptions(optionsBuilder.build())
            return this
        }

        // Note: added
        fun setJavaMultipleFiles(javaMultipleFiles: Boolean): Builder {
            val optionsBuilder = DescriptorProtos.FileOptions.newBuilder()
            optionsBuilder.setJavaMultipleFiles(javaMultipleFiles)
            mFileDescProtoBuilder.mergeOptions(optionsBuilder.build())
            return this
        }

        // Note: changed
        fun addSchema(schema: DynamicSchema): Builder {
            for (file in schema.mFileDescSet.fileList) {
                if (!contains(file)) {
                    mFileDescSetBuilder.addFile(file)
                }
            }
            return this
        }

        // Note: added
        private fun contains(fileDesc: DescriptorProtos.FileDescriptorProto): Boolean {
            val files = mFileDescSetBuilder.fileList
            for (file in files) {
                if (file.name == fileDesc.name) {
                    return true
                }
            }
            return false
        }
    }

    companion object {
        // --- public static ---

        /**
         * Creates a new dynamic schema builder
         *
         * @return the schema builder
         */
        @JvmStatic
        fun newBuilder(): Builder = Builder()

        /**
         * Parses a serialized schema descriptor (from input stream; closes the stream)
         *
         * @param schemaDescIn the descriptor input stream
         * @return the schema object
         */
        @JvmStatic
        @Throws(Descriptors.DescriptorValidationException::class, IOException::class)
        fun parseFrom(schemaDescIn: InputStream): DynamicSchema {
            try {
                var len: Int
                val buf = ByteArray(4096)
                val baos = ByteArrayOutputStream()
                while ((schemaDescIn.read(buf).also { len = it }) > 0) {
                    baos.write(buf, 0, len)
                }
                return parseFrom(baos.toByteArray())
            } finally {
                schemaDescIn.close()
            }
        }

        /**
         * Parses a serialized schema descriptor (from byte array)
         *
         * @param schemaDescBuf the descriptor byte array
         * @return the schema object
         */
        @JvmStatic
        @Throws(Descriptors.DescriptorValidationException::class, IOException::class)
        fun parseFrom(schemaDescBuf: ByteArray): DynamicSchema = DynamicSchema(DescriptorProtos.FileDescriptorSet.parseFrom(schemaDescBuf))
    }
}
