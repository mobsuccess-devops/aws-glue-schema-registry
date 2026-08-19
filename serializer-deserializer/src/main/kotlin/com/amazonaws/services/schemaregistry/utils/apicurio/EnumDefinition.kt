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

class EnumDefinition private constructor(
    private val mEnumType: DescriptorProtos.EnumDescriptorProto,
) {
    // --- public ---

    override fun toString(): String = mEnumType.toString()

    // --- package ---

    internal fun getEnumType(): DescriptorProtos.EnumDescriptorProto = mEnumType

    /**
     * EnumDefinition.Builder
     */
    class Builder internal constructor(
        enumName: String,
        allowAlias: Boolean?,
    ) {
        private val mEnumTypeBuilder: DescriptorProtos.EnumDescriptorProto.Builder =
            DescriptorProtos.EnumDescriptorProto.newBuilder()

        init {
            mEnumTypeBuilder.setName(enumName)
            if (allowAlias != null) {
                val optionsBuilder = DescriptorProtos.EnumOptions.newBuilder()
                optionsBuilder.setAllowAlias(allowAlias)
                mEnumTypeBuilder.mergeOptions(optionsBuilder.build())
            }
        }

        // --- public ---

        fun addValue(
            name: String,
            num: Int,
        ): Builder {
            val enumValBuilder = DescriptorProtos.EnumValueDescriptorProto.newBuilder()
            enumValBuilder.setName(name).setNumber(num)
            mEnumTypeBuilder.addValue(enumValBuilder.build())
            return this
        }

        fun build(): EnumDefinition = EnumDefinition(mEnumTypeBuilder.build())
    }

    companion object {
        // --- public static ---

        @JvmStatic
        fun newBuilder(enumName: String): Builder = newBuilder(enumName, null)

        @JvmStatic
        fun newBuilder(
            enumName: String,
            allowAlias: Boolean?,
        ): Builder = Builder(enumName, allowAlias)
    }
}
