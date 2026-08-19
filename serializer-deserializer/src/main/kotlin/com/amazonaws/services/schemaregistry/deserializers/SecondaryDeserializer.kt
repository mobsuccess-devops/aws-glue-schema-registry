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

package com.amazonaws.services.schemaregistry.deserializers

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import java.lang.reflect.InvocationTargetException

// `open`: the test suites mock this type.
open class SecondaryDeserializer private constructor() {
    private var clz: Class<*>? = null
    private var obj: Any? = null

    open fun configure(
        configs: Map<String, *>?,
        isKey: Boolean,
    ) {
        try {
            val secConfigure = clz!!.getMethod("configure", Map::class.java, Boolean::class.javaPrimitiveType)
            secConfigure.invoke(obj, configs, isKey)
        } catch (e: NoSuchMethodException) {
            throw AWSSchemaRegistryException("Can't find method called configure or invoke it.", e)
        } catch (e: InvocationTargetException) {
            throw AWSSchemaRegistryException("Can't find method called configure or invoke it.", e)
        } catch (e: IllegalAccessException) {
            throw AWSSchemaRegistryException("Can't find method called configure or invoke it.", e)
        }
    }

    open fun validate(configs: Map<String, *>): Boolean {
        val secDeserializer = configs[AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER].toString()

        try {
            clz = Class.forName(secDeserializer)
            obj = clz!!.newInstance()
            return clz!!.interfaces.asList().contains(
                Class.forName("org.apache.kafka.common.serialization.Deserializer"),
            )
        } catch (e: ClassNotFoundException) {
            throw AWSSchemaRegistryException("Can't find the class or instantiate it.", e)
        } catch (e: IllegalAccessException) {
            throw AWSSchemaRegistryException("Can't find the class or instantiate it.", e)
        } catch (e: InstantiationException) {
            throw AWSSchemaRegistryException("Can't find the class or instantiate it.", e)
        }
    }

    open fun deserialize(
        topic: String?,
        data: ByteArray?,
    ): Any {
        if (obj == null) {
            throw AWSSchemaRegistryException("Didn't find secondary deserializer.")
        }

        try {
            val secDeserialize = clz!!.getMethod("deserialize", String::class.java, ByteArray::class.java)
            return secDeserialize.invoke(obj, topic, data)
        } catch (e: NoSuchMethodException) {
            throw AWSSchemaRegistryException("Can't find method called deserialize or invoke it.", e)
        } catch (e: InvocationTargetException) {
            throw AWSSchemaRegistryException("Can't find method called deserialize or invoke it.", e)
        } catch (e: IllegalAccessException) {
            throw AWSSchemaRegistryException("Can't find method called deserialize or invoke it.", e)
        }
    }

    open fun close() {
        if (obj == null) {
            throw AWSSchemaRegistryException("Didn't find secondary deserializer.")
        }

        try {
            clz!!.getMethod("close").invoke(obj)
        } catch (e: NoSuchMethodException) {
            throw AWSSchemaRegistryException("Can't find method called close or invoke it.", e)
        } catch (e: InvocationTargetException) {
            throw AWSSchemaRegistryException("Can't find method called close or invoke it.", e)
        } catch (e: IllegalAccessException) {
            throw AWSSchemaRegistryException("Can't find method called close or invoke it.", e)
        }
    }

    companion object {
        @JvmStatic
        fun newInstance(): SecondaryDeserializer = SecondaryDeserializer()
    }
}
