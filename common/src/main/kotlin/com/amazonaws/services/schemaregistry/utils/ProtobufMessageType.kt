package com.amazonaws.services.schemaregistry.utils

import org.apache.commons.lang3.StringUtils

/**
 *  Defines a set of supported Data Formats for Protobuf Messages
 */
enum class ProtobufMessageType(
    private val messageTypeName: String,
    val value: Int,
) {
    /**
     * Unknown
     */
    UNKNOWN("UNKNOWN", 0),

    /**
     * Pojo object type
     */
    POJO("POJO", 1),

    /**
     * DynamicMessage object type
     */
    DYNAMIC_MESSAGE("DYNAMIC_MESSAGE", 2),
    ;

    fun getName(): String = messageTypeName

    companion object {
        @JvmStatic
        fun fromName(name: String?): ProtobufMessageType {
            val resolved = if (!StringUtils.isEmpty(name)) name!!.uppercase() else name
            return valueOf(resolved!!)
        }
    }
}
