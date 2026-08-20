package com.amazonaws.services.schemaregistry.utils.external

import com.amazonaws.services.schemaregistry.common.AWSSchemaNamingStrategy

class CustomNamingStrategy : AWSSchemaNamingStrategy {
    /**
     * Returns the schemaName.
     *
     * @param transportName topic Name or stream name etc.
     * @return schema name.
     */
    override fun getSchemaName(transportName: String?): String? = transportName
}
