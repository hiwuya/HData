package me.jayer.hdata.cassandra.internal

import com.datastax.oss.driver.api.core.type.DataTypes
import com.datastax.oss.driver.api.core.type.ListType
import com.datastax.oss.driver.api.core.type.MapType
import com.datastax.oss.driver.api.core.type.SetType
import com.datastax.oss.driver.api.core.type.TupleType
import org.apache.beam.sdk.schemas.Schema

/**
 * Maps Cassandra CQL types to Beam schema types.
 *
 * @author wuya
 */
internal object CassandraTypeMappings {

    fun toBeamType(cqlType: Any): Schema.FieldType {
        // Handle collection types first.
        if (cqlType is ListType) {
            return Schema.FieldType.array(toBeamType(cqlType.elementType))
        }
        if (cqlType is SetType) {
            return Schema.FieldType.array(toBeamType(cqlType.elementType))
        }
        if (cqlType is MapType) {
            return Schema.FieldType.map(toBeamType(cqlType.keyType), toBeamType(cqlType.valueType))
        }
        if (cqlType is TupleType) {
            return if (cqlType.componentTypes.isNotEmpty()) {
                Schema.FieldType.array(toBeamType(cqlType.componentTypes[0]))
            } else {
                Schema.FieldType.array(Schema.FieldType.STRING)
            }
        }

        // Scalar types.
        return when (cqlType) {
            DataTypes.TEXT, DataTypes.ASCII -> Schema.FieldType.STRING
            DataTypes.INT -> Schema.FieldType.INT32
            DataTypes.BIGINT, DataTypes.COUNTER -> Schema.FieldType.INT64
            DataTypes.SMALLINT -> Schema.FieldType.INT16
            DataTypes.TINYINT -> Schema.FieldType.BYTE
            DataTypes.FLOAT -> Schema.FieldType.FLOAT
            DataTypes.DOUBLE -> Schema.FieldType.DOUBLE
            DataTypes.BOOLEAN -> Schema.FieldType.BOOLEAN
            DataTypes.DECIMAL -> Schema.FieldType.DECIMAL
            DataTypes.VARINT -> Schema.FieldType.DECIMAL
            DataTypes.DATE -> Schema.FieldType.DATETIME
            DataTypes.TIMESTAMP -> Schema.FieldType.DATETIME
            DataTypes.TIME -> Schema.FieldType.STRING
            DataTypes.TIMEUUID, DataTypes.UUID -> Schema.FieldType.STRING
            DataTypes.BLOB -> Schema.FieldType.BYTES
            DataTypes.INET -> Schema.FieldType.STRING
            DataTypes.DURATION -> Schema.FieldType.STRING
            else -> Schema.FieldType.STRING
        }
    }
}
