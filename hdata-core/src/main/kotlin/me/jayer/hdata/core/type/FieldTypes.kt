package me.jayer.hdata.core.type

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes

/**
 * @author wuya
 * @date 2022-08-24
 */
object FieldTypes {
    val STRING: Schema.FieldType = Schema.FieldType.STRING
    val BYTE: Schema.FieldType = Schema.FieldType.BYTE
    val BYTES: Schema.FieldType = Schema.FieldType.BYTES
    val INT16: Schema.FieldType = Schema.FieldType.INT16
    val INT32: Schema.FieldType = Schema.FieldType.INT32
    val INT64: Schema.FieldType = Schema.FieldType.INT64
    val FLOAT: Schema.FieldType = Schema.FieldType.FLOAT
    val DOUBLE: Schema.FieldType = Schema.FieldType.DOUBLE
    val DECIMAL: Schema.FieldType = Schema.FieldType.DECIMAL
    val BOOLEAN: Schema.FieldType = Schema.FieldType.BOOLEAN

    // logicalTypes
    val DATE: Schema.FieldType = Schema.FieldType.logicalType(SqlTypes.DATE)
    val TIME: Schema.FieldType = Schema.FieldType.logicalType(SqlTypes.TIME)
    val DATETIME: Schema.FieldType = Schema.FieldType.logicalType(SqlTypes.DATETIME)
    val TIMESTAMP: Schema.FieldType = Schema.FieldType.logicalType(SqlTypes.TIMESTAMP)
}