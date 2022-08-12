package io.jayer.hdata.jdbc

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.schemas.logicaltypes.UuidLogicalType
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.sql.Types
import java.time.LocalDateTime
import java.util.*

/**
 * @author wuya
 * @date 2022-08-12
 */
object SchemaConverter {

    fun convertToBeamField(column: Column): Schema.Field {
        var fieldType = when (Class.forName(column.className).kotlin) {
            Boolean::class -> if (column.type == Types.BIT && column.precision > 1) {
                // https://bugs.mysql.com/bug.php?id=100722
                Schema.FieldType.BYTES
            } else {
                Schema.FieldType.BOOLEAN
            }

            Byte::class -> Schema.FieldType.BYTE
            ByteArray::class -> Schema.FieldType.BYTES
            Short::class -> Schema.FieldType.INT16
            Int::class -> Schema.FieldType.INT32
            Long::class -> Schema.FieldType.INT64
            Float::class -> Schema.FieldType.FLOAT
            Double::class -> Schema.FieldType.DOUBLE
            String::class -> Schema.FieldType.STRING
            BigInteger::class, BigDecimal::class -> Schema.FieldType.DECIMAL
            Date::class, Time::class, Timestamp::class, LocalDateTime::class -> Schema.FieldType.DATETIME
            UUID::class -> Schema.FieldType.logicalType(UuidLogicalType())
            else -> if (column.type == Types.JAVA_OBJECT) {
                Schema.FieldType.STRING
            } else {
                throw UnsupportedOperationException("Convert ${column.typeName} to Beam schema type is not supported")
            }
        }

        if (column.type == Types.ARRAY) {
            fieldType = Schema.FieldType.array(fieldType)
        }

        return Schema.Field.of(column.label, fieldType).withNullable(column.nullable)
    }

    fun convertToBeamSchema(columns: List<Column>): Schema {
        return Schema.builder().addFields(columns.map { convertToBeamField(it) }).build()
    }
}