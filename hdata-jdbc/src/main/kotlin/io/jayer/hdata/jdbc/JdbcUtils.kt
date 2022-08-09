package io.jayer.hdata.jdbc

import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.schemas.logicaltypes.UuidLogicalType
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.*
import java.sql.Date
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf

/**
 * @author wuya
 * @date 2022-08-04
 */
object JdbcUtils {

    fun convertToBeamFieldType(metadata: ResultSetMetaData, columnIndex: Int): Schema.FieldType {
        val columnType = metadata.getColumnType(columnIndex)
        val columnTypeName = metadata.getColumnTypeName(columnIndex)
        val columnClass = Class.forName(metadata.getColumnClassName(columnIndex)).kotlin
        val precision = metadata.getPrecision(columnIndex)

        val result = when (columnClass) {
            Boolean::class -> if (columnType == Types.BIT && precision > 1) {
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
            else -> if (columnType == Types.JAVA_OBJECT) {
                Schema.FieldType.STRING
            } else {
                throw UnsupportedOperationException("Convert $columnTypeName to Beam schema type is not supported")
            }
        }

        return if (columnType == Types.ARRAY) Schema.FieldType.array(result) else result
    }

    fun convertToBeamSchema(metadata: ResultSetMetaData): Schema {
        val builder = Schema.builder()
        for (i in 1..metadata.columnCount) {
            val label = metadata.getColumnLabel(i)
            val fieldType = convertToBeamFieldType(metadata, i)
            val isNullable = metadata.isNullable(i) == ResultSetMetaData.columnNullable
            val field = Schema.Field.of(label, fieldType).withNullable(isNullable)
            builder.addField(field)
        }
        return builder.build()
    }

    fun inferBeamSchema(dataSource: DataSource, query: String): Schema {
        return dataSource.connection.use { conn ->
            conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { ps ->
                convertToBeamSchema(ps.metaData)
            }
        }
    }

    private fun getColumnTypes(dataSource: DataSource, table: String): Map<String, KClass<*>> {
        val sql = "SELECT * FROM `${table}` WHERE 1=2"
        return dataSource.connection.use { connection ->
            connection.prepareStatement(sql).use { ps ->
                val metadata = ps.metaData
                IntRange(1, metadata.columnCount)
                    .associate { metadata.getColumnLabel(it) to Class.forName(metadata.getColumnClassName(it)).kotlin }
            }
        }
    }

    fun getFirstNumericPrimaryKey(dataSource: DataSource, table: String): String? {
        val numericColumns = getColumnTypes(dataSource, table).filter { it.value.isSubclassOf(Number::class) }.keys
        return dataSource.connection.use { connection ->
            connection.metaData.getPrimaryKeys(connection.catalog, null, table).use { rs ->
                while (rs.next()) {
                    val key = rs.getString("COLUMN_NAME")
                    if (key in numericColumns) {
                        return key
                    }
                }
            }
            null
        }
    }

    fun queryPartitionRange(
        dataSource: DataSource,
        table: String,
        where: String,
        partitionColumn: String
    ): OffsetRange {
        val result = DoubleArray(2)
         dataSource.connection.use { connection ->
            var sql = "SELECT min(`$partitionColumn`), max(`$partitionColumn`) FROM `$table`"
            if (where.isNotBlank()) {
                sql += " WHERE $where"
            }

            connection.prepareStatement(sql).use { ps ->
                ps.executeQuery().use { rs ->
                    while(rs.next()) {
                        result[0] = rs.getDouble(1)
                        result[1] = rs.getDouble(2) + 1
                    }
                }
            }
        }

         return OffsetRange(result[0].toLong(), result[1].toLong())
    }
}