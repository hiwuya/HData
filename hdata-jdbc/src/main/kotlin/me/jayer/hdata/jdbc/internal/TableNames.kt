package me.jayer.hdata.jdbc.internal

import java.math.BigInteger

/**
 * Table name utilities.
 *
 * @author wuya
 * @date 2022-08-04
 */
object TableNames {

    private val RANGE = Regex("""\$\{(\d+)-(\d+)}""")
    private val ONE = BigInteger.ONE

    /** Prevents a misconfiguration from expanding into millions of tables at graph construction time and exhausting driver memory. */
    internal const val MAX_RESOLVED_TABLES = 10_000

    /**
     * Expands the table range syntax: `t_order_${'$'}{00-15}` -> `t_order_00` .. `t_order_15`,
     * with the left zero padding width following how the range start is written.
     */
    fun resolve(tables: List<String>): List<String> {
        val resolved = ArrayList<String>(minOf(tables.size, MAX_RESOLVED_TABLES))
        tables.forEach { table ->
            val matches = RANGE.findAll(table).toList()
            require(matches.size <= 1) {
                "each table name can contain at most one table range: $table"
            }
            val match = matches.singleOrNull()
            if (match == null) {
                require(resolved.size < MAX_RESOLVED_TABLES) {
                    "the number of tables after expansion must not exceed $MAX_RESOLVED_TABLES"
                }
                resolved.add(table)
                return@forEach
            }

            val padding = match.groupValues[1].length
            val from = match.groupValues[1].toBigInteger()
            val to = match.groupValues[2].toBigInteger()
            require(from <= to) { "illegal table name range: $table, the start must be <= the end, but from[$from] > to[$to]" }

            val count = to.subtract(from).add(ONE)
            val available = MAX_RESOLVED_TABLES - resolved.size
            require(count <= BigInteger.valueOf(available.toLong())) {
                "the number of tables after expansion must not exceed $MAX_RESOLVED_TABLES: $table"
            }
            repeat(count.toInt()) { offset ->
                val index = from.add(BigInteger.valueOf(offset.toLong()))
                resolved.add(RANGE.replaceFirst(table, index.toString().padStart(padding, '0')))
            }
        }
        return resolved
    }

    /** `db.t_order` -> `db` to `t_order`; the schema is null when there is no qualifier. */
    fun split(table: String): Pair<String?, String> {
        val separator = table.lastIndexOf('.')
        return if (separator < 0) {
            null to table
        } else {
            table.substring(0, separator).trim('`', '"') to table.substring(separator + 1).trim('`', '"')
        }
    }
}
