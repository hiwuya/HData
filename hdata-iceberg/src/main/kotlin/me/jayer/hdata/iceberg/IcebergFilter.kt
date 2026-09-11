package me.jayer.hdata.iceberg

import org.apache.iceberg.expressions.Expression
import org.apache.iceberg.expressions.Expressions.and
import org.apache.iceberg.expressions.Expressions.equal
import org.apache.iceberg.expressions.Expressions.greaterThan
import org.apache.iceberg.expressions.Expressions.greaterThanOrEqual
import org.apache.iceberg.expressions.Expressions.`in`
import org.apache.iceberg.expressions.Expressions.isNull
import org.apache.iceberg.expressions.Expressions.lessThan
import org.apache.iceberg.expressions.Expressions.lessThanOrEqual
import org.apache.iceberg.expressions.Expressions.notEqual
import org.apache.iceberg.expressions.Expressions.notNull
import org.apache.iceberg.expressions.Expressions.or

/**
 * Parses a SQL-like WHERE string into an Iceberg [Expression] for the read side to do predicate push-down.
 *
 * Supported syntax (case-insensitive):
 * - Comparison: `col = v` / `!=` or `<>` / `>` / `>=` / `<` / `<=`
 * - Set: `col IN (v1, v2, ...)`
 * - Null: `col IS NULL` / `col IS NOT NULL`
 * - Combination: `AND` / `OR`, groupable with `()`
 * - Value: quoted string `'...'`, integer, decimal, `true` / `false` / `null`
 *
 * On parse failure it throws [IllegalArgumentException], so the user discovers a malformed filter at
 * graph-construction time rather than silently reading everything at runtime.
 */
object IcebergFilter {
    fun parse(text: String): Expression = Parser(tokenize(text)).parse()
}

private class Parser(private val tokens: List<Token>) {
    private var pos = 0

    fun parse(): Expression {
        val result = parseOr()
        if (pos != tokens.size) throw IllegalArgumentException("Unparseable filter fragment: ${tokens.drop(pos)}")
        return result
    }

    private fun peek(): Token? = tokens.getOrNull(pos)
    private fun next(): Token {
        if (pos >= tokens.size) throw IllegalArgumentException("Incomplete filter, missing operand")
        return tokens[pos++]
    }

    private fun parseOr(): Expression {
        var left = parseAnd()
        while (peek() == Token.Kw("OR")) {
            next()
            left = or(left, parseAnd())
        }
        return left
    }

    private fun parseAnd(): Expression {
        var left = parsePrimary()
        while (peek() == Token.Kw("AND")) {
            next()
            left = and(left, parsePrimary())
        }
        return left
    }

    private fun parsePrimary(): Expression {
        val t = peek()
        if (t == Token.LParen) {
            next()
            val e = parseOr()
            if (peek() != Token.RParen) throw IllegalArgumentException("Missing closing parenthesis")
            next()
            return e
        }
        return parsePredicate()
    }

    private fun parsePredicate(): Expression {
        val col = next()
        if (col !is Token.Ident) throw IllegalArgumentException("Expected a column name, but got $col")
        val op = next()
        return when {
            op == Token.Kw("IS") -> {
                val not = peek() == Token.Kw("NOT")
                if (not) next()
                if (next() != Token.Kw("NULL")) throw IllegalArgumentException("IS must be followed by NULL")
                if (not) notNull<Any>(col.value) else isNull<Any>(col.value)
            }
            op is Token.Op && op.value == "IN" -> {
                if (next() != Token.LParen) throw IllegalArgumentException("IN must be followed by (")
                val values = mutableListOf<Any?>()
                while (true) {
                    values.add(parseValue(next()))
                    if (peek() == Token.Comma) {
                        next()
                        continue
                    }
                    break
                }
                if (next() != Token.RParen) throw IllegalArgumentException("IN is missing a closing parenthesis")
                `in`<Any>(col.value, values)
            }
            op is Token.Op -> buildPredicate(col.value, op.value, parseValue(next()))
            else -> throw IllegalArgumentException("Expected a comparison operator after column ${col.value}, but got $op")
        }
    }

    private fun buildPredicate(col: String, op: String, v: Any?): Expression {
        // A comparison operator needs a comparable value; preserve the value's real type (Long/Double/String/Boolean).
        // We must not blindly cast to Comparable<Any> — otherwise, when Iceberg does metric-based pruning at the
        // manifest level, it cannot obtain the literal's concrete type and degrades to "no pruning", losing the
        // benefit of predicate push-down.
        val c = v as? Comparable<*> ?: throw IllegalArgumentException("Operator $op needs a comparable value, but got $v")
        return when (op) {
            "=" -> equal(col, c)
            "!=" -> notEqual(col, c)
            "<>" -> notEqual(col, c)
            ">" -> greaterThan(col, c)
            ">=" -> greaterThanOrEqual(col, c)
            "<" -> lessThan(col, c)
            "<=" -> lessThanOrEqual(col, c)
            else -> throw IllegalArgumentException("Unsupported operator $op")
        }
    }

    private fun parseValue(t: Token): Any? = when (t) {
        is Token.Str -> t.value
        is Token.Num -> if (t.value.contains('.')) t.value.toDouble() else t.value.toLong()
        is Token.Kw -> when (t.value) {
            "TRUE" -> true
            "FALSE" -> false
            "NULL" -> null
            else -> throw IllegalArgumentException("Unexpected keyword $t")
        }
        else -> throw IllegalArgumentException("Expected a value, but got $t")
    }
}

private sealed class Token {
    data class Ident(val value: String) : Token()
    data class Op(val value: String) : Token()
    data class Str(val value: String) : Token()
    data class Num(val value: String) : Token()
    data class Kw(val value: String) : Token()
    data object LParen : Token()
    data object RParen : Token()
    data object Comma : Token()
}

private fun tokenize(text: String): List<Token> {
    val tokens = mutableListOf<Token>()
    var i = 0
    val n = text.length
    while (i < n) {
        val c = text[i]
        when {
            c.isWhitespace() -> i++
            c == '(' -> { tokens.add(Token.LParen); i++ }
            c == ')' -> { tokens.add(Token.RParen); i++ }
            c == ',' -> { tokens.add(Token.Comma); i++ }
            c == '\'' -> {
                val end = text.indexOf('\'', i + 1)
                if (end < 0) throw IllegalArgumentException("String is missing a closing quote")
                tokens.add(Token.Str(text.substring(i + 1, end)))
                i = end + 1
            }
            c == '=' || c == '!' || c == '<' || c == '>' -> {
                val two = text.substring(i, (i + 2).coerceAtMost(n))
                val op = when {
                    two in setOf("<=", ">=", "<>", "!=") -> two
                    c == '=' -> "="
                    c == '!' -> throw IllegalArgumentException("! must be followed by = or <>")
                    else -> c.toString()
                }
                tokens.add(Token.Op(op))
                i += if (op.length == 2) 2 else 1
            }
                c.isLetter() -> {
                    var j = i
                    while (j < n && (text[j].isLetter() || text[j].isDigit() || text[j] == '_')) j++
                    val raw = text.substring(i, j)
                    val upper = raw.uppercase()
                    // Distinguish keywords (AND/OR/IN/IS/NOT/NULL/TRUE/FALSE) from column names; column names keep
                    // their original case (Iceberg field names are case-sensitive, so `age` written in the filter
                    // must match the table's `age` verbatim).
                    if (upper in setOf("AND", "OR", "IN", "IS", "NOT", "NULL", "TRUE", "FALSE")) {
                        tokens.add(Token.Kw(upper))
                    } else {
                        tokens.add(Token.Ident(raw))
                    }
                    i = j
                }
            c.isDigit() || (c == '-' && i + 1 < n && text[i + 1].isDigit()) -> {
                var j = i
                while (j < n && (text[j].isDigit() || text[j] == '.')) j++
                tokens.add(Token.Num(text.substring(i, j)))
                i = j
            }
            else -> throw IllegalArgumentException("Unrecognized character '$c'")
        }
    }
    return tokens
}

/**
 * Package-level entry point, reused by [IcebergReadConfig.validate] and the read side.
 */
fun parseIcebergFilter(text: String): Expression = IcebergFilter.parse(text)
