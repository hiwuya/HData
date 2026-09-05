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
 * 把类 SQL 的 WHERE 字符串解析成 Iceberg [Expression]，供读取端做谓词下推。
 *
 * 支持的语法（大小写不敏感）：
 * - 比较：`col = v` / `!=` 或 `<>` / `>` / `>=` / `<` / `<=`
 * - 集合：`col IN (v1, v2, ...)`
 * - 空值：`col IS NULL` / `col IS NOT NULL`
 * - 组合：`AND` / `OR`，可用 `()` 分组
 * - 值：引号字符串 `'...'`、整数、小数、`true` / `false` / `null`
 *
 * 解析失败抛 [IllegalArgumentException]，让用户在构图阶段就发现写错的过滤条件，而不是运行时静默全读。
 */
object IcebergFilter {
    fun parse(text: String): Expression = Parser(tokenize(text)).parse()
}

private class Parser(private val tokens: List<Token>) {
    private var pos = 0

    fun parse(): Expression {
        val result = parseOr()
        if (pos != tokens.size) throw IllegalArgumentException("无法解析的过滤条件片段：${tokens.drop(pos)}")
        return result
    }

    private fun peek(): Token? = tokens.getOrNull(pos)
    private fun next(): Token {
        if (pos >= tokens.size) throw IllegalArgumentException("过滤条件不完整，缺少操作数")
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
            if (peek() != Token.RParen) throw IllegalArgumentException("缺少右括号")
            next()
            return e
        }
        return parsePredicate()
    }

    private fun parsePredicate(): Expression {
        val col = next()
        if (col !is Token.Ident) throw IllegalArgumentException("期望列名，但遇到 $col")
        val op = next()
        return when {
            op == Token.Kw("IS") -> {
                val not = peek() == Token.Kw("NOT")
                if (not) next()
                if (next() != Token.Kw("NULL")) throw IllegalArgumentException("IS 之后必须是 NULL")
                if (not) notNull<Any>(col.value) else isNull<Any>(col.value)
            }
            op is Token.Op && op.value == "IN" -> {
                if (next() != Token.LParen) throw IllegalArgumentException("IN 之后必须是 (")
                val values = mutableListOf<Any?>()
                while (true) {
                    values.add(parseValue(next()))
                    if (peek() == Token.Comma) {
                        next()
                        continue
                    }
                    break
                }
                if (next() != Token.RParen) throw IllegalArgumentException("IN 缺少右括号")
                `in`<Any>(col.value, values)
            }
            op is Token.Op -> buildPredicate(col.value, op.value, parseValue(next()))
            else -> throw IllegalArgumentException("列 ${col.value} 之后期望比较操作符，但遇到 $op")
        }
    }

    private fun buildPredicate(col: String, op: String, v: Any?): Expression {
        // 比较操作符需要可比较的值；保留值的真实类型（Long/Double/String/Boolean），
        // 不能统一 cast 成 Comparable<Any>——否则 Iceberg 在 manifest 级做指标裁剪时
        // 拿不到字面量的具体类型，会退化成"不裁剪"，谓词下推的收益就没了。
        val c = v as? Comparable<*> ?: throw IllegalArgumentException("操作符 $op 需要可比较的值，但遇到 $v")
        return when (op) {
            "=" -> equal(col, c)
            "!=" -> notEqual(col, c)
            "<>" -> notEqual(col, c)
            ">" -> greaterThan(col, c)
            ">=" -> greaterThanOrEqual(col, c)
            "<" -> lessThan(col, c)
            "<=" -> lessThanOrEqual(col, c)
            else -> throw IllegalArgumentException("不支持的操作符 $op")
        }
    }

    private fun parseValue(t: Token): Any? = when (t) {
        is Token.Str -> t.value
        is Token.Num -> if (t.value.contains('.')) t.value.toDouble() else t.value.toLong()
        is Token.Kw -> when (t.value) {
            "TRUE" -> true
            "FALSE" -> false
            "NULL" -> null
            else -> throw IllegalArgumentException("非预期的关键字 $t")
        }
        else -> throw IllegalArgumentException("期望值，但遇到 $t")
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
                if (end < 0) throw IllegalArgumentException("字符串缺少闭合引号")
                tokens.add(Token.Str(text.substring(i + 1, end)))
                i = end + 1
            }
            c == '=' || c == '!' || c == '<' || c == '>' -> {
                val two = text.substring(i, (i + 2).coerceAtMost(n))
                val op = when {
                    two in setOf("<=", ">=", "<>", "!=") -> two
                    c == '=' -> "="
                    c == '!' -> throw IllegalArgumentException("! 后面必须是 = 或 <>")
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
                    // 关键字（AND/OR/IN/IS/NOT/NULL/TRUE/FALSE）与列名区分开；列名保留原始大小写
                    // （Iceberg 字段名区分大小写，过滤条件里写的 `age` 必须原样匹配表的 `age`）。
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
            else -> throw IllegalArgumentException("无法识别的字符 '$c'")
        }
    }
    return tokens
}

/**
 * 包级入口，供 [IcebergReadConfig.validate] 与读取端复用。
 */
fun parseIcebergFilter(text: String): Expression = IcebergFilter.parse(text)
