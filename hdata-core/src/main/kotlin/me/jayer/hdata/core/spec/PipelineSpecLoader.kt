package me.jayer.hdata.core.spec

import me.jayer.hdata.core.exception.HDataException
import tools.jackson.databind.ObjectMapper
import java.io.File

/**
 * 读取并解析 pipeline 文件。
 *
 * 解析前会做一遍 `${VAR}` / `${VAR:-default}` 变量替换，用于把密码之类的敏感信息
 * 挪出配置文件。查找顺序为：显式传入的变量 -> JVM system property -> 环境变量。
 *
 * 变量名必须匹配 `[A-Za-z_][A-Za-z0-9_.]*`，因此不会误伤 JDBC 连接器的表名区间语法 `${0-9}`。
 *
 * @author wuya
 * @date 2022-08-30
 */
object PipelineSpecLoader {

    private val VARIABLE = Regex("""\$\{([A-Za-z_][A-Za-z0-9_.]*)(?::-([^}]*))?}""")

    fun load(file: File, variables: Map<String, String> = emptyMap()): PipelineSpec {
        if (!file.isFile) {
            throw HDataException("pipeline 文件不存在: ${file.absolutePath}")
        }
        return parse(file.readText(), SpecMappers.forFile(file), file.path, variables)
    }

    fun parse(
        text: String,
        mapper: ObjectMapper,
        source: String,
        variables: Map<String, String> = emptyMap(),
    ): PipelineSpec {
        val interpolated = interpolate(text, source, variables)
        val spec = try {
            mapper.readValue(interpolated, PipelineSpec::class.java)
        } catch (e: Exception) {
            throw HDataException("解析 pipeline 文件失败[$source]: ${e.message}", e)
        }
        validate(spec.pipeline, source)
        return spec
    }

    private fun interpolate(text: String, source: String, variables: Map<String, String>): String =
        VARIABLE.replace(text) { match ->
            val name = match.groupValues[1]
            val default = match.groups[2]?.value
            variables[name]
                ?: System.getProperty(name)
                ?: System.getenv(name)
                ?: default
                ?: throw HDataException("pipeline 文件[$source] 引用了未定义的变量 \${$name}，可用 -D$name=... 或环境变量提供，或写成 \${$name:-默认值}")
        }

    private fun validate(spec: TransformSpec, source: String) {
        if (!spec.composite) {
            throw HDataException("pipeline 文件[$source] 的顶层 pipeline 必须是 chain 或 composite，实际为: ${spec.kind}")
        }
        if (spec.children().isEmpty()) {
            throw HDataException("pipeline 文件[$source] 没有声明任何 transform")
        }
        validateRecursively(spec)
    }

    private fun validateRecursively(spec: TransformSpec) {
        if (spec.composite) {
            if (spec.chain) {
                spec.children().drop(1).forEach { child ->
                    if (child.inputRefs().isNotEmpty()) {
                        throw HDataException("chain[${spec.displayName}] 内的 transform[${child.displayName}] 不能声明 input，输入由上一个节点隐式提供；如需引用其他节点请改用 composite")
                    }
                }
            }
            (spec.children() + spec.extraTransforms).forEach { validateRecursively(it) }
        } else {
            // 触发 kind 计算，尽早暴露缺少 type 的节点
            spec.kind
            if (spec.output != null) {
                throw HDataException("transform[${spec.displayName}] 不是复合节点，不能声明 output；引用它的输出请写 ${spec.displayName}.<tag>")
            }
        }
    }
}
