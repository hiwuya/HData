package me.jayer.hdata.core.spec

import me.jayer.hdata.core.exception.HDataException
import tools.jackson.databind.ObjectMapper
import java.io.File

/**
 * Reads and parses the pipeline file.
 *
 * Before parsing, performs `${VAR}` / `${VAR:-default}` variable substitution, used to keep
 * sensitive information such as passwords out of the config file. Lookup order: explicitly
 * passed variables -> JVM system property -> environment variable.
 *
 * Variable names must match `[A-Za-z_][A-Za-z0-9_.]*`, so the JDBC connector's table-name range
 * syntax `${0-9}` is not accidentally matched.
 *
 * @author wuya
 * @date 2022-08-30
 */
object PipelineSpecLoader {

    private val VARIABLE = Regex("""\$\{([A-Za-z_][A-Za-z0-9_.]*)(?::-([^}]*))?}""")

    fun load(file: File, variables: Map<String, String> = emptyMap()): PipelineSpec {
        if (!file.isFile) {
            throw HDataException("pipeline file does not exist: ${file.absolutePath}")
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
            throw HDataException("failed to parse pipeline file [$source]: ${e.message}", e)
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
                ?: throw HDataException("pipeline file [$source] references an undefined variable \${$name}; provide it via -D$name=... or an environment variable, or write it as \${$name:-default}")
        }

    private fun validate(spec: TransformSpec, source: String) {
        if (!spec.composite) {
            throw HDataException("pipeline file [$source]: the top-level pipeline must be a chain or composite, but was: ${spec.kind}")
        }
        if (spec.children().isEmpty()) {
            throw HDataException("pipeline file [$source] declares no transforms")
        }
        validateRecursively(spec)
    }

    private fun validateRecursively(spec: TransformSpec) {
        if (spec.composite) {
            if (spec.chain) {
                spec.children().drop(1).forEach { child ->
                    if (child.inputRefs().isNotEmpty()) {
                        throw HDataException("transform[${child.displayName}] inside chain[${spec.displayName}] cannot declare input; the input is implicitly provided by the previous node; if you need to reference another node, use composite instead")
                    }
                }
            }
            (spec.children() + spec.extraTransforms).forEach { validateRecursively(it) }
        } else {
            // trigger kind computation to surface nodes missing a type early
            spec.kind
            if (spec.output != null) {
                throw HDataException("transform[${spec.displayName}] is not a composite node and cannot declare output; to reference its output, write ${spec.displayName}.<tag>")
            }
        }
    }
}
