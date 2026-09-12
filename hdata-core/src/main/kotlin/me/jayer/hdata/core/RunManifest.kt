package me.jayer.hdata.core

import me.jayer.hdata.core.graph.PipelineGraph
import me.jayer.hdata.core.spec.SpecMappers
import org.apache.beam.sdk.PipelineResult
import org.apache.beam.sdk.options.PipelineOptions
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.security.MessageDigest
import java.time.Instant

/** A runner-neutral invocation record that deliberately excludes connector configuration and secrets. */
internal object RunManifest {
    private const val FORMAT_VERSION = 1

    fun write(path: String?, pipelineFile: File, options: PipelineOptions, graph: PipelineGraph, phase: Phase, state: PipelineResult.State? = null) {
        if (path.isNullOrBlank()) return
        val target = File(path).absoluteFile
        target.parentFile?.mkdirs()
        require(target.parentFile?.isDirectory != false) { "Cannot create run manifest directory for $target" }
        val manifest = Manifest(FORMAT_VERSION, phase, Instant.now().toString(), pipelineFile.absolutePath, sha256(pipelineFile),
            options.runner.name, options.jobName,
            options.`as`(org.apache.beam.sdk.options.StreamingOptions::class.java).isStreaming, state?.name,
            graph.nodes.map { node -> Node(node.name, node.type, node.supportTier?.name, node.deliveryCapabilities?.let {
                Delivery(it.deliveryMode.name, it.replayBehavior.name, it.ordering.name, it.requiresIdempotencyKey, it.notes)
            }) }, graph.undeclaredDeliveryCapabilities().map { it.name }, graph.undeclaredSupportTier().map { it.name })
        val temporary = File(target.parentFile ?: File("."), ".${target.name}.tmp-${ProcessHandle.current().pid()}")
        SpecMappers.CONFIG.writerWithDefaultPrettyPrinter().writeValue(temporary, manifest)
        try {
            Files.move(temporary.toPath(), target.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
        } catch (_: java.nio.file.AtomicMoveNotSupportedException) {
            Files.move(temporary.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING)
        }
    }

    private fun sha256(file: File): String = MessageDigest.getInstance("SHA-256").digest(file.readBytes()).joinToString("") { "%02x".format(it) }

    private data class Manifest(val formatVersion: Int, val phase: Phase, val generatedAt: String, val pipelineFile: String,
        val pipelineFingerprintSha256: String, val runner: String?, val jobName: String?, val streaming: Boolean,
        val state: String?, val nodes: List<Node>, val undeclaredDeliveryContracts: List<String>, val undeclaredSupportTiers: List<String>)
    private data class Node(val name: String, val type: String, val supportTier: String?, val delivery: Delivery?)
    private data class Delivery(val mode: String, val replay: String, val ordering: String, val requiresIdempotencyKey: Boolean, val notes: String?)
    enum class Phase { VALIDATED, SUBMITTED, TERMINAL }
}
