package me.jayer.hdata.core.spec

import me.jayer.hdata.core.exception.HDataException

/** Declares whether a pipeline is submitted as a bounded batch or a streaming job. */
data class ExecutionSpec(val mode: String = AUTO) {
    fun resolvedMode(): Mode = when (mode.trim().lowercase()) {
        AUTO -> Mode.AUTO
        BATCH -> Mode.BATCH
        STREAMING -> Mode.STREAMING
        else -> throw HDataException("unsupported execution.mode [$mode]; available: auto / batch / streaming")
    }

    enum class Mode { AUTO, BATCH, STREAMING }

    companion object {
        const val AUTO = "auto"
        const val BATCH = "batch"
        const val STREAMING = "streaming"
    }
}
