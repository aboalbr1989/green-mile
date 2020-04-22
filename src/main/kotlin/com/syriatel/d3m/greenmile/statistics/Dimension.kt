package com.syriatel.d3m.greenmile.statistics

import com.syriatel.d3m.greenmile.domain.Action

data class Dimension(
        val idTemplate: Action.() -> String,
        val criteria: Action.() -> Boolean,
        val metrics: Map<String, Action.() -> Number>
) {
    fun process(action: Action) =
            metrics.mapValues {
                it.value(action).toDouble()
            }.let {
                DimensionStatistics(
                        count = 1,
                        last = action.timeStamp,
                        sum = it,
                        max = it
                )
            }

}

infix fun List<Dimension>.of(action: Action) = filter {
    it.criteria(action)
}.map {
    it.idTemplate(action) to it.process(action)
}.toMap()

data class MetricsHolder(
        val metrics: List<Dimension>
)

class DimensionBuilder {
    val cost = "cost" to { action: Action ->
        (action.cost ?: 0.0).toFloat()
    }

    val duration = "duration" to { action: Action ->
        ((action.map["actualDuration"] ?: 0.0) as Number).toFloat()
    }

    val dataSize = "dataSize" to { action: Action ->
        ((action.map["actualByte"] ?: 0.0) as Number).toFloat()
    }

    infix fun Pair<String, Action.() -> Number>.and(second: Pair<String, Action.() -> Number>) = arrayOf(this, second)
    infix fun Array<Pair<String, Action.() -> Number>>.and(second: Pair<String, Action.() -> Number>) = this + second

    var name: Action.() -> String = { "" }
    var satisfies: Action.() -> Boolean = { true }
    var metrics: Array<Pair<String, Action.() -> Number>> = arrayOf(cost, duration, dataSize)

}

class DimensionsBuilder {
    var dimensions: MutableList<Dimension> = mutableListOf()

    fun dimension(fn: DimensionBuilder.() -> Unit) {
        val b1 = DimensionBuilder().apply(fn)
        dimensions.add(
                Dimension(
                        b1.name,
                        b1.satisfies,
                        mapOf(*b1.metrics)
                )
        )
    }

}


fun dimensions(builder: DimensionsBuilder.() -> Unit): List<Dimension> =
        DimensionsBuilder().apply(builder).dimensions
