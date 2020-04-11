package com.syriatel.d3m.greenmile.metrics

import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.utils.windowStore
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.WindowStore

import java.time.*


abstract class ThroughTransformer<K, V> : Transformer<K, V, KeyValue<K, V>> {
    override fun transform(p0: K, p1: V): KeyValue<K, V> {
        process(p0, p1)
        return KeyValue(p0, p1)
    }

    abstract fun process(key: K, value: V)
}


data class Statistics(
        val counts: Map<String, Long> = mapOf(),
        val sums: Map<String, Float> = mapOf(),
        val last: Map<String, LocalDateTime> = mapOf(),
        val max: Map<String, Float> = mapOf()
)


class CustomerStatistics(
        val store: String = "statistics",
        val windowFunction: (LocalDateTime) -> Instant = ::hourlyWindow,
        val multiValue: List<MultiMetricDimension> = listOf()
) : ThroughTransformer<String, Action>() {
    lateinit var ctx: ProcessorContext
    lateinit var statistics: WindowStore<String, Statistics>

    override fun process(key: String, value: Action) {
        val windowStart = windowFunction(value.timeStamp).toEpochMilli()
        val customerStates = statistics.fetch(key, windowStart) ?: Statistics()

        val result = calculateMultiValue(
                action = value,
                oldStatistics = customerStates
        )

        statistics.put(key, result, windowStart)
    }

    fun calculateMultiValue(action: Action, oldStatistics: Statistics) =
            multiValue.filter {
                it.criteria(action)
            }.flatMap { d ->
                d.metrics.map {
                    (d.idTemplate(action) + "#" + it.key) to it.value(action).toFloat()
                }
            }.toMap().let {
                oldStatistics.copy(
                        counts = oldStatistics.counts.incrementCounts(
                                multiValue.filter { d -> d.criteria(action) }.map { d -> d.idTemplate(action) }
                        ),
                        sums = oldStatistics.sums.incrementSum(
                                it
                        ),
                        max = oldStatistics.max.updateMax(
                                it
                        ),
                        last = oldStatistics.last.updateLast(
                                multiValue.filter { d -> d.criteria(action) }.map { d -> d.idTemplate(action) to action.timeStamp }.toMap()
                        )
                )
            }


    override fun init(ctx: ProcessorContext) {
        this.ctx = ctx
        statistics = ctx.windowStore(this.store)
    }

    override fun close() {
    }

    fun Map<String, Long>.incrementCounts(params: List<String>): Map<String, Long> =
            this + params.map {
                it to getOrDefault(it, 0L) + 1L
            }

    fun Map<String, Float>.incrementSum(params: Map<String, Float>): Map<String, Float> =
            this + params.mapValues {
                getOrDefault(it.key, 0.0f) + it.value
            }

    fun Map<String, Float>.updateMax(params: Map<String, Float>): Map<String, Float> =
            this + params.mapValues {
                maxOf(getOrDefault(it.key, 0.0f), it.value)
            }

    fun Map<String, LocalDateTime>.updateLast(params: Map<String, LocalDateTime>): Map<String, LocalDateTime> =
            this + params.mapValues {
                maxOf(getOrDefault(it.key, LocalDateTime.MIN), it.value)
            }

}

fun hourlyWindow(timestamp: LocalDateTime): Instant =
        timestamp.toLocalDate().atTime(timestamp.hour, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant()


fun dailyWindow(timestamp: LocalDateTime): Instant =
        timestamp.toLocalDate().atStartOfDay().atZone(ZoneId.systemDefault()).toInstant()


