package com.syriatel.d3m.greenmile.metrics

import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.utils.serdeFor
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.state.WindowStore
import java.sql.Timestamp
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
        val dimensions: List<Dimension>,
        val store: String = "statistics",
        val windowFunction: (LocalDateTime) -> Instant = ::hourlyWindow
) : ThroughTransformer<String, Action>() {
    lateinit var ctx: ProcessorContext
    lateinit var statistics: WindowStore<String, Statistics?>

    override fun process(key: String, value: Action) {
        val customerStates = statistics.fetch(key, hourlyWindow(value.timeStamp).toEpochMilli()) ?: Statistics()
        val result = customerStates.copy(
                counts = customerStates.counts + dimensions.filter {
                    it.criteria(value)
                }.map {
                    it.id(value) to customerStates.counts.getOrDefault(it.id(value), 0L) + 1
                }.toMap(),
                sums = customerStates.sums + dimensions.filter {
                    it.criteria(value)
                }.map {
                    it.id(value) to customerStates.sums.getOrDefault(it.id(value), 0.0f) + it.value(value).toFloat()
                }.toMap(),
                last = customerStates.last + dimensions.filter {
                    it.criteria(value)
                }.map {
                    it.id(value) to maxOf(customerStates.last.getOrDefault(it.id(value), LocalDateTime.MIN), value.timeStamp)
                }.toMap(),
                max = customerStates.max + dimensions.filter {
                    it.criteria(value)
                }.map {
                    it.id(value) to maxOf(customerStates.max.getOrDefault(it.id(value), Float.MIN_VALUE), it.value(value).toFloat())
                }.toMap()
        )
        statistics.put(key, result, hourlyWindow(value.timeStamp).toEpochMilli())
    }

    override fun init(ctx: ProcessorContext) {
        this.ctx = ctx
        statistics = ctx.getStateStore(this.store) as WindowStore<String, Statistics?>
    }

    override fun close() {
    }

}


fun hourlyWindow(timestamp: LocalDateTime) =
        timestamp.toLocalDate().atTime(timestamp.hour, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant()


fun dailyWindow(timestamp: LocalDateTime) =
        timestamp.toLocalDate().atStartOfDay().atZone(ZoneId.systemDefault()).toInstant()