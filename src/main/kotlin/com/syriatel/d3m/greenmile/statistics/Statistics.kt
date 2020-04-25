package com.syriatel.d3m.greenmile.statistics

import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.utils.fullJoin
import com.syriatel.d3m.greenmile.utils.materializedAsKeyValueStore
import com.syriatel.d3m.greenmile.utils.materializedAsWindowStore
import com.syriatel.d3m.greenmile.utils.serdeFor
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.*
import java.time.Duration
import java.time.LocalDateTime


class Statistics(map: Map<String, DimensionStatistics> = mapOf()) : Map<String, DimensionStatistics> by map {
    infix fun accumulate(state: Map<String, DimensionStatistics>) =
            Statistics(fullJoin(state, replaceNullWith = DimensionStatistics()) { v1, v2 ->
                v1 + v2
            })
}

data class DimensionStatistics(
        val count: Long = 0L,
        val last: LocalDateTime = LocalDateTime.MIN,
        val sum: Map<String, Double> = mapOf(),
        val max: Map<String, Double> = mapOf()
) {
    operator fun plus(other: DimensionStatistics) =
            DimensionStatistics(
                    count = count + other.count,
                    last = maxOf(last, other.last),
                    sum = sum.fullJoin(other.sum, 0.0) { v1, v2 ->
                        v1 + v2
                    },
                    max = max.fullJoin(other.max, 0.0) { v1, v2 ->
                        maxOf(v1, v2)
                    }
            )

    operator fun minus(other: DimensionStatistics) =
            DimensionStatistics(
                    count = count - other.count,
                    last = maxOf(last, other.last),
                    sum = sum.fullJoin(other.sum, 0.0) { v1, v2 ->
                        v1 - v2
                    },
                    max = max.fullJoin(other.max, 0.0) { v1, v2 ->
                        maxOf(v1, v2)
                    }
            )
}

fun TimeWindowedKStream<String, Action>.statistics(dimensions: List<Dimension>, storeName: String): KTable<Windowed<String>, Statistics> = aggregate(
        { Statistics() },
        { _, action, statistics ->
            statistics.accumulate(dimensions of action)
        },
        materializedAsWindowStore<String, Statistics>(storeName)
                .withValueSerde(serdeFor())
                .withValueSerde(serdeFor())
                .withRetention(Duration.ofDays(30)
                )

)


inline fun <reified K, reified V> KTable<Windowed<K>, V>.windowed(crossinline windowMapper: (Window) -> Window): KGroupedTable<Windowed<K>, V> =
        groupBy({ key, value ->
            KeyValue(Windowed(
                    key.key(),
                    windowMapper(key.window())
            ), value)
        }, Grouped.with(WindowedSerdes.TimeWindowedSerde(serdeFor<K>()), serdeFor()))


fun KGroupedTable<Windowed<String>, Statistics>.materialize(storeName: String): KTable<Windowed<String>, Statistics> =
        aggregate({ Statistics() },
                { _, value, aggregate ->
                    Statistics(aggregate.fullJoin(value, DimensionStatistics()) { v1, v2 ->
                        v1 + v2
                    })
                },
                { _, value, aggregate ->
                    Statistics(aggregate.fullJoin(value, DimensionStatistics()) { v1, v2 ->
                        v1 - v2
                    })
                },
                materializedAsKeyValueStore<Windowed<String>, Statistics>(storeName).withKeySerde(
                        WindowedSerdes.TimeWindowedSerde(serdeFor())
                ).withValueSerde(
                        serdeFor()
                )
        )


fun KTable<Windowed<String>, Statistics>.rollup(storeName: String, fn: (Window) -> Window) =
        windowed(fn).materialize(storeName)