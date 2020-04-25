package com.syriatel.d3m.greenmile


import com.syriatel.d3m.greenmile.criteria.offNet
import com.syriatel.d3m.greenmile.criteria.onNet
import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.domain.ActionType
import com.syriatel.d3m.greenmile.statistics.Dimension
import com.syriatel.d3m.greenmile.statistics.dimensions
import com.syriatel.d3m.greenmile.statistics.rollup
import com.syriatel.d3m.greenmile.statistics.statistics
import com.syriatel.d3m.greenmile.transformers.actionCsvSerde
import com.syriatel.d3m.greenmile.utils.*
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.TimeWindows
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration
import java.util.*


@SpringBootApplication
class GreenMile

fun main(args: Array<String>) {
    runApplication<GreenMile>(*args)
}


@Configuration
@EnableConfigurationProperties(KafkaProperties::class)
class StreamsApp {

    @Bean(name = ["greenMileTopology"])
    fun topology(
            metrics: List<Dimension>
    ) = StreamsBuilder().apply {

        val actions = actions()
        val hourly = actions.groupByKey().windowedBy(
                TimeWindows.of(Duration.ofHours(1))
        ).statistics(metrics, "hourly")

        val daily = hourly.rollup("daily") { it.daily }
        daily.rollup("weekly") { it.weekly }
        daily.rollup("monthly") { it.monthly }
    }

    @Bean
    fun kafkaStreams(streamsBuilder: StreamsBuilder, properties: KafkaProperties) =
            KafkaStreams(streamsBuilder.build(), Properties().apply {
                putAll(properties.buildStreamsProperties())
            }).apply {
                start()
            }


    @Bean
    fun metrics(): List<Dimension> =
            dimensions {
                dimension {
                    name = { type.name }
                    criteria = { onNet }
                    metrics = cost and duration and dataSize
                }
                dimension {
                    name = { "to_competitors" }
                    criteria = { offNet }
                    metrics = cost and duration and dataSize
                }
            }
}

fun StreamsBuilder.actions(actionSerde: Serde<Action> = actionCsvSerde): KStream<String, Action> = stream(ActionType.values().map { it.topic }, Consumed.with(
        serdeFor(),
        actionSerde,
        { record, _ ->
            (record.value() as Action).timeStamp.timestamp().toEpochMilli()
        },
        Topology.AutoOffsetReset.LATEST
))

