package com.syriatel.d3m.greenmile


import com.syriatel.d3m.greenmile.criteria.offNet
import com.syriatel.d3m.greenmile.criteria.onNet
import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.domain.ActionType
import com.syriatel.d3m.greenmile.statistics.MetricsHolder
import com.syriatel.d3m.greenmile.statistics.dimensions
import com.syriatel.d3m.greenmile.transformers.actionCsvSerde
import com.syriatel.d3m.greenmile.utils.serdeFor
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.ZoneId
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
            metrics: MetricsHolder
    ) = StreamsBuilder().apply {
    }

    @Bean
    fun kafkaStreams(streamsBuilder: StreamsBuilder, properties: KafkaProperties) =
            KafkaStreams(streamsBuilder.build(), Properties().apply {
                putAll(properties.buildStreamsProperties())
            }).apply {
                start()
            }


    @Bean
    fun metrics(): MetricsHolder =
            MetricsHolder(dimensions {
                dimension {
                    name = { type.name }
                    satisfies = { onNet }
                    metrics = cost and duration and dataSize
                }
                dimension {
                    name = { "to_competitors" }
                    satisfies = { offNet }
                    metrics = cost and duration and dataSize
                }
            })
}

fun StreamsBuilder.actions(): KStream<String, Action> = stream(ActionType.values().map { it.topic }, Consumed.with(
        serdeFor(),
        actionCsvSerde,
        { record, _ ->
            (record.value() as Action).timeStamp.atZone(ZoneId.systemDefault()).toEpochSecond() * 1000
        },
        Topology.AutoOffsetReset.LATEST
))

