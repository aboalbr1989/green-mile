package com.syriatel.d3m.greenmile


import com.syriatel.d3m.greenmile.criteria.onNet
import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.domain.ActionType
import com.syriatel.d3m.greenmile.metrics.*
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Predicate
import org.apache.kafka.streams.kstream.TransformerSupplier
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
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
            customerStatistics: CustomerStatistics
    ) = StreamsBuilder().apply {
        ActionType.values().map {
            stream<String, String>(it.topic).mapValues { _, v ->
                it.toAction(v.split(",").toTypedArray())
            }
        }.reduce { s1, s2 ->
            s1.merge(s2)
        }.transform(
                TransformerSupplier {
                    customerStatistics
                }
        )
    }

    @Bean
    fun kafkaStreams(streamsBuilder: StreamsBuilder, properties: KafkaProperties) =
            KafkaStreams(streamsBuilder.build(), Properties().apply {
                putAll(properties.buildStreamsProperties())
            }).apply {
                start()
            }


    @Bean
    fun customerMetrics() = CustomerStatistics(
            multiValue =
            listOf(
                    MultiMetricDimension(
                            idTemplate = {
                                "onNet$type"
                            },
                            criteria = {
                                onNet
                            },
                            metrics = mapOf(
                                    actionDuration,
                                    actionCost
                            )
                    )
            )
    )

}
