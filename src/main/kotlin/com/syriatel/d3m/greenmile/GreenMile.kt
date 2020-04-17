package com.syriatel.d3m.greenmile


import com.syriatel.d3m.greenmile.criteria.onNet
import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.domain.ActionType
import com.syriatel.d3m.greenmile.metrics.*
import com.syriatel.d3m.greenmile.profiling.CustomerProfile
import com.syriatel.d3m.greenmile.profiling.customerProfiles
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.ValueJoiner
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
        val profiles = customerProfiles()
        val actions = ActionType.values().map {
            stream<String, String>(it.topic).mapValues { _, v ->
                it.toAction(v.split(",").toTypedArray())
            }
        }.reduce { s1, s2 ->
            s1.merge(s2)
        }
        actions.join(
                profiles, ValueJoiner { v1: Action, v2: CustomerProfile ->
            v1.copy(map = (v1.map + ("profile" to v2)).toMutableMap())
        })

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
