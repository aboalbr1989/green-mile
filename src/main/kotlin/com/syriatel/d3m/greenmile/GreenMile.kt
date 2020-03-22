package com.syriatel.d3m.greenmile


import com.syriatel.d3m.greenmile.domain.ActionType
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
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
    fun topology() = StreamsBuilder().apply {
        ActionType.values().map {
            stream<String, String>(it.topic).mapValues { _, v ->
                it.toAction(v.split(",").toTypedArray())
            }

            @Bean
            fun kafkaStreams(streamsBuilder: StreamsBuilder, properties: KafkaProperties) =
                    KafkaStreams(streamsBuilder.build(), Properties().apply {
                        putAll(properties.buildStreamsProperties())
                    }).apply {
                        start()
                    }
        }
    }
}

//data class NamedDim(
//        val nameProvider: Action.() -> String,
//        val criteria: Action.() -> Boolean,
//        val value: Action.() -> Number
//)
//
//
//fun Statistics.calculate(dim: NamedDim, action: Action): Statistics =
//        when (dim.criteria(action)) {
//            true -> {
//                val name = dim.nameProvider(action)
//                val value = dim.value(action)
//                val count = counts.getOrDefault(name, 0) + 1
//                val sum = sums.getOrDefault(name, 0.0f) + value.toFloat()
//                val last = action.timeStamp
//                val mx = this.max.getOrDefault(name, 0.0f).coerceAtLeast(value.toFloat())
//                copy(
//                        counts = counts + (name to count),
//                        max = max + (name to mx),
//                        sums = sums + (name to sum),
//                        last = this.last + (name to last)
//                )
//            }
//            false -> this
//        }
