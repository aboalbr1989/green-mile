package com.syriatel.d3m.greenmile


import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.domain.ActionType
import com.syriatel.d3m.greenmile.metrics.CustomerStatistics
import com.syriatel.d3m.greenmile.metrics.Dimension
import com.syriatel.d3m.greenmile.metrics.Statistics
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Transformer
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
    fun topology() = StreamsBuilder().apply {
        ActionType.values().map {
            stream<String, String>(it.topic).mapValues { _, v ->
                it.toAction(v.split(",").toTypedArray())
            }
        }.reduce { s1, s2 ->
            s1.merge(s2)
        }
    }

    @Bean
    fun kafkaStreams(streamsBuilder: StreamsBuilder, properties: KafkaProperties) =
            KafkaStreams(streamsBuilder.build(), Properties().apply {
                putAll(properties.buildStreamsProperties())
            }).apply {
                start()
            }
}


object StatisticTransformers : TransformerSupplier<String, Action, KeyValue<String, Action>> {
    override fun get(): Transformer<String, Action, KeyValue<String, Action>> =
            CustomerStatistics(listOf(
                    Dimension(
                            id = {
                                "calls"
                            },
                            criteria = {
                                call
                            },
                            value = {
                                map["duration"] as Number
                            }
                    )
            , Dimension(
                    id = {
                        "sms"
                    },
                    criteria =  {
                        sms
                    },
                    value = {
                        1
                    }
            )
                    ))
}
