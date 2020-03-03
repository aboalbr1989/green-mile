package com.syriatel.d3m.greenmile


import com.syriatel.d3m.greenmile.utils.kafkaProducer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Materialized
import org.hibernate.annotations.NamedQuery
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@SpringBootApplication
class GreenMile

fun main(args: Array<String>) {
    runApplication<GreenMile>(*args)
}


@Configuration
@EnableConfigurationProperties(KafkaProperties::class)
class StreamsApp {

    @Bean(name = ["greenMileTopology"])
    fun topology() =
            StreamsBuilder().apply {
                stream<String, String>("hello", Consumed.with(Serdes.String(), Serdes.String())).flatMapValues { v ->
                    v.split(Regex("\\s+"))
                }.groupBy { _, v ->
                    v
                }.count(Materialized.`as`("word-counts")).toStream().foreach { s, l ->
                    println("$s = $l")
                }
            }

    @Bean
    fun kafkaStreams(
            streamsBuilder: StreamsBuilder, properties: KafkaProperties) =
            KafkaStreams(streamsBuilder.build(), Properties().apply {
                putAll(properties.buildStreamsProperties())
            }.apply {
                println(this)
            }).apply {
                start()
            }


    @Bean
    fun bootstrap(properties: KafkaProperties) = CommandLineRunner {
        val producer = kafkaProducer<String, String>(properties.buildProducerProperties())
        Executors.newScheduledThreadPool(1).apply {
            scheduleAtFixedRate({
                for (i in 1..100) {
                    producer.send(ProducerRecord("hello", "Hello World"))
                }
                producer.flush()

            }, 0, 1, TimeUnit.MILLISECONDS)
        }
    }


}


fun <K, V> KafkaProducer<K, V>.produceMessage(topic: String, key: K? = null, value: V) {
    send(ProducerRecord(topic, key, value))
}



