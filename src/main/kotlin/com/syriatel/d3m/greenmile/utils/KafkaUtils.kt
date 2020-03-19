package com.syriatel.d3m.greenmile.utils

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.springframework.kafka.support.serializer.JsonSerde
import java.util.*

val map = mapOf(
        Long::class to Serdes.LongSerde(),
        String::class to Serdes.StringSerde(),
        Int::class to Serdes.IntegerSerde()
)

@Suppress("UNCHECKED_CAST")
inline fun <reified T> serdeFor(): Serde<T> =
        (map[T::class] ?: JsonSerde<T>()) as Serde<T>


inline fun <reified K, reified V> kafkaProducer(properties: Map<String, Any>) =
        KafkaProducer<K, V>(Properties().apply {
            putAll(properties)
        }, serdeFor<K>().serializer(), serdeFor<V>().serializer())

fun <K, V> KafkaProducer<K, V>.produceMessage(topic: String, key: K? = null, value: V) {
    send(ProducerRecord(topic, key, value))
}