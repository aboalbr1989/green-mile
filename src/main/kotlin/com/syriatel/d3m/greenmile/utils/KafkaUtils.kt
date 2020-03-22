package com.syriatel.d3m.greenmile.utils

import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import java.util.*
import kotlin.reflect.KClass

val map = mapOf(
        Long::class to Serdes.LongSerde(),
        String::class to Serdes.StringSerde(),
        Int::class to Serdes.IntegerSerde()
)

@Suppress("UNCHECKED_CAST")
inline fun <reified T> serdeFor(): Serde<T> =
        (map[T::class] ?: JsonSerde<T>(T::class.java)) as Serde<T>


inline fun <reified K, reified V> kafkaProducer(properties: Map<String, Any>) =
        KafkaProducer<K, V>(Properties().apply {
            putAll(properties)
        }, serdeFor<K>().serializer(), serdeFor<V>().serializer())

fun <K, V> KafkaProducer<K, V>.produceMessage(topic: String, key: K? = null, value: V) {
    send(ProducerRecord(topic, key, value))
}

val objectMapper = ObjectMapper().registerKotlinModule()
        .registerModules(
                Jdk8Module(),
                JavaTimeModule()
        )

class JsonSerde<T>(val clazz: Class<T>) : Serde<T> {
    override fun deserializer(): Deserializer<T?> =
            Deserializer { _, bytes ->
                bytes?.let {
                    objectMapper.readValue(it, clazz)
                }
            }

    override fun serializer(): Serializer<T?> =
            Serializer { _, t ->
                println(t)
                t?.let {
                    objectMapper.writeValueAsString(it).toByteArray()
                }.apply {
                    println(objectMapper.writeValueAsString(t))
                }

            }

}
