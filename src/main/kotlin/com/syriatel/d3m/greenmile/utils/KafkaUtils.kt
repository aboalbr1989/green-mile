package com.syriatel.d3m.greenmile.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.WindowStore
import java.util.*

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

val objectMapper: ObjectMapper = ObjectMapper().registerKotlinModule()
        .registerModules(
                Jdk8Module(),
                JavaTimeModule()
        )

class JsonSerde<T>(private val clazz: Class<T>) : Serde<T> {
    override fun deserializer(): Deserializer<T?> =
            Deserializer { _, bytes ->
                bytes?.let {
                    objectMapper.readValue(it, clazz)
                }
            }

    override fun serializer(): Serializer<T?> =
            Serializer { _, t ->
                t?.let {
                    objectMapper.writeValueAsString(it).toByteArray()
                }
            }

}

@Suppress("UNCHECKED_CAST")
inline fun <reified K, reified V> ProcessorContext.stateStore(name: String): KeyValueStore<K, V> =
        getStateStore(name) as KeyValueStore<K, V>

@Suppress("UNCHECKED_CAST")
inline fun <reified K, reified V> ProcessorContext.windowStore(name: String): WindowStore<K, V> =
        getStateStore(name) as WindowStore<K, V>


inline fun <reified K, reified V> materializedAsKeyValueStore(name: String): Materialized<K, V, KeyValueStore<Bytes, ByteArray>> =
        Materialized.`as`<K, V, KeyValueStore<Bytes, ByteArray>>(name)


inline fun <reified K, reified V> materializedAsWindowStore(name: String): Materialized<K, V, WindowStore<Bytes, ByteArray>> =
        Materialized.`as`<K, V, WindowStore<Bytes, ByteArray>>(name)
