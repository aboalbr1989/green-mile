package com.syriatel.d3m.greenmile.ml


import com.fasterxml.jackson.databind.JsonNode
import com.syriatel.d3m.greenmile.schema.ObjectDescriptor
import com.syriatel.d3m.greenmile.utils.materializedAsKeyValueStore
import com.syriatel.d3m.greenmile.utils.serdeFor
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*


data class MLModule(
        val version: Long,
        val resultSchema: ObjectDescriptor
)


fun StreamsBuilder.mlModules(): GlobalKTable<String, MLModule> =
        globalTable("mlModules", Consumed.with(serdeFor(), serdeFor()))


data class MLResultUpdate(
        val moduleName: String,
        val value: JsonNode
)


class MLProfile(
        map: Map<String, JsonNode> = mapOf()
) : Map<String, JsonNode> by map

fun StreamsBuilder.mlProfiles(): KTable<String, MLProfile> =
        stream<String, MLResultUpdate>("ml-updates", Consumed.with(
                serdeFor(), serdeFor()
        )).join<String, MLModule, MLResultUpdate>(mlModules(),
                KeyValueMapper { _, v ->
                    v.moduleName
                },
                ValueJoiner { v1, _ ->
                    v1
                }
        ).groupByKey().aggregate(
                { MLProfile() }, { _, v, a -> MLProfile(a + (v.moduleName to v.value)) },
                materializedAsKeyValueStore<String, MLProfile>(
                        "ml-profile"
                ).withKeySerde(serdeFor()).withValueSerde(serdeFor())
        )
