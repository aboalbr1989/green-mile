package com.syriatel.d3m.greenmile.profiling

import com.syriatel.d3m.greenmile.utils.materializedAsKeyValueStore
import com.syriatel.d3m.greenmile.utils.serdeFor
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import kotlin.reflect.jvm.internal.impl.descriptors.FieldDescriptor


fun StreamsBuilder.profileFields() =
        globalTable<String, FieldDescriptor>("profile-fields", Consumed.with(
                serdeFor(), serdeFor()
        ), materializedAsKeyValueStore<String, FieldDescriptor>("profile-fields")
                .withValueSerde(serdeFor())
                .withValueSerde(serdeFor())
        )

//
//fun StreamsBuilder.customerProfile() =
//        stream<String, ObjectNode>("profile-changes", Consumed.with(serdeFor(), serdeFor())).flatMapValues { s, objectNode ->
//            objectNode.fieldNames().asSequence().map {
//                it to objectNode[it]
//            }.toList()
//        }.join(
//                profileFields(),
//                KeyValueMapper { _, v ->
//                    v.first
//                },
//                ValueJoiner { v1, v2 ->
//                    v1 to v2
//                }
//        ).filter { _, v ->
//            v.first.second == v.second
//        }.groupByKey()