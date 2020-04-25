package com.syriatel.d3m.greenmile.handset

import com.syriatel.d3m.greenmile.utils.materializedAsKeyValueStore
import com.syriatel.d3m.greenmile.utils.serdeFor
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.GlobalKTable
import java.time.LocalDate

data class HandSet(
        val imei: String,
        val dualSim: Boolean,
        val type: String,
        val maker: String,
        val model: String,
        val os: String,
        val firstUse: LocalDate,
        val mostAdvancedTechnology: String
)


fun StreamsBuilder.handsets(): GlobalKTable<String, HandSet> = globalTable("handsets", Consumed.with(
        serdeFor(),
        serdeFor()
), materializedAsKeyValueStore("handsets"))
