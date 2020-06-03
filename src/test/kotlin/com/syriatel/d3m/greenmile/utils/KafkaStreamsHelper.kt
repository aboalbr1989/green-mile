package com.syriatel.d3m.greenmile.utils

import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import java.util.*

fun initDriver(fn: StreamsBuilder.() -> Unit): TopologyTestDriver =
        TopologyTestDriver(StreamsBuilder().apply(fn).build(), Properties().apply {
            this[StreamsConfig.APPLICATION_ID_CONFIG] = "dummy"
            this[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "dummy:9092"
        })