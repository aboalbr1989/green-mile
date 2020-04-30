package com.syriatel.d3m.greenmile.handsets

import com.syriatel.d3m.greenmile.handset.HandSet
import com.syriatel.d3m.greenmile.handset.handsets
import com.syriatel.d3m.greenmile.utils.serdeFor
import org.apache.kafka.streams.StreamsBuilder

import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.util.*

class HandsetsStreamsTests {

    lateinit var testDriver: TopologyTestDriver


    lateinit var input: TestInputTopic<String, HandSet>

    @BeforeEach
    fun setup() {
        testDriver = TopologyTestDriver(
                StreamsBuilder().apply {
                    handsets()
                }.build(),
                Properties().apply {
                    this[StreamsConfig.APPLICATION_ID_CONFIG] = "dummy"
                    this[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "dummy:9092"
                }
        )

        input = testDriver.createInputTopic("handsets", serdeFor<String>().serializer(), serdeFor<HandSet>().serializer())
    }

    @AfterEach
    fun destroy() {
        testDriver.close()
    }

    @Test
    fun `should update handset`() {

        val handSet = HandSet(
                imei = "0000",
                type = "Phone",
                dualSim = true,
                firstUse = LocalDate.of(2020, 1, 1),
                maker = "Huwawi",
                model = "Nova 3i",
                mostAdvancedTechnology = "4G",
                os = "Android 8.2"
        )

        input.pipeInput(handSet.imei, handSet)



        Assertions.assertEquals(handSet, testDriver.getKeyValueStore<String, HandSet>("handsets")[handSet.imei])

        input.pipeInput(handSet.imei, handSet.copy(dualSim = false))

        Assertions.assertEquals(handSet.copy(dualSim = false), testDriver.getKeyValueStore<String, HandSet>("handsets")[handSet.imei])

    }


}