package com.syriatel.d3m.greenmile.metrics

import com.syriatel.d3m.greenmile.call
import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.domain.ActionType
import com.syriatel.d3m.greenmile.utils.serdeFor
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.kstream.TransformerSupplier
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.hibernate.validator.internal.util.Contracts.assertNotNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.LocalDateTime
import java.util.*


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
                                1
                            }
                    )
            ))
}


class StatisticsTests {

    var testDriver: TopologyTestDriver = TopologyTestDriver(
            StreamsBuilder().apply {
                addStateStore(
                        Stores.windowStoreBuilder(

                                Stores.inMemoryWindowStore(
                                        "statistics",
                                        Duration.ofDays(30),
                                        Duration.ofDays(1),
                                        false
                                ),
                                serdeFor<String>(),
                                serdeFor<Statistics>()
                        )
                )
                stream<String, Action>("hello", Consumed.with(
                        serdeFor<String>(),
                        serdeFor<Action>()
                )).transform(
                        StatisticTransformers,
                        "statistics"
                ).to("result", Produced.with(
                        serdeFor(),
                        serdeFor()
                ))


            }.build(), Properties().apply {
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092")
        put(StreamsConfig.APPLICATION_ID_CONFIG, "dummy")
    })

    val factory = ConsumerRecordFactory<String, Action>("hello", serdeFor<String>().serializer(), serdeFor<Action>().serializer())

    @Test
    fun test() {

        val date = LocalDateTime.of(2020, 1, 1, 10, 0, 0)
        testDriver.pipeInput(
                factory.create(
                        listOf(
                                KeyValue(
                                        "88957030",
                                        Action(
                                                timeStamp = date,
                                                type = ActionType.Call
                                        )
                                )
                        )
                )
        )
        val store = testDriver.getWindowStore<String, Statistics>("statistics")

        assertNotNull(store)
        Assertions.assertEquals(1L, store.fetch("88957030", hourlyWindow(date).toEpochMilli()).counts["calls"])
    }


    @AfterEach
    fun tearDown() {
    }
}