package com.syriatel.d3m.greenmile.statistics

import com.syriatel.d3m.greenmile.actions
import com.syriatel.d3m.greenmile.criteria.call
import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.domain.ActionType
import com.syriatel.d3m.greenmile.utils.`for`
import com.syriatel.d3m.greenmile.utils.daily
import com.syriatel.d3m.greenmile.utils.dailyWindow
import com.syriatel.d3m.greenmile.utils.serdeFor
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

class CustomerStatisticsTests {

    @Nested
    inner class CalculateStatisticsOnMultiValueMetricsTests {
        private val costFunction: Action.() -> Float = {
            (cost ?: 0.0).toFloat()
        }

        @Test
        fun `should update count & sum & max & last of events match criteria`() {
            val met = dimensions {
                dimension {
                    name = { "calls" }
                    criteria = { call }
                    metrics = arrayOf("cost" to costFunction)
                }
            }

            val oldStatistics =
                    Statistics(
                            mapOf("calls" to DimensionStatistics(
                                    count = 10L,
                                    last = LocalDateTime.of(2020, 1, 1, 0, 0, 0),
                                    sum = mapOf(
                                            "cost" to 25.0
                                    ),
                                    max = mapOf(
                                            "cost" to 40.0
                                    )
                            ))
                    )

            assertThat(oldStatistics.accumulate(met of
                    Action(
                            timeStamp = LocalDateTime.of(
                                    2020, 1, 2, 0, 0, 0
                            ),
                            type = ActionType.Call,
                            cost = 1.0
                    )
            )).matches { all ->
                all["calls"]?.let {
                    it.count == 11L &&
                            it.sum["cost"] == 26.0 &&
                            it.max["cost"] == 40.0 &&
                            it.last == LocalDateTime.of(2020, 1, 2, 0, 0, 0)
                } ?: false
            }

            assertThat(oldStatistics.accumulate(met of
                    Action(
                            type = ActionType.SMS
                    )
            )).matches { all ->
                all["calls"]?.let {
                    it.count == 10L
                } ?: false
            }
        }
    }

}

class CustomerStatisticsStreamsTests {
    val factory = ConsumerRecordFactory(
            serdeFor<String>().serializer(), serdeFor<Action>().serializer()
    )

    lateinit var testDriver: TopologyTestDriver


    private fun createCall(timestamp: String) =
            Action(
                    timeStamp = LocalDateTime.parse(timestamp),
                    type = ActionType.Call
            )

    private fun createCall(timestamp: LocalDateTime) =
            Action(
                    timeStamp = timestamp,
                    type = ActionType.Call
            )

    @BeforeEach
    fun setup() {
        val dimensions = dimensions {
            dimension {
                name = { "calls" }
                criteria = { call }
                metrics = arrayOf(cost)
            }
        }

        testDriver = TopologyTestDriver(
                StreamsBuilder().apply {
                    actions(serdeFor()).groupByKey().windowedBy(
                            TimeWindows.of(Duration.ofHours(1))
                    ).statistics(dimensions, "hourly-statistics")
                            .rollup("daily-statistics") {
                                it.daily
                            }

                }.build(),
                Properties().apply {
                    this[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "dummy:9092"
                    this[StreamsConfig.APPLICATION_ID_CONFIG] = "dummy"
                }

        )
    }

    @Test
    fun `should calculate hourly statistics`() {
        val hourlyStatistics = testDriver.getWindowStore<String, Statistics>("hourly-statistics")

        testDriver.pipeInput(
                listOf(12, 13, 14).map {
                    factory.create("rec", "0933886839", createCall("2020-01-01T$it:10:00.000").copy(cost = 13.0))
                }
        )

        val sequence = hourlyStatistics.all().asSequence()

        assertThat(sequence.toList()).allMatch {
            (it.value["calls"]?.count == 1L) and (it.value["calls"]?.sum?.get("cost") == 13.0)
        }

    }


    @Test
    fun `should calculate daily statistics`() {
        val daily = testDriver.getKeyValueStore<Windowed<String>, Statistics>("daily-statistics")
        testDriver.pipeInput(listOf(5, 4, 3, 2, 1).flatMap { d ->
            listOf(1, 3, 4, 5, 6, 7, 3).map { h ->
                factory.create("rec", "988957030",
                        createCall(LocalDateTime.of(
                                2020, 1, d, h, 0, 0
                        ))
                )
            }
        })
        daily.all().forEach {
            assertEquals(7L, it.value["calls"]?.count)
        }

        daily[LocalDate.of(2020, 1, 5).dailyWindow `for` "988957030"]
    }

    @AfterEach
    fun tearDown() {
        testDriver.close()
    }
}
