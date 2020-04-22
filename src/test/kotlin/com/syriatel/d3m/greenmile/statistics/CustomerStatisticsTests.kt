package com.syriatel.d3m.greenmile.statistics

import com.syriatel.d3m.greenmile.actions
import com.syriatel.d3m.greenmile.criteria.call
import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.domain.ActionType
import com.syriatel.d3m.greenmile.utils.serdeFor
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.state.WindowStore
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.assertj.core.api.Assertions.assertThat
import org.hibernate.validator.internal.util.Contracts.assertNotNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.*
import kotlin.streams.toList


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
                    satisfies = { call }
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
            serdeFor<String>().serializer(), serdeFor<String>().serializer()
    )

    lateinit var testDriver: TopologyTestDriver
    lateinit var stats: WindowStore<String, Map<String, DimensionStatistics>>
    lateinit var storeName: String

    @BeforeEach
    fun setup() {
        val dimensions = dimensions {
            dimension {
                name = { "calls" }
                satisfies = { call }
                metrics = arrayOf(cost)
            }
        }

        testDriver = TopologyTestDriver(
                StreamsBuilder().apply {
                    storeName = actions().groupByKey().windowedBy(
                            TimeWindows.of(Duration.ofHours(1))
                    ).statistics(dimensions, "hourly-statistics").queryableStoreName()
                }.build(),
                Properties().apply {
                    this[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "dummy:9092"
                    this[StreamsConfig.APPLICATION_ID_CONFIG] = "dummy"
                }

        )
    }

    @Test
    fun test() {
        stats = testDriver.getWindowStore(storeName)
        javaClass.classLoader.getResourceAsStream("cdrs/rec_sample.csv")?.use { inputStream ->
            testDriver.pipeInput(inputStream.bufferedReader().lines().map {
                factory.create("rec", it.split(",")[1], it)
            }.toList())
        }
        assertNotNull(stats.fetch("0933886839", timestampOf("2020-01-01T12:00:00.000"))?.also {
            assertEquals(1L, it["calls"]?.count)
            assertEquals(13.0, (it["calls"] ?: error("")).sum["cost"])
        })

    }

    @AfterEach
    fun tearDown() {
        testDriver.close()
    }


}

fun timestampOf(date: String) =
        LocalDateTime.parse(date).timestamp()


fun LocalDateTime.timestamp() = atZone(ZoneId.systemDefault()).toEpochSecond() * 1000

fun Long.dateTime() = Instant.ofEpochMilli(this).atZone(ZoneId.systemDefault()).toLocalDateTime()