package com.syriatel.d3m.greenmile.metrics

import com.syriatel.d3m.greenmile.criteria.call
import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.domain.ActionType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.LocalDateTime


class CustomerStatisticsTests {

    val statistics = CustomerStatistics()

    @Nested
    inner class CalculateStatisticsOnMultiValueMetricsTests {
        private val costFunction: Action.() -> Float = {
            (cost ?: 0.0).toFloat()
        }


        @Test
        fun `should update count & sum & max & last of events match criteria`() {

            val customerStatistics = CustomerStatistics(
                    multiValue = listOf(
                            MultiMetricDimension(
                                    idTemplate = {
                                        "calls"
                                    },
                                    criteria = {
                                        call
                                    },
                                    metrics = mapOf(
                                            "cost" to costFunction
                                    )
                            )
                    )
            )

            val oldStatistics = Statistics(
                    counts = mapOf("calls" to 10L),
                    sums = mapOf("calls#cost" to 25.0f),
                    max = mapOf("calls#cost" to 40.0f),
                    last = mapOf("calls" to LocalDateTime.of(2020, 1, 1, 0, 0, 0))
            )

            assertThat(customerStatistics.calculateMultiValue(
                    Action(
                            timeStamp = LocalDateTime.of(
                                    2020, 1, 2, 0, 0, 0
                            ),
                            type = ActionType.Call,
                            cost = 1.0
                    ),
                    oldStatistics
            )).matches {
                it.counts["calls"] == 11L &&
                        it.sums["calls#cost"] == 26.0f &&
                        it.max["calls#cost"] == 40.0f &&
                        it.last["calls"] == LocalDateTime.of(
                        2020, 1, 2, 0, 0, 0
                )
            }

            assertThat(customerStatistics.calculateMultiValue(
                    Action(
                            type = ActionType.SMS
                    ),
                    oldStatistics
            )).matches {
                it.counts["calls"] == 10L
            }


        }


    }

    @Test
    fun `test increment counts`() {
        val counts = mapOf("calls" to 10L, "sms" to 11L, "data" to 100L)
        val keysToIncrement = listOf("sms", "data")
        statistics.apply {

            assertThat(counts.incrementCounts(keysToIncrement)).matches { result ->
                result["calls"] == counts["calls"] &&
                        result["sms"] == (counts["sms"] ?: 0L) + 1L &&
                        result["data"] == (counts["data"] ?: 0L) + 1L
            }
        }
    }

    @Test
    fun `test increment sum`() {
        val sums = mapOf("calls#cost" to 10f, "sms#cost" to 11.0f, "data#bytes" to 100_000.0f)
        val metrics = mapOf("calls#cost" to 12f, "data#bytes" to 1000f)
        statistics.apply {
            assertThat(sums.incrementSum(metrics)).matches { result ->
                result["calls#cost"] == 22f &&
                        result["sms#cost"] == 11f &&
                        result["data#bytes"] == 101_000f
            }
        }
    }

    @Test
    fun `test update max`() {
        val max = mapOf("calls#cost" to 10f, "sms#cost" to 11.0f, "data#bytes" to 1_001.0f)
        val metrics = mapOf(
                "calls#cost" to 12f,
                "data#bytes" to 1000f
        )
        statistics.apply {
            assertThat(max.updateMax(metrics)).matches { result ->
                result["calls#cost"] == metrics["calls#cost"] &&
                        result["sms#cost"] == max["sms#cost"] &&
                        result["data#bytes"] == max["data#bytes"]
            }
        }
    }

    @Test
    fun `test update latest`() {
        val last = mapOf(
                "calls" to LocalDate.of(2020, 1, 1).atStartOfDay(),
                "sms" to LocalDate.of(2020, 2, 1).atStartOfDay(),
                "data" to LocalDate.of(2020, 4, 1).atStartOfDay())
        val metrics = mapOf(
                "calls" to LocalDate.of(2020, 5, 1).atStartOfDay(),
                "data" to LocalDate.of(2020, 3, 1).atStartOfDay()
        )
        statistics.apply {

            assertThat(last.updateLast(metrics)).matches { result ->
                result["calls"] == metrics["calls"] &&
                        result["sms"] == last["sms"] &&
                        result["data"] == last["data"]
            }
        }
    }

}