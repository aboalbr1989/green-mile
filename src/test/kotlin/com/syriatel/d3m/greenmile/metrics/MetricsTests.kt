package com.syriatel.d3m.greenmile.metrics

import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.domain.ActionType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.time.LocalTime

class CountTests {
    @Test
    fun `should calculate total actions`() {
        val acc = listOf(1, 5, 8, 9)

        acc.forEach { ac ->
            ActionType.values().forEach { at ->
                Assertions.assertEquals(ac + 1, countOf(at, ac, Action(
                        type = at
                )))
                ActionType.values().toSet().filter { it != at }.forEach {
                    Assertions.assertEquals(ac, countOf(at, ac, Action(
                            type = it
                    )))
                }
            }
        }

    }

    @Test
    fun `calculate sum of field based on criteria`() {

        val atNight: Action.() -> Boolean = {
            this.timeStamp.toLocalTime().let {
                it.isBefore(LocalTime.of(23, 59)) && it.isAfter(LocalTime.of(18, 0))
            }
        }


        Assertions.assertEquals(10.0, sumOf(8.0, Action(cost = 2.0, type = ActionType.Call, timeStamp = LocalDateTime.of(
                2020, 1, 1, 19, 10
        )), atNight, { this.cost }))
    }

    @Test
    fun `calculate sum of field based on criteria2`() {
        val actions = listOf(
                Action(performedBy = "0933886839", type = ActionType.Call, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 12, 1), map = mutableMapOf("usageServiceType" to 10)),

                Action(performedBy = "0933886850", type = ActionType.Call, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 15, 10), map = mutableMapOf("usageServiceType" to 10)),

                Action(performedBy = "0933886780", type = ActionType.Call, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 19, 59), map = mutableMapOf("usageServiceType" to 10)),

                Action(performedBy = "0933887850", type = ActionType.Call, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 16, 10), map = mutableMapOf("usageServiceType" to 47)),

                Action(performedBy = "0933789850", type = ActionType.SMS, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 19, 10), map = mutableMapOf("usageServiceType" to 10)),

                Action(performedBy = "0933789850", type = ActionType.Call, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 21, 10), map = mutableMapOf("usageServiceType" to 10))
        )
        val results = listOf(1, 2, 3, 3, 3, 3)

        val criteria2: Action?.() -> Boolean = {
            if (this == null)
                false
            else
                (this.timeStamp.toLocalTime().let {
                    it.isAfter(LocalTime.MIDNIGHT) && it.isBefore(LocalTime.of(20, 0))
                }) && type == ActionType.Call && map["usageServiceType"] == 10
        }

        actions.forEachIndexed { i, action ->
            Assertions.assertEquals(results[i], count(if (i > 0) results[i - 1] else 0, action, criteria2))
        }


    }

    @Test
    fun `should find the latest action by cost`() {
        val actions = listOf(
                Action(performedBy = "0933886839", type = ActionType.Call, cost = null),

                Action(performedBy = "0933886839", type = ActionType.Call, cost = 16.0),

                Action(performedBy = "0933886839", type = ActionType.Call, cost = 17.0),

                Action(performedBy = "0933886839", type = ActionType.Call, cost = 19.0),

                Action(performedBy = "0933886839", type = ActionType.SMS, cost = 31.0),

                Action(performedBy = "0933886839", type = ActionType.Call, cost = null)

        )


        var result2: Double? = null

        for (a in actions) {
            result2 = max(result2, a, { it.type == ActionType.Call }, { it.cost })
        }
        Assertions.assertEquals(19.0, result2)


    }

    @Test
    fun `should find the latest action and all costs are null`() {
        val actions = listOf(
                Action(performedBy = "0933886839", type = ActionType.Call, cost = null),

                Action(performedBy = "0933886839", type = ActionType.Call, cost = null),
                Action(performedBy = "0933886839", type = ActionType.SMS, cost = null),
                Action(performedBy = "0933886839", type = ActionType.Call, cost = null),
                Action(performedBy = "0933886839", type = ActionType.Call, cost = null)


        )


        var result2: Double? = null

        for (a in actions) {
            result2 = max(result2, a, { it.type == ActionType.Call }, { it.cost })
        }
        Assertions.assertEquals(null, result2)


    }

    @Test
    fun `should find the latest action by timestamp`() {
        val actions = listOf(
                Action(performedBy = "0933886839", type = ActionType.Call, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 12, 1), cost = 15.0),

                Action(performedBy = "0933886839", type = ActionType.Call, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 15, 10), cost = 16.0),

                Action(performedBy = "0933886839", type = ActionType.Call, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 19, 59), cost = 17.0),

                Action(performedBy = "0933886839", type = ActionType.Call, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 16, 10), cost = 19.0),

                Action(performedBy = "0933886839", type = ActionType.SMS, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 19, 10), cost = 31.0),

                Action(performedBy = "0933886839", type = ActionType.Call, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 21, 10), cost = null)

        )


        var result: LocalDateTime? = null
        for (a in actions) {
            result = max(result, a, { it.type == ActionType.Call })
            { it.timeStamp }
        }
        Assertions.assertEquals(LocalDateTime.of(2020, 1, 1, 21, 10), result)


    }
}