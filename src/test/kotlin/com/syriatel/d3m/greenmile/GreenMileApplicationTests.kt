package com.syriatel.d3m.greenmile

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.junit.jupiter.api.Assertions.assertEquals
import java.time.LocalDateTime
import java.time.LocalTime


@SpringBootTest
class GreenMileApplicationTests {

    val actions = listOf(
            Action(performedBy = "0933886839", type = ActionType.Call, timeStamp = LocalDateTime.of(
                    2020, 1, 1, 12, 1), cost = 500.0)
                    .apply { put("usageServiceType", 10) },

            Action(performedBy = "0933886839", type = ActionType.Call, timeStamp = LocalDateTime.of(
                    2020, 1, 1, 15, 10), cost = 30.5)
                    .apply { put("usageServiceType", 11) },

            Action(performedBy = "0933886839", type = ActionType.Call, timeStamp = LocalDateTime.of(
                    2020, 1, 1, 19, 59), cost = 55.3)
                    .apply { put("usageServiceType", 10) },


            Action(performedBy = "0933886839", type = ActionType.Msg, timeStamp = LocalDateTime.of(
                    2020, 1, 1, 16, 10), cost = 0.1)
                    .apply { put("usageServiceType", 11) },

            Action(performedBy = "0933886839", type = ActionType.Msg, timeStamp = LocalDateTime.of(
                    2020, 1, 1, 19, 10), cost = 1.0)
                    .apply { put("usageServiceType", 11) },

            Action(performedBy = "0933886839", type = ActionType.Msg, timeStamp = LocalDateTime.of(
                    2020, 1, 1, 21, 10), cost = 3.3)
                    .apply { put("usageServiceType", 10) }

    )

    @Test
    fun contextLoads() {
    }

    @Test
    fun `should be count actions`() {
        var acc = 0

        actions.forEach {
            acc = countOf(acc, it) {
                call
            }
        }
        assertEquals(3, acc)
    }

    @Test
    fun `should be sum`() {
        var costAction = 0.0

        actions.forEach {
            costAction = sumOf(costAction, it, { cost }) {
                call and onNet
            }
            println(costAction)
        }
        assertEquals(555.3, costAction)
    }

    @Test
    fun `roaming test`() {
        val actions = listOf(
                Action(type = ActionType.Call, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 12, 1), cost = 500.0)
                        .apply { put("usageServiceType", 10) },
                Action(type = ActionType.Call, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 12, 1), cost = 500.0)
                        .apply { put("usageServiceType", 11) },
                Action(type = ActionType.Call, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 12, 1), cost = 500.0)
                        .apply { put("usageServiceType", 21) },


                Action(type = ActionType.Msg, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 12, 1), cost = 500.0)
                        .apply { put("usageServiceType", 21) },
                Action(type = ActionType.Msg, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 12, 1), cost = 500.0)
                        .apply { put("usageServiceType", 22) },
                Action(type = ActionType.Msg, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 12, 1), cost = 500.0)
                        .apply { put("usageServiceType", 31) },


                Action(type = ActionType.DataSession, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 12, 1), cost = 500.0)
                        .apply { put("usageServiceType", 31) },

                Action(type = ActionType.DataSession, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 12, 1), cost = 500.0)
                        .apply { put("usageServiceType", 33) },
                Action(type = ActionType.DataSession, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 12, 1), cost = 500.0)
                        .apply { put("usageServiceType", 21) }

        )
        var costAction = 0.0
        var count = 0

        actions.forEach {
            costAction = sumOf(costAction, it, { cost }) {
                call and onNet and timeBetween(LocalTime.MIDNIGHT, LocalTime.of(20, 0))
            }
        }

        actions.forEach {
            costAction = sumOf(costAction, it, { cost }) {
                sms and onNet and timeBetween(LocalTime.MIDNIGHT, LocalTime.of(20, 0))
            }
        }

        actions.forEach {
            costAction = sumOf(costAction, it, { cost }) {
                dataSession and onNet and timeBetween(LocalTime.MIDNIGHT, LocalTime.of(20, 0))
            }
        }
        assertEquals(1500.0, costAction)


    }
}
