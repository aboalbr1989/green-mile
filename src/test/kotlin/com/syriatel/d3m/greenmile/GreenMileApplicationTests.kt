package com.syriatel.d3m.greenmile

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import javax.swing.UIManager.put


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
    fun `roaming data onNet at midnight`() {
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
                dataSession and onNet and timeBetween(LocalTime.MIDNIGHT, LocalTime.of(20, 0))
            }
        }
        assertEquals(1500.0, costAction)

    }

    @Test
    fun `roaming call onNet at midnight`() {
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

        assertEquals(1500.0, costAction)
    }

    @Test
    fun `roaming sms onNet at midnight`() {
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
                sms and onNet and timeBetween(LocalTime.MIDNIGHT, LocalTime.of(20, 0))
            }
        }
        assertEquals(1500.0, costAction)
    }

    @Test
    fun `traffic data between 1am to 08am`() {
        val action = Action(type = ActionType.Call,
                timeStamp = LocalDate.now().atTime(12, 1), cost = 500.0,
                map = mutableMapOf(
                        "sdasdas" to 10,
                        "fdsfsdf" to 13
                )
        )
        val actions = action.run {
            listOf(
                    this,
                    copy(type = ActionType.DataSession,
                            timeStamp = timeStamp.withHour(0),
                            map = mutableMapOf("actualByte" to 123456L)
                    ),
                    copy(type = ActionType.DataSession, timeStamp = timeStamp.withHour(7),
                            map = (map + ("actualByte" to 123456)).toMutableMap()
                    ),
                    copy(type = ActionType.Msg,
                            timeStamp = LocalDateTime.of(2020, 1, 1, 2, 0),
                            cost = 500.0))
        }

        var traffic = 0.0


        actions.forEach {
            traffic = sumOf(traffic, it, { (get("actualByte") as Number?)?.toDouble() }) {
                dataSession and timeBetween(from = LocalTime.of(1, 0),
                        to = LocalTime.of(8, 0))
            }
        }
        assertEquals(123456.0 * 1, traffic)
    }

    @Test
    fun `test time between when start lt end`() {
        val start = LocalTime.of(1, 0)
        val end = LocalTime.of(8, 0)

        assertTrue(Action(
                timeStamp = LocalDate.now().atTime(2, 0),
                type = ActionType.Msg
        ).timeBetween(start, end))

        assertFalse(Action(
                timeStamp = LocalDate.now().atTime(0, 11),
                type = ActionType.Msg
        ).timeBetween(start, end))
    }

    @Test
    fun `test time between when start gt end`() {
        val start = LocalTime.of(22, 0)
        val end = LocalTime.of(1, 0)

        listOf(
                LocalTime.of(0, 10) to true,
                LocalTime.of(22, 0) to true,
                LocalTime.of(21, 59) to false,
                LocalTime.of(0, 1) to true,
                LocalTime.of(23, 32) to true,
                LocalTime.of(12, 5) to true

        ).map {
            Action(type = ActionType.Msg, timeStamp = LocalDate.now().atTime(it.first)) to it.second
        }.forEach {
            assertEquals(it.first.timeBetween(start, end), it.second)
        }
    }


}


