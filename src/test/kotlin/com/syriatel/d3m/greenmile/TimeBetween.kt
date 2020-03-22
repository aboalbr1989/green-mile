package com.syriatel.d3m.greenmile

import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.domain.ActionType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime

class TimeBetween {
    val action = listOf(

            Action(type = ActionType.Call,
                    timeStamp = LocalDate.now().atTime(0,0))to true,

            Action(type = ActionType.Call,
                    timeStamp = LocalDate.now().atTime(2,0))to true,

            Action(type = ActionType.Call,
                    timeStamp = LocalDate.now().atTime(16,0))to true,

            Action(type = ActionType.DataSession,
                    timeStamp = LocalDate.now().atTime(23,0)) to false,

            Action(type = ActionType.DataSession,
                    timeStamp = LocalDate.now().atTime(21,0)) to false


    )

    @Test
    fun `time between 00 to 20`(){
            val midNight = LocalTime.of(0,0)
            val endHour = LocalTime.of(20,0)

            action.forEach {
                assertEquals(it.second,it.first.timeBetween(midNight,endHour))


            }
    }

    @Test
    fun `test time between when start lt end`() {
        val start = LocalTime.of(1, 0)
        val end = LocalTime.of(8, 0)

        Assertions.assertTrue(Action(
                timeStamp = LocalDate.now().atTime(2, 0),
                type = ActionType.Msg
        ).timeBetween(start, end))

        Assertions.assertFalse(Action(
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
                LocalTime.of(12, 5) to false

        ).map {
            Action(type = ActionType.Msg, timeStamp = LocalDate.now().atTime(it.first)) to it.second
        }.forEach {
            Assertions.assertEquals(it.first.timeBetween(start, end), it.second)
        }
    }



}


