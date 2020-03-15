package com.syriatel.d3m.greenmile

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.junit.jupiter.api.Assertions.assertEquals
import java.time.LocalDateTime

@SpringBootTest
class GreenMileApplicationTests {

    val actions= listOf(
            Action(performedBy = "0933886839", type = ActionType.Call, timeStamp = LocalDateTime.of(
                    2020, 1, 1, 12, 1)),

            Action(performedBy = "0933886839", type = ActionType.Call, timeStamp = LocalDateTime.of(
                    2020, 1, 1, 15, 10)),

            Action(performedBy = "0933886839", type = ActionType.Call, timeStamp = LocalDateTime.of(
                    2020, 1, 1, 19, 59)),

            Action(performedBy = "0933886839", type = ActionType.Msg, timeStamp = LocalDateTime.of(
                    2020, 1, 1, 16, 10)),

            Action(performedBy = "0933886839", type = ActionType.Msg, timeStamp = LocalDateTime.of(
                    2020, 1, 1, 19, 10)),

            Action(performedBy = "0933886839", type = ActionType.Msg, timeStamp = LocalDateTime.of(
                    2020, 1, 1, 21, 10))

    )

    @Test
    fun contextLoads() {
    }

    @Test
    fun `should be count of action`(){

        var acc = 0;
        actions.forEach {
            acc = countOf(acc, it, ActionType.Call)
            println(acc)
        }
        assertEquals(3,acc)
    }

    fun countOf(acc: Int,action:Action,actionType : ActionType): Int{
            if(action.type == actionType)
                    return acc + 1
            else
                return acc
      }
}
