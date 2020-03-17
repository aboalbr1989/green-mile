package com.syriatel.d3m.greenmile

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.time.LocalTime

class TransformerTests {

    @Test
    fun `should transform rec cdr to action`() {

        val recArray = arrayOf("20200101121000","0933886839","020202020202","030303","not used",
                "20200101121011","0933886839","46","60","090909090909","transactionType","11111111","1000.0",
                "13.0","0.0","0.0","1","0993995060","181818","1919191919","2020202020","21212121","222222",
                "23232323","24242424","25252525","262626","2727272727","2828282828","29292929","3030303030",
                "31313131","32323232","3333333","34343434","35353535")

        val result = processRec(recArray)

        assertEquals(result.timeStamp, LocalDateTime.of(2020, 1, 1, 12, 10, 0))
        assertEquals(result.timeStamp,result.get("startedAt"))
        assertEquals(result.get("finishedAt"), LocalDateTime.of(2020, 1, 1, 12, 10, 11))

        assertEquals(result.type, ActionType.Call)


    }



    @Test
    fun `should calculate total actions`() {
        val acc = listOf(1, 5, 8, 9)

        //sumOfSmsPerWeek(Action,acc)
        acc.forEach { ac ->
            ActionType.values().forEach { at ->
                assertEquals(ac + 1, countOf(at, ac, Action(
                        type = at
                )))
                ActionType.values().toSet().filter { it != at }.forEach {
                    assertEquals(ac, countOf(at, ac, Action(
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


        val col: (String) -> (Action.() -> Number) = { name ->
            {
                getOrDefault(name, 0) as Number
            }
        }

        assertEquals(10.0, sumOf(8.0, Action(cost = 2.0, type = ActionType.Call, timeStamp = LocalDateTime.of(
                2020, 1, 1, 19, 10
        )), atNight, { this.cost }))


    }

    @Test
    fun `calculate sum of field based on criteria2`() {
        val actions = listOf(
                Action(performedBy = "0933886839", type = ActionType.Call, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 12, 1)).apply { put("usageServiceType", 10) },

                Action(performedBy = "0933886850", type = ActionType.Call, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 15, 10)).apply { put("usageServiceType", 10) },

                Action(performedBy = "0933886780", type = ActionType.Call, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 19, 59)).apply { put("usageServiceType", 10) },

                Action(performedBy = "0933887850", type = ActionType.Call, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 16, 10)).apply { put("usageServiceType", 47) },

                Action(performedBy = "0933789850", type = ActionType.Msg, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 19, 10)).apply { put("usageServiceType", 10) },

                Action(performedBy = "0933789850", type = ActionType.Call, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 21, 10)).apply { put("usageServiceType", 10) }
        )
        val results = listOf(1, 2, 3, 3, 3, 3)

        val criteria2: Action?.() -> Boolean = {
            if (this == null)
                false
            else
                (this.timeStamp.toLocalTime().let {
                    it.isAfter(LocalTime.MIDNIGHT) && it.isBefore(LocalTime.of(20, 0))
                }) && type == ActionType.Call && get("usageServiceType") == 10
        }




        actions.forEachIndexed { i, action ->
            assertEquals(results[i], count(if (i > 0) results[i - 1] else 0, action, criteria2))
        }


    }

    @Test
    fun `should find the latest action by cost`() {
        val actions = listOf(
                Action(performedBy = "0933886839", type = ActionType.Call,  cost = null),

                Action(performedBy = "0933886839", type = ActionType.Call,  cost = 16.0),

                Action(performedBy = "0933886839", type = ActionType.Call,  cost = 17.0),

                Action(performedBy = "0933886839", type = ActionType.Call,  cost = 19.0),

                Action(performedBy = "0933886839", type = ActionType.Msg, cost = 31.0),

                Action(performedBy = "0933886839", type = ActionType.Call,  cost = null)

        )


        var result2: Double? = null

        for (a in actions) {
            result2 = max(result2, a, { it.type == ActionType.Call }, { it.cost })
        }
        assertEquals(19.0, result2)


    }

    @Test
    fun `should find the latest action and all costs are null`() {
        val actions = listOf(
                Action(performedBy = "0933886839", type = ActionType.Call, cost = null),

                Action(performedBy = "0933886839", type = ActionType.Call, cost = null),
                Action(performedBy = "0933886839", type = ActionType.Msg, cost = null),
                Action(performedBy = "0933886839", type = ActionType.Call, cost = null),
                Action(performedBy = "0933886839", type = ActionType.Call, cost = null)


                )


        var result2: Double? = null

        for (a in actions) {
            result2 = max(result2, a, { it.type == ActionType.Call }, { it.cost })
        }
        assertEquals(null, result2)


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

                Action(performedBy = "0933886839", type = ActionType.Msg, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 19, 10), cost = 31.0),

                Action(performedBy = "0933886839", type = ActionType.Call, timeStamp = LocalDateTime.of(
                        2020, 1, 1, 21, 10), cost = null)

        )


        fun newCriteria(action:Action):Boolean {
            return action.type == ActionType.Call
        }
        //Max(maxValue,action,criteria,field)
        var result:LocalDateTime?= null
        for( a in actions){
            result= max(result,a,{it.type == ActionType.Call})
                    { it.timeStamp }
        }
        assertEquals(LocalDateTime.of(2020, 1, 1, 21, 10),result)




    }
}

/**
 * =========== Implement Functions Under
 */

fun <T : Comparable<T>> max(latestMax: T?, action: Action, criteria: (Action) -> Boolean, field: (Action) -> T?): T? {
    if (criteria(action))
        if (field(action) != null )
            if (latestMax == null)
                return field(action)
            else if (field(action)?:0 > latestMax)
                return field(action)

    return latestMax
}

fun countOf(type: ActionType, it: Int, action: Action): Int {
    return if (action.type == type) it + 1
    else it
}


fun sumOf(acc: Number, action: Action, criteria: Action.() -> Boolean, field: (Action.() -> Number?)) =
        if (criteria(action)) {
            acc.toDouble() + (action.field()?.toDouble() ?: 0.0)
        } else acc.toDouble()


fun count(acc: Int, action: Action, fn: Action?.() -> Boolean = { this != null }): Int =
        sumOf(acc, action, fn, { 1 }).toInt()
