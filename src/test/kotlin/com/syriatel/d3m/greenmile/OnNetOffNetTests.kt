package com.syriatel.d3m.greenmile

import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.domain.ActionType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test
import java.time.LocalDateTime

class OnNetOffNetTests {
    val actionOnNet = listOf(

            Action(type = ActionType.Call,
                    map = mutableMapOf("usageServiceType" to 10)) to true,
            Action(type = ActionType.DataSession,
                    map = mutableMapOf("usageServiceType" to 31)) to true,
            Action(type = ActionType.Msg,
                    map = mutableMapOf("usageServiceType" to 21)) to true,

            Action(type = ActionType.Call,
                    map = mutableMapOf("usageServiceType" to 11)) to false,
            Action(type = ActionType.Msg,
                    map = mutableMapOf("usageServiceType" to 22)) to false,
            Action(type = ActionType.DataSession,
                    map = mutableMapOf("usageServiceType" to 33)) to false
    )

    val actionOffNet = listOf(

            Action(type = ActionType.Call,
                    map = mutableMapOf("usageServiceType" to 11)) to true,
            Action(type = ActionType.DataSession,
                    map = mutableMapOf("usageServiceType" to 31)) to false,
            Action(type = ActionType.Msg,
                    map = mutableMapOf("usageServiceType" to 22)) to true,

            Action(type = ActionType.Call,
                    map = mutableMapOf("usageServiceType" to 10)) to false,

            Action(type = ActionType.DataSession,
                    map = mutableMapOf("usageServiceType" to 33)) to false
    )


    @Test
    fun `test OnNet Actions`() {
        actionOnNet.forEach {
            assertEquals(it.second, it.first.onNet)
        }
    }

    @Test
    fun `test OffNet Actions`() {
        actionOffNet.forEach {
            assertEquals(it.second,it.first.offNet)
        }
    }

}