package com.syriatel.d3m.greenmile

import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.domain.ActionType
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.LocalDateTime


class RoamingTests {

    val action = Action(
            type = ActionType.Call,
            map = mutableMapOf(
                    "usageServiceType" to "15"
            )
    )

    @Test
    fun `roaming should be true when call and usageService is 15`() {
        assertTrue(action.copy(type = ActionType.Call, map = mutableMapOf("usageServiceType" to 15)).roaming)
    }

    @Test
    fun `roaming should be true when sms and usageService is 24`() {
        assertTrue(action.copy(type = ActionType.Msg, map = mutableMapOf("usageServiceType" to 24)).roaming)
    }

    @Test
    fun `roaming should be true when data and usageService is 33`() {
        assertTrue(action.copy(type = ActionType.DataSession, map = mutableMapOf("usageServiceType" to 33)).roaming)
    }


    @Test
    fun `roaming test`() {
        val actions = listOf(

                Action( type = ActionType.Call,
                        map = mutableMapOf("usageServiceType"  to 15)) to true,
                Action( type = ActionType.DataSession,
                        map = mutableMapOf("usageServiceType"  to 33)) to true,
                Action( type = ActionType.Msg,
                        map = mutableMapOf("usageServiceType"  to 24)) to true,

                Action( type = ActionType.Call,
                        map = mutableMapOf("usageServiceType"  to 10)) to false,
                Action( type = ActionType.Msg,
                        map = mutableMapOf("usageServiceType"  to 21)) to false,
                Action( type = ActionType.DataSession,
                        map = mutableMapOf("usageServiceType"  to 31)) to false


        )

       actions.forEach {
           assertEquals(it.first.roaming,it.second)
       }
    }

    }
