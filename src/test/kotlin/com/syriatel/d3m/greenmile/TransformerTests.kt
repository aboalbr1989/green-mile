package com.syriatel.d3m.greenmile

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDateTime

class TransformerTests {

    @Test
    fun `should transform rec cdr to action`(){
        val rec = arrayOf(
                "20200101121000")

        val result = processRec(rec)

        assertEquals(result.startedAt, LocalDateTime.of(2020,1,1,12,10,0))
        assertEquals(result.type, ActionType.Call)
    }

}