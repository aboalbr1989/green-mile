package com.syriatel.d3m.greenmile.domain

import com.syriatel.d3m.greenmile.transformers.processData
import com.syriatel.d3m.greenmile.transformers.processMon
import com.syriatel.d3m.greenmile.transformers.processRec
import com.syriatel.d3m.greenmile.transformers.processSms
import java.time.LocalDateTime

data class Action(
        val timeStamp: LocalDateTime = LocalDateTime.now(),
        val performedBy: String = "",
        val performerCell: String = "",
        val type: ActionType,
        val offer: String = "",
        val cost: Double? = null,
        val map: MutableMap<String, Any?> = mutableMapOf()
) : MutableMap<String, Any?> by map

