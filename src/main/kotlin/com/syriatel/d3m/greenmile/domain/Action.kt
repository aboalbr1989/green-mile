package com.syriatel.d3m.greenmile.domain


import java.time.LocalDateTime

data class Action(
        val timeStamp: LocalDateTime = LocalDateTime.now(),
        val performedBy: String = "",
        val performerCell: String = "",
        val type: ActionType = ActionType.ActivateBundle,
        val offer: String = "",
        val cost: Double? = null,
        val map: MutableMap<String, Any?> = mutableMapOf()
)

