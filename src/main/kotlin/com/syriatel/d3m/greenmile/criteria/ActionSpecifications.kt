package com.syriatel.d3m.greenmile.criteria

import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.domain.ActionType
import java.time.LocalTime

val Action.international: Boolean
    get() = when (type) {
        ActionType.Call -> map["usageServiceType"] == 13

        else -> false
    }

enum class SessionType {
    THREE_G, TWO_G, LTE
}

val Action.roaming: Boolean
    get() = when (type) {
        ActionType.Call -> map["usageServiceType"] == 15
        ActionType.SMS -> map["usageServiceType"] == 24
        ActionType.DataSession -> map["usageServiceType"] == 33

        else -> false
    }


val Action.call: Boolean
    get() = type === ActionType.Call

val Action.sms: Boolean
    get() = type === ActionType.SMS

val Action.dataSession: Boolean
    get() = type === ActionType.DataSession


fun Action.timeBetween(from: LocalTime, to: LocalTime): Boolean {
    return if (timeStamp.toLocalTime() == from || timeStamp.toLocalTime() == to)
        true
    else if (from.isBefore(to))
        timeStamp.toLocalTime().let {
            it.isAfter(from) && it.isBefore(to)
        }
    else {
        timeStamp.toLocalTime().let {
            it.isAfter(from) || it.isBefore(to)
        }
    }
}

val Action.onNet: Boolean
    get() = when (type) {
        ActionType.Call -> map["usageServiceType"] == 10
        ActionType.SMS -> map["usageServiceType"] == 21
        ActionType.DataSession -> map["usageServiceType"] == 31
        else -> false
    }

val Action.offNet: Boolean
    get() = when (type) {
        ActionType.Call -> map["usageServiceType"] == 11
        ActionType.SMS -> map["usageServiceType"] == 22

        else -> false
    }

val Action.twoG: Boolean
    get() = map["sessionType"] == 2

val Action.threeG: Boolean
    get() = map["sessionType"] == 1

val Action.lte: Boolean
    get() = map["sessionType"] == 6

val Action.sessionType: SessionType
    get() = when (map["sessionType"]) {
        2 -> SessionType.LTE
        3 -> SessionType.THREE_G
        else -> SessionType.TWO_G
    }

