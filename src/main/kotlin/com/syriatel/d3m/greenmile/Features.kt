package com.syriatel.d3m.greenmile

import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.domain.ActionType
import java.time.LocalTime

fun countOf(acc: Int, action: Action, criteria: Action.() -> Boolean): Int {
    return if (criteria(action))
        acc + 1
    else
        acc
}

fun sumOf(acc: Double, action: Action, field: Action.() -> Double?, criteria: Action.() -> Boolean): Double {
    return if (criteria(action))
        acc + (action.field() ?: 0.0)
    else
        return acc
}


val Action.call: Boolean
    get() = type === ActionType.Call

val Action.sms: Boolean
    get() = type === ActionType.Msg

val Action.dataSession: Boolean
    get() = type === ActionType.DataSession


fun Action.timeBetween(from: LocalTime, to: LocalTime): Boolean {
    if(timeStamp.toLocalTime().equals(from) || timeStamp.toLocalTime().equals(to))
        return true
    else if (from.isBefore(to))
        return timeStamp.toLocalTime().let {
            it.isAfter(from) && it.isBefore(to)
        }
    else {
        return timeStamp.toLocalTime().let {
            it.isAfter(from) || it.isBefore(to)
        }
    }
}

val Action.onNet: Boolean
    get() = when (type) {
        ActionType.Call -> map["usageServiceType"] == 10
        ActionType.Msg -> map["usageServiceType"] == 21
        ActionType.DataSession -> map["usageServiceType"] == 31
        else -> false
    }

val Action.offNet: Boolean
    get() = when (type) {
        ActionType.Call -> map["usageServiceType"] == 11
        ActionType.Msg -> map["usageServiceType"] == 22

        else -> false
    }

val Action.roaming: Boolean
    get() = when (type) {
        ActionType.Call -> map["usageServiceType"] == 15
        ActionType.Msg -> map["usageServiceType"] == 24
        ActionType.DataSession -> map["usageServiceType"] == 33

        else -> false
    }

val Action.international: Boolean
    get() = when (type) {
        ActionType.Call -> map["usageServiceType"] == 13

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

enum class SessionType {
    THREE_G, TWO_G, LTE
}