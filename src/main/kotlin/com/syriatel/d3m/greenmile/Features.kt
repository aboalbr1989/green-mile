package com.syriatel.d3m.greenmile

import org.apache.kafka.common.protocol.types.Field
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

fun Action.timeBetween(from: LocalTime, to: LocalTime): Boolean =
        if (from.isBefore(to))
            timeStamp.toLocalTime().let {
                it.plusNanos(1).isAfter(
                        from
                ) && it.minusNanos(1).isBefore(
                        to
                )
            }
        else {
            timeStamp.toLocalTime().let {
                it.plusNanos(1).isAfter(
                        from
                ) || it.minusNanos(1).isBefore(
                        to
                )
            }
        }


val Action.onNet: Boolean
    get() = when (type) {
        ActionType.Call -> get("usageServiceType") == 10
        ActionType.Msg -> get("usageServiceType") == 21
        ActionType.DataSession -> get("usageServiceType") == 31
        else -> false
    }

val Action.offNet: Boolean
    get() = when (type) {
        ActionType.Call -> get("usageServiceType") == 11
        ActionType.Msg -> get("usageServiceType") == 22

        else -> false
    }

val Action.roaming: Boolean
    get() = when (type) {
        ActionType.Call -> get("usageServiceType") == 15
        ActionType.Msg -> get("usageServiceType") == 24
        ActionType.DataSession -> get("usageServiceType") == 33

        else -> false
    }

val Action.international: Boolean
    get() = when (type) {
        ActionType.Call -> get("usageServiceType") == 13

        else -> false
    }

val Action.twoG: Boolean
    get() = get("sessionType") == 2

val Action.threeG: Boolean
    get() = get("sessionType") == 1

val Action.lte: Boolean
    get() = get("sessionType") == 6

val Action.sessionType: SessionType
    get() = when (get("sessionType")) {
        2 -> SessionType.LTE
        3 -> SessionType.THREE_G
        else -> SessionType.TWO_G
    }

enum class SessionType {
    THREE_G, TWO_G, LTE
}