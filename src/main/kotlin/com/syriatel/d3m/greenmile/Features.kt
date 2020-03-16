package com.syriatel.d3m.greenmile

import java.time.LocalTime

fun countOf(acc: Int, action: Action, criteria: Action.() -> Boolean): Int {
    return if (criteria(action))
        acc + 1
    else
        acc
}

fun sumOf(acc: Double, action: Action, field: Action.() -> Double?, criteria: Action.() -> Boolean): Double {
    return if (criteria(action))
        acc + action.field()!!
    else
        return acc
}


infix fun (Action.() -> Boolean).and(fn: (Action.() -> Boolean)): (Action.() -> Boolean) = {
    this@and(this) && fn(this)
}


infix fun (Action.() -> Boolean).or(fn: (Action.() -> Boolean)): (Action.() -> Boolean) = {
    this@or(this) || fn(this)
}

val Action.call: Boolean
    get() = type === ActionType.Call

val Action.sms: Boolean
    get() = type === ActionType.Msg


fun Action.timeBetween(from: LocalTime, to: LocalTime): Boolean =
        timeStamp.toLocalTime().let { it.isAfter(from) && it.isBefore(to) }

val Action.onNet: Boolean
    get() = getOrDefault("usageServiceType", 0) == 10

val Action.offNet: Boolean
    get() = getOrDefault("usageServiceType", 0) == 11