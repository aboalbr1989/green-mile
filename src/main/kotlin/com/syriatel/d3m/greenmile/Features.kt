package com.syriatel.d3m.greenmile
import java.time.LocalTime
import javax.management.Query.plus

fun countOf(acc: Int, action: Action, criteria: Action.() -> Boolean): Int {
    return if (criteria(action))
        acc + 1
    else
        acc
}

fun sumOf(acc: Double,action: Action,field : Action.() -> Double?,criteria: Action.() -> Boolean): Double {
    return if(criteria(action))
          acc + action.field()!!
    else
       return acc
}



fun sms(): Action.() -> Boolean = {
    type === ActionType.Msg
}

fun call(): Action.() -> Boolean = {
    type === ActionType.Call
}

fun withType(type: ActionType): Action.() -> Boolean = {
    this.type === type
}

fun timeBetween(from: LocalTime, to: LocalTime): Action.() -> Boolean = {
    timeStamp.toLocalTime().let {
        it.isAfter(from) && it.isBefore(to)
    }
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
    get() = getOrDefault("usageServiceType", 10) == 10