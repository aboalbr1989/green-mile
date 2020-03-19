package com.syriatel.d3m.greenmile.metrics

import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.domain.ActionType

fun <T : Comparable<T>> max(latestMax: T?, action: Action, criteria: (Action) -> Boolean, field: (Action) -> T?): T? {
    if (criteria(action))
        if (field(action) != null)
            if (latestMax == null)
                return field(action)
            else if (field(action) ?: 0 > latestMax)
                return field(action)

    return latestMax
}

fun countOf(type: ActionType, it: Int, action: Action): Int {
    return if (action.type == type) it + 1
    else it
}


fun sumOf(acc: Number, action: Action, criteria: Action.() -> Boolean, field: (Action.() -> Number?)) =
        if (criteria(action)) {
            acc.toDouble() + (action.field()?.toDouble() ?: 0.0)
        } else acc.toDouble()


fun count(acc: Int, action: Action, fn: Action?.() -> Boolean = { this != null }): Int =
        sumOf(acc, action, fn, { 1 }).toInt()
