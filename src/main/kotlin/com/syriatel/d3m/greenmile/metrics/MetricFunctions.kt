package com.syriatel.d3m.greenmile.metrics

import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.domain.ActionType

fun <T : Comparable<T>> max(latestMax: T?, action: Action, criteria: (Action) -> Boolean, field: (Action) -> T?): T? {
    return if (criteria(action)) {
        field(action)?.let { v ->
            if (latestMax == null)
                return@let field(action)
            else
                return@let maxOf(v, latestMax)
        } ?: latestMax
    } else {
        latestMax
    }

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


val actionCost = "cost" to { action: Action ->
    (action.cost ?: 0.0).toFloat()
}

val actionDuration = "duration" to { action: Action ->
    ((action.map["actualDuration"] ?: 0.0) as Number).toFloat()
}

val dataSize = "dataSize" to { action: Action ->
    ((action.map["actualByte"] ?: 0.0) as Number).toFloat()
}