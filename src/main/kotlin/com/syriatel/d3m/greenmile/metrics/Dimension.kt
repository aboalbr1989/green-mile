package com.syriatel.d3m.greenmile.metrics

import com.syriatel.d3m.greenmile.domain.Action

data class Dimension(
        val id: Action.() -> String,
        val criteria: Action.() -> Boolean,
        val value: Action.() -> Number
)
