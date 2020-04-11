package com.syriatel.d3m.greenmile.metrics

import com.syriatel.d3m.greenmile.domain.Action

data class MultiMetricDimension(
        val idTemplate: Action.() -> String,
        val criteria: Action.() -> Boolean,
        val metrics: Map<String, Action.() -> Number>
)
