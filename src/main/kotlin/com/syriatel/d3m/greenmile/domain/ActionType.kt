package com.syriatel.d3m.greenmile.domain

import com.syriatel.d3m.greenmile.transformers.processData
import com.syriatel.d3m.greenmile.transformers.processMon
import com.syriatel.d3m.greenmile.transformers.processRec
import com.syriatel.d3m.greenmile.transformers.processSms

enum class ActionType(val topic: String, val toAction: (Array<String>) -> Action) {
    Call("rec", processRec),
    Msg("sms", processSms),
    DataSession("data", processData),
    ActivateBundle("mon", processMon),
}