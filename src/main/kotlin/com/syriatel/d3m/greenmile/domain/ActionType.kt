package com.syriatel.d3m.greenmile.domain

import com.syriatel.d3m.greenmile.transformers.*

enum class ActionType(val topic: String, val toAction: (Array<String>) -> Action) {
    Call("rec", processRec),
    SMS("sms", processSms),
    DataSession("data", processData),
    ActivateBundle("mon", processMon),
    Recharge("mgr", processVou)
}