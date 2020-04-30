package com.syriatel.d3m.greenmile.domain

import org.springframework.stereotype.Component

@Component
class NamedCriteriaRepository {
    val map = mutableMapOf<String, Action.() -> Boolean>()
    fun save(name: String, criteria: Action.() -> Boolean) {
        if (map.containsKey(name)) {
            throw CriteriaAlreadyExist(name)
        }
        map.putIfAbsent(name, criteria)
    }

    fun fetch(name: String): Action.() -> Boolean =
            map[name] ?: throw CriteriaNotFound(name)
}


