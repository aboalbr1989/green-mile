package com.syriatel.d3m.greenmile.generation

import com.syriatel.d3m.greenmile.domain.ActionType
import java.time.LocalDateTime


class FieldGenerator<T>(
        val fieldName: String,
        val supplier: () -> T
)

fun buildMap(data: List<FieldGenerator<Any>>): Map<String, Any> =
        mapOf(
                *data.map {
                    it.fieldName to it.supplier()
                }.toTypedArray()
        )

fun <T> generator(fieldName: String,
                  supplier: () -> T) = FieldGenerator<T>(
        fieldName = fieldName,
        supplier = supplier
)


fun generatorOf(vararg pairs: Pair<String, () -> Any>) =
        pairs.map {
            FieldGenerator(it.first, it.second)
        }.toList()


fun main() {
    generatorOf(
            "timeStamp" to {
                LocalDateTime.now()
            },
            "type" to {
                listOf(ActionType.Call).random()
            },
            "performedBy" to {
                listOf("988957030", "993995179").random()
            },
            "performerCell" to {
                listOf("0001", "0002")
            }

    )
}