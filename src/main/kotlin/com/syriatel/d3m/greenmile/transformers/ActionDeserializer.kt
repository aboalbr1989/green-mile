package com.syriatel.d3m.greenmile.transformers

import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.domain.ActionType
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import java.nio.charset.Charset


class ActionDeserializer(val delimiter: String = ",") : Deserializer<Action> {
    override fun deserialize(topic: String, value: ByteArray): Action? =
            ActionType.values().findLast {
                it.topic === topic
            }?.toAction(value.toString(Charset.defaultCharset()).split(delimiter).toTypedArray())
}

val actionCsvSerde: Serde<Action> = object : Serde<Action> {
    val des = ActionDeserializer()
    override fun deserializer(): Deserializer<Action> =
            des

    override fun serializer(): Serializer<Action> {
        TODO("Not yet implemented")
    }
}



