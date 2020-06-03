package com.syriatel.d3m.greenmile.profiling

import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController

@RestController
class Rest(val kafkaStreams: KafkaStreams) {

    @GetMapping("/api/key-values/{storeName}/{key}")
    fun fetchValue(
            @PathVariable storeName: String,
            @PathVariable key: String
    ): JsonNode =
            kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                    storeName,
                    keyValueStore<String, JsonNode>()
            ))[key]
}