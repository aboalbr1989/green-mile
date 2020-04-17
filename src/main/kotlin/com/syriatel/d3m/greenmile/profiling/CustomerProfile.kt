package com.syriatel.d3m.greenmile.profiling

import com.syriatel.d3m.greenmile.utils.materializedAsKeyValueStore
import com.syriatel.d3m.greenmile.utils.serdeFor
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KTable
import java.time.LocalDate


fun StreamsBuilder.customerProfiles(): KTable<String, CustomerProfile> =
        table("customer-profiles", Consumed.with(
                serdeFor(),
                serdeFor()
        ), materializedAsKeyValueStore("customer-profiles-store",
                serdeFor(),
                serdeFor()
        ))


data class CustomerProfile(
        var gender: String? = null,
        var birthDate: LocalDate? = null,
        var lineDate: LocalDate? = null,
        var tariffProfile: String? = null,
        var lineType: String? = null
)

