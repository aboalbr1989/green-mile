package com.syriatel.d3m.greenmile.profiling

import com.syriatel.d3m.greenmile.utils.serdeFor
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.util.*

class CustomerProfileStreamTests {
    lateinit var testDriver: TopologyTestDriver

    val records = ConsumerRecordFactory(
            "customer-profiles",
            serdeFor<String>().serializer(),
            serdeFor<CustomerProfile>().serializer()
    )

    @BeforeEach
    fun setup() {
        testDriver = TopologyTestDriver(
                StreamsBuilder().apply {
                    customerProfiles()
                }.build(),
                Properties().apply {
                    this[StreamsConfig.APPLICATION_ID_CONFIG] = "dummy"
                    this[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "dummy:9092"
                }
        )

    }


    @Test
    fun `should update customer profile`() {
        testDriver.getKeyValueStore<String, CustomerProfile>("customer-profiles-store").apply {
            testDriver.pipeInput(
                    records.create(
                            "customer-profiles",
                            listOf(
                                    KeyValue(
                                            "988957030",
                                            CustomerProfile(
                                                    birthDate = LocalDate.parse("1989-05-03"),
                                                    gender = "male",
                                                    activationDate = LocalDate.parse("2010-03-03")
                                            )
                                    )
                            )
                    )
            )


            assertEquals(LocalDate.of(1989, 5, 3), this["988957030"].birthDate)
            assertEquals("male", this["988957030"].gender)
            assertEquals(LocalDate.of(2010, 3, 3), this["988957030"].activationDate)
        }

    }

    @AfterEach
    fun destroy() {
        testDriver.close()
    }


}