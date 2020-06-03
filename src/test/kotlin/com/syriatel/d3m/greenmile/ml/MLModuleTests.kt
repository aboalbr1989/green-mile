package com.syriatel.d3m.greenmile.ml

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.util.RawValue
import com.syriatel.d3m.greenmile.schema.DoubleDescriptor
import com.syriatel.d3m.greenmile.schema.ObjectDescriptor
import com.syriatel.d3m.greenmile.schema.StringDescriptor
import com.syriatel.d3m.greenmile.utils.initDriver
import com.syriatel.d3m.greenmile.utils.serdeFor
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.KeyValueStore
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.*


class JsonNodeSerdeTests {

    lateinit var testDriver: TopologyTestDriver

    lateinit var input: TestInputTopic<String, String>

    @BeforeEach
    fun setup() {
        testDriver = TopologyTestDriver(
                StreamsBuilder().apply {
                    stream<String, JsonNode>("json", Consumed.with(serdeFor(), serdeFor())).mapValues { _, v ->
                        (v as ObjectNode).put("name", "ne3ma")
                    }.to("json2", Produced.with(
                            serdeFor(),
                            serdeFor()
                    ))
                }.build(),
                Properties().apply {
                    this[StreamsConfig.APPLICATION_ID_CONFIG] = "dummy"
                    this[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "dummy:9092"
                }
        )
        input = testDriver.createInputTopic("json", serdeFor<String>().serializer(), serdeFor<String>().serializer())
    }


    @Test
    fun `should deserialize string as JsonNode from topic`() {
        input.pipeInput(
                "hi",
                """{"name":"ibraheem","date":"1989-05-03"}"""
        )
        val output = testDriver.createOutputTopic("json2", serdeFor<String>().deserializer(), serdeFor<JsonNode>().deserializer())

        assertEquals("ne3ma", output.readKeyValue().value["name"].asText())
    }


    @AfterEach
    fun destroy() {
        testDriver.close()
    }


}


class MLPropertiesUpdateTests {
    lateinit var driver: TopologyTestDriver
    lateinit var mlResultUpdates: TestInputTopic<String, MLResultUpdate>

    @BeforeEach
    fun setup() {
        driver = initDriver {
            mlProfiles()
        }
        driver.createInputTopic(
                "mlModules", serdeFor<String>().serializer(), serdeFor<MLModule>().serializer()
        ).pipeInput("age", MLModule(
                version = 1L,
                resultSchema = ObjectDescriptor(
                        name = "age",
                        fields = listOf(
                                StringDescriptor(
                                        name = "prediction"
                                ),
                                DoubleDescriptor(
                                        name = "probability",
                                        min = 0.0,
                                        max = 1.0
                                ),
                                StringDescriptor(
                                        name = "LastUpdate", format = "date"
                                )
                        )
                ))
        )

        mlResultUpdates = driver.createInputTopic("ml-updates", serdeFor<String>().serializer(), serdeFor<MLResultUpdate>().serializer())

    }

    @Test
    fun `should update result for gsm for existed module`() {
        val store: KeyValueStore<String, MLProfile> = driver.getKeyValueStore("ml-profile")
        mlResultUpdates.pipeInput(
                "988957030",
                MLResultUpdate(
                        moduleName = "age",
                        value = JsonNodeFactory.instance.rawValueNode(
                                RawValue("""
                                    {
                                        "prediction": "18-25",
                                        "probability": 0.678,
                                        "LastUpdate": "2020-05-04"
                                    }
                                """.trimIndent())
                        )
                )
        )
        store["988957030"]["age"]?.let {
            assertEquals("18-25", it["prediction"].asText())
        }
    }

    @Test
    fun `should ignore result for gsm for non existed module`() {
        val store: KeyValueStore<String, MLProfile> = driver.getKeyValueStore("ml-profile")
        mlResultUpdates.pipeInput(
                "988957030",
                MLResultUpdate(
                        moduleName = "gender",
                        value = JsonNodeFactory.instance.rawValueNode(
                                RawValue("""{
                                        "prediction": "male",
                                        "probability": 0.678,
                                        "LastUpdate": "2020-05-04"
                                    }
                                """.trimIndent())
                        )
                )
        )
        assertNull(store["988957030"])
    }


    @AfterEach
    fun destroy() {
        driver.close()
    }
}