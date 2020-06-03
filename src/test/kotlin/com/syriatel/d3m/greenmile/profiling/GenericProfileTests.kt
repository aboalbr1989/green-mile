package com.syriatel.d3m.greenmile.profiling

import com.syriatel.d3m.greenmile.utils.initDriver
import jdk.nashorn.internal.ir.ObjectNode
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.BeforeEach

class GenericProfileTests {
    lateinit var topology: TopologyTestDriver

    lateinit var changes: TestInputTopic<String, ObjectNode>


    @BeforeEach
    fun setup() {
        initDriver {
            //profileChanges().groupByKey().aggregate()
        }
    }

    fun `should update profile with property`() {

    }

}