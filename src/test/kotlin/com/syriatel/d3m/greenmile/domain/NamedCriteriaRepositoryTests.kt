package com.syriatel.d3m.greenmile.domain

import com.syriatel.d3m.greenmile.criteria.call
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows

class NamedCriteriaRepositoryTests {
    lateinit var repository: NamedCriteriaRepository

    @BeforeEach
    fun setup() {
        repository = NamedCriteriaRepository()
    }

    @Test
    fun `should save and fetch criteria`() {
        val criteria: Action.() -> Boolean = {
            call
        }
        assertDoesNotThrow {
            repository.save("calls", criteria)
        }

        assertEquals(
                criteria,
                repository.fetch("calls")
        )
    }

    @Test
    fun `should throw criteria not found exception when fetch not existed criteria`() {
        val e = assertThrows<CriteriaNotFound> {
            repository.fetch("calls")
        }
        assertEquals("calls", e.message)
    }

    @Test
    fun `should throw criteria already exist when save criteria with same name as existed one`() {
        repository.save("calls") {
            call
        }
        val exception = assertThrows<CriteriaAlreadyExist> {
            repository.save("calls") {
                call
            }
        }
        assertEquals(exception.message, "calls")
    }
}