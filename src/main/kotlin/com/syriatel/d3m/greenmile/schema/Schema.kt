package com.syriatel.d3m.greenmile.schema

import org.springframework.data.jpa.repository.JpaRepository
import java.util.*
import javax.persistence.CascadeType
import javax.persistence.Entity
import javax.persistence.Id
import javax.persistence.OneToMany


@Entity
data class Field(
        @Id
        val id: UUID = UUID.randomUUID(),
        val type: String = "",
        val name: String = "",
        val optional: Boolean = false,
        val ref: String? = null
)

@Entity
data class Schema(
        @Id
        val id: UUID = UUID.randomUUID(),
        @OneToMany(targetEntity = Field::class, orphanRemoval = true, cascade = [CascadeType.ALL])
        var fields: MutableList<Field> = mutableListOf(),
        val name: String = "",
        val version: Long = 0
)

interface SchemaRegistry : JpaRepository<Schema, UUID>
