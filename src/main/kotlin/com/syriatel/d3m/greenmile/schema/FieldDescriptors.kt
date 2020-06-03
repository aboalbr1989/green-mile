package com.syriatel.d3m.greenmile.schema

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes(
        JsonSubTypes.Type(name = "String", value = StringDescriptor::class),
        JsonSubTypes.Type(name = "Double", value = DoubleDescriptor::class),
        JsonSubTypes.Type(name = "Float", value = FloatDescriptor::class),
        JsonSubTypes.Type(name = "Integer", value = IntegerDescriptor::class),
        JsonSubTypes.Type(name = "Object", value = ObjectDescriptor::class),
        JsonSubTypes.Type(name = "GeoPoint", value = GeoLocationDescriptor::class),
        JsonSubTypes.Type(name = "Array", value = ArrayDescriptor::class)
)
interface FieldDescriptor {
    val name: String
}


class StringDescriptor(
        override val name: String, val maxLength: Int = Int.MAX_VALUE, val minLength: Int = 0, val format: String? = null, val pattern: String? = null
) : FieldDescriptor

class DoubleDescriptor(
        override val name: String, val max: Double = Double.MAX_VALUE, val min: Double = Double.MIN_VALUE
) : FieldDescriptor

class FloatDescriptor(
        override val name: String, val max: Float = Float.MAX_VALUE, val min: Float = Float.MIN_VALUE
) : FieldDescriptor

class IntegerDescriptor(
        override val name: String, val max: Int = Int.MAX_VALUE, val min: Int = Int.MIN_VALUE
) : FieldDescriptor

open class ObjectDescriptor(
        override val name: String, val fields: List<FieldDescriptor>
) : FieldDescriptor

class ArrayDescriptor(
        override val name: String, val elementFields: List<FieldDescriptor>
) : FieldDescriptor


class GeoLocationDescriptor(
        override val name: String
) : ObjectDescriptor(name, fields = listOf(
        StringDescriptor(name = "zone"),
        StringDescriptor(name = "area"),
        StringDescriptor(name = "subarea"),
        StringDescriptor(name = "city"),
        StringDescriptor(name = "site"),
        ObjectDescriptor(
                name = "location",
                fields = listOf(
                        FloatDescriptor(name = "latitude"),
                        FloatDescriptor(name = "longitude")
                )
        ),
        ObjectDescriptor(
                name = "siteLocation",
                fields = listOf(
                        FloatDescriptor(name = "latitude"),
                        FloatDescriptor(name = "longitude")
                )
        )
))